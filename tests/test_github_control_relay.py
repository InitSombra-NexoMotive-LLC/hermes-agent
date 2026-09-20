import json
import socket
import subprocess
import threading
from pathlib import Path

import pytest

from gateway.github_control_relay import RelayConfig, RelayError, StatusRelay, UnixSocketClient


TRUSTED = "a" * 40

def command(command_id="nvidia-status-20260920-001"):
 return {"protocol_version":1,"command_id":command_id,"source":"github-control","worker_id":"nvidia-control","workstream":"CONTROL_PLANE","task_id":"fixture","command_type":"STATUS","instructions":"status only"}

class Socket:
 def __init__(self): self.submits=0
 def request(self, payload):
  if payload["verb"] == "submit-command": self.submits += 1; return {"status":"ACCEPTED"}
  return {"status":"OK","command_id":payload["command_id"],"state":"COMPLETED","sanitized_result":"safe","result_digest":"digest","result_truncated":False,"created_at":"t","updated_at":"t","completed_at":"t","reason":None,"worker_id":"nvidia-control","workstream":"CONTROL_PLANE","payload":"must-not-publish"}

def git(*args, cwd=None): return subprocess.run(["git",*args], cwd=cwd, check=True, text=True, stdout=subprocess.PIPE).stdout.strip()

def test_end_to_end_command_publish_and_restart(tmp_path):
 bare=tmp_path/'remote.git'; git('init','--bare',str(bare))
 work=tmp_path/'seed'; git('clone',str(bare),str(work)); git('checkout','-b','control/nvidia-command-inbox',cwd=work)
 path=work/'control-inbox/commands'; path.mkdir(parents=True); (path/'nvidia-status-20260920-001.json').write_text(json.dumps(command()))
 git('add','.',cwd=work); git('-c','user.name=fixture','-c','user.email=fixture@example.invalid','commit','-m','command',cwd=work); head=git('rev-parse','HEAD',cwd=work); git('push','origin','HEAD:control/nvidia-command-inbox',cwd=work)
 sock=Socket(); config=RelayConfig(remote=str(bare),branch='control/nvidia-command-inbox',trusted_head=head,workspace=tmp_path/'relay-work',state_db=tmp_path/'relay.db')
 relay=StatusRelay(config, sock, lambda sha:{"login":"InitSombra-NexoMotive-LLC","id":239685310})
 assert relay.poll_once()==['nvidia-status-20260920-001'] and sock.submits==1
 git('fetch','origin',cwd=work)
 assert git('show','origin/control/nvidia-command-inbox:control-inbox/results/nvidia-status-20260920-001.json',cwd=work)
 assert StatusRelay(config,sock,lambda sha:{"login":"InitSombra-NexoMotive-LLC","id":239685310}).poll_once()==[] and sock.submits==1

@pytest.mark.parametrize('patch',[
 {"command_type":"CONTINUE"},{"worker_id":"other"},{"extra":"nope"},{"protocol_version":2},
 {"command_id":":bad"},{"command_id":".bad"},{"command_id":"_bad"},{"command_id":"-bad"},
])
def test_schema_rejections(tmp_path,patch):
 data=command(); data.update(patch)
 with pytest.raises(RelayError): StatusRelay.validate_command(data)


def test_actual_unix_socket_framing_and_response_validation(tmp_path):
 path=str(tmp_path/'socket')
 server=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); server.bind(path); server.listen(1)
 def serve():
  conn,_=server.accept()
  with conn:
   assert conn.makefile('rb').readline().endswith(b'\n')
   conn.sendall(b'{"ok":true,"protocol":1,"result":{"status":"ACCEPTED"}}\n')
  server.close()
 thread=threading.Thread(target=serve); thread.start()
 assert UnixSocketClient(path,timeout=1).request({'verb':'submit-command'})=={'status':'ACCEPTED'}
 thread.join()
 with pytest.raises(RelayError): UnixSocketClient(str(tmp_path/'missing'),timeout=.01).request({'verb':'command-status'})
