import json, subprocess
from pathlib import Path
import pytest
from gateway.github_control_relay import RelayConfig, RelayError, StatusRelay

BRANCH='control/nvidia-command-inbox'
def git(*args,cwd):
 return subprocess.run(['git',*args],cwd=cwd,check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout.strip()
def command():
 return {'protocol_version':1,'command_id':'crash','source':'github-control','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'fixture','command_type':'STATUS','instructions':'status'}
class NoSocket:
 def __init__(self):self.calls=[]
 def request(self,payload):self.calls.append(payload);raise AssertionError('socket must not be called')
def setup(tmp_path):
 bare=tmp_path/'remote.git';git('init','--bare',str(bare),cwd=tmp_path)
 seed=tmp_path/'seed';git('clone',str(bare),str(seed),cwd=tmp_path);git('checkout','-b',BRANCH,cwd=seed);(seed/'sentinel.txt').write_text('preserve');git('add','.',cwd=seed);git('-c','user.name=x','-c','user.email=x@y','commit','-m','base',cwd=seed);base=git('rev-parse','HEAD',cwd=seed);git('push','origin',f'HEAD:{BRANCH}',cwd=seed)
 work=tmp_path/'relay';git('clone',str(bare),str(work),cwd=tmp_path);git('checkout',BRANCH,cwd=work);sock=NoSocket();relay=StatusRelay(RelayConfig(remote=str(bare),branch=BRANCH,trusted_head=base,workspace=work,state_db=tmp_path/'state.db'),sock,lambda _: {})
 data=command();result={'command_id':'crash','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','state':'COMPLETED','reason':None,'created_at':'t','updated_at':'t','completed_at':'t','sanitized_result':'safe','result_digest':'x'*64,'result_truncated':False}
 relay.save('crash',base,'DISCOVERED',data);relay.save('crash',base,'SUBMITTED',data);relay.save('crash',base,'RESULT_READY',data,result=result)
 with relay.db() as d:d.execute("INSERT INTO relay_meta VALUES('head',?)",(base,))
 target=work/'control-inbox/results/crash.json';target.parent.mkdir(parents=True);raw=json.dumps(result,sort_keys=True,separators=(',',':'))+'\n'
 return relay,bare,base,target,raw,sock
def remote_result(bare):
 clone=bare.parent/'check';git('clone',str(bare),str(clone),cwd=bare.parent);git('checkout',BRANCH,cwd=clone);return clone
def no_remote_result(bare):
 check=bare.parent/'absence';git('clone',str(bare),str(check),cwd=bare.parent);git('checkout',BRANCH,cwd=check);assert not (check/'control-inbox/results/crash.json').exists()
def count_result(bare):
 return len([x for x in git('log','--format=%H','--','control-inbox/results/crash.json',cwd=bare.parent/'check').splitlines() if x])
def verify(relay,bare,target,sock):
 assert relay.row('crash')[1]=='PUBLISHED' and not sock.calls and (relay.c.workspace/'sentinel.txt').read_text()=='preserve'
 check=remote_result(bare);assert (check/target.relative_to(relay.c.workspace)).read_bytes()==json.dumps(json.loads(relay.row('crash')[3]),sort_keys=True,separators=(',',':')).encode()+b'\n';assert count_result(bare)==1

def test_crash_after_result_write_before_add_recovers(tmp_path):
 relay,bare,_,target,raw,sock=setup(tmp_path);target.write_text(raw);relay.poll_once();verify(relay,bare,target,sock)
def test_crash_after_result_add_before_commit_recovers(tmp_path):
 relay,bare,_,target,raw,sock=setup(tmp_path);target.write_text(raw);git('add','--',str(target.relative_to(relay.c.workspace)),cwd=relay.c.workspace);relay.poll_once();verify(relay,bare,target,sock)
def test_crash_after_local_commit_before_push_recovers(tmp_path):
 relay,bare,_,target,raw,sock=setup(tmp_path);target.write_text(raw);git('add','.',cwd=relay.c.workspace);git('-c','user.name=x','-c','user.email=x@y','commit','-m','local',cwd=relay.c.workspace);relay.poll_once();verify(relay,bare,target,sock);relay.poll_once();assert count_result(bare)==1
def test_unexpected_dirty_workspace_rejected(tmp_path):
 relay,bare,base,target,raw,sock=setup(tmp_path);a=relay.c.workspace/'bad';b=relay.c.workspace/'also-bad';a.write_text('a');b.write_text('b')
 with pytest.raises(RelayError) as e:relay.poll_once()
 assert e.value.code=='WORKSPACE_DIRTY' and a.exists() and b.exists() and relay.row('crash')[1]=='RESULT_READY' and not sock.calls and relay.git('rev-parse','HEAD')==base;no_remote_result(bare)
def test_mismatched_staged_result_rejected(tmp_path):
 relay,bare,_,target,raw,sock=setup(tmp_path);target.write_text('{}\n');git('add','.',cwd=relay.c.workspace)
 with pytest.raises(RelayError) as e:relay.poll_once()
 assert e.value.code=='WORKSPACE_DIRTY' and target.exists() and not sock.calls;no_remote_result(bare)
def test_symlink_result_rejected(tmp_path):
 relay,bare,_,target,raw,sock=setup(tmp_path);target.symlink_to(relay.c.workspace/'sentinel.txt')
 with pytest.raises(RelayError) as e:relay.poll_once()
 assert e.value.code=='WORKSPACE_DIRTY' and target.is_symlink() and not sock.calls;no_remote_result(bare)
