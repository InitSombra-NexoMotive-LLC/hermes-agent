import json,subprocess,pytest
from gateway.github_control_relay import RelayConfig,StatusRelay,RelayError

def command(command_id='expected',**overrides):
 d={'protocol_version':1,'command_id':command_id,'source':'github-control','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'x','command_type':'STATUS','instructions':'status'};d.update(overrides);return d
def git(*args,cwd=None):return subprocess.run(['git',*args],cwd=cwd,check=True,text=True,stdout=subprocess.PIPE).stdout.strip()
def relay_repo(tmp_path,path,data):
 bare=tmp_path/'remote.git';git('init','--bare',str(bare));work=tmp_path/'work';git('clone',str(bare),str(work));git('checkout','-b','control/nvidia-command-inbox',cwd=work);p=work/path;p.parent.mkdir(parents=True);p.write_text(json.dumps(data));git('add','.',cwd=work);git('-c','user.name=x','-c','user.email=x@y','commit','-m','command',cwd=work);head=git('rev-parse','HEAD',cwd=work);git('push','origin','HEAD:control/nvidia-command-inbox',cwd=work);relay_work=tmp_path/'relay';git('clone',str(bare),str(relay_work));git('checkout','control/nvidia-command-inbox',cwd=relay_work);c=RelayConfig(remote=str(bare),branch='control/nvidia-command-inbox',trusted_head=head,workspace=relay_work,state_db=tmp_path/'state.db');return StatusRelay(c,type('S',(),{'request':lambda self,p:{'status':'ACCEPTED'} if p['verb']=='submit-command' else {'status':'OK','state':'RUNNING','command_id':p['command_id'],'worker_id':'nvidia-control','workstream':'CONTROL_PLANE','created_at':'t','updated_at':'t','result_truncated':False}})(),lambda _:{'login':'InitSombra-NexoMotive-LLC','id':239685310}),head
def test_command_filename_must_match_command_id(tmp_path):
 r,h=relay_repo(tmp_path,'control-inbox/commands/different.json',command())
 with pytest.raises(RelayError) as e:r.poll_once()
 assert e.value.code=='INVALID_COMMAND_COMMIT'
def test_command_file_requires_json_extension(tmp_path):
 r,h=relay_repo(tmp_path,'control-inbox/commands/expected',command())
 with pytest.raises(RelayError) as e:r.poll_once()
 assert e.value.code=='INVALID_COMMAND_COMMIT'
def test_duplicate_id_different_commit_freezes(tmp_path):
 r,h=relay_repo(tmp_path,'control-inbox/commands/expected.json',command());r.save('expected',h,'DISCOVERED',command());r.save('expected',h,'SUBMITTED',command())
 # A differing source is an immutable duplicate conflict.
 with pytest.raises(RelayError) as e:r.resume('expected','b'*40,command())
 assert e.value.code=='DUPLICATE_COMMAND' and r.row('expected')[1]=='SUBMITTED'
def test_same_source_commit_resumes_idempotently(tmp_path):
 r,h=relay_repo(tmp_path,'control-inbox/commands/expected.json',command());r.save('expected',h,'DISCOVERED',command());assert r.resume('expected',h,command()) is False and r.row('expected')[1]=='SUBMITTED'
def test_same_source_commit_different_payload_rejected(tmp_path):
 r,h=relay_repo(tmp_path,'control-inbox/commands/expected.json',command());r.save('expected',h,'DISCOVERED',command(instructions='old'))
 with pytest.raises(RelayError) as e:r.poll_once()
 assert e.value.code=='DUPLICATE_COMMAND' and json.loads(r.row('expected')[2])['instructions']=='old'
def test_protocol_boolean_true_rejected():
 with pytest.raises(RelayError) as e:StatusRelay.validate_command(command(protocol_version=True))
 assert e.value.code=='INVALID_COMMAND'
def test_instructions_over_8000_rejected():
 assert StatusRelay.validate_command(command(instructions='x'*8000))
 with pytest.raises(RelayError) as e:StatusRelay.validate_command(command(instructions='x'*8001))
 assert e.value.code=='INVALID_COMMAND'
