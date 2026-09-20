import json
import subprocess
import pytest
from gateway.github_control_relay import RelayConfig,StatusRelay,RelayError,SAFE_RESULT

def command(command_id='nvidia-status-20260920-001'): return {'protocol_version':1,'command_id':command_id,'source':'github-control','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'fixture','command_type':'STATUS','instructions':'status'}
def git(*args,cwd=None): return subprocess.run(['git',*args],cwd=cwd,check=True,text=True,stdout=subprocess.PIPE).stdout.strip()
class Socket:
 def request(self,p): return {'status':'OK'}
def _result_relay(tmp_path):
 work=tmp_path/'repo';work.mkdir();git('init',cwd=work);git('-c','user.name=x','-c','user.email=x@y','commit','--allow-empty','-m','base',cwd=work);data=command();c=RelayConfig(remote='unused',branch='b',trusted_head=git('rev-parse','HEAD',cwd=work),workspace=work,state_db=tmp_path/'state.db');relay=StatusRelay(c,Socket(),lambda _: {});result={k:None for k in SAFE_RESULT};result.update({'command_id':data['command_id'],'state':'COMPLETED','sanitized_result':'safe','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','result_truncated':False});relay.save(data['command_id'],'a'*40,'DISCOVERED',data);relay.save(data['command_id'],'a'*40,'SUBMITTED',data);relay.save(data['command_id'],'a'*40,'RESULT_READY',data,result=result);p=work/'control-inbox/results';p.mkdir(parents=True);(p/(data['command_id']+'.json')).write_text(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n');git('add','.',cwd=work);git('-c','user.name=x','-c','user.email=x@y','commit','-m','result',cwd=work);return relay,data,git('rev-parse','HEAD',cwd=work)
def test_matching_result_commit_reconciles_result_ready(tmp_path):
 relay,data,sha=_result_relay(tmp_path);relay.validate_result_commit(sha,relay.changes(sha));assert relay.row(data['command_id'])[1]=='PUBLISHED'
def test_crash_after_push_before_published_state_reconciles(tmp_path):
 relay,data,sha=_result_relay(tmp_path);relay.validate_result_commit(sha,relay.changes(sha));assert relay.row(data['command_id'])[4]==sha
def test_published_result_commit_is_idempotent(tmp_path):
 relay,data,sha=_result_relay(tmp_path);relay.validate_result_commit(sha,relay.changes(sha));relay.validate_result_commit(sha,relay.changes(sha));assert relay.row(data['command_id'])[1]=='PUBLISHED'
def test_unknown_result_commit_is_rejected(tmp_path):
 relay,data,sha=_result_relay(tmp_path)
 with pytest.raises(RelayError):relay.validate_result_commit(sha,[['A','control-inbox/results/unknown.json']])
def test_mismatched_result_content_is_rejected(tmp_path):
 relay,data,sha=_result_relay(tmp_path);path=relay.c.workspace/'control-inbox/results'/(data['command_id']+'.json');path.write_text('{}\n');git('add','.',cwd=relay.c.workspace);git('-c','user.name=x','-c','user.email=x@y','commit','-m','bad',cwd=relay.c.workspace);bad=git('rev-parse','HEAD',cwd=relay.c.workspace)
 with pytest.raises(RelayError):relay.validate_result_commit(bad,relay.changes(bad))
def test_result_commit_with_additional_file_is_rejected(tmp_path):
 relay,data,sha=_result_relay(tmp_path);(relay.c.workspace/'extra').write_text('x');git('add','.',cwd=relay.c.workspace);git('-c','user.name=x','-c','user.email=x@y','commit','-m','extra',cwd=relay.c.workspace);bad=git('rev-parse','HEAD',cwd=relay.c.workspace)
 with pytest.raises(RelayError):relay.validate_result_commit(bad,relay.changes(bad))
def test_result_commit_wrong_mode_is_rejected(tmp_path):
 relay,data,sha=_result_relay(tmp_path);path=relay.c.workspace/'control-inbox/results'/(data['command_id']+'.json');path.unlink();path.symlink_to('../x');git('add','-A',cwd=relay.c.workspace);git('-c','user.name=x','-c','user.email=x@y','commit','-m','link',cwd=relay.c.workspace);bad=git('rev-parse','HEAD',cwd=relay.c.workspace)
 with pytest.raises(RelayError):relay.validate_result_commit(bad,relay.changes(bad))


def test_rewritten_history_rejected_before_command_resume(tmp_path,monkeypatch):
 relay,data,sha=_result_relay(tmp_path);relay.save(data['command_id'],'a'*40,'PUBLISHED',data,published=sha)
 pending=command('pending');relay.save('pending','b'*40,'DISCOVERED',pending);relay.save('pending','b'*40,'SUBMITTED',pending)
 calls=[];relay.socket.request=lambda payload:calls.append(payload) or {'status':'OK'};monkeypatch.setattr(relay,'prepare',lambda:'new');monkeypatch.setattr(relay,'ancestor',lambda a,b:False)
 with pytest.raises(RelayError):relay.poll_once()
 assert calls==[] and relay.row('pending')[1]=='SUBMITTED'
def test_poll_resumes_pending_after_transport_validation(tmp_path,monkeypatch):
 relay,data,sha=_result_relay(tmp_path);relay.save(data['command_id'],'a'*40,'PUBLISHED',data,published=sha)
 pending=command('pending');relay.save('pending','b'*40,'DISCOVERED',pending);relay.save('pending','b'*40,'SUBMITTED',pending)
 trace=[];monkeypatch.setattr(relay,'prepare',lambda:trace.append('fetch') or sha);monkeypatch.setattr(relay,'ancestor',lambda a,b:trace.append('ancestry') or True);monkeypatch.setattr(relay,'git',lambda *args: '' if args[0]=='rev-list' else sha)
 relay.socket.request=lambda payload:trace.append(payload['verb']) or {'status':'OK','state':'RUNNING'}
 assert relay.poll_once()==[] and trace.index('ancestry')<trace.index('command-status') and relay.row('pending')[1]=='SUBMITTED'
