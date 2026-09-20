import json, subprocess
import pytest
from gateway.github_control_relay import RelayConfig, RelayError, StatusRelay, SAFE_RESULT

def git(*args,cwd=None): return subprocess.run(['git',*args],cwd=cwd,check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout.strip()
def setup(tmp_path):
 bare=tmp_path/'remote.git';git('init','--bare',str(bare));seed=tmp_path/'seed';git('clone',str(bare),str(seed));git('checkout','-b','control/nvidia-command-inbox',cwd=seed);git('-c','user.name=x','-c','user.email=x@y','commit','--allow-empty','-m','base',cwd=seed);git('push','origin','HEAD:control/nvidia-command-inbox',cwd=seed);relay_work=tmp_path/'relay';git('clone',str(bare),str(relay_work));git('checkout','control/nvidia-command-inbox',cwd=relay_work);competitor=tmp_path/'competitor';git('clone',str(bare),str(competitor));git('checkout','control/nvidia-command-inbox',cwd=competitor)
 c=RelayConfig(remote=str(bare),branch='control/nvidia-command-inbox',trusted_head=git('rev-parse','HEAD',cwd=relay_work),workspace=relay_work,state_db=tmp_path/'state.db');relay=StatusRelay(c,None,lambda _: {})
 data={'protocol_version':1,'command_id':'race','source':'github-control','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'x','command_type':'STATUS','instructions':'status'};result={k:None for k in SAFE_RESULT};result.update({'command_id':'race','state':'COMPLETED','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','sanitized_result':'safe','result_truncated':False});relay.save('race','a'*40,'DISCOVERED',data);relay.save('race','a'*40,'SUBMITTED',data);relay.save('race','a'*40,'RESULT_READY',data,result=result)
 return relay,competitor,result,bare
def advance(comp):
 (comp/'marker').write_text('valid');git('add','.',cwd=comp);git('-c','user.name=c','-c','user.email=c@x','commit','-m','advance',cwd=comp);git('push','origin','HEAD:control/nvidia-command-inbox',cwd=comp);return git('rev-parse','HEAD',cwd=comp)
def test_push_rejection_preserves_result_ready(tmp_path):
 relay,comp,result,bare=setup(tmp_path);advance(comp)
 with pytest.raises(RelayError) as raised:relay.publish('race',result)
 assert raised.value.code=='REMOTE_ADVANCED' and relay.row('race')[1]=='RESULT_READY' and json.loads(relay.row('race')[3])==result
def test_remote_fast_forward_returns_remote_advanced(tmp_path):
 relay,comp,result,bare=setup(tmp_path);parent=git('rev-parse','HEAD',cwd=relay.c.workspace);advanced=advance(comp)
 with pytest.raises(RelayError) as raised:relay.publish('race',result)
 assert raised.value.code=='REMOTE_ADVANCED' and relay.ancestor(parent,advanced)
 git('fetch','origin',cwd=comp)
 with pytest.raises(subprocess.CalledProcessError):git('show','HEAD:control-inbox/results/race.json',cwd=comp)
def test_next_poll_validates_fast_forward_before_publish(tmp_path):
 relay,comp,result,bare=setup(tmp_path)
 later={'protocol_version':1,'command_id':'later','source':'github-control','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'x','command_type':'STATUS','instructions':'status'}
 path=comp/'control-inbox/commands';path.mkdir(parents=True);(path/'later.json').write_text(json.dumps(later));git('add','.',cwd=comp);git('-c','user.name=c','-c','user.email=c@x','commit','-m','later',cwd=comp);later_sha=git('rev-parse','HEAD',cwd=comp);git('push','origin','HEAD:control/nvidia-command-inbox',cwd=comp)
 with pytest.raises(RelayError) as raised:relay.publish('race',result)
 assert raised.value.code=='REMOTE_ADVANCED' and relay.row('race')[1]=='RESULT_READY'
 trace=[];relay.actor_lookup=lambda sha:trace.append('actor') or {'login':'InitSombra-NexoMotive-LLC','id':239685310}
 class Sock:
  def request(self,payload):
   trace.append(payload['verb']+':'+payload['command_id'])
   if payload['verb']=='submit-command':return {'status':'ACCEPTED'}
   return {'status':'OK','state':'RUNNING'}
 relay.socket=Sock();original=relay.publish
 relay.publish=lambda command_id,value:trace.append('publish') or original(command_id,value)
 assert relay.poll_once()==['race'] and relay.row('race')[1]=='PUBLISHED' and relay.row('later')[1]=='SUBMITTED'
 assert trace.index('actor')<trace.index('publish') and trace.count('publish')==1
 git('fetch','origin',cwd=comp);history=git('log','--format=%H','origin/control/nvidia-command-inbox',cwd=comp)
 assert later_sha in history and git('show','origin/control/nvidia-command-inbox:control-inbox/results/race.json',cwd=comp)
 assert history.count(later_sha)==1

def test_invalid_intervening_commit_prevents_result_push(tmp_path):
 relay,comp,result,bare=setup(tmp_path);(comp/'unauthorized-marker.txt').write_text('no');git('add','.',cwd=comp);git('-c','user.name=c','-c','user.email=c@x','commit','-m','unauthorized',cwd=comp);bad=git('rev-parse','HEAD',cwd=comp);git('push','origin','HEAD:control/nvidia-command-inbox',cwd=comp)
 with pytest.raises(RelayError) as advanced:relay.publish('race',result)
 assert advanced.value.code=='REMOTE_ADVANCED' and relay.row('race')[1]=='RESULT_READY'
 calls=[];relay.socket=type('Socket',(),{'request':lambda self,payload:calls.append(payload)})()
 relay.publish=lambda *args:calls.append('publish')
 with pytest.raises(RelayError) as rejected:relay.poll_once()
 assert rejected.value.code=='UNKNOWN_RESULT_COMMIT' and rejected.value.code in __import__('gateway.github_control_relay',fromlist=['PERMANENT']).PERMANENT
 assert calls==[] and relay.row('race')[1]=='RESULT_READY'
 with relay.db() as d: assert d.execute("SELECT value FROM relay_meta WHERE key='head'").fetchone() is None
 git('fetch','origin',cwd=comp);assert bad in git('log','--format=%H','origin/control/nvidia-command-inbox',cwd=comp)
 with pytest.raises(subprocess.CalledProcessError):git('show','origin/control/nvidia-command-inbox:control-inbox/results/race.json',cwd=comp)

def test_rewritten_remote_during_push_is_rejected(tmp_path):
 relay,comp,result,bare=setup(tmp_path);parent=git('rev-parse','HEAD',cwd=relay.c.workspace);stored=json.loads(relay.row('race')[3])
 git('checkout','--orphan','rewritten',cwd=comp);(comp/'rewritten-marker').write_text('root');git('add','.',cwd=comp);git('-c','user.name=c','-c','user.email=c@x','commit','-m','rewritten',cwd=comp);rewritten=git('rev-parse','HEAD',cwd=comp);git('push','--force','origin','HEAD:control/nvidia-command-inbox',cwd=comp)
 with pytest.raises(RelayError) as raised:relay.publish('race',result)
 assert raised.value.code=='TRANSPORT_HISTORY_CHANGED' and raised.value.code in __import__('gateway.github_control_relay',fromlist=['PERMANENT']).PERMANENT
 assert not relay.ancestor(parent,'FETCH_HEAD') and relay.row('race')[1]=='RESULT_READY' and json.loads(relay.row('race')[3])==stored
 git('fetch','origin',cwd=comp);assert git('rev-parse','origin/control/nvidia-command-inbox',cwd=comp)==rewritten
 with pytest.raises(subprocess.CalledProcessError):git('show','origin/control/nvidia-command-inbox:control-inbox/results/race.json',cwd=comp)


def test_ambiguous_success_reconciles_without_duplicate(tmp_path,monkeypatch):
 relay,comp,result,bare=setup(tmp_path);original=relay.git;pushes=[]
 def ambiguous(*args):
  if args[0]=='push':
   original(*args);pushes.append('pushed');raise RelayError('GIT_UNAVAILABLE')
  return original(*args)
 monkeypatch.setattr(relay,'git',ambiguous)
 with pytest.raises(RelayError) as raised:relay.publish('race',result)
 assert raised.value.code=='REMOTE_ADVANCED' and relay.row('race')[1]=='RESULT_READY' and json.loads(relay.row('race')[3])==result
 git('fetch','origin',cwd=comp);remote_sha=git('rev-parse','origin/control/nvidia-command-inbox',cwd=comp);assert git('show','origin/control/nvidia-command-inbox:control-inbox/results/race.json',cwd=comp)
 monkeypatch.setattr(relay,'git',original);calls=[];relay.publish=lambda *args:calls.append('publish');relay.socket=type('Socket',(),{'request':lambda self,payload:calls.append(payload)})()
 assert relay.poll_once()==[] and relay.row('race')[1]=='PUBLISHED' and relay.row('race')[4]==remote_sha and calls==[]
 history=git('log','origin/control/nvidia-command-inbox','--format=%H','--','control-inbox/results/race.json',cwd=comp);assert len(history.splitlines())==1
 assert relay.poll_once()==[] and calls==[]
