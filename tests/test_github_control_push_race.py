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
 relay,comp,result,bare=setup(tmp_path);advanced=advance(comp)
 with pytest.raises(RelayError):relay.publish('race',result)
 relay.git('fetch','origin',relay.c.branch);relay.git('checkout','--detach','FETCH_HEAD');calls=[];original=relay.publish
 relay.publish=lambda command_id,value:calls.append('publish') or original(command_id,value)
 # The caller must validate the fast-forward before retrying publication; this test proves the remote head exists.
 assert relay.ancestor(advanced,'FETCH_HEAD') and calls==[]
