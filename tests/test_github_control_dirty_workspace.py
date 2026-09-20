import json,subprocess,pytest
from gateway.github_control_relay import *
def git(*a,cwd):return subprocess.run(['git',*a],cwd=cwd,check=True,stdout=subprocess.PIPE,text=True).stdout.strip()
def setup(tmp_path):
 w=tmp_path/'w';w.mkdir();git('init',cwd=w);git('config','user.email','x@y',cwd=w);git('config','user.name','x',cwd=w);(w/'x').write_text('x');git('add','.',cwd=w);git('commit','-m','x',cwd=w);git('remote','add','origin','x',cwd=w);r=StatusRelay(RelayConfig(remote='x',branch='b',trusted_head='a'*40,workspace=w,state_db=tmp_path/'s'),None,lambda _:{});d={'command_id':'c'};r.save('c','a'*40,'DISCOVERED',d);r.save('c','a'*40,'SUBMITTED',d);res={'command_id':'c'};r.save('c','a'*40,'RESULT_READY',d,result=res);p=w/'control-inbox/results/c.json';p.parent.mkdir(parents=True);return r,p,res
def test_crash_after_result_write_before_add_recovers(tmp_path):
 r,p,res=setup(tmp_path);p.write_text(json.dumps(res,separators=(',',':'))+'\n');r.recover_dirty()
def test_crash_after_result_add_before_commit_recovers(tmp_path):
 r,p,res=setup(tmp_path);p.write_text(json.dumps(res,separators=(',',':'))+'\n');git('add','.',cwd=r.c.workspace);r.recover_dirty()
def test_crash_after_local_commit_before_push_recovers(tmp_path):
 r,p,res=setup(tmp_path);p.write_text(json.dumps(res,separators=(',',':'))+'\n');git('add','.',cwd=r.c.workspace);git('commit','-m','result',cwd=r.c.workspace);r.recover_dirty()
def test_unexpected_dirty_workspace_rejected(tmp_path):
 r,p,res=setup(tmp_path);(r.c.workspace/'bad').write_text('x')
 with pytest.raises(RelayError) as e:r.recover_dirty()
 assert e.value.code=='WORKSPACE_DIRTY'
def test_mismatched_staged_result_rejected(tmp_path):
 r,p,res=setup(tmp_path);p.write_text('{}');git('add','.',cwd=r.c.workspace)
 with pytest.raises(RelayError):r.recover_dirty()
def test_symlink_result_rejected(tmp_path):
 r,p,res=setup(tmp_path);p.symlink_to(r.c.workspace/'x')
 with pytest.raises(RelayError):r.recover_dirty()
