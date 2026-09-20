import pytest
from gateway.github_control_relay import RelayConfig,StatusRelay,RelayError,PERMANENT

def setup(tmp_path):
 r=StatusRelay(RelayConfig(remote='x',branch='b',trusted_head='a'*40,workspace=tmp_path/'w',state_db=tmp_path/'s.db'),None,lambda _:{});d={'command_id':'c','x':1};return r,d,'a'*40,{'safe':'result'}
def test_direct_terminal_insert_rejected(tmp_path):
 r,d,c,result=setup(tmp_path)
 for state in ('SUBMITTED','RESULT_READY','PUBLISHED','FAILED'):
  with pytest.raises(RelayError):r.save('c',c,state,d,result=result,published='p',reason='EXECUTION_FAILED')
 assert r.row('c') is None
def test_legal_lifecycle_path_accepts(tmp_path):
 r,d,c,result=setup(tmp_path);r.save('c',c,'DISCOVERED',d);r.save('c',c,'SUBMITTED',d);r.save('c',c,'RESULT_READY',d,result=result);r.save('c',c,'PUBLISHED',d,result=result,published='p');assert r.row('c')[1]=='PUBLISHED'
def test_lifecycle_regression_rejected(tmp_path):
 r,d,c,result=setup(tmp_path);r.save('c',c,'DISCOVERED',d);r.save('c',c,'SUBMITTED',d)
 with pytest.raises(RelayError):r.save('c',c,'DISCOVERED',d)
def test_published_terminal_fields_immutable(tmp_path):
 r,d,c,result=setup(tmp_path);r.save('c',c,'DISCOVERED',d);r.save('c',c,'SUBMITTED',d);r.save('c',c,'RESULT_READY',d,result=result);r.save('c',c,'PUBLISHED',d,result=result,published='p')
 with pytest.raises(RelayError):r.save('c',c,'PUBLISHED',d,result={'other':1},published='p')
def test_failed_terminal_reason_immutable(tmp_path):
 r,d,c,result=setup(tmp_path);r.save('c',c,'DISCOVERED',d);r.save('c',c,'FAILED',d,reason='EXECUTION_FAILED')
 with pytest.raises(RelayError):r.save('c',c,'FAILED',d,reason='SCHEDULING_FAILED')
 assert 'INVALID_LIFECYCLE_STATE' in PERMANENT
