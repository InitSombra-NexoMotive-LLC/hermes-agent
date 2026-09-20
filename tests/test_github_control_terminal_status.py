import hashlib,pytest
from gateway.github_control_relay import StatusRelay,RelayConfig,RelayError

def relay(tmp_path,status):
 r=StatusRelay(RelayConfig(remote='x',branch='x',trusted_head='a'*40,workspace=tmp_path/'w',state_db=tmp_path/'s'),None,lambda _:{});r.socket=type('S',(),{'request':lambda s,p:status})();return r
def good(**x):
 text='safe';d={'status':'OK','command_id':'c','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','state':'COMPLETED','sanitized_result':text,'result_digest':hashlib.sha256(text.encode()).hexdigest(),'result_truncated':0,'created_at':'t','updated_at':'t','completed_at':'t','reason':None};d.update(x);return d
def bad(tmp_path,**x):
 with pytest.raises(RelayError) as e:relay(tmp_path,good(**x)).result('c')
 assert e.value.code=='STATUS_INVALID_RESPONSE'
def test_mismatched_terminal_status_identity_rejected(tmp_path):bad(tmp_path,command_id='no')
def test_mismatched_nonterminal_status_identity_rejected(tmp_path):bad(tmp_path,state='RUNNING',command_id='no')
def test_result_truncated_outside_zero_one_rejected(tmp_path):bad(tmp_path,result_truncated=2)
def test_completed_status_requires_sanitized_result_and_digest(tmp_path):bad(tmp_path,sanitized_result=None)
def test_completed_status_digest_must_match_summary(tmp_path):bad(tmp_path,result_digest='0'*64)
def test_failed_status_allows_only_safe_reason(tmp_path):bad(tmp_path,state='FAILED',reason='raw error',sanitized_result=None,result_digest=None,completed_at=None)
def test_valid_failed_status_accepts_null_result_fields(tmp_path):
 assert relay(tmp_path,good(state='FAILED',reason='EXECUTION_FAILED',sanitized_result=None,result_digest=None,completed_at=None)).result('c')['reason']=='EXECUTION_FAILED'
def test_terminal_status_extra_fields_not_published(tmp_path):
 got=relay(tmp_path,good(local_secret='no')).result('c');assert 'local_secret' not in got and set(got)<=set(__import__('gateway.github_control_relay',fromlist=['SAFE_RESULT']).SAFE_RESULT)
