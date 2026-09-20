import json,os,pytest
from gateway.github_control_relay import *
def cfg(tmp_path,**x):
 d={'remote':APPROVED_REMOTE,'branch':'control/nvidia-command-inbox','trusted_head':'a'*40,'workspace':'/mnt/ssd-cloud/hermes-github-control-transport','state_db':'/mnt/ssd-cloud/hermes-github-control-state/relay.db','gateway_socket':'/home/rodrigo/.hermes/gateway.sock','timeout':30,'retries':3,'poll_seconds':30};d.update(x);p=tmp_path/'c';p.write_text(json.dumps(d));p.chmod(0o600);return p
def test_valid_mode_0600_config_loads(tmp_path):assert load_config(cfg(tmp_path))['remote']==APPROVED_REMOTE
def test_config_symlink_rejected(tmp_path):
 p=cfg(tmp_path);q=tmp_path/'q';q.symlink_to(p)
 with pytest.raises(RelayError): load_config(q)
def test_config_wrong_mode_rejected(tmp_path):
 p=cfg(tmp_path);p.chmod(0o644)
 with pytest.raises(RelayError): load_config(p)
def test_config_unknown_key_rejected(tmp_path):
 with pytest.raises(RelayError):load_config(cfg(tmp_path,x=1))
def test_config_wrong_branch_or_remote_rejected(tmp_path):
 for x in ({'branch':'x'},{'remote':'x'}):
  with pytest.raises(RelayError):load_config(cfg(tmp_path,**x))
class R:
 def __init__(self,e=None):self.e=e
 def poll_once(self):
  if self.e:raise self.e
def code(e):
 with pytest.raises(SystemExit) as x:run_service_iteration(R(e))
 return x.value.code
def test_permanent_error_exits_73():assert code(RelayError('INVALID_COMMAND'))==73
def test_transient_error_keeps_service_alive():assert run_service_iteration(R(RelayError('REMOTE_ADVANCED'))) is None
def test_unknown_relay_error_exits_nonzero():assert code(RelayError('OTHER'))==1
def test_unexpected_exception_exits_nonzero():assert code(ValueError())==1
def test_logs_contain_only_safe_outcome_codes(caplog):run_service_iteration(R(RelayError('REMOTE_ADVANCED')));assert 'REMOTE_ADVANCED' in caplog.text and 'Traceback' not in caplog.text
def test_systemd_prevents_restart_for_exit_73():assert 'RestartPreventExitStatus=73' in open('systemd/hermes-github-control-relay.service').read()
