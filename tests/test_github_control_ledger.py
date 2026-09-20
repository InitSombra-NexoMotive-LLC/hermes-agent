from gateway.github_control_ledger import CommandLedger
from gateway.github_control_sanitize import sanitize_result

def payload():
    return {'command_id':'x','worker_id':'nvidia-control','workstream':'CONTROL_PLANE'}

def test_monotonic_and_terminal_immutability(tmp_path):
    ledger=CommandLedger(tmp_path/'x.db'); ledger.accept(payload())
    assert ledger.transition('x','QUEUED',('ACCEPTED',))
    assert ledger.transition('x','RUNNING',('QUEUED',))
    assert not ledger.transition('x','QUEUED',('RUNNING',))
    assert ledger.transition('x','DELIVERED',('RUNNING',))
    assert ledger.complete('x',sanitize_result('ok'))
    assert ledger.state('x')=='COMPLETED'
    assert not ledger.fail('x','EXECUTION_FAILED')

def test_failure_reason_is_safe_and_immutable(tmp_path):
    ledger=CommandLedger(tmp_path/'x.db'); ledger.accept(payload())
    assert ledger.fail('x','EXECUTION_FAILED')
    assert ledger.status('x')['reason']=='EXECUTION_FAILED'
    assert not ledger.fail('x','RESULT_SANITIZATION_FAILED')
    assert ledger.status('x')['reason']=='EXECUTION_FAILED'

def test_unsupported_reason_is_normalized(tmp_path):
    ledger=CommandLedger(tmp_path/'x.db'); ledger.accept(payload())
    assert ledger.fail('x','secret=do-not-persist')
    assert ledger.status('x')['reason']=='EXECUTION_FAILED'
    assert 'secret' not in ledger.status('x')['reason']

def test_migration_repeat(tmp_path):
    CommandLedger(tmp_path/'x.db'); CommandLedger(tmp_path/'x.db')
