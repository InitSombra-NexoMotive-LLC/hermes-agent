from gateway.github_control_ledger import CommandLedger
from gateway.github_control_sanitize import sanitize_result
def p():return {'command_id':'x','worker_id':'nvidia-control','workstream':'CONTROL_PLANE'}
def test_monotonic(tmp_path):
 l=CommandLedger(tmp_path/'x.db');l.accept(p());assert l.transition('x','QUEUED',('ACCEPTED',));assert l.transition('x','RUNNING',('QUEUED',));assert not l.transition('x','QUEUED',('RUNNING',));assert l.transition('x','DELIVERED',('RUNNING',));assert l.complete('x',sanitize_result('ok'));assert l.state('x')=='COMPLETED';assert not l.fail('x','X')
def test_migration_repeat(tmp_path):
 l=CommandLedger(tmp_path/'x.db');CommandLedger(tmp_path/'x.db')
