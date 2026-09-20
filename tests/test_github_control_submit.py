from gateway.github_control import CommandLedger,SubmitCommand
from gateway.worker_session_registry import WorkerSessionRegistry
def payload(**x):
 p={'command_id':'cmd','source':'github-control','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'CP-7','command_type':'STATUS','instructions':'status'};p.update(x);return p
def ready(tmp_path):
 r=WorkerSessionRegistry(tmp_path/'r.db');c=r.start('nvidia-control','CONTROL_PLANE','chat');r.consume('nvidia-control',c,'chat','fixture-session-nvidia');return CommandLedger(tmp_path/'l.db'),r
def test_submit_states_duplicate_and_transport(tmp_path):
 l,r=ready(tmp_path);seen=[]
 def schedule(e,command,ledger):seen.append(e);ledger.record(command,'DELIVERED')
 s=SubmitCommand(l,r,schedule);assert s.submit(payload())['status']=='ACCEPTED' and l.state('cmd')=='QUEUED';assert len(seen)==1 and seen[0].source.chat_id=='github-control';assert s.submit(payload())['status']=='DUPLICATE'
def test_missing_mapping_validation_and_schedule_failure(tmp_path):
 l=CommandLedger(tmp_path/'l.db');r=WorkerSessionRegistry(tmp_path/'r.db');s=SubmitCommand(l,r,lambda *_:None);assert s.submit(payload())['status']=='SESSION_NOT_CONFIGURED';assert s.submit(payload(worker_id='gcp-stratega'))['status']=='UNAUTHORIZED';assert s.submit(payload(command_type='SHELL'))['status']=='INVALID_COMMAND'
 l,r=ready(tmp_path);s=SubmitCommand(l,r,lambda *_:(_ for _ in ()).throw(RuntimeError()));assert s.submit(payload(command_id='bad'))['status']=='SCHEDULING_FAILED' and l.state('bad')=='FAILED'
