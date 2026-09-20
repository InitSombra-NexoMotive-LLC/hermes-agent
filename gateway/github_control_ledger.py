import json,sqlite3
from datetime import datetime,timezone
from pathlib import Path
class CommandLedger:
 def __init__(self,path):
  self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
  with self._db() as d:
   d.execute('CREATE TABLE IF NOT EXISTS github_control_commands(command_id TEXT PRIMARY KEY,state TEXT NOT NULL,payload TEXT NOT NULL,reason TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)')
   cols={x[1] for x in d.execute('PRAGMA table_info(github_control_commands)')}
   for n,t in {'worker_id':'TEXT','workstream':'TEXT','completed_at':'TEXT','sanitized_result':'TEXT','result_digest':'TEXT','result_truncated':'INTEGER NOT NULL DEFAULT 0'}.items():
    if n not in cols:d.execute(f'ALTER TABLE github_control_commands ADD COLUMN {n} {t}')
 def _db(self):return sqlite3.connect(self.path,timeout=5)
 def accept(self,p):
  t=datetime.now(timezone.utc).isoformat()
  with self._db() as d:
   try:
    d.execute('INSERT INTO github_control_commands(command_id,state,payload,reason,created_at,updated_at,worker_id,workstream) VALUES(?,?,?,?,?,?,?,?)',(p['command_id'],'ACCEPTED',json.dumps(p),'',t,t,p.get('worker_id'),p.get('workstream')))
    return True
   except sqlite3.IntegrityError:return False
 def transition(self,id,target,allowed):
  legal={'QUEUED':{'ACCEPTED'},'RUNNING':{'QUEUED'},'DELIVERED':{'RUNNING'},'COMPLETED':{'DELIVERED'},'FAILED':{'ACCEPTED','QUEUED','RUNNING','DELIVERED'}}
  if set(allowed)-legal.get(target,set()):return False
  with self._db() as d:
   q=','.join('?'*len(allowed));r=d.execute(f'UPDATE github_control_commands SET state=?,updated_at=? WHERE command_id=? AND state IN ({q})',(target,datetime.now(timezone.utc).isoformat(),id,*allowed));return r.rowcount==1
 def complete(self,id,result):
  t=datetime.now(timezone.utc).isoformat()
  with self._db() as d:r=d.execute('UPDATE github_control_commands SET state=?,sanitized_result=?,result_digest=?,result_truncated=?,completed_at=?,updated_at=? WHERE command_id=? AND state=?',('COMPLETED',result['summary'],result['digest'],int(result['truncated']),t,t,id,'DELIVERED'));return r.rowcount==1
 def fail(self,id,reason):
  if not isinstance(reason,str) or reason not in {'EXECUTION_FAILED','RESULT_SANITIZATION_FAILED','RESULT_PERSISTENCE_FAILED','INVALID_LIFECYCLE_STATE','SCHEDULING_FAILED'}:reason='EXECUTION_FAILED'
  with self._db() as d:
   q=','.join('?'*4);r=d.execute(f'UPDATE github_control_commands SET state=?,reason=?,updated_at=? WHERE command_id=? AND state IN ({q})',('FAILED',reason,datetime.now(timezone.utc).isoformat(),id,'ACCEPTED','QUEUED','RUNNING','DELIVERED'));return r.rowcount==1
 def status(self,id):
  with self._db() as d:
   r=d.execute('SELECT command_id,worker_id,workstream,state,reason,created_at,updated_at,completed_at,sanitized_result,result_digest,result_truncated FROM github_control_commands WHERE command_id=?',(id,)).fetchone()
  if not r:return None
  return dict(zip(('command_id','worker_id','workstream','state','reason','created_at','updated_at','completed_at','sanitized_result','result_digest','result_truncated'),r))
 def state(self,id):
  s=self.status(id);return s['state'] if s else None
