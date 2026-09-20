"""Local-only GitHub Control command validation, persistence, and scheduling."""
from __future__ import annotations
import json,sqlite3
from datetime import datetime,timezone
from pathlib import Path
from typing import Any,Callable
from gateway.github_control_event import build_worker_control_event
from gateway.worker_session_registry import BindError
ALLOWED={"STATUS","CONTINUE","RECOVER","RECHECK","START_TASK","PAUSE","RESUME","STOP_AFTER_SAFE_POINT"};REQUIRED={"command_id","source","worker_id","workstream","task_id","command_type","instructions"}
class CommandError(ValueError):pass
def now():return datetime.now(timezone.utc).isoformat()
class CommandLedger:
 def __init__(self,path:Path):
  self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
  with self._connect() as db:db.execute("CREATE TABLE IF NOT EXISTS github_control_commands(command_id TEXT PRIMARY KEY,state TEXT NOT NULL,payload TEXT NOT NULL,reason TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
 def _connect(self):return sqlite3.connect(self.path,timeout=5)
 def state(self,id):
  with self._connect() as db:
   r=db.execute('SELECT state FROM github_control_commands WHERE command_id=?',(id,)).fetchone();return r[0] if r else None
 def record(self,p,state,reason=None):
  with self._connect() as db:
   prior=db.execute('SELECT created_at FROM github_control_commands WHERE command_id=?',(p['command_id'],)).fetchone()
   db.execute('INSERT OR REPLACE INTO github_control_commands VALUES(?,?,?,?,?,?)',(p['command_id'],state,json.dumps(p),reason,prior[0] if prior else now(),now()))
class SubmitCommand:
 def __init__(self,ledger,registry,schedule:Callable):self.ledger,self.registry,self.schedule=ledger,registry,schedule
 def submit(self,p:dict[str,Any]):
  if not isinstance(p,dict) or not REQUIRED<=p.keys() or not isinstance(p['instructions'],str) or len(p['instructions'])>8000:return {'status':'INVALID'}
  if p['source']!='github-control' or p['worker_id']!='nvidia-control' or p['workstream']!='CONTROL_PLANE':return {'command_id':p['command_id'],'status':'UNAUTHORIZED'}
  if p['command_type'] not in ALLOWED:return {'command_id':p['command_id'],'status':'INVALID_COMMAND'}
  if self.ledger.state(p['command_id']) in {'ACCEPTED','QUEUED','DELIVERED'}:return {'command_id':p['command_id'],'status':'DUPLICATE'}
  try:event=build_worker_control_event(self.registry,p)
  except BindError as e:return {'command_id':p['command_id'],'status':str(e)}
  self.ledger.record(p,'ACCEPTED')
  try:self.schedule(event,lambda:self.ledger.record(p,'DELIVERED'));self.ledger.record(p,'QUEUED');return {'command_id':p['command_id'],'status':'ACCEPTED','accepted_at':now()}
  except Exception:self.ledger.record(p,'FAILED','SCHEDULING_FAILED');return {'command_id':p['command_id'],'status':'SCHEDULING_FAILED'}
