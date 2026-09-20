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
from gateway.github_control_ledger import CommandLedger
class SubmitCommand:
 def __init__(self,ledger,registry,schedule:Callable):self.ledger,self.registry,self.schedule=ledger,registry,schedule
 def submit(self,p:dict[str,Any]):
  if not isinstance(p,dict) or not REQUIRED<=p.keys() or not isinstance(p['instructions'],str) or len(p['instructions'])>8000:return {'status':'INVALID'}
  if p['source']!='github-control' or p['worker_id']!='nvidia-control' or p['workstream']!='CONTROL_PLANE':return {'command_id':p['command_id'],'status':'UNAUTHORIZED'}
  if p['command_type'] not in ALLOWED:return {'command_id':p['command_id'],'status':'INVALID_COMMAND'}
  if self.ledger.state(p['command_id']) in {'ACCEPTED','QUEUED','RUNNING','DELIVERED','COMPLETED','FAILED'}:return {'command_id':p['command_id'],'status':'DUPLICATE'}
  try:event=build_worker_control_event(self.registry,p)
  except BindError as e:return {'command_id':p['command_id'],'status':str(e)}
  self.ledger.accept(p)
  try:
   if not self.ledger.transition(p['command_id'],'QUEUED',('ACCEPTED',)):raise RuntimeError()
   self.schedule(event,p,self.ledger);return {'command_id':p['command_id'],'status':'ACCEPTED','accepted_at':now()}
  except Exception:self.ledger.fail(p['command_id'],'SCHEDULING_FAILED');return {'command_id':p['command_id'],'status':'SCHEDULING_FAILED'}
