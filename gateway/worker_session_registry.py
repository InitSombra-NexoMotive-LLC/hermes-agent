"""One-time local binding from a trusted Telegram event to a worker session."""
import json,secrets,sqlite3
from datetime import datetime,timezone,timedelta
from pathlib import Path
class BindError(ValueError):pass
class WorkerSessionRegistry:
 def __init__(self,path:Path):
  path.parent.mkdir(parents=True,exist_ok=True);self.db=sqlite3.connect(path)
  self.db.executescript('CREATE TABLE IF NOT EXISTS worker_sessions(worker_id TEXT PRIMARY KEY,workstream TEXT,session_key TEXT,bound_at TEXT,bound_via TEXT,verified INT);CREATE TABLE IF NOT EXISTS bind_challenges(worker_id TEXT PRIMARY KEY,challenge TEXT,expires_at TEXT,telegram_chat_id TEXT,used INT DEFAULT 0)');self.db.commit()
 def start(self,worker_id,workstream,telegram_chat_id,ttl=600):
  if self.db.execute('SELECT 1 FROM worker_sessions WHERE worker_id=?',(worker_id,)).fetchone():raise BindError('REBIND_REQUIRED')
  # Telegram adapters may yield numeric IDs while local admin tooling stores
  # strings. Canonicalize solely by representation; never coerce an identity.
  identity=str(telegram_chat_id)
  c='NVIDIA-CONTROL-BIND-'+secrets.token_urlsafe(24); exp=datetime.now(timezone.utc)+timedelta(seconds=ttl);self.db.execute('INSERT OR REPLACE INTO bind_challenges VALUES(?,?,?,?,0)',(worker_id,c,exp.isoformat(),identity));self.db.commit();return c
 def consume(self,worker_id,text,telegram_chat_id,session_key):
  r=self.db.execute('SELECT challenge,expires_at,telegram_chat_id,used FROM bind_challenges WHERE worker_id=?',(worker_id,)).fetchone()
  if not r or r[3] or text!=r[0] or str(telegram_chat_id)!=str(r[2]) or datetime.fromisoformat(r[1])<datetime.now(timezone.utc):raise BindError('BIND_REJECTED')
  self.db.execute('INSERT INTO worker_sessions VALUES(?,?,?,?,?,1)',(worker_id,'CONTROL_PLANE',session_key,datetime.now(timezone.utc).isoformat(),'telegram-bootstrap'));self.db.execute('UPDATE bind_challenges SET used=1 WHERE worker_id=?',(worker_id,));self.db.commit()
 def try_consume(self,worker_id,text,telegram_chat_id,session_key):
  try:self.consume(worker_id,text,telegram_chat_id,session_key);return True
  except BindError:return False
 def resolve(self,worker_id):
  r=self.db.execute('SELECT session_key FROM worker_sessions WHERE worker_id=? AND verified=1',(worker_id,)).fetchone()
  if not r:raise BindError('SESSION_NOT_CONFIGURED')
  return r[0]
