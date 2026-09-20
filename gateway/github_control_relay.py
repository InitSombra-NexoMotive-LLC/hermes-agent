"""Fail-closed STATUS-only Git transport relay; never executes command text."""
from __future__ import annotations
import json, os, socket, sqlite3, subprocess, time
from dataclasses import dataclass
from pathlib import Path

FIELDS={"protocol_version","command_id","source","worker_id","workstream","task_id","command_type","instructions"}
SAFE_RESULT={"command_id","worker_id","workstream","state","reason","created_at","updated_at","completed_at","sanitized_result","result_digest","result_truncated"}

@dataclass
class RelayConfig:
 remote:str; branch:str; trusted_head:str; workspace:Path; state_db:Path
 timeout:float=30; retries:int=3

class UnixSocketClient:
 def __init__(self,path): self.path=path
 def request(self,payload):
  with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
   s.settimeout(30); s.connect(self.path); s.sendall(json.dumps(payload).encode()+b'\n')
   return json.loads(s.makefile('rb').readline())['result']

class StatusRelay:
 def __init__(self,config,socket_client,actor_lookup):
  self.c=config; self.socket=socket_client; self.actor_lookup=actor_lookup; self.c.workspace=Path(config.workspace); self.c.state_db=Path(config.state_db); self.c.state_db.parent.mkdir(parents=True,exist_ok=True); self._db()
 def _db(self):
  d=sqlite3.connect(self.c.state_db); d.execute('CREATE TABLE IF NOT EXISTS relay_commands(command_id TEXT PRIMARY KEY, source_commit TEXT NOT NULL, lifecycle TEXT NOT NULL, published_commit TEXT)'); d.execute('CREATE TABLE IF NOT EXISTS relay_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)'); d.commit(); return d
 def _git(self,*args): return subprocess.run(['git','-C',str(self.c.workspace),*args],check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=self.c.timeout).stdout.strip()
 def _prepare(self):
  if not self.c.workspace.exists():
   subprocess.run(['git','clone','--no-checkout',self.c.remote,str(self.c.workspace)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=self.c.timeout)
  self._git('fetch','--prune','origin',self.c.branch)
  self._git('checkout','-B',self.c.branch,'FETCH_HEAD')
  return self._git('rev-parse','HEAD')
 def _ancestor(self,a,b): return subprocess.run(['git','-C',str(self.c.workspace),'merge-base','--is-ancestor',a,b],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
 @staticmethod
 def validate_command(data):
  if not isinstance(data,dict) or set(data)!=FIELDS: raise ValueError('INVALID_COMMAND')
  if not all(isinstance(data[k],str) for k in FIELDS-{'protocol_version'}) or data['protocol_version']!=1: raise ValueError('INVALID_COMMAND')
  if data['source']!='github-control' or data['worker_id']!='nvidia-control' or data['workstream']!='CONTROL_PLANE' or data['command_type']!='STATUS': raise ValueError('INVALID_COMMAND')
  if not data['command_id'] or len(data['command_id'])>128 or any(c not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-' for c in data['command_id']): raise ValueError('INVALID_COMMAND')
  return data
 def _changes(self,commit): return [line.split('\t',1) for line in self._git('diff-tree','--root','--no-commit-id','--name-status','-r',commit).splitlines()]
 def _verify_actor(self,commit):
  actor=self.actor_lookup(commit)
  if not isinstance(actor,dict) or actor.get('login')!='InitSombra-NexoMotive-LLC' or actor.get('id')!=239685310: raise RuntimeError('ACTOR_VERIFICATION_FAILED')
 def _result(self,command_id):
  for _ in range(self.c.retries):
   status=self.socket.request({'verb':'command-status','command_id':command_id})
   if status.get('status')=='OK' and status.get('state') in {'COMPLETED','FAILED'}: return {k:status.get(k) for k in SAFE_RESULT}
   time.sleep(min(1,self.c.timeout))
  raise RuntimeError('STATUS_PENDING')
 def _publish(self,command_id,result):
  target=self.c.workspace/'control-inbox/results'/f'{command_id}.json'
  if target.exists(): raise RuntimeError('RESULT_EXISTS')
  target.parent.mkdir(parents=True,exist_ok=True); target.write_text(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n')
  self._git('add','--',str(target.relative_to(self.c.workspace))); self._git('-c','user.name=Hermes GitHub Control Relay','-c','user.email=relay@localhost','commit','-m',f'control(nvidia): publish sanitized result {command_id}')
  for _ in range(self.c.retries):
   try: self._git('push','origin',f'HEAD:{self.c.branch}'); return self._git('rev-parse','HEAD')
   except subprocess.CalledProcessError:
    self._git('fetch','origin',self.c.branch)
    if not self._ancestor('HEAD','FETCH_HEAD'): raise RuntimeError('TRANSPORT_HISTORY_CHANGED')
    self._git('rebase','FETCH_HEAD')
  raise RuntimeError('PUSH_FAILED')
 def poll_once(self):
  head=self._prepare(); db=self._db(); row=db.execute("SELECT value FROM relay_meta WHERE key='head'").fetchone(); base=row[0] if row else self.c.trusted_head
  if not self._ancestor(base,head): raise RuntimeError('TRANSPORT_HISTORY_CHANGED')
  if not self._ancestor(self.c.trusted_head,head): raise RuntimeError('UNTRUSTED_TRANSPORT_HEAD')
  start=f'{base}^' if not row else base
  commits=([head] if not row and base==head else self._git('rev-list','--reverse',f'{start}..{head}').splitlines()); completed=[]
  for commit in filter(None,commits):
   changes=self._changes(commit); command_changes=[p for s,p in changes if p.startswith('control-inbox/commands/')]
   if not command_changes:
    if not all(s=='A' and p.startswith('control-inbox/results/') for s,p in changes): raise RuntimeError('UNVERIFIED_TRANSPORT_CHANGE')
    continue
   if len(changes)!=1 or len(command_changes)!=1 or changes[0][0]!='A' or '/' in command_changes[0].removeprefix('control-inbox/commands/'): raise RuntimeError('INVALID_COMMAND_COMMIT')
   if not self._git('ls-tree',commit,'--',command_changes[0]).startswith('100644 '): raise RuntimeError('INVALID_COMMAND_COMMIT')
   self._verify_actor(commit); raw=self._git('show',f'{commit}:{command_changes[0]}')
   if len(raw.encode())>16384: raise RuntimeError('INVALID_COMMAND')
   try: data=self.validate_command(json.loads(raw))
   except Exception: raise RuntimeError('INVALID_COMMAND') from None
   existing=db.execute('SELECT lifecycle FROM relay_commands WHERE command_id=?',(data['command_id'],)).fetchone()
   if existing: raise RuntimeError('DUPLICATE_COMMAND')
   db.execute('INSERT INTO relay_commands VALUES(?,?,?,NULL)',(data['command_id'],commit,'DISCOVERED')); db.commit()
   reply=self.socket.request({'verb':'submit-command',**data})
   if reply.get('status') not in {'ACCEPTED','DUPLICATE'}: raise RuntimeError('SUBMIT_FAILED')
   db.execute('UPDATE relay_commands SET lifecycle=? WHERE command_id=?',('SUBMITTED',data['command_id'])); db.commit()
   result=self._result(data['command_id']); published=self._publish(data['command_id'],result)
   db.execute('UPDATE relay_commands SET lifecycle=?,published_commit=? WHERE command_id=?',('PUBLISHED',published,data['command_id'])); db.commit(); completed.append(data['command_id'])
  db.execute("INSERT OR REPLACE INTO relay_meta VALUES('head',?)",(self._git('rev-parse','HEAD'),)); db.commit(); return completed

def github_actor_lookup(commit):
 raw=subprocess.run(['gh','api',f'repos/InitSombra-NexoMotive-LLC/hermes-agent/commits/{commit}'],check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=30).stdout
 data=json.loads(raw); return data.get('author') or {}

def main():
 config_path=Path(os.environ['HERMES_GITHUB_CONTROL_RELAY_CONFIG'])
 config=json.loads(config_path.read_text())
 relay=StatusRelay(RelayConfig(**{k:config[k] for k in ('remote','branch','trusted_head','workspace','state_db','timeout','retries') if k in config}),UnixSocketClient(config['gateway_socket']),github_actor_lookup)
 while True:
  try: relay.poll_once()
  except Exception: pass
  time.sleep(min(max(float(config.get('poll_seconds',30)),5),300))
if __name__=='__main__': main()