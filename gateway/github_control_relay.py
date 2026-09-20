"""Fail-closed STATUS-only GitHub control relay; command text is never executed."""
from __future__ import annotations
import hashlib, json, logging, os, re, socket, sqlite3, subprocess, time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

LOG=logging.getLogger(__name__)
FIELDS={'protocol_version','command_id','source','worker_id','workstream','task_id','command_type','instructions'}
SAFE_RESULT={'command_id','worker_id','workstream','state','reason','created_at','updated_at','completed_at','sanitized_result','result_digest','result_truncated'}
PERMANENT={'CONFIG_INVALID','TRANSPORT_HISTORY_CHANGED','UNTRUSTED_TRANSPORT_HEAD','ACTOR_VERIFICATION_FAILED','INVALID_COMMAND','INVALID_COMMAND_COMMIT','UNVERIFIED_TRANSPORT_CHANGE','DUPLICATE_COMMAND','RESULT_EXISTS','WORKSPACE_INVALID','WORKSPACE_REMOTE_MISMATCH','UNKNOWN_RESULT_COMMIT','INVALID_LIFECYCLE_STATE','STATUS_INVALID_RESPONSE','WORKSPACE_DIRTY'}
TRANSIENT={'GIT_UNAVAILABLE','SOCKET_UNAVAILABLE','SOCKET_INVALID_RESPONSE','ACTOR_LOOKUP_UNAVAILABLE','STATUS_UNAVAILABLE','SUBMIT_FAILED','REMOTE_ADVANCED'}
APPROVED_REMOTE='https://github.com/InitSombra-NexoMotive-LLC/hermes-agent.git'
class RelayError(RuntimeError):
 def __init__(self,code): super().__init__(code); self.code=code
@dataclass
class RelayConfig:
 remote:str; branch:str; trusted_head:str; workspace:Path; state_db:Path; timeout:float=30; retries:int=3; poll_seconds:float=30
class UnixSocketClient:
 def __init__(self,path,timeout=30,protocol=1,max_bytes=65536): self.path=path; self.timeout=timeout; self.protocol=protocol; self.max_bytes=max_bytes
 def request(self,payload):
  try:
   with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
    s.settimeout(self.timeout); s.connect(self.path); s.sendall(json.dumps(payload,separators=(',',':')).encode()+b'\n'); raw=s.makefile('rb').readline(self.max_bytes+1)
   if not raw or not raw.endswith(b'\n') or len(raw)>self.max_bytes: raise RelayError('SOCKET_INVALID_RESPONSE')
   response=json.loads(raw)
   if not isinstance(response,dict) or response.get('ok') is not True or response.get('protocol')!=self.protocol or not isinstance(response.get('result'),dict): raise RelayError('SOCKET_INVALID_RESPONSE')
   return response['result']
  except RelayError: raise
  except (OSError,ValueError,json.JSONDecodeError): raise RelayError('SOCKET_UNAVAILABLE') from None
class StatusRelay:
 def __init__(self,c,socket_client,actor_lookup):
  self.c=c; self.c.workspace=Path(c.workspace); self.c.state_db=Path(c.state_db); self.socket=socket_client; self.actor_lookup=actor_lookup; self.c.state_db.parent.mkdir(parents=True,exist_ok=True)
  with self.db() as d:
   d.execute('CREATE TABLE IF NOT EXISTS relay_commands(command_id TEXT PRIMARY KEY,source_commit TEXT NOT NULL,lifecycle TEXT NOT NULL,command_json TEXT NOT NULL,result_json TEXT,published_commit TEXT,reason TEXT)')
   d.execute('CREATE TABLE IF NOT EXISTS relay_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
 @contextmanager
 def db(self):
  d=sqlite3.connect(self.c.state_db,timeout=self.c.timeout)
  try: yield d; d.commit()
  except: d.rollback(); raise
  finally: d.close()
 def git(self,*args):
  try:return subprocess.run(['git','-C',str(self.c.workspace),*args],check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=self.c.timeout).stdout.strip()
  except (OSError,subprocess.SubprocessError): raise RelayError('GIT_UNAVAILABLE') from None
 def recover_dirty(self):
  dirty=self.git('status','--porcelain','--untracked-files=all').splitlines()
  if not dirty:return
  if len(dirty)!=1: raise RelayError('WORKSPACE_DIRTY')
  entry=dirty[0];path=entry[3:]
  if entry[:2] not in {'??','A '} or not path.startswith('control-inbox/results/') or Path(self.c.workspace/path).is_symlink(): raise RelayError('WORKSPACE_DIRTY')
  command_id=path.removeprefix('control-inbox/results/').removesuffix('.json');row=self.row(command_id)
  if path!=f'control-inbox/results/{command_id}.json' or not row or row[1]!='RESULT_READY' or not row[3]: raise RelayError('WORKSPACE_DIRTY')
  try: exact=(self.c.workspace/path).read_bytes()==json.dumps(json.loads(row[3]),sort_keys=True,separators=(',',':')).encode()+b'\n'
  except OSError: exact=False
  if not exact: raise RelayError('WORKSPACE_DIRTY')
 def prepare(self):
  if not self.c.workspace.exists() or (self.c.workspace.is_dir() and not any(self.c.workspace.iterdir())):
   if self.c.workspace.exists(): self.c.workspace.rmdir()
   try: subprocess.run(['git','clone','--no-checkout',self.c.remote,str(self.c.workspace)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=self.c.timeout)
   except (OSError,subprocess.SubprocessError): raise RelayError('GIT_UNAVAILABLE') from None
  if not (self.c.workspace/'.git').is_dir(): raise RelayError('WORKSPACE_INVALID')
  if self.git('remote','get-url','origin')!=self.c.remote: raise RelayError('WORKSPACE_REMOTE_MISMATCH')
  self.recover_dirty()
  self.git('fetch','--prune','origin',self.c.branch); self.git('checkout','--detach','FETCH_HEAD'); return self.git('rev-parse','HEAD')
 def ancestor(self,a,b): return subprocess.run(['git','-C',str(self.c.workspace),'merge-base','--is-ancestor',a,b],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
 @staticmethod
 def validate_command(data):
  if not isinstance(data,dict) or set(data)!=FIELDS or type(data.get('protocol_version')) is not int or data['protocol_version']!=1: raise RelayError('INVALID_COMMAND')
  if not all(isinstance(data.get(k),str) for k in FIELDS-{'protocol_version'}) or len(data['instructions'])>8000: raise RelayError('INVALID_COMMAND')
  if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',data['command_id']): raise RelayError('INVALID_COMMAND')
  if (data['source'],data['worker_id'],data['workstream'],data['command_type'])!=('github-control','nvidia-control','CONTROL_PLANE','STATUS'): raise RelayError('INVALID_COMMAND')
  return data
 def verify_actor(self,commit):
  try: actor=self.actor_lookup(commit)
  except Exception: raise RelayError('ACTOR_LOOKUP_UNAVAILABLE') from None
  if not isinstance(actor,dict) or actor.get('login')!='InitSombra-NexoMotive-LLC' or actor.get('id')!=239685310: raise RelayError('ACTOR_VERIFICATION_FAILED')
 def changes(self,commit): return [x.split('\t',1) for x in self.git('diff-tree','--root','--no-commit-id','--name-status','-r',commit).splitlines()]
 def raw_git(self,*args):
  try:return subprocess.run(['git','-C',str(self.c.workspace),*args],check=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=self.c.timeout).stdout
  except (OSError,subprocess.SubprocessError): raise RelayError('GIT_UNAVAILABLE') from None
 def validate_result_commit(self,commit,changes):
  if len(changes)!=1 or changes[0][0]!='A' or not changes[0][1].startswith('control-inbox/results/'): raise RelayError('UNKNOWN_RESULT_COMMIT')
  path=changes[0][1]; command_id=path.removeprefix('control-inbox/results/').removesuffix('.json')
  if path!=f'control-inbox/results/{command_id}.json' or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',command_id): raise RelayError('UNKNOWN_RESULT_COMMIT')
  if not self.git('ls-tree',commit,'--',path).startswith('100644 '): raise RelayError('UNKNOWN_RESULT_COMMIT')
  row=self.row(command_id)
  if not row or row[1] not in {'RESULT_READY','PUBLISHED'} or not row[3]: raise RelayError('UNKNOWN_RESULT_COMMIT')
  expected=json.dumps(json.loads(row[3]),sort_keys=True,separators=(',',':')).encode()+b'\n'
  if self.raw_git('show',f'{commit}:{path}')!=expected: raise RelayError('UNKNOWN_RESULT_COMMIT')
  try:
   if set(json.loads(expected))!=SAFE_RESULT: raise RelayError('UNKNOWN_RESULT_COMMIT')
  except RelayError: raise
  except Exception: raise RelayError('UNKNOWN_RESULT_COMMIT') from None
  if row[1]=='PUBLISHED' and row[4]!=commit: raise RelayError('UNKNOWN_RESULT_COMMIT')
  if row[1]=='RESULT_READY': self.save(command_id,row[0],'PUBLISHED',json.loads(row[2]),result=json.loads(row[3]),published=commit)

 def row(self,command_id):
  with self.db() as d:return d.execute('SELECT source_commit,lifecycle,command_json,result_json,published_commit,reason FROM relay_commands WHERE command_id=?',(command_id,)).fetchone()
 def save(self,command_id,commit,state,data,result=None,published=None,reason=None):
  legal={'DISCOVERED':{'SUBMITTED','FAILED'},'SUBMITTED':{'RESULT_READY','FAILED'},'RESULT_READY':{'PUBLISHED','FAILED'},'PUBLISHED':set(),'FAILED':set()}; canonical=json.dumps(data,sort_keys=True); encoded=json.dumps(result,sort_keys=True) if result is not None else None
  if state=='FAILED' and reason not in {'EXECUTION_FAILED','RESULT_SANITIZATION_FAILED','RESULT_PERSISTENCE_FAILED','INVALID_LIFECYCLE_STATE','SCHEDULING_FAILED'}: raise RelayError('INVALID_LIFECYCLE_STATE')
  with self.db() as d:
   current=d.execute('SELECT lifecycle,source_commit,command_json,result_json,published_commit,reason FROM relay_commands WHERE command_id=?',(command_id,)).fetchone()
   if not current:
    if state!='DISCOVERED': raise RelayError('INVALID_LIFECYCLE_STATE')
    d.execute('INSERT INTO relay_commands(command_id,source_commit,lifecycle,command_json,result_json,published_commit,reason) VALUES(?,?,?,?,?,?,?)',(command_id,commit,state,canonical,None,None,None)); return
   old,old_commit,old_command,old_result,old_published,old_reason=current
   if old==state:
    if old_commit!=commit or old_command!=canonical or (encoded is not None and encoded!=old_result) or (published is not None and published!=old_published) or (reason is not None and reason!=old_reason): raise RelayError('INVALID_LIFECYCLE_STATE')
    return
   if state not in legal[old] or old_commit!=commit or old_command!=canonical: raise RelayError('INVALID_LIFECYCLE_STATE')
   if state=='RESULT_READY' and encoded is None: raise RelayError('INVALID_LIFECYCLE_STATE')
   if state=='PUBLISHED' and (old_result is None or encoded not in {None,old_result} or not published): raise RelayError('INVALID_LIFECYCLE_STATE')
   d.execute('UPDATE relay_commands SET lifecycle=?,result_json=COALESCE(?,result_json),published_commit=COALESCE(?,published_commit),reason=COALESCE(?,reason) WHERE command_id=?',(state,encoded,published,reason,command_id))
 def result(self,command_id):
  status=self.socket.request({'verb':'command-status','command_id':command_id})
  if status.get('status')!='OK': raise RelayError('STATUS_UNAVAILABLE')
  states={'ACCEPTED','QUEUED','RUNNING','DELIVERED','COMPLETED','FAILED'}
  if status.get('command_id')!=command_id or status.get('worker_id')!='nvidia-control' or status.get('workstream')!='CONTROL_PLANE' or status.get('state') not in states: raise RelayError('STATUS_INVALID_RESPONSE')
  truncated=status.get('result_truncated')
  if not isinstance(truncated,(bool,int)) or truncated not in {False,True,0,1} or not isinstance(status.get('created_at'),str) or not status['created_at'] or not isinstance(status.get('updated_at'),str) or not status['updated_at']: raise RelayError('STATUS_INVALID_RESPONSE')
  if status['state'] not in {'COMPLETED','FAILED'}: return None
  if status['state']=='COMPLETED':
   summary,digest=status.get('sanitized_result'),status.get('result_digest')
   if not isinstance(summary,str) or len(summary)>4096 or not isinstance(digest,str) or not re.fullmatch(r'[0-9a-f]{64}',digest) or hashlib.sha256(summary.encode()).hexdigest()!=digest or not isinstance(status.get('completed_at'),str) or not status['completed_at'] or status.get('reason') not in {None,''}: raise RelayError('STATUS_INVALID_RESPONSE')
  else:
   if status.get('reason') not in {'EXECUTION_FAILED','RESULT_SANITIZATION_FAILED','RESULT_PERSISTENCE_FAILED','INVALID_LIFECYCLE_STATE','SCHEDULING_FAILED'} or status.get('sanitized_result') is not None or status.get('result_digest') is not None or status.get('completed_at') is not None: raise RelayError('STATUS_INVALID_RESPONSE')
  clean={k:status.get(k) for k in SAFE_RESULT};clean['result_truncated']=bool(truncated);return clean
 def publish(self,command_id,result):
  target=self.c.workspace/'control-inbox/results'/f'{command_id}.json'
  if target.exists():
   row=self.row(command_id);expected=json.dumps(result,sort_keys=True,separators=(',',':'))+'\n'
   if target.is_symlink() or not row or row[1]!='RESULT_READY' or target.read_text()!=expected: raise RelayError('WORKSPACE_DIRTY')
  else: target.parent.mkdir(parents=True,exist_ok=True); target.write_text(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n')
  verified_parent=self.git('rev-parse','HEAD')
  self.git('add','--',str(target.relative_to(self.c.workspace))); self.git('-c','user.name=Hermes GitHub Control Relay','-c','user.email=relay@localhost','commit','-m',f'control(nvidia): publish sanitized result {command_id}')
  try: self.git('push','origin',f'HEAD:{self.c.branch}'); return self.git('rev-parse','HEAD')
  except RelayError:
   self.git('fetch','origin',self.c.branch)
   if not self.ancestor(verified_parent,'FETCH_HEAD'): raise RelayError('TRANSPORT_HISTORY_CHANGED')
   raise RelayError('REMOTE_ADVANCED')
 def resume(self,command_id,commit,data):
  row=self.row(command_id)
  if row and row[0]!=commit: raise RelayError('DUPLICATE_COMMAND')
  state=row[1] if row else 'DISCOVERED'
  if not row: self.save(command_id,commit,'DISCOVERED',data)
  if state=='PUBLISHED' or state=='FAILED': return False
  if state=='DISCOVERED':
   reply=self.socket.request({'verb':'submit-command',**data})
   if reply.get('status') not in {'ACCEPTED','DUPLICATE'}: raise RelayError('SUBMIT_FAILED')
   self.save(command_id,commit,'SUBMITTED',data); state='SUBMITTED'
  if state=='SUBMITTED':
   result=self.result(command_id)
   if result is None:return False
   self.save(command_id,commit,'RESULT_READY',data,result=result); state='RESULT_READY'
  if state=='RESULT_READY':
   row=self.row(command_id); result=json.loads(row[3]); published=self.publish(command_id,result); self.save(command_id,commit,'PUBLISHED',data,result=result,published=published); return True
  return False
 def poll_once(self):
  head=self.prepare()
  with self.db() as d: row=d.execute("SELECT value FROM relay_meta WHERE key='head'").fetchone()
  base=row[0] if row else self.c.trusted_head
  if not self.ancestor(self.c.trusted_head,head): raise RelayError('UNTRUSTED_TRANSPORT_HEAD')
  if not self.ancestor(base,head): raise RelayError('TRANSPORT_HISTORY_CHANGED')
  commits=([head] if not row and base==head else self.git('rev-list','--reverse',f'{base}..{head}').splitlines()); done=[]
  for commit in commits:
   changes=self.changes(commit); commands=[p for s,p in changes if p.startswith('control-inbox/commands/')]
   if not commands:
    self.validate_result_commit(commit,changes)
    continue
   if len(changes)!=1 or len(commands)!=1 or changes[0][0]!='A' or '/' in commands[0].removeprefix('control-inbox/commands/') or not self.git('ls-tree',commit,'--',commands[0]).startswith('100644 '): raise RelayError('INVALID_COMMAND_COMMIT')
   self.verify_actor(commit); raw=self.git('show',f'{commit}:{commands[0]}')
   if len(raw.encode())>16384: raise RelayError('INVALID_COMMAND')
   try:data=self.validate_command(json.loads(raw))
   except RelayError: raise
   except Exception: raise RelayError('INVALID_COMMAND') from None
   if commands[0]!=f"control-inbox/commands/{data['command_id']}.json": raise RelayError('INVALID_COMMAND_COMMIT')
   existing=self.row(data['command_id'])
   if existing and (existing[0]!=commit or existing[2]!=json.dumps(data,sort_keys=True)): raise RelayError('DUPLICATE_COMMAND')
   self.save(data['command_id'],commit,'DISCOVERED',data) if not existing else None
  with self.db() as d:d.execute("INSERT INTO relay_meta VALUES('head',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(head,)); pending=d.execute("SELECT command_id,source_commit,command_json FROM relay_commands WHERE lifecycle IN ('DISCOVERED','SUBMITTED','RESULT_READY')").fetchall()
  for command_id,commit,raw in pending:
   if self.resume(command_id,commit,json.loads(raw)): done.append(command_id)
  return done

def github_actor_lookup(commit):
 raw=subprocess.run(['gh','api',f'repos/InitSombra-NexoMotive-LLC/hermes-agent/commits/{commit}'],check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=30).stdout; return (json.loads(raw).get('author') or {})
def load_config(path):
 try:
  p=Path(path);st=p.lstat();required={'remote','branch','trusted_head','workspace','state_db','gateway_socket'};optional={'timeout','retries','poll_seconds'}
  if p.is_symlink() or not p.is_file() or st.st_uid!=os.getuid() or st.st_mode&0o777!=0o600: raise ValueError
  raw=json.loads(p.read_text())
  if not isinstance(raw,dict) or set(raw)-required-optional or required-set(raw) or raw['remote']!=APPROVED_REMOTE or raw['branch']!='control/nvidia-command-inbox' or not isinstance(raw['trusted_head'],str) or not re.fullmatch(r'[0-9a-f]{40}',raw['trusted_head']): raise ValueError
  if any(not isinstance(raw[k],str) or not Path(raw[k]).is_absolute() for k in ('workspace','state_db','gateway_socket')): raise ValueError
  if 'timeout' in raw and (type(raw['timeout']) not in (int,float) or not 1<=raw['timeout']<=120): raise ValueError
  if 'retries' in raw and (type(raw['retries']) is not int or not 0<=raw['retries']<=10): raise ValueError
  if 'poll_seconds' in raw and (type(raw['poll_seconds']) not in (int,float) or not 5<=raw['poll_seconds']<=300): raise ValueError
  return raw
 except Exception: raise RelayError('CONFIG_INVALID')
def run_service_iteration(relay):
 try: relay.poll_once()
 except RelayError as e:
  if e.code in TRANSIENT: LOG.error('relay outcome: %s',e.code);return
  if e.code in PERMANENT: LOG.error('relay outcome: %s',e.code);raise SystemExit(73)
  LOG.error('relay outcome: INTERNAL_FAILURE');raise SystemExit(1)
 except Exception: LOG.error('relay outcome: INTERNAL_FAILURE');raise SystemExit(1)
def main():
 try: config=load_config(os.environ['HERMES_GITHUB_CONTROL_RELAY_CONFIG']);c=RelayConfig(**{k:config[k] for k in RelayConfig.__annotations__ if k in config});relay=StatusRelay(c,UnixSocketClient(config['gateway_socket'],timeout=c.timeout),github_actor_lookup)
 except Exception: LOG.error('relay outcome: CONFIG_INVALID');raise SystemExit(73)
 while True: run_service_iteration(relay);time.sleep(c.poll_seconds)
if __name__=='__main__':main()
