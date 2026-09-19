"""Local-only GitHub Control message validation and durable idempotency."""
from __future__ import annotations
import json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ALLOWED={"STATUS","CONTINUE","RECOVER","RECHECK","START_TASK","PAUSE","RESUME","STOP_AFTER_SAFE_POINT"}
REQUIRED={"command_id","source","worker_id","workstream","task_id","command_type","instructions"}
class CommandError(ValueError): pass
class CommandLedger:
 def __init__(self,path:Path):
  path.parent.mkdir(parents=True,exist_ok=True);self.db=sqlite3.connect(path)
  self.db.execute("CREATE TABLE IF NOT EXISTS github_control_commands(command_id TEXT PRIMARY KEY,state TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL)");self.db.commit()
 def accept(self,p:dict[str,Any])->str:
  if not isinstance(p,dict) or not REQUIRED<=p.keys():raise CommandError("INVALID")
  if p["source"]!="github-control" or p["worker_id"]!="nvidia-control" or p["workstream"]!="CONTROL_PLANE":raise CommandError("UNAUTHORIZED")
  if p["command_type"] not in ALLOWED or not isinstance(p["instructions"],str) or len(p["instructions"])>8000:raise CommandError("INVALID")
  try:self.db.execute("INSERT INTO github_control_commands VALUES(?,?,?,?)",(p["command_id"],"QUEUED",json.dumps(p),datetime.now(timezone.utc).isoformat()));self.db.commit();return "ACCEPTED"
  except sqlite3.IntegrityError:return "DUPLICATE"
def command_text(p:dict[str,Any])->str:
 return "[GITHUB CONTROL COMMAND]\n"+"\n".join(f"{k.upper()}: {p[k]}" for k in ("command_id","worker_id","workstream","task_id","command_type","instructions"))
