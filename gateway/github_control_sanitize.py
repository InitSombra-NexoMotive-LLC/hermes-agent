"""Fail-closed sanitization for public GitHub control results."""
from __future__ import annotations
import hashlib,re
MAX_RESULT_CHARS=4096
_SECRET=re.compile(r'(?i)(bearer\s+\S+|gh[ropsu]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+|(?:password|secret|token|authorization|api[_-]?key)\s*[=:]\s*\S+)')
_PRIVATE=re.compile(r'-----BEGIN [A-Z ]*PRIVATE KEY-----')
_SESSION=re.compile(r'agent:[^\s]+|telegram:\S+')
def sanitize_result(value):
 if not isinstance(value,(str,int,float,bool,type(None),dict,list)):raise ValueError('UNSUPPORTED_RESULT')
 text=str(value) if not isinstance(value,(dict,list)) else __import__('json').dumps(value,sort_keys=True)
 if _PRIVATE.search(text):raise ValueError('PRIVATE_KEY_REJECTED')
 text=''.join(c if c >= ' ' or c in '\n\t' else ' ' for c in text)
 text=_SESSION.sub('[REDACTED]',_SECRET.sub('[REDACTED]',text))
 truncated=len(text)>MAX_RESULT_CHARS;text=text[:MAX_RESULT_CHARS]
 return {'summary':text,'digest':hashlib.sha256(text.encode()).hexdigest(),'truncated':truncated}
