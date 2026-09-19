"""Trusted, gateway-owned worker-control MessageEvent construction."""
from __future__ import annotations
from gateway.platforms.event import MessageEvent
from gateway.platforms.event import MessageType
from gateway.session import SessionSource
from gateway.config import Platform
from gateway.worker_session_registry import WorkerSessionRegistry,BindError
_TRUST=object()
def trusted_override(event):
 return event.metadata.get('_github_control_trust') is _TRUST and event.metadata.get('session_key_override')
def build_worker_control_event(registry,command):
 if command.get('worker_id')!='nvidia-control' or command.get('workstream')!='CONTROL_PLANE':raise BindError('UNAUTHORIZED')
 key=registry.resolve('nvidia-control')
 text='[GITHUB CONTROL] '+command['command_type']+'\n'+command['instructions']
 return MessageEvent(text=text,message_type=MessageType.TEXT,source=SessionSource(platform=Platform.LOCAL,chat_id='github-control',chat_name='github-control',user_id='nvidia-control'),internal=True,allow_gateway_control=False,metadata={'_github_control_trust':_TRUST,'session_key_override':key,'github_control':{'command_id':command['command_id'],'worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':command['task_id'],'command_type':command['command_type']}})
