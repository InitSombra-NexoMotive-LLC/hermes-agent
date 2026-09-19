import asyncio
from pathlib import Path
import pytest
from gateway.worker_session_registry import WorkerSessionRegistry
from gateway.session import build_session_key
from tests.gateway.test_steer_command import _make_event,_make_runner,_make_source,_session_entry
def test_telegram_bind_consumes_before_agent(monkeypatch,tmp_path):
 source=_make_source(); source.platform=__import__('gateway.config',fromlist=['Platform']).Platform.TELEGRAM;source.chat_id='bind-chat'
 reg=WorkerSessionRegistry(tmp_path/'bind.db');challenge=reg.start('nvidia-control','CONTROL_PLANE','bind-chat');monkeypatch.setenv('HERMES_GITHUB_CONTROL_DB',str(tmp_path/'bind.db'))
 runner,adapter=_make_runner(_session_entry());called=[]
 async def no_agent(*args):called.append(args);return 'bad'
 runner._handle_message_with_agent=no_agent
 event=_make_event(challenge);event.source=source
 result=asyncio.run(runner._handle_message(event))
 assert result=='NVIDIA worker session bootstrap accepted.' and not called and reg.resolve('nvidia-control')==build_session_key(source)
def test_nonmatching_telegram_continues(monkeypatch,tmp_path):
 source=_make_source();source.platform=__import__('gateway.config',fromlist=['Platform']).Platform.TELEGRAM;source.chat_id='bind-chat';reg=WorkerSessionRegistry(tmp_path/'bind.db');reg.start('nvidia-control','CONTROL_PLANE','bind-chat');monkeypatch.setenv('HERMES_GITHUB_CONTROL_DB',str(tmp_path/'bind.db'))
 runner,adapter=_make_runner(_session_entry());called=[]
 async def agent(*args):called.append(args);return 'normal'
 runner._handle_message_with_agent=agent;event=_make_event('ordinary');event.source=source
 assert asyncio.run(runner._handle_message(event))=='normal' and called
