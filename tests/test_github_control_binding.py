from pathlib import Path
import pytest
from gateway.worker_session_registry import WorkerSessionRegistry,BindError
from gateway.github_control_event import build_worker_control_event,trusted_override
def test_bind_and_trusted_event(tmp_path):
 r=WorkerSessionRegistry(tmp_path/'s.db');c=r.start('nvidia-control','CONTROL_PLANE','chat');r.consume('nvidia-control',c,'chat','fixture-session');e=build_worker_control_event(r,{'command_id':'c','worker_id':'nvidia-control','workstream':'CONTROL_PLANE','task_id':'t','command_type':'STATUS','instructions':'status'});assert trusted_override(e)=='fixture-session' and e.source.chat_id=='github-control' and e.metadata['github_control']['worker_id']=='nvidia-control'
def test_bind_replay_wrong_identity_and_no_untrusted_override(tmp_path):
 r=WorkerSessionRegistry(tmp_path/'s.db');c=r.start('nvidia-control','CONTROL_PLANE','chat')
 with pytest.raises(BindError):r.consume('nvidia-control',c,'other','s')
 r.consume('nvidia-control',c,'chat','s')
 with pytest.raises(BindError):r.consume('nvidia-control',c,'chat','x')
 class E:metadata={'session_key_override':'hijack'}
 assert trusted_override(E()) is False
