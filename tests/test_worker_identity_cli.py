from datetime import timedelta
from pathlib import Path
import sqlite3
import pytest
from gateway.worker_session_registry import BindError, WorkerSessionRegistry, _now


def registry(tmp_path):
    return WorkerSessionRegistry(tmp_path / "identity.db")


def test_identity_status_is_safe_and_has_no_raw_identifier(tmp_path):
    r = registry(tmp_path)
    r.start("nvidia-control", "CONTROL_PLANE", "configured-raw")
    status = r.identity_status("nvidia-control", "CONTROL_PLANE")
    assert status["configured_fingerprint"] != "configured-raw"
    assert "configured-raw" not in repr(status)
    assert status["observed_fingerprint"] == "NONE"


def test_confirm_requires_exact_failed_bind_observation_and_audits(tmp_path):
    r = registry(tmp_path)
    challenge = r.start("nvidia-control", "CONTROL_PLANE", "configured")
    assert r.record_failed_bind_observation("nvidia-control", "CONTROL_PLANE", challenge, "observed")
    before = r.identity_status("nvidia-control", "CONTROL_PLANE")
    assert not before["match"] and before["observed_during_recent_bind_attempt"]
    r.confirm_observed_identity("nvidia-control", "CONTROL_PLANE")
    after = r.identity_status("nvidia-control", "CONTROL_PLANE")
    assert after["match"] and after["configured_fingerprint"] == after["observed_fingerprint"]
    audit = r.db.execute("SELECT old_fingerprint,new_fingerprint,provenance FROM telegram_identity_audit").fetchone()
    assert audit[0] != audit[1] and audit[2] == "local-admin-confirmed-telegram-bootstrap"
    assert r.db.execute("SELECT COUNT(*) FROM worker_sessions").fetchone()[0] == 0


def test_observation_rejects_wrong_scope_and_confirmation_without_evidence(tmp_path):
    r = registry(tmp_path)
    challenge = r.start("nvidia-control", "CONTROL_PLANE", "configured")
    assert not r.record_failed_bind_observation("other", "CONTROL_PLANE", challenge, "observed")
    assert not r.record_failed_bind_observation("nvidia-control", "OTHER", challenge, "observed")
    with pytest.raises(BindError, match="OBSERVATION_NOT_CONFIRMABLE"):
        r.confirm_observed_identity("nvidia-control", "CONTROL_PLANE")


def test_session_bind_retires_previous_challenge(tmp_path):
    r = registry(tmp_path)
    old = r.start("nvidia-control", "CONTROL_PLANE", "configured")
    fresh, expires = r.begin_session_bind("nvidia-control", "CONTROL_PLANE")
    assert fresh != old and expires
    with pytest.raises(BindError):
        r.consume("nvidia-control", old, "configured", "session")


def test_observation_challenge_is_separate_from_session_binding(tmp_path):
    r = registry(tmp_path)
    challenge = r.start_identity_observation("nvidia-control", "CONTROL_PLANE")
    assert r.try_consume_identity_observation("nvidia-control", "CONTROL_PLANE", challenge, "observed")
    assert not r.try_consume_identity_observation("nvidia-control", "CONTROL_PLANE", challenge, "other")
    assert r.db.execute("SELECT COUNT(*) FROM worker_sessions").fetchone()[0] == 0
    assert r.identity_status("nvidia-control", "CONTROL_PLANE")["observation_timestamp"]


def test_stale_observation_cannot_replace_identity(tmp_path):
    r = registry(tmp_path)
    challenge = r.start("nvidia-control", "CONTROL_PLANE", "configured")
    assert r.record_failed_bind_observation("nvidia-control", "CONTROL_PLANE", challenge, "observed")
    r.db.execute("UPDATE telegram_identity_observations SET expires_at=?", ((_now()-timedelta(seconds=1)).isoformat(),))
    r.db.commit()
    with pytest.raises(BindError, match="OBSERVATION_NOT_CONFIRMABLE"):
        r.confirm_observed_identity("nvidia-control", "CONTROL_PLANE")
    assert r.identity_status("nvidia-control", "CONTROL_PLANE")["configured_fingerprint"] != r.identity_status("nvidia-control", "CONTROL_PLANE")["observed_fingerprint"]
