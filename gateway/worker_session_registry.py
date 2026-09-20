"""Protected local worker-session and Telegram-identity registry."""
from __future__ import annotations

import hashlib
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


class BindError(RuntimeError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint(value: object | None) -> str:
    return "NONE" if value is None else hashlib.sha256(str(value).encode()).hexdigest()[:12].upper()


class WorkerSessionRegistry:
    """Local-only trusted binding state. Never return raw session or Telegram IDs."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS worker_sessions(
          worker_id TEXT PRIMARY KEY,workstream TEXT,session_key TEXT,bound_at TEXT,bound_via TEXT,verified INT);
        CREATE TABLE IF NOT EXISTS bind_challenges(
          worker_id TEXT PRIMARY KEY,challenge TEXT,expires_at TEXT,telegram_chat_id TEXT,used INT DEFAULT 0);
        CREATE TABLE IF NOT EXISTS worker_telegram_identities(
          worker_id TEXT PRIMARY KEY,workstream TEXT,telegram_chat_id TEXT,updated_at TEXT,provenance TEXT);
        CREATE TABLE IF NOT EXISTS telegram_identity_observations(
          worker_id TEXT PRIMARY KEY,workstream TEXT,telegram_chat_id TEXT,observed_at TEXT,expires_at TEXT,provenance TEXT);
        CREATE TABLE IF NOT EXISTS identity_observation_challenges(
          worker_id TEXT PRIMARY KEY,workstream TEXT,challenge TEXT,expires_at TEXT,used INT DEFAULT 0);
        CREATE TABLE IF NOT EXISTS telegram_identity_audit(
          id INTEGER PRIMARY KEY,worker_id TEXT,workstream TEXT,old_fingerprint TEXT,new_fingerprint TEXT,
          observed_at TEXT,confirmed_at TEXT,provenance TEXT);
        """)
        self.db.commit()

    def _expected(self, worker_id: str) -> tuple[str, str] | None:
        row = self.db.execute("SELECT telegram_chat_id,workstream FROM worker_telegram_identities WHERE worker_id=?", (worker_id,)).fetchone()
        if row:
            return str(row[0]), row[1]
        # Compatibility for the pre-identity-registry pending bind state.
        row = self.db.execute("SELECT telegram_chat_id,'CONTROL_PLANE' FROM bind_challenges WHERE worker_id=?", (worker_id,)).fetchone()
        return (str(row[0]), row[1]) if row else None

    def identity_status(self, worker_id: str, workstream: str) -> dict:
        expected = self._expected(worker_id)
        observed = self.db.execute("SELECT telegram_chat_id,observed_at,expires_at,provenance FROM telegram_identity_observations WHERE worker_id=? AND workstream=?", (worker_id, workstream)).fetchone()
        return {"worker_id": worker_id, "workstream": workstream,
                "configured_fingerprint": _fingerprint(expected[0] if expected else None),
                "observed_fingerprint": _fingerprint(observed[0] if observed else None),
                "match": bool(expected and observed and expected[0] == str(observed[0])),
                "observed_during_recent_bind_attempt": bool(observed and observed[3] == "failed-bind-attempt"),
                "observation_timestamp": observed[1] if observed else None,
                "observation_expires_at": observed[2] if observed else None}

    def record_failed_bind_observation(self, worker_id: str, workstream: str, text: str, telegram_chat_id: object) -> bool:
        """Record only an exact, active challenge from a wrong trusted identity."""
        if worker_id != "nvidia-control" or workstream != "CONTROL_PLANE":
            return False
        row = self.db.execute("SELECT challenge,expires_at,telegram_chat_id,used FROM bind_challenges WHERE worker_id=?", (worker_id,)).fetchone()
        if not row or row[3] or text != row[0] or datetime.fromisoformat(row[1]) < _now() or str(telegram_chat_id) == str(row[2]):
            return False
        expires = min(datetime.fromisoformat(row[1]), _now() + timedelta(minutes=10))
        self.db.execute("INSERT OR REPLACE INTO telegram_identity_observations VALUES(?,?,?,?,?,?)", (worker_id, workstream, str(telegram_chat_id), _now().isoformat(), expires.isoformat(), "failed-bind-attempt"))
        self.db.commit()
        return True

    def start_identity_observation(self, worker_id: str, workstream: str, ttl: int = 600) -> str:
        if worker_id != "nvidia-control" or workstream != "CONTROL_PLANE":
            raise BindError("IDENTITY_SCOPE_REJECTED")
        challenge = "NVIDIA-IDENTITY-OBSERVE-" + secrets.token_urlsafe(24)
        expires = _now() + timedelta(seconds=ttl)
        self.db.execute("INSERT OR REPLACE INTO identity_observation_challenges VALUES(?,?,?,?,0)", (worker_id, workstream, challenge, expires.isoformat()))
        self.db.commit()
        return challenge

    def try_consume_identity_observation(self, worker_id: str, workstream: str, text: str, telegram_chat_id: object) -> bool:
        row = self.db.execute("SELECT workstream,challenge,expires_at,used FROM identity_observation_challenges WHERE worker_id=?", (worker_id,)).fetchone()
        if not row or row[0] != workstream or row[3] or text != row[1] or datetime.fromisoformat(row[2]) < _now():
            return False
        self.db.execute("INSERT OR REPLACE INTO telegram_identity_observations VALUES(?,?,?,?,?,?)", (worker_id, workstream, str(telegram_chat_id), _now().isoformat(), row[2], "identity-observation-challenge"))
        self.db.execute("UPDATE identity_observation_challenges SET used=1 WHERE worker_id=?", (worker_id,))
        self.db.commit()
        return True

    def confirm_observed_identity(self, worker_id: str, workstream: str) -> dict:
        observed = self.db.execute("SELECT telegram_chat_id,observed_at,expires_at,provenance FROM telegram_identity_observations WHERE worker_id=? AND workstream=?", (worker_id, workstream)).fetchone()
        if not observed or observed[3] not in {"failed-bind-attempt", "identity-observation-challenge"} or datetime.fromisoformat(observed[2]) < _now():
            raise BindError("OBSERVATION_NOT_CONFIRMABLE")
        prior = self._expected(worker_id)
        self.db.execute("INSERT OR REPLACE INTO worker_telegram_identities VALUES(?,?,?,?,?)", (worker_id, workstream, str(observed[0]), _now().isoformat(), "local-admin-confirmed-telegram-bootstrap"))
        self.db.execute("INSERT INTO telegram_identity_audit(worker_id,workstream,old_fingerprint,new_fingerprint,observed_at,confirmed_at,provenance) VALUES(?,?,?,?,?,?,?)", (worker_id, workstream, _fingerprint(prior[0] if prior else None), _fingerprint(observed[0]), observed[1], _now().isoformat(), "local-admin-confirmed-telegram-bootstrap"))
        self.db.commit()
        return self.identity_status(worker_id, workstream)

    def start(self, worker_id, workstream, telegram_chat_id=None, ttl=600):
        if self.db.execute("SELECT 1 FROM worker_sessions WHERE worker_id=?", (worker_id,)).fetchone():
            raise BindError("REBIND_REQUIRED")
        expected = self._expected(worker_id)
        identity = str(telegram_chat_id) if telegram_chat_id is not None else (expected[0] if expected else None)
        if identity is None:
            raise BindError("TELEGRAM_IDENTITY_NOT_CONFIGURED")
        c = "NVIDIA-CONTROL-BIND-" + secrets.token_urlsafe(24)
        exp = _now() + timedelta(seconds=ttl)
        self.db.execute("INSERT OR REPLACE INTO bind_challenges VALUES(?,?,?,?,0)", (worker_id, c, exp.isoformat(), identity))
        self.db.commit()
        return c

    def retire_challenge(self, worker_id: str) -> None:
        self.db.execute("UPDATE bind_challenges SET used=1 WHERE worker_id=?", (worker_id,))
        self.db.commit()

    def begin_session_bind(self, worker_id: str, workstream: str, ttl: int = 600) -> tuple[str, str]:
        if worker_id != "nvidia-control" or workstream != "CONTROL_PLANE":
            raise BindError("BIND_SCOPE_REJECTED")
        self.retire_challenge(worker_id)
        challenge = self.start(worker_id, workstream, ttl=ttl)
        expiry = self.db.execute("SELECT expires_at FROM bind_challenges WHERE worker_id=?", (worker_id,)).fetchone()[0]
        return challenge, expiry

    def consume(self, worker_id, text, telegram_chat_id, session_key):
        row = self.db.execute("SELECT challenge,expires_at,telegram_chat_id,used FROM bind_challenges WHERE worker_id=?", (worker_id,)).fetchone()
        if not row or row[3] or text != row[0] or str(telegram_chat_id) != str(row[2]) or datetime.fromisoformat(row[1]) < _now():
            raise BindError("BIND_REJECTED")
        self.db.execute("INSERT INTO worker_sessions VALUES(?,?,?,?,?,1)", (worker_id, "CONTROL_PLANE", session_key, _now().isoformat(), "telegram-bootstrap"))
        self.db.execute("UPDATE bind_challenges SET used=1 WHERE worker_id=?", (worker_id,))
        self.db.commit()

    def try_consume(self, worker_id, text, telegram_chat_id, session_key):
        try:
            self.consume(worker_id, text, telegram_chat_id, session_key)
            return True
        except BindError:
            return False

    def resolve(self, worker_id):
        row = self.db.execute("SELECT session_key FROM worker_sessions WHERE worker_id=? AND verified=1", (worker_id,)).fetchone()
        if not row:
            raise BindError("SESSION_NOT_CONFIGURED")
        return row[0]
