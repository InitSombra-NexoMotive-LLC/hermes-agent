#!/usr/bin/env python3
"""Read-only local identity-candidate report for NVIDIA binding.

Never prints Telegram identifiers or challenge material.  Candidates are tied to
the latest failed bind by an exact message-to-pending-challenge match, when the
normal gateway message ledger retained that message.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

HERMES_HOME = Path("/home/rodrigo/.hermes")


def fingerprint(value: object | None) -> str:
    if value is None:
        return "NONE"
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12].upper()


def main() -> None:
    control = sqlite3.connect(HERMES_HOME / "github-control.sqlite3")
    pending = control.execute(
        "SELECT challenge, telegram_chat_id FROM bind_challenges "
        "WHERE worker_id = ? ORDER BY expires_at DESC LIMIT 1",
        ("nvidia-control",),
    ).fetchone()
    challenge = pending[0] if pending else None
    configured = pending[1] if pending else None

    state = sqlite3.connect(HERMES_HOME / "state.db")
    rows = state.execute(
        """
        SELECT s.chat_id, s.last_activity_at, s.profile_name, s.session_key,
               s.source, s.title,
               EXISTS(
                 SELECT 1 FROM messages m
                 WHERE m.session_id = s.id AND m.role = 'user' AND m.content = ?
               ) AS observed_failed_bind
        FROM sessions s
        WHERE s.source = 'telegram' AND s.chat_id IS NOT NULL
        ORDER BY s.last_activity_at DESC
        LIMIT 12
        """,
        (challenge,),
    ).fetchall()
    candidates = []
    for chat_id, last_activity, profile, session_key, source, title, observed in rows:
        candidates.append(
            {
                "candidate_fingerprint": fingerprint(chat_id),
                "observed_during_failed_bind_attempt": bool(observed),
                "last_activity": last_activity,
                "profile": profile or "default",
                "source": source,
                "title_present": bool(title),
                "session_metadata_present": bool(session_key),
            }
        )
    print(json.dumps({
        "configured_identity_fingerprint": fingerprint(configured),
        "failed_bind_identity_captured": any(
            item["observed_during_failed_bind_attempt"] for item in candidates
        ),
        "candidates": candidates,
    }, indent=2))


if __name__ == "__main__":
    main()
