"""Shared production runtime for local GitHub-control delivery and status."""
from __future__ import annotations

import re

from gateway.github_control_sanitize import sanitize_result

_COMMAND_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")


def command_status_response(request: dict, ledger) -> dict:
    command_id = request.get("command_id") if isinstance(request, dict) else None
    if not isinstance(command_id, str) or not _COMMAND_ID.fullmatch(command_id):
        return {"status": "INVALID"}
    record = ledger.status(command_id)
    return {"status": "NOT_FOUND"} if record is None else {"status": "OK", **record}


async def deliver_command(runner, ledger, event, command) -> str:
    command_id = command["command_id"]
    if not ledger.transition(command_id, "RUNNING", ("QUEUED",)):
        ledger.fail(command_id, "INVALID_LIFECYCLE_STATE")
        return "INVALID_LIFECYCLE_STATE"
    if not ledger.transition(command_id, "DELIVERED", ("RUNNING",)):
        ledger.fail(command_id, "INVALID_LIFECYCLE_STATE")
        return "INVALID_LIFECYCLE_STATE"
    try:
        result = await runner._handle_message(event)
    except Exception:
        ledger.fail(command_id, "EXECUTION_FAILED")
        return "EXECUTION_FAILED"
    try:
        sanitized = sanitize_result(result)
    except Exception:
        ledger.fail(command_id, "RESULT_SANITIZATION_FAILED")
        return "RESULT_SANITIZATION_FAILED"
    try:
        if not ledger.complete(command_id, sanitized):
            ledger.fail(command_id, "RESULT_PERSISTENCE_FAILED")
            return "RESULT_PERSISTENCE_FAILED"
    except Exception:
        ledger.fail(command_id, "RESULT_PERSISTENCE_FAILED")
        return "RESULT_PERSISTENCE_FAILED"
    return "COMPLETED"
