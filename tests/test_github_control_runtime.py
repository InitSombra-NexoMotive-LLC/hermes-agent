import asyncio
import concurrent.futures
import json
import threading

from gateway.control_socket import GatewayControlServer
from gateway.github_control import SubmitCommand
from gateway.github_control_ledger import CommandLedger
from gateway.github_control_runtime import command_status_response, deliver_command
from gateway.run import _github_control_command_status_handler
from gateway.worker_session_registry import WorkerSessionRegistry


def command(command_id="cmd"):
    return {"command_id": command_id, "worker_id": "nvidia-control", "workstream": "CONTROL_PLANE"}


def prepare(ledger, command_id="cmd", state="QUEUED"):
    ledger.accept(command(command_id))
    if state in {"QUEUED", "RUNNING", "DELIVERED"}:
        ledger.transition(command_id, "QUEUED", ("ACCEPTED",))
    if state in {"RUNNING", "DELIVERED"}:
        ledger.transition(command_id, "RUNNING", ("QUEUED",))
    if state == "DELIVERED":
        ledger.transition(command_id, "DELIVERED", ("RUNNING",))


def wire(server, request):
    return json.loads(server.handle_request_line(json.dumps(request).encode()))


def test_real_status_wire_contract(tmp_path):
    ledger = CommandLedger(tmp_path / "state.db")
    server = GatewayControlServer(tmp_path, verb_handlers={"command-status": lambda request: command_status_response(request, ledger)})
    assert wire(server, {"verb": "command-status"})["result"]["status"] == "INVALID"
    assert wire(server, {"verb": "command-status", "command_id": 1})["result"]["status"] == "INVALID"
    assert wire(server, {"verb": "command-status", "command_id": "bad id"})["result"]["status"] == "INVALID"
    assert wire(server, {"verb": "command-status", "command_id": "x" * 129})["result"]["status"] == "INVALID"
    assert wire(server, {"verb": "command-status", "command_id": "none"})["result"]["status"] == "NOT_FOUND"
    for state in ("ACCEPTED", "QUEUED", "RUNNING", "DELIVERED"):
        command_id = f"state-{state.lower()}"
        prepare(ledger, command_id, state)
        nonterminal = wire(server, {"verb": "command-status", "command_id": command_id})["result"]
        assert nonterminal["state"] == state and nonterminal["sanitized_result"] is None
    ledger.complete("state-delivered", {"summary": "safe", "digest": "digest", "truncated": False})
    completed = wire(server, {"verb": "command-status", "command_id": "state-delivered"})["result"]
    assert completed["sanitized_result"] == "safe" and completed["result_digest"] == "digest"
    assert "payload" not in completed and "instructions" not in completed
    prepare(ledger, "failed")
    ledger.fail("failed", "EXECUTION_FAILED")
    assert wire(server, {"verb": "command-status", "command_id": "failed"})["result"]["reason"] == "EXECUTION_FAILED"


class Runner:
    def __init__(self, result=None, error=None): self.result, self.error, self.calls = result, error, 0
    async def _handle_message(self, event):
        self.calls += 1
        if self.error: raise self.error
        return self.result


def test_delivery_success_and_failure_scopes(tmp_path, monkeypatch):
    ledger = CommandLedger(tmp_path / "state.db")
    prepare(ledger)
    runner = Runner("token=secret")
    assert asyncio.run(deliver_command(runner, ledger, object(), command())) == "COMPLETED"
    status = ledger.status("cmd")
    assert status["state"] == "COMPLETED" and "secret" not in status["sanitized_result"]
    for name, runner_error, patch_target, expected in [
        ("execution", ValueError("raw execution secret"), None, "EXECUTION_FAILED"),
        ("sanitize", None, "sanitize_result", "RESULT_SANITIZATION_FAILED"),
        ("persist", None, "complete", "RESULT_PERSISTENCE_FAILED"),
    ]:
        cmd = f"{name}-cmd"; prepare(ledger, cmd)
        if patch_target == "sanitize_result": monkeypatch.setattr("gateway.github_control_runtime.sanitize_result", lambda value: (_ for _ in ()).throw(ValueError("raw sanitizer secret")))
        if patch_target == "complete": monkeypatch.setattr(ledger, "complete", lambda *args: False)
        assert asyncio.run(deliver_command(Runner("safe", runner_error), ledger, object(), command(cmd))) == expected
        assert ledger.status(cmd)["reason"] == expected and "secret" not in str(ledger.status(cmd))
        monkeypatch.undo()


def test_rejected_transitions_prevent_execution(tmp_path):
    ledger = CommandLedger(tmp_path / "state.db")
    runner = Runner("safe")
    prepare(ledger, "running-rejected", "ACCEPTED")
    assert asyncio.run(deliver_command(runner, ledger, object(), command("running-rejected"))) == "INVALID_LIFECYCLE_STATE"
    prepare(ledger, "delivered-rejected")
    original = ledger.transition
    ledger.transition = lambda cid, target, allowed: False if target == "DELIVERED" else original(cid, target, allowed)
    assert asyncio.run(deliver_command(runner, ledger, object(), command("delivered-rejected"))) == "INVALID_LIFECYCLE_STATE"
    assert runner.calls == 0


def test_concurrent_submit_executes_once_through_production_delivery(tmp_path):
    ledger = CommandLedger(tmp_path / "state.db")
    registry = WorkerSessionRegistry(tmp_path / "registry.db")
    token = registry.start("nvidia-control", "CONTROL_PLANE", "chat")
    registry.consume("nvidia-control", token, "chat", "fixture-session-nvidia")
    runner = Runner("safe")
    scheduled = []
    scheduled_lock = threading.Lock()
    def schedule(event, submitted, submitted_ledger):
        with scheduled_lock:
            scheduled.append(asyncio.run(deliver_command(runner, submitted_ledger, event, submitted)))
    submit = SubmitCommand(ledger, registry, schedule)
    payload = {**command("once"), "source": "github-control", "task_id": "CP-7", "command_type": "STATUS", "instructions": "status"}
    barrier = threading.Barrier(2)
    def invoke():
        barrier.wait()
        thread_registry = WorkerSessionRegistry(tmp_path / "registry.db")
        return SubmitCommand(ledger, thread_registry, schedule).submit(payload)["status"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: invoke(), range(2)))
    assert sorted(outcomes) == ["ACCEPTED", "DUPLICATE"]
    assert runner.calls == 1 and scheduled == ["COMPLETED"]


def test_first_status_request_uses_registered_production_seam(tmp_path):
    async def exercise():
        calls = []
        server = GatewayControlServer(
            tmp_path,
            verb_handlers={"command-status": lambda request: calls.append(request) or _github_control_command_status_handler(request, tmp_path / "ledger.db")},
        )
        assert await server.start()
        try:
            reader, writer = await asyncio.open_unix_connection(str(tmp_path / "gateway.sock"))
            writer.write(json.dumps({"verb": "command-status", "command_id": "first-status-request-001"}).encode() + b"\n")
            await writer.drain()
            response = json.loads(await reader.readline())
            writer.close(); await writer.wait_closed()
        finally:
            await server.stop()
        assert calls == [{"verb": "command-status", "command_id": "first-status-request-001"}]
        assert response.get("ok") is True and type(response.get("protocol")) is int and response["protocol"] == 1
        assert isinstance(response.get("result"), dict) and response["result"].get("status") == "NOT_FOUND"
    asyncio.run(exercise())
