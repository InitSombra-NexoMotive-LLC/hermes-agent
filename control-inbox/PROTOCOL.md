# NVIDIA Hermes GitHub control inbox

This branch is a command transport for the `nvidia-control` / `CONTROL_PLANE` Hermes worker.

## Security boundary

- Authorized GitHub actor: `InitSombra-NexoMotive-LLC` (GitHub user ID `239685310`).
- This repository is public. Never commit credentials, tokens, private keys, raw Telegram identifiers, raw Hermes session keys, customer data, or other secrets.
- The NVIDIA relay must independently query GitHub's API for each command commit and require the exact authorized actor login and numeric user ID above.
- The relay must reject force-pushed or rewritten history, commands outside the allowlist, duplicate command IDs, malformed payloads, multiple command files in one commit, and commits that modify anything outside `control-inbox/commands/`.
- The relay may submit only high-level Hermes control commands through the local filesystem control socket. It must never execute command text as a shell command.
- Results must be sanitized and written only under `control-inbox/results/`.

## Command path

`control-inbox/commands/<command_id>.json`

Required JSON fields:

```json
{
  "protocol_version": 1,
  "command_id": "unique-id",
  "source": "github-control",
  "worker_id": "nvidia-control",
  "workstream": "CONTROL_PLANE",
  "task_id": "status-or-work-item",
  "command_type": "STATUS",
  "instructions": "High-level instructions for Hermes"
}
```

Allowed command types:

- `STATUS`
- `CONTINUE`
- `RECOVER`
- `RECHECK`
- `START_TASK`
- `PAUSE`
- `RESUME`
- `STOP_AFTER_SAFE_POINT`

## Result path

`control-inbox/results/<command_id>.json`

A result records the command ID, lifecycle state, sanitized summary, timestamps, worker/workstream, and non-secret evidence. Lifecycle states are `ACCEPTED`, `QUEUED`, `RUNNING`, `DELIVERED`, `COMPLETED`, or `FAILED`.

## Current state

`TRANSPORT_CREATED_NOT_CONNECTED`

The branch exists, but it cannot reach the NVIDIA machine until a reviewed local relay is installed and bound to Hermes's protected Unix control socket.
