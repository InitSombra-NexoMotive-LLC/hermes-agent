# STATUS-only GitHub control relay

The relay accepts only signed-by-API `STATUS` command commits on `control/nvidia-command-inbox`, and uses the existing local Unix gateway socket. It does not listen on TCP and never executes command instructions as shell input.

## Configuration

Create `/home/rodrigo/.config/hermes/github-control-relay.json` mode `0600` with non-secret paths and the pinned initial transport SHA:

```json
{"remote":"https://github.com/InitSombra-NexoMotive-LLC/hermes-agent.git","branch":"control/nvidia-command-inbox","trusted_head":"<40-lowercase-hex-sha>","workspace":"/mnt/ssd-cloud/hermes-github-control-transport","state_db":"/mnt/ssd-cloud/hermes-github-control-state/relay.db","gateway_socket":"/home/rodrigo/.hermes/gateway.sock","timeout":30,"retries":3,"poll_seconds":30}
```

GitHub authentication is inherited from the existing `gh` credential manager; do not put credentials in this file or unit.

## Required configuration and recovery

The mode-0600 JSON configuration requires `remote`, `branch`, `trusted_head`, `workspace`, `state_db`, and `gateway_socket`; optional bounded `timeout`, `retries`, and `poll_seconds` control retry behavior. The workspace must be empty or a Git repository whose `origin` exactly matches `remote`. The relay freezes with a permanent integrity exit code on history, actor, schema, or workspace integrity failures; investigate and reset only with a newly verified transport history. Temporary GitHub or Unix-socket outages are retried on later polls without deleting state. Rollback never deletes the audit state database or transport workspace.

## Install (not performed by this candidate)

1. Copy `systemd/hermes-github-control-relay.service` to `~/.config/systemd/user/`.
2. Create dedicated mode-0700 workspace and state directories:
   `/mnt/ssd-cloud/hermes-github-control-transport` and `/mnt/ssd-cloud/hermes-github-control-state`.
3. Verify the gateway socket is already mode `0600` and owned by `rodrigo`.
4. Run `systemctl --user daemon-reload` then `systemctl --user enable --now hermes-github-control-relay.service`.
5. Check only with `systemctl --user status hermes-github-control-relay.service` and journal metadata; logs must not contain commands or secrets.

## Stop and rollback

- Stop: `systemctl --user stop hermes-github-control-relay.service`.
- Disable: `systemctl --user disable hermes-github-control-relay.service`.
- Roll back source by checking out the prior verified feature SHA, run focused tests, and restart only after explicit authorization. Preserve the relay state database and transport workspace for audit/recovery; do not delete them as part of rollback.
