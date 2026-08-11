# Usage

Set the table and namespace for every command:

```bash
export CODEX_COORDINATOR_TABLE=codex-multi-session-coordinator-dev
export CODEX_COORDINATOR_SCOPE=aurora
```

## Register

Coordinator registration replaces any prior coordinator registration. Save the returned token in
the coordinator session only; it is not committed to the repository.

```bash
codex-coordinator register \
  --role coordinator \
  --actor-id <coordinator-task-id> \
  --title "Aurora integration coordinator"
```

Register a worker:

```bash
codex-coordinator register \
  --role worker \
  --actor-id <worker-task-id> \
  --title "Price ingestion"
```

## Request and grant

The worker queues a request:

```bash
codex-coordinator request \
  --actor-id <worker-task-id> \
  --token <worker-registration-token> \
  --summary "Deploy and validate PR 501"
```

The coordinator inspects status and grants the request:

```bash
codex-coordinator status --pretty
codex-coordinator grant \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --request-id <request-id> \
  --ttl-seconds 3600
```

The grant output contains an opaque `lease_token`. The worker must use that token for all guarded
operations. A worker that is told to wait ends its turn; it does not poll continuously.

Registration tokens are only printed by `register`; `status` deliberately redacts registration and
lease tokens.

## Status and release

```bash
codex-coordinator heartbeat \
  --actor-id <worker-task-id> \
  --token <worker-registration-token> \
  --phase testing-dev \
  --message "integration assertions running" \
  --lease-token <lease-token>

codex-coordinator status --pretty
```

Release only after the coordinator policy says the shared environment is safe:

```bash
codex-coordinator release \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  --phase beta-green \
  --evidence '{"merge_revision":"...","beta_run":"..."}'
```

## Guard a command

The guard checks the current owner and token before starting the child process:

```bash
codex-coordinator guard \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  -- scripts/build.sh
```

The guard is a fail-closed convenience, not a replacement for IAM. A process already accepted by
AWS cannot be reliably cancelled by this wrapper.

## Recovery

Expiration never authorizes automatic takeover. The coordinator first inspects the environment and
then marks recovery required:

```bash
codex-coordinator recover \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --reason "worker disappeared after dev deployment; AWS state verified"
```

After inspecting the environment, explicitly clear the block with evidence:

```bash
codex-coordinator complete-recovery \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --evidence '{"verified_by":"...","environment":"clean"}'
```

There is no automatic takeover based only on expiry.
