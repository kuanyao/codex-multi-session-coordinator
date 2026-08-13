# Usage

Run these commands from the repository root. Every example uses
`./.venv/bin/codex-coordinator` explicitly so it does not depend on virtual-environment activation
or a shell's `PATH`. Before a stateful operation, a safe local preflight is:

```bash
test -x ./.venv/bin/codex-coordinator
./.venv/bin/codex-coordinator --help >/dev/null
```

If the executable is absent, create/install the environment before retrying; do not substitute a
different global executable:

```bash
python3.13 -m venv .venv
./.venv/bin/python -m pip install -e .
```

Set the table and namespace for every command:

```bash
export CODEX_COORDINATOR_TABLE=codex-multi-session-coordinator-dev
export CODEX_COORDINATOR_SCOPE=aurora
```

The global options `--table`, `--region`, `--scope`, and `--json` are accepted either before or
after the subcommand. For `guard`, place `--` after all coordinator options and immediately before
the child command so child options are passed through unchanged:

```bash
./.venv/bin/codex-coordinator guard \
  --table codex-multi-session-coordinator-dev \
  --scope aurora \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  -- <child-command> --child-option
```

## Register

Coordinator registration replaces any prior coordinator registration. Save the returned token in
the coordinator session only; it is not committed to the repository. Save the returned generation
with it so status can identify which credential is current after context loss or replacement.

```bash
./.venv/bin/codex-coordinator register \
  --role coordinator \
  --actor-id <coordinator-task-id> \
  --title "Aurora integration coordinator"
```

Register a worker:

```bash
./.venv/bin/codex-coordinator register \
  --role worker \
  --actor-id <worker-task-id> \
  --title "Price ingestion"
```

## Request and grant

The worker queues a request:

```bash
./.venv/bin/codex-coordinator request \
  --actor-id <worker-task-id> \
  --registration-token <worker-registration-token> \
  --summary "Deploy and validate PR 501"
```

The coordinator inspects status and grants the request:

```bash
./.venv/bin/codex-coordinator status --pretty
./.venv/bin/codex-coordinator grant \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --request-id <request-id> \
  --ttl-seconds 3600
```

The grant output contains an opaque `lease_token`. The worker must use that token for all guarded
operations. A worker that is told to wait ends its turn; it does not poll continuously.

Registration tokens are only printed by `register`; `status` deliberately redacts registration and
lease tokens.

### Recover a lost coordinator registration token

If a coordinator token is lost or a heartbeat reports it as stale, first use `status --pretty` and
record the current coordinator actor/generation and the complete lease identity. Registering the
same coordinator actor replaces only the `COORDINATOR` record; it does not release, renew, recover,
or otherwise modify a held lease, its owner, or its fencing number.

Run `register` exactly once and privately retain both returned `token` and `generation`:

```bash
./.venv/bin/codex-coordinator register \
  --role coordinator \
  --actor-id <coordinator-task-id> \
  --title "Aurora integration coordinator"
```

Re-read `status --pretty` and confirm the coordinator has the returned generation while the entire
lease identity is unchanged. Then heartbeat with the newly returned token and no lease token:

```bash
./.venv/bin/codex-coordinator heartbeat \
  --actor-id <coordinator-task-id> \
  --registration-token <new-coordinator-registration-token> \
  --phase <current-phase> \
  --message "coordinator registration recovered"
```

If the generation changes again unexpectedly, stop instead of repeatedly registering: another
coordinator registration is replacing this one. A coordinator registration heartbeat never proves
or extends worker lease ownership; the worker continues using its existing registration and lease
tokens.

## Status and release

```bash
./.venv/bin/codex-coordinator heartbeat \
  --actor-id <worker-task-id> \
  --registration-token <worker-registration-token> \
  --phase testing-dev \
  --message "integration assertions running" \
  --lease-token <lease-token>

./.venv/bin/codex-coordinator status --pretty
```

`--phase` is the preferred heartbeat option. `--state` is accepted as a compatibility alias and
stores the same workflow-phase value. It does not directly set the registration's durable `state`,
which the coordinator manages as `registered` or `active`.

`--registration-token` is the preferred option for `heartbeat` and `request`; the shorter `--token`
remains accepted for compatibility. Registration, coordinator, and lease tokens are distinct and
must not be substituted for one another.

Treat actor IDs and tokens as exact opaque values: copy or reuse the machine-returned values from
`register` and `grant`; never reconstruct them from a visible prefix or suffix. If registration
validation fails, a lease-bearing heartbeat makes read-only registration lookups but performs no
write and does not inspect the lease token. The error identifies the submitted actor and explicitly
says when lease validation was not reached. After registration succeeds, lease errors distinguish
an actor mismatch from a stale token without printing the token or the current owner.

A heartbeat without `--lease-token` only reports registration liveness. With `--lease-token`, it
also proves that the lease is held by the actor and has not expired. The registration and lease
heartbeat timestamps update atomically. An expired held lease is not renewed and neither timestamp
advances; the command instead reports that explicit coordinator recovery is required.

## Extend a current lease

Worker heartbeat never renews a lease. Before expiry, only the current coordinator can extend the
same held lease. The caller supplies the complete status identity as compare-and-swap inputs; the
operation refuses expired, free, recovery, changed-identity, stale-token, and replaced-generation
cases. Durations are measured from the operation time, must move `expires_at` forward, and are
bounded to 60 through 86400 seconds.

```bash
./.venv/bin/codex-coordinator extend \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --coordinator-generation <coordinator-generation> \
  --owner-id <current-owner-id> \
  --request-id <current-request-id> \
  --fencing <current-fencing> \
  --expected-expires-at <current-expires-at> \
  --ttl-seconds 21600 \
  --reason "active integration repair cannot safely finish before expiry" \
  --evidence '{"external_state":"contained","queue_checked":true}'
```

The transaction checks the current coordinator token/generation, granted request, and exact held
lease identity. It changes only lease expiry and last-extension audit pointers, and writes an
append-only `EXTENSION#...` record. It does not change owner, request, fencing, grant time, request
state/order, or worker heartbeat time. Re-read status after success and verify those invariants.

Release only after the coordinator policy says the shared environment is safe:

```bash
./.venv/bin/codex-coordinator release \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  --phase beta-green \
  --evidence '{"merge_revision":"...","beta_run":"..."}'
```

After the command succeeds, the worker must explicitly notify the coordinator (or the owning
task) with `RELEASE_INTEGRATION_LEASE`, including the lease ID, final main/dev/beta revisions,
successful workflow URL, test and cleanup results, and confirmation that no exclusive operation
remains. The release command is the durable state transition; the notification closes the
conversational handoff. If the notification channel is unavailable, report that failure and leave
the durable release evidence queryable for coordinator verification.

## Guard a command

The guard checks the current owner and token before starting the child process:

```bash
./.venv/bin/codex-coordinator guard \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  -- scripts/build.sh
```

The guard is a fail-closed convenience, not a replacement for IAM. A process already accepted by
AWS cannot be reliably cancelled by this wrapper.

### Separate coordinator and child AWS credentials

The coordinator's DynamoDB lease check and the guarded child may require different AWS accounts.
Never prefix the whole coordinator command with the child's `AWS_PROFILE`: boto3 would use that
profile for the parent DynamoDB lookup before the child is authorized. Use `--child-aws-profile`
before `--`; guard copies the parent environment only after the lease check succeeds and changes
`AWS_PROFILE` in that child copy. The coordinator process environment is not changed.

One guarded Route53 mutation using management-account credentials:

```bash
./.venv/bin/codex-coordinator guard \
  --table codex-multi-session-coordinator-dev \
  --region us-east-1 \
  --scope aurora \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  --child-aws-profile aurora-management \
  -- \
  aws route53 change-resource-record-sets \
    --hosted-zone-id <hosted-zone-id> \
    --change-batch file://<change-batch.json>
```

Capture the returned change ID only after that command succeeds. A waiter is a separate guarded
child and uses the same child-only profile pattern:

```bash
./.venv/bin/codex-coordinator guard \
  --table codex-multi-session-coordinator-dev \
  --region us-east-1 \
  --scope aurora \
  --actor-id <worker-task-id> \
  --lease-token <lease-token> \
  --child-aws-profile aurora-management \
  -- \
  aws route53 wait resource-record-sets-changed --id <non-empty-change-id>
```

Do not run the waiter or `get-change` when the mutation failed or the change ID is empty. A missing
coordinator DynamoDB table now produces a concise parent-credential diagnostic instead of a
botocore traceback.

## Recovery

Expiration never makes a lease free and never authorizes automatic takeover. It disables worker
heartbeat and guarded commands while the durable `held` record continues to block new grants. The
coordinator first confirms with `status --pretty` that the expected fencing/owner record is still
held, inspects the external environment for in-flight mutations, and then marks recovery required:

```bash
./.venv/bin/codex-coordinator recover \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --reason "worker disappeared after dev deployment; AWS state verified"
```

After inspecting the environment, explicitly clear the block with evidence:

```bash
./.venv/bin/codex-coordinator complete-recovery \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --evidence '{"verified_by":"...","environment":"clean"}'
```

There is no automatic takeover based only on expiry.

If the same transaction owner must continue after expiry, do not use `complete-recovery`, which
makes the lease free. First verify external containment, then atomically enter recovery with the
complete prior identity:

```bash
./.venv/bin/codex-coordinator recover-exact \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --coordinator-generation <coordinator-generation> \
  --owner-id <current-owner-id> \
  --request-id <current-request-id> \
  --fencing <current-fencing> \
  --expected-expires-at <expired-expires-at> \
  --reason "expired while the same transaction retained contained recovery responsibility" \
  --evidence '{"external_state":"contained","queued_request_untouched":true}'
```

After re-reading status and confirming `recovery_required` with the identical owner/request/fence,
resume that same granted request:

```bash
./.venv/bin/codex-coordinator resume-recovery \
  --coordinator-id <coordinator-task-id> \
  --coordinator-token <coordinator-registration-token> \
  --coordinator-generation <coordinator-generation> \
  --owner-id <current-owner-id> \
  --request-id <current-request-id> \
  --fencing <current-fencing> \
  --expected-expires-at <expired-expires-at> \
  --ttl-seconds 21600 \
  --reason "same owner must repair and finish the interrupted transaction" \
  --evidence '{"external_state":"contained","recovery_reviewed":true}'
```

The resume transaction preserves owner, request, original grant time, and queue order; increments
fencing; rotates the lease token; updates the granted request with the new fence/token; and writes
an append-only `RECOVERY_RESUME#...` audit record. The coordinator must deliver the new token and
fencing value privately to the same worker. The prior token/fence remains invalid.
