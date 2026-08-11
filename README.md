# Codex Multi-Session Coordinator

`codex-multi-session-coordinator` is a small, reusable coordination tool for parallel Codex
sessions or other workers that share a mutable environment. It stores registrations, ordered work
requests, and one exclusive lease in DynamoDB. It does not send Codex-to-Codex messages; a human or
coordinator session reports communication failures in its own task window.

## Quick start

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
export CODEX_COORDINATOR_TABLE=codex-multi-session-coordinator-dev
export CODEX_COORDINATOR_SCOPE=aurora

codex-coordinator register --role coordinator --actor-id <coordinator-id> --title "Aurora coordinator"
codex-coordinator register --role worker --actor-id <worker-id> --title "Price ingestion"
```

See [docs/usage.md](docs/usage.md) for the worker/coordinator flow and [docs/design.md](docs/design.md)
for the failure model. The CDK stack is under `infra/cdk`.
