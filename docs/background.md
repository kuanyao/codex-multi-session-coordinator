# Background

Parallel coding sessions are useful when work is split by contracts, modules, or independent
issues. They become unsafe when sessions share a mutable development environment or a single
integration path. Conversation instructions alone cannot reliably answer:

- Which coordinator is current?
- Which worker owns the environment?
- Is a request queued or already granted?
- Is a lease stale or merely long-running?
- Can another session safely deploy or mutate fixtures?

This project provides a small durable control plane for those questions. It is intentionally
independent of Aurora and does not attempt to become a workflow engine, CI/CD system, or messaging
bus.

Codex task messaging remains an optional human-facing adapter. If a session cannot reach another
session, the tool reports durable state and the session reports the communication failure in its
own conversation window.
