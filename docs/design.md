# Design

## Scope

The coordination scope and resource name are caller-provided strings. One DynamoDB table can hold
many scopes. A caller can use `aurora` as a scope and `aurora/dev-integration` as a resource in its
own policy metadata, while another project uses the same table for a different resource.

The first implementation keeps one lease per scope. Resource-specific leases can be added when a
second concrete use case requires them; the table schema already leaves room for a bounded resource
field in requests and lease records.

## Records

All records use `scope` as the partition key and `record_id` as the sort key:

- `COORDINATOR`: exactly one current registration. Registering a new coordinator replaces the old
  one and changes its token/generation.
- `WORKER#<actor-id>`: worker registration and latest durable status.
- `REQUEST#<request-id>`: queued or granted request with bounded metadata.
- `LEASE`: current state, owner, opaque lease token, fencing number, phase, expiry, and evidence.

The table uses on-demand capacity, AWS-managed encryption, point-in-time recovery, and TTL for
registration/request cleanup. TTL is not used to authorize lease takeover.

## Conditional transitions

Lease grant is conditional on the current coordinator token, queued request state, and a free lease.
The lease token and monotonically increasing fencing number identify the grant. Release and heartbeat
require the current owner and exact lease token. A stale worker cannot release a replacement lease.

Coordinator replacement is deliberate. The new coordinator can inspect the old lease and put it into
explicit recovery. It cannot silently take over a held lease merely because its expiration time has
passed.

## Failure modes and controls

| Failure | Control |
| --- | --- |
| Two coordinators register | Last registration replaces the old token; stale commands fail. |
| Two grants race | Conditional lease write and request state prevent a second grant. |
| Worker crashes | Lease becomes stale; coordinator must inspect and explicitly recover. |
| Worker loses messaging | Durable status remains queryable; worker reports the problem locally. |
| Coordinator crashes | New registration replaces it; recovery is explicit and verified. |
| Command bypasses guard | The CLI cannot prevent every external AWS mutation; policy and IAM remain required. |
| Lease expires during a long test | Expiry blocks new grants; it does not authorize takeover. |
| DynamoDB is unavailable | Commands fail closed; no mutation authorization is inferred. |

## Why no S3

Lease state and conditional ownership are small, mutable, and queried by key. DynamoDB is the direct
fit. S3 can be added later for an append-only event archive if audit retention or analysis requires
it; it is not part of the first control plane.

## Why no Codex messaging client

The Codex desktop task-messaging capability is an application integration, not a stable public AWS
or shell interface for this project. The CLI exposes durable state and fails clearly. A coordinator
session can use its available task tools to notify or resume workers, while the project remains
usable by humans and non-Codex workers.
