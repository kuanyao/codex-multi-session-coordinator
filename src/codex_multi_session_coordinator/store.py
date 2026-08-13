"""DynamoDB persistence and conditional lease transitions."""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key


class CoordinationError(RuntimeError):
    """A caller-visible coordination failure."""


MIN_LEASE_TTL_SECONDS = 60
MAX_LEASE_TTL_SECONDS = 86400


@dataclass(frozen=True)
class Registration:
    scope: str
    actor_id: str
    role: str
    token: str
    generation: str


def now() -> int:
    return int(time.time())


def record_id(kind: str, actor_id: str | None = None) -> str:
    return kind if actor_id is None else f"{kind}#{actor_id}"


class CoordinatorStore:
    def __init__(self, table_name: str, region: str | None = None, table: Any | None = None):
        self._resource = boto3.resource("dynamodb", region_name=region)
        self.table = table or self._resource.Table(table_name)
        self.table_name = table_name

    def _get(self, scope: str, key: str) -> dict[str, Any] | None:
        return self.table.get_item(Key={"scope": scope, "record_id": key}).get("Item")

    @staticmethod
    def _validate_lease_ttl(ttl_seconds: int) -> None:
        if not MIN_LEASE_TTL_SECONDS <= ttl_seconds <= MAX_LEASE_TTL_SECONDS:
            raise CoordinationError(
                f"lease duration must be between {MIN_LEASE_TTL_SECONDS} and "
                f"{MAX_LEASE_TTL_SECONDS} seconds"
            )

    def register(self, scope: str, actor_id: str, role: str, title: str) -> Registration:
        if role not in {"worker", "coordinator"}:
            raise CoordinationError("role must be worker or coordinator")
        token = str(uuid.uuid4())
        generation = str(uuid.uuid4())
        timestamp = now()
        key = record_id("COORDINATOR") if role == "coordinator" else record_id("WORKER", actor_id)
        item = {
            "scope": scope,
            "record_id": key,
            "actor_id": actor_id,
            "role": role,
            "title": title,
            "token": token,
            "generation": generation,
            "registered_at": timestamp,
            "last_seen_at": timestamp,
            "state": "registered",
            "ttl": timestamp + 86400,
        }
        # Coordinator registration intentionally replaces the prior coordinator. A generation
        # token makes stale coordinator commands fail after replacement.
        self.table.put_item(Item=item)
        return Registration(scope, actor_id, role, token, generation)

    def recover_worker_registration(
        self,
        scope: str,
        actor_id: str,
        lease_token: str,
        expected_generation: str,
        request_id: str,
        fencing: int,
        expected_expires_at: int,
        title: str,
        reason: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Rotate a stale worker registration while preserving one exact active lease."""
        if not reason.strip():
            raise CoordinationError("worker registration recovery reason is required")
        if not isinstance(evidence, dict):
            raise CoordinationError("worker registration recovery evidence must be a JSON object")
        worker_key = record_id("WORKER", actor_id)
        worker = self._get(scope, worker_key)
        lease = self._get(scope, "LEASE")
        request = self._get(scope, record_id("REQUEST", request_id))
        if (
            not worker
            or worker.get("actor_id") != actor_id
            or worker.get("role") != "worker"
            or worker.get("generation") != expected_generation
        ):
            raise CoordinationError("worker registration generation changed; recovery refused")
        if (
            not lease
            or lease.get("state") != "held"
            or lease.get("owner_id") != actor_id
            or lease.get("lease_token") != lease_token
            or lease.get("request_id") != request_id
            or int(lease.get("fencing", -1)) != fencing
            or int(lease.get("expires_at", -1)) != expected_expires_at
        ):
            raise CoordinationError("active lease identity or token changed; registration recovery refused")
        if not request or request.get("state") != "granted" or request.get("actor_id") != actor_id:
            raise CoordinationError("granted request identity changed; registration recovery refused")
        timestamp = now()
        if expected_expires_at <= timestamp:
            raise CoordinationError(
                f"lease expired at {expected_expires_at}; coordinator recovery is required first"
            )
        token = str(uuid.uuid4())
        generation = str(uuid.uuid4())
        recovery_id = str(uuid.uuid4())
        audit_item = {
            "scope": scope,
            "record_id": record_id("REGISTRATION_RECOVERY", recovery_id),
            "recovery_id": recovery_id,
            "actor_id": actor_id,
            "previous_generation": expected_generation,
            "generation": generation,
            "request_id": request_id,
            "fencing": fencing,
            "expires_at": expected_expires_at,
            "recovered_at": timestamp,
            "reason": reason,
            "evidence": evidence,
        }
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": worker_key},
                    "UpdateExpression": (
                        "SET #token = :token, #generation = :generation, #title = :title, "
                        "registered_at = :at, last_seen_at = :at, #state = :registered, "
                        "#ttl = :ttl, previous_generation = :expected, "
                        "registration_recovery_id = :recovery_id"
                    ),
                    "ConditionExpression": (
                        "actor_id = :actor AND #role = :worker AND #generation = :expected"
                    ),
                    "ExpressionAttributeNames": {
                        "#token": "token",
                        "#generation": "generation",
                        "#title": "title",
                        "#state": "state",
                        "#ttl": "ttl",
                        "#role": "role",
                    },
                    "ExpressionAttributeValues": {
                        ":token": token,
                        ":generation": generation,
                        ":title": title,
                        ":at": timestamp,
                        ":registered": "registered",
                        ":ttl": timestamp + 86400,
                        ":expected": expected_generation,
                        ":recovery_id": recovery_id,
                        ":actor": actor_id,
                        ":worker": "worker",
                    },
                }},
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "LEASE"},
                    "ConditionExpression": (
                        "#state = :held AND owner_id = :actor AND lease_token = :lease "
                        "AND request_id = :request AND fencing = :fence "
                        "AND #expires = :expected_expires AND #expires > :now"
                    ),
                    "ExpressionAttributeNames": {"#state": "state", "#expires": "expires_at"},
                    "ExpressionAttributeValues": {
                        ":held": "held",
                        ":actor": actor_id,
                        ":lease": lease_token,
                        ":request": request_id,
                        ":fence": fencing,
                        ":expected_expires": expected_expires_at,
                        ":now": timestamp,
                    },
                }},
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": record_id("REQUEST", request_id)},
                    "ConditionExpression": "#state = :granted AND actor_id = :actor AND fencing = :fence",
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":granted": "granted",
                        ":actor": actor_id,
                        ":fence": fencing,
                    },
                }},
                {"Put": {
                    "TableName": self.table_name,
                    "Item": audit_item,
                    "ConditionExpression": "attribute_not_exists(record_id)",
                }},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError(
                "worker registration recovery conflicted with a changed registration, request, or lease"
            ) from exc
        return {
            **audit_item,
            "role": "worker",
            "token": token,
        }

    def heartbeat(
        self,
        scope: str,
        actor_id: str,
        token: str,
        phase: str,
        message: str,
        lease_token: str | None = None,
    ) -> dict[str, Any]:
        coordinator = self._get(scope, "COORDINATOR")
        role_key = "COORDINATOR" if coordinator and coordinator.get("actor_id") == actor_id else record_id("WORKER", actor_id)
        item = self._get(scope, role_key)
        lease_note = "; lease token was not checked" if lease_token else ""
        if not item:
            raise CoordinationError(
                f"registration is missing for actor {actor_id!r}; verify --actor-id{lease_note}"
            )
        if item.get("token") != token:
            generation = item.get("generation", "unknown")
            raise CoordinationError(
                f"registration token is stale; current generation is {generation}{lease_note}"
            )
        timestamp = now()
        registration_update = {
            "Key": {"scope": scope, "record_id": role_key},
            "UpdateExpression": "SET last_seen_at = :seen, #phase = :phase, #message = :message, #state = :state, #ttl = :ttl",
            "ExpressionAttributeNames": {
                "#phase": "phase",
                "#message": "message",
                "#state": "state",
                "#token": "token",
                "#ttl": "ttl",
            },
            "ExpressionAttributeValues": {
                ":seen": timestamp,
                ":phase": phase,
                ":message": message,
                ":state": "active",
                ":ttl": timestamp + 86400,
                ":token": token,
            },
            "ConditionExpression": "#token = :token",
        }
        if lease_token:
            self.require_active_lease(scope, actor_id, lease_token, timestamp)
            client = self.table.meta.client
            lease_update = {
                "Key": {"scope": scope, "record_id": "LEASE"},
                "UpdateExpression": "SET last_heartbeat_at = :seen",
                "ExpressionAttributeValues": {
                    ":seen": timestamp,
                    ":token": lease_token,
                    ":owner": actor_id,
                    ":held": "held",
                },
                "ConditionExpression": "lease_token = :token AND owner_id = :owner AND #state = :held AND #expires > :seen",
                "ExpressionAttributeNames": {"#state": "state", "#expires": "expires_at"},
            }
            try:
                client.transact_write_items(TransactItems=[
                    {"Update": {"TableName": self.table_name, **registration_update}},
                    {"Update": {"TableName": self.table_name, **lease_update}},
                ])
            except client.exceptions.TransactionCanceledException as exc:
                self.require_active_lease(scope, actor_id, lease_token, now())
                raise CoordinationError("heartbeat conflicted with a changed registration or lease") from exc
        else:
            self.table.update_item(**registration_update)
        return self.status(scope)

    def request(self, scope: str, actor_id: str, token: str, summary: str, metadata: dict[str, Any] | None = None) -> str:
        worker = self._get(scope, record_id("WORKER", actor_id))
        if not worker or worker.get("token") != token:
            raise CoordinationError("worker registration is missing or stale")
        request_id = str(uuid.uuid4())
        timestamp = now()
        self.table.put_item(Item={
            "scope": scope,
            "record_id": record_id("REQUEST", request_id),
            "request_id": request_id,
            "actor_id": actor_id,
            "summary": summary,
            "metadata": metadata or {},
            "state": "queued",
            "created_at": timestamp,
            "ttl": timestamp + 7 * 86400,
        }, ConditionExpression="attribute_not_exists(record_id)")
        return request_id

    def grant(self, scope: str, coordinator_id: str, coordinator_token: str, request_id: str, ttl_seconds: int) -> dict[str, Any]:
        self._validate_lease_ttl(ttl_seconds)
        coordinator = self._get(scope, "COORDINATOR")
        request = self._get(scope, record_id("REQUEST", request_id))
        lease = self._get(scope, "LEASE")
        if not coordinator or coordinator.get("actor_id") != coordinator_id or coordinator.get("token") != coordinator_token:
            raise CoordinationError("coordinator registration is missing or stale")
        if not request or request.get("state") != "queued":
            raise CoordinationError("request is missing or not queued")
        if lease and lease.get("state") not in {None, "free"}:
            raise CoordinationError("a lease is already held or recovery is required")
        token = str(uuid.uuid4())
        fence = int((lease or {}).get("fencing", 0)) + 1
        timestamp = now()
        lease_item = {
            "scope": scope,
            "record_id": "LEASE",
            "state": "held",
            "owner_id": request["actor_id"],
            "lease_token": token,
            "fencing": fence,
            "request_id": request_id,
            "purpose": request["summary"],
            "granted_at": timestamp,
            "expires_at": timestamp + ttl_seconds,
            "last_heartbeat_at": timestamp,
        }
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"ConditionCheck": {"TableName": self.table_name, "Key": {"scope": scope, "record_id": "COORDINATOR"}, "ConditionExpression": "actor_id = :actor AND #token = :token", "ExpressionAttributeNames": {"#token": "token"}, "ExpressionAttributeValues": {":actor": coordinator_id, ":token": coordinator_token}}},
                {"Put": {"TableName": self.table_name, "Item": lease_item, "ConditionExpression": "attribute_not_exists(record_id) OR #state = :free", "ExpressionAttributeNames": {"#state": "state"}, "ExpressionAttributeValues": {":free": "free"}}},
                {"Update": {"TableName": self.table_name, "Key": {"scope": scope, "record_id": record_id("REQUEST", request_id)}, "UpdateExpression": "SET #state = :granted, lease_token = :lease, fencing = :fence, granted_at = :granted_at", "ExpressionAttributeNames": {"#state": "state"}, "ExpressionAttributeValues": {":queued": "queued", ":granted": "granted", ":lease": token, ":fence": fence, ":granted_at": timestamp}, "ConditionExpression": "#state = :queued"}},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError("grant conflicted with a changed coordinator, request, or lease") from exc
        return lease_item

    def extend(
        self,
        scope: str,
        coordinator_id: str,
        coordinator_token: str,
        coordinator_generation: str,
        owner_id: str,
        request_id: str,
        fencing: int,
        expected_expires_at: int,
        ttl_seconds: int,
        reason: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Extend one current, unexpired lease without changing its ownership identity."""
        self._validate_lease_ttl(ttl_seconds)
        if not reason.strip():
            raise CoordinationError("extension reason is required")
        if not isinstance(evidence, dict):
            raise CoordinationError("extension evidence must be a JSON object")
        coordinator = self._get(scope, "COORDINATOR")
        lease = self._get(scope, "LEASE")
        request = self._get(scope, record_id("REQUEST", request_id))
        if (
            not coordinator
            or coordinator.get("actor_id") != coordinator_id
            or coordinator.get("token") != coordinator_token
            or coordinator.get("generation") != coordinator_generation
        ):
            raise CoordinationError("coordinator registration token or generation is stale")
        if not lease or lease.get("state") != "held":
            raise CoordinationError("lease is missing or not held; extension is not allowed")
        if (
            lease.get("owner_id") != owner_id
            or lease.get("request_id") != request_id
            or int(lease.get("fencing", -1)) != fencing
            or int(lease.get("expires_at", -1)) != expected_expires_at
        ):
            raise CoordinationError("lease identity or expected expiry changed; extension refused")
        if not request or request.get("state") != "granted" or request.get("actor_id") != owner_id:
            raise CoordinationError("granted request identity changed; extension refused")
        timestamp = now()
        if expected_expires_at <= timestamp:
            raise CoordinationError(
                f"lease expired at {expected_expires_at}; use explicit coordinator recovery"
            )
        new_expires_at = timestamp + ttl_seconds
        if new_expires_at <= expected_expires_at:
            raise CoordinationError("requested duration does not move expires_at forward")
        extension_id = str(uuid.uuid4())
        audit_item = {
            "scope": scope,
            "record_id": record_id("EXTENSION", extension_id),
            "extension_id": extension_id,
            "coordinator_id": coordinator_id,
            "coordinator_generation": coordinator_generation,
            "owner_id": owner_id,
            "request_id": request_id,
            "fencing": fencing,
            "previous_expires_at": expected_expires_at,
            "expires_at": new_expires_at,
            "ttl_seconds": ttl_seconds,
            "extended_at": timestamp,
            "reason": reason,
            "evidence": evidence,
        }
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "COORDINATOR"},
                    "ConditionExpression": "actor_id = :actor AND #token = :token AND #generation = :generation",
                    "ExpressionAttributeNames": {"#token": "token", "#generation": "generation"},
                    "ExpressionAttributeValues": {
                        ":actor": coordinator_id,
                        ":token": coordinator_token,
                        ":generation": coordinator_generation,
                    },
                }},
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": record_id("REQUEST", request_id)},
                    "ConditionExpression": "#state = :granted AND actor_id = :owner AND fencing = :fence",
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":granted": "granted",
                        ":owner": owner_id,
                        ":fence": fencing,
                    },
                }},
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "LEASE"},
                    "UpdateExpression": (
                        "SET #expires = :new_expires, last_extended_at = :now, "
                        "last_extension_id = :extension_id, last_extension_reason = :reason, "
                        "last_extension_evidence = :evidence"
                    ),
                    "ConditionExpression": (
                        "#state = :held AND owner_id = :owner AND request_id = :request "
                        "AND fencing = :fence AND #expires = :expected AND #expires > :now"
                    ),
                    "ExpressionAttributeNames": {"#state": "state", "#expires": "expires_at"},
                    "ExpressionAttributeValues": {
                        ":held": "held",
                        ":owner": owner_id,
                        ":request": request_id,
                        ":fence": fencing,
                        ":expected": expected_expires_at,
                        ":now": timestamp,
                        ":new_expires": new_expires_at,
                        ":extension_id": extension_id,
                        ":reason": reason,
                        ":evidence": evidence,
                    },
                }},
                {"Put": {
                    "TableName": self.table_name,
                    "Item": audit_item,
                    "ConditionExpression": "attribute_not_exists(record_id)",
                }},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError(
                "extension conflicted with a changed coordinator, request, or lease"
            ) from exc
        return audit_item

    def resume_recovery(
        self,
        scope: str,
        coordinator_id: str,
        coordinator_token: str,
        coordinator_generation: str,
        owner_id: str,
        request_id: str,
        fencing: int,
        expected_expires_at: int,
        ttl_seconds: int,
        reason: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Resume the same recovered transaction with a new token and fencing value."""
        self._validate_lease_ttl(ttl_seconds)
        if not reason.strip():
            raise CoordinationError("recovery resume reason is required")
        if not isinstance(evidence, dict):
            raise CoordinationError("recovery resume evidence must be a JSON object")
        coordinator = self._get(scope, "COORDINATOR")
        lease = self._get(scope, "LEASE")
        request = self._get(scope, record_id("REQUEST", request_id))
        if (
            not coordinator
            or coordinator.get("actor_id") != coordinator_id
            or coordinator.get("token") != coordinator_token
            or coordinator.get("generation") != coordinator_generation
        ):
            raise CoordinationError("coordinator registration token or generation is stale")
        if not lease or lease.get("state") != "recovery_required":
            raise CoordinationError("lease is not in recovery_required state")
        if (
            lease.get("owner_id") != owner_id
            or lease.get("request_id") != request_id
            or int(lease.get("fencing", -1)) != fencing
            or int(lease.get("expires_at", -1)) != expected_expires_at
        ):
            raise CoordinationError("recovery lease identity or expected expiry changed")
        if not request or request.get("state") != "granted" or request.get("actor_id") != owner_id:
            raise CoordinationError("granted request identity changed; recovery resume refused")
        timestamp = now()
        new_fencing = fencing + 1
        new_token = str(uuid.uuid4())
        new_expires_at = timestamp + ttl_seconds
        resume_id = str(uuid.uuid4())
        audit_item = {
            "scope": scope,
            "record_id": record_id("RECOVERY_RESUME", resume_id),
            "resume_id": resume_id,
            "coordinator_id": coordinator_id,
            "coordinator_generation": coordinator_generation,
            "owner_id": owner_id,
            "request_id": request_id,
            "previous_fencing": fencing,
            "fencing": new_fencing,
            "previous_expires_at": expected_expires_at,
            "expires_at": new_expires_at,
            "ttl_seconds": ttl_seconds,
            "resumed_at": timestamp,
            "reason": reason,
            "evidence": evidence,
        }
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "COORDINATOR"},
                    "ConditionExpression": "actor_id = :actor AND #token = :token AND #generation = :generation",
                    "ExpressionAttributeNames": {"#token": "token", "#generation": "generation"},
                    "ExpressionAttributeValues": {
                        ":actor": coordinator_id,
                        ":token": coordinator_token,
                        ":generation": coordinator_generation,
                    },
                }},
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "LEASE"},
                    "UpdateExpression": (
                        "SET #state = :held, lease_token = :lease, fencing = :new_fence, "
                        "#expires = :new_expires, last_heartbeat_at = :now, resumed_at = :now, "
                        "last_recovery_resume_id = :resume_id, recovery_resume_reason = :reason, "
                        "recovery_resume_evidence = :evidence REMOVE recovery_reason"
                    ),
                    "ConditionExpression": (
                        "#state = :recovery AND owner_id = :owner AND request_id = :request "
                        "AND fencing = :fence AND #expires = :expected"
                    ),
                    "ExpressionAttributeNames": {"#state": "state", "#expires": "expires_at"},
                    "ExpressionAttributeValues": {
                        ":recovery": "recovery_required",
                        ":held": "held",
                        ":owner": owner_id,
                        ":request": request_id,
                        ":fence": fencing,
                        ":new_fence": new_fencing,
                        ":expected": expected_expires_at,
                        ":new_expires": new_expires_at,
                        ":lease": new_token,
                        ":now": timestamp,
                        ":resume_id": resume_id,
                        ":reason": reason,
                        ":evidence": evidence,
                    },
                }},
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": record_id("REQUEST", request_id)},
                    "UpdateExpression": "SET lease_token = :lease, fencing = :new_fence, resumed_at = :now",
                    "ConditionExpression": "#state = :granted AND actor_id = :owner AND fencing = :fence",
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":granted": "granted",
                        ":owner": owner_id,
                        ":fence": fencing,
                        ":new_fence": new_fencing,
                        ":lease": new_token,
                        ":now": timestamp,
                    },
                }},
                {"Put": {
                    "TableName": self.table_name,
                    "Item": audit_item,
                    "ConditionExpression": "attribute_not_exists(record_id)",
                }},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError(
                "recovery resume conflicted with a changed coordinator, request, or lease"
            ) from exc
        return {
            **audit_item,
            "state": "held",
            "lease_token": new_token,
        }

    def release(self, scope: str, actor_id: str, lease_token: str, phase: str, evidence: dict[str, Any]) -> dict[str, Any]:
        lease = self._get(scope, "LEASE")
        if not lease or lease.get("owner_id") != actor_id or lease.get("lease_token") != lease_token or lease.get("state") != "held":
            raise CoordinationError("lease is missing, stale, or not owned by this actor")
        timestamp = now()
        self.table.update_item(
            Key={"scope": scope, "record_id": "LEASE"},
            UpdateExpression="SET #state = :free, released_at = :released, release_phase = :phase, evidence = :evidence REMOVE owner_id, lease_token, request_id, purpose, expires_at",
            ExpressionAttributeNames={"#state": "state"},
            ExpressionAttributeValues={":free": "free", ":released": timestamp, ":phase": phase, ":evidence": evidence, ":token": lease_token, ":owner": actor_id, ":held": "held"},
            ConditionExpression="lease_token = :token AND owner_id = :owner AND #state = :held",
        )
        return self.status(scope)

    def status(self, scope: str) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        response = self.table.query(KeyConditionExpression=Key("scope").eq(scope))
        items.extend(response.get("Items", []))
        while response.get("LastEvaluatedKey"):
            response = self.table.query(KeyConditionExpression=Key("scope").eq(scope), ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
        by_id = {item["record_id"]: self._redact(item) for item in items}
        return {
            "scope": scope,
            "lease": by_id.get("LEASE"),
            "coordinator": by_id.get("COORDINATOR"),
            "workers": [self._redact(item) for item in items if item["record_id"].startswith("WORKER#")],
            "requests": sorted((self._redact(item) for item in items if item["record_id"].startswith("REQUEST#")), key=lambda item: item.get("created_at", 0)),
        }

    def current_lease(self, scope: str) -> dict[str, Any] | None:
        """Return the raw lease for an authorization check; never print this directly."""
        return self._get(scope, "LEASE")

    def require_active_lease(
        self,
        scope: str,
        actor_id: str,
        lease_token: str,
        timestamp: int | None = None,
    ) -> dict[str, Any]:
        """Return a currently usable lease or raise a precise fail-closed error."""
        lease = self.current_lease(scope)
        if not lease:
            raise CoordinationError("lease is missing; coordinator recovery may be required")
        if lease.get("state") != "held":
            raise CoordinationError(f"lease state is {lease.get('state', 'unknown')}; coordinator recovery is required")
        if lease.get("owner_id") != actor_id:
            raise CoordinationError(
                f"lease is not held by actor {actor_id!r}; verify --actor-id"
            )
        if lease.get("lease_token") != lease_token:
            raise CoordinationError(
                f"lease token is stale for actor {actor_id!r}; use the exact token from the grant output"
            )
        checked_at = now() if timestamp is None else timestamp
        if int(lease.get("expires_at", 0)) <= checked_at:
            raise CoordinationError(
                f"lease expired at {lease.get('expires_at', 0)}; coordinator recovery is required"
            )
        return lease

    @staticmethod
    def _redact(item: dict[str, Any]) -> dict[str, Any]:
        safe = dict(item)
        safe.pop("token", None)
        safe.pop("lease_token", None)
        return safe

    def recover(self, scope: str, coordinator_id: str, coordinator_token: str, reason: str) -> dict[str, Any]:
        coordinator = self._get(scope, "COORDINATOR")
        if not coordinator or coordinator.get("actor_id") != coordinator_id or coordinator.get("token") != coordinator_token:
            raise CoordinationError("coordinator registration is missing or stale")
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "COORDINATOR"},
                    "ConditionExpression": "actor_id = :actor AND #token = :token",
                    "ExpressionAttributeNames": {"#token": "token"},
                    "ExpressionAttributeValues": {":actor": coordinator_id, ":token": coordinator_token},
                }},
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "LEASE"},
                    "UpdateExpression": "SET #state = :recovery, recovery_reason = :reason, recovery_at = :at",
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":recovery": "recovery_required",
                        ":reason": reason,
                        ":at": now(),
                        ":held": "held",
                    },
                    "ConditionExpression": "attribute_exists(record_id) AND #state = :held",
                }},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError("recovery conflicted with a changed coordinator or lease") from exc
        return self.status(scope)

    def recover_exact(
        self,
        scope: str,
        coordinator_id: str,
        coordinator_token: str,
        coordinator_generation: str,
        owner_id: str,
        request_id: str,
        fencing: int,
        expected_expires_at: int,
        reason: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Enter recovery only if the complete coordinator and lease identity is unchanged."""
        if not reason.strip():
            raise CoordinationError("recovery reason is required")
        if not isinstance(evidence, dict):
            raise CoordinationError("recovery evidence must be a JSON object")
        coordinator = self._get(scope, "COORDINATOR")
        lease = self._get(scope, "LEASE")
        request = self._get(scope, record_id("REQUEST", request_id))
        if (
            not coordinator
            or coordinator.get("actor_id") != coordinator_id
            or coordinator.get("token") != coordinator_token
            or coordinator.get("generation") != coordinator_generation
        ):
            raise CoordinationError("coordinator registration token or generation is stale")
        if (
            not lease
            or lease.get("state") != "held"
            or lease.get("owner_id") != owner_id
            or lease.get("request_id") != request_id
            or int(lease.get("fencing", -1)) != fencing
            or int(lease.get("expires_at", -1)) != expected_expires_at
        ):
            raise CoordinationError("held lease identity or expected expiry changed")
        if not request or request.get("state") != "granted" or request.get("actor_id") != owner_id:
            raise CoordinationError("granted request identity changed; recovery refused")
        timestamp = now()
        recovery_id = str(uuid.uuid4())
        audit_item = {
            "scope": scope,
            "record_id": record_id("RECOVERY", recovery_id),
            "recovery_id": recovery_id,
            "coordinator_id": coordinator_id,
            "coordinator_generation": coordinator_generation,
            "owner_id": owner_id,
            "request_id": request_id,
            "fencing": fencing,
            "expires_at": expected_expires_at,
            "recovery_at": timestamp,
            "reason": reason,
            "evidence": evidence,
        }
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "COORDINATOR"},
                    "ConditionExpression": "actor_id = :actor AND #token = :token AND #generation = :generation",
                    "ExpressionAttributeNames": {"#token": "token", "#generation": "generation"},
                    "ExpressionAttributeValues": {
                        ":actor": coordinator_id,
                        ":token": coordinator_token,
                        ":generation": coordinator_generation,
                    },
                }},
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": record_id("REQUEST", request_id)},
                    "ConditionExpression": "#state = :granted AND actor_id = :owner AND fencing = :fence",
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":granted": "granted",
                        ":owner": owner_id,
                        ":fence": fencing,
                    },
                }},
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "LEASE"},
                    "UpdateExpression": (
                        "SET #state = :recovery, recovery_reason = :reason, recovery_at = :at, "
                        "recovery_evidence = :evidence, recovery_id = :recovery_id"
                    ),
                    "ConditionExpression": (
                        "#state = :held AND owner_id = :owner AND request_id = :request "
                        "AND fencing = :fence AND #expires = :expected"
                    ),
                    "ExpressionAttributeNames": {"#state": "state", "#expires": "expires_at"},
                    "ExpressionAttributeValues": {
                        ":held": "held",
                        ":recovery": "recovery_required",
                        ":owner": owner_id,
                        ":request": request_id,
                        ":fence": fencing,
                        ":expected": expected_expires_at,
                        ":reason": reason,
                        ":at": timestamp,
                        ":evidence": evidence,
                        ":recovery_id": recovery_id,
                    },
                }},
                {"Put": {
                    "TableName": self.table_name,
                    "Item": audit_item,
                    "ConditionExpression": "attribute_not_exists(record_id)",
                }},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError(
                "exact recovery conflicted with a changed coordinator, request, or lease"
            ) from exc
        return audit_item

    def complete_recovery(self, scope: str, coordinator_id: str, coordinator_token: str, evidence: dict[str, Any]) -> dict[str, Any]:
        """Clear an explicitly reviewed recovery state and return the lease to free."""
        coordinator = self._get(scope, "COORDINATOR")
        if not coordinator or coordinator.get("actor_id") != coordinator_id or coordinator.get("token") != coordinator_token:
            raise CoordinationError("coordinator registration is missing or stale")
        client = self.table.meta.client
        try:
            client.transact_write_items(TransactItems=[
                {"ConditionCheck": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "COORDINATOR"},
                    "ConditionExpression": "actor_id = :actor AND #token = :token",
                    "ExpressionAttributeNames": {"#token": "token"},
                    "ExpressionAttributeValues": {":actor": coordinator_id, ":token": coordinator_token},
                }},
                {"Update": {
                    "TableName": self.table_name,
                    "Key": {"scope": scope, "record_id": "LEASE"},
                    "UpdateExpression": "SET #state = :free, recovery_completed_at = :at, recovery_evidence = :evidence REMOVE recovery_reason",
                    "ExpressionAttributeNames": {"#state": "state"},
                    "ExpressionAttributeValues": {
                        ":free": "free",
                        ":at": now(),
                        ":evidence": evidence,
                        ":recovery": "recovery_required",
                    },
                    "ConditionExpression": "#state = :recovery",
                }},
            ])
        except client.exceptions.TransactionCanceledException as exc:
            raise CoordinationError("recovery completion conflicted with a changed coordinator or lease") from exc
        return self.status(scope)
