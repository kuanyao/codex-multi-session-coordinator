from __future__ import annotations

from types import SimpleNamespace

import pytest

from codex_multi_session_coordinator.store import CoordinationError, CoordinatorStore


class FakeTransactionCanceledException(Exception):
    pass


class FakeClient:
    exceptions = SimpleNamespace(TransactionCanceledException=FakeTransactionCanceledException)

    def __init__(self) -> None:
        self.transact_calls: list[dict] = []

    def transact_write_items(self, **kwargs):
        self.transact_calls.append(kwargs)


class FakeTable:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}
        self.update_calls: list[dict] = []
        self.meta = SimpleNamespace(client=FakeClient())

    def put_item(self, *, Item, **kwargs):
        self.items[(Item["scope"], Item["record_id"])] = dict(Item)

    def get_item(self, *, Key):
        item = self.items.get((Key["scope"], Key["record_id"]))
        return {"Item": dict(item)} if item else {}

    def query(self, **kwargs):
        return {"Items": [dict(item) for item in self.items.values()]}

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)


def test_new_coordinator_replaces_previous_registration() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    first = store.register("aurora", "coord-a", "coordinator", "first")
    second = store.register("aurora", "coord-b", "coordinator", "second")

    current = store.status("aurora")["coordinator"]
    assert current["actor_id"] == "coord-b"
    assert "token" not in current
    assert second.token != first.token


def test_stale_coordinator_token_reports_current_generation_without_touching_lease() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    first = store.register("aurora", "coord-a", "coordinator", "coordinator")
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "fencing": 16,
        "expires_at": 200,
    })
    current = store.register("aurora", "coord-a", "coordinator", "coordinator")
    lease_before = table.get_item(Key={"scope": "aurora", "record_id": "LEASE"})["Item"]

    with pytest.raises(
        CoordinationError,
        match=f"registration token is stale; current generation is {current.generation}",
    ):
        store.heartbeat("aurora", "coord-a", first.token, "testing", "stale")

    assert table.update_calls == []
    assert table.get_item(Key={"scope": "aurora", "record_id": "LEASE"})["Item"] == lease_before


def test_missing_registration_is_distinct_from_stale_token() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)

    with pytest.raises(
        CoordinationError,
        match="^registration is missing for actor 'worker-a'; verify --actor-id$",
    ):
        store.heartbeat("aurora", "worker-a", "unknown", "testing", "missing")


def test_missing_registration_says_lease_token_was_not_checked() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "expires_at": 200,
    })

    with pytest.raises(
        CoordinationError,
        match=(
            "^registration is missing for actor 'worker-typo'; verify --actor-id; "
            "lease token was not checked$"
        ),
    ):
        store.heartbeat(
            "aurora",
            "worker-typo",
            "registration-a",
            "testing",
            "missing",
            lease_token="lease-typo",
        )

    assert table.update_calls == []
    assert table.meta.client.transact_calls == []


def test_stale_registration_says_lease_token_was_not_checked() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    registration = store.register("aurora", "worker-a", "worker", "worker")

    with pytest.raises(
        CoordinationError,
        match=(
            f"^registration token is stale; current generation is {registration.generation}; "
            "lease token was not checked$"
        ),
    ):
        store.heartbeat(
            "aurora",
            "worker-a",
            "stale-registration",
            "testing",
            "stale",
            lease_token="lease-a",
        )

    assert table.update_calls == []
    assert table.meta.client.transact_calls == []


def test_worker_registration_is_scoped_by_actor() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    first = store.register("aurora", "worker-a", "worker", "one")
    second = store.register("aurora", "worker-b", "worker", "two")

    workers = {item["actor_id"] for item in store.status("aurora")["workers"]}
    assert workers == {"worker-a", "worker-b"}
    assert first.token != second.token


def test_worker_registration_recovery_preserves_exact_active_lease() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    worker = store.register("aurora", "worker-a", "worker", "worker")
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "REQUEST#request-a",
        "request_id": "request-a",
        "actor_id": "worker-a",
        "state": "granted",
        "fencing": 17,
    })
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "request_id": "request-a",
        "fencing": 17,
        "granted_at": 50,
        "expires_at": 200,
    })

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        result = store.recover_worker_registration(
            "aurora",
            "worker-a",
            "lease-a",
            worker.generation,
            "request-a",
            17,
            200,
            "worker",
            "saved registration token is stale",
            {"mutation_after_error": False},
        )

    assert result["previous_generation"] == worker.generation
    assert result["generation"] != worker.generation
    assert result["token"] != worker.token
    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    assert len(transaction) == 4
    worker_update = transaction[0]["Update"]
    lease_check = transaction[1]["ConditionCheck"]
    request_check = transaction[2]["ConditionCheck"]
    audit_put = transaction[3]["Put"]
    assert worker_update["Key"] == {"scope": "aurora", "record_id": "WORKER#worker-a"}
    assert "#generation = :expected" in worker_update["ConditionExpression"]
    assert lease_check["Key"] == {"scope": "aurora", "record_id": "LEASE"}
    assert "lease_token = :lease" in lease_check["ConditionExpression"]
    assert "fencing = :fence" in lease_check["ConditionExpression"]
    assert request_check["ExpressionAttributeValues"][":granted"] == "granted"
    assert audit_put["Item"]["evidence"] == {"mutation_after_error": False}
    assert all("Update" not in item or item["Update"]["Key"]["record_id"] != "LEASE" for item in transaction)
    assert all("Update" not in item or item["Update"]["Key"]["record_id"] != "REQUEST#request-a" for item in transaction)


def test_worker_registration_recovery_refuses_expired_lease() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    worker = store.register("aurora", "worker-a", "worker", "worker")
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "REQUEST#request-a",
        "request_id": "request-a",
        "actor_id": "worker-a",
        "state": "granted",
        "fencing": 17,
    })
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "request_id": "request-a",
        "fencing": 17,
        "expires_at": 100,
    })

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        with pytest.raises(CoordinationError, match="coordinator recovery is required first"):
            store.recover_worker_registration(
                "aurora", "worker-a", "lease-a", worker.generation,
                "request-a", 17, 100, "worker", "reason", {},
            )

    assert table.meta.client.transact_calls == []


def test_coordinator_heartbeat_aliases_registration_token() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    registration = store.register("aurora", "coord-a", "coordinator", "coordinator")

    store.heartbeat("aurora", "coord-a", registration.token, "none", "initialized")

    assert len(table.update_calls) == 1
    update = table.update_calls[0]
    assert update["Key"] == {"scope": "aurora", "record_id": "COORDINATOR"}
    assert update["ConditionExpression"] == "#token = :token"
    assert update["ExpressionAttributeNames"]["#token"] == "token"
    assert update["ExpressionAttributeNames"]["#ttl"] == "ttl"
    assert "#ttl = :ttl" in update["UpdateExpression"]
    assert update["ExpressionAttributeValues"][":token"] == registration.token


def test_worker_heartbeat_aliases_registration_token() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    registration = store.register("aurora", "worker-a", "worker", "worker")

    store.heartbeat("aurora", "worker-a", registration.token, "testing", "running")

    assert len(table.update_calls) == 1
    update = table.update_calls[0]
    assert update["Key"] == {"scope": "aurora", "record_id": "WORKER#worker-a"}
    assert update["ConditionExpression"] == "#token = :token"
    assert update["ExpressionAttributeNames"]["#token"] == "token"
    assert update["ExpressionAttributeNames"]["#ttl"] == "ttl"
    assert "#ttl = :ttl" in update["UpdateExpression"]
    assert update["ExpressionAttributeValues"][":token"] == registration.token


def test_worker_lease_heartbeat_supplies_complete_condition_values() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    registration = store.register("aurora", "worker-a", "worker", "worker")
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "expires_at": 200,
    })

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        store.heartbeat(
            "aurora",
            "worker-a",
            registration.token,
            "testing",
            "running",
            lease_token="lease-a",
        )

    assert table.update_calls == []
    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    assert len(transaction) == 2
    registration_update = transaction[0]["Update"]
    lease_update = transaction[1]["Update"]
    assert registration_update["Key"] == {"scope": "aurora", "record_id": "WORKER#worker-a"}
    assert lease_update["Key"] == {"scope": "aurora", "record_id": "LEASE"}
    assert lease_update["ConditionExpression"] == (
        "lease_token = :token AND owner_id = :owner AND #state = :held AND #expires > :seen"
    )
    assert lease_update["ExpressionAttributeNames"] == {"#state": "state", "#expires": "expires_at"}
    assert lease_update["ExpressionAttributeValues"] == {
        ":seen": 100,
        ":token": "lease-a",
        ":owner": "worker-a",
        ":held": "held",
    }


def test_expired_lease_heartbeat_fails_without_advancing_registration_or_lease() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    registration = store.register("aurora", "worker-a", "worker", "worker")
    worker_before = table.get_item(Key={"scope": "aurora", "record_id": "WORKER#worker-a"})["Item"]
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "expires_at": 99,
        "last_heartbeat_at": 80,
    })

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        with pytest.raises(CoordinationError, match="lease expired at 99; coordinator recovery is required"):
            store.heartbeat(
                "aurora",
                "worker-a",
                registration.token,
                "testing",
                "running",
                lease_token="lease-a",
            )

    assert table.update_calls == []
    assert table.meta.client.transact_calls == []
    assert table.get_item(Key={"scope": "aurora", "record_id": "WORKER#worker-a"})["Item"] == worker_before
    lease = table.get_item(Key={"scope": "aurora", "record_id": "LEASE"})["Item"]
    assert lease["state"] == "held"
    assert lease["last_heartbeat_at"] == 80


def test_guard_authorization_reports_expired_held_lease_as_recovery_required() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "expires_at": 99,
    })

    with pytest.raises(CoordinationError, match="lease expired at 99; coordinator recovery is required"):
        store.require_active_lease("aurora", "worker-a", "lease-a", timestamp=100)

    assert table.get_item(Key={"scope": "aurora", "record_id": "LEASE"})["Item"]["state"] == "held"


def test_lease_authorization_distinguishes_actor_from_token_mismatch() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": "held",
        "owner_id": "worker-a",
        "lease_token": "lease-a",
        "expires_at": 200,
    })

    with pytest.raises(
        CoordinationError,
        match="^lease is not held by actor 'worker-typo'; verify --actor-id$",
    ):
        store.require_active_lease("aurora", "worker-typo", "lease-a", timestamp=100)

    with pytest.raises(
        CoordinationError,
        match=(
            "^lease token is stale for actor 'worker-a'; "
            "use the exact token from the grant output$"
        ),
    ):
        store.require_active_lease("aurora", "worker-a", "lease-typo", timestamp=100)


def test_recovery_atomically_checks_current_coordinator_and_held_lease() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    coordinator = store.register("aurora", "coord-a", "coordinator", "coordinator")
    table.put_item(Item={"scope": "aurora", "record_id": "LEASE", "state": "held", "fencing": 13})

    store.recover("aurora", "coord-a", coordinator.token, "expired fencing 13")

    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    coordinator_check = transaction[0]["ConditionCheck"]
    lease_update = transaction[1]["Update"]
    assert coordinator_check["ConditionExpression"] == "actor_id = :actor AND #token = :token"
    assert coordinator_check["ExpressionAttributeValues"] == {
        ":actor": "coord-a",
        ":token": coordinator.token,
    }
    assert lease_update["ConditionExpression"] == "attribute_exists(record_id) AND #state = :held"
    assert lease_update["ExpressionAttributeValues"][":held"] == "held"


def test_recovery_completion_atomically_checks_current_coordinator_and_recovery_state() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    coordinator = store.register("aurora", "coord-a", "coordinator", "coordinator")
    table.put_item(Item={"scope": "aurora", "record_id": "LEASE", "state": "recovery_required", "fencing": 13})

    store.complete_recovery("aurora", "coord-a", coordinator.token, {"environment": "clean"})

    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    coordinator_check = transaction[0]["ConditionCheck"]
    lease_update = transaction[1]["Update"]
    assert coordinator_check["ExpressionAttributeValues"][":token"] == coordinator.token
    assert lease_update["ConditionExpression"] == "#state = :recovery"
    assert lease_update["ExpressionAttributeValues"][":recovery"] == "recovery_required"
    assert lease_update["ExpressionAttributeValues"][":free"] == "free"


def lease_transition_store(state: str = "held", expires_at: int = 200):
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    coordinator = store.register("aurora", "coord-a", "coordinator", "coordinator")
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "REQUEST#request-a",
        "request_id": "request-a",
        "actor_id": "worker-a",
        "state": "granted",
        "fencing": 16,
        "lease_token": "lease-a",
    })
    table.put_item(Item={
        "scope": "aurora",
        "record_id": "LEASE",
        "state": state,
        "owner_id": "worker-a",
        "request_id": "request-a",
        "lease_token": "lease-a",
        "fencing": 16,
        "granted_at": 50,
        "expires_at": expires_at,
    })
    return table, store, coordinator


def test_extend_atomically_pins_all_identities_and_writes_audit_record() -> None:
    table, store, coordinator = lease_transition_store()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        result = store.extend(
            "aurora",
            "coord-a",
            coordinator.token,
            coordinator.generation,
            "worker-a",
            "request-a",
            16,
            200,
            3600,
            "candidate repair needs more time",
            {"incident": "scheduler"},
        )

    assert result["previous_expires_at"] == 200
    assert result["expires_at"] == 3700
    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    assert len(transaction) == 4
    coordinator_check = transaction[0]["ConditionCheck"]
    request_check = transaction[1]["ConditionCheck"]
    lease_update = transaction[2]["Update"]
    audit_put = transaction[3]["Put"]
    assert "#generation = :generation" in coordinator_check["ConditionExpression"]
    assert coordinator_check["ExpressionAttributeValues"][":generation"] == coordinator.generation
    assert request_check["ExpressionAttributeValues"] == {
        ":granted": "granted",
        ":owner": "worker-a",
        ":fence": 16,
    }
    assert lease_update["ConditionExpression"] == (
        "#state = :held AND owner_id = :owner AND request_id = :request "
        "AND fencing = :fence AND #expires = :expected AND #expires > :now"
    )
    assert set(lease_update["UpdateExpression"].split()) >= {"#expires", "last_extended_at"}
    assert "owner_id" not in lease_update["UpdateExpression"]
    assert "request_id" not in lease_update["UpdateExpression"]
    assert "fencing" not in lease_update["UpdateExpression"]
    assert "granted_at" not in lease_update["UpdateExpression"]
    assert audit_put["Item"]["evidence"] == {"incident": "scheduler"}


@pytest.mark.parametrize("state,expires_at", [("free", 200), ("recovery_required", 200), ("held", 100)])
def test_extend_refuses_non_current_held_lease(state: str, expires_at: int) -> None:
    table, store, coordinator = lease_transition_store(state, expires_at)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        with pytest.raises(CoordinationError):
            store.extend(
                "aurora", "coord-a", coordinator.token, coordinator.generation,
                "worker-a", "request-a", 16, expires_at, 3600, "reason", {},
            )

    assert table.meta.client.transact_calls == []


def test_extend_refuses_replaced_coordinator_generation_and_unbounded_duration() -> None:
    table, store, coordinator = lease_transition_store()

    with pytest.raises(CoordinationError, match="token or generation is stale"):
        store.extend(
            "aurora", "coord-a", coordinator.token, "old-generation",
            "worker-a", "request-a", 16, 200, 3600, "reason", {},
        )
    with pytest.raises(CoordinationError, match="between 60 and 86400"):
        store.extend(
            "aurora", "coord-a", coordinator.token, coordinator.generation,
            "worker-a", "request-a", 16, 200, 86401, "reason", {},
        )
    assert table.meta.client.transact_calls == []


def test_resume_recovery_preserves_transaction_and_rotates_fence_and_token() -> None:
    table, store, coordinator = lease_transition_store("recovery_required", 90)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        result = store.resume_recovery(
            "aurora",
            "coord-a",
            coordinator.token,
            coordinator.generation,
            "worker-a",
            "request-a",
            16,
            90,
            3600,
            "same transaction must repair dev",
            {"ecs": "contained"},
        )

    assert result["state"] == "held"
    assert result["previous_fencing"] == 16
    assert result["fencing"] == 17
    assert result["lease_token"] != "lease-a"
    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    assert len(transaction) == 4
    lease_update = transaction[1]["Update"]
    request_update = transaction[2]["Update"]
    assert "owner_id" not in lease_update["UpdateExpression"]
    assert "request_id" not in lease_update["UpdateExpression"]
    assert "granted_at" not in lease_update["UpdateExpression"]
    assert lease_update["ExpressionAttributeValues"][":new_fence"] == 17
    assert request_update["ExpressionAttributeValues"][":new_fence"] == 17
    assert transaction[3]["Put"]["Item"]["evidence"] == {"ecs": "contained"}


def test_recover_exact_pins_current_transaction_and_writes_audit_record() -> None:
    table, store, coordinator = lease_transition_store("held", 90)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("codex_multi_session_coordinator.store.now", lambda: 100)
        result = store.recover_exact(
            "aurora",
            "coord-a",
            coordinator.token,
            coordinator.generation,
            "worker-a",
            "request-a",
            16,
            90,
            "expired during contained repair",
            {"ecs": "contained"},
        )

    assert result["fencing"] == 16
    transaction = table.meta.client.transact_calls[0]["TransactItems"]
    assert len(transaction) == 4
    lease_update = transaction[2]["Update"]
    assert lease_update["ConditionExpression"] == (
        "#state = :held AND owner_id = :owner AND request_id = :request "
        "AND fencing = :fence AND #expires = :expected"
    )
    assert transaction[3]["Put"]["Item"]["evidence"] == {"ecs": "contained"}
