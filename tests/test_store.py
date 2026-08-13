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


def test_worker_registration_is_scoped_by_actor() -> None:
    table = FakeTable()
    store = CoordinatorStore("table", table=table)
    first = store.register("aurora", "worker-a", "worker", "one")
    second = store.register("aurora", "worker-b", "worker", "two")

    workers = {item["actor_id"] for item in store.status("aurora")["workers"]}
    assert workers == {"worker-a", "worker-b"}
    assert first.token != second.token


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
