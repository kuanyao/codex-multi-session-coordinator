from __future__ import annotations

from types import SimpleNamespace

from codex_multi_session_coordinator.store import CoordinatorStore


class FakeTable:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}
        self.meta = SimpleNamespace(client=SimpleNamespace())

    def put_item(self, *, Item, **kwargs):
        self.items[(Item["scope"], Item["record_id"])] = dict(Item)

    def get_item(self, *, Key):
        item = self.items.get((Key["scope"], Key["record_id"]))
        return {"Item": dict(item)} if item else {}

    def query(self, **kwargs):
        return {"Items": [dict(item) for item in self.items.values()]}


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
