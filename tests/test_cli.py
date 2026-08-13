from __future__ import annotations

from codex_multi_session_coordinator import cli
from codex_multi_session_coordinator.cli import parser
from codex_multi_session_coordinator.store import CoordinationError


def heartbeat_arguments(phase_option: str, value: str):
    return parser().parse_args([
        "heartbeat",
        "--actor-id",
        "worker-a",
        "--token",
        "registration-token",
        phase_option,
        value,
        "--message",
        "waiting for review",
    ])


def test_heartbeat_accepts_phase() -> None:
    arguments = heartbeat_arguments("--phase", "awaiting-review")

    assert arguments.phase == "awaiting-review"


def test_heartbeat_accepts_state_as_phase_alias() -> None:
    arguments = heartbeat_arguments("--state", "waiting")

    assert arguments.phase == "waiting"


def test_guard_surfaces_expiry_and_does_not_run_command(monkeypatch, capsys) -> None:
    class ExpiredLeaseStore:
        def __init__(self, table_name, region):
            pass

        def require_active_lease(self, scope, actor_id, lease_token):
            raise CoordinationError("lease expired at 99; coordinator recovery is required")

    command_ran = False

    def unexpected_run(*args, **kwargs):
        nonlocal command_ran
        command_ran = True

    monkeypatch.setattr(cli, "CoordinatorStore", ExpiredLeaseStore)
    monkeypatch.setattr(cli.subprocess, "run", unexpected_run)
    monkeypatch.setattr(
        cli.sys,
        "argv",
        [
            "codex-coordinator",
            "--table",
            "table",
            "--scope",
            "aurora",
            "guard",
            "--actor-id",
            "worker-a",
            "--lease-token",
            "lease-a",
            "--",
            "git",
            "push",
        ],
    )

    assert cli.main() == 1
    assert not command_ran
    assert "coordination error: lease expired at 99; coordinator recovery is required" in capsys.readouterr().err
