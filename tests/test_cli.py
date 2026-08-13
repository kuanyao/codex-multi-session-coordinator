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


def test_heartbeat_accepts_registration_token_with_post_command_globals() -> None:
    arguments = parser().parse_args([
        "heartbeat",
        "--table",
        "codex-multi-session-coordinator-dev",
        "--region",
        "us-east-1",
        "--scope",
        "aurora",
        "--actor-id",
        "aurora-integration-coordinator-a81f",
        "--registration-token",
        "registration-a",
        "--phase",
        "testing-dev",
        "--message",
        "coordinator checking active integration",
    ])

    assert arguments.table == "codex-multi-session-coordinator-dev"
    assert arguments.region == "us-east-1"
    assert arguments.scope == "aurora"
    assert arguments.token == "registration-a"
    assert arguments.phase == "testing-dev"


def test_request_accepts_registration_token_aliases() -> None:
    preferred = parser().parse_args([
        "request",
        "--actor-id",
        "worker-a",
        "--registration-token",
        "registration-a",
        "--summary",
        "integration work",
    ])
    compatible = parser().parse_args([
        "request",
        "--actor-id",
        "worker-a",
        "--token",
        "registration-b",
        "--summary",
        "integration work",
    ])

    assert preferred.token == "registration-a"
    assert compatible.token == "registration-b"


def test_heartbeat_accepts_phase() -> None:
    arguments = heartbeat_arguments("--phase", "awaiting-review")

    assert arguments.phase == "awaiting-review"


def test_heartbeat_accepts_state_as_phase_alias() -> None:
    arguments = heartbeat_arguments("--state", "waiting")

    assert arguments.phase == "waiting"


def test_guard_accepts_global_options_after_subcommand() -> None:
    arguments = parser().parse_args([
        "guard",
        "--table",
        "coordinator-table",
        "--scope",
        "aurora",
        "--region",
        "us-east-1",
        "--actor-id",
        "worker-a",
        "--lease-token",
        "lease-a",
        "--",
        "provider-retry",
        "--ticker",
        "AAPL",
    ])

    assert arguments.table == "coordinator-table"
    assert arguments.scope == "aurora"
    assert arguments.region == "us-east-1"
    assert arguments.actor_id == "worker-a"
    assert arguments.lease_token == "lease-a"
    assert arguments.command_args == ["--", "provider-retry", "--ticker", "AAPL"]


def test_global_options_work_before_or_after_non_guard_subcommand() -> None:
    before = parser().parse_args([
        "--table",
        "before-table",
        "--scope",
        "before-scope",
        "--json",
        "status",
        "--pretty",
    ])
    after = parser().parse_args([
        "status",
        "--table",
        "after-table",
        "--scope",
        "after-scope",
        "--pretty",
    ])

    assert (before.table, before.scope) == ("before-table", "before-scope")
    assert before.json is True
    assert (after.table, after.scope) == ("after-table", "after-scope")


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
