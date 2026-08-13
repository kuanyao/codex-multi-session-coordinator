from __future__ import annotations

import os

from botocore.exceptions import ClientError

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


def test_recover_worker_registration_parses_exact_active_identity() -> None:
    arguments = parser().parse_args([
        "recover-worker-registration",
        "--actor-id", "worker-a",
        "--lease-token", "lease-a",
        "--expected-generation", "generation-a",
        "--request-id", "request-a",
        "--fencing", "17",
        "--expected-expires-at", "200",
        "--title", "Product site worker",
        "--reason", "saved registration token is stale",
        "--evidence", '{"mutation_after_error":false}',
    ])

    assert arguments.command == "recover-worker-registration"
    assert arguments.expected_generation == "generation-a"
    assert arguments.fencing == 17
    assert arguments.expected_expires_at == 200


def test_heartbeat_accepts_phase() -> None:
    arguments = heartbeat_arguments("--phase", "awaiting-review")

    assert arguments.phase == "awaiting-review"


def test_heartbeat_accepts_state_as_phase_alias() -> None:
    arguments = heartbeat_arguments("--state", "waiting")

    assert arguments.phase == "waiting"


def test_extend_requires_complete_compare_and_swap_identity() -> None:
    arguments = parser().parse_args([
        "extend",
        "--coordinator-id", "coord-a",
        "--coordinator-token", "token-a",
        "--coordinator-generation", "generation-a",
        "--owner-id", "worker-a",
        "--request-id", "request-a",
        "--fencing", "16",
        "--expected-expires-at", "200",
        "--ttl-seconds", "21600",
        "--reason", "repair needs more time",
        "--evidence", '{"ecs":"contained"}',
    ])

    assert arguments.command == "extend"
    assert arguments.fencing == 16
    assert arguments.expected_expires_at == 200
    assert arguments.ttl_seconds == 21600


def test_exact_recovery_commands_parse_complete_identity() -> None:
    common = [
        "--coordinator-id", "coord-a",
        "--coordinator-token", "token-a",
        "--coordinator-generation", "generation-a",
        "--owner-id", "worker-a",
        "--request-id", "request-a",
        "--fencing", "16",
        "--expected-expires-at", "200",
        "--reason", "contained recovery",
        "--evidence", '{"ecs":"contained"}',
    ]
    recover = parser().parse_args(["recover-exact", *common])
    resume = parser().parse_args([
        "resume-recovery", *common, "--ttl-seconds", "21600",
    ])

    assert recover.command == "recover-exact"
    assert resume.command == "resume-recovery"
    assert resume.ttl_seconds == 21600


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


def test_guard_accepts_child_only_aws_profile() -> None:
    arguments = parser().parse_args([
        "guard",
        "--actor-id", "worker-a",
        "--lease-token", "lease-a",
        "--child-aws-profile", "aurora-management",
        "--",
        "aws", "route53", "change-resource-record-sets",
    ])

    assert arguments.child_aws_profile == "aurora-management"
    assert arguments.command_args == [
        "--", "aws", "route53", "change-resource-record-sets",
    ]


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


def test_guard_applies_aws_profile_only_to_child(monkeypatch) -> None:
    class ActiveLeaseStore:
        def __init__(self, table_name, region):
            assert os.environ.get("AWS_PROFILE") == "coordinator-parent"

        def require_active_lease(self, scope, actor_id, lease_token):
            assert os.environ.get("AWS_PROFILE") == "coordinator-parent"

    observed_environment = None

    def record_run(command, check, env):
        nonlocal observed_environment
        observed_environment = env
        assert command == ["aws", "route53", "list-hosted-zones"]
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setenv("AWS_PROFILE", "coordinator-parent")
    monkeypatch.setattr(cli, "CoordinatorStore", ActiveLeaseStore)
    monkeypatch.setattr(cli.subprocess, "run", record_run)
    monkeypatch.setattr(cli.sys, "argv", [
        "codex-coordinator",
        "--table", "table",
        "guard",
        "--actor-id", "worker-a",
        "--lease-token", "lease-a",
        "--child-aws-profile", "aurora-management",
        "--",
        "aws", "route53", "list-hosted-zones",
    ])

    assert cli.main() == 0
    assert os.environ["AWS_PROFILE"] == "coordinator-parent"
    assert observed_environment is not None
    assert observed_environment["AWS_PROFILE"] == "aurora-management"


def test_resource_not_found_explains_parent_credential_context(monkeypatch, capsys) -> None:
    class MissingTableStore:
        def __init__(self, table_name, region):
            pass

        def status(self, scope):
            raise ClientError(
                {
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": "Requested resource not found",
                    }
                },
                "Query",
            )

    monkeypatch.setattr(cli, "CoordinatorStore", MissingTableStore)
    monkeypatch.setattr(cli.sys, "argv", [
        "codex-coordinator", "--table", "coordinator-table", "status",
    ])

    assert cli.main() == 1
    error = capsys.readouterr().err
    assert "coordinator parent AWS credential context" in error
    assert "--child-aws-profile before --" in error
    assert "Traceback" not in error
