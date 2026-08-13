from __future__ import annotations

from codex_multi_session_coordinator.cli import parser


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
