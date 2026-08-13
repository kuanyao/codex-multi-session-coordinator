"""Command-line interface for the coordinator lease store."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
from typing import Any

from .store import CoordinationError, CoordinatorStore


def add_global_arguments(target: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default = argparse.SUPPRESS if suppress_defaults else None
    target.add_argument(
        "--table",
        default=default if suppress_defaults else os.environ.get("CODEX_COORDINATOR_TABLE"),
        required=False,
        help="DynamoDB table (or CODEX_COORDINATOR_TABLE)",
    )
    target.add_argument(
        "--region",
        default=default if suppress_defaults else os.environ.get("AWS_REGION"),
        help="AWS region (or AWS_REGION)",
    )
    target.add_argument(
        "--scope",
        default=default if suppress_defaults else os.environ.get("CODEX_COORDINATOR_SCOPE", "default"),
        help="coordination scope (or CODEX_COORDINATOR_SCOPE; default: default)",
    )
    target.add_argument(
        "--json",
        action="store_true",
        default=default if suppress_defaults else False,
        help="emit JSON output where supported",
    )


def command_parser(commands: Any, name: str) -> argparse.ArgumentParser:
    command = commands.add_parser(name)
    add_global_arguments(command, suppress_defaults=True)
    return command


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Coordinate exclusive access to shared work resources.")
    add_global_arguments(root)
    commands = root.add_subparsers(dest="command", required=True)
    register = command_parser(commands, "register")
    register.add_argument("--role", choices=["worker", "coordinator"], required=True)
    register.add_argument("--actor-id", required=True)
    register.add_argument("--title", required=True)
    heartbeat = command_parser(commands, "heartbeat")
    heartbeat.add_argument("--actor-id", required=True)
    heartbeat.add_argument("--token", required=True)
    heartbeat.add_argument(
        "--phase",
        "--state",
        dest="phase",
        required=True,
        help="workflow phase to report (--state is a compatibility alias)",
    )
    heartbeat.add_argument("--message", default="")
    heartbeat.add_argument("--lease-token")
    request = command_parser(commands, "request")
    request.add_argument("--actor-id", required=True)
    request.add_argument("--token", required=True)
    request.add_argument("--summary", required=True)
    request.add_argument("--metadata", default="{}")
    grant = command_parser(commands, "grant")
    grant.add_argument("--coordinator-id", required=True)
    grant.add_argument("--coordinator-token", required=True)
    grant.add_argument("--request-id", required=True)
    grant.add_argument("--ttl-seconds", type=int, default=3600)
    release = command_parser(commands, "release")
    release.add_argument("--actor-id", required=True)
    release.add_argument("--lease-token", required=True)
    release.add_argument("--phase", default="complete")
    release.add_argument("--evidence", default="{}")
    recover = command_parser(commands, "recover")
    recover.add_argument("--coordinator-id", required=True)
    recover.add_argument("--coordinator-token", required=True)
    recover.add_argument("--reason", required=True)
    complete_recovery = command_parser(commands, "complete-recovery")
    complete_recovery.add_argument("--coordinator-id", required=True)
    complete_recovery.add_argument("--coordinator-token", required=True)
    complete_recovery.add_argument("--evidence", default="{}")
    status = command_parser(commands, "status")
    status.add_argument("--pretty", action="store_true")
    guard = command_parser(commands, "guard")
    guard.add_argument("--actor-id", required=True)
    guard.add_argument("--lease-token", required=True)
    guard.add_argument("command_args", nargs=argparse.REMAINDER)
    return root


def output(value: Any, as_json: bool, pretty: bool = False) -> None:
    if as_json or isinstance(value, (dict, list)):
        print(json.dumps(value, indent=2 if pretty else None, sort_keys=True, default=str))
    else:
        print(value)


def main() -> int:
    args = parser().parse_args()
    if not args.table:
        print("CODEX_COORDINATOR_TABLE or --table is required", file=sys.stderr)
        return 2
    store = CoordinatorStore(args.table, args.region)
    try:
        if args.command == "register":
            result = store.register(args.scope, args.actor_id, args.role, args.title)
            output(result.__dict__, True)
        elif args.command == "heartbeat":
            output(store.heartbeat(args.scope, args.actor_id, args.token, args.phase, args.message, args.lease_token), args.json)
        elif args.command == "request":
            request_id = store.request(args.scope, args.actor_id, args.token, args.summary, json.loads(args.metadata))
            output({"request_id": request_id}, True)
        elif args.command == "grant":
            output(store.grant(args.scope, args.coordinator_id, args.coordinator_token, args.request_id, args.ttl_seconds), True)
        elif args.command == "release":
            output(store.release(args.scope, args.actor_id, args.lease_token, args.phase, json.loads(args.evidence)), args.json)
        elif args.command == "recover":
            output(store.recover(args.scope, args.coordinator_id, args.coordinator_token, args.reason), args.json)
        elif args.command == "complete-recovery":
            output(store.complete_recovery(args.scope, args.coordinator_id, args.coordinator_token, json.loads(args.evidence)), args.json)
        elif args.command == "status":
            output(store.status(args.scope), True, args.pretty)
        elif args.command == "guard":
            command = args.command_args[1:] if args.command_args[:1] == ["--"] else args.command_args
            if not command:
                raise CoordinationError("guard requires a command after --")
            store.require_active_lease(args.scope, args.actor_id, args.lease_token)
            result = subprocess.run(command, check=False)
            return result.returncode
        return 0
    except (CoordinationError, ValueError, json.JSONDecodeError) as exc:
        print(f"coordination error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
