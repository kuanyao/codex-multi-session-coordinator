from pathlib import Path

import pytest


@pytest.mark.parametrize("path", [Path("README.md"), Path("docs/usage.md")])
def test_operational_docs_do_not_use_path_dependent_cli_invocations(path: Path) -> None:
    bare_invocations = [
        line for line in path.read_text().splitlines()
        if line.startswith("codex-coordinator ")
    ]

    assert bare_invocations == []


def test_usage_documents_executable_preflight() -> None:
    usage = Path("docs/usage.md").read_text()

    assert "test -x ./.venv/bin/codex-coordinator" in usage
    assert "./.venv/bin/codex-coordinator --help >/dev/null" in usage
