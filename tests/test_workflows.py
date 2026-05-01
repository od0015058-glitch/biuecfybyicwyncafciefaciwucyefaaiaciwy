"""Sanity tests for the GitHub Actions workflows.

Stage-15-Step-E #6 follow-up #2: pin the new
``.github/workflows/integration.yml`` workflow's shape so a future
edit can't silently break it. We don't need a full Actions-runner
schema — just enough to catch the typos that would render the
workflow unparseable, and to encode the safety invariants this
workflow's PR description documented.

This file deliberately uses **only the stdlib** for parsing — the
project has zero runtime YAML dependency, so we don't add PyYAML
just to lint a CI file. The text-level checks below catch every
class of mistake we care about.
"""

from __future__ import annotations

import pathlib
import re

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
INTEGRATION_WF = WORKFLOWS_DIR / "integration.yml"


def _read_workflow_text(name: str) -> str:
    path = WORKFLOWS_DIR / name
    assert path.exists(), f"missing workflow: {path}"
    return path.read_text(encoding="utf-8")


def test_integration_workflow_file_exists():
    assert INTEGRATION_WF.exists(), (
        "integration workflow file must exist at "
        ".github/workflows/integration.yml"
    )


def test_integration_workflow_has_a_name():
    text = _read_workflow_text("integration.yml")
    assert re.search(r"^name:\s*\S+", text, re.MULTILINE), (
        "workflow must have a top-level ``name:`` field"
    )


def test_integration_workflow_is_manual_dispatch_only():
    """Critical safety invariant: the integration suite touches a
    real Telegram bot and consumes credits / triggers user-visible
    side effects. It MUST run only on manual ``workflow_dispatch``
    so a fork PR or scheduled run can't kick it off.
    """
    text = _read_workflow_text("integration.yml")
    # Find the ``on:`` block. It can be ``on:`` followed by a
    # mapping or by a list. Either way, the body lives between
    # ``on:`` and the next zero-indent block (``jobs:`` or
    # ``concurrency:`` etc.).
    on_match = re.search(
        r"^on:\s*$\n(.*?)^(?=\S)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert on_match, "workflow must have an ``on:`` block"
    on_block = on_match.group(1)
    assert "workflow_dispatch" in on_block, (
        "integration suite must be triggered manually via "
        "``workflow_dispatch`` so unauthorised runs can't fire"
    )
    forbidden = ["push:", "pull_request:", "schedule:", "release:"]
    for trigger in forbidden:
        # Match only at zero or two-space indent under ``on:`` —
        # the stricter check would need a YAML parser, but in
        # practice the workflow body is well-formed so a string
        # presence check is enough.
        assert trigger not in on_block, (
            f"integration workflow must not have automatic "
            f"trigger ``{trigger}``; every run sends real "
            f"Telegram messages"
        )


def test_integration_workflow_has_timeout_minutes():
    """A hung Telethon client (stale session, offline bot) must
    not be able to burn the workflow's full 6-hour budget."""
    text = _read_workflow_text("integration.yml")
    timeout_match = re.search(
        r"^\s+timeout-minutes:\s*(\d+)\s*$",
        text,
        re.MULTILINE,
    )
    assert timeout_match, "missing job-level ``timeout-minutes``"
    timeout = int(timeout_match.group(1))
    assert 1 <= timeout <= 60, (
        "integration timeout should be a small bound (1-60 min); "
        "the suite normally finishes in <5 min"
    )


def test_integration_workflow_passes_secrets_via_env():
    """Each of the four required secrets must reach pytest via the
    env block — the conftest fixture reads them from env. If the
    YAML stops binding even one secret, the suite would skip with
    no signal."""
    text = _read_workflow_text("integration.yml")
    required = (
        "TG_API_ID",
        "TG_API_HASH",
        "TG_TEST_SESSION_STRING",
        "TG_TEST_BOT_USERNAME",
    )
    for var in required:
        # Each binding should reference the GitHub secret of the
        # same name. ``${{ secrets.X }}`` is the canonical shape.
        pattern = (
            rf"{var}:\s*\$\{{\{{\s*secrets\.{var}\s*\}}\}}"
        )
        assert re.search(pattern, text), (
            f"workflow env must bind {var} via "
            f"``${{{{ secrets.{var} }}}}`` so the integration "
            f"conftest fixture can pick it up"
        )


def test_integration_workflow_has_concurrency_group():
    """Manual re-triggers shouldn't pile up — a stale run should
    cancel when the operator kicks a fresh one."""
    text = _read_workflow_text("integration.yml")
    assert re.search(r"^concurrency:", text, re.MULTILINE), (
        "integration workflow should declare a concurrency group "
        "so re-triggers cancel in-flight runs"
    )
    assert "cancel-in-progress: true" in text, (
        "concurrency.cancel-in-progress should be true"
    )


def test_integration_workflow_uses_latest_python_3_12():
    text = _read_workflow_text("integration.yml")
    assert re.search(
        r"python-version:\s*['\"]?3\.(11|12)['\"]?",
        text,
    ), "workflow should pin Python 3.11 or 3.12"


def test_integration_workflow_installs_dev_requirements():
    """Telethon lives in requirements-dev.txt; the workflow must
    install it or the integration suite would skip with
    ``telethon not installed``."""
    text = _read_workflow_text("integration.yml")
    assert "requirements-dev.txt" in text, (
        "integration workflow must install requirements-dev.txt "
        "(telethon, pytest-asyncio) — otherwise the conftest "
        "skip-on-missing-import path fires"
    )


def test_main_ci_workflow_still_exists():
    """Sanity: the new workflow doesn't accidentally replace the
    main pytest workflow."""
    main_ci = WORKFLOWS_DIR / "ci.yml"
    assert main_ci.exists(), (
        "The default ci.yml unit-test workflow must continue to "
        "exist; the new integration.yml is additive."
    )
