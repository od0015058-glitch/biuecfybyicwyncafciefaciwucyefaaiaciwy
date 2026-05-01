"""Stage-15-Step-E #5 — admin role hierarchy + lookup helpers.

These tests pin the role-comparison contract (``role_at_least`` is the
single chokepoint for every future gate) and the env-list-fallback
behaviour of ``effective_role``.
"""

from __future__ import annotations

import pytest

from admin_roles import (
    ROLE_ORDER,
    ROLE_OPERATOR,
    ROLE_SUPER,
    ROLE_VIEWER,
    VALID_ROLES,
    effective_role,
    normalize_role,
    role_at_least,
)


# ---------------------------------------------------------------------
# Constants / hierarchy invariants
# ---------------------------------------------------------------------


def test_role_order_lowest_to_highest():
    assert ROLE_ORDER == (ROLE_VIEWER, ROLE_OPERATOR, ROLE_SUPER)
    # Index ordering is the source of truth for `role_at_least`. A
    # future regression that re-orders the tuple would silently
    # invert every gate; pin it.
    assert ROLE_ORDER.index(ROLE_VIEWER) == 0
    assert ROLE_ORDER.index(ROLE_OPERATOR) == 1
    assert ROLE_ORDER.index(ROLE_SUPER) == 2


def test_valid_roles_matches_order():
    assert VALID_ROLES == frozenset(ROLE_ORDER)


# ---------------------------------------------------------------------
# normalize_role
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("viewer", "viewer"),
        ("operator", "operator"),
        ("super", "super"),
        ("VIEWER", "viewer"),
        (" Operator ", "operator"),
        ("\tsuper\n", "super"),
    ],
)
def test_normalize_role_accepts_known_values(raw, expected):
    assert normalize_role(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", " ", None, "admin", "supr", "super_admin", "owner", "🛡"],
)
def test_normalize_role_rejects_unknown(raw):
    assert normalize_role(raw) is None


# ---------------------------------------------------------------------
# role_at_least
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "role, required, expected",
    [
        # Self-comparison: each role meets its own minimum.
        ("viewer", "viewer", True),
        ("operator", "operator", True),
        ("super", "super", True),
        # Strictly above.
        ("super", "viewer", True),
        ("super", "operator", True),
        ("operator", "viewer", True),
        # Strictly below.
        ("viewer", "operator", False),
        ("viewer", "super", False),
        ("operator", "super", False),
    ],
)
def test_role_at_least_ordering(role, required, expected):
    assert role_at_least(role, required) is expected


@pytest.mark.parametrize("role", [None, "", "admin", "supr", "owner"])
def test_role_at_least_unknown_role_is_denied(role):
    """Fail-closed: an unrecognised actor role NEVER passes a gate.

    The point of the helper is to reduce a triple of (role, required,
    decision) into a boolean. A typo on either side must produce
    False — anything else means a misconfigured deploy could
    accidentally grant access.
    """
    assert role_at_least(role, "viewer") is False
    assert role_at_least(role, "super") is False


@pytest.mark.parametrize("required", ["", "admin", "supr", "owner"])
def test_role_at_least_unknown_required_is_denied(required):
    """A typo in the call-site (``role_at_least(actor, "supr")``) must
    fail closed too. If we matched it as "anything", a typo would
    accidentally grant access; if we matched it as "always-deny",
    the gate breaks loudly. Pick deny."""
    assert role_at_least("super", required) is False


# ---------------------------------------------------------------------
# effective_role — env-list backward-compat
# ---------------------------------------------------------------------


def test_effective_role_returns_db_role_when_set():
    assert effective_role(123, "operator", is_env_admin=False) == "operator"


def test_effective_role_db_role_wins_over_env_list():
    """Even if a Telegram id is in ``ADMIN_USER_IDS``, an explicit DB
    role row demotes them. Without this, an operator could not
    *demote* a legacy super-admin without also editing the env file."""
    assert effective_role(7, "viewer", is_env_admin=True) == "viewer"


def test_effective_role_falls_back_to_super_for_env_admin():
    """A Telegram id in ``ADMIN_USER_IDS`` but not in ``admin_roles``
    is treated as ``super`` for backward-compat — that's the whole
    deployment today, so any other behaviour would lock the operator
    out the moment this PR ships."""
    assert effective_role(7, None, is_env_admin=True) == "super"


def test_effective_role_returns_none_for_non_admin():
    assert effective_role(7, None, is_env_admin=False) is None


def test_effective_role_returns_none_for_none_telegram_id():
    """Anonymous channel posts have ``message.from_user`` ``None``;
    surface that as 'no admin', not a defaulted role."""
    assert effective_role(None, None, is_env_admin=False) is None
    assert effective_role(None, "super", is_env_admin=True) is None


def test_effective_role_invalid_db_role_falls_back_to_env_list():
    """A corrupted ``admin_roles.role`` value (e.g. left over from a
    manual SQL fix that pre-dated the CHECK constraint) shouldn't
    lock the legacy env-list admin out — fall through to the
    ``is_env_admin`` branch instead of returning ``None``."""
    assert effective_role(7, "owner", is_env_admin=True) == "super"
    assert effective_role(7, "owner", is_env_admin=False) is None


def test_effective_role_normalizes_db_role_casing():
    """A legacy row written in mixed case (or with whitespace) still
    resolves cleanly via :func:`normalize_role`."""
    assert effective_role(7, "Operator", is_env_admin=False) == "operator"
    assert effective_role(7, " SUPER ", is_env_admin=False) == "super"
