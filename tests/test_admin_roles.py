"""Stage-15-Step-E #5 — admin role hierarchy + lookup helpers.

These tests pin the role-comparison contract (``role_at_least`` is the
single chokepoint for every future gate) and the env-list-fallback
behaviour of ``effective_role``.
"""

from __future__ import annotations

import logging

import pytest

from admin_roles import (
    ROLE_ORDER,
    ROLE_OPERATOR,
    ROLE_SUPER,
    ROLE_VIEWER,
    VALID_ROLES,
    effective_role,
    ensure_env_admins_have_roles,
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


# ---------------------------------------------------------------------
# ensure_env_admins_have_roles — Stage-15-Step-E #5 follow-up #3
# ---------------------------------------------------------------------


class _FakeRoleDB:
    """Minimal stand-in for ``database.Database`` exposing only the
    two methods :func:`ensure_env_admins_have_roles` calls.

    Records every UPSERT in ``writes`` so tests can assert ordering,
    payload shape, and idempotency without weaving the asyncpg fixture.
    """

    def __init__(
        self,
        existing: dict[int, str] | None = None,
        *,
        get_raise_for: set[int] | None = None,
        set_raise_for: set[int] | None = None,
    ):
        self._roles = dict(existing or {})
        self._get_raise_for = set(get_raise_for or set())
        self._set_raise_for = set(set_raise_for or set())
        self.writes: list[tuple[int, str, str | None]] = []

    async def get_admin_role(self, telegram_id: int) -> str | None:
        if telegram_id in self._get_raise_for:
            raise RuntimeError("simulated DB read failure")
        return self._roles.get(int(telegram_id))

    async def set_admin_role(
        self,
        telegram_id: int,
        role: str,
        *,
        granted_by: int | None = None,
        notes: str | None = None,
    ) -> str:
        if telegram_id in self._set_raise_for:
            raise RuntimeError("simulated DB write failure")
        # Mirror the production semantics — UPSERT.
        self._roles[int(telegram_id)] = role
        self.writes.append((int(telegram_id), role, notes))
        return role


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_promotes_missing():
    db = _FakeRoleDB()
    counts = await ensure_env_admins_have_roles(db, [1001, 2002])
    assert counts == {
        "promoted": 2,
        "skipped_existing": 0,
        "skipped_invalid": 0,
        "errors": 0,
    }
    assert sorted(t for t, _, _ in db.writes) == [1001, 2002]
    # Both rows should land at ``super`` per the contract.
    assert all(role == "super" for _, role, _ in db.writes)


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_does_not_downgrade():
    """A user already in ``admin_roles`` with ``operator`` (because a
    super demoted them but their env entry was kept as a safety net)
    must NOT be overwritten back to super."""
    db = _FakeRoleDB(existing={500: "operator", 600: "viewer"})
    counts = await ensure_env_admins_have_roles(db, [500, 600, 700])
    assert counts == {
        "promoted": 1,
        "skipped_existing": 2,
        "skipped_invalid": 0,
        "errors": 0,
    }
    # Only the missing id (700) was written.
    assert db.writes == [(700, "super", "auto-promoted from ADMIN_USER_IDS at boot")]


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_idempotent_on_second_call():
    """A second invocation finds the rows from the first and bumps
    ``skipped_existing`` instead of rewriting."""
    db = _FakeRoleDB()
    first = await ensure_env_admins_have_roles(db, [1001, 2002])
    second = await ensure_env_admins_have_roles(db, [1001, 2002])
    assert first["promoted"] == 2
    assert second == {
        "promoted": 0,
        "skipped_existing": 2,
        "skipped_invalid": 0,
        "errors": 0,
    }
    # Only one write per id total (idempotent).
    assert sorted(t for t, _, _ in db.writes) == [1001, 2002]


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_skips_non_positive(caplog):
    db = _FakeRoleDB()
    with caplog.at_level(logging.WARNING, logger="bot.admin_roles"):
        counts = await ensure_env_admins_have_roles(db, [-5, 0, 42])
    assert counts == {
        "promoted": 1,
        "skipped_existing": 0,
        "skipped_invalid": 2,
        "errors": 0,
    }
    assert db.writes == [(42, "super", "auto-promoted from ADMIN_USER_IDS at boot")]
    # WARN logs should fire for both 0 and -5.
    warn_messages = [r.message for r in caplog.records]
    assert any("non-positive" in m for m in warn_messages)


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_skips_non_int(caplog):
    db = _FakeRoleDB()
    with caplog.at_level(logging.WARNING, logger="bot.admin_roles"):
        # Mix of types — stringy ints (which int() coerces fine), a
        # garbage string, and a float-like value.
        counts = await ensure_env_admins_have_roles(
            db, [42, "not-a-number", None, "0", 100]
        )
    # 42 + 100 promote; "0" coerces to 0 and is rejected (non-positive);
    # "not-a-number" + None bump skipped_invalid via the int() except.
    assert counts == {
        "promoted": 2,
        "skipped_existing": 0,
        "skipped_invalid": 3,
        "errors": 0,
    }
    assert sorted(t for t, _, _ in db.writes) == [42, 100]


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_handles_get_failure():
    """A transient DB read failure for one id should not stop
    promotion of the others."""
    db = _FakeRoleDB(get_raise_for={500})
    counts = await ensure_env_admins_have_roles(db, [500, 600])
    assert counts == {
        "promoted": 1,
        "skipped_existing": 0,
        "skipped_invalid": 0,
        "errors": 1,
    }
    # 500 was skipped due to the read failure; 600 still promoted.
    assert db.writes == [(600, "super", "auto-promoted from ADMIN_USER_IDS at boot")]


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_handles_set_failure():
    """A transient DB write failure for one id should not stop the
    next one from succeeding."""
    db = _FakeRoleDB(set_raise_for={500})
    counts = await ensure_env_admins_have_roles(db, [500, 600])
    assert counts == {
        "promoted": 1,
        "skipped_existing": 0,
        "skipped_invalid": 0,
        "errors": 1,
    }
    # Only 600 lands; 500 raised on the UPSERT.
    assert db.writes == [(600, "super", "auto-promoted from ADMIN_USER_IDS at boot")]


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_dedupes_input():
    """Caller passing the same id twice (e.g. after ``parse_admin_user_ids``
    semantics changed) should still only write once."""
    db = _FakeRoleDB()
    counts = await ensure_env_admins_have_roles(db, [42, 42, 42])
    assert counts["promoted"] == 1
    assert db.writes == [(42, "super", "auto-promoted from ADMIN_USER_IDS at boot")]


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_empty_input_no_op():
    db = _FakeRoleDB()
    counts = await ensure_env_admins_have_roles(db, [])
    assert counts == {
        "promoted": 0,
        "skipped_existing": 0,
        "skipped_invalid": 0,
        "errors": 0,
    }
    assert db.writes == []


@pytest.mark.asyncio
async def test_ensure_env_admins_have_roles_custom_notes():
    db = _FakeRoleDB()
    await ensure_env_admins_have_roles(
        db, [42], notes="custom-bootstrap-marker"
    )
    assert db.writes == [(42, "super", "custom-bootstrap-marker")]


# ---------------------------------------------------------------------
# Bundled bug fix regression — parse_admin_user_ids drops non-positive ids
# ---------------------------------------------------------------------


def test_parse_admin_user_ids_drops_non_positive(caplog):
    """Bug fix: a typo (`123,-456`) or accidental chat-id paste would
    silently put a never-matchable row in the admin set, and (with
    Stage-15-Step-E #5 follow-up #3 on top) seed a bogus
    ``admin_roles`` row in the DB. Parser drops them now."""
    from admin import parse_admin_user_ids

    with caplog.at_level(logging.WARNING, logger="bot.admin"):
        result = parse_admin_user_ids("123,-456,0,789")
    assert result == frozenset({123, 789})
    warn_messages = [r.message for r in caplog.records]
    assert any("non-positive" in m for m in warn_messages)
    # Both -456 and 0 should be flagged — two separate WARN records.
    assert sum(1 for m in warn_messages if "non-positive" in m) == 2
