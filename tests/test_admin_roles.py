"""Tests for the :mod:`admin_roles` module — Stage-15-Step-E #5
first slice. Covers the role hierarchy, env parsing, predicates,
and the snapshot accessor.
"""

from __future__ import annotations

import os

import admin_roles
from admin_roles import Role


def _reset() -> None:
    """Wipe all role env vars and reload from env so each test
    starts with empty buckets."""
    for k in (
        admin_roles.ENV_VIEWER,
        admin_roles.ENV_OPERATOR,
        admin_roles.ENV_SUPER,
    ):
        os.environ.pop(k, None)
    admin_roles.reload_from_env()


# ---- Role enum -----------------------------------------------------


def test_role_enum_ordered_by_value():
    assert Role.VIEWER.value == 10
    assert Role.OPERATOR.value == 20
    assert Role.SUPER.value == 30
    # Strict ordering required for the has_role hierarchy contract.
    assert Role.VIEWER < Role.OPERATOR < Role.SUPER


def test_role_enum_values_spaced_for_future_intermediate():
    """The 10/20/30 spacing is deliberate — a future ``MODERATOR=15``
    must be insertable without renumbering. Pin the numeric values
    so a refactor that re-bases them gets caught.
    """
    assert Role.OPERATOR.value - Role.VIEWER.value == 10
    assert Role.SUPER.value - Role.OPERATOR.value == 10


# ---- _parse_role_user_ids -----------------------------------------


def test_parse_role_user_ids_empty():
    assert admin_roles._parse_role_user_ids("", "X") == frozenset()
    assert admin_roles._parse_role_user_ids(None, "X") == frozenset()
    assert admin_roles._parse_role_user_ids("   ", "X") == frozenset()


def test_parse_role_user_ids_simple():
    assert admin_roles._parse_role_user_ids("1,2,3", "X") == frozenset({1, 2, 3})


def test_parse_role_user_ids_strips_whitespace():
    assert admin_roles._parse_role_user_ids(
        " 1 , 2 ,3", "X"
    ) == frozenset({1, 2, 3})


def test_parse_role_user_ids_dedupes():
    assert admin_roles._parse_role_user_ids("1,2,1,3,2", "X") == frozenset(
        {1, 2, 3}
    )


def test_parse_role_user_ids_ignores_non_integer(caplog):
    """Tolerant: a typo doesn't crash startup, just gets a WARNING."""
    with caplog.at_level("WARNING"):
        result = admin_roles._parse_role_user_ids("1,bad,3", "FOO_VAR")
    assert result == frozenset({1, 3})
    assert any(
        "FOO_VAR" in record.getMessage() and "bad" in record.getMessage()
        for record in caplog.records
    )


def test_parse_role_user_ids_ignores_non_positive(caplog):
    """``0`` and negatives are dropped + logged. They wouldn't grant
    access to anyone (no real user has id <= 0) but bloating the
    frozenset for snapshot tooling is sloppy.
    """
    with caplog.at_level("WARNING"):
        result = admin_roles._parse_role_user_ids("1,0,-5,2", "BAR")
    assert result == frozenset({1, 2})
    msgs = [r.getMessage() for r in caplog.records]
    assert any("0" in m and "BAR" in m for m in msgs)
    assert any("-5" in m and "BAR" in m for m in msgs)


# ---- get_user_role -------------------------------------------------


def test_get_user_role_returns_super_when_in_super_bucket():
    _reset()
    os.environ[admin_roles.ENV_SUPER] = "100"
    admin_roles.reload_from_env()
    assert admin_roles.get_user_role(100) is Role.SUPER
    _reset()


def test_get_user_role_returns_operator():
    _reset()
    os.environ[admin_roles.ENV_OPERATOR] = "200"
    admin_roles.reload_from_env()
    assert admin_roles.get_user_role(200) is Role.OPERATOR
    _reset()


def test_get_user_role_returns_viewer():
    _reset()
    os.environ[admin_roles.ENV_VIEWER] = "300"
    admin_roles.reload_from_env()
    assert admin_roles.get_user_role(300) is Role.VIEWER
    _reset()


def test_get_user_role_returns_none_for_unknown():
    _reset()
    assert admin_roles.get_user_role(999) is None
    _reset()


def test_get_user_role_returns_none_for_none_input():
    _reset()
    assert admin_roles.get_user_role(None) is None
    _reset()


def test_get_user_role_returns_none_for_zero():
    _reset()
    assert admin_roles.get_user_role(0) is None
    _reset()


def test_get_user_role_returns_none_for_negative():
    _reset()
    assert admin_roles.get_user_role(-12345) is None
    _reset()


def test_get_user_role_super_wins_over_lower_when_listed_in_both():
    """Defensive: an id listed in both ADMIN_USER_IDS and
    ADMIN_VIEWER_USER_IDS should resolve to SUPER, not VIEWER —
    the most-privileged grant wins. Guards against a half-completed
    "demote" change leaving the SUPER entry behind."""
    _reset()
    os.environ[admin_roles.ENV_SUPER] = "777"
    os.environ[admin_roles.ENV_VIEWER] = "777"
    admin_roles.reload_from_env()
    assert admin_roles.get_user_role(777) is Role.SUPER
    _reset()


def test_get_user_role_operator_wins_over_viewer_when_listed_in_both():
    _reset()
    os.environ[admin_roles.ENV_OPERATOR] = "888"
    os.environ[admin_roles.ENV_VIEWER] = "888"
    admin_roles.reload_from_env()
    assert admin_roles.get_user_role(888) is Role.OPERATOR
    _reset()


# ---- has_role ------------------------------------------------------


def test_has_role_super_passes_all_levels():
    _reset()
    os.environ[admin_roles.ENV_SUPER] = "100"
    admin_roles.reload_from_env()
    assert admin_roles.has_role(100, Role.VIEWER)
    assert admin_roles.has_role(100, Role.OPERATOR)
    assert admin_roles.has_role(100, Role.SUPER)
    _reset()


def test_has_role_operator_passes_viewer_and_operator():
    _reset()
    os.environ[admin_roles.ENV_OPERATOR] = "200"
    admin_roles.reload_from_env()
    assert admin_roles.has_role(200, Role.VIEWER)
    assert admin_roles.has_role(200, Role.OPERATOR)
    assert not admin_roles.has_role(200, Role.SUPER)
    _reset()


def test_has_role_viewer_passes_only_viewer():
    _reset()
    os.environ[admin_roles.ENV_VIEWER] = "300"
    admin_roles.reload_from_env()
    assert admin_roles.has_role(300, Role.VIEWER)
    assert not admin_roles.has_role(300, Role.OPERATOR)
    assert not admin_roles.has_role(300, Role.SUPER)
    _reset()


def test_has_role_non_admin_fails_all():
    _reset()
    assert not admin_roles.has_role(999, Role.VIEWER)
    assert not admin_roles.has_role(999, Role.OPERATOR)
    assert not admin_roles.has_role(999, Role.SUPER)
    _reset()


def test_has_role_none_fails_all():
    _reset()
    assert not admin_roles.has_role(None, Role.VIEWER)
    assert not admin_roles.has_role(None, Role.OPERATOR)
    assert not admin_roles.has_role(None, Role.SUPER)
    _reset()


# ---- get_admins_for_role -------------------------------------------


def test_get_admins_for_viewer_returns_all_admins():
    _reset()
    os.environ[admin_roles.ENV_VIEWER] = "10"
    os.environ[admin_roles.ENV_OPERATOR] = "20"
    os.environ[admin_roles.ENV_SUPER] = "30"
    admin_roles.reload_from_env()
    assert admin_roles.get_admins_for_role(Role.VIEWER) == frozenset(
        {10, 20, 30}
    )
    _reset()


def test_get_admins_for_operator_excludes_viewers():
    _reset()
    os.environ[admin_roles.ENV_VIEWER] = "10"
    os.environ[admin_roles.ENV_OPERATOR] = "20"
    os.environ[admin_roles.ENV_SUPER] = "30"
    admin_roles.reload_from_env()
    assert admin_roles.get_admins_for_role(Role.OPERATOR) == frozenset(
        {20, 30}
    )
    _reset()


def test_get_admins_for_super_returns_only_super():
    _reset()
    os.environ[admin_roles.ENV_VIEWER] = "10"
    os.environ[admin_roles.ENV_OPERATOR] = "20"
    os.environ[admin_roles.ENV_SUPER] = "30"
    admin_roles.reload_from_env()
    assert admin_roles.get_admins_for_role(Role.SUPER) == frozenset({30})
    _reset()


def test_get_admins_for_role_empty_when_no_admins():
    _reset()
    assert admin_roles.get_admins_for_role(Role.VIEWER) == frozenset()
    assert admin_roles.get_admins_for_role(Role.OPERATOR) == frozenset()
    assert admin_roles.get_admins_for_role(Role.SUPER) == frozenset()
    _reset()


# ---- set_admin_role_user_ids (test override) ----------------------


def test_set_admin_role_user_ids_overrides():
    _reset()
    admin_roles.set_admin_role_user_ids(Role.SUPER, {500, 600})
    assert admin_roles.get_user_role(500) is Role.SUPER
    assert admin_roles.get_user_role(600) is Role.SUPER
    _reset()


def test_set_admin_role_user_ids_drops_non_positive():
    _reset()
    admin_roles.set_admin_role_user_ids(Role.SUPER, {0, -1, 700})
    assert admin_roles.get_user_role(0) is None
    assert admin_roles.get_user_role(-1) is None
    assert admin_roles.get_user_role(700) is Role.SUPER
    _reset()


def test_set_admin_role_user_ids_drops_non_int():
    _reset()
    admin_roles.set_admin_role_user_ids(
        Role.SUPER, ["abc", "1", "2x", 3]  # type: ignore[list-item]
    )
    # Only the parseable positive ints survive.
    assert admin_roles.get_user_role(1) is Role.SUPER
    assert admin_roles.get_user_role(3) is Role.SUPER
    _reset()


# ---- role_status_snapshot -----------------------------------------


def test_role_status_snapshot_shape():
    _reset()
    os.environ[admin_roles.ENV_VIEWER] = "10,20"
    os.environ[admin_roles.ENV_OPERATOR] = "30"
    os.environ[admin_roles.ENV_SUPER] = "40,50,60"
    admin_roles.reload_from_env()
    snap = admin_roles.role_status_snapshot()
    assert set(snap.keys()) == {"VIEWER", "OPERATOR", "SUPER"}
    assert snap["VIEWER"] == [10, 20]
    assert snap["OPERATOR"] == [30]
    assert snap["SUPER"] == [40, 50, 60]
    _reset()


def test_role_status_snapshot_returns_sorted_lists():
    """Pin: snapshot order is stable so dashboards / tests
    don't depend on hash randomization.
    """
    _reset()
    os.environ[admin_roles.ENV_SUPER] = "30,10,20"
    admin_roles.reload_from_env()
    snap = admin_roles.role_status_snapshot()
    assert snap["SUPER"] == [10, 20, 30]
    _reset()


# ---- reload_from_env ------------------------------------------------


def test_reload_from_env_picks_up_new_env_vars():
    _reset()
    assert admin_roles.get_admins_for_role(Role.SUPER) == frozenset()
    os.environ[admin_roles.ENV_SUPER] = "999"
    admin_roles.reload_from_env()
    assert admin_roles.get_admins_for_role(Role.SUPER) == frozenset({999})
    _reset()
    # After reset, the previous SUPER assignment must be cleared.
    assert admin_roles.get_admins_for_role(Role.SUPER) == frozenset()
