"""Admin role hierarchy for Meowassist.

Stage-15-Step-E #5 first slice — STARTED, not finished.

Pre-this-module Meowassist had a single admin gate: ``is_admin``
in :mod:`admin` either returned True (full access) or False (no
access at all). Every admin command (broadcast, credit, debit,
promo create / list / revoke) checked this same boolean. That's
fine for a one-operator deploy but brittle the moment a second
person needs visibility — they get all-or-nothing.

The original Step-E suggestion table row 5 reads::

    Admin role system — currently all admins have full access.
    Add roles: viewer (read-only dashboard), operator (can
    broadcast, manage promos), super (can edit users, refund).
    Store in DB, not env.

This module ships the **first slice**: introduce the role
hierarchy + the predicates that callers will gate on, sourced
from env vars (the cheapest backing store, identical pattern
to the existing :data:`admin._ADMIN_USER_IDS`). The "store in
DB, not env" follow-up is intentionally deferred — it's a
schema change + alembic migration + admin UI for editing the
table, all of which is naturally a separate PR. Building the
predicates first means the next PR can simply swap the env
parser out for a DB query without touching any of the call
sites that adopt the predicates between now and then.

Role hierarchy
==============

We use a strictly-ordered ``IntEnum`` so a "required role" check
can compare numerically::

    Role.VIEWER   = 10
    Role.OPERATOR = 20
    Role.SUPER    = 30

Higher numeric value = more permissions. ``has_role(uid, OPERATOR)``
returns True if the user is an OPERATOR **or** a SUPER. The numbers
are intentionally spaced (10 / 20 / 30) so a future intermediate
role (``MODERATOR=15``, say) can be inserted without shifting any
existing constant — important because the enum value gets recorded
in audit log lines and we don't want a renumbering to make old logs
ambiguous.

Role grants by role:

* **VIEWER** — read-only dashboard / metrics / balance lookup. Can
  inspect but cannot move money or change state. The right level
  for a stakeholder who wants to see usage without being trusted
  to edit anything.
* **OPERATOR** — VIEWER + broadcast + promo create / list / revoke.
  Can change marketing-side state but cannot touch user balances.
  The right level for a community manager.
* **SUPER** — OPERATOR + admin_credit + admin_debit + future
  user-edit endpoints. Money-touching commands. Reserved for the
  bot owner / a small core team.

Backward compatibility
======================

The pre-existing ``ADMIN_USER_IDS`` env var keeps its meaning:
ids listed there are granted **SUPER** role. This means existing
deploys flip over with no config change. The two new env vars are
opt-in:

* ``ADMIN_VIEWER_USER_IDS`` — comma-separated ids granted VIEWER.
* ``ADMIN_OPERATOR_USER_IDS`` — comma-separated ids granted OPERATOR.

If the same id appears in multiple lists, the **highest** role wins
(e.g. an id in both ``ADMIN_USER_IDS`` and ``ADMIN_VIEWER_USER_IDS``
ends up as SUPER, not VIEWER — defence against an operator
demotion that left the SUPER entry behind).

Public surface
==============

* :func:`get_user_role` — return the highest role granted to a
  user, or ``None`` if they aren't an admin at all.
* :func:`has_role` — predicate "user has at least this role".
* :func:`get_admins_for_role` — frozenset of users at OR above a
  given role (used by notifiers to fan-out only to ops/super).
* :func:`set_admin_role_user_ids` — runtime override for tests.
* :func:`reload_from_env` — re-parse the env vars (also for tests
  / hot-reload scenarios; production reads at import time).

The :mod:`admin` module's existing :func:`admin.is_admin` and
:func:`admin.get_admin_user_ids` remain in place and continue to
work — :func:`admin.is_admin(uid)` is now equivalent to
``has_role(uid, Role.VIEWER)`` (any role qualifies as "admin").

What's NOT in this slice
========================

* DB-backed role storage. The env-only first slice ships the
  predicates without the persistence layer. Migration to a DB
  table is a separate PR (see HANDOFF.md §5 boundary doc).
* Per-handler role-gate refactor. The new predicates exist but
  every existing handler still calls :func:`admin.is_admin`. That's
  intentional: bundling the role-split with a 9-handler refactor
  inside the same PR makes it harder to review and increases the
  risk that a single typo nukes admin access entirely. The
  per-handler wiring is the next PR after this.
* A web admin endpoint to view/edit role assignments. Belongs in
  the same DB-migration follow-up.
* Audit logging of role-elevation attempts. Useful for forensic
  work but not first-slice critical.
"""

from __future__ import annotations

import enum
import logging
import os

log = logging.getLogger("bot.admin_roles")


class Role(enum.IntEnum):
    """Ordered admin role hierarchy.

    ``IntEnum`` so callers can compare numerically:
    ``has_role(uid, Role.OPERATOR)`` is implemented as
    ``Role(get_user_role(uid)) >= Role.OPERATOR``.
    """

    VIEWER = 10
    OPERATOR = 20
    SUPER = 30


# ---------------------------------------------------------------------
# Env var names.
#
# Spelled out explicitly here so the rest of the module doesn't pass
# raw strings around — typos in env var names are silent failures
# (the var simply isn't found and the role bucket is empty), so we
# bind the names once and refer to the constants everywhere.
# ---------------------------------------------------------------------

ENV_VIEWER = "ADMIN_VIEWER_USER_IDS"
ENV_OPERATOR = "ADMIN_OPERATOR_USER_IDS"
# ``ADMIN_USER_IDS`` is the long-standing super-admin env var; we
# read the same one for back-compat (any id in there is granted
# SUPER role).
ENV_SUPER = "ADMIN_USER_IDS"


def _parse_role_user_ids(raw: str | None, env_name: str) -> frozenset[int]:
    """Parse a comma-separated list of Telegram user ids.

    Mirrors the tolerant behaviour of :func:`admin.parse_admin_user_ids`:
    None / empty → empty set; whitespace-only entries silently skipped;
    non-integer entries logged at WARNING and dropped so a typo doesn't
    crash the bot at startup.

    ``env_name`` is included in the WARNING so the operator can tell
    *which* env var has the typo when several roles are misconfigured
    at once.
    """
    if not raw:
        return frozenset()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            uid = int(part)
        except ValueError:
            log.warning(
                "%s: ignoring non-integer entry %r", env_name, part
            )
            continue
        # Telegram user ids are positive 64-bit integers. Negative
        # values are chat / supergroup ids by aiogram convention,
        # not user ids. ``0`` is also impossible for any real user.
        # Drop those rather than silently granting a role to a
        # bogus id (defensive — wouldn't actually grant access to
        # anyone since no real user has id <= 0, but keeps the
        # frozenset clean for snapshot tooling).
        if uid <= 0:
            log.warning(
                "%s: ignoring non-positive id %r "
                "(Telegram user ids are positive)",
                env_name, part,
            )
            continue
        out.add(uid)
    return frozenset(out)


# Module-level role buckets. Read once at import; can be overridden at
# runtime via :func:`set_admin_role_user_ids` (tests) or
# :func:`reload_from_env` (env hot-reload).
_role_user_ids: dict[Role, frozenset[int]] = {
    Role.VIEWER: _parse_role_user_ids(os.getenv(ENV_VIEWER), ENV_VIEWER),
    Role.OPERATOR: _parse_role_user_ids(os.getenv(ENV_OPERATOR), ENV_OPERATOR),
    Role.SUPER: _parse_role_user_ids(os.getenv(ENV_SUPER), ENV_SUPER),
}


def reload_from_env() -> None:
    """Re-parse the role env vars. Mostly for tests; production
    reads at module-import time."""
    global _role_user_ids
    _role_user_ids = {
        Role.VIEWER: _parse_role_user_ids(
            os.getenv(ENV_VIEWER), ENV_VIEWER
        ),
        Role.OPERATOR: _parse_role_user_ids(
            os.getenv(ENV_OPERATOR), ENV_OPERATOR
        ),
        Role.SUPER: _parse_role_user_ids(
            os.getenv(ENV_SUPER), ENV_SUPER
        ),
    }


def set_admin_role_user_ids(
    role: Role, ids: frozenset[int] | set[int] | list[int]
) -> None:
    """Override one role's id set at runtime. Intended for tests."""
    global _role_user_ids
    new_ids: set[int] = set()
    for i in ids:
        try:
            uid = int(i)
        except (TypeError, ValueError):
            log.warning(
                "set_admin_role_user_ids: dropping non-int id %r", i
            )
            continue
        if uid <= 0:
            log.warning(
                "set_admin_role_user_ids: dropping non-positive id %r",
                i,
            )
            continue
        new_ids.add(uid)
    _role_user_ids = {**_role_user_ids, role: frozenset(new_ids)}


def get_user_role(telegram_id: int | None) -> Role | None:
    """Return the highest role granted to *telegram_id*, or None.

    "Highest" is by numeric value of :class:`Role`. If the same id is
    listed under multiple env vars, the most-privileged grant wins —
    a SUPER entry shadows any VIEWER / OPERATOR entry for the same id.

    None is returned for non-admins, ``None`` input, or any id ≤ 0
    (defensive — see :func:`_parse_role_user_ids` for the reasoning).
    """
    if telegram_id is None:
        return None
    if telegram_id <= 0:
        return None
    # Walk roles from highest to lowest so the first match is the
    # most-privileged grant. ``IntEnum`` iteration order matches
    # declaration order; we explicitly sort by value descending
    # so adding a future intermediate role doesn't depend on the
    # declaration order.
    for role in sorted(_role_user_ids, key=lambda r: r.value, reverse=True):
        if telegram_id in _role_user_ids[role]:
            return role
    return None


def has_role(telegram_id: int | None, required: Role) -> bool:
    """Return True iff *telegram_id* has at least *required* role.

    Hierarchy is honoured: a SUPER passes ``has_role(uid, OPERATOR)``,
    an OPERATOR passes ``has_role(uid, VIEWER)``, etc. A user with no
    role at all returns False.

    Designed as a drop-in replacement for the existing
    :func:`admin.is_admin` predicate at call sites that want to
    gate at a specific level.
    """
    actual = get_user_role(telegram_id)
    if actual is None:
        return False
    return actual.value >= required.value


def get_admins_for_role(required: Role) -> frozenset[int]:
    """Return the set of user ids at OR above *required* role.

    Used by notifiers (model-discovery DM, pending-alert DM, fx-rate
    failure DM) when the next iteration wants to fan out only to
    OPERATOR-and-above rather than every admin. First slice doesn't
    rewire the existing notifiers — they continue to call
    :func:`admin.get_admin_user_ids` (= every admin = every role) so
    the behaviour is unchanged. The follow-up PR can switch each
    notifier to the appropriate role threshold.
    """
    out: set[int] = set()
    for role, ids in _role_user_ids.items():
        if role.value >= required.value:
            out.update(ids)
    return frozenset(out)


def role_status_snapshot() -> dict[str, list[int]]:
    """Diagnostic dict: role name → sorted id list.

    Stable shape for ops dashboards / future ``/admin_roles`` command.
    Returns sorted lists rather than the raw frozensets so the output
    is deterministic across Python versions / hash-randomization.
    """
    return {
        role.name: sorted(_role_user_ids[role])
        for role in sorted(_role_user_ids, key=lambda r: r.value)
    }
