"""Stage-15-Step-E #5 — admin role hierarchy + lookup helpers.

Today the admin surface is a flat env-list (``ADMIN_USER_IDS``). Step-E #5
introduces a DB-tracked, graduated set of roles so the team can scale
beyond a single super-admin without handing every operator the keys to
the wallet credit/debit flow.

Three roles, lowest → highest privilege:

* ``viewer``   — read the admin dashboard and audit log. No writes.
* ``operator`` — viewer's surface + broadcasts, promo codes, gift codes.
                 Cannot edit wallet balances.
* ``super``    — operator's surface + wallet credit/debit + refunds +
                 user-field edits. Same surface as a legacy env-list admin.

This first slice ships:

* The role hierarchy (constants + ``role_at_least`` ordering check).
* ``effective_role(...)`` resolution helper that honours the env-list
  fallback for backward-compat (a Telegram id in ``ADMIN_USER_IDS`` is
  treated as ``super`` even when not yet present in the ``admin_roles``
  DB table).
* The DB primitives (``Database.get_admin_role`` etc) live in
  ``database.py`` — this module imports them lazily through the
  passed-in ``db_role`` argument so the helper stays sync + cheap.

Subsequent PRs will:
* Wire ``role_at_least(...)`` into the existing admin command handlers
  (e.g. gate ``/admin_credit`` to ``role >= super``, ``/admin_broadcast``
  to ``role >= operator``, ``/admin_metrics`` to ``role >= viewer``).
* Add a ``/admin/roles`` web page for the operator to manage roles
  through a browser instead of Telegram DMs.

Why not OAuth / SSO: same trade-off as the rest of the admin surface.
The bot already has the env-list and the HMAC cookie; layering external
identity providers on top is multi-week work for a single-operator
deployment. A DB-tracked role table is the smallest step that captures
the "scale beyond one admin" use case the user asked about.
"""

from __future__ import annotations


# ---------------------------------------------------------------------
# Role constants + ordering
# ---------------------------------------------------------------------
#
# ``ROLE_ORDER`` is the source of truth for the privilege hierarchy.
# ``role_at_least`` is the only consumer; everything else (typing,
# validation, DB CHECK) keys off ``VALID_ROLES``.

ROLE_VIEWER = "viewer"
ROLE_OPERATOR = "operator"
ROLE_SUPER = "super"

# Lowest → highest privilege. Index into this tuple for ordering.
ROLE_ORDER: tuple[str, ...] = (ROLE_VIEWER, ROLE_OPERATOR, ROLE_SUPER)

VALID_ROLES: frozenset[str] = frozenset(ROLE_ORDER)


def normalize_role(raw: str | None) -> str | None:
    """Lowercase + strip + validate. Returns ``None`` for unknown
    inputs so callers can branch on a single sentinel.

    The DB CHECK constraint enforces this on the write side; this
    helper is the read-side / parser-side counterpart so a user typing
    ``" Super "`` into ``/admin_role_grant`` still lands cleanly.
    """
    if raw is None:
        return None
    cleaned = raw.strip().lower()
    if cleaned not in VALID_ROLES:
        return None
    return cleaned


def role_at_least(role: str | None, required: str) -> bool:
    """Return ``True`` iff *role* meets or exceeds the *required* minimum.

    Defensive on both sides:

    * Unknown / ``None`` / mistyped *role* always returns ``False`` —
      "we couldn't determine your role" must NEVER mean "you have access".
    * Unknown / mistyped *required* also returns ``False`` — a typo in
      the call-site (``role_at_least(r, "supr")``) should fail closed,
      not match every input.

    Comparison is by index in :data:`ROLE_ORDER`, so adding a new role
    is a matter of inserting it at the right index there.
    """
    if role not in VALID_ROLES or required not in VALID_ROLES:
        return False
    return ROLE_ORDER.index(role) >= ROLE_ORDER.index(required)


def effective_role(
    telegram_id: int | None,
    db_role: str | None,
    *,
    is_env_admin: bool,
) -> str | None:
    """Resolve a Telegram user's effective admin role.

    Resolution order:

    1. ``db_role`` — when present + valid, win. The DB row is the source
       of truth once an operator has graduated the user from the env
       list.
    2. ``is_env_admin`` — backward-compat fallback. A Telegram id in
       ``ADMIN_USER_IDS`` is treated as :data:`ROLE_SUPER` so the
       legacy admin surface keeps working without forcing an
       op-by-op DB seed.
    3. Otherwise ``None`` (not an admin).

    The caller is responsible for fetching ``db_role`` (via
    ``Database.get_admin_role``) and computing ``is_env_admin`` (via
    ``admin.is_admin``); we keep this helper sync + dependency-free so
    it can be used from contexts (tests, formatters) that don't have
    access to the DB pool.
    """
    if telegram_id is None:
        return None
    if db_role is not None:
        normalized = normalize_role(db_role)
        if normalized is not None:
            return normalized
        # ``db_role`` was set but invalid (corrupted row). Fall through
        # to the env-list check rather than locking the legacy admin
        # out — fail-soft beats fail-closed when the operator is
        # actively recovering from a bad SQL fix.
    if is_env_admin:
        return ROLE_SUPER
    return None


__all__ = [
    "ROLE_VIEWER",
    "ROLE_OPERATOR",
    "ROLE_SUPER",
    "ROLE_ORDER",
    "VALID_ROLES",
    "normalize_role",
    "role_at_least",
    "effective_role",
]
