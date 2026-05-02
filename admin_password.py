"""DB-backed override layer for the admin-panel password.

Stage-15-Step-E #10b row 25. Until now the admin panel password was
``ADMIN_PASSWORD`` env-only — rotation required SSH + ``.env`` edit
+ bot restart. This module adds a DB-backed scrypt-hashed override
slot so a super-floored operator can rotate the password from
``/admin/profile`` without touching the VPS.

Resolution order on login:

1. DB-backed scrypt hash (preferred when set) — any plaintext that
   verifies against the stored hash is accepted.
2. env ``ADMIN_PASSWORD`` plaintext (constant-time compare) —
   back-compat for fresh deploys that haven't rotated yet.

Storage format: ``scrypt$<n>$<r>$<p>$<salt_b64>$<hash_b64>``. The
``system_settings.ADMIN_PASSWORD_HASH`` row carries that string;
never the plaintext.

Default scrypt cost factors: ``n=2**15`` (32 768), ``r=8``, ``p=1``
— ≈30 ms / verify on a modern VPS, the same ballpark as the
``rate_limit.consume_login_token`` 30 s refill window so a wrong-
password response stays cheap enough to keep the per-IP throttle's
constant-time guarantees while still kneecapping online brute force.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import logging
import secrets
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database import Database

log = logging.getLogger("bot.admin_password")


# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

ADMIN_PASSWORD_HASH_SETTING_KEY: str = "ADMIN_PASSWORD_HASH"

# OWASP modern-practice floor (length-over-complexity). Anything
# shorter than 12 is "weak by default". The MAX is a DoS guard —
# scrypt input feeds straight into hashlib so an 8 GiB password
# would happily pin a CPU at verify time.
MIN_PASSWORD_LENGTH: int = 12
MAX_PASSWORD_LENGTH: int = 1024

# scrypt cost factors. ``n`` MUST be a power of two; ``(n * r * p)``
# is implicitly bounded by ``hashlib.scrypt``'s internal checks. With
# ``n=2**15``, ``r=8``, ``p=1``: 32 768 × 128 = 4 MiB working set,
# ≈30 ms on a modern VPS — fast enough for the wrong-password
# response to stay under the per-IP throttle's 30 s refill window,
# slow enough that a 10⁴-attempt brute force takes ≥5 minutes per
# token bucket refill (and the rate-limit caps that at one attempt
# per 30 s anyway).
SCRYPT_N: int = 2 ** 15
SCRYPT_R: int = 8
SCRYPT_P: int = 1
SCRYPT_KEYLEN: int = 64
SCRYPT_SALT_BYTES: int = 16

# ``hashlib.scrypt``'s ``maxmem`` defaults to 32 MiB which is JUST
# below the working-set of n=2**15, r=8 (≈33.5 MiB). We pass a 128 MiB
# ceiling to leave headroom for the standard parameters AND any
# future bump up to ``SCRYPT_N_MAX`` without rewriting verify-time
# checks. The derivation itself doesn't actually allocate this much —
# the ceiling only acts as a "max acceptable" gate.
SCRYPT_MAXMEM: int = 128 * 1024 * 1024

# Hash-parse safety bounds. A maliciously-large stored ``n`` could
# DoS the verify path; the bounds reject obviously-poisoned values
# before scrypt is even invoked.
SCRYPT_N_MAX: int = 2 ** 20
SCRYPT_R_MAX: int = 64
SCRYPT_P_MAX: int = 64
SCRYPT_SALT_MIN_BYTES: int = 8
SCRYPT_HASH_MIN_BYTES: int = 16

# Module-level cache of the currently-resolved hash. ``None`` means
# "no DB override; fall back to env``ADMIN_PASSWORD``".
_ADMIN_PASSWORD_HASH_OVERRIDE: str | None = None


# ------------------------------------------------------------------
# Strength validator
# ------------------------------------------------------------------

def validate_password_strength(plaintext: object) -> str | None:
    """Return ``None`` on success, else a human-readable error.

    Bias toward "long & boring" rather than "short & forced ALL
    THE TYPES" — modern NIST 800-63B / OWASP guidance. We require
    a letter AND at least one digit-or-symbol so the operator at
    least types something past pure alphabetics; we DON'T require
    upper+lower+digit+symbol because that pushes operators into
    predictable "Password1!" patterns.
    """
    if not isinstance(plaintext, str):
        return "password must be a string"
    if len(plaintext) < MIN_PASSWORD_LENGTH:
        return (
            f"password must be at least {MIN_PASSWORD_LENGTH} "
            f"characters long"
        )
    if len(plaintext) > MAX_PASSWORD_LENGTH:
        return (
            f"password must be no more than {MAX_PASSWORD_LENGTH} "
            f"characters long"
        )
    if not plaintext.strip():
        return "password must not be whitespace-only"
    has_letter = any(c.isalpha() for c in plaintext)
    has_other = any(not c.isalpha() for c in plaintext)
    if not (has_letter and has_other):
        return (
            "password must contain at least one letter and at "
            "least one digit or symbol"
        )
    return None


# ------------------------------------------------------------------
# Hash format
# ------------------------------------------------------------------

def _b64encode(raw: bytes) -> str:
    """Standard base64 without ``=`` padding (storage compactness)."""
    return base64.b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(s: str) -> bytes:
    pad = (-len(s)) % 4
    return base64.b64decode(s + "=" * pad)


def hash_password(
    plaintext: str,
    *,
    n: int = SCRYPT_N,
    r: int = SCRYPT_R,
    p: int = SCRYPT_P,
) -> str:
    """Return a stored-form scrypt hash of *plaintext*.

    The cost factors ``n / r / p`` are encoded into the stored value
    so a future parameter bump can verify older hashes alongside
    newer ones without a flag-day rotation.
    """
    if not isinstance(plaintext, str):
        raise TypeError("hash_password: plaintext must be str")
    if not isinstance(n, int) or isinstance(n, bool) or n < 2:
        raise ValueError("hash_password: n must be a positive int ≥ 2")
    if (n & (n - 1)) != 0:
        raise ValueError("hash_password: n must be a power of two")
    if not isinstance(r, int) or isinstance(r, bool) or r < 1:
        raise ValueError("hash_password: r must be a positive int")
    if not isinstance(p, int) or isinstance(p, bool) or p < 1:
        raise ValueError("hash_password: p must be a positive int")
    salt = secrets.token_bytes(SCRYPT_SALT_BYTES)
    digest = hashlib.scrypt(
        plaintext.encode("utf-8"),
        salt=salt,
        n=n, r=r, p=p,
        maxmem=SCRYPT_MAXMEM,
        dklen=SCRYPT_KEYLEN,
    )
    return f"scrypt${n}${r}${p}${_b64encode(salt)}${_b64encode(digest)}"


def verify_password(plaintext: object, stored: object) -> bool:
    """Constant-time compare of *plaintext* against *stored*.

    Returns ``False`` on any parse failure / type mismatch — a
    malformed stored value behaves like "no hash configured", never
    like "any password matches".
    """
    if not isinstance(plaintext, str) or not isinstance(stored, str):
        return False
    parts = stored.split("$")
    if len(parts) != 6 or parts[0] != "scrypt":
        return False
    try:
        n = int(parts[1])
        r = int(parts[2])
        p = int(parts[3])
        salt = _b64decode(parts[4])
        expected = _b64decode(parts[5])
    except (ValueError, binascii.Error):
        return False
    # Re-validate the parameters we'd accept so a stored hash with
    # absurd cost factors can't DoS verify time.
    if n < 2 or (n & (n - 1)) != 0 or n > SCRYPT_N_MAX:
        return False
    if r < 1 or r > SCRYPT_R_MAX:
        return False
    if p < 1 or p > SCRYPT_P_MAX:
        return False
    if len(salt) < SCRYPT_SALT_MIN_BYTES:
        return False
    if len(expected) < SCRYPT_HASH_MIN_BYTES:
        return False
    try:
        actual = hashlib.scrypt(
            plaintext.encode("utf-8"),
            salt=salt,
            n=n, r=r, p=p,
            maxmem=SCRYPT_MAXMEM,
            dklen=len(expected),
        )
    except (ValueError, MemoryError):
        return False
    return hmac.compare_digest(actual, expected)


# ------------------------------------------------------------------
# Override accessors
# ------------------------------------------------------------------

def set_admin_password_hash_override(value: str) -> None:
    """Replace the in-process hash override.

    Validates the stored format up front so an unparseable value
    never makes it into the cache."""
    global _ADMIN_PASSWORD_HASH_OVERRIDE
    if not isinstance(value, str):
        raise TypeError(
            "set_admin_password_hash_override: value must be str"
        )
    parts = value.split("$")
    if len(parts) != 6 or parts[0] != "scrypt":
        raise ValueError(
            "set_admin_password_hash_override: stored hash must be "
            "the scrypt$N$r$p$salt$hash format"
        )
    _ADMIN_PASSWORD_HASH_OVERRIDE = value


def clear_admin_password_hash_override() -> bool:
    """Drop the in-process override.  Returns ``True`` if one was set."""
    global _ADMIN_PASSWORD_HASH_OVERRIDE
    had = _ADMIN_PASSWORD_HASH_OVERRIDE is not None
    _ADMIN_PASSWORD_HASH_OVERRIDE = None
    return had


def get_admin_password_hash_override() -> str | None:
    """Return the current in-process override (or ``None``)."""
    return _ADMIN_PASSWORD_HASH_OVERRIDE


# ------------------------------------------------------------------
# DB refresh
# ------------------------------------------------------------------

async def refresh_admin_password_hash_from_db(
    db: "Database | None",
) -> str | None:
    """Reload the hash from ``system_settings``.

    Transient DB errors keep the previous cache (fail-soft); a row
    that exists but doesn't parse as scrypt-formatted is treated
    as "no override" (login falls back to env). Returns the active
    cache value (or ``None``).
    """
    global _ADMIN_PASSWORD_HASH_OVERRIDE
    if db is None:
        return _ADMIN_PASSWORD_HASH_OVERRIDE
    try:
        raw = await db.get_setting(ADMIN_PASSWORD_HASH_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_admin_password_hash_from_db: get_setting failed; "
            "keeping previous cache",
        )
        return _ADMIN_PASSWORD_HASH_OVERRIDE
    if raw is None:
        _ADMIN_PASSWORD_HASH_OVERRIDE = None
        return None
    if not isinstance(raw, str):
        log.warning(
            "refresh_admin_password_hash_from_db: rejected non-string "
            "stored value type=%s; clearing override",
            type(raw).__name__,
        )
        _ADMIN_PASSWORD_HASH_OVERRIDE = None
        return None
    parts = raw.split("$")
    if len(parts) != 6 or parts[0] != "scrypt":
        log.warning(
            "refresh_admin_password_hash_from_db: malformed stored "
            "hash; clearing override",
        )
        _ADMIN_PASSWORD_HASH_OVERRIDE = None
        return None
    _ADMIN_PASSWORD_HASH_OVERRIDE = raw
    return raw


# ------------------------------------------------------------------
# Resolver
# ------------------------------------------------------------------

def verify_admin_password(
    plaintext: object, env_expected: str = "",
) -> bool:
    """Verify a submitted plaintext against the active credential.

    Resolution order:

    1. DB-backed scrypt hash, if present — any plaintext that
       verifies against the stored hash is accepted.
    2. ``env_expected`` plaintext compare (back-compat for deploys
       that haven't rotated yet).

    Both paths are constant-time on the actual compare. Returns
    ``False`` for non-string *plaintext* / when neither credential
    is configured.
    """
    if not isinstance(plaintext, str):
        return False
    if _ADMIN_PASSWORD_HASH_OVERRIDE:
        return verify_password(plaintext, _ADMIN_PASSWORD_HASH_OVERRIDE)
    if not isinstance(env_expected, str) or not env_expected:
        return False
    return hmac.compare_digest(plaintext, env_expected)


def get_admin_password_source(env_expected: str = "") -> str:
    """Return ``db`` / ``env`` / ``unset`` so the profile page can
    show where the live password actually lives.
    """
    if _ADMIN_PASSWORD_HASH_OVERRIDE:
        return "db"
    if isinstance(env_expected, str) and env_expected:
        return "env"
    return "unset"
