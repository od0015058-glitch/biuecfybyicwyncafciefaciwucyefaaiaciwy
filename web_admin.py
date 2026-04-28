"""Web admin panel — Stage-8.

Mounts a small aiohttp + jinja2 dashboard under ``/admin/`` on the same
web app that already serves ``/nowpayments-webhook``. One process, one
Dockerfile, one deploy.

Auth model:
    * The admin password is set via ``ADMIN_PASSWORD`` env var.
    * On successful login, we set a ``meow_admin`` cookie carrying a
      base64url-encoded ``"<expires_at_iso>|<hmac>"`` payload, signed
      with ``ADMIN_SESSION_SECRET`` via HMAC-SHA256.
    * Every protected route re-verifies the cookie. A tampered or
      expired cookie is treated identically to "not logged in" → 302
      back to ``/admin/login``.
    * Cookies are issued with ``HttpOnly`` + ``SameSite=Lax``. Set
      ``ADMIN_COOKIE_SECURE=1`` (the default) so they're also
      ``Secure``-flagged and only sent over HTTPS — turn this off
      *only* when running locally over plain HTTP.

Why HMAC cookies instead of server-side sessions:
    * No session store to persist or rotate.
    * Stateless — restart the bot mid-day and admins stay logged in
      until their cookie expires.
    * One person, low concurrency, no need for revocation primitives.

Pages so far:
    * ``GET  /admin/login``           login form               (Part-1)
    * ``POST /admin/login``           check password           (Part-1)
    * ``GET  /admin/logout``          clear cookie             (Part-1)
    * ``GET  /admin/``                dashboard / metrics      (Part-1)
    * ``GET  /admin/promos``          list + create form       (Part-2)
    * ``POST /admin/promos``          create new promo         (Part-2)
    * ``POST /admin/promos/{code}/revoke``  soft-delete a code (Part-2)

CSRF defence (Part-2):
    Even though the session cookie is ``SameSite=Lax`` (which already
    blocks cross-site form-POSTs in modern browsers), every POST form
    additionally carries a hidden ``csrf_token`` field. The token is
    derived from ``HMAC-SHA256(session_secret, "csrf:" + session_cookie)``
    and validated via constant-time compare. This is belt-and-suspenders
    defence — older browsers, proxy quirks, future cookie-attribute
    changes — and means we don't have to assume browser SameSite
    enforcement to keep the admin panel safe.

Flash messages (Part-2):
    A short-lived ``meow_flash`` cookie (10s TTL, signed) carries a
    one-shot status banner across the redirect after a POST. Server
    reads + clears on next render. Survives the redirect cycle without
    needing a server-side session store.

Subsequent Stage-8-Part-* PRs add /admin/gifts, /admin/users,
/admin/broadcast, /admin/transactions on top of this scaffold.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp_jinja2
import jinja2
from aiohttp import web

# Imported for the class-level ``TRANSACTIONS_GATEWAY_VALUES`` /
# ``TRANSACTIONS_STATUS_VALUES`` allow-lists used by
# parse_transactions_query (Stage-8-Part-6). Only the class is
# referenced — not the module-level ``db`` singleton — so the admin
# still works against the injected DB in tests.
from database import Database
from rate_limit import (
    client_ip_for_rate_limit,
    consume_login_token,
    install_login_rate_limit,
)

log = logging.getLogger("bot.web_admin")

# Cookie name + key for stashing the validated identity into the request
# scope so handlers can read it without re-running HMAC. We deliberately
# don't expose username / id on the login page — there's only ever one
# admin password — but we set a stable identity string so audit lines
# can attribute actions to "the admin who logged in at <ts>".
COOKIE_NAME = "meow_admin"

# aiohttp 3.9+ wants typed ``AppKey`` for ``app[...]`` storage instead
# of bare string keys (otherwise it emits ``NotAppKeyWarning`` which
# our pytest config (filterwarnings=error) escalates to a failure).
# Same pattern used in ``rate_limit.WEBHOOK_RATE_LIMIT_CACHE_KEY``.
APP_KEY_PASSWORD: web.AppKey = web.AppKey("admin_password", str)
APP_KEY_SESSION_SECRET: web.AppKey = web.AppKey("admin_session_secret", str)
APP_KEY_TTL_HOURS: web.AppKey = web.AppKey("admin_ttl_hours", int)
APP_KEY_COOKIE_SECURE: web.AppKey = web.AppKey("admin_cookie_secure", bool)
APP_KEY_DB: web.AppKey = web.AppKey("admin_db", object)
APP_KEY_INSTALLED: web.AppKey = web.AppKey("admin_routes_installed", bool)
# Stage-8-Part-5: the aiogram ``Bot`` used by the broadcast page's
# background task to send Telegram messages. Optional — handlers
# render a friendly banner and refuse to start jobs when it's absent
# (e.g. unit tests that don't wire up a bot).
APP_KEY_BOT: web.AppKey = web.AppKey("admin_bot", object)
# In-memory registry of broadcast jobs. Keyed by short uuid, values
# are dicts of the shape documented in :func:`_new_broadcast_job`.
# Bounded to ``BROADCAST_MAX_HISTORY`` entries (oldest evicted once
# completed so an active job is never dropped). State is lost on
# process restart — the Telegram ``/admin_broadcast`` command has the
# same semantics, so this matches operator expectations.
APP_KEY_BROADCAST_JOBS: web.AppKey = web.AppKey(
    "admin_broadcast_jobs", dict
)
# Background asyncio.Task handles, kept so setup_admin_routes can
# cancel in-flight jobs during a clean app shutdown.
APP_KEY_BROADCAST_TASKS: web.AppKey = web.AppKey(
    "admin_broadcast_tasks", dict
)

# Per-request flag set by the auth middleware. ``request[]`` doesn't
# emit NotAppKeyWarning for string keys (only ``app[]`` does), so
# this stays a plain string for readability.
REQUEST_KEY_AUTHED = "admin_authed"

# Default cookie lifetime — long enough that the user isn't constantly
# logging back in, short enough that a stolen cookie auto-expires
# within a day. Override via ``ADMIN_SESSION_TTL_HOURS``.
DEFAULT_TTL_HOURS = 24

# Templates live next to this module so a `pip install -e .` deploy
# (or a Docker COPY) doesn't lose them.
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates" / "admin"


# ---------------------------------------------------------------------
# Cookie signing helpers (pure functions, easy to unit-test)
# ---------------------------------------------------------------------


def _b64url_encode(data: bytes) -> str:
    """URL-safe base64 without padding — friendlier in cookie values."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def sign_cookie(
    expires_at: datetime,
    *,
    secret: str,
) -> str:
    """Return a signed cookie value carrying *expires_at* (UTC ISO).

    Format: ``<b64(iso)>.<b64(hmac_sha256(iso))>`` — no per-user
    payload, since there's only one admin identity. Plenty of headroom
    if we want to add user_id / role later.
    """
    if not secret:
        raise ValueError("ADMIN_SESSION_SECRET must not be empty")
    iso = expires_at.astimezone(timezone.utc).isoformat()
    iso_bytes = iso.encode("utf-8")
    sig = hmac.new(
        secret.encode("utf-8"), iso_bytes, hashlib.sha256
    ).digest()
    return f"{_b64url_encode(iso_bytes)}.{_b64url_encode(sig)}"


def verify_cookie(
    raw: str | None,
    *,
    secret: str,
    now: datetime | None = None,
) -> bool:
    """Return True iff *raw* is a well-formed, unexpired, valid cookie.

    Constant-time signature compare. A None / empty / malformed cookie
    is silently treated as invalid — never raises. We intentionally
    don't surface "expired" vs "tampered" vs "malformed" to the caller
    because the only correct action in any case is "redirect to login".
    """
    if not raw or not secret:
        return False
    try:
        iso_b64, sig_b64 = raw.split(".", 1)
        iso_bytes = _b64url_decode(iso_b64)
        provided_sig = _b64url_decode(sig_b64)
    except (ValueError, base64.binascii.Error):
        return False

    expected_sig = hmac.new(
        secret.encode("utf-8"), iso_bytes, hashlib.sha256
    ).digest()
    if not hmac.compare_digest(expected_sig, provided_sig):
        return False

    try:
        expires_at = datetime.fromisoformat(iso_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return False

    if expires_at.tzinfo is None:
        # Defensive: a cookie missing tz info almost certainly means a
        # signing bug elsewhere, so refuse it.
        return False

    if now is None:
        now = datetime.now(timezone.utc)
    return now < expires_at


# ---------------------------------------------------------------------
# Auth middleware / decorator
# ---------------------------------------------------------------------


@web.middleware
async def admin_auth_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Annotate the request with whether the caller is logged in.

    Doesn't redirect on its own — `_require_auth` handles that. The
    middleware just stamps the request so per-route handlers can
    check ``request[REQUEST_KEY_AUTHED]`` cheaply.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    raw = request.cookies.get(COOKIE_NAME)
    request[REQUEST_KEY_AUTHED] = verify_cookie(raw, secret=secret)
    return await handler(request)


def _require_auth(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    """Wrap a handler so it 302s to /admin/login when not authed."""

    async def wrapper(request: web.Request) -> web.StreamResponse:
        if not request.get(REQUEST_KEY_AUTHED, False):
            return web.HTTPFound(location="/admin/login")
        return await handler(request)

    wrapper.__name__ = handler.__name__
    return wrapper


# ---------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------


async def login_get(request: web.Request) -> web.StreamResponse:
    # Already logged in? Bounce to the dashboard.
    if request.get(REQUEST_KEY_AUTHED, False):
        return web.HTTPFound(location="/admin/")
    return aiohttp_jinja2.render_template(
        "login.html", request, {"error": None}
    )


async def login_post(request: web.Request) -> web.StreamResponse:
    expected = request.app.get(APP_KEY_PASSWORD, "")
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    ttl_hours = request.app.get(APP_KEY_TTL_HOURS, DEFAULT_TTL_HOURS)
    secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    if not expected or not secret:
        # Misconfigured deploy — refuse to log anyone in. Better to
        # surface this loudly than silently let in everyone with an
        # empty cookie.
        log.error(
            "Login attempted but ADMIN_PASSWORD or ADMIN_SESSION_SECRET "
            "is unset — refusing."
        )
        return aiohttp_jinja2.render_template(
            "login.html",
            request,
            {"error": "Admin panel is not configured. Set ADMIN_PASSWORD and ADMIN_SESSION_SECRET."},
            status=500,
        )

    # Per-IP token-bucket throttle. Keyed via the shared ``client_ip_for
    # _rate_limit`` helper so a reverse-proxy deploy with
    # ``TRUST_PROXY_HEADERS=1`` actually buckets per real client rather
    # than collapsing every attempt onto the proxy's TCP address. The
    # throttle runs BEFORE the password compare so a spraying attacker
    # doesn't get constant-time feedback on their guesses.
    client_key = client_ip_for_rate_limit(request)
    if not await consume_login_token(request.app, client_key):
        log.warning(
            "admin login: rate-limited key=%s remote=%s",
            client_key, request.remote,
        )
        return aiohttp_jinja2.render_template(
            "login.html",
            request,
            {"error": "Too many login attempts. Please wait a minute."},
            status=429,
        )

    form = await request.post()
    submitted = str(form.get("password", ""))

    # Constant-time compare on the bytes — the lengths can differ, but
    # ``compare_digest`` handles that without leaking which one was
    # right.
    if not hmac.compare_digest(submitted, expected):
        log.warning(
            "admin login: bad password key=%s remote=%s",
            client_key, request.remote,
        )
        return aiohttp_jinja2.render_template(
            "login.html",
            request,
            {"error": "Wrong password."},
            status=401,
        )

    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
    cookie_value = sign_cookie(expires_at, secret=secret)

    log.info(
        "admin login: success from %s (cookie expires %s)",
        request.remote, expires_at.isoformat(),
    )
    response = web.HTTPFound(location="/admin/")
    response.set_cookie(
        COOKIE_NAME,
        cookie_value,
        max_age=int(timedelta(hours=ttl_hours).total_seconds()),
        httponly=True,
        secure=secure,
        samesite="Lax",
        path="/admin/",
    )
    return response


async def logout(request: web.Request) -> web.StreamResponse:
    response = web.HTTPFound(location="/admin/login")
    response.del_cookie(COOKIE_NAME, path="/admin/")
    return response


async def dashboard(request: web.Request) -> web.StreamResponse:
    db = request.app.get(APP_KEY_DB)
    metrics: dict
    db_error: str | None = None
    if db is None:
        # Local dev / unit-test path where the app didn't get a Database
        # wired up. Render with empty data so the UI is at least visible.
        # Keys MUST match ``Database.get_system_metrics`` (and
        # ``admin.format_metrics`` consumers) so the template renders
        # the same in dev / DB-error / live: users_total, users_active_7d,
        # revenue_usd, spend_usd, top_models[{model,count,cost_usd}].
        metrics = {
            "users_total": 0,
            "users_active_7d": 0,
            "revenue_usd": 0.0,
            "spend_usd": 0.0,
            "top_models": [],
        }
        db_error = "No database wired up (development mode)."
    else:
        try:
            metrics = await db.get_system_metrics()
        except Exception:
            log.exception("dashboard: get_system_metrics failed")
            # Same shape as the dev-mode fallback above — see comment.
            metrics = {
                "users_total": 0,
                "users_active_7d": 0,
                "revenue_usd": 0.0,
                "spend_usd": 0.0,
                "top_models": [],
            }
            db_error = "Database query failed — see logs."

    return aiohttp_jinja2.render_template(
        "dashboard.html",
        request,
        {
            "metrics": metrics,
            "db_error": db_error,
            "active_page": "dashboard",
        },
    )


# ---------------------------------------------------------------------
# CSRF + flash helpers (Stage-8-Part-2)
# ---------------------------------------------------------------------


FLASH_COOKIE = "meow_flash"
FLASH_TTL_SECONDS = 10  # one redirect hop is plenty


def csrf_token_for(request: web.Request) -> str:
    """Return the CSRF token for this request's logged-in session.

    Derived deterministically from the session cookie value via
    ``HMAC-SHA256(secret, "csrf:" + cookie_value)`` so we don't need
    a server-side store. A new login produces a new cookie produces
    a new token, so logging out invalidates pending form tokens
    automatically.

    Returns "" when there is no logged-in session — handlers that
    render forms (which always require auth) won't hit this path.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_value = request.cookies.get(COOKIE_NAME, "")
    if not secret or not cookie_value:
        return ""
    digest = hmac.new(
        secret.encode("utf-8"),
        b"csrf:" + cookie_value.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return _b64url_encode(digest)


def verify_csrf_token(request: web.Request, submitted: str) -> bool:
    """Constant-time compare of *submitted* against the expected token."""
    expected = csrf_token_for(request)
    if not expected or not submitted:
        return False
    return hmac.compare_digest(expected, submitted)


def set_flash(
    response: web.StreamResponse,
    *,
    kind: str,
    message: str,
    secret: str,
    cookie_secure: bool = True,
) -> None:
    """Stash a one-shot status banner in a short-lived signed cookie.

    The next request to render a page reads + clears it via
    :func:`pop_flash`. Survives the post-redirect-get cycle without a
    server-side session store. Signed so a malicious user can't inject
    arbitrary banner text via cookie tampering.

    *kind* is one of "success" / "error" / "info" — controls the CSS
    class on the rendered banner. *message* is plain text (no HTML).
    """
    if not secret:
        return  # half-configured deploy — silently skip the banner
    payload = f"{kind}|{message}".encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    value = f"{_b64url_encode(payload)}.{_b64url_encode(sig)}"
    response.set_cookie(
        FLASH_COOKIE,
        value,
        max_age=FLASH_TTL_SECONDS,
        httponly=True,
        secure=cookie_secure,
        samesite="Lax",
        path="/admin/",
    )


def pop_flash(
    request: web.Request, response: web.StreamResponse
) -> dict | None:
    """Read and clear the flash cookie. Returns ``{kind, message}`` or None.

    Called by GET handlers right before rendering. The mutation on
    *response* (``del_cookie``) is what makes it one-shot — the
    browser's next request won't carry it.
    """
    raw = request.cookies.get(FLASH_COOKIE)
    if not raw:
        return None
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    if not secret:
        # Can't verify, so don't trust. Clear it anyway so a stale
        # cookie doesn't haunt the user.
        response.del_cookie(FLASH_COOKIE, path="/admin/")
        return None
    try:
        payload_b64, sig_b64 = raw.split(".", 1)
        payload = _b64url_decode(payload_b64)
        provided_sig = _b64url_decode(sig_b64)
    except (ValueError, base64.binascii.Error):
        response.del_cookie(FLASH_COOKIE, path="/admin/")
        return None
    expected_sig = hmac.new(
        secret.encode("utf-8"), payload, hashlib.sha256
    ).digest()
    if not hmac.compare_digest(expected_sig, provided_sig):
        response.del_cookie(FLASH_COOKIE, path="/admin/")
        return None
    try:
        text = payload.decode("utf-8")
        kind, message = text.split("|", 1)
    except (UnicodeDecodeError, ValueError):
        response.del_cookie(FLASH_COOKIE, path="/admin/")
        return None
    response.del_cookie(FLASH_COOKIE, path="/admin/")
    return {"kind": kind, "message": message}


# ---------------------------------------------------------------------
# Promo codes (Stage-8-Part-2)
# ---------------------------------------------------------------------


# Discount upper bound — DECIMAL(10,4) max is 999_999.9999 (alembic
# 0001 / promo_codes.discount_amount). We cap a touch lower so the
# parser never produces a value that would crash the INSERT with
# "numeric field overflow". This also guards the Telegram-side
# /admin_promo_create command via the shared validator.
DISCOUNT_AMOUNT_MAX = 999_999.0


# Days-until-expiry upper bound. ``timedelta(days=N)`` raises
# ``OverflowError`` for N > 999_999_999, and Postgres TIMESTAMPTZ
# silently overflows beyond year 294276. 100 years is far longer
# than any sane gift-code campaign and stays well below both limits.
# Without this cap, an admin pasting a giant integer in the form
# crashed the create handler with an uncaught OverflowError → 500
# (instead of a friendly red banner).
EXPIRES_IN_DAYS_MAX = 36_500


def parse_promo_form(form) -> dict | str:
    """Parse the /admin/promos create form. Mirror of
    :func:`admin.parse_promo_create_args` for the Telegram-side
    command but takes named form fields instead of positional CLI args.

    Returns a dict shaped::

        {
          "code": "WELCOME20",
          "discount_percent": 20 | None,
          "discount_amount": None | float,
          "max_uses": int | None,
          "expires_in_days": int | None,
        }

    On failure returns a short error key the caller can render:
    ``"missing_code"``, ``"bad_code"``, ``"missing_discount"``,
    ``"bad_discount_kind"``, ``"bad_percent"``, ``"bad_amount"``,
    ``"discount_too_large"``, ``"bad_max_uses"``, ``"bad_days"``.
    """
    code_raw = (form.get("code") or "").strip()
    if not code_raw:
        return "missing_code"
    code = code_raw.upper()
    if len(code) > 64 or not all(c.isalnum() or c in "_-" for c in code):
        return "bad_code"

    kind = (form.get("discount_kind") or "").strip().lower()
    if kind not in ("percent", "amount"):
        return "bad_discount_kind"

    raw_value = (form.get("discount_value") or "").strip()
    if not raw_value:
        return "missing_discount"

    discount_percent: int | None = None
    discount_amount: float | None = None
    if kind == "percent":
        # Strip a trailing % so admins can paste "20%" or "20" both.
        cleaned = raw_value.rstrip("%").strip()
        try:
            pct = int(cleaned)
        except ValueError:
            return "bad_percent"
        if not (1 <= pct <= 100):
            return "bad_percent"
        discount_percent = pct
    else:
        cleaned = raw_value.lstrip("$").strip()
        try:
            amount = float(cleaned)
        except ValueError:
            return "bad_amount"
        # NaN / Inf / non-positive
        if not (amount == amount) or amount in (
            float("inf"), float("-inf")
        ) or amount <= 0:
            return "bad_amount"
        if amount > DISCOUNT_AMOUNT_MAX:
            return "discount_too_large"
        discount_amount = round(amount, 4)

    raw_max = (form.get("max_uses") or "").strip()
    max_uses: int | None = None
    if raw_max:
        try:
            max_uses = int(raw_max)
        except ValueError:
            return "bad_max_uses"
        if max_uses <= 0:
            return "bad_max_uses"

    raw_days = (form.get("expires_in_days") or "").strip()
    expires_in_days: int | None = None
    if raw_days:
        try:
            expires_in_days = int(raw_days)
        except ValueError:
            return "bad_days"
        if expires_in_days <= 0:
            return "bad_days"
        if expires_in_days > EXPIRES_IN_DAYS_MAX:
            return "days_too_large"

    return {
        "code": code,
        "discount_percent": discount_percent,
        "discount_amount": discount_amount,
        "max_uses": max_uses,
        "expires_in_days": expires_in_days,
    }


_PROMO_FORM_ERR_TEXT = {
    "missing_code": "Enter a code.",
    "bad_code": "Code must be 1-64 chars, letters/numbers/_/- only.",
    "missing_discount": "Enter a discount value.",
    "bad_discount_kind": "Pick a discount type (percent or amount).",
    "bad_percent": "Percent must be a whole number between 1 and 100.",
    "bad_amount": "Amount must be a positive number (USD).",
    "discount_too_large": (
        f"Amount must be at most ${DISCOUNT_AMOUNT_MAX:,.2f} (DB limit)."
    ),
    "bad_max_uses": "Max uses must be a positive integer (or leave blank).",
    "bad_days": "Days-until-expiry must be a positive integer (or leave blank).",
    "days_too_large": (
        f"Days-until-expiry must be at most {EXPIRES_IN_DAYS_MAX:,} (≈100 years)."
    ),
}


async def promos_get(request: web.Request) -> web.StreamResponse:
    """List promo codes + render the create form."""
    db = request.app.get(APP_KEY_DB)
    rows: list = []
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        try:
            rows = await db.list_promo_codes(limit=100)
        except Exception:
            log.exception("promos_get: list_promo_codes failed")
            db_error = "Database query failed — see logs."

    response = aiohttp_jinja2.render_template(
        "promos.html",
        request,
        {
            "rows": rows,
            "db_error": db_error,
            "active_page": "promos",
            "csrf_token": csrf_token_for(request),
            "flash": None,  # filled in below
        },
    )
    flash = pop_flash(request, response)
    if flash is not None:
        # Re-render with the flash. Doing it this way keeps pop_flash
        # cheap (one HMAC) and idempotent (always clears the cookie
        # exactly once per page load).
        response = aiohttp_jinja2.render_template(
            "promos.html",
            request,
            {
                "rows": rows,
                "db_error": db_error,
                "active_page": "promos",
                "csrf_token": csrf_token_for(request),
                "flash": flash,
            },
        )
        # Re-emit the cleared cookie on the new response.
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def promos_create(request: web.Request) -> web.StreamResponse:
    """Handle POST /admin/promos — create a new promo code.

    Always 302s back to /admin/promos with a flash message describing
    the outcome. We never re-render the form with errors inline because
    that would mean the URL `/admin/promos` would look like a state
    machine rather than a stable list view; flash messages keep nav
    predictable.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    form = await request.post()

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning("promos_create: CSRF token mismatch from %s", request.remote)
        response = web.HTTPFound(location="/admin/promos")
        set_flash(
            response,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    parsed = parse_promo_form(form)
    response = web.HTTPFound(location="/admin/promos")
    if isinstance(parsed, str):
        set_flash(
            response,
            kind="error",
            message=_PROMO_FORM_ERR_TEXT.get(parsed, f"Invalid input ({parsed})."),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot create.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    expires_at = None
    if parsed["expires_in_days"] is not None:
        expires_at = datetime.now(timezone.utc) + timedelta(
            days=parsed["expires_in_days"]
        )

    try:
        ok = await db.create_promo_code(
            code=parsed["code"],
            discount_percent=parsed["discount_percent"],
            discount_amount=parsed["discount_amount"],
            max_uses=parsed["max_uses"],
            expires_at=expires_at,
        )
    except ValueError as exc:
        set_flash(
            response,
            kind="error",
            message=str(exc),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response
    except Exception:
        log.exception("promos_create: create_promo_code failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if not ok:
        set_flash(
            response,
            kind="error",
            message=f"Code '{parsed['code']}' already exists. Pick another or revoke the existing one first.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if parsed["discount_percent"] is not None:
        disc = f"{parsed['discount_percent']}%"
    else:
        disc = f"${parsed['discount_amount']:.2f}"
    cap_label = (
        f"{parsed['max_uses']} uses"
        if parsed["max_uses"] is not None
        else "unlimited uses"
    )
    exp_label = (
        f", expires in {parsed['expires_in_days']} days"
        if parsed["expires_in_days"] is not None
        else ""
    )
    log.info(
        "web_admin promos_create: code=%s disc=%s cap=%s",
        parsed["code"], disc, parsed["max_uses"],
    )
    set_flash(
        response,
        kind="success",
        message=f"Created '{parsed['code']}': {disc}, {cap_label}{exp_label}.",
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return response


async def promos_revoke(request: web.Request) -> web.StreamResponse:
    """POST /admin/promos/{code}/revoke — soft-delete a promo code."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    form = await request.post()
    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning("promos_revoke: CSRF token mismatch from %s", request.remote)
        response = web.HTTPFound(location="/admin/promos")
        set_flash(
            response,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    code = request.match_info.get("code", "").upper()
    response = web.HTTPFound(location="/admin/promos")
    if not code or len(code) > 64 or not all(
        c.isalnum() or c in "_-" for c in code
    ):
        set_flash(
            response,
            kind="error",
            message="Invalid code in URL.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot revoke.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    try:
        ok = await db.revoke_promo_code(code)
    except Exception:
        log.exception("promos_revoke: revoke_promo_code failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if ok:
        log.info("web_admin promos_revoke: code=%s", code)
        set_flash(
            response,
            kind="success",
            message=f"Revoked '{code}'.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response,
            kind="info",
            message=f"'{code}' was already revoked or doesn't exist.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
    return response


# ---------------------------------------------------------------------
# Gift codes (Stage-8-Part-3)
# ---------------------------------------------------------------------
#
# Distinct from promo codes: gift codes credit balance directly, no
# purchase required. Admin sets "10 people can each redeem $5" → up to
# 10 distinct telegram_ids each get $5 added to their wallet.
#
# Routes:
#   GET  /admin/gifts                             — list + create form
#   POST /admin/gifts                             — create
#   POST /admin/gifts/{code}/revoke               — soft-delete


# DB column is DECIMAL(10,4); cap a hair below the column max so the
# parser never produces a value that would crash the INSERT.
GIFT_AMOUNT_MAX = 999_999.0


def parse_gift_form(form) -> dict | str:
    """Parse the /admin/gifts create form.

    Returns a dict shaped::

        {
          "code": "BIRTHDAY5",
          "amount_usd": 5.0,
          "max_uses": int | None,
          "expires_in_days": int | None,
        }

    On failure returns one of the error keys: ``"missing_code"``,
    ``"bad_code"``, ``"missing_amount"``, ``"bad_amount"``,
    ``"amount_too_large"``, ``"bad_max_uses"``, ``"bad_days"``.
    """
    code_raw = (form.get("code") or "").strip()
    if not code_raw:
        return "missing_code"
    code = code_raw.upper()
    if len(code) > 64 or not all(c.isalnum() or c in "_-" for c in code):
        return "bad_code"

    raw_amount = (form.get("amount_usd") or "").strip()
    if not raw_amount:
        return "missing_amount"
    cleaned = raw_amount.lstrip("$").strip()
    try:
        amount = float(cleaned)
    except ValueError:
        return "bad_amount"
    # NaN / Inf / non-positive
    if not (amount == amount) or amount in (
        float("inf"), float("-inf")
    ) or amount <= 0:
        return "bad_amount"
    if amount > GIFT_AMOUNT_MAX:
        return "amount_too_large"

    raw_max = (form.get("max_uses") or "").strip()
    max_uses: int | None = None
    if raw_max:
        try:
            max_uses = int(raw_max)
        except ValueError:
            return "bad_max_uses"
        if max_uses <= 0:
            return "bad_max_uses"

    raw_days = (form.get("expires_in_days") or "").strip()
    expires_in_days: int | None = None
    if raw_days:
        try:
            expires_in_days = int(raw_days)
        except ValueError:
            return "bad_days"
        if expires_in_days <= 0:
            return "bad_days"
        if expires_in_days > EXPIRES_IN_DAYS_MAX:
            return "days_too_large"

    return {
        "code": code,
        "amount_usd": round(amount, 4),
        "max_uses": max_uses,
        "expires_in_days": expires_in_days,
    }


_GIFT_FORM_ERR_TEXT = {
    "missing_code": "Enter a code.",
    "bad_code": "Code must be 1-64 chars, letters/numbers/_/- only.",
    "missing_amount": "Enter a USD amount each redeemer should receive.",
    "bad_amount": "Amount must be a positive number (USD).",
    "amount_too_large": (
        f"Amount must be at most ${GIFT_AMOUNT_MAX:,.2f} (DB limit)."
    ),
    "bad_max_uses": "Max redemptions must be a positive integer (or leave blank).",
    "bad_days": "Days-until-expiry must be a positive integer (or leave blank).",
    "days_too_large": (
        f"Days-until-expiry must be at most {EXPIRES_IN_DAYS_MAX:,} (≈100 years)."
    ),
}


async def gifts_get(request: web.Request) -> web.StreamResponse:
    """List gift codes + render the create form."""
    db = request.app.get(APP_KEY_DB)
    rows: list = []
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        try:
            rows = await db.list_gift_codes(limit=100)
        except Exception:
            log.exception("gifts_get: list_gift_codes failed")
            db_error = "Database query failed — see logs."

    response = aiohttp_jinja2.render_template(
        "gifts.html",
        request,
        {
            "rows": rows,
            "db_error": db_error,
            "active_page": "gifts",
            "csrf_token": csrf_token_for(request),
            "flash": None,
        },
    )
    flash = pop_flash(request, response)
    if flash is not None:
        response = aiohttp_jinja2.render_template(
            "gifts.html",
            request,
            {
                "rows": rows,
                "db_error": db_error,
                "active_page": "gifts",
                "csrf_token": csrf_token_for(request),
                "flash": flash,
            },
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def gifts_create(request: web.Request) -> web.StreamResponse:
    """POST /admin/gifts — create a new gift code."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    form = await request.post()

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning("gifts_create: CSRF token mismatch from %s", request.remote)
        response = web.HTTPFound(location="/admin/gifts")
        set_flash(
            response,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    parsed = parse_gift_form(form)
    response = web.HTTPFound(location="/admin/gifts")
    if isinstance(parsed, str):
        set_flash(
            response,
            kind="error",
            message=_GIFT_FORM_ERR_TEXT.get(parsed, f"Invalid input ({parsed})."),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot create.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    expires_at = None
    if parsed["expires_in_days"] is not None:
        expires_at = datetime.now(timezone.utc) + timedelta(
            days=parsed["expires_in_days"]
        )

    try:
        ok = await db.create_gift_code(
            code=parsed["code"],
            amount_usd=parsed["amount_usd"],
            max_uses=parsed["max_uses"],
            expires_at=expires_at,
        )
    except ValueError as exc:
        set_flash(
            response,
            kind="error",
            message=str(exc),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response
    except Exception:
        log.exception("gifts_create: create_gift_code failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if not ok:
        set_flash(
            response,
            kind="error",
            message=(
                f"Code '{parsed['code']}' already exists. "
                "Pick another or revoke the existing one first."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    cap_label = (
        f"{parsed['max_uses']} redemptions"
        if parsed["max_uses"] is not None
        else "unlimited"
    )
    exp_label = (
        f", expires in {parsed['expires_in_days']} days"
        if parsed["expires_in_days"] is not None
        else ""
    )
    log.info(
        "web_admin gifts_create: code=%s amount=%s cap=%s",
        parsed["code"], parsed["amount_usd"], parsed["max_uses"],
    )
    set_flash(
        response,
        kind="success",
        message=(
            f"Created '{parsed['code']}': "
            f"${parsed['amount_usd']:.2f} per user, {cap_label}{exp_label}."
        ),
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return response


async def gifts_revoke(request: web.Request) -> web.StreamResponse:
    """POST /admin/gifts/{code}/revoke — soft-delete a gift code."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    form = await request.post()
    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning("gifts_revoke: CSRF token mismatch from %s", request.remote)
        response = web.HTTPFound(location="/admin/gifts")
        set_flash(
            response,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    code = request.match_info.get("code", "").upper()
    response = web.HTTPFound(location="/admin/gifts")
    if not code or len(code) > 64 or not all(
        c.isalnum() or c in "_-" for c in code
    ):
        set_flash(
            response,
            kind="error",
            message="Invalid code in URL.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot revoke.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    try:
        ok = await db.revoke_gift_code(code)
    except Exception:
        log.exception("gifts_revoke: revoke_gift_code failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if ok:
        log.info("web_admin gifts_revoke: code=%s", code)
        set_flash(
            response,
            kind="success",
            message=f"Revoked '{code}'.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response,
            kind="info",
            message=f"'{code}' was already revoked or doesn't exist.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
    return response


# ---------------------------------------------------------------------
# Users (Stage-8-Part-4)
# ---------------------------------------------------------------------
#
# Routes:
#   GET  /admin/users                         — search form + results
#   GET  /admin/users/{telegram_id}           — detail page (balance,
#                                               recent transactions,
#                                               credit/debit form)
#   POST /admin/users/{telegram_id}/adjust    — credit or debit
#
# Admin attribution:
#   The telegram-side /admin_credit flow passes ``message.from_user.id``
#   into ``admin_adjust_balance(admin_telegram_id=...)`` — that id is
#   baked into ``transactions.gateway_invoice_id`` as
#   ``admin-<id>-<ms>-<rand>``. The web panel has no such id (auth is
#   a shared password, not a per-telegram-admin login), so we pass
#   ``0`` as a sentinel and prepend ``[web]`` to the stored ``notes``
#   so the audit trail is unambiguous about the source. ``0`` is safe
#   because real Telegram ids are strictly positive, and the UNIQUE
#   constraint on ``gateway_invoice_id`` still holds (timestamp + 4
#   bytes of randomness give plenty of headroom).

ADMIN_WEB_SENTINEL_ID = 0

# Upper bound on a single adjustment. DB column is DECIMAL(10,4), but
# a six-figure wallet adjustment is almost certainly a fat-fingered
# extra zero — reject loudly so the admin notices before it commits.
# Override via ``WEB_ADMIN_ADJUST_MAX_USD`` if you really do need to
# move more in one shot.
ADJUST_MAX_USD = 100_000.0


def parse_adjust_form(form) -> dict | str:
    """Parse the /admin/users/<id>/adjust form.

    Returns a dict shaped::

        {
          "action": "credit" | "debit",
          "amount_usd": 1.23,
          "reason": "stuck invoice refund",
        }

    On failure returns one of the error keys: ``"bad_action"``,
    ``"missing_amount"``, ``"bad_amount"``, ``"amount_too_large"``,
    ``"missing_reason"``, ``"bad_reason"``.
    """
    action = (form.get("action") or "").strip().lower()
    if action not in ("credit", "debit"):
        return "bad_action"

    raw_amount = (form.get("amount_usd") or "").strip()
    if not raw_amount:
        return "missing_amount"
    cleaned = raw_amount.lstrip("$").strip()
    try:
        amount = float(cleaned)
    except ValueError:
        return "bad_amount"
    if not (amount == amount) or amount in (
        float("inf"), float("-inf")
    ) or amount <= 0:
        return "bad_amount"
    if amount > ADJUST_MAX_USD:
        return "amount_too_large"

    reason = (form.get("reason") or "").strip()
    if not reason:
        return "missing_reason"
    if len(reason) > 500:
        return "bad_reason"

    return {
        "action": action,
        "amount_usd": round(amount, 4),
        "reason": reason,
    }


_ADJUST_FORM_ERR_TEXT = {
    "bad_action": "Pick credit or debit.",
    "missing_amount": "Enter a USD amount.",
    "bad_amount": "Amount must be a positive number (USD).",
    "amount_too_large": (
        f"Amount must be at most ${ADJUST_MAX_USD:,.2f} per adjustment."
    ),
    "missing_reason": "A reason is required (stored in the ledger).",
    "bad_reason": "Reason must be 500 characters or fewer.",
}


async def users_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/users — render the search form + results."""
    db = request.app.get(APP_KEY_DB)
    query = (request.query.get("q") or "").strip()
    rows: list = []
    db_error: str | None = None
    searched = bool(query)
    if searched:
        if db is None:
            db_error = "No database wired up (development mode)."
        else:
            try:
                rows = await db.search_users(query, limit=50)
            except Exception:
                log.exception("users_get: search_users failed")
                db_error = "Database query failed — see logs."

    response = aiohttp_jinja2.render_template(
        "users.html",
        request,
        {
            "query": query,
            "rows": rows,
            "searched": searched,
            "db_error": db_error,
            "active_page": "users",
            "flash": None,
        },
    )
    flash = pop_flash(request, response)
    if flash is not None:
        response = aiohttp_jinja2.render_template(
            "users.html",
            request,
            {
                "query": query,
                "rows": rows,
                "searched": searched,
                "db_error": db_error,
                "active_page": "users",
                "flash": flash,
            },
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def user_detail_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/users/{telegram_id} — detail + adjust form."""
    raw_id = request.match_info.get("telegram_id", "")
    try:
        user_id = int(raw_id)
    except ValueError:
        return web.HTTPFound(location="/admin/users")

    db = request.app.get(APP_KEY_DB)
    summary: dict | None = None
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        try:
            summary = await db.get_user_admin_summary(
                user_id, recent_tx_limit=20
            )
        except Exception:
            log.exception("user_detail_get: get_user_admin_summary failed")
            db_error = "Database query failed — see logs."

    context = {
        "user_id": user_id,
        "summary": summary,
        "db_error": db_error,
        "active_page": "users",
        "csrf_token": csrf_token_for(request),
        "flash": None,
    }
    response = aiohttp_jinja2.render_template(
        "user_detail.html", request, context
    )
    flash = pop_flash(request, response)
    if flash is not None:
        context["flash"] = flash
        response = aiohttp_jinja2.render_template(
            "user_detail.html", request, context
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def user_adjust_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/users/{telegram_id}/adjust — credit or debit.

    Redirects back to the detail page with a signed flash banner
    describing the outcome, mirroring the promos / gifts flows.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    raw_id = request.match_info.get("telegram_id", "")
    try:
        user_id = int(raw_id)
    except ValueError:
        return web.HTTPFound(location="/admin/users")

    form = await request.post()
    detail_url = f"/admin/users/{user_id}"
    response = web.HTTPFound(location=detail_url)

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning(
            "user_adjust_post: CSRF token mismatch from %s", request.remote
        )
        set_flash(
            response,
            kind="error",
            message=(
                "Form submission was rejected (CSRF). "
                "Refresh and try again."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    parsed = parse_adjust_form(form)
    if isinstance(parsed, str):
        set_flash(
            response,
            kind="error",
            message=_ADJUST_FORM_ERR_TEXT.get(
                parsed, f"Invalid input ({parsed})."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot adjust.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    sign = +1 if parsed["action"] == "credit" else -1
    delta = sign * parsed["amount_usd"]
    # Prepend "[web]" so the stored audit note makes the source clear
    # even if someone later grep's transactions.notes looking for
    # telegram-admin activity.
    note = f"[web] {parsed['reason']}"

    try:
        result = await db.admin_adjust_balance(
            telegram_id=user_id,
            delta_usd=delta,
            reason=note,
            admin_telegram_id=ADMIN_WEB_SENTINEL_ID,
        )
    except Exception:
        log.exception("user_adjust_post: admin_adjust_balance failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if result is None:
        # Same disambiguation dance as admin._handle_balance_op: one
        # extra read on the failure path tells us whether the user
        # doesn't exist or the debit would take them below zero.
        try:
            summary = await db.get_user_admin_summary(user_id)
        except Exception:
            log.exception(
                "user_adjust_post: get_user_admin_summary follow-up failed"
            )
            summary = None
        if summary is None:
            set_flash(
                response,
                kind="error",
                message=f"No user with id {user_id}.",
                secret=secret,
                cookie_secure=cookie_secure,
            )
        else:
            set_flash(
                response,
                kind="error",
                message=(
                    f"Refused — debit of ${parsed['amount_usd']:.4f} "
                    f"would take user {user_id} below zero "
                    f"(current balance: ${summary['balance_usd']:.4f})."
                ),
                secret=secret,
                cookie_secure=cookie_secure,
            )
        return response

    log.info(
        "web_admin user_adjust: user=%s delta=$%.4f tx=%d reason=%r",
        user_id, delta, result["transaction_id"], parsed["reason"],
    )
    verb = "Credited" if sign > 0 else "Debited"
    set_flash(
        response,
        kind="success",
        message=(
            f"{verb} user {user_id} ${parsed['amount_usd']:.4f}. "
            f"New balance: ${result['new_balance']:.4f}. "
            f"Tx #{result['transaction_id']}."
        ),
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return response


# ---------------------------------------------------------------------
# Broadcast (Stage-8-Part-5)
# ---------------------------------------------------------------------
#
# Design sketch:
#
# * ``GET  /admin/broadcast`` renders a form (textarea + optional
#   "only users active in the last N days" filter) plus a list of
#   recent jobs. ``POST  /admin/broadcast`` validates, kicks off a
#   background ``asyncio.Task``, and 302s to the detail page.
# * ``GET  /admin/broadcast/{job_id}`` renders the live-progress page,
#   which polls ``GET  /admin/broadcast/{job_id}/status`` every second
#   via a small vanilla-JS snippet baked into the template (no new
#   runtime deps / no HTMX). The status endpoint returns JSON so the
#   same data can be scraped by curl for out-of-browser monitoring.
# * State is kept in ``app[APP_KEY_BROADCAST_JOBS]`` — a dict keyed
#   by opaque id. Each job dict has fields::
#
#       {
#         "id": "abcd1234",
#         "text": "<preview>",       # first 120 chars, UI-safe
#         "full_text_len": 280,      # cheap way to show length
#         "only_active_days": 7 | None,
#         "state": "queued" | "running" | "completed" | "failed",
#         "total": 0,                # set once recipients are fetched
#         "sent": 0, "blocked": 0, "failed": 0, "i": 0,
#         "error": None | "...",     # populated on "failed"
#         "created_at": iso8601-utc,
#         "started_at": iso8601 | None,
#         "completed_at": iso8601 | None,
#       }
#
#   The background task updates the job dict in-place. All reads
#   from the HTTP handlers go through a copy so the JSON serializer
#   isn't racing with the writer coroutine.
#
# * We bound the registry to ``BROADCAST_MAX_HISTORY`` entries (newest
#   wins), pruning only ``completed`` / ``failed`` jobs so a long
#   broadcast backlog can never be silently killed mid-run.
#
# The Telegram ``/admin_broadcast`` command already exists and stays
# authoritative — the web page is the same feature with a different
# front-end. Both callers share ``admin._do_broadcast`` under the hood
# so the paced-send + retry-after + error-bucketing behaviour is
# identical.


BROADCAST_MAX_HISTORY = 50
# Upper bound on the broadcast body. Aligns with the Telegram command
# (``admin._BROADCAST_MAX_TEXT_LEN``) — kept as a separate constant
# here rather than imported so a hotfix to one doesn't silently move
# the other. The two are compared in tests.
BROADCAST_TEXT_MAX_LEN = 3500
# Mirror of ``admin._BROADCAST_ACTIVE_DAYS_MAX``. Kept as a separate
# constant for the same reason as ``BROADCAST_TEXT_MAX_LEN`` above —
# the two forms have independent validation surfaces and the web
# caller shouldn't import private admin.py symbols. The pair is
# asserted equal in tests so drift shows up as a test failure.
BROADCAST_ACTIVE_DAYS_MAX = 36_500


def parse_broadcast_web_form(form) -> dict | str:
    """Parse the /admin/broadcast submission form.

    Returns a dict shaped::

        {
          "text": "…",
          "only_active_days": 7 | None,
        }

    On failure returns one of the error keys: ``"missing_text"``,
    ``"text_too_long"``, ``"bad_active"``, ``"active_too_large"``.
    """
    text = (form.get("text") or "").strip()
    if not text:
        return "missing_text"
    if len(text) > BROADCAST_TEXT_MAX_LEN:
        return "text_too_long"

    raw_active = (form.get("only_active_days") or "").strip()
    only_active_days: int | None
    if not raw_active:
        only_active_days = None
    else:
        try:
            only_active_days = int(raw_active)
        except ValueError:
            return "bad_active"
        if only_active_days <= 0:
            return "bad_active"
        if only_active_days > BROADCAST_ACTIVE_DAYS_MAX:
            return "active_too_large"

    return {"text": text, "only_active_days": only_active_days}


_BROADCAST_FORM_ERR_TEXT = {
    "missing_text": "Broadcast body is required.",
    "text_too_long": (
        f"Broadcast body must be at most {BROADCAST_TEXT_MAX_LEN} characters."
    ),
    "bad_active": "Active-days filter must be a positive integer.",
    "active_too_large": (
        f"Active-days filter must be at most {BROADCAST_ACTIVE_DAYS_MAX:,} "
        f"(≈10 decades)."
    ),
}


def _now_iso() -> str:
    """Wall-clock ISO-8601 (UTC, seconds precision) for job timestamps.

    Deliberately NOT monotonic — operator-visible timestamps should
    line up with log lines and the DB's ``created_at`` values, which
    are also wall-clock UTC.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_broadcast_job(
    *,
    text: str,
    only_active_days: int | None,
) -> dict:
    """Build a fresh job dict in its initial ``"queued"`` state.

    ``id`` is ``secrets.token_urlsafe(6)`` — 8-ish chars of randomness,
    short enough to URL without wrapping but wide enough to make
    guessing someone else's job id pointless (and it's only usable by
    a logged-in admin anyway).

    ``text_preview`` is stored truncated so the jobs-list page can
    render a snippet without dumping a 3500-char body into every row.
    """
    preview = text if len(text) <= 120 else text[:117] + "…"
    return {
        "id": secrets.token_urlsafe(6),
        "text_preview": preview,
        "full_text_len": len(text),
        "only_active_days": only_active_days,
        "state": "queued",
        "total": 0,
        "sent": 0,
        "blocked": 0,
        "failed": 0,
        "i": 0,
        "error": None,
        "created_at": _now_iso(),
        "started_at": None,
        "completed_at": None,
    }


def _store_broadcast_job(app: web.Application, job: dict) -> None:
    """Record *job* in the registry and evict old completed entries.

    We never evict a job whose ``state`` is ``queued`` or ``running``
    — a rolling eviction policy must not silently kill live work.
    """
    jobs: dict = app[APP_KEY_BROADCAST_JOBS]
    jobs[job["id"]] = job
    if len(jobs) > BROADCAST_MAX_HISTORY:
        terminal = [
            jid for jid, j in jobs.items()
            if j["state"] in ("completed", "failed")
            and jid != job["id"]
        ]
        # Evict oldest terminal jobs first. ``jobs`` is insertion-
        # ordered (CPython dicts preserve insertion order) so the
        # list above is naturally "oldest first". Trim until we're
        # back under cap.
        while len(jobs) > BROADCAST_MAX_HISTORY and terminal:
            jobs.pop(terminal.pop(0), None)


async def _run_broadcast_job(
    *,
    app: web.Application,
    job: dict,
    text: str,
) -> None:
    """Background coroutine that does the actual fan-out.

    Runs under ``asyncio.create_task`` from :func:`broadcast_post`.
    Swallows exceptions into ``job["error"]`` so a DB or Telegram
    failure marks the job "failed" rather than leaking up into the
    aiohttp error log as an unretrieved task exception.
    """
    db = app.get(APP_KEY_DB)
    bot = app.get(APP_KEY_BOT)

    job["state"] = "running"
    job["started_at"] = _now_iso()

    if db is None or bot is None:
        # Should never happen in production (both wired up by
        # setup_admin_routes) — belt-and-suspenders so a misconfigured
        # test path doesn't silently "complete" a zero-recipient job.
        job["state"] = "failed"
        job["error"] = (
            "Background task launched without a DB or bot wired up."
        )
        job["completed_at"] = _now_iso()
        return

    try:
        recipients = await db.iter_broadcast_recipients(
            only_active_days=job["only_active_days"]
        )
    except Exception as exc:
        log.exception(
            "broadcast_job=%s: recipient query failed", job["id"]
        )
        job["state"] = "failed"
        job["error"] = f"DB query failed: {exc}"
        job["completed_at"] = _now_iso()
        return

    job["total"] = len(recipients)
    if not recipients:
        job["state"] = "completed"
        job["completed_at"] = _now_iso()
        return

    async def _on_progress(stats: dict) -> None:
        job["i"] = stats["i"]
        job["sent"] = stats["sent"]
        job["blocked"] = stats["blocked"]
        job["failed"] = stats["failed"]

    try:
        # Import locally so a test that doesn't need admin.py
        # (e.g. pure form-parser tests) doesn't pay the aiogram
        # import cost at module-load time.
        from admin import _do_broadcast

        stats = await _do_broadcast(
            bot,
            recipients=recipients,
            text=text,
            admin_id=0,  # web-admin sentinel — see ADMIN_WEB_SENTINEL_ID
            progress_callback=_on_progress,
        )
    except asyncio.CancelledError:
        job["state"] = "failed"
        job["error"] = "Cancelled (admin panel shutting down)."
        job["completed_at"] = _now_iso()
        raise
    except Exception as exc:
        log.exception("broadcast_job=%s: _do_broadcast raised", job["id"])
        job["state"] = "failed"
        job["error"] = f"Broadcast failed: {exc}"
        job["completed_at"] = _now_iso()
        return

    job["i"] = stats["total"]
    job["sent"] = stats["sent"]
    job["blocked"] = stats["blocked"]
    job["failed"] = stats["failed"]
    job["state"] = "completed"
    job["completed_at"] = _now_iso()


async def broadcast_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/broadcast — form + recent jobs list."""
    jobs: dict = request.app[APP_KEY_BROADCAST_JOBS]
    # Newest first. Copy dicts so a background writer can't mutate
    # under the Jinja template iterator.
    recent = [dict(j) for j in reversed(list(jobs.values()))]

    response = aiohttp_jinja2.render_template(
        "broadcast.html",
        request,
        {
            "active_page": "broadcast",
            "csrf_token": csrf_token_for(request),
            "recent": recent,
            "text_max_len": BROADCAST_TEXT_MAX_LEN,
            "flash": None,
        },
    )
    flash = pop_flash(request, response)
    if flash is not None:
        response = aiohttp_jinja2.render_template(
            "broadcast.html",
            request,
            {
                "active_page": "broadcast",
                "csrf_token": csrf_token_for(request),
                "recent": recent,
                "text_max_len": BROADCAST_TEXT_MAX_LEN,
                "flash": flash,
            },
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def broadcast_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/broadcast — validate form + kick off background job."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    form = await request.post()
    back = web.HTTPFound(location="/admin/broadcast")

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning(
            "broadcast_post: CSRF token mismatch from %s", request.remote
        )
        set_flash(
            back,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return back

    parsed = parse_broadcast_web_form(form)
    if isinstance(parsed, str):
        set_flash(
            back,
            kind="error",
            message=_BROADCAST_FORM_ERR_TEXT.get(
                parsed, f"Invalid input ({parsed})."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return back

    if request.app.get(APP_KEY_BOT) is None:
        set_flash(
            back,
            kind="error",
            message=(
                "Bot is not wired up — cannot start a broadcast. "
                "This is almost certainly a deploy misconfiguration."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return back

    job = _new_broadcast_job(
        text=parsed["text"],
        only_active_days=parsed["only_active_days"],
    )
    _store_broadcast_job(request.app, job)
    task = asyncio.create_task(
        _run_broadcast_job(
            app=request.app,
            job=job,
            text=parsed["text"],
        ),
        name=f"broadcast-{job['id']}",
    )
    request.app[APP_KEY_BROADCAST_TASKS][job["id"]] = task
    log.info(
        "broadcast_post: started job=%s len=%d active=%s",
        job["id"], job["full_text_len"], parsed["only_active_days"],
    )
    return web.HTTPFound(location=f"/admin/broadcast/{job['id']}")


async def broadcast_detail_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/broadcast/{job_id} — live-progress page."""
    job_id = request.match_info.get("job_id", "")
    job = request.app[APP_KEY_BROADCAST_JOBS].get(job_id)
    if job is None:
        response = web.HTTPFound(location="/admin/broadcast")
        set_flash(
            response,
            kind="error",
            message=(
                f"Unknown broadcast job {job_id!r}. "
                f"(Jobs are in-memory and are lost on process restart.)"
            ),
            secret=request.app.get(APP_KEY_SESSION_SECRET, ""),
            cookie_secure=request.app.get(APP_KEY_COOKIE_SECURE, True),
        )
        return response

    # Snapshot so the template never sees a half-updated dict.
    return aiohttp_jinja2.render_template(
        "broadcast_detail.html",
        request,
        {
            "active_page": "broadcast",
            "job": dict(job),
        },
    )


async def broadcast_status_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/broadcast/{job_id}/status — JSON for polling."""
    job_id = request.match_info.get("job_id", "")
    job = request.app[APP_KEY_BROADCAST_JOBS].get(job_id)
    if job is None:
        return web.json_response(
            {"error": "unknown_job", "job_id": job_id}, status=404
        )
    # Snapshot before handing to json_response so a concurrent
    # writer can't mutate mid-serialize.
    return web.json_response(dict(job))


# ---------------------------------------------------------------------
# Stage-8-Part-6: paginated transactions browser
# ---------------------------------------------------------------------
# Read-only ledger explorer that mirrors the Telegram admin's per-user
# tx view but at global scope with filters. No write paths —
# credit/debit still lives on the user-detail page so the audit trail
# has one canonical entry point.


TRANSACTIONS_PER_PAGE_DEFAULT = 50
# Pagination defense-in-depth bound. ``Database.list_transactions``
# already clamps to ``TRANSACTIONS_MAX_PER_PAGE`` but we also refuse
# anything larger at the form boundary so "500" in the query string
# doesn't silently quiet-drop to 200 without the UI acknowledging it.
TRANSACTIONS_PER_PAGE_MAX = 200
# Allow-listed ``per_page`` choices surfaced in the dropdown. Keeps
# the UI honest about which values will actually take effect.
TRANSACTIONS_PER_PAGE_CHOICES = (25, 50, 100, 200)


def parse_transactions_query(query) -> dict:
    """Parse the ``/admin/transactions`` query string into a normalised
    dict consumed by both :func:`Database.list_transactions` and the
    template's "active filters" chips.

    Unknown or malformed values are silently dropped (no flash /
    redirect) — a filter that doesn't make sense should render the
    unfiltered page rather than an error banner, matching the
    behaviour of every other admin page that reads query params.

    Returns a dict shaped::

        {
          "gateway": "nowpayments" | "admin" | "gift" | None,
          "status": "PENDING" | "PARTIAL" | "SUCCESS" | "EXPIRED"
                    | "FAILED" | "REFUNDED" | None,
          "telegram_id": int | None,
          "page": int,          # >= 1
          "per_page": int,      # clamped to TRANSACTIONS_PER_PAGE_MAX
        }
    """
    gateway_raw = (query.get("gateway") or "").strip()
    gateway: str | None = None
    if gateway_raw and gateway_raw in Database.TRANSACTIONS_GATEWAY_VALUES:
        gateway = gateway_raw

    status_raw = (query.get("status") or "").strip()
    status: str | None = None
    if status_raw and status_raw in Database.TRANSACTIONS_STATUS_VALUES:
        status = status_raw

    tid_raw = (query.get("telegram_id") or "").strip()
    telegram_id: int | None = None
    if tid_raw:
        try:
            telegram_id = int(tid_raw)
        except ValueError:
            # Ignore — an un-parseable id chip would otherwise make
            # "undo filter" harder than just clicking the other
            # chips.
            telegram_id = None

    try:
        page = max(1, int(query.get("page", "1")))
    except (ValueError, TypeError):
        page = 1
    try:
        per_page = int(query.get("per_page", str(TRANSACTIONS_PER_PAGE_DEFAULT)))
    except (ValueError, TypeError):
        per_page = TRANSACTIONS_PER_PAGE_DEFAULT
    per_page = max(1, min(per_page, TRANSACTIONS_PER_PAGE_MAX))

    return {
        "gateway": gateway,
        "status": status,
        "telegram_id": telegram_id,
        "page": page,
        "per_page": per_page,
    }


def _encode_tx_query(filters: dict, *, page: int | None = None) -> str:
    """Rebuild a canonical ``/admin/transactions?…`` query string from
    a parsed ``filters`` dict. ``page`` override lets the prev/next
    links reuse the same dict without mutating it.

    Empty values are omitted so the URL stays readable when no
    filter is applied.
    """
    from urllib.parse import urlencode

    params: list[tuple[str, str]] = []
    if filters.get("gateway"):
        params.append(("gateway", filters["gateway"]))
    if filters.get("status"):
        params.append(("status", filters["status"]))
    if filters.get("telegram_id") is not None:
        params.append(("telegram_id", str(filters["telegram_id"])))
    effective_page = filters.get("page", 1) if page is None else page
    if effective_page != 1:
        params.append(("page", str(effective_page)))
    if filters.get("per_page", TRANSACTIONS_PER_PAGE_DEFAULT) != TRANSACTIONS_PER_PAGE_DEFAULT:
        params.append(("per_page", str(filters["per_page"])))
    return urlencode(params)


async def transactions_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/transactions — paginated ledger browser."""
    db = request.app[APP_KEY_DB]
    filters = parse_transactions_query(request.rel_url.query)

    try:
        page_result = await db.list_transactions(
            gateway=filters["gateway"],
            status=filters["status"],
            telegram_id=filters["telegram_id"],
            page=filters["page"],
            per_page=filters["per_page"],
        )
    except ValueError:
        # parse_transactions_query already mapped unknown enums to
        # None, so this branch is reachable only if the DB layer
        # adds a new validation rule. Degrade to an empty page with
        # the filters stripped rather than 500ing.
        log.warning(
            "transactions_get: list_transactions rejected filters=%s",
            filters,
        )
        page_result = {
            "rows": [],
            "total": 0,
            "page": 1,
            "per_page": filters["per_page"],
            "total_pages": 0,
        }

    # Build prev/next URLs so the template stays dumb — no URL
    # manipulation logic in Jinja.
    prev_url: str | None = None
    next_url: str | None = None
    if page_result["page"] > 1:
        q = _encode_tx_query(filters, page=page_result["page"] - 1)
        prev_url = f"/admin/transactions?{q}" if q else "/admin/transactions"
    if page_result["page"] < page_result["total_pages"]:
        q = _encode_tx_query(filters, page=page_result["page"] + 1)
        next_url = f"/admin/transactions?{q}" if q else "/admin/transactions"

    return aiohttp_jinja2.render_template(
        "transactions.html",
        request,
        {
            "active_page": "transactions",
            "filters": filters,
            "result": page_result,
            "prev_url": prev_url,
            "next_url": next_url,
            "gateway_choices": sorted(Database.TRANSACTIONS_GATEWAY_VALUES),
            "status_choices": sorted(Database.TRANSACTIONS_STATUS_VALUES),
            "per_page_choices": TRANSACTIONS_PER_PAGE_CHOICES,
        },
    )


# ---------------------------------------------------------------------
# App wiring
# ---------------------------------------------------------------------


def setup_admin_routes(
    app: web.Application,
    *,
    db,
    password: str,
    session_secret: str,
    ttl_hours: int = DEFAULT_TTL_HOURS,
    cookie_secure: bool = True,
    bot=None,
) -> None:
    """Mount the admin panel onto *app*.

    Called from ``main.start_webhook_server``. Idempotent — refusing
    a second call with a clear log line beats silently overwriting
    state on a hot reload.
    """
    if app.get(APP_KEY_INSTALLED):
        log.warning("setup_admin_routes called twice — ignoring second call.")
        return

    if not password:
        log.warning(
            "ADMIN_PASSWORD is not set — web admin panel will be "
            "unreachable (login will refuse all attempts)."
        )
    if not session_secret:
        # Intentionally leave session_secret empty so the "not
        # configured" guard in ``login_post`` (``not expected or
        # not secret``) correctly refuses every attempt.
        #
        # Earlier versions of this branch auto-generated a random
        # per-process secret on the theory that it was harmless —
        # but auto-generating made the secret non-empty, which
        # bypassed the guard and let a sysadmin who set
        # ADMIN_PASSWORD but forgot ADMIN_SESSION_SECRET silently
        # log in (Devin Review caught this on PR #54). Refusing
        # to start with a half-configured admin panel is the safer
        # default — surface the misconfig at log time, not by
        # accident at runtime.
        log.warning(
            "ADMIN_SESSION_SECRET is not set — login_post will refuse "
            "every attempt until it's configured. Set ADMIN_SESSION_SECRET "
            "(any random 32+ char string) in your environment to enable "
            "the admin panel."
        )

    app[APP_KEY_PASSWORD] = password
    app[APP_KEY_SESSION_SECRET] = session_secret
    app[APP_KEY_TTL_HOURS] = ttl_hours
    app[APP_KEY_COOKIE_SECURE] = cookie_secure
    app[APP_KEY_DB] = db
    # Stage-8-Part-5: broadcast plumbing. The bot reference is
    # optional so unit tests that don't need Telegram fan-out can
    # still mount the routes — broadcast_post refuses to start a job
    # when it's missing.
    app[APP_KEY_BOT] = bot
    app[APP_KEY_BROADCAST_JOBS] = {}
    app[APP_KEY_BROADCAST_TASKS] = {}

    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        # autoescape is ON by default for .html files via select_autoescape;
        # being explicit here protects us if a future template ever loses
        # the .html extension.
        autoescape=jinja2.select_autoescape(["html"]),
    )
    app.middlewares.append(admin_auth_middleware)

    # Per-IP token-bucket throttle on /admin/login. Mounted here so the
    # cache lives on the same aiohttp app the handler reads from. See
    # ``rate_limit.install_login_rate_limit`` for defaults.
    install_login_rate_limit(app)

    app.router.add_get("/admin/login", login_get)
    app.router.add_post("/admin/login", login_post)
    app.router.add_get("/admin/logout", logout)
    app.router.add_get("/admin/", _require_auth(dashboard))
    # Redirect /admin → /admin/ so users typing the bare path land
    # cleanly. aiohttp doesn't treat trailing-slash variants as the
    # same route by default.
    app.router.add_get(
        "/admin",
        lambda r: web.HTTPFound(location="/admin/"),
    )

    # Stage-8-Part-2: promo codes.
    app.router.add_get("/admin/promos", _require_auth(promos_get))
    app.router.add_post("/admin/promos", _require_auth(promos_create))
    app.router.add_post(
        "/admin/promos/{code}/revoke",
        _require_auth(promos_revoke),
    )

    # Stage-8-Part-3: gift codes.
    app.router.add_get("/admin/gifts", _require_auth(gifts_get))
    app.router.add_post("/admin/gifts", _require_auth(gifts_create))
    app.router.add_post(
        "/admin/gifts/{code}/revoke",
        _require_auth(gifts_revoke),
    )

    # Stage-8-Part-4: users.
    app.router.add_get("/admin/users", _require_auth(users_get))
    app.router.add_get(
        "/admin/users/{telegram_id}",
        _require_auth(user_detail_get),
    )
    app.router.add_post(
        "/admin/users/{telegram_id}/adjust",
        _require_auth(user_adjust_post),
    )

    # Stage-8-Part-5: broadcast.
    app.router.add_get("/admin/broadcast", _require_auth(broadcast_get))
    app.router.add_post("/admin/broadcast", _require_auth(broadcast_post))
    app.router.add_get(
        "/admin/broadcast/{job_id}",
        _require_auth(broadcast_detail_get),
    )
    app.router.add_get(
        "/admin/broadcast/{job_id}/status",
        _require_auth(broadcast_status_get),
    )

    # Stage-8-Part-6: transactions browser (read-only, paginated).
    app.router.add_get(
        "/admin/transactions",
        _require_auth(transactions_get),
    )

    app[APP_KEY_INSTALLED] = True
    log.info("Web admin routes installed under /admin/")
