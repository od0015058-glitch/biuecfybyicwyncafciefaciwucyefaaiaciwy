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

import base64
import hashlib
import hmac
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp_jinja2
import jinja2
from aiohttp import web

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

    form = await request.post()
    submitted = str(form.get("password", ""))

    # Constant-time compare on the bytes — the lengths can differ, but
    # ``compare_digest`` handles that without leaking which one was
    # right.
    if not hmac.compare_digest(submitted, expected):
        log.warning(
            "admin login: bad password from %s",
            request.remote,
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

    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        # autoescape is ON by default for .html files via select_autoescape;
        # being explicit here protects us if a future template ever loses
        # the .html extension.
        autoescape=jinja2.select_autoescape(["html"]),
    )
    app.middlewares.append(admin_auth_middleware)

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

    app[APP_KEY_INSTALLED] = True
    log.info("Web admin routes installed under /admin/")
