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
import io
import logging
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp_jinja2
import jinja2
import pyotp
import qrcode
import qrcode.image.svg as qrsvg
from aiohttp import web

# Imported for the class-level ``TRANSACTIONS_GATEWAY_VALUES`` /
# ``TRANSACTIONS_STATUS_VALUES`` allow-lists used by
# parse_transactions_query (Stage-8-Part-6). Only the class is
# referenced — not the module-level ``db`` singleton — so the admin
# still works against the injected DB in tests.
import strings as bot_strings_module
from database import Database
from formatting import format_usd
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
# Stage-9-Step-3: optional TOTP / 2FA. ``APP_KEY_TOTP_SECRET`` carries
# the configured base32 secret (empty = 2FA disabled, login is
# password-only and backwards-compatible). ``APP_KEY_TOTP_ISSUER`` is
# the human label baked into the otpauth:// URI so the entry shows up
# in authenticator apps as "Meowassist Admin: admin" rather than a
# bare hostname.
APP_KEY_TOTP_SECRET: web.AppKey = web.AppKey("admin_totp_secret", str)
APP_KEY_TOTP_ISSUER: web.AppKey = web.AppKey("admin_totp_issuer", str)
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
# Stage-15-Step-F: tests inject a no-op kill function here so
# ``control_force_stop_post`` doesn't actually murder the test
# process. Production never sets this — ``bot_health.request_force_stop``
# defaults to ``os.kill`` against the current PID.
APP_KEY_FORCE_STOP_FN: web.AppKey = web.AppKey(
    "admin_force_stop_fn", object
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
# TOTP / 2FA helpers (Stage-9-Step-3)
# ---------------------------------------------------------------------


# RFC-6238 advises a one-step (±30 s) tolerance window so an honest
# code that ticks over while the form is in flight still verifies.
# Anything wider hands free codes to a brute-forcer; pyotp's default
# is 0 (exact match). We pin the value explicitly so a future pyotp
# release can't widen it on us.
TOTP_VALID_WINDOW = 1


# Persian (U+06F0..U+06F9) and Arabic-Indic (U+0660..U+0669) digit
# ranges, mapped to ASCII ``0``-``9``. Used by ``verify_totp_code``
# to transparently accept TOTP codes pasted from a Persian / Arabic
# locale clipboard. Built once at import time so each verify call
# is a single ``str.translate`` walk.
_FARSI_ARABIC_DIGIT_TRANSLATION = str.maketrans(
    "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
    "01234567890123456789",
)


def _normalize_totp_secret(secret: str) -> str:
    """Return *secret* uppercased + de-spaced (authenticator-app friendly).

    Authenticators format secrets as space-separated 4-char chunks for
    readability; operators paste those raw into ``ADMIN_2FA_SECRET``
    and end up with verification failures. Strip whitespace + uppercase
    so a copy-pasted ``"abcd efgh ijkl mnop"`` still works.
    """
    return "".join(secret.split()).upper()


def validate_totp_secret(secret: str) -> str:
    """Return the normalized base32 secret or raise ``ValueError``.

    Empty / whitespace-only input means "2FA disabled" — return ``""``
    so the caller can short-circuit. Any non-empty value must base32-
    decode cleanly with at least 80 bits of entropy (16 base32 chars =
    80 bits, the RFC-4226 floor). We deliberately reject shorter
    strings even though pyotp would accept them, because a 6-char
    "secret" is brute-forceable in seconds.
    """
    if not secret or not secret.strip():
        return ""
    norm = _normalize_totp_secret(secret)
    if len(norm) < 16:
        raise ValueError(
            "ADMIN_2FA_SECRET must be at least 16 base32 characters "
            "(≥ 80 bits of entropy). Generate a fresh one with "
            "pyotp.random_base32() or copy the value from "
            "/admin/enroll_2fa."
        )
    try:
        # ``casefold=True`` lets lowercase secrets decode; we already
        # uppercased above but keep the flag for defence in depth.
        base64.b32decode(norm, casefold=True)
    except (ValueError, TypeError, base64.binascii.Error) as exc:
        raise ValueError(
            "ADMIN_2FA_SECRET is not a valid base32 string. Use only "
            "the characters A-Z and 2-7 (padding optional) — generate "
            "a fresh one at /admin/enroll_2fa."
        ) from exc
    return norm


def verify_totp_code(secret: str, submitted: str) -> bool:
    """Return True iff *submitted* is the current TOTP for *secret*.

    Wraps ``pyotp.TOTP.verify`` so the rest of the module never has
    to think about pyotp directly. ``submitted`` is normalized
    (whitespace stripped, but case preserved — TOTP codes are
    digits only) before dispatch so a stray space copied with the
    code still verifies.

    Stage-15-Step-F bundled bug fix: non-ASCII digit characters
    (Persian ``۰۱۲۳۴۵۶۷۸۹`` U+06F0–U+06F9, Arabic-Indic ``٠١٢٣٤٥٦٧٨٩``
    U+0660–U+0669, full-width digits, etc.) used to be accepted by
    ``str.isdigit()`` but rejected by ``pyotp.TOTP.verify`` — so a
    Persian admin pasting their authenticator code from a Persian-
    locale clipboard saw a confusing "Invalid 2FA code" error
    rather than logging in. This is the bot's primary user base.
    The fix is two-step: (1) translate Persian / Arabic-Indic
    digits to ASCII before validation so the verify path
    transparently accepts what the operator typed, and (2) tighten
    the format check to ``isascii() and isdigit()`` so any
    *remaining* non-ASCII digit class (full-width, mathematical,
    Bengali, …) fails with a fast ``False`` rather than reaching
    pyotp and raising into the broad-except.
    """
    if not secret or not submitted:
        return False
    cleaned = "".join(submitted.split())

    # Persian (U+06F0..U+06F9) and Arabic-Indic (U+0660..U+0669)
    # digit ranges. ``str.translate`` is a one-shot O(n) walk; the
    # table is built once at module import and reused every call.
    cleaned = cleaned.translate(_FARSI_ARABIC_DIGIT_TRANSLATION)

    if not (cleaned.isascii() and cleaned.isdigit() and len(cleaned) == 6):
        return False
    try:
        return bool(
            pyotp.TOTP(secret).verify(cleaned, valid_window=TOTP_VALID_WINDOW)
        )
    except Exception:
        # Defence in depth: a malformed secret should never crash the
        # request — the startup guard already validates the secret,
        # but if a future code path mutates it we'd rather refuse the
        # login than 500.
        log.exception("verify_totp_code: pyotp raised on submitted code")
        return False


def build_otpauth_uri(secret: str, *, issuer: str, account: str = "admin") -> str:
    """Render a standard ``otpauth://totp/...`` provisioning URI."""
    return pyotp.TOTP(secret).provisioning_uri(name=account, issuer_name=issuer)


def render_qr_svg(uri: str) -> str:
    """Render a self-contained inline SVG QR for *uri*.

    Uses ``qrcode.image.svg.SvgPathImage`` so we don't pull in Pillow
    just to ship one image. The returned string is safe to drop into
    a Jinja template via the ``|safe`` filter.
    """
    qr = qrcode.QRCode(box_size=8, border=2)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(image_factory=qrsvg.SvgPathImage)
    buf = io.BytesIO()
    img.save(buf)
    return buf.getvalue().decode("utf-8")


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
        "login.html",
        request,
        {
            "error": None,
            "show_2fa_field": bool(request.app.get(APP_KEY_TOTP_SECRET, "")),
        },
    )


async def login_post(request: web.Request) -> web.StreamResponse:
    expected = request.app.get(APP_KEY_PASSWORD, "")
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    totp_secret = request.app.get(APP_KEY_TOTP_SECRET, "")
    show_2fa = bool(totp_secret)
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
            {
                "error": "Admin panel is not configured. Set ADMIN_PASSWORD and ADMIN_SESSION_SECRET.",
                "show_2fa_field": show_2fa,
            },
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
        await _record_audit_safe(
            request,
            "login_deny",
            outcome="deny",
            meta={"reason": "rate_limited"},
        )
        return aiohttp_jinja2.render_template(
            "login.html",
            request,
            {
                "error": "Too many login attempts. Please wait a minute.",
                "show_2fa_field": show_2fa,
            },
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
        await _record_audit_safe(
            request,
            "login_deny",
            outcome="deny",
            meta={"reason": "bad_password"},
        )
        return aiohttp_jinja2.render_template(
            "login.html",
            request,
            {"error": "Wrong password.", "show_2fa_field": show_2fa},
            status=401,
        )

    # Stage-9-Step-3: optional second factor. Gate on TOTP secret so
    # password-only deploys are unchanged; when configured, the 2FA
    # check runs AFTER the password compare so an attacker without
    # the password gets a generic "Wrong password" error and can't
    # use the form to brute-force the 6-digit code in isolation.
    if totp_secret:
        submitted_code = str(form.get("code", ""))
        if not verify_totp_code(totp_secret, submitted_code):
            reason = "missing_2fa" if not submitted_code.strip() else "bad_2fa"
            log.warning(
                "admin login: %s key=%s remote=%s",
                reason, client_key, request.remote,
            )
            await _record_audit_safe(
                request,
                "login_deny",
                outcome="deny",
                meta={"reason": reason},
            )
            return aiohttp_jinja2.render_template(
                "login.html",
                request,
                {
                    "error": "Invalid 2FA code.",
                    "show_2fa_field": show_2fa,
                },
                status=401,
            )

    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
    cookie_value = sign_cookie(expires_at, secret=secret)

    log.info(
        "admin login: success from %s (cookie expires %s)",
        request.remote, expires_at.isoformat(),
    )
    await _record_audit_safe(request, "login_ok")
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


def _collect_ipn_health() -> dict:
    """Snapshot the per-process IPN drop counters for the dashboard tile.

    Stage-15-Step-D #5. ``payments.get_ipn_drop_counters()``,
    ``tetrapay.get_tetrapay_drop_counters()`` and
    ``zarinpal.get_zarinpal_drop_counters()`` are all process-local
    and reset to zero on every restart, so the tile is labelled
    "since last restart" in the template. Each gateway is collected
    behind its own ``try`` so a future regression in one accessor
    (or a missing-import edge case in tests) cannot blank out the
    other panels.

    Stage-15-Step-E #9 bundled fix: Zarinpal shipped its own
    ``_ZARINPAL_DROP_COUNTERS`` registry in
    Stage-15-Step-E #8 (PR #126) but the dashboard tile and the
    Prometheus exposition both forgot to consume it, so an operator
    debugging a Zarinpal verify-failure spike had to grep the bot
    logs to count drops. The dashboard now surfaces Zarinpal
    counters alongside NowPayments and TetraPay, and ``metrics.py``
    exposes ``meowassist_zarinpal_drops_total{reason="..."}`` as a
    Prometheus counter so the alerting rules already targeting the
    other two gateways auto-extend to the third.
    """
    nowpayments: dict[str, int]
    tetrapay: dict[str, int]
    zarinpal: dict[str, int]
    try:
        from payments import get_ipn_drop_counters

        nowpayments = dict(get_ipn_drop_counters())
    except Exception:
        log.exception("dashboard: get_ipn_drop_counters failed")
        nowpayments = {}
    try:
        from tetrapay import get_tetrapay_drop_counters

        tetrapay = dict(get_tetrapay_drop_counters())
    except Exception:
        log.exception("dashboard: get_tetrapay_drop_counters failed")
        tetrapay = {}
    try:
        from zarinpal import get_zarinpal_drop_counters

        zarinpal = dict(get_zarinpal_drop_counters())
    except Exception:
        log.exception("dashboard: get_zarinpal_drop_counters failed")
        zarinpal = {}
    return {
        "nowpayments": nowpayments,
        "tetrapay": tetrapay,
        "zarinpal": zarinpal,
        "nowpayments_total": sum(nowpayments.values()),
        "tetrapay_total": sum(tetrapay.values()),
        "zarinpal_total": sum(zarinpal.values()),
    }


async def dashboard(request: web.Request) -> web.StreamResponse:
    db = request.app.get(APP_KEY_DB)
    metrics: dict
    db_error: str | None = None
    # Local dev / unit-test path where the app didn't get a Database
    # wired up — and the DB-error path — both render the same shape
    # so the template renders the same in dev / DB-error / live.
    # Keys MUST match ``Database.get_system_metrics`` (and
    # ``admin.format_metrics`` consumers): users_total, users_active_7d,
    # revenue_usd, spend_usd, top_models[{model,count,cost_usd}],
    # pending_payments_count, pending_payments_oldest_age_hours.
    # Stage-12-Step-B: dashboard reads the same alert threshold as
    # the proactive admin-DM loop so the "overdue" tile and any DM
    # the operator received reference the same set of rows. The
    # import is local to keep web_admin's import surface slim — this
    # is the only call site.
    from pending_alert import get_pending_alert_threshold_hours
    threshold_hours = get_pending_alert_threshold_hours()
    empty_metrics: dict = {
        "users_total": 0,
        "users_active_7d": 0,
        "revenue_usd": 0.0,
        "spend_usd": 0.0,
        "top_models": [],
        "pending_payments_count": 0,
        "pending_payments_oldest_age_hours": None,
        "pending_payments_over_threshold_count": 0,
        "pending_alert_threshold_hours": threshold_hours,
    }
    if db is None:
        metrics = dict(empty_metrics)
        db_error = "No database wired up (development mode)."
    else:
        try:
            metrics = await db.get_system_metrics(
                pending_alert_threshold_hours=threshold_hours,
            )
        except Exception:
            log.exception("dashboard: get_system_metrics failed")
            metrics = dict(empty_metrics)
            db_error = "Database query failed — see logs."

    ipn_health = _collect_ipn_health()

    return aiohttp_jinja2.render_template(
        "dashboard.html",
        request,
        {
            "metrics": metrics,
            "db_error": db_error,
            "ipn_health": ipn_health,
            "active_page": "dashboard",
        },
    )


# ---------------------------------------------------------------------
# Monetization (Stage-15-Step-E #9)
# ---------------------------------------------------------------------


# 30 days mirrors ``Database.get_monetization_summary``'s default and
# the ``get_system_metrics`` top-models horizon — keeping the two
# admin surfaces aligned ("the same 30 days") avoids confusion when
# the operator cross-references the dashboard's "Top models" tile
# against the monetization table.
_MONETIZATION_DEFAULT_WINDOW_DAYS: int = 30
_MONETIZATION_TOP_MODELS_LIMIT: int = 10


def _empty_monetization_summary(
    *, window_days: int, markup: float = 0.0,
) -> dict:
    """Return a zero-everything monetization summary in the shape the
    template expects. Used as the dev-mode and DB-error fallback so
    the page renders cleanly even when the DB is unreachable.

    The shape MUST stay aligned with ``Database.get_monetization_summary``
    — Devin Review caught a similar mismatch on PR #54 where the
    template wanted ``user_count`` but the real DB returned
    ``users_total``, which 500'd every dashboard load.
    """
    zero_block = {
        "revenue_usd": 0.0,
        "charged_usd": 0.0,
        "openrouter_cost_usd": 0.0,
        "gross_margin_usd": 0.0,
        "gross_margin_pct": 0.0,
        "net_profit_usd": 0.0,
    }
    return {
        "markup": float(markup),
        "lifetime": dict(zero_block),
        "window": {"days": int(window_days), **zero_block},
        "by_model": [],
    }


async def monetization(request: web.Request) -> web.StreamResponse:
    """``/admin/monetization`` — first slice of Stage-15-Step-E #9.

    Renders the bot's revenue / OpenRouter cost / gross margin
    breakdown over a fixed 30-day window plus lifetime totals. The
    per-model table sorts by *charged USD descending* so the biggest
    margin contributors are at the top.

    DB unreachable / query failure → render the empty-zero shape
    plus an inline error banner. Same fail-soft pattern the
    ``dashboard`` handler uses; a flaky DB shouldn't 500 the
    operator's home view.
    """
    db = request.app.get(APP_KEY_DB)
    summary: dict
    db_error: str | None = None

    # Read the markup once for the empty-fallback path so the page
    # still shows the operator their current pricing config even
    # when the DB is out. Cheap and stable.
    try:
        from pricing import get_markup
        markup_for_fallback = float(get_markup())
    except Exception:
        log.exception("monetization: get_markup failed")
        markup_for_fallback = 1.0

    if db is None:
        summary = _empty_monetization_summary(
            window_days=_MONETIZATION_DEFAULT_WINDOW_DAYS,
            markup=markup_for_fallback,
        )
        db_error = "No database wired up (development mode)."
    else:
        try:
            summary = await db.get_monetization_summary(
                window_days=_MONETIZATION_DEFAULT_WINDOW_DAYS,
                top_models_limit=_MONETIZATION_TOP_MODELS_LIMIT,
            )
        except Exception:
            log.exception("monetization: get_monetization_summary failed")
            summary = _empty_monetization_summary(
                window_days=_MONETIZATION_DEFAULT_WINDOW_DAYS,
                markup=markup_for_fallback,
            )
            db_error = "Database query failed — see logs."

    return aiohttp_jinja2.render_template(
        "monetization.html",
        request,
        {
            "summary": summary,
            "db_error": db_error,
            "active_page": "monetization",
        },
    )


# ---------------------------------------------------------------------
# Admin audit helper (Stage-9-Step-2)
# ---------------------------------------------------------------------


# Slug → human label mapping for the /admin/audit page filter dropdown.
# Keep this in sync with every ``record_admin_audit`` callsite in this
# module — anything not listed here still records and displays, but
# won't appear in the filter UI.
AUDIT_ACTION_LABELS: dict[str, str] = {
    "login_ok": "Login (success)",
    "login_deny": "Login (denied)",
    "promo_create": "Promo created",
    "promo_revoke": "Promo revoked",
    "gift_create": "Gift created",
    "gift_revoke": "Gift revoked",
    "user_adjust": "Wallet credit / debit",
    "user_edit": "User fields edited",
    "broadcast_start": "Broadcast started",
    "string_save": "Bot text override saved",
    "string_revert": "Bot text override reverted",
    "enroll_2fa_view": "2FA enrolment page viewed",
    # Stage-12-Step-A: refund flow on /admin/transactions.
    "refund_issued": "Refund issued",
    "refund_refused": "Refund refused",
    # Stage-14: model & gateway toggles.
    "model_disable": "AI model disabled",
    "model_enable": "AI model enabled",
    "gateway_disable": "Gateway disabled",
    "gateway_enable": "Gateway enabled",
    # Stage-15-Step-F first slice: emergency control panel.
    # These slugs were already being recorded by ``record_admin_audit``
    # at the kill-switch / force-stop call sites in this module, but
    # they were not exposed in the filter dropdown — meaning an
    # operator reviewing what fired during an incident couldn't
    # narrow the feed to "force-stop only" without scrolling through
    # the full audit log. Bundled fix in this PR.
    "control_force_stop": "Bot force-stopped",
    "control_disable_all_models": "All AI models disabled (kill-switch)",
    "control_enable_all_models": "All AI models re-enabled",
    "control_disable_all_gateways": "All gateways disabled (kill-switch)",
    "control_enable_all_gateways": "All gateways re-enabled",
    # Stage-15-Step-F follow-up #3: alert-loop audit rows. ``actor``
    # is fixed to ``"bot_health_alert"`` (the loop, not a human),
    # so an operator can filter actor=bot_health_alert to pull just
    # the alert-loop incidents.
    "bot_health_alert": "Bot-health alert DM sent",
    "bot_health_recovery": "Bot-health recovery DM sent",
    # Stage-15-Step-E #5 (follow-up to PR #123): admin-role CRUD
    # was already recording these slugs at the
    # ``/admin_role_grant`` / ``/admin_role_revoke`` Telegram
    # handlers, but they were missed by the original Step-E #5 PR
    # *and* by the audit-dropdown sweep in Stage-15-Step-F follow-up
    # #3. The rows were stored correctly, but an operator filtering
    # the audit feed to "role changes only" while reviewing who got
    # promoted couldn't pick those slugs out of the dropdown — they
    # had to scroll the full unfiltered feed. Bundled fix in this
    # PR. A regression test in ``tests/test_web_admin.py`` pins both
    # labels so a future PR can't drop them again.
    "role_grant": "Admin role granted",
    "role_revoke": "Admin role revoked",
}


async def _record_audit_safe(
    request: web.Request,
    action: str,
    *,
    target: str | None = None,
    outcome: str = "ok",
    meta: dict | None = None,
) -> None:
    """Best-effort audit-log write. Swallows every exception so an
    audit-write failure can never block the underlying admin
    operation. The actor is derived from the auth context — for now
    every web admin shares one identity (``"web"``) since the panel
    only enforces a single password. The IP comes from the same
    helper used by the rate limiter so reverse-proxy deploys with
    ``TRUST_PROXY_HEADERS=1`` get the real client address rather
    than the proxy's TCP address."""
    db = request.app.get(APP_KEY_DB)
    if db is None:
        return
    try:
        ip = client_ip_for_rate_limit(request)
    except Exception:
        ip = request.remote
    try:
        await db.record_admin_audit(
            actor="web",
            action=action,
            target=target,
            ip=ip,
            outcome=outcome,
            meta=meta,
        )
    except Exception:
        log.exception(
            "audit log write failed action=%s target=%s outcome=%s",
            action, target, outcome,
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
# Upper bound on the ``max_uses`` field of promo / gift codes.
# Pre-fix this was unbounded — an admin typing ``max_uses=2147483648``
# (or larger) would overflow PostgreSQL's INTEGER column on insert and
# the asyncpg driver would raise ``NumericValueOutOfRangeError``,
# which the route handler caught with the generic ``"DB write failed
# — see logs."`` flash. The admin had no way to know the real cause
# was that they fat-fingered an extra digit. Now we reject anything
# above this cap up-front with a clear validation message. 1M
# distinct uses is already implausibly large for any single
# promo/gift code; anything beyond that is almost certainly a typo.
MAX_USES_CAP = 1_000_000


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
    # ASCII-only: ``str.isalnum`` returns True for Unicode digits and
    # letters (Persian "۱", Roman numerals, Cyrillic homoglyphs of
    # Latin letters, etc.). A code stored as ``"PROMO۱"`` would never
    # match a user typing ``"PROMO1"`` with an ASCII digit, so the
    # admin's promo silently never redeems. Constrain to ASCII
    # ``[A-Z0-9_-]`` so the stored code is always exactly what a user
    # typing on a standard keyboard can produce.
    if len(code) > 64 or not all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code
    ):
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
        if max_uses > MAX_USES_CAP:
            return "max_uses_too_large"

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
    "max_uses_too_large": (
        f"Max uses must be at most {MAX_USES_CAP:,} (DB INTEGER limit)."
    ),
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
    await _record_audit_safe(
        request,
        "promo_create",
        target=f"promo:{parsed['code']}",
        meta={
            "discount_percent": parsed["discount_percent"],
            "discount_amount": parsed["discount_amount"],
            "max_uses": parsed["max_uses"],
            "expires_in_days": parsed["expires_in_days"],
        },
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
    # ASCII-only validation matches ``parse_promo_form`` so the URL
    # path can't carry a Unicode-digit lookalike past the revoke
    # gate (the DB lookup would simply 404, but rejecting upstream
    # gives a clearer flash banner).
    if not code or len(code) > 64 or not all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code
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
        await _record_audit_safe(
            request, "promo_revoke", target=f"promo:{code}",
        )
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
    # ASCII-only: see the equivalent guard in ``parse_promo_form`` for
    # the reasoning. A gift code containing a Persian / Roman-numeral
    # / Cyrillic-homoglyph character would store fine but would never
    # match a user typing the visually-identical ASCII version.
    if len(code) > 64 or not all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code
    ):
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
        if max_uses > MAX_USES_CAP:
            return "max_uses_too_large"

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
    "max_uses_too_large": (
        f"Max redemptions must be at most {MAX_USES_CAP:,} (DB INTEGER limit)."
    ),
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
    await _record_audit_safe(
        request,
        "gift_create",
        target=f"gift:{parsed['code']}",
        meta={
            "amount_usd": parsed["amount_usd"],
            "max_uses": parsed["max_uses"],
            "expires_in_days": parsed["expires_in_days"],
        },
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
    # ASCII-only validation matches ``parse_gift_form``.
    if not code or len(code) > 64 or not all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code
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
        await _record_audit_safe(
            request, "gift_revoke", target=f"gift:{code}",
        )
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
# Stage-12-Step-D: per-code redemption drilldown.
# ---------------------------------------------------------------------
#
# ``GET /admin/gifts/{code}/redemptions`` — list every gift_redemptions
# row for one code (newest first), with telegram_id, username,
# redeemed_at, transaction_id, and the per-redemption USD figure
# joined from transactions.amount_usd_credited. Backed by the alembic
# 0013 ``idx_gift_redemptions_code_redeemed_at`` index. Mirrors the
# Stage-9-Step-8 ``/admin/users/{id}/usage`` per-page layout
# (paginated, per-page picker, prev/next).

GIFT_REDEMPTIONS_PER_PAGE_DEFAULT = 50
GIFT_REDEMPTIONS_PER_PAGE_MAX = 200
GIFT_REDEMPTIONS_PER_PAGE_CHOICES = (25, 50, 100, 200)


def _is_valid_gift_code(code: str) -> bool:
    """ASCII-only [A-Za-z0-9_-] gift-code shape, max 64 chars.

    Mirrors the validation in ``parse_gift_form`` and ``gifts_revoke``
    so a tampered URL can't smuggle SQL fragments or weird Unicode
    into ``Database.list_gift_code_redemptions(code=...)`` even though
    the SQL itself is fully parameterised.
    """
    return bool(code) and len(code) <= 64 and all(
        (c.isascii() and c.isalnum()) or c in "_-" for c in code
    )


async def gift_redemptions_get(
    request: web.Request,
) -> web.StreamResponse:
    """GET /admin/gifts/{code}/redemptions — paginated drilldown.

    Stage-12-Step-D. Renders the list of every redemption for one
    gift code: telegram_id, username, redeemed_at, transaction_id,
    and the per-redemption USD figure. Aggregates (count + sum +
    first/last) above the table.
    """
    raw_code = request.match_info.get("code", "")
    code = raw_code.upper()
    if not _is_valid_gift_code(code):
        return web.HTTPFound(location="/admin/gifts")

    try:
        page = max(1, int(request.rel_url.query.get("page", "1")))
    except (ValueError, TypeError):
        page = 1
    try:
        per_page = int(
            request.rel_url.query.get(
                "per_page", str(GIFT_REDEMPTIONS_PER_PAGE_DEFAULT)
            )
        )
    except (ValueError, TypeError):
        per_page = GIFT_REDEMPTIONS_PER_PAGE_DEFAULT
    per_page = max(1, min(per_page, GIFT_REDEMPTIONS_PER_PAGE_MAX))

    db = request.app.get(APP_KEY_DB)
    gift_meta: dict | None = None
    page_result: dict | None = None
    aggregates: dict | None = None
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        try:
            gift_meta = await db.get_gift_code(code)
            if gift_meta is not None:
                page_result = await db.list_gift_code_redemptions(
                    code=code, page=page, per_page=per_page,
                )
                aggregates = await db.get_gift_code_redemption_aggregates(
                    code
                )
        except Exception:
            log.exception(
                "gift_redemptions_get: query failed code=%r", code,
            )
            db_error = "Database query failed — see logs."

    # If the code itself doesn't exist, redirect back to the list with
    # a flash. We deliberately don't 404 — a deep link to a deleted
    # code is more friendly with a banner explanation than a hard
    # error page, and is consistent with the user-detail page
    # behaviour.
    if (
        db is not None
        and db_error is None
        and gift_meta is None
    ):
        secret = request.app.get(APP_KEY_SESSION_SECRET, "")
        cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
        response = web.HTTPFound(location="/admin/gifts")
        set_flash(
            response,
            kind="info",
            message=f"Gift code '{code}' not found.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    # Pre-build prev/next URLs (mirrors user_usage_get).
    prev_url = next_url = None
    base = f"/admin/gifts/{code}/redemptions"
    qs_extra = (
        f"&per_page={per_page}"
        if per_page != GIFT_REDEMPTIONS_PER_PAGE_DEFAULT else ""
    )
    if page_result is not None:
        if page_result["page"] > 1:
            p = page_result["page"] - 1
            prev_url = (
                base if p == 1 and not qs_extra
                else f"{base}?page={p}{qs_extra}"
            )
        if page_result["page"] < page_result["total_pages"]:
            p = page_result["page"] + 1
            next_url = f"{base}?page={p}{qs_extra}"

    return aiohttp_jinja2.render_template(
        "gift_redemptions.html",
        request,
        {
            "active_page": "gifts",
            "code": code,
            "gift": gift_meta,
            "result": page_result,
            "aggregates": aggregates,
            "db_error": db_error,
            "prev_url": prev_url,
            "next_url": next_url,
            "per_page": per_page,
            "per_page_choices": GIFT_REDEMPTIONS_PER_PAGE_CHOICES,
        },
    )


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
        "supported_languages": list(
            bot_strings_module.SUPPORTED_LANGUAGES
        ),
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


# Stage-9-Step-8: per-user usage browser pagination knobs.
USAGE_LOGS_PER_PAGE_DEFAULT = 50
USAGE_LOGS_PER_PAGE_MAX = 200
USAGE_LOGS_PER_PAGE_CHOICES = (25, 50, 100, 200)


async def user_usage_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/users/{telegram_id}/usage — per-user AI usage log.

    Stage-9-Step-8. Last N AI calls for one user with model, token
    counts, and per-call cost. Backed by the new
    ``idx_usage_logs_telegram_created`` index — without that index
    this query was a sequential scan over the whole table.
    """
    raw_id = request.match_info.get("telegram_id", "")
    try:
        user_id = int(raw_id)
    except ValueError:
        return web.HTTPFound(location="/admin/users")

    try:
        page = max(1, int(request.rel_url.query.get("page", "1")))
    except (ValueError, TypeError):
        page = 1
    try:
        per_page = int(
            request.rel_url.query.get(
                "per_page", str(USAGE_LOGS_PER_PAGE_DEFAULT)
            )
        )
    except (ValueError, TypeError):
        per_page = USAGE_LOGS_PER_PAGE_DEFAULT
    per_page = max(1, min(per_page, USAGE_LOGS_PER_PAGE_MAX))

    db = request.app.get(APP_KEY_DB)
    page_result: dict | None = None
    aggregates: dict | None = None
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        try:
            page_result = await db.list_user_usage_logs(
                telegram_id=user_id, page=page, per_page=per_page,
            )
            aggregates = await db.get_user_usage_aggregates(user_id)
        except Exception:
            log.exception(
                "user_usage_get: list_user_usage_logs failed user=%s",
                user_id,
            )
            db_error = "Database query failed — see logs."

    # Pre-build prev/next URLs.
    prev_url = next_url = None
    base = f"/admin/users/{user_id}/usage"
    qs_extra = (
        f"&per_page={per_page}"
        if per_page != USAGE_LOGS_PER_PAGE_DEFAULT else ""
    )
    if page_result is not None:
        if page_result["page"] > 1:
            p = page_result["page"] - 1
            prev_url = base if p == 1 and not qs_extra else f"{base}?page={p}{qs_extra}"
        if page_result["page"] < page_result["total_pages"]:
            p = page_result["page"] + 1
            next_url = f"{base}?page={p}{qs_extra}"

    return aiohttp_jinja2.render_template(
        "user_usage.html",
        request,
        {
            "active_page": "users",
            "user_id": user_id,
            "result": page_result,
            "aggregates": aggregates,
            "db_error": db_error,
            "prev_url": prev_url,
            "next_url": next_url,
            "per_page": per_page,
            "per_page_choices": USAGE_LOGS_PER_PAGE_CHOICES,
        },
    )


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
    await _record_audit_safe(
        request,
        "user_adjust",
        target=f"user:{user_id}",
        meta={
            "delta_usd": delta,
            "new_balance_usd": result["new_balance"],
            "transaction_id": result["transaction_id"],
            "reason": parsed["reason"],
        },
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


# Stage-9-Step-10: write progress to the durable ``broadcast_jobs``
# table at most once every ``BROADCAST_DB_PROGRESS_FLUSH_EVERY``
# recipients. The in-memory ``job`` dict still updates on every
# send (so the live-progress page polling against
# ``broadcast_status_get`` sees real-time numbers); the throttle
# only applies to the DB mirror so a 10 000-recipient broadcast
# isn't 10 000 UPDATE statements. Terminal transitions
# (``completed`` / ``failed`` / ``cancelled`` / ``interrupted``)
# always flush regardless of the throttle.
BROADCAST_DB_PROGRESS_FLUSH_EVERY: int = 25


async def _persist_broadcast_progress(
    db, job: dict, *, force: bool = False
) -> None:
    """Mirror the in-memory ``job`` dict's progress to ``broadcast_jobs``.

    Throttled to one UPDATE per
    ``BROADCAST_DB_PROGRESS_FLUSH_EVERY`` recipients; ``force=True``
    bypasses the throttle (terminal-state transitions always flush).
    Best-effort — a DB blip mid-broadcast logs a warning and lets
    the worker keep sending. The in-memory dict is the source of
    truth for the live-progress page; the DB is the durable mirror.
    """
    if db is None:
        return
    if not force:
        i = int(job.get("i", 0) or 0)
        if i and i % BROADCAST_DB_PROGRESS_FLUSH_EVERY != 0:
            return
    try:
        await db.update_broadcast_job(
            job["id"],
            total=int(job.get("total", 0) or 0),
            sent=int(job.get("sent", 0) or 0),
            blocked=int(job.get("blocked", 0) or 0),
            failed=int(job.get("failed", 0) or 0),
            i=int(job.get("i", 0) or 0),
        )
    except Exception:
        log.warning(
            "broadcast_job=%s: progress flush to broadcast_jobs failed",
            job.get("id"),
            exc_info=True,
        )


async def _persist_broadcast_state(
    db,
    job: dict,
    *,
    state: str,
    error: str | None = None,
    started: bool = False,
    completed: bool = False,
    cancel_requested: bool | None = None,
) -> None:
    """Mirror a state transition (queued → running → terminal) to
    ``broadcast_jobs``. Always force-flushes progress counters
    alongside the new state so a terminal row never carries stale
    sent/blocked/failed numbers. Best-effort (logs and continues
    on DB failure)."""
    if db is None:
        return
    try:
        await db.update_broadcast_job(
            job["id"],
            state=state,
            error=error,
            total=int(job.get("total", 0) or 0),
            sent=int(job.get("sent", 0) or 0),
            blocked=int(job.get("blocked", 0) or 0),
            failed=int(job.get("failed", 0) or 0),
            i=int(job.get("i", 0) or 0),
            cancel_requested=cancel_requested,
            started_at_now=started,
            completed_at_now=completed,
        )
    except Exception:
        log.warning(
            "broadcast_job=%s: state transition to %s failed to persist",
            job.get("id"), state,
            exc_info=True,
        )


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
        # Stage-9-Step-6 soft-cancel flag. Set by
        # ``broadcast_cancel_post``; polled by ``_do_broadcast`` at
        # the top of every send. ``False`` means the job was never
        # cancelled; if the flag was set but the loop already drained
        # to completion the resulting state stays ``"completed"`` —
        # cancellation is best-effort, not retroactive.
        "cancel_requested": False,
    }


#: Set of broadcast-job states the in-memory eviction policy is
#: allowed to drop. MUST stay in sync with
#: ``Database.BROADCAST_JOB_TERMINAL_STATES`` — drift between the
#: two means a job that's terminal in the durable registry would
#: still be pinned in memory (or vice-versa) and the eviction cap
#: stops working. Stage-9-Step-10 added ``"interrupted"`` here
#: alongside the original three; Devin Review caught the omission
#: in the eviction tuple at PR-time.
_BROADCAST_TERMINAL_STATES_FOR_EVICTION: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled", "interrupted"}
)


def _store_broadcast_job(app: web.Application, job: dict) -> None:
    """Record *job* in the registry and evict old completed entries.

    We never evict a job whose ``state`` is ``queued`` or ``running``
    — a rolling eviction policy must not silently kill live work.
    Terminal states (``completed`` / ``failed`` / ``cancelled`` /
    ``interrupted``) are evictable.
    """
    jobs: dict = app[APP_KEY_BROADCAST_JOBS]
    jobs[job["id"]] = job
    if len(jobs) > BROADCAST_MAX_HISTORY:
        terminal = [
            jid for jid, j in jobs.items()
            if j["state"] in _BROADCAST_TERMINAL_STATES_FOR_EVICTION
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
    # Stage-9-Step-10: mirror the queued → running transition.
    await _persist_broadcast_state(
        db, job, state="running", started=True
    )

    if db is None or bot is None:
        # Should never happen in production (both wired up by
        # setup_admin_routes) — belt-and-suspenders so a misconfigured
        # test path doesn't silently "complete" a zero-recipient job.
        job["state"] = "failed"
        job["error"] = (
            "Background task launched without a DB or bot wired up."
        )
        job["completed_at"] = _now_iso()
        await _persist_broadcast_state(
            db, job, state="failed",
            error=job["error"], completed=True,
        )
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
        await _persist_broadcast_state(
            db, job, state="failed",
            error=job["error"], completed=True,
        )
        return

    job["total"] = len(recipients)
    if not recipients:
        job["state"] = "completed"
        job["completed_at"] = _now_iso()
        await _persist_broadcast_state(
            db, job, state="completed", completed=True
        )
        return

    async def _on_progress(stats: dict) -> None:
        job["i"] = stats["i"]
        job["sent"] = stats["sent"]
        job["blocked"] = stats["blocked"]
        job["failed"] = stats["failed"]
        # Stage-9-Step-10: throttled mirror of progress to the
        # durable broadcast_jobs row so a process restart leaves a
        # forensic trail (best-effort; doesn't block the worker on
        # a transient DB blip).
        await _persist_broadcast_progress(db, job)

    def _cancel_requested() -> bool:
        # Stage-9-Step-6: ``broadcast_cancel_post`` flips this flag in
        # the live job dict. Polled at the top of every send loop in
        # ``admin._do_broadcast``; honoured within one pacing tick.
        return bool(job.get("cancel_requested"))

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
            should_cancel=_cancel_requested,
        )
    except asyncio.CancelledError:
        # Stage-9-Step-10 bundled bug fix: this branch fires when the
        # worker's asyncio Task is ``cancel()``-ed — which happens on
        # app shutdown (the on_cleanup hook cancels every entry in
        # ``APP_KEY_BROADCAST_TASKS``), NOT when an admin clicked the
        # "Cancel" button. Pre-fix we labelled the resulting row
        # ``state="failed"``, which conflated three semantically
        # different terminal states (``failed`` = exception in the
        # send loop; ``cancelled`` = admin clicked cancel and we
        # exited cleanly between sends; this branch = process killed
        # mid-send). The new ``"interrupted"`` state lets the recent-
        # jobs page distinguish a deploy-time restart from a code
        # bug, and matches the orphan-sweep state
        # ``mark_orphan_broadcast_jobs_interrupted`` writes for jobs
        # whose worker task didn't even get to ``except`` block (the
        # process was SIGKILL-ed).
        job["state"] = "interrupted"
        job["error"] = "Cancelled (admin panel shutting down)."
        job["completed_at"] = _now_iso()
        await _persist_broadcast_state(
            db, job, state="interrupted",
            error=job["error"], completed=True,
        )
        raise
    except Exception as exc:
        log.exception("broadcast_job=%s: _do_broadcast raised", job["id"])
        job["state"] = "failed"
        job["error"] = f"Broadcast failed: {exc}"
        job["completed_at"] = _now_iso()
        await _persist_broadcast_state(
            db, job, state="failed",
            error=job["error"], completed=True,
        )
        return

    # ``i`` is the count of recipients we actually attempted — for a
    # cancelled run that's ``sent + blocked + failed`` (every loop
    # iteration that didn't bail at the cancel-check), NOT
    # ``stats["total"]`` which is the recipient list length.
    job["sent"] = stats["sent"]
    job["blocked"] = stats["blocked"]
    job["failed"] = stats["failed"]
    if stats.get("cancelled"):
        job["i"] = stats["sent"] + stats["blocked"] + stats["failed"]
        job["state"] = "cancelled"
    else:
        job["i"] = stats["total"]
        job["state"] = "completed"
    job["completed_at"] = _now_iso()
    await _persist_broadcast_state(
        db, job, state=job["state"], completed=True
    )


async def broadcast_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/broadcast — form + recent jobs list.

    Stage-9-Step-10: the recent-jobs list is read from the durable
    ``broadcast_jobs`` table so a process restart doesn't orphan
    history. The in-memory ``APP_KEY_BROADCAST_JOBS`` dict is
    layered on top — a live-running job's progress counters in
    memory may be a few sends ahead of the throttled DB mirror, so
    if a job is present in both we prefer the in-memory copy for
    the live numbers (the row's terminal state always comes from
    the DB on a completed run).
    """
    db = request.app.get(APP_KEY_DB)
    in_memory: dict = request.app[APP_KEY_BROADCAST_JOBS]

    rows: list[dict] = []
    if db is not None:
        try:
            rows = await db.list_broadcast_jobs()
        except Exception:
            log.warning(
                "broadcast_get: list_broadcast_jobs failed; "
                "falling back to in-memory registry only",
                exc_info=True,
            )
            rows = []

    # Layer in-memory live data on top of the DB rows for jobs that
    # are still active (the throttled progress flush may be a few
    # sends behind the in-memory counters).
    if rows:
        recent: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            seen.add(row["id"])
            if row["state"] in ("queued", "running"):
                live = in_memory.get(row["id"])
                if live is not None:
                    recent.append(dict(live))
                    continue
            recent.append(row)
        # Surface any in-memory-only jobs the DB hasn't observed yet
        # (e.g. a row INSERT that lost a race with the recent-jobs
        # GET, or a test that didn't wire up the DB). Iterate
        # oldest → newest (insertion order) so each ``insert(0, …)``
        # pushes older items down — the final prefix is newest-first,
        # matching the DB rows' ``ORDER BY created_at DESC``. (Reversing
        # first would yield oldest-first; Devin Review caught this on
        # the first revision of PR #91.)
        for jid, live in in_memory.items():
            if jid not in seen:
                recent.insert(0, dict(live))
    else:
        # DB unavailable / empty — fall back to the in-memory dict
        # only. Newest first; copy dicts so a background writer
        # can't mutate under the Jinja template iterator.
        recent = [dict(j) for j in reversed(list(in_memory.values()))]

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
    # Stage-9-Step-10: insert the durable mirror row before kicking
    # off the worker so a crash between create_task and the worker's
    # first state write still leaves a forensic trail. Best-effort:
    # if the DB is unavailable the in-memory job still runs (the
    # broadcast itself doesn't depend on broadcast_jobs).
    db = request.app.get(APP_KEY_DB)
    if db is not None:
        try:
            await db.insert_broadcast_job(
                job_id=job["id"],
                text_preview=job["text_preview"],
                full_text_len=job["full_text_len"],
                only_active_days=job["only_active_days"],
                state="queued",
            )
        except Exception:
            log.warning(
                "broadcast_post: insert_broadcast_job failed for "
                "job=%s; in-memory job will still run but the "
                "durable mirror is missing.",
                job["id"],
                exc_info=True,
            )
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
    await _record_audit_safe(
        request,
        "broadcast_start",
        target=f"broadcast:{job['id']}",
        meta={
            "text_len": job["full_text_len"],
            "only_active_days": parsed["only_active_days"],
        },
    )
    return web.HTTPFound(location=f"/admin/broadcast/{job['id']}")


async def _resolve_broadcast_job(
    request: web.Request, job_id: str
) -> dict | None:
    """Look up a broadcast job by id, preferring the live in-memory
    dict and falling back to the durable ``broadcast_jobs`` row.

    Stage-9-Step-10: prior to durable storage, an admin who reloaded
    a `/admin/broadcast/{id}` link after a process restart got an
    "unknown job" redirect. Now the DB row carries the terminal
    state forward so the link still resolves — we just lose live
    progress polling once the worker task is gone.
    """
    live = request.app[APP_KEY_BROADCAST_JOBS].get(job_id)
    if live is not None:
        return dict(live)
    db = request.app.get(APP_KEY_DB)
    if db is None:
        return None
    try:
        return await db.get_broadcast_job(job_id)
    except Exception:
        log.warning(
            "_resolve_broadcast_job: get_broadcast_job(%s) failed",
            job_id, exc_info=True,
        )
        return None


async def broadcast_detail_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/broadcast/{job_id} — live-progress page."""
    job_id = request.match_info.get("job_id", "")
    job = await _resolve_broadcast_job(request, job_id)
    if job is None:
        response = web.HTTPFound(location="/admin/broadcast")
        set_flash(
            response,
            kind="error",
            message=(
                f"Unknown broadcast job {job_id!r}. "
                f"(Job not found in the durable registry.)"
            ),
            secret=request.app.get(APP_KEY_SESSION_SECRET, ""),
            cookie_secure=request.app.get(APP_KEY_COOKIE_SECURE, True),
        )
        return response

    # ``_resolve_broadcast_job`` already returned a snapshot dict, so
    # the template never sees a half-updated row mutated by a
    # concurrent worker.
    return aiohttp_jinja2.render_template(
        "broadcast_detail.html",
        request,
        {
            "active_page": "broadcast",
            "job": job,
            # Stage-9-Step-6: CSRF token for the cancel-button form.
            "csrf_token": csrf_token_for(request),
        },
    )


async def broadcast_status_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/broadcast/{job_id}/status — JSON for polling."""
    job_id = request.match_info.get("job_id", "")
    job = await _resolve_broadcast_job(request, job_id)
    if job is None:
        return web.json_response(
            {"error": "unknown_job", "job_id": job_id}, status=404
        )
    # ``_resolve_broadcast_job`` already returned a snapshot dict.
    return web.json_response(job)


async def broadcast_cancel_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/broadcast/{job_id}/cancel — flip the soft-cancel flag.

    Stage-9-Step-6. The cancel is *cooperative* — we just set
    ``job["cancel_requested"] = True`` in the in-memory job dict.
    The running ``_do_broadcast`` loop polls this flag at the top of
    each iteration and exits cleanly within one pacing tick. The
    background task itself is NOT ``task.cancel()``-ed because
    ``CancelledError`` mid-send would be counted as a failure for a
    recipient who actually received the message.

    Idempotent: a second cancel on an already-cancelled job is a
    no-op redirect with no audit double-write. Refuses with a flash
    error on terminal-state jobs (``completed`` / ``failed`` /
    already ``cancelled``).
    """
    job_id = request.match_info.get("job_id", "")
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    back = web.HTTPFound(location=f"/admin/broadcast/{job_id}")

    form = await request.post()
    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning(
            "broadcast_cancel_post: CSRF token mismatch from %s",
            request.remote,
        )
        set_flash(
            back,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return back

    job = request.app[APP_KEY_BROADCAST_JOBS].get(job_id)
    if job is None:
        # Don't redirect into a 404 detail page; bounce to the index.
        response = web.HTTPFound(location="/admin/broadcast")
        set_flash(
            response,
            kind="error",
            message=(
                f"Broadcast job '{job_id}' not found "
                "(it may have been evicted from the in-memory registry, "
                "or its worker task is no longer running after a restart)."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if job["state"] not in ("queued", "running"):
        set_flash(
            back,
            kind="error",
            message=(
                f"Cannot cancel a {job['state']} broadcast — only "
                "queued or running jobs can be cancelled."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return back

    if job.get("cancel_requested"):
        # Already in the cancellation window; don't re-audit.
        set_flash(
            back,
            kind="info",
            message=(
                "Cancel already requested — the worker will exit at the "
                "next loop iteration."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return back

    job["cancel_requested"] = True
    # Stage-9-Step-10: mirror the cancel flag to the durable mirror so
    # the recent-jobs list shows "cancelled" status promptly even
    # before the worker reaches its next loop iteration. Best-effort.
    db = request.app.get(APP_KEY_DB)
    if db is not None:
        try:
            await db.update_broadcast_job(
                job_id, cancel_requested=True
            )
        except Exception:
            log.warning(
                "broadcast_cancel_post: cancel flag mirror failed "
                "for job=%s; in-memory flag still set so the worker "
                "will honour it on the next loop iteration.",
                job_id, exc_info=True,
            )
    log.info(
        "broadcast_cancel_post: cancel requested for job=%s "
        "(state=%s, sent=%d/%d)",
        job_id, job["state"], job.get("sent", 0), job.get("total", 0),
    )
    await _record_audit_safe(
        request,
        "broadcast_cancel",
        target=f"broadcast:{job_id}",
        meta={
            "state_at_cancel": job["state"],
            "sent_at_cancel": job.get("sent", 0),
            "total": job.get("total", 0),
        },
    )
    set_flash(
        back,
        kind="info",
        message=(
            "Cancel requested — the worker will stop at the next "
            "recipient (within ~1 second)."
        ),
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return back


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


# Stage-9-Step-7: page size used by the CSV streamer.
# 500 rows ≈ 100 KB after CSV serialization — small enough that we
# don't pin the asyncpg connection for too long on a single page,
# big enough that we don't pay round-trip overhead for every row.
TRANSACTIONS_CSV_BATCH_SIZE = 500
# Defence-in-depth: refuse a CSV export beyond this many rows so a
# pathological filter ("everything ever") can't lock the connection
# pool indefinitely. 500k rows ≈ 100 MB CSV which is already past
# what a browser-side download will gracefully handle.
TRANSACTIONS_CSV_MAX_ROWS = 500_000

# Header row is hoisted to a module constant so the test can pin it
# without copy-pasting the column list. Order MUST match the values
# yielded in :func:`transactions_csv_get`.
TRANSACTIONS_CSV_HEADERS = (
    "transaction_id",
    "telegram_id",
    "gateway",
    "currency",
    "amount_crypto_or_rial",
    "amount_usd",
    "status",
    "gateway_invoice_id",
    "created_at",
    "completed_at",
    "notes",
)


def _csv_quote(value) -> str:
    """Minimal RFC 4180 CSV field encoder.

    We could use the stdlib ``csv`` module here but ``csv.writer``
    expects a writable text-IO target; for streaming we want to emit
    one row at a time as a string and let aiohttp handle the
    transport. Hand-rolling keeps the streamer trivially testable
    and avoids the ``StringIO`` allocation per batch. None ⇒ empty
    field.
    """
    if value is None:
        return ""
    s = str(value)
    if any(ch in s for ch in ('"', ",", "\n", "\r")):
        # Double up internal quotes, then wrap.
        return '"' + s.replace('"', '""') + '"'
    return s


def _format_tx_row_for_csv(row: dict) -> str:
    """Serialize one row from ``Database.list_transactions['rows']``
    into a single CSV line (with trailing CRLF).

    Numeric ``amount_usd`` is emitted with **4 decimal places, no
    commas, no dollar sign** — CSV is a machine-readable format and
    accounting software (Excel, QuickBooks) will reject ``$1,234``
    but happily import ``1234.5678``. The 4-decimal precision matches
    the in-UI ``format_usd`` default so a manual reconciliation
    against the on-screen ledger is exact.
    """
    fields = [
        row["id"],
        row["telegram_id"] if row["telegram_id"] is not None else "",
        row["gateway"],
        row["currency"],
        f"{row['amount_crypto_or_rial']}" if row["amount_crypto_or_rial"] is not None else "",
        f"{row['amount_usd']:.4f}",
        row["status"],
        row["gateway_invoice_id"] or "",
        row["created_at"] or "",
        row["completed_at"] or "",
        row["notes"] or "",
    ]
    return ",".join(_csv_quote(f) for f in fields) + "\r\n"


async def transactions_csv_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/transactions?format=csv — streamed CSV export.

    Stage-9-Step-7. Same filter semantics as the HTML page (gateway,
    status, telegram_id) but pagination params are ignored — a CSV
    export is always full-result. Streamed via aiohttp
    :class:`StreamResponse` in batches of
    ``TRANSACTIONS_CSV_BATCH_SIZE`` so even a 500k-row export
    doesn't blow the bot's memory.
    """
    db = request.app[APP_KEY_DB]
    filters = parse_transactions_query(request.rel_url.query)

    response = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/csv; charset=utf-8",
            # Filename includes the timestamp so an admin running
            # multiple exports doesn't accidentally overwrite an
            # in-progress download. ``transactions-YYYYMMDDTHHMMSSZ.csv``.
            "Content-Disposition": (
                "attachment; "
                f"filename=\"transactions-{_now_compact()}.csv\""
            ),
            # Defence-in-depth: explicitly disable any caching layer
            # between the bot and the admin's browser. A cached CSV
            # would leak ledger data to a later admin session that
            # logged in to the same machine.
            "Cache-Control": "no-store, max-age=0",
        },
    )
    await response.prepare(request)

    # Header row first.
    header = ",".join(_csv_quote(h) for h in TRANSACTIONS_CSV_HEADERS) + "\r\n"
    await response.write(header.encode("utf-8"))

    page = 1
    rows_emitted = 0
    while True:
        try:
            page_result = await db.list_transactions(
                gateway=filters["gateway"],
                status=filters["status"],
                telegram_id=filters["telegram_id"],
                page=page,
                per_page=TRANSACTIONS_CSV_BATCH_SIZE,
            )
        except ValueError:
            # Filters were already enum-validated by
            # parse_transactions_query, so reaching this branch means
            # the DB layer added a new validation rule mid-export.
            # Truncate cleanly rather than raising; the partial CSV
            # is still useful for forensics.
            log.warning(
                "transactions_csv_get: list_transactions rejected "
                "filters=%s mid-export",
                filters,
            )
            break

        rows = page_result.get("rows", [])
        if not rows:
            break

        chunk_lines: list[str] = []
        for row in rows:
            chunk_lines.append(_format_tx_row_for_csv(row))
            rows_emitted += 1
            if rows_emitted >= TRANSACTIONS_CSV_MAX_ROWS:
                log.warning(
                    "transactions_csv_get: reached cap of %d rows "
                    "for filters=%s — truncating",
                    TRANSACTIONS_CSV_MAX_ROWS, filters,
                )
                break
        await response.write("".join(chunk_lines).encode("utf-8"))

        if rows_emitted >= TRANSACTIONS_CSV_MAX_ROWS:
            break
        if page >= page_result.get("total_pages", 0):
            break
        page += 1

    await response.write_eof()

    # Audit the export (best-effort; never break the response over a
    # failed audit insert).
    await _record_audit_safe(
        request,
        "transactions_export_csv",
        target="transactions",
        meta={
            "rows": rows_emitted,
            "filters": {
                "gateway": filters.get("gateway"),
                "status": filters.get("status"),
                "telegram_id": filters.get("telegram_id"),
            },
        },
    )
    log.info(
        "transactions_csv_get: exported %d rows for filters=%s",
        rows_emitted, filters,
    )
    return response


def _now_compact() -> str:
    """``20260101T120000Z`` style timestamp for CSV filenames.

    Hoisted out of ``transactions_csv_get`` so a future caller
    needing the same shape (e.g. ledger-snapshot dump) can reuse it.
    """
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")


async def transactions_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/transactions — paginated ledger browser.

    Special-cases ``?format=csv`` to delegate to the streaming CSV
    exporter (see :func:`transactions_csv_get`).
    """
    if request.rel_url.query.get("format", "").lower() == "csv":
        return await transactions_csv_get(request)

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

    # Stage-9-Step-7: pre-build the CSV-export query string. Same
    # filters as the page, plus ``format=csv`` and explicitly NO
    # pagination params (CSV exports the whole filtered set).
    csv_query_parts = _encode_tx_query({**filters, "page": 1})
    csv_query = (
        csv_query_parts + "&format=csv" if csv_query_parts else "format=csv"
    )

    context = {
        "active_page": "transactions",
        "filters": filters,
        "result": page_result,
        "prev_url": prev_url,
        "next_url": next_url,
        "gateway_choices": sorted(Database.TRANSACTIONS_GATEWAY_VALUES),
        "status_choices": sorted(Database.TRANSACTIONS_STATUS_VALUES),
        "per_page_choices": TRANSACTIONS_PER_PAGE_CHOICES,
        "csv_query": csv_query,
        # Stage-12-Step-A: drives the inline "Refund" button on
        # SUCCESS rows. Templates can't import the constant, so
        # we hand it through the context (matches the
        # ``gateway_choices`` / ``status_choices`` pattern above).
        "refundable_gateways": sorted(Database.REFUNDABLE_GATEWAYS),
        "refund_reason_max_chars": REFUND_REASON_MAX_CHARS,
        "csrf_token": csrf_token_for(request),
        "flash": None,
    }
    response = aiohttp_jinja2.render_template(
        "transactions.html", request, context
    )
    # Stage-12-Step-A: refund POSTs redirect back here with a flash
    # banner. Mirror the users / promos / gifts re-render pattern.
    flash = pop_flash(request, response)
    if flash is not None:
        context["flash"] = flash
        response = aiohttp_jinja2.render_template(
            "transactions.html", request, context
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


# ---------------------------------------------------------------------
# Stage-12-Step-A: refund a SUCCESS transaction.
# ---------------------------------------------------------------------
#
# An admin clicking "Refund" on a SUCCESS row in the transactions
# browser POSTs here. The handler is gateway-agnostic — it only
# touches the ledger + wallet (the actual money-movement back to the
# user is the operator's responsibility, off-platform; NowPayments
# has no programmatic refund API and TetraPay's would be a future
# enhancement). Every refund writes a ``refund_issued`` audit row
# (or a ``refund_refused`` row when the operator's request is
# rejected) so the audit log distinguishes "we tried" from "we
# succeeded".

# The route prepends this prefix to the operator-supplied reason so
# audit trails are easy to grep across web vs Telegram-DM-initiated
# wallet movements (mirrors the ``user_adjust`` note convention).
_REFUND_REASON_PREFIX = "[web] "

# Hard cap on the operator-supplied reason. Calculated as the DB-side
# ``Database.REFUND_REASON_MAX_LEN`` minus the prefix length so the
# stored ``reason_raw + prefix`` value fits within the DB-layer limit
# without truncation. A previous version of this constant hard-coded
# ``500`` (the DB cap) which let a 500-char operator reason slip
# past the form validation, get prefixed to 506 chars, then trip the
# ``ValueError`` raised by ``Database.refund_transaction`` — caught by
# the route's exception handler, but only after rendering a confusing
# "Invalid input: reason longer than … (500); got 506" banner. Now
# the form validation is the single source of truth and rejects
# oversize input cleanly with the actual operator-facing limit.
REFUND_REASON_MAX_CHARS = (
    Database.REFUND_REASON_MAX_LEN - len(_REFUND_REASON_PREFIX)
)


_REFUND_REFUSAL_TEXT = {
    Database.REFUND_REFUSAL_NOT_SUCCESS: (
        "Refund refused — only SUCCESS rows can be refunded "
        "(this row is in status {current_status})."
    ),
    Database.REFUND_REFUSAL_GATEWAY_NOT_REFUNDABLE: (
        "Refund refused — this gateway is not eligible for the "
        "refund flow. Use the Users page to credit/debit instead."
    ),
    Database.REFUND_REFUSAL_INSUFFICIENT_BALANCE: (
        "Refund refused — user has spent the credit. Current "
        "balance ${balance_usd:.4f} is below the refund amount "
        "${amount_usd:.4f}. Debit them manually first via the "
        "Users page, then retry."
    ),
}


async def transaction_refund_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/transactions/{transaction_id}/refund — issue a refund.

    Form fields:
        * ``csrf_token`` — required; same scheme as every other
          POST endpoint.
        * ``reason`` — required; free text, capped at
          ``REFUND_REASON_MAX_CHARS`` chars after strip.

    Always redirects back to ``/admin/transactions`` (the caller's
    list view) with a flash banner describing the outcome.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    raw_id = request.match_info.get("transaction_id", "")
    try:
        tx_id = int(raw_id)
    except ValueError:
        return web.HTTPFound(location="/admin/transactions")
    # Reject zero / negative ids early — would never match a real
    # SERIAL row and the DB method asserts on it anyway.
    if tx_id <= 0:
        return web.HTTPFound(location="/admin/transactions")

    form = await request.post()
    redirect_url = "/admin/transactions"
    response = web.HTTPFound(location=redirect_url)

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning(
            "transaction_refund_post: CSRF token mismatch from %s",
            request.remote,
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

    reason_raw = str(form.get("reason", "")).strip()
    if not reason_raw:
        set_flash(
            response,
            kind="error",
            message="Refund reason is required.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response
    if len(reason_raw) > REFUND_REASON_MAX_CHARS:
        set_flash(
            response,
            kind="error",
            message=(
                f"Refund reason too long "
                f"(max {REFUND_REASON_MAX_CHARS} chars; "
                f"got {len(reason_raw)})."
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
            message="No database wired up — cannot refund.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    # Form validation above already capped ``reason_raw`` such that the
    # prefixed string fits within the DB-side ``REFUND_REASON_MAX_LEN``
    # — see the comment on ``REFUND_REASON_MAX_CHARS`` for the math.
    note = f"{_REFUND_REASON_PREFIX}{reason_raw}"

    try:
        result = await db.refund_transaction(
            transaction_id=tx_id,
            reason=note,
            admin_telegram_id=ADMIN_WEB_SENTINEL_ID,
        )
    except ValueError as exc:
        log.warning(
            "transaction_refund_post: refund_transaction validation "
            "rejected tx=%d: %s",
            tx_id, exc,
        )
        set_flash(
            response,
            kind="error",
            message=f"Invalid input: {exc}",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response
    except Exception:
        log.exception(
            "transaction_refund_post: refund_transaction failed tx=%d",
            tx_id,
        )
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if result is None:
        set_flash(
            response,
            kind="error",
            message=f"No transaction with id {tx_id}.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        await _record_audit_safe(
            request,
            "refund_refused",
            target=f"transaction:{tx_id}",
            outcome="not_found",
            meta={"reason": reason_raw},
        )
        return response

    if "error" in result:
        err = result["error"]
        # Resolve the human-friendly banner template + interpolate
        # whichever subset of (current_status, balance_usd,
        # amount_usd) is relevant to that error variant.
        template = _REFUND_REFUSAL_TEXT.get(
            err, "Refund refused (reason: {error})."
        )
        message = template.format(
            error=err,
            current_status=result.get("current_status") or "?",
            balance_usd=result.get("balance_usd") or 0.0,
            amount_usd=result.get("amount_usd") or 0.0,
        )
        set_flash(
            response,
            kind="error",
            message=message,
            secret=secret,
            cookie_secure=cookie_secure,
        )
        await _record_audit_safe(
            request,
            "refund_refused",
            target=f"transaction:{tx_id}",
            outcome=err,
            meta={
                "reason": reason_raw,
                "current_status": result.get("current_status"),
                "balance_usd": result.get("balance_usd"),
                "amount_usd": result.get("amount_usd"),
            },
        )
        return response

    log.info(
        "web_admin transaction_refund: tx=%d user=%d amount=$%.4f "
        "reason=%r",
        tx_id, result["telegram_id"],
        result["amount_refunded_usd"], reason_raw,
    )
    await _record_audit_safe(
        request,
        "refund_issued",
        target=f"transaction:{tx_id}",
        meta={
            "telegram_id": result["telegram_id"],
            "amount_refunded_usd": result["amount_refunded_usd"],
            "new_balance_usd": result["new_balance_usd"],
            "reason": reason_raw,
        },
    )
    set_flash(
        response,
        kind="success",
        message=(
            f"Refunded transaction #{tx_id} — "
            f"debited ${result['amount_refunded_usd']:.4f} from "
            f"user {result['telegram_id']} "
            f"(new balance ${result['new_balance_usd']:.4f})."
        ),
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return response


# ---------------------------------------------------------------------
# Stage-9-Step-1.6: editable bot strings
# ---------------------------------------------------------------------
#
# The compiled string table in ``strings.py`` is the source of truth
# for every user-visible label. The ``/admin/strings`` page lets the
# operator override individual ``(lang, key)`` pairs at runtime — the
# DB row in ``bot_strings`` shadows the compiled default. Reverting
# (deleting the DB row) resurrects the compiled default.
#
# After every successful write the in-memory cache in
# ``strings.set_overrides`` is refreshed so the next ``t()`` call
# inside the bot serves the new value immediately.

# Hard cap on the override length the admin can submit. The compiled
# defaults max out around ~600 chars; allowing ~2 KB gives the operator
# room to add explanatory text without letting them paste a megabyte
# JSON blob into a button label.
STRING_OVERRIDE_MAX_CHARS = 2048


def _filter_compiled_strings(
    *,
    lang_filter: str | None,
    search: str | None,
    overrides_map: dict[tuple[str, str], str],
) -> list[dict]:
    """Build the per-row list rendered by ``strings.html``.

    Each row carries:
        * ``lang`` / ``key`` — the (lang, key) pair
        * ``default`` — compiled default text
        * ``current`` — override (or default if no override)
        * ``has_override`` — bool, drives the "revert" button
        * ``edit_url`` — fully-qualified link to the per-string editor

    Filters: case-insensitive substring match against either the slug
    or the current text. Lang filter clamps to a single locale.
    """
    rows: list[dict] = []
    needle = (search or "").strip().lower()
    for lang, key, default in bot_strings_module.iter_compiled_strings():
        if lang_filter and lang != lang_filter:
            continue
        override = overrides_map.get((lang, key))
        current = override if override is not None else default
        if needle:
            haystack = f"{key} {current}".lower()
            if needle not in haystack:
                continue
        rows.append(
            {
                "lang": lang,
                "key": key,
                "default": default,
                "current": current,
                "has_override": override is not None,
                "edit_url": f"/admin/strings/{lang}/{key}",
            }
        )
    return rows


async def strings_get(request: web.Request) -> web.StreamResponse:
    """List every (lang, key) string with current value + override flag."""
    db = request.app.get(APP_KEY_DB)
    overrides_map: dict[tuple[str, str], str] = {}
    db_error: str | None = None
    if db is not None:
        try:
            overrides_map = await db.load_all_string_overrides()
        except Exception:
            log.exception("strings_get: load_all_string_overrides failed")
            db_error = "Database query failed — see logs."

    lang_filter = request.query.get("lang") or None
    if lang_filter not in bot_strings_module.SUPPORTED_LANGUAGES:
        lang_filter = None
    search = request.query.get("q") or None

    rows = _filter_compiled_strings(
        lang_filter=lang_filter,
        search=search,
        overrides_map=overrides_map,
    )

    response = aiohttp_jinja2.render_template(
        "strings.html",
        request,
        {
            "rows": rows,
            "lang_filter": lang_filter,
            "search": search or "",
            "supported_langs": list(
                bot_strings_module.SUPPORTED_LANGUAGES
            ),
            "override_count": sum(1 for r in rows if r["has_override"]),
            "db_error": db_error,
            "active_page": "strings",
            "csrf_token": csrf_token_for(request),
            "flash": None,
        },
    )
    flash = pop_flash(request, response)
    if flash is not None:
        response = aiohttp_jinja2.render_template(
            "strings.html",
            request,
            {
                "rows": rows,
                "lang_filter": lang_filter,
                "search": search or "",
                "supported_langs": list(
                    bot_strings_module.SUPPORTED_LANGUAGES
                ),
                "override_count": sum(1 for r in rows if r["has_override"]),
                "db_error": db_error,
                "active_page": "strings",
                "csrf_token": csrf_token_for(request),
                "flash": flash,
            },
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def string_detail_get(request: web.Request) -> web.StreamResponse:
    """Single-string editor: shows compiled default, current override
    (if any), and a textarea pre-filled with the override-or-default."""
    lang = request.match_info["lang"]
    key = request.match_info["key"]
    if lang not in bot_strings_module.SUPPORTED_LANGUAGES:
        raise web.HTTPNotFound()
    default = bot_strings_module.get_compiled_default(lang, key)
    if default is None:
        # Slug doesn't exist in the compiled table for this lang —
        # don't let the admin invent new slugs (they'd never be read
        # by t()). 404 is the right answer.
        raise web.HTTPNotFound()

    db = request.app.get(APP_KEY_DB)
    override: str | None = None
    db_error: str | None = None
    if db is not None:
        try:
            overrides_map = await db.load_all_string_overrides()
            override = overrides_map.get((lang, key))
        except Exception:
            log.exception("string_detail_get: load failed")
            db_error = "Database query failed — see logs."

    response = aiohttp_jinja2.render_template(
        "string_detail.html",
        request,
        {
            "lang": lang,
            "key": key,
            "default": default,
            "override": override,
            "current": override if override is not None else default,
            "max_chars": STRING_OVERRIDE_MAX_CHARS,
            "db_error": db_error,
            "active_page": "strings",
            "csrf_token": csrf_token_for(request),
            "flash": None,
        },
    )
    flash = pop_flash(request, response)
    if flash is not None:
        response = aiohttp_jinja2.render_template(
            "string_detail.html",
            request,
            {
                "lang": lang,
                "key": key,
                "default": default,
                "override": override,
                "current": override if override is not None else default,
                "max_chars": STRING_OVERRIDE_MAX_CHARS,
                "db_error": db_error,
                "active_page": "strings",
                "csrf_token": csrf_token_for(request),
                "flash": flash,
            },
        )
        response.del_cookie(FLASH_COOKIE, path="/admin/")
    return response


async def _refresh_overrides_cache(db) -> None:
    """Reload the in-memory override cache so the bot serves the
    new values on the very next ``t()`` call. Failures are logged
    but not fatal — the admin already saved successfully."""
    try:
        overrides = await db.load_all_string_overrides()
        bot_strings_module.set_overrides(overrides)
    except Exception:
        log.exception(
            "string override cache refresh failed — bot may serve "
            "stale text until next process restart"
        )


async def string_save_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/strings/{lang}/{key} — upsert a single override."""
    lang = request.match_info["lang"]
    key = request.match_info["key"]
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    redirect_to = f"/admin/strings/{lang}/{key}"
    response = web.HTTPFound(location=redirect_to)

    if lang not in bot_strings_module.SUPPORTED_LANGUAGES:
        raise web.HTTPNotFound()
    if bot_strings_module.get_compiled_default(lang, key) is None:
        raise web.HTTPNotFound()

    form = await request.post()
    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning(
            "string_save_post: CSRF token mismatch from %s", request.remote
        )
        set_flash(
            response,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    raw = str(form.get("value", ""))
    # Telegram strips leading/trailing whitespace on inline-button
    # text anyway; trim it server-side so the override matches
    # what users will actually see.
    value = raw.strip()
    if not value:
        set_flash(
            response,
            kind="error",
            message="Override cannot be empty. Use 'Revert' to restore the default.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response
    if len(value) > STRING_OVERRIDE_MAX_CHARS:
        set_flash(
            response,
            kind="error",
            message=(
                f"Override is {len(value):,} characters; max is "
                f"{STRING_OVERRIDE_MAX_CHARS:,}."
            ),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    # Validate the override's ``str.format`` placeholders against the
    # compiled default's allowed kwarg names. Pre-fix, a typo like
    # ``{bal}`` (when the default is ``{balance}``) silently saved
    # into the DB and then every ``t()`` call rendering this slug
    # crashed with ``KeyError: 'bal'`` until an admin reverted the
    # override. With this check the admin gets immediate feedback at
    # save time and the broken value never reaches the override cache.
    validation_error = bot_strings_module.validate_override(lang, key, value)
    if validation_error is not None:
        set_flash(
            response,
            kind="error",
            message=validation_error,
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot save.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    try:
        await db.upsert_string_override(
            lang, key, value, updated_by="web"
        )
    except Exception:
        log.exception("string_save_post: upsert failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    await _refresh_overrides_cache(db)
    log.info(
        "string override saved lang=%s key=%s len=%d actor=web",
        lang, key, len(value),
    )
    await _record_audit_safe(
        request,
        "string_save",
        target=f"string:{lang}:{key}",
        meta={"length": len(value)},
    )
    set_flash(
        response,
        kind="success",
        message=f"Saved override for {lang}:{key}.",
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return response


async def string_revert_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/strings/{lang}/{key}/revert — drop the DB row so
    the compiled default takes effect again."""
    lang = request.match_info["lang"]
    key = request.match_info["key"]
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)

    redirect_to = f"/admin/strings/{lang}/{key}"
    response = web.HTTPFound(location=redirect_to)

    if lang not in bot_strings_module.SUPPORTED_LANGUAGES:
        raise web.HTTPNotFound()
    if bot_strings_module.get_compiled_default(lang, key) is None:
        raise web.HTTPNotFound()

    form = await request.post()
    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning(
            "string_revert_post: CSRF token mismatch from %s", request.remote
        )
        set_flash(
            response,
            kind="error",
            message="Form submission was rejected (CSRF). Refresh and try again.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot revert.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    try:
        deleted = await db.delete_string_override(lang, key)
    except Exception:
        log.exception("string_revert_post: delete failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    await _refresh_overrides_cache(db)
    if deleted:
        log.info("string override reverted lang=%s key=%s actor=web", lang, key)
        await _record_audit_safe(
            request,
            "string_revert",
            target=f"string:{lang}:{key}",
        )
        set_flash(
            response,
            kind="success",
            message=f"Reverted {lang}:{key} to compiled default.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response,
            kind="info",
            message=f"{lang}:{key} had no override — nothing to revert.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
    return response


# ---------------------------------------------------------------------
# User-field editor + audit log viewer (Stage-9-Step-2)
# ---------------------------------------------------------------------


# Length / value caps on the editable user fields. Kept generous so
# we don't reject a legitimate model id we haven't seen before, but
# tight enough that a malformed POST can't explode the row size.
USER_FIELD_MODEL_MAX_CHARS = 200
USER_FIELD_USERNAME_MAX_CHARS = 64
USER_FIELD_FREE_MSGS_MAX = 1_000_000


def parse_user_edit_form(form, *, current: dict) -> dict | str:
    """Parse the /admin/users/{id}/edit form into a dict of changed
    fields, or return an error-key string on validation failure.

    Only fields whose new value differs from ``current`` make it into
    the returned dict — that way ``update_user_admin_fields`` skips
    no-op writes and the audit-log entry only mentions what actually
    changed.

    ``current`` is the user's current row from
    ``Database.get_user_admin_summary``; we read ``language_code``,
    ``active_model``, ``memory_enabled`` (optional), ``username``,
    and ``free_messages_left`` from it.

    Error keys: ``bad_lang``, ``bad_model``, ``bad_memory``,
    ``bad_free_messages``, ``free_messages_too_large``,
    ``bad_username``, ``username_too_long``.
    """
    fields: dict = {}

    raw_lang = (form.get("language_code") or "").strip()
    if raw_lang:
        if raw_lang not in bot_strings_module.SUPPORTED_LANGUAGES:
            return "bad_lang"
        if raw_lang != (current.get("language_code") or ""):
            fields["language_code"] = raw_lang

    raw_model = (form.get("active_model") or "").strip()
    if raw_model:
        # Sanity-check the shape — OpenRouter ids are always
        # ``provider/name``. Anything else is almost certainly a typo
        # and would 400 on the next message anyway.
        #
        # Pre-fix the shape check was just ``"/" not in raw_model``,
        # which accepted ``"foo/"`` (provider + empty name),
        # ``"/bar"`` (empty provider + name), ``"/"``, ``"a/b/c"``
        # (ambiguous double-provider), and any string containing
        # whitespace mid-id (e.g. ``"openai/ gpt-4"`` after strip kept
        # the inner space). Each of those wrote garbage into
        # ``users.active_model`` and the user's next chat 400'd at
        # OpenRouter, surfacing as ``ai_provider_unavailable`` with no
        # hint that an admin just bricked their model. Tighten to:
        # exactly one ``/``, both sides non-empty, neither side
        # contains whitespace. We deliberately don't restrict the
        # allowed character set further (dots, dashes, colons,
        # underscores all appear in legitimate IDs like
        # ``qwen/qwen-2.5-72b-instruct:free``), but whitespace is a
        # reliable typo signal that no real model id contains.
        if len(raw_model) > USER_FIELD_MODEL_MAX_CHARS:
            return "bad_model"
        parts = raw_model.split("/")
        if len(parts) != 2:
            return "bad_model"
        provider, name = parts
        if not provider or not name:
            return "bad_model"
        if any(c.isspace() for c in raw_model):
            return "bad_model"
        if raw_model != (current.get("active_model") or ""):
            fields["active_model"] = raw_model

    # Memory toggle uses the standard "checkbox + hidden marker" trick:
    # a checkbox only submits ``on`` when checked, so we pair it with a
    # hidden ``memory_enabled_present=1`` field that tells us the form
    # actually rendered the toggle. Without the hidden field we can't
    # distinguish "unchecked" from "field omitted", and a partial form
    # would silently flip memory off for every user.
    if form.get("memory_enabled_present"):
        raw_memory = form.get("memory_enabled")
        new_memory = (
            str(raw_memory).lower() in ("on", "true", "1", "yes")
            if raw_memory is not None else False
        )
        current_memory = bool(current.get("memory_enabled", False))
        if new_memory != current_memory:
            fields["memory_enabled"] = new_memory

    raw_free = (form.get("free_messages_left") or "").strip()
    if raw_free:
        try:
            free_value = int(raw_free)
        except ValueError:
            return "bad_free_messages"
        if free_value < 0:
            return "bad_free_messages"
        if free_value > USER_FIELD_FREE_MSGS_MAX:
            return "free_messages_too_large"
        if free_value != int(current.get("free_messages_left") or 0):
            fields["free_messages_left"] = free_value

    # Username is the only optional / clearable field — empty input
    # means "clear it" (set to NULL). A whitespace-only value is also
    # treated as "clear".
    if "username" in form:
        raw_username = (form.get("username") or "").strip()
        if raw_username:
            if len(raw_username) > USER_FIELD_USERNAME_MAX_CHARS:
                return "username_too_long"
            # Telegram usernames are alphanumeric + underscore. We
            # accept the canonical form without the leading "@".
            cleaned = raw_username.lstrip("@")
            # Pre-fix bug: a raw value of ``"@"`` / ``"@@@"`` / etc.
            # collapsed to ``""`` after ``lstrip("@")`` and slipped
            # past the ``all(...)`` check (``all(empty_iterable)`` is
            # ``True``) — the empty string then got written to
            # ``users.username``, which is distinct from ``NULL`` at
            # the SQL level and breaks downstream ``WHERE username IS
            # NULL`` / display-name fallback logic. Reject explicitly;
            # admins who actually want to clear the field can submit
            # an empty string via the regular ``raw_username`` falsy
            # branch below.
            if not cleaned:
                return "bad_username"
            if not all(c.isalnum() or c == "_" for c in cleaned):
                return "bad_username"
            new_username: str | None = cleaned
        else:
            new_username = None
        if new_username != current.get("username"):
            fields["username"] = new_username

    return fields


_USER_EDIT_ERR_TEXT = {
    "bad_lang": "Pick a supported language code.",
    "bad_model": "Active model must be a non-empty 'provider/model' id.",
    "bad_memory": "Memory toggle had an unexpected value.",
    "bad_free_messages": "Free messages must be a non-negative integer.",
    "free_messages_too_large": (
        f"Free messages must be at most {USER_FIELD_FREE_MSGS_MAX:,}."
    ),
    "bad_username": (
        "Username must be alphanumeric or underscore (no spaces, no '@')."
    ),
    "username_too_long": (
        f"Username must be at most {USER_FIELD_USERNAME_MAX_CHARS} chars."
    ),
}


async def user_edit_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/users/{telegram_id}/edit — update non-balance fields.

    Balance is intentionally NOT editable here; it routes through
    ``user_adjust_post`` so every change leaves a transactions-ledger
    row. This handler only touches the allow-listed
    ``Database.USER_EDITABLE_FIELDS`` set.
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
            "user_edit_post: CSRF token mismatch from %s", request.remote
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

    db = request.app.get(APP_KEY_DB)
    if db is None:
        set_flash(
            response,
            kind="error",
            message="No database wired up — cannot edit.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    # Refetch the current row so the diff in ``parse_user_edit_form``
    # is against the latest state — we don't want to clobber a value
    # someone else changed since the form was rendered.
    try:
        summary = await db.get_user_admin_summary(user_id)
    except Exception:
        log.exception("user_edit_post: get_user_admin_summary failed")
        set_flash(
            response,
            kind="error",
            message="Database read failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if summary is None:
        set_flash(
            response,
            kind="error",
            message=f"No user with id {user_id}.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    parsed = parse_user_edit_form(form, current=summary)
    if isinstance(parsed, str):
        set_flash(
            response,
            kind="error",
            message=_USER_EDIT_ERR_TEXT.get(parsed, f"Invalid input ({parsed})."),
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if not parsed:
        set_flash(
            response,
            kind="info",
            message="No changes — every field already matches.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    try:
        result = await db.update_user_admin_fields(
            user_id, fields=parsed,
        )
    except ValueError as exc:
        # ``update_user_admin_fields`` raises ValueError on
        # disallowed columns. The allow-list keeps this unreachable
        # in normal flow; surface as a generic error if it ever fires.
        log.warning(
            "user_edit_post: update rejected user=%s err=%s",
            user_id, exc,
        )
        set_flash(
            response,
            kind="error",
            message="Field rejected — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response
    except Exception:
        log.exception("user_edit_post: update_user_admin_fields failed")
        set_flash(
            response,
            kind="error",
            message="Database write failed — see logs.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    if result is None:
        set_flash(
            response,
            kind="error",
            message=f"No user with id {user_id}.",
            secret=secret,
            cookie_secure=cookie_secure,
        )
        return response

    log.info(
        "web_admin user_edit: user=%s fields=%s",
        user_id, sorted(parsed.keys()),
    )
    await _record_audit_safe(
        request,
        "user_edit",
        target=f"user:{user_id}",
        meta={"changed": parsed},
    )
    summary_changes = ", ".join(f"{k}" for k in sorted(parsed.keys()))
    set_flash(
        response,
        kind="success",
        message=f"Saved changes to: {summary_changes}.",
        secret=secret,
        cookie_secure=cookie_secure,
    )
    return response


async def audit_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/audit — read-only feed of admin activity.

    Filters: ``?action=<slug>`` narrows by action, ``?actor=<id>``
    narrows by actor (currently always ``"web"`` — left in place
    for the day per-admin identity lands).
    """
    db = request.app.get(APP_KEY_DB)
    rows: list[dict] = []
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        action_filter = (request.query.get("action") or "").strip() or None
        actor_filter = (request.query.get("actor") or "").strip() or None
        try:
            rows = await db.list_admin_audit_log(
                limit=200,
                action=action_filter,
                actor=actor_filter,
            )
        except Exception:
            log.exception("audit_get: list_admin_audit_log failed")
            db_error = "Database query failed — see logs."

    context = {
        "rows": rows,
        "db_error": db_error,
        "active_page": "audit",
        "csrf_token": csrf_token_for(request),
        "action_labels": AUDIT_ACTION_LABELS,
        "selected_action": (request.query.get("action") or "").strip(),
        "selected_actor": (request.query.get("actor") or "").strip(),
    }
    return aiohttp_jinja2.render_template(
        "audit.html", request, context
    )


# ---------------------------------------------------------------------
# 2FA enrolment helper page (Stage-9-Step-3)
# ---------------------------------------------------------------------


async def enroll_2fa_get(request: web.Request) -> web.StreamResponse:
    """Render the TOTP enrolment helper.

    Always behind the admin login. The page does NOT mutate the
    configured secret — it just renders the operator-friendly view of
    whatever's currently in ``ADMIN_2FA_SECRET`` (so they can re-scan
    the QR after losing their device) and, when nothing is configured,
    suggests a freshly-generated random secret to copy into the env
    file. Restarting the bot is required to pick the new value up.
    """
    issuer = request.app.get(APP_KEY_TOTP_ISSUER, "Meowassist Admin") or "Meowassist Admin"
    configured_secret = request.app.get(APP_KEY_TOTP_SECRET, "")

    if configured_secret:
        secret = configured_secret
        is_suggestion = False
    else:
        # No secret on the running app — generate one so the operator
        # has something to paste into ``ADMIN_2FA_SECRET``. We
        # deliberately do NOT cache it server-side: the next page load
        # gets a fresh suggestion. That way an operator who eyeballs
        # the page without copying the secret can't be locked into a
        # value an attacker also saw via, e.g., a screenshot in chat.
        secret = pyotp.random_base32()
        is_suggestion = True

    uri = build_otpauth_uri(secret, issuer=issuer)
    qr_svg = render_qr_svg(uri)

    await _record_audit_safe(
        request,
        "enroll_2fa_view",
        meta={"is_suggestion": is_suggestion},
    )
    return aiohttp_jinja2.render_template(
        "enroll_2fa.html",
        request,
        {
            "active_page": "enroll_2fa",
            "secret": secret,
            "issuer": issuer,
            "uri": uri,
            "qr_svg": qr_svg,
            "is_suggestion": is_suggestion,
        },
    )


# ---------------------------------------------------------------------
# Stage-14: model & gateway toggle pages
# ---------------------------------------------------------------------

# Gateway labels for the admin UI. Matches handlers.SUPPORTED_PAY_CURRENCIES
# plus "tetrapay" for the Rial card gateway.
_GATEWAY_CARD_LIST: list[dict[str, str]] = [
    {"key": "tetrapay", "label": "TetraPay (Rial card)"},
]
_GATEWAY_CRYPTO_LIST: list[dict[str, str]] = [
    {"key": "btc", "label": "₿ Bitcoin"},
    {"key": "eth", "label": "Ξ Ethereum"},
    {"key": "ltc", "label": "🔷 Litecoin"},
    {"key": "ton", "label": "💎 TON"},
    {"key": "trx", "label": "⚡ TRON (TRX)"},
    {"key": "usdttrc20", "label": "💵 USDT (TRC20)"},
    {"key": "usdterc20", "label": "💵 USDT (ERC20)"},
    {"key": "usdtbsc", "label": "💵 USDT (BEP20)"},
    {"key": "usdtton", "label": "💵 USDT (TON)"},
]

# Provider display labels reused from handlers.py; importing them would
# create a circular import (handlers → web_admin), so duplicate the
# small map here.
_ADMIN_PROVIDER_LABELS: dict[str, str] = {
    "openai": "🟢 OpenAI",
    "anthropic": "🟣 Anthropic",
    "google": "🔵 Google",
    "x-ai": "⚫ xAI",
    "deepseek": "🐋 DeepSeek",
}


async def models_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/models — list all catalog models with disable/enable toggles."""
    from admin_toggles import get_disabled_models
    from models_catalog import get_catalog

    disabled = get_disabled_models()
    catalog = await get_catalog()

    providers: list[tuple[str, list]] = []
    for provider in sorted(catalog.by_provider.keys()):
        models = sorted(catalog.by_provider[provider], key=lambda m: m.id)
        providers.append((provider, models))

    total_models = sum(len(ms) for _, ms in providers)

    ctx = {
        "active_page": "models",
        "csrf_token": csrf_token_for(request),
        "flash": None,
        "providers": providers,
        "provider_labels": _ADMIN_PROVIDER_LABELS,
        "disabled": disabled,
        "disabled_count": len(disabled),
        "total_models": total_models,
    }
    response = aiohttp_jinja2.render_template("models.html", request, ctx)
    flash = pop_flash(request, response)
    if flash is not None:
        ctx["flash"] = flash
        response = aiohttp_jinja2.render_template("models.html", request, ctx)
    return response


async def _models_toggle_post(
    request: web.Request, *, enable: bool
) -> web.StreamResponse:
    """Shared POST handler for model enable / disable.

    Stage-15-Step-D #4 audit: ``model_id`` is read from the POST
    form body (``form.get("model_id")``) — NOT from a URL path
    parameter — so model IDs with embedded ``/`` characters
    (``openai/gpt-4o``, ``anthropic/claude-3-5-sonnet``) work
    transparently. aiohttp's path-template parameter matchers
    don't traverse ``/`` by default and would otherwise require
    the ``{model_id:.+}`` regex form, but the form-body design
    sidesteps that entirely.

    Stage-15-Step-D #3-extension-2 fix: wraps the canonical DB
    write in ``try`` / ``except`` so a transient
    ``asyncpg.ConnectionDoesNotExist`` (or any other
    ``Exception``) renders a flash error and a clean 302
    redirect back to the panel instead of bubbling up to a 500
    response. This complements PR #114 — PR #114 made the
    *post-write resync* fail-soft, but the **write itself** was
    still bare-await. On a transient blip the admin panel would
    return 500 even though the form was a valid request, leaving
    the operator confused about whether the toggle actually took
    effect (it usually didn't, since the DB write itself failed).
    Audit + cache-refresh only run on a successful write so the
    in-memory cache stays consistent with the DB state.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    db = request.app[APP_KEY_DB]
    form = await request.post()

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning("models_toggle: CSRF token mismatch from %s", request.remote)
        response = web.HTTPFound(location="/admin/models")
        set_flash(response, kind="error",
                  message="Form submission was rejected (CSRF). Refresh and try again.",
                  secret=secret, cookie_secure=cookie_secure)
        return response

    model_id = str(form.get("model_id", "")).strip()
    response = web.HTTPFound(location="/admin/models")
    if not model_id:
        set_flash(response, kind="warn", message="Missing model id.",
                  secret=secret, cookie_secure=cookie_secure)
        return response

    from admin_toggles import refresh_disabled_models

    try:
        if enable:
            await db.enable_model(model_id)
        else:
            await db.disable_model(model_id)
    except Exception:
        log.exception(
            "models_toggle: %s_model(%r) failed; rendering flash error",
            "enable" if enable else "disable",
            model_id,
        )
        set_flash(
            response, kind="error",
            message=(
                f"Failed to {'enable' if enable else 'disable'} model — "
                "DB error, see logs. The toggle did not take effect."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
        return response

    await refresh_disabled_models(db)
    audit_action = "model_enable" if enable else "model_disable"
    await _record_audit_safe(request, audit_action, target=model_id)
    verb = "Enabled" if enable else "Disabled"
    set_flash(
        response, kind="success", message=f"{verb} model: {model_id}",
        secret=secret, cookie_secure=cookie_secure,
    )
    return response


async def models_disable_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/models/disable."""
    return await _models_toggle_post(request, enable=False)


async def models_enable_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/models/enable."""
    return await _models_toggle_post(request, enable=True)


async def gateways_get(request: web.Request) -> web.StreamResponse:
    """GET /admin/gateways — list all payment gateways with toggles."""
    from admin_toggles import get_disabled_gateways

    disabled = get_disabled_gateways()

    ctx = {
        "active_page": "gateways",
        "csrf_token": csrf_token_for(request),
        "flash": None,
        "card_gateways": _GATEWAY_CARD_LIST,
        "crypto_gateways": _GATEWAY_CRYPTO_LIST,
        "disabled": disabled,
    }
    response = aiohttp_jinja2.render_template("gateways.html", request, ctx)
    flash = pop_flash(request, response)
    if flash is not None:
        ctx["flash"] = flash
        response = aiohttp_jinja2.render_template("gateways.html", request, ctx)
    return response


async def _gateways_toggle_post(
    request: web.Request, *, enable: bool
) -> web.StreamResponse:
    """Shared POST handler for gateway enable / disable.

    Stage-15-Step-D #3-extension-2 fix: same write-side fail-soft
    pattern as :func:`_models_toggle_post`. The canonical
    ``db.disable_gateway`` / ``db.enable_gateway`` call is wrapped
    in ``try`` / ``except`` so a transient DB blip renders a
    flash error and a clean 302 instead of a 500.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    db = request.app[APP_KEY_DB]
    form = await request.post()

    if not verify_csrf_token(request, str(form.get("csrf_token", ""))):
        log.warning("gateways_toggle: CSRF token mismatch from %s", request.remote)
        response = web.HTTPFound(location="/admin/gateways")
        set_flash(response, kind="error",
                  message="Form submission was rejected (CSRF). Refresh and try again.",
                  secret=secret, cookie_secure=cookie_secure)
        return response

    gateway_key = str(form.get("gateway_key", "")).strip()
    response = web.HTTPFound(location="/admin/gateways")
    if not gateway_key:
        set_flash(response, kind="warn", message="Missing gateway key.",
                  secret=secret, cookie_secure=cookie_secure)
        return response

    from admin_toggles import refresh_disabled_gateways

    try:
        if enable:
            await db.enable_gateway(gateway_key)
        else:
            await db.disable_gateway(gateway_key)
    except Exception:
        log.exception(
            "gateways_toggle: %s_gateway(%r) failed; rendering flash error",
            "enable" if enable else "disable",
            gateway_key,
        )
        set_flash(
            response, kind="error",
            message=(
                f"Failed to {'enable' if enable else 'disable'} gateway — "
                "DB error, see logs. The toggle did not take effect."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
        return response

    await refresh_disabled_gateways(db)
    audit_action = "gateway_enable" if enable else "gateway_disable"
    await _record_audit_safe(request, audit_action, target=gateway_key)
    verb = "Enabled" if enable else "Disabled"
    set_flash(
        response, kind="success", message=f"{verb} gateway: {gateway_key}",
        secret=secret, cookie_secure=cookie_secure,
    )
    return response


async def gateways_disable_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/gateways/disable."""
    return await _gateways_toggle_post(request, enable=False)


async def gateways_enable_post(request: web.Request) -> web.StreamResponse:
    """POST /admin/gateways/enable."""
    return await _gateways_toggle_post(request, enable=True)


# ---------------------------------------------------------------------
# Bot health & emergency control (Stage-15-Step-F)
# ---------------------------------------------------------------------
#
# Renders a single-page operator panel at ``/admin/control``:
#   * traffic-light status tile (idle / healthy / busy / degraded /
#     under-attack / down) computed by ``bot_health.compute_bot_status``
#   * live signals: in-flight chat slots, IPN drop totals,
#     login-throttle bucket count, disabled-models / disabled-gateways
#     counts, background loop heartbeats, process uptime + PID
#   * master kill-switches for every AI model and every payment
#     gateway (one click → write every key into the disabled tables)
#   * force-stop button: sends ``SIGTERM`` to the running PID via
#     ``bot_health.request_force_stop``. The operator's process
#     supervisor (systemd / docker / pm2) is expected to restart the
#     bot — this is the "kill it before it bleeds out" button, not a
#     graceful pause. For pause-only, use the kill-switches instead.
#
# Every POST handler is CSRF-protected via ``verify_csrf_token`` and
# audit-logged via ``_record_audit_safe`` so the operator can review
# emergency actions after the fact.

# Process boot timestamp — defer to the ``bot_health`` module so the
# uptime gauge in the control panel and the classifier's
# never-ticked-loop grace window agree on the same reference epoch.
# Using ``time.time()`` (wall clock) rather than ``time.monotonic()``
# because we render the value as "since process start" in the
# operator's local time, not as a stopwatch.
from bot_health import get_process_start_epoch as _bot_health_start_epoch

_BOT_PROCESS_START_EPOCH: float = _bot_health_start_epoch()


# Every gateway key the bot recognises. The crypto tickers come from
# ``handlers.SUPPORTED_PAY_CURRENCIES``; the card gateways are the
# Rial-side ones the bot ships. Imported lazily inside the helper so
# this module's import surface stays small (and so a future
# ``handlers.py`` edit doesn't fight an import cycle).
_KNOWN_CARD_GATEWAY_KEYS: tuple[str, ...] = ("tetrapay", "zarinpal")


def _all_gateway_keys() -> list[str]:
    """Return every gateway key the bot recognises (card + crypto).

    Order is stable across calls so the audit-log meta payload is
    diff-friendly when the operator hits "disable all" twice.
    """
    keys: list[str] = list(_KNOWN_CARD_GATEWAY_KEYS)
    try:
        from handlers import SUPPORTED_PAY_CURRENCIES

        keys.extend(ticker for _, ticker in SUPPORTED_PAY_CURRENCIES)
    except Exception:
        # Degrade rather than crash the panel if ``handlers.py`` is
        # half-imported under tests — the card gateway list is still
        # actionable on its own.
        log.exception("control: failed to import SUPPORTED_PAY_CURRENCIES")
    return keys


def _all_model_ids() -> list[str]:
    """Return every model id the in-memory OpenRouter catalog exposes.

    Reads ``models_catalog._catalog`` directly — the same warm cache
    the picker, pricing, and discovery loops use. Falls back to the
    empty list if the catalog hasn't loaded yet (cold-start window
    or test wiring without a refresh) — disabling zero models is a
    no-op so that's fine.
    """
    try:
        import models_catalog

        catalog = models_catalog._catalog
    except Exception:
        log.exception("control: failed to read models catalog")
        return []
    if catalog is None or not catalog.models:
        return []
    return sorted({m.id for m in catalog.models})


def _collect_control_signals(
    *, app: web.Application, db_error: str | None,
) -> dict:
    """Snapshot every numeric signal the control panel renders.

    Pure-ish — reads in-process counters and the OpenRouter catalog;
    no DB calls. Each accessor is wrapped in its own ``try`` so a
    regression in one source doesn't blank the rest of the panel.
    """
    # In-flight chat slots.
    try:
        from rate_limit import chat_inflight_count, login_throttle_active_count

        inflight_count = chat_inflight_count()
        login_keys = login_throttle_active_count(app)
    except Exception:
        log.exception("control: rate_limit accessor failed")
        inflight_count = 0
        login_keys = 0

    # IPN drop totals (NowPayments + TetraPay + Zarinpal). Reuses
    # the dashboard's collector so the totals match the dashboard
    # "IPN health" tile.
    ipn_health = _collect_ipn_health()
    ipn_drops_total = (
        int(ipn_health.get("nowpayments_total", 0))
        + int(ipn_health.get("tetrapay_total", 0))
        + int(ipn_health.get("zarinpal_total", 0))
    )

    # Disabled-{models,gateways} counts.
    try:
        from admin_toggles import (
            get_disabled_gateways,
            get_disabled_models,
        )

        disabled_models_count = len(get_disabled_models())
        disabled_gateways_count = len(get_disabled_gateways())
    except Exception:
        log.exception("control: admin_toggles accessor failed")
        disabled_models_count = 0
        disabled_gateways_count = 0

    # Background-loop heartbeats.
    try:
        from metrics import _LOOP_METRIC_NAMES, get_loop_last_tick
    except Exception:
        log.exception("control: metrics accessor failed")
        loop_names: tuple[str, ...] = ()
        get_last = lambda _name: None  # noqa: E731
    else:
        loop_names = _LOOP_METRIC_NAMES
        get_last = get_loop_last_tick

    now = time.time()
    loops: list[dict] = []
    loop_ticks_for_classifier: dict[str, float] = {}
    for name in loop_names:
        last = get_last(name)
        if last is None or last == 0.0:
            loops.append({"name": name, "last_tick_age_s": None})
            continue
        age = max(0, int(now - float(last)))
        loops.append({"name": name, "last_tick_age_s": age})
        loop_ticks_for_classifier[name] = float(last)

    # Total catalog sizes for the kill-switch summary.
    total_models = len(_all_model_ids())
    total_gateways = len(_all_gateway_keys())

    return {
        "inflight_count": int(inflight_count),
        "ipn_drops_total": int(ipn_drops_total),
        "login_throttle_active_keys": int(login_keys),
        "disabled_models_count": int(disabled_models_count),
        "disabled_gateways_count": int(disabled_gateways_count),
        "total_models_count": int(total_models),
        "total_gateways_count": int(total_gateways),
        "loops": loops,
        "loop_ticks_for_classifier": loop_ticks_for_classifier,
        "loop_names_for_classifier": tuple(loop_names),
        "uptime_seconds": max(0, int(now - _BOT_PROCESS_START_EPOCH)),
        "pid": os.getpid(),
    }


async def control_get(request: web.Request) -> web.StreamResponse:
    """``GET /admin/control`` — render the bot-health + emergency panel."""
    db = request.app.get(APP_KEY_DB)
    db_error: str | None = None
    if db is None:
        db_error = "No database wired up (development mode)."
    else:
        # Cheap probe — same shape as the dashboard's read so the
        # panel surfaces the *same* DB-error condition the operator
        # sees on the home tile. ``get_system_metrics`` is the
        # cheapest representative call; we only care about whether
        # the pool is alive.
        try:
            from pending_alert import get_pending_alert_threshold_hours
            await db.get_system_metrics(
                pending_alert_threshold_hours=(
                    get_pending_alert_threshold_hours()
                ),
            )
        except Exception:
            log.exception("control: db probe failed")
            db_error = "Database query failed — see logs."

    signals = _collect_control_signals(app=request.app, db_error=db_error)

    from bot_health import compute_bot_status

    # Read the bot-health alert loop's most-recent rate-windowed drop
    # count so the panel + the loop + Prometheus all classify
    # identically. The panel can't observe a rate-of-drops on its own
    # (each request is a snapshot, not a window) so we delegate to
    # the loop's bookkeeping. ``0`` until the loop has ticked once.
    try:
        from bot_health_alert import latest_observed_recent_drops

        ipn_drops_recent = latest_observed_recent_drops()
    except Exception:
        log.exception("control: latest_observed_recent_drops failed")
        ipn_drops_recent = 0
    signals["ipn_drops_recent"] = ipn_drops_recent

    status = compute_bot_status(
        inflight_count=signals["inflight_count"],
        ipn_drops_total=signals["ipn_drops_total"],
        ipn_drops_recent=ipn_drops_recent,
        loop_ticks=signals["loop_ticks_for_classifier"],
        expected_loops=signals["loop_names_for_classifier"],
        db_error=db_error,
        login_throttle_active_keys=signals["login_throttle_active_keys"],
    )

    ctx = {
        "active_page": "control",
        "csrf_token": csrf_token_for(request),
        "flash": None,
        "status": status,
        "signals": signals,
    }
    response = aiohttp_jinja2.render_template("control.html", request, ctx)
    flash = pop_flash(request, response)
    if flash is not None:
        ctx["flash"] = flash
        response = aiohttp_jinja2.render_template("control.html", request, ctx)
    return response


def _control_csrf_guard(
    request: web.Request, form, *, redirect_to: str = "/admin/control",
) -> web.StreamResponse | None:
    """Verify the CSRF token; return a redirect-with-flash on failure.

    Returns ``None`` on success so the caller can ``if guard:`` test.
    """
    if verify_csrf_token(request, str(form.get("csrf_token", ""))):
        return None
    log.warning(
        "control: CSRF token mismatch from %s (path=%s)",
        request.remote, request.path,
    )
    response = web.HTTPFound(location=redirect_to)
    set_flash(
        response, kind="error",
        message="Form submission was rejected (CSRF). Refresh and try again.",
        secret=request.app.get(APP_KEY_SESSION_SECRET, ""),
        cookie_secure=request.app.get(APP_KEY_COOKIE_SECURE, True),
    )
    return response


async def control_disable_all_models_post(
    request: web.Request,
) -> web.StreamResponse:
    """``POST /admin/control/disable-all-models`` — master kill-switch."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    db = request.app[APP_KEY_DB]
    form = await request.post()

    guard = _control_csrf_guard(request, form)
    if guard is not None:
        return guard

    from admin_toggles import refresh_disabled_models

    model_ids = _all_model_ids()
    newly_disabled = 0
    failed = 0
    for model_id in model_ids:
        try:
            if await db.disable_model(model_id, actor="web:control"):
                newly_disabled += 1
        except Exception:
            log.exception(
                "control: disable_all_models — disable_model(%r) failed",
                model_id,
            )
            failed += 1
    await refresh_disabled_models(db)
    await _record_audit_safe(
        request, "control_disable_all_models",
        meta={
            "total_models": len(model_ids),
            "newly_disabled": newly_disabled,
            "failed": failed,
        },
    )
    response = web.HTTPFound(location="/admin/control")
    if failed:
        set_flash(
            response, kind="error",
            message=(
                f"Disabled {newly_disabled} of {len(model_ids)} models — "
                f"{failed} write(s) failed (see logs)."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response, kind="success",
            message=(
                f"Disabled all {len(model_ids)} model(s). "
                f"{newly_disabled} newly disabled."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
    return response


async def control_enable_all_models_post(
    request: web.Request,
) -> web.StreamResponse:
    """``POST /admin/control/enable-all-models`` — clear the disabled-models table."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    db = request.app[APP_KEY_DB]
    form = await request.post()

    guard = _control_csrf_guard(request, form)
    if guard is not None:
        return guard

    from admin_toggles import refresh_disabled_models

    # Snapshot from the DB (not the in-memory cache) so a freshly-
    # restarted process that hasn't warmed the cache yet still
    # re-enables every row. ``get_disabled_models`` returns a set of
    # model_id strings.
    try:
        before = list(await db.get_disabled_models())
    except Exception:
        log.exception(
            "control: enable_all_models — get_disabled_models read failed"
        )
        before = []
    cleared = 0
    failed = 0
    for model_id in before:
        try:
            if await db.enable_model(model_id):
                cleared += 1
        except Exception:
            log.exception(
                "control: enable_all_models — enable_model(%r) failed",
                model_id,
            )
            failed += 1
    await refresh_disabled_models(db)
    await _record_audit_safe(
        request, "control_enable_all_models",
        meta={"cleared": cleared, "failed": failed},
    )
    response = web.HTTPFound(location="/admin/control")
    if failed:
        set_flash(
            response, kind="error",
            message=(
                f"Re-enabled {cleared} model(s) — {failed} write(s) failed "
                "(see logs)."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response, kind="success",
            message=f"Re-enabled all {cleared} previously-disabled model(s).",
            secret=secret, cookie_secure=cookie_secure,
        )
    return response


async def control_disable_all_gateways_post(
    request: web.Request,
) -> web.StreamResponse:
    """``POST /admin/control/disable-all-gateways`` — master kill-switch."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    db = request.app[APP_KEY_DB]
    form = await request.post()

    guard = _control_csrf_guard(request, form)
    if guard is not None:
        return guard

    from admin_toggles import refresh_disabled_gateways

    gateway_keys = _all_gateway_keys()
    newly_disabled = 0
    failed = 0
    for key in gateway_keys:
        try:
            if await db.disable_gateway(key, actor="web:control"):
                newly_disabled += 1
        except Exception:
            log.exception(
                "control: disable_all_gateways — disable_gateway(%r) failed",
                key,
            )
            failed += 1
    await refresh_disabled_gateways(db)
    await _record_audit_safe(
        request, "control_disable_all_gateways",
        meta={
            "total_gateways": len(gateway_keys),
            "newly_disabled": newly_disabled,
            "failed": failed,
        },
    )
    response = web.HTTPFound(location="/admin/control")
    if failed:
        set_flash(
            response, kind="error",
            message=(
                f"Disabled {newly_disabled} of {len(gateway_keys)} gateways "
                f"— {failed} write(s) failed (see logs)."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response, kind="success",
            message=(
                f"Disabled all {len(gateway_keys)} payment gateway "
                f"key(s). {newly_disabled} newly disabled."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
    return response


async def control_enable_all_gateways_post(
    request: web.Request,
) -> web.StreamResponse:
    """``POST /admin/control/enable-all-gateways`` — clear the disabled-gateways table."""
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    db = request.app[APP_KEY_DB]
    form = await request.post()

    guard = _control_csrf_guard(request, form)
    if guard is not None:
        return guard

    from admin_toggles import refresh_disabled_gateways

    # Read from the DB (not the in-memory cache) — see
    # ``control_enable_all_models_post`` for the rationale.
    try:
        before = list(await db.get_disabled_gateways())
    except Exception:
        log.exception(
            "control: enable_all_gateways — get_disabled_gateways read failed"
        )
        before = []
    cleared = 0
    failed = 0
    for key in before:
        try:
            if await db.enable_gateway(key):
                cleared += 1
        except Exception:
            log.exception(
                "control: enable_all_gateways — enable_gateway(%r) failed",
                key,
            )
            failed += 1
    await refresh_disabled_gateways(db)
    await _record_audit_safe(
        request, "control_enable_all_gateways",
        meta={"cleared": cleared, "failed": failed},
    )
    response = web.HTTPFound(location="/admin/control")
    if failed:
        set_flash(
            response, kind="error",
            message=(
                f"Re-enabled {cleared} gateway(s) — {failed} write(s) "
                "failed (see logs)."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
    else:
        set_flash(
            response, kind="success",
            message=f"Re-enabled all {cleared} previously-disabled gateway(s).",
            secret=secret, cookie_secure=cookie_secure,
        )
    return response


async def control_force_stop_post(
    request: web.Request,
) -> web.StreamResponse:
    """``POST /admin/control/force-stop`` — SIGTERM the running bot.

    The handler:

    1. Verifies the CSRF token (else 302 + flash).
    2. Verifies the ``confirm`` field equals ``FORCE-STOP`` (the
       template injects this so a stray click on the button without
       the JS confirm dialog firing still hits a second guard).
    3. Audit-logs the action *before* signalling — once SIGTERM
       lands, the asyncio loop unwinds and the audit-write would
       race the DB pool teardown.
    4. Sets the flash banner so the next page render (after the
       supervisor restarts the bot) tells the operator the request
       was received.
    5. Calls ``bot_health.request_force_stop`` *after* returning
       the response, so the browser actually sees a 302 instead of
       a connection-reset.

    The ``request.app`` may store a test-injected kill function at
    ``APP_KEY_FORCE_STOP_FN`` — production never sets it, so the
    primitive defaults to ``os.kill`` against the current PID.
    """
    secret = request.app.get(APP_KEY_SESSION_SECRET, "")
    cookie_secure = request.app.get(APP_KEY_COOKIE_SECURE, True)
    form = await request.post()

    guard = _control_csrf_guard(request, form)
    if guard is not None:
        return guard

    confirm = str(form.get("confirm", "")).strip()
    if confirm != "FORCE-STOP":
        log.warning(
            "control: force-stop POST missing confirm sentinel from %s",
            request.remote,
        )
        response = web.HTTPFound(location="/admin/control")
        set_flash(
            response, kind="error",
            message=(
                "Force-stop request missing confirmation. The button "
                "must be submitted from the panel form."
            ),
            secret=secret, cookie_secure=cookie_secure,
        )
        return response

    log.warning(
        "control: force-stop confirmed from %s — signalling pid=%d",
        request.remote, os.getpid(),
    )
    await _record_audit_safe(
        request, "control_force_stop",
        outcome="ok",
        meta={"pid": os.getpid()},
    )

    response = web.HTTPFound(location="/admin/control")
    set_flash(
        response, kind="success",
        message=(
            "Force-stop signal sent. The bot process will exit and "
            "restart via your supervisor."
        ),
        secret=secret, cookie_secure=cookie_secure,
    )

    # Schedule the kill *after* this handler returns the response so
    # the browser actually receives the 302. ``call_later(0, …)``
    # runs on the next event-loop tick; aiohttp is mid-tick right
    # now finishing the response. Tests inject a no-op kill_fn via
    # APP_KEY_FORCE_STOP_FN so the test process isn't actually
    # signalled.
    from bot_health import request_force_stop

    kill_fn = request.app.get(APP_KEY_FORCE_STOP_FN)
    loop = asyncio.get_event_loop()
    loop.call_later(
        0.05,
        lambda: request_force_stop(kill_fn=kill_fn),
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
    bot=None,
    totp_secret: str = "",
    totp_issuer: str = "Meowassist Admin",
) -> None:
    """Mount the admin panel onto *app*.

    Called from ``main.start_webhook_server``. Idempotent — refusing
    a second call with a clear log line beats silently overwriting
    state on a hot reload.

    Stage-9-Step-3 bundled bug fix: refuse to start the panel when
    either ``password`` or ``session_secret`` is *non-empty but
    whitespace-only* (a common ``ADMIN_PASSWORD=" "`` deploy typo).
    The previous behaviour stored the whitespace string verbatim and
    silently rejected every login attempt — operators spent hours
    debugging "wrong password" before realising they had a stray
    space in their .env. We still allow truly empty values (so the
    documented "panel unreachable in dev when env vars unset" path
    keeps working); only whitespace-only values fail-fast at startup.

    ``totp_secret`` enables optional TOTP / 2FA enforcement on
    ``/admin/login``. Empty string keeps the password-only flow
    untouched. Non-empty values are validated as base32 at boot via
    ``validate_totp_secret`` — invalid input raises ``ValueError``
    with a clear message rather than failing on first login.
    """
    if app.get(APP_KEY_INSTALLED):
        log.warning("setup_admin_routes called twice — ignoring second call.")
        return

    # Bundled bug fix (Stage-9-Step-3): whitespace-only credentials are
    # always a deploy typo — surface immediately instead of "panel
    # unreachable, login refuses everything" half a day later.
    if password and not password.strip():
        raise ValueError(
            "ADMIN_PASSWORD contains only whitespace — refusing to start "
            "with a half-configured admin panel. Either set a real "
            "password or leave the variable empty to keep the panel "
            "disabled."
        )
    if session_secret and not session_secret.strip():
        raise ValueError(
            "ADMIN_SESSION_SECRET contains only whitespace — refusing to "
            "start with a half-configured admin panel. Either set a real "
            "secret (≥32 random chars) or leave the variable empty."
        )

    # Validate the TOTP secret at boot so a base32 typo is rejected
    # immediately. Empty input → 2FA disabled (back-compat).
    try:
        totp_secret = validate_totp_secret(totp_secret)
    except ValueError:
        # Re-raise with context so the deploy log makes the
        # misconfig obvious without needing to grep into the helper.
        raise

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
    # Stage-9-Step-3: validated TOTP secret (or "" when 2FA disabled).
    app[APP_KEY_TOTP_SECRET] = totp_secret
    app[APP_KEY_TOTP_ISSUER] = totp_issuer or "Meowassist Admin"
    if totp_secret:
        log.info(
            "Admin 2FA is ENABLED (issuer=%s). Login requires a 6-digit "
            "TOTP code in addition to ADMIN_PASSWORD.",
            app[APP_KEY_TOTP_ISSUER],
        )
    else:
        log.info(
            "Admin 2FA is disabled (ADMIN_2FA_SECRET unset). Login is "
            "password-only. Visit /admin/enroll_2fa to provision a "
            "secret."
        )

    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        # autoescape is ON by default for .html files via select_autoescape;
        # being explicit here protects us if a future template ever loses
        # the .html extension.
        autoescape=jinja2.select_autoescape(["html"]),
        # Stage-9-Step-7: single canonical USD formatter — see
        # ``formatting.format_usd`` for why the ad-hoc per-template
        # ``"${:,.4f}".format(...)`` calls were replaced.
        filters={"format_usd": format_usd},
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

    # Stage-15-Step-E #9: bot monetization rollup.
    app.router.add_get(
        "/admin/monetization",
        _require_auth(monetization),
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
    # Stage-12-Step-D: per-code redemption drilldown.
    app.router.add_get(
        "/admin/gifts/{code}/redemptions",
        _require_auth(gift_redemptions_get),
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
    # Stage-9-Step-8: per-user AI usage log browser.
    app.router.add_get(
        "/admin/users/{telegram_id}/usage",
        _require_auth(user_usage_get),
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
    # Stage-9-Step-6: soft-cancel for a running/queued broadcast.
    app.router.add_post(
        "/admin/broadcast/{job_id}/cancel",
        _require_auth(broadcast_cancel_post),
    )

    # Stage-8-Part-6: transactions browser (read-only, paginated).
    app.router.add_get(
        "/admin/transactions",
        _require_auth(transactions_get),
    )
    # Stage-12-Step-A: refund a SUCCESS transaction. Issued from the
    # inline form on the transactions browser; CSRF-protected and
    # audit-logged. The handler always redirects back to the list
    # view with a flash banner.
    app.router.add_post(
        "/admin/transactions/{transaction_id}/refund",
        _require_auth(transaction_refund_post),
    )

    # Stage-9-Step-1.6: editable bot strings.
    app.router.add_get("/admin/strings", _require_auth(strings_get))
    app.router.add_get(
        "/admin/strings/{lang}/{key}",
        _require_auth(string_detail_get),
    )
    app.router.add_post(
        "/admin/strings/{lang}/{key}",
        _require_auth(string_save_post),
    )
    app.router.add_post(
        "/admin/strings/{lang}/{key}/revert",
        _require_auth(string_revert_post),
    )

    # Stage-9-Step-2: user-field editor + audit-log viewer.
    app.router.add_post(
        "/admin/users/{telegram_id}/edit",
        _require_auth(user_edit_post),
    )
    app.router.add_get("/admin/audit", _require_auth(audit_get))

    # Stage-9-Step-3: TOTP / 2FA enrolment helper. Always behind the
    # admin login. Operators who haven't configured ADMIN_2FA_SECRET
    # yet get a freshly-suggested random secret to copy into env;
    # operators who have already configured one get the QR for the
    # current value (re-pairing a new device).
    app.router.add_get(
        "/admin/enroll_2fa",
        _require_auth(enroll_2fa_get),
    )

    # Stage-14: model & gateway toggle pages.
    app.router.add_get("/admin/models", _require_auth(models_get))
    app.router.add_post("/admin/models/disable", _require_auth(models_disable_post))
    app.router.add_post("/admin/models/enable", _require_auth(models_enable_post))
    app.router.add_get("/admin/gateways", _require_auth(gateways_get))
    app.router.add_post("/admin/gateways/disable", _require_auth(gateways_disable_post))
    app.router.add_post("/admin/gateways/enable", _require_auth(gateways_enable_post))

    # Stage-15-Step-F: bot health & emergency control panel.
    app.router.add_get("/admin/control", _require_auth(control_get))
    app.router.add_post(
        "/admin/control/disable-all-models",
        _require_auth(control_disable_all_models_post),
    )
    app.router.add_post(
        "/admin/control/enable-all-models",
        _require_auth(control_enable_all_models_post),
    )
    app.router.add_post(
        "/admin/control/disable-all-gateways",
        _require_auth(control_disable_all_gateways_post),
    )
    app.router.add_post(
        "/admin/control/enable-all-gateways",
        _require_auth(control_enable_all_gateways_post),
    )
    app.router.add_post(
        "/admin/control/force-stop",
        _require_auth(control_force_stop_post),
    )

    # Stage-9-Step-10: durable broadcast registry orphan sweep.
    # Any row left in ``queued`` / ``running`` from before the
    # restart is flipped to ``interrupted`` so the recent-jobs page
    # doesn't forever show a phantom "running" job whose worker
    # task no longer exists. Best-effort — a DB blip at startup
    # logs a warning but doesn't block the app from coming up.
    async def _sweep_orphan_broadcast_jobs(_app: web.Application) -> None:
        db_ref = _app.get(APP_KEY_DB)
        if db_ref is None:
            return
        try:
            n = await db_ref.mark_orphan_broadcast_jobs_interrupted()
        except Exception:
            log.warning(
                "broadcast_jobs orphan sweep failed at startup "
                "(broadcast_jobs table may be missing the migration "
                "0007_broadcast_jobs).",
                exc_info=True,
            )
            return
        if n:
            log.info(
                "broadcast_jobs orphan sweep: marked %d row(s) "
                "as interrupted (queued/running before restart).",
                n,
            )
    app.on_startup.append(_sweep_orphan_broadcast_jobs)

    app[APP_KEY_INSTALLED] = True
    log.info("Web admin routes installed under /admin/")
