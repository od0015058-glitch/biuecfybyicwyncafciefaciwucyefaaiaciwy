"""Stage-15-Step-E #8 tests: Zarinpal (Iranian card / Shaparak) gateway.

Covers:

* ``usd_to_irr_amount`` — pure rounding helper (mirrors TetraPay).
* ``create_order`` — POST shape, success path, non-100 code,
  malformed responses, missing merchant_id / WEBHOOK_BASE_URL,
  missing authority field, metadata propagation.
* ``verify_payment`` — POST shape, ``code=100`` (settled now),
  ``code=101`` (already verified — also success), failure codes,
  missing inputs.
* ``zarinpal_callback`` — happy path (locked-USD credit + verify +
  finalize), missing Authority, ``Status=NOK``, unknown invoice,
  poisoned legacy IRR row, verify rejection, verify transport
  errors, finalize-returning-None refresh-loop, Telegram failure
  best-effort, promo bonus rendering, query-param casing tolerance.
* Bundled bug fix: ``model_discovery._parse_positive_int_env`` and
  ``fx_rates._parse_int_env`` env-parser missing-floor / import-time
  crash regression tests.
"""

from __future__ import annotations

import asyncio
import math
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from aiohttp import web

import zarinpal


# ---------------------------------------------------------------------
# usd_to_irr_amount — pure unit tests (mirrors TetraPay's helper)
# ---------------------------------------------------------------------


def test_usd_to_irr_amount_rounds_to_integer_rial():
    # $1 at 100_000 toman/USD => 100_000 toman => 1_000_000 IRR
    assert zarinpal.usd_to_irr_amount(1.0, 100_000.0) == 1_000_000


def test_usd_to_irr_amount_rounds_at_toman_boundary():
    assert zarinpal.usd_to_irr_amount(1.5, 100_000.0) == 1_500_000


def test_usd_to_irr_amount_rounds_fractional_toman_correctly():
    # $1.234 at 100_000 = 123_400 toman; round; * 10 = 1_234_000 IRR
    assert zarinpal.usd_to_irr_amount(1.234, 100_000.0) == 1_234_000


def test_usd_to_irr_amount_refuses_nan_amount_usd():
    with pytest.raises(ValueError, match="amount_usd"):
        zarinpal.usd_to_irr_amount(math.nan, 100_000.0)


def test_usd_to_irr_amount_refuses_negative_amount_usd():
    with pytest.raises(ValueError, match="amount_usd"):
        zarinpal.usd_to_irr_amount(-1.0, 100_000.0)


def test_usd_to_irr_amount_refuses_inf_rate():
    with pytest.raises(ValueError, match="rate_toman_per_usd"):
        zarinpal.usd_to_irr_amount(5.0, math.inf)


def test_usd_to_irr_amount_refuses_zero_rate():
    with pytest.raises(ValueError, match="rate_toman_per_usd"):
        zarinpal.usd_to_irr_amount(5.0, 0.0)


# ---------------------------------------------------------------------
# config helpers
# ---------------------------------------------------------------------


def test_api_base_default(monkeypatch):
    monkeypatch.delenv("ZARINPAL_API_BASE", raising=False)
    assert zarinpal._api_base() == "https://payment.zarinpal.com/pg"


def test_api_base_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("ZARINPAL_API_BASE", "https://sandbox.zarinpal.com/pg/")
    assert zarinpal._api_base() == "https://sandbox.zarinpal.com/pg"


def test_callback_url_empty_without_webhook_base(monkeypatch):
    monkeypatch.delenv("WEBHOOK_BASE_URL", raising=False)
    assert zarinpal._callback_url() == ""


def test_callback_url_appends_zarinpal_callback(monkeypatch):
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://bot.example.com/")
    assert zarinpal._callback_url() == "https://bot.example.com/zarinpal-callback"


def test_timeout_seconds_default(monkeypatch):
    monkeypatch.delenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", raising=False)
    assert zarinpal._timeout_seconds() == 10.0


def test_timeout_seconds_override(monkeypatch):
    monkeypatch.setenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "5.5")
    assert zarinpal._timeout_seconds() == 5.5


def test_timeout_seconds_rejects_zero(monkeypatch):
    """A zero timeout in aiohttp means 'no timeout' — a hung Zarinpal
    endpoint would stall invoice creation indefinitely. The parser
    falls back to the default with a warning."""
    monkeypatch.setenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "0")
    assert zarinpal._timeout_seconds() == 10.0


def test_timeout_seconds_rejects_negative(monkeypatch):
    monkeypatch.setenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "-1")
    assert zarinpal._timeout_seconds() == 10.0


def test_timeout_seconds_rejects_nan(monkeypatch):
    monkeypatch.setenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "nan")
    assert zarinpal._timeout_seconds() == 10.0


def test_timeout_seconds_rejects_inf(monkeypatch):
    monkeypatch.setenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "inf")
    assert zarinpal._timeout_seconds() == 10.0


def test_timeout_seconds_rejects_garbage(monkeypatch):
    monkeypatch.setenv("ZARINPAL_REQUEST_TIMEOUT_SECONDS", "abc")
    assert zarinpal._timeout_seconds() == 10.0


# ---------------------------------------------------------------------
# create_order — happy path + failure modes
# ---------------------------------------------------------------------


@pytest.fixture
def _zarinpal_env(monkeypatch):
    """Pin a known Zarinpal merchant id + webhook base for every test
    that touches HTTP. Tests that need the missing-merchant path
    explicitly delete these via monkeypatch."""
    monkeypatch.setenv(
        "ZARINPAL_MERCHANT_ID", "00000000-0000-0000-0000-000000000000"
    )
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://bot.example.com")
    monkeypatch.setenv(
        "ZARINPAL_API_BASE", "https://payment.zarinpal.test/pg"
    )
    # Reset drop counters between tests
    for k in zarinpal._ZARINPAL_DROP_COUNTERS:
        zarinpal._ZARINPAL_DROP_COUNTERS[k] = 0


class _FakeResponse:
    """Mimic aiohttp's response context manager for tests."""

    def __init__(
        self, payload, *, status: int = 200, json_ok: bool = True
    ):
        self._payload = payload
        self.status = status
        self._json_ok = json_ok

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self, content_type=None):
        if not self._json_ok:
            raise aiohttp.ContentTypeError(MagicMock(), MagicMock())
        return self._payload

    async def text(self):
        return str(self._payload)


class _FakeSession:
    """Mimic aiohttp.ClientSession for tests. Captures the last POST."""

    def __init__(
        self,
        response_payload,
        *,
        status: int = 200,
        json_ok: bool = True,
        raise_exc: Exception | None = None,
    ):
        self._response = response_payload
        self._status = status
        self._json_ok = json_ok
        self._raise_exc = raise_exc
        self.last_url: str | None = None
        self.last_json: dict | None = None
        self.last_timeout = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, *, json=None, timeout=None):
        self.last_url = url
        self.last_json = json
        self.last_timeout = timeout
        if self._raise_exc is not None:
            raise self._raise_exc
        return _FakeResponse(
            self._response, status=self._status, json_ok=self._json_ok
        )


@pytest.fixture
def _patch_session(monkeypatch):
    """Return a factory: caller provides a payload (and optional status
    / error), gets back the FakeSession instance plus a spy that
    intercepts ``aiohttp.ClientSession()``."""

    sessions: list[_FakeSession] = []

    def _factory(*, payload=None, status=200, json_ok=True, raise_exc=None):
        session = _FakeSession(
            payload,
            status=status,
            json_ok=json_ok,
            raise_exc=raise_exc,
        )
        sessions.append(session)

        def _client_session():
            return session

        monkeypatch.setattr(zarinpal.aiohttp, "ClientSession", _client_session)
        return session

    return _factory


async def test_create_order_happy_path(_zarinpal_env, _patch_session):
    session = _patch_session(payload={
        "data": {
            "code": 100,
            "message": "Success",
            "authority": "A0000000000000000000000000000000abcd",
            "fee_type": "Merchant",
            "fee": 0,
        },
        "errors": [],
    })
    order = await zarinpal.create_order(
        amount_usd=5.0,
        rate_toman_per_usd=100_000.0,
        description="Wallet top-up",
        user_id=42,
    )

    assert order.authority == "A0000000000000000000000000000000abcd"
    assert order.amount_irr == 5_000_000  # 5 USD * 100k = 500k toman = 5M IRR
    assert order.amount_usd == 5.0
    assert order.locked_rate_toman_per_usd == 100_000.0
    assert order.fee_type == "Merchant"
    assert order.fee == 0
    # StartPay URL uses the same API base host
    assert order.payment_url == (
        "https://payment.zarinpal.test/pg/StartPay/"
        "A0000000000000000000000000000000abcd"
    )

    # Verify the POST shape: api_base + /v4/payment/request.json,
    # JSON body with merchant_id / amount / currency / callback_url.
    assert session.last_url == (
        "https://payment.zarinpal.test/pg/v4/payment/request.json"
    )
    assert session.last_json["merchant_id"] == (
        "00000000-0000-0000-0000-000000000000"
    )
    assert session.last_json["amount"] == 5_000_000
    assert session.last_json["currency"] == "IRR"
    assert "user=42" in session.last_json["description"]
    assert session.last_json["callback_url"] == (
        "https://bot.example.com/zarinpal-callback"
    )
    # No metadata when email / mobile not supplied
    assert "metadata" not in session.last_json


async def test_create_order_propagates_email_and_mobile_metadata(
    _zarinpal_env, _patch_session,
):
    session = _patch_session(payload={
        "data": {
            "code": 100,
            "message": "Success",
            "authority": "A1",
            "fee_type": "Merchant",
            "fee": 0,
        },
        "errors": [],
    })
    await zarinpal.create_order(
        amount_usd=5.0,
        rate_toman_per_usd=100_000.0,
        description="x",
        user_id=1,
        email="a@b.example",
        mobile="+98123",
    )
    assert session.last_json["metadata"] == {
        "email": "a@b.example",
        "mobile": "+98123",
    }


async def test_create_order_refuses_when_merchant_id_missing(monkeypatch):
    monkeypatch.delenv("ZARINPAL_MERCHANT_ID", raising=False)
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://bot.example.com")
    with pytest.raises(zarinpal.ZarinpalError, match="ZARINPAL_MERCHANT_ID"):
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )


async def test_create_order_refuses_when_webhook_base_missing(monkeypatch):
    monkeypatch.setenv("ZARINPAL_MERCHANT_ID", "merchant-x")
    monkeypatch.delenv("WEBHOOK_BASE_URL", raising=False)
    with pytest.raises(zarinpal.ZarinpalError, match="WEBHOOK_BASE_URL"):
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )


async def test_create_order_raises_on_non_100_code(_zarinpal_env, _patch_session):
    _patch_session(payload={
        "data": [],
        "errors": {"code": -10, "message": "Invalid merchant"},
    })
    with pytest.raises(zarinpal.ZarinpalError) as excinfo:
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )
    # Error path: ``data`` was an empty list, not a dict — code parses
    # to ``None`` and the failure message reflects that.
    assert excinfo.value.code is None


async def test_create_order_raises_on_explicit_failure_code(
    _zarinpal_env, _patch_session,
):
    _patch_session(payload={
        "data": {"code": -11, "message": "merchant blocked"},
        "errors": {"code": -11, "message": "merchant blocked"},
    })
    with pytest.raises(zarinpal.ZarinpalError) as excinfo:
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )
    assert excinfo.value.code == -11


async def test_create_order_raises_on_non_json_response(
    _zarinpal_env, _patch_session,
):
    _patch_session(payload="<html>upstream error</html>", json_ok=False)
    with pytest.raises(zarinpal.ZarinpalError, match="non-JSON"):
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )


async def test_create_order_raises_when_authority_missing(
    _zarinpal_env, _patch_session,
):
    _patch_session(payload={
        "data": {"code": 100, "message": "Success"},
        "errors": [],
    })
    with pytest.raises(zarinpal.ZarinpalError, match="missing authority"):
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )


async def test_create_order_raises_on_non_object_body(_zarinpal_env, _patch_session):
    _patch_session(payload=["not", "a", "dict"])
    with pytest.raises(zarinpal.ZarinpalError, match="non-object body"):
        await zarinpal.create_order(
            amount_usd=5.0,
            rate_toman_per_usd=100_000.0,
            description="x",
            user_id=1,
        )


# ---------------------------------------------------------------------
# verify_payment — happy + failure modes
# ---------------------------------------------------------------------


async def test_verify_payment_happy_path_code_100(_zarinpal_env, _patch_session):
    session = _patch_session(payload={
        "data": {
            "code": 100,
            "message": "Verified",
            "ref_id": 12345678,
            "card_pan": "5022-29**-****-1234",
            "fee_type": "Merchant",
            "fee": 0,
        },
        "errors": [],
    })
    result = await zarinpal.verify_payment("auth-xyz", 5_000_000)
    assert result["data"]["code"] == 100
    assert session.last_url == (
        "https://payment.zarinpal.test/pg/v4/payment/verify.json"
    )
    assert session.last_json["authority"] == "auth-xyz"
    assert session.last_json["amount"] == 5_000_000


async def test_verify_payment_happy_path_code_101_already_verified(
    _zarinpal_env, _patch_session,
):
    """Zarinpal returns code=101 when verify is called twice for the
    same authority. From our perspective this is still SUCCESS — the
    order is settled. ``finalize_payment`` is itself idempotent so a
    duplicate credit doesn't happen at the DB layer."""
    _patch_session(payload={
        "data": {"code": 101, "message": "Verified previously"},
        "errors": [],
    })
    result = await zarinpal.verify_payment("auth-xyz", 5_000_000)
    assert result["data"]["code"] == 101


async def test_verify_payment_raises_on_failure_code(_zarinpal_env, _patch_session):
    _patch_session(payload={
        "data": {"code": -50, "message": "Settlement reversed"},
        "errors": [],
    })
    with pytest.raises(zarinpal.ZarinpalError) as excinfo:
        await zarinpal.verify_payment("auth-xyz", 5_000_000)
    assert excinfo.value.code == -50


async def test_verify_payment_refuses_empty_authority(_zarinpal_env):
    with pytest.raises(zarinpal.ZarinpalError, match="non-empty authority"):
        await zarinpal.verify_payment("", 5_000_000)


async def test_verify_payment_refuses_when_merchant_missing(monkeypatch):
    monkeypatch.delenv("ZARINPAL_MERCHANT_ID", raising=False)
    with pytest.raises(zarinpal.ZarinpalError, match="ZARINPAL_MERCHANT_ID"):
        await zarinpal.verify_payment("auth-xyz", 5_000_000)


async def test_verify_payment_refuses_non_positive_amount(_zarinpal_env):
    with pytest.raises(zarinpal.ZarinpalError, match="positive integer amount_irr"):
        await zarinpal.verify_payment("auth-xyz", 0)


async def test_verify_payment_refuses_non_integer_amount(_zarinpal_env):
    with pytest.raises(zarinpal.ZarinpalError, match="positive integer amount_irr"):
        await zarinpal.verify_payment("auth-xyz", 5.5)  # type: ignore[arg-type]


async def test_verify_payment_raises_on_non_json(_zarinpal_env, _patch_session):
    _patch_session(payload="<html>503</html>", json_ok=False)
    with pytest.raises(zarinpal.ZarinpalError, match="non-JSON"):
        await zarinpal.verify_payment("auth-xyz", 5_000_000)


# ---------------------------------------------------------------------
# zarinpal_callback — full handler tests
# ---------------------------------------------------------------------


def _make_request(query: dict[str, str]) -> web.Request:
    """Build a minimal aiohttp ``Request``-lookalike with query params."""
    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    request = MagicMock(spec=[])
    request.query = dict(query)
    request.remote = "1.2.3.4"
    request.app = {"bot": bot}
    return request


@pytest.fixture
def patched_db(monkeypatch):
    """Replace zarinpal.db with a MagicMock whose coroutines return
    happy-path values."""
    db = MagicMock()
    db.record_payment_status_transition = AsyncMock(return_value=42)
    db.get_pending_invoice_amount_usd = AsyncMock(return_value=5.0)
    db.get_pending_invoice_amount_irr = AsyncMock(return_value=5_000_000)
    db.finalize_payment = AsyncMock(
        return_value={
            "telegram_id": 7,
            "delta_credited": 5.0,
            "amount_usd_credited": 5.0,
            "promo_bonus_credited": 0.0,
        }
    )
    db.get_user_language = AsyncMock(return_value="fa")
    monkeypatch.setattr(zarinpal, "db", db)
    return db


@pytest.fixture
def patched_verify(monkeypatch):
    """Stub out ``verify_payment`` so the callback tests don't need
    full HTTP plumbing."""
    mock = AsyncMock(return_value={
        "data": {"code": 100, "message": "Verified", "ref_id": 1},
        "errors": [],
    })
    monkeypatch.setattr(zarinpal, "verify_payment", mock)
    return mock


async def test_callback_happy_path_credits_at_locked_amount(
    _zarinpal_env, patched_db, patched_verify,
):
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    assert response.content_type == "text/html"
    assert "تأیید" in response.text
    patched_db.get_pending_invoice_amount_usd.assert_awaited_once_with("auth-1")
    patched_db.get_pending_invoice_amount_irr.assert_awaited_once_with("auth-1")
    # verify is called with the LOCKED IRR figure read from our ledger,
    # NOT anything the user could supply via the URL — that's the
    # tampered-redirect defense.
    patched_verify.assert_awaited_once_with("auth-1", 5_000_000)
    patched_db.finalize_payment.assert_awaited_once_with("auth-1", 5.0)
    request.app["bot"].send_message.assert_awaited_once()


async def test_callback_drops_missing_authority(
    _zarinpal_env, patched_db, patched_verify,
):
    request = _make_request({"Status": "OK"})  # no Authority
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    assert "ناموفق" in response.text  # failure HTML
    patched_db.finalize_payment.assert_not_awaited()
    patched_verify.assert_not_awaited()
    assert zarinpal.get_zarinpal_drop_counters()["missing_authority"] == 1


async def test_callback_status_nok_records_noop_and_does_not_credit(
    _zarinpal_env, patched_db, patched_verify,
):
    """User cancelled or card declined — Zarinpal redirects with
    Status=NOK. We record the transition for audit, leave the PENDING
    row alone for the reaper, and don't call verify or finalize."""
    request = _make_request({"Authority": "auth-1", "Status": "NOK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    assert "ناموفق" in response.text
    patched_verify.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    patched_db.record_payment_status_transition.assert_awaited_once()
    record_call = patched_db.record_payment_status_transition.call_args_list[0]
    assert record_call.kwargs.get("outcome") == "noop"
    assert zarinpal.get_zarinpal_drop_counters()["non_success_callback"] == 1


async def test_callback_unknown_invoice_drops_with_failure_html(
    _zarinpal_env, patched_db, patched_verify,
):
    """No PENDING/PARTIAL row for this authority — could be a forged
    callback OR an already-finalized refresh-loop. Either way, refuse
    to credit. Same defense as the TetraPay path."""
    patched_db.get_pending_invoice_amount_usd.return_value = None
    request = _make_request({"Authority": "auth-unknown", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    patched_verify.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    assert zarinpal.get_zarinpal_drop_counters()["unknown_invoice"] == 1


async def test_callback_missing_irr_figure_drops_without_verifying(
    _zarinpal_env, patched_db, patched_verify,
):
    """Defensive drop for a poisoned legacy row that has a USD figure
    but no rial figure (or a non-finite rial figure). We refuse to
    verify because we'd send the wrong amount and Zarinpal would
    server-side reject the verify anyway."""
    patched_db.get_pending_invoice_amount_irr.return_value = None
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    patched_verify.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    assert zarinpal.get_zarinpal_drop_counters()["unknown_invoice"] == 1


async def test_callback_verify_rejection_records_and_drops(
    _zarinpal_env, patched_db, patched_verify,
):
    """Zarinpal explicitly rejected the verify. Record the transition
    with outcome=rejected so a refresh-loop on the same Authority
    no-ops via the dedupe table — and don't credit."""
    patched_verify.side_effect = zarinpal.ZarinpalError(
        -50, "Settlement reversed", body={},
    )
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    patched_db.finalize_payment.assert_not_awaited()
    patched_db.record_payment_status_transition.assert_awaited_once()
    record_call = patched_db.record_payment_status_transition.call_args_list[0]
    assert record_call.kwargs.get("outcome") == "rejected"
    assert zarinpal.get_zarinpal_drop_counters()["verify_failed"] == 1


async def test_callback_verify_timeout_does_not_record(
    _zarinpal_env, patched_db, patched_verify,
):
    """Transient failure — but Zarinpal's callback is a user-redirect,
    not a retried server-to-server webhook. We can't record a
    transition (which would block a future reaper-driven retry) and
    the user just sees the failure HTML. Document the gap for a
    future backfill reaper."""
    patched_verify.side_effect = asyncio.TimeoutError()
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200  # user-facing 200 with failure HTML
    patched_db.record_payment_status_transition.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    assert zarinpal.get_zarinpal_drop_counters()["verify_failed"] == 1


async def test_callback_verify_client_error_does_not_record(
    _zarinpal_env, patched_db, patched_verify,
):
    patched_verify.side_effect = aiohttp.ClientError("connection reset")
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    patched_db.record_payment_status_transition.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()


async def test_callback_finalize_returning_none_shows_success_for_replay(
    _zarinpal_env, patched_db, patched_verify,
):
    """``finalize_payment`` returns ``None`` on a refresh-loop (the
    row was already finalized by a previous callback delivery). The
    user already saw the credit DM on the first delivery; show
    success again so the second tab doesn't look broken."""
    patched_db.finalize_payment.return_value = None
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    assert "تأیید" in response.text  # success HTML, not failure
    assert zarinpal.get_zarinpal_drop_counters()["replay"] == 1


async def test_callback_telegram_failure_does_not_break_response(
    _zarinpal_env, patched_db, patched_verify,
):
    """Wallet was already credited before the Telegram DM. A
    notification failure must NOT propagate as an error response —
    we'd just confuse the user with a broken-looking page after a
    successful payment."""
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    request.app["bot"].send_message.side_effect = RuntimeError(
        "telegram refused: user blocked"
    )
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    assert "تأیید" in response.text  # still the success page
    patched_db.finalize_payment.assert_awaited_once()


async def test_callback_credit_notification_includes_promo_bonus(
    _zarinpal_env, patched_db, patched_verify,
):
    """When ``promo_bonus_credited > 0`` the user-facing DM appends
    the bonus line in addition to the base credit notification."""
    patched_db.finalize_payment.return_value = {
        "telegram_id": 7,
        "delta_credited": 5.0,
        "amount_usd_credited": 5.0,
        "promo_bonus_credited": 1.5,
    }
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    await zarinpal.zarinpal_callback(request)

    request.app["bot"].send_message.assert_awaited_once()
    sent_text = request.app["bot"].send_message.call_args.args[1]
    # Both the base credit line and the promo-bonus line should be
    # present.
    assert "5.00" in sent_text or "5.0" in sent_text  # base credit
    assert "1.5" in sent_text  # promo bonus


async def test_callback_accepts_lowercase_query_params(
    _zarinpal_env, patched_db, patched_verify,
):
    """Zarinpal's docs use ``Authority`` / ``Status`` (capital first
    letter) but a misconfigured custom domain or a future schema
    tweak could deliver lowercase. Accept both casings."""
    request = _make_request({"authority": "auth-1", "status": "ok"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200
    patched_verify.assert_awaited_once_with("auth-1", 5_000_000)
    patched_db.finalize_payment.assert_awaited_once_with("auth-1", 5.0)


async def test_callback_returns_html_content_type_on_success(
    _zarinpal_env, patched_db, patched_verify,
):
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)
    assert response.content_type == "text/html"


async def test_callback_returns_html_content_type_on_failure(
    _zarinpal_env, patched_db, patched_verify,
):
    request = _make_request({"Authority": "auth-1", "Status": "NOK"})
    response = await zarinpal.zarinpal_callback(request)
    assert response.content_type == "text/html"


async def test_callback_uses_locked_irr_not_url_amount(
    _zarinpal_env, patched_db, patched_verify,
):
    """Defense-in-depth pin: verify is called with the IRR figure
    we read from OUR ledger, not anything the user could supply via
    the URL. A malicious user could craft a callback URL with a
    ``?Amount=1`` query param and a guessed Authority — the helper
    would still pull the locked figure from the PENDING row."""
    patched_db.get_pending_invoice_amount_irr.return_value = 7_777_777
    request = _make_request({
        "Authority": "auth-1",
        "Status": "OK",
        "Amount": "1",  # attacker-supplied; must be ignored
    })
    await zarinpal.zarinpal_callback(request)

    patched_verify.assert_awaited_once_with("auth-1", 7_777_777)


async def test_callback_outer_guard_returns_failure_html_on_crash(
    _zarinpal_env, patched_db, patched_verify, monkeypatch,
):
    """An unexpected crash in the handler must NOT propagate. Zarinpal
    won't retry the user-redirect, so a 500 just looks broken to the
    user. The outer guard catches and returns the failure HTML so the
    user knows something went wrong."""

    async def _explode(*_a, **_kw):
        raise RuntimeError("synthetic boom")

    monkeypatch.setattr(zarinpal.db, "get_pending_invoice_amount_usd", _explode)
    request = _make_request({"Authority": "auth-1", "Status": "OK"})
    response = await zarinpal.zarinpal_callback(request)

    assert response.status == 200  # never 500
    assert "ناموفق" in response.text


# ---------------------------------------------------------------------
# Bundled bug fix: model_discovery._parse_positive_int_env
# ---------------------------------------------------------------------


def test_parse_positive_int_env_blank_returns_default(monkeypatch):
    import model_discovery

    monkeypatch.delenv("DISCOVERY_INTERVAL_SECONDS", raising=False)
    assert model_discovery._parse_positive_int_env(
        "DISCOVERY_INTERVAL_SECONDS", 21_600
    ) == 21_600


def test_parse_positive_int_env_garbage_returns_default(monkeypatch):
    """The pre-fix code did ``int(os.getenv(...))`` at module-import
    time with no try/except, so a non-numeric env value crashed the
    bot's entire import chain. Now we fall back to the default."""
    import model_discovery

    monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "abc")
    assert model_discovery._parse_positive_int_env(
        "DISCOVERY_INTERVAL_SECONDS", 21_600
    ) == 21_600


def test_parse_positive_int_env_zero_clamps_to_minimum(monkeypatch):
    """Pre-fix: ``DISCOVERY_INTERVAL_SECONDS=0`` (a typo for ``60``)
    would busy-loop the discovery refresher hammering OpenRouter every
    iteration as fast as the network allowed. The floor closes that
    gap by clamping any value below ``minimum`` (default 1) up."""
    import model_discovery

    monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "0")
    assert model_discovery._parse_positive_int_env(
        "DISCOVERY_INTERVAL_SECONDS", 21_600
    ) == 1


def test_parse_positive_int_env_negative_clamps_to_minimum(monkeypatch):
    """Pre-fix: a negative interval would silently degrade
    ``asyncio.sleep`` to a no-op yield (every refresh as fast as the
    loop allows). Floor handles it."""
    import model_discovery

    monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "-1")
    assert model_discovery._parse_positive_int_env(
        "DISCOVERY_INTERVAL_SECONDS", 21_600
    ) == 1


def test_parse_positive_int_env_valid_value_passes_through(monkeypatch):
    import model_discovery

    monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "300")
    assert model_discovery._parse_positive_int_env(
        "DISCOVERY_INTERVAL_SECONDS", 21_600
    ) == 300


def test_parse_positive_int_env_minimum_override_allows_zero(monkeypatch):
    """A caller that legitimately wants ``0`` (e.g. a test-mode
    cadence) opts out of the floor with ``minimum=0``."""
    import model_discovery

    monkeypatch.setenv("DISCOVERY_INTERVAL_SECONDS", "0")
    assert model_discovery._parse_positive_int_env(
        "DISCOVERY_INTERVAL_SECONDS", 21_600, minimum=0
    ) == 0


# ---------------------------------------------------------------------
# Bundled bug fix: fx_rates._parse_int_env
# ---------------------------------------------------------------------


def test_fx_rates_parse_int_env_zero_clamps_to_minimum(monkeypatch):
    """Pre-fix: ``FX_REFRESH_INTERVAL_SECONDS=0`` (a typo for ``600``)
    would busy-loop the FX refresher hammering Nobitex every iteration.
    The floor closes that gap."""
    import fx_rates

    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "0")
    assert fx_rates._parse_int_env(
        "FX_REFRESH_INTERVAL_SECONDS", 600
    ) == 1


def test_fx_rates_parse_int_env_negative_clamps_to_minimum(monkeypatch):
    import fx_rates

    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "-60")
    assert fx_rates._parse_int_env(
        "FX_REFRESH_INTERVAL_SECONDS", 600
    ) == 1


def test_fx_rates_parse_int_env_valid_value_passes_through(monkeypatch):
    import fx_rates

    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "300")
    assert fx_rates._parse_int_env(
        "FX_REFRESH_INTERVAL_SECONDS", 600
    ) == 300


def test_fx_rates_parse_int_env_garbage_returns_default(monkeypatch):
    import fx_rates

    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "abc")
    assert fx_rates._parse_int_env(
        "FX_REFRESH_INTERVAL_SECONDS", 600
    ) == 600


def test_fx_rates_parse_int_env_minimum_override_allows_zero(monkeypatch):
    """Backward compat: a caller that explicitly wants the legacy
    no-floor semantics (e.g. a test that needs zero) can pass
    ``minimum=0``."""
    import fx_rates

    monkeypatch.setenv("FX_REFRESH_INTERVAL_SECONDS", "0")
    assert fx_rates._parse_int_env(
        "FX_REFRESH_INTERVAL_SECONDS", 600, minimum=0
    ) == 0
