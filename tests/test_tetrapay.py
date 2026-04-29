"""Stage-11-Step-C tests: TetraPay (Rial card) gateway integration.

Covers:

* ``usd_to_irr_amount`` — pure rounding helper.
* ``create_order`` — POST shape, success path, non-100 status,
  malformed responses, missing API key / WEBHOOK_BASE_URL, missing
  fields.
* ``verify_payment`` — POST shape, success, non-100 status, transport
  errors.
* ``tetrapay_webhook`` — happy path (with replay-dedupe + verify +
  finalize), bad JSON, missing authority, non-success callback,
  unknown invoice (no PENDING row), verify failure, replay drop,
  Telegram-notify best-effort.
* ``create_pending_transaction`` defensive guards (the bundled bug
  fix for Stage-11-Step-C): NaN / Inf / non-positive amount_usd /
  amount_crypto / promo_bonus_usd / locked rate.
"""

from __future__ import annotations

import json
import math
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from aiohttp import web

import tetrapay


# ---------------------------------------------------------------------
# usd_to_irr_amount — pure unit tests
# ---------------------------------------------------------------------


def test_usd_to_irr_amount_rounds_to_integer_rial():
    # $1 at 100_000 toman/USD => 100_000 toman => 1_000_000 IRR exactly
    assert tetrapay.usd_to_irr_amount(1.0, 100_000.0) == 1_000_000


def test_usd_to_irr_amount_rounds_at_toman_boundary():
    # $1.50 at 100_000 toman/USD => 150_000 toman exactly => 1_500_000 IRR
    assert tetrapay.usd_to_irr_amount(1.5, 100_000.0) == 1_500_000


def test_usd_to_irr_amount_rounds_fractional_toman_correctly():
    # $1.234 at 100_000 = 123_400 toman; round to 123_400; * 10 = 1_234_000
    assert tetrapay.usd_to_irr_amount(1.234, 100_000.0) == 1_234_000


def test_usd_to_irr_amount_refuses_nan_amount_usd():
    with pytest.raises(ValueError, match="amount_usd"):
        tetrapay.usd_to_irr_amount(math.nan, 100_000.0)


def test_usd_to_irr_amount_refuses_negative_amount_usd():
    with pytest.raises(ValueError, match="amount_usd"):
        tetrapay.usd_to_irr_amount(-1.0, 100_000.0)


def test_usd_to_irr_amount_refuses_inf_rate():
    with pytest.raises(ValueError, match="rate_toman_per_usd"):
        tetrapay.usd_to_irr_amount(5.0, math.inf)


def test_usd_to_irr_amount_refuses_zero_rate():
    with pytest.raises(ValueError, match="rate_toman_per_usd"):
        tetrapay.usd_to_irr_amount(5.0, 0.0)


# ---------------------------------------------------------------------
# create_order — happy path + failure modes
# ---------------------------------------------------------------------


@pytest.fixture
def _tetrapay_env(monkeypatch):
    """Pin a known TetraPay API key + webhook base for every test that
    touches HTTP. Tests that need to assert the missing-key path
    explicitly delete these via monkeypatch."""
    monkeypatch.setenv("TETRAPAY_API_KEY", "test-key-32-bytes-padding-padding")
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://bot.example.com")
    monkeypatch.setenv("TETRAPAY_API_BASE", "https://tetra98.test")
    # Reset drop counters between tests
    for k in tetrapay._TETRAPAY_DROP_COUNTERS:
        tetrapay._TETRAPAY_DROP_COUNTERS[k] = 0


class _FakeResponse:
    """Mimic aiohttp's response context manager for tests."""

    def __init__(self, payload: dict | str, *, status: int = 200, json_ok: bool = True):
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

    def __init__(self, response_payload, *, status: int = 200, json_ok: bool = True,
                 raise_exc: Exception | None = None):
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
        return _FakeResponse(self._response, status=self._status, json_ok=self._json_ok)


@pytest.fixture
def _patch_session(monkeypatch):
    """Return a factory: caller provides a payload (and optional status /
    error), gets back the FakeSession instance plus a spy that
    intercepts ``aiohttp.ClientSession()``."""

    sessions: list[_FakeSession] = []

    def _factory(*, payload=None, status=200, json_ok=True, raise_exc=None):
        session = _FakeSession(
            payload, status=status, json_ok=json_ok, raise_exc=raise_exc
        )
        sessions.append(session)

        def _client_session():
            return session

        monkeypatch.setattr(tetrapay.aiohttp, "ClientSession", _client_session)
        return session

    return _factory


async def test_create_order_happy_path(_tetrapay_env, _patch_session):
    session = _patch_session(payload={
        "status": "100",
        "payment_url_web": "https://tetra98.test/pay/abc",
        "Authority": "auth-xyz-123",
        "tracking_id": "trk-789",
    })
    order = await tetrapay.create_order(
        amount_usd=5.0,
        rate_toman_per_usd=100_000.0,
        description="Wallet top-up",
        user_id=42,
        hash_id="fixed-hash-id",
    )

    assert order.authority == "auth-xyz-123"
    assert order.payment_url == "https://tetra98.test/pay/abc"
    assert order.tracking_id == "trk-789"
    assert order.amount_irr == 5_000_000  # 5 USD * 100k toman = 500k toman = 5M IRR
    assert order.amount_usd == 5.0
    assert order.locked_rate_toman_per_usd == 100_000.0
    assert order.hash_id == "fixed-hash-id"

    # Verify the POST shape: api_base + /api/create_order, JSON body with
    # ApiKey / Hash_id / Amount / CallbackURL.
    assert session.last_url == "https://tetra98.test/api/create_order"
    assert session.last_json["ApiKey"] == "test-key-32-bytes-padding-padding"
    assert session.last_json["Hash_id"] == "fixed-hash-id"
    assert session.last_json["Amount"] == 5_000_000
    assert "user=42" in session.last_json["Description"]
    assert session.last_json["CallbackURL"] == (
        "https://bot.example.com/tetrapay-webhook"
    )


async def test_create_order_refuses_when_api_key_missing(monkeypatch):
    monkeypatch.delenv("TETRAPAY_API_KEY", raising=False)
    monkeypatch.setenv("WEBHOOK_BASE_URL", "https://bot.example.com")
    with pytest.raises(tetrapay.TetraPayError, match="TETRAPAY_API_KEY"):
        await tetrapay.create_order(
            amount_usd=5.0, rate_toman_per_usd=100_000.0,
            description="x", user_id=1,
        )


async def test_create_order_refuses_when_webhook_base_missing(monkeypatch):
    monkeypatch.setenv("TETRAPAY_API_KEY", "test-key")
    monkeypatch.delenv("WEBHOOK_BASE_URL", raising=False)
    with pytest.raises(tetrapay.TetraPayError, match="WEBHOOK_BASE_URL"):
        await tetrapay.create_order(
            amount_usd=5.0, rate_toman_per_usd=100_000.0,
            description="x", user_id=1,
        )


async def test_create_order_raises_on_non_success_status(_tetrapay_env, _patch_session):
    _patch_session(payload={"status": "404", "error": "Insufficient funds"})
    with pytest.raises(tetrapay.TetraPayError) as excinfo:
        await tetrapay.create_order(
            amount_usd=5.0, rate_toman_per_usd=100_000.0,
            description="x", user_id=1,
        )
    assert excinfo.value.status == "404"


async def test_create_order_raises_on_non_json_response(_tetrapay_env, _patch_session):
    _patch_session(payload="<html>upstream error</html>", json_ok=False)
    with pytest.raises(tetrapay.TetraPayError, match="non-JSON"):
        await tetrapay.create_order(
            amount_usd=5.0, rate_toman_per_usd=100_000.0,
            description="x", user_id=1,
        )


async def test_create_order_raises_when_authority_missing(_tetrapay_env, _patch_session):
    _patch_session(payload={
        "status": "100",
        "payment_url_web": "https://tetra98.test/pay/abc",
        # Authority missing
    })
    with pytest.raises(tetrapay.TetraPayError, match="missing"):
        await tetrapay.create_order(
            amount_usd=5.0, rate_toman_per_usd=100_000.0,
            description="x", user_id=1,
        )


async def test_create_order_generates_random_hash_id_by_default(
    _tetrapay_env, _patch_session,
):
    session = _patch_session(payload={
        "status": "100",
        "payment_url_web": "https://tetra98.test/pay/abc",
        "Authority": "auth-1",
        "tracking_id": "t-1",
    })
    order = await tetrapay.create_order(
        amount_usd=5.0, rate_toman_per_usd=100_000.0,
        description="x", user_id=1,
    )
    # Hash_id is 24 hex chars from secrets.token_hex(12)
    assert len(order.hash_id) == 24
    assert all(c in "0123456789abcdef" for c in order.hash_id)
    assert session.last_json["Hash_id"] == order.hash_id


# ---------------------------------------------------------------------
# verify_payment — happy + failure modes
# ---------------------------------------------------------------------


async def test_verify_payment_happy_path(_tetrapay_env, _patch_session):
    session = _patch_session(payload={"status": "100", "RefID": "ref-987"})
    result = await tetrapay.verify_payment("auth-xyz")
    assert result["status"] == "100"
    assert result["RefID"] == "ref-987"
    assert session.last_url == "https://tetra98.test/api/verify"
    assert session.last_json["Authority"] == "auth-xyz"


async def test_verify_payment_raises_on_non_success(_tetrapay_env, _patch_session):
    _patch_session(payload={"status": "404", "error": "Order not settled"})
    with pytest.raises(tetrapay.TetraPayError) as excinfo:
        await tetrapay.verify_payment("auth-xyz")
    assert excinfo.value.status == "404"


async def test_verify_payment_refuses_empty_authority(_tetrapay_env):
    with pytest.raises(tetrapay.TetraPayError, match="non-empty authority"):
        await tetrapay.verify_payment("")


async def test_verify_payment_refuses_when_api_key_missing(monkeypatch):
    monkeypatch.delenv("TETRAPAY_API_KEY", raising=False)
    with pytest.raises(tetrapay.TetraPayError, match="TETRAPAY_API_KEY"):
        await tetrapay.verify_payment("auth-xyz")


# ---------------------------------------------------------------------
# tetrapay_webhook — full handler tests
# ---------------------------------------------------------------------


def _make_request(body: bytes) -> web.Request:
    """Build a minimal aiohttp ``Request``-lookalike."""
    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    request = MagicMock(spec=[])
    request.read = AsyncMock(return_value=body)
    request.remote = "1.2.3.4"
    request.app = {"bot": bot}
    return request


@pytest.fixture
def patched_db(monkeypatch):
    """Replace tetrapay.db with a MagicMock whose coroutines return
    happy-path values."""
    db = MagicMock()
    db.record_payment_status_transition = AsyncMock(return_value=42)
    db.get_pending_invoice_amount_usd = AsyncMock(return_value=5.0)
    db.finalize_payment = AsyncMock(
        return_value={
            "telegram_id": 7,
            "delta_credited": 5.0,
            "amount_usd_credited": 5.0,
            "promo_bonus_credited": 0.0,
        }
    )
    db.get_user_language = AsyncMock(return_value="fa")
    monkeypatch.setattr(tetrapay, "db", db)
    return db


@pytest.fixture
def patched_verify(monkeypatch):
    """Stub out ``verify_payment`` so the webhook tests don't need
    full HTTP plumbing."""
    mock = AsyncMock(return_value={"status": "100", "RefID": "ref-1"})
    monkeypatch.setattr(tetrapay, "verify_payment", mock)
    return mock


async def test_webhook_happy_path_credits_at_locked_amount(
    _tetrapay_env, patched_db, patched_verify,
):
    body = json.dumps({
        "status": "100",
        "authority": "auth-1",
        "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_db.get_pending_invoice_amount_usd.assert_awaited_once_with("auth-1")
    patched_verify.assert_awaited_once_with("auth-1")
    patched_db.finalize_payment.assert_awaited_once_with("auth-1", 5.0)
    request.app["bot"].send_message.assert_awaited_once()


async def test_webhook_drops_bad_json(_tetrapay_env, patched_db, patched_verify):
    request = _make_request(b"not-json{{{")
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_db.finalize_payment.assert_not_awaited()
    patched_verify.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["bad_json"] == 1


async def test_webhook_drops_non_object_json(_tetrapay_env, patched_db, patched_verify):
    request = _make_request(b'["a","b"]')
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_db.finalize_payment.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["bad_json"] == 1


async def test_webhook_drops_missing_authority(_tetrapay_env, patched_db, patched_verify):
    body = json.dumps({"status": "100", "hash_id": "hid-1"}).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_db.finalize_payment.assert_not_awaited()
    patched_verify.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["missing_authority"] == 1


async def test_webhook_drops_replay(_tetrapay_env, patched_db, patched_verify):
    """``record_payment_status_transition`` returning ``None`` means the
    same (authority, status) pair was already observed — handler must
    bail before verify or finalize."""
    patched_db.record_payment_status_transition.return_value = None
    body = json.dumps({
        "status": "100", "authority": "auth-1", "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_verify.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["replay"] == 1


async def test_webhook_non_success_callback_does_not_credit(
    _tetrapay_env, patched_db, patched_verify,
):
    body = json.dumps({
        "status": "200",  # non-success per spec; only "100" is settled
        "authority": "auth-1",
        "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_verify.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["non_success_callback"] == 1


async def test_webhook_unknown_invoice_does_not_credit(
    _tetrapay_env, patched_db, patched_verify,
):
    """No PENDING / PARTIAL row for this authority — handler must NOT
    credit. Same defense as the NowPayments path."""
    patched_db.get_pending_invoice_amount_usd.return_value = None
    body = json.dumps({
        "status": "100", "authority": "auth-unknown", "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_verify.assert_not_awaited()
    patched_db.finalize_payment.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["unknown_invoice"] == 1


async def test_webhook_verify_failure_does_not_credit(
    _tetrapay_env, patched_db, patched_verify,
):
    """A user could craft a forged callback. Only TetraPay's verify
    endpoint authoritatively confirms settlement; if it rejects, we
    refuse to credit."""
    patched_verify.side_effect = tetrapay.TetraPayError(
        "404", "verify rejected", body={},
    )
    body = json.dumps({
        "status": "100", "authority": "auth-1", "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_db.finalize_payment.assert_not_awaited()
    assert tetrapay.get_tetrapay_drop_counters()["verify_failed"] == 1


async def test_webhook_handles_uppercase_authority_field(
    _tetrapay_env, patched_db, patched_verify,
):
    """TetraPay's docs are inconsistent about field casing. Accept both
    ``authority`` and ``Authority`` to avoid an outage if they tweak
    their response shape."""
    body = json.dumps({
        "status": "100",
        "Authority": "auth-cased",
        "Hash_id": "hid-cased",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200
    patched_db.get_pending_invoice_amount_usd.assert_awaited_once_with("auth-cased")
    patched_db.finalize_payment.assert_awaited_once_with("auth-cased", 5.0)


async def test_webhook_finalize_returning_none_logs_and_returns_200(
    _tetrapay_env, patched_db, patched_verify,
):
    """finalize_payment returns None for an already-finalized row (a
    second concurrent webhook delivery wins the race). The handler
    must NOT 5xx — that would trigger a TetraPay retry storm."""
    patched_db.finalize_payment.return_value = None
    body = json.dumps({
        "status": "100", "authority": "auth-1", "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(body)
    response = await tetrapay.tetrapay_webhook(request)

    assert response.status == 200


async def test_webhook_telegram_failure_does_not_500(
    _tetrapay_env, patched_db, patched_verify,
):
    """User blocked the bot. Wallet was already credited; the
    notification failure must NOT propagate as a 5xx."""
    request_body = json.dumps({
        "status": "100", "authority": "auth-1", "hash_id": "hid-1",
    }).encode("utf-8")
    request = _make_request(request_body)
    request.app["bot"].send_message.side_effect = RuntimeError(
        "telegram refused: user blocked"
    )
    response = await tetrapay.tetrapay_webhook(request)

    # Still 200, even though the notification raised.
    assert response.status == 200
    patched_db.finalize_payment.assert_awaited_once()


# ---------------------------------------------------------------------
# Bundled bug fix: create_pending_transaction defensive guards
# ---------------------------------------------------------------------


class _FakeConn:
    def __init__(self, return_value):
        self._return_value = return_value
        self.executed: list[tuple] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def fetchval(self, query, *args):
        self.executed.append((query, args))
        return self._return_value


class _FakePool:
    def __init__(self, return_value=42):
        self._return_value = return_value
        self.acquired_count = 0
        self.last_conn: _FakeConn | None = None

    def acquire(self):
        self.acquired_count += 1
        self.last_conn = _FakeConn(self._return_value)
        return self.last_conn


@pytest.fixture
def fake_db():
    """Build a Database-like object whose pool returns a stub
    connection that records the executed SQL but always returns 42
    (a fake transaction_id) on success.

    We import the real Database class so the bound method we're
    testing has access to ``self.pool`` etc."""
    from database import Database

    instance = Database.__new__(Database)
    instance.pool = _FakePool(return_value=42)
    return instance


async def test_create_pending_transaction_refuses_nan_amount_usd(fake_db):
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=math.nan,
        gateway_invoice_id="auth-x",
    )
    assert inserted is False
    assert fake_db.pool.acquired_count == 0  # never reached the DB


async def test_create_pending_transaction_refuses_inf_amount_usd(fake_db):
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=math.inf,
        gateway_invoice_id="auth-x",
    )
    assert inserted is False
    assert fake_db.pool.acquired_count == 0


async def test_create_pending_transaction_refuses_zero_amount_usd(fake_db):
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=0.0,
        gateway_invoice_id="auth-x",
    )
    assert inserted is False
    assert fake_db.pool.acquired_count == 0


async def test_create_pending_transaction_refuses_nan_amount_crypto(fake_db):
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=math.nan, amount_usd=5.0,
        gateway_invoice_id="auth-x",
    )
    assert inserted is False


async def test_create_pending_transaction_refuses_negative_promo_bonus(fake_db):
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=5.0,
        gateway_invoice_id="auth-x",
        promo_bonus_usd=-1.0,
    )
    assert inserted is False


async def test_create_pending_transaction_refuses_nan_locked_rate(fake_db):
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=5.0,
        gateway_invoice_id="auth-x",
        gateway_locked_rate_toman_per_usd=math.nan,
    )
    assert inserted is False


async def test_create_pending_transaction_accepts_zero_promo_bonus(fake_db):
    """Zero is a legitimate value (no promo) — the guard must only
    reject *negative* / non-finite, not exactly zero."""
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=5.0,
        gateway_invoice_id="auth-x",
        promo_bonus_usd=0.0,
    )
    assert inserted is True
    assert fake_db.pool.acquired_count == 1


async def test_create_pending_transaction_accepts_null_locked_rate(fake_db):
    """Crypto rows pass ``None`` for the rate — that's a legitimate
    "no rate lock" signal, not a defensive failure."""
    inserted = await fake_db.create_pending_transaction(
        telegram_id=7, gateway="NowPayments", currency_used="usdttrc20",
        amount_crypto=5.123456, amount_usd=5.0,
        gateway_invoice_id="np-1",
        gateway_locked_rate_toman_per_usd=None,
    )
    assert inserted is True


async def test_create_pending_transaction_persists_locked_rate_when_provided(fake_db):
    """Sanity check that the new column flows through to the SQL
    parameter list as the 9th positional value."""
    await fake_db.create_pending_transaction(
        telegram_id=7, gateway="tetrapay", currency_used="IRR",
        amount_crypto=1_000_000.0, amount_usd=5.0,
        gateway_invoice_id="auth-rate-test",
        gateway_locked_rate_toman_per_usd=100_000.0,
    )
    # The fake conn captures the args; the rate must be the 9th param
    # (1-indexed: $9 in the SQL).
    query, args = fake_db.pool.last_conn.executed[0]
    assert "gateway_locked_rate_toman_per_usd" in query
    assert args[8] == 100_000.0
