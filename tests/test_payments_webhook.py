"""Stage-9-Step-4 tests: IPN webhook replay-dedupe + bug-fix bundle.

We pin the new ``payment_status_transitions`` dedupe contract and the
upgraded drop-counter / log-level behaviour for malformed IPNs. The
test stubs out asyncpg by patching the module-level ``db`` singleton
in ``payments``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import web

import payments


SECRET = "test-ipn-secret-32-bytes-padding"


def _sign(body: bytes) -> str:
    return hmac.new(SECRET.encode("utf-8"), body, hashlib.sha512).hexdigest()


def _make_body(**overrides) -> bytes:
    base = {
        "payment_id": 9999,
        "payment_status": "finished",
        "pay_address": "TQrZ9wBzPvF7nPq3xY5kE2rLgM8aWvJh1d",
        "price_amount": 5,
        "price_currency": "usd",
        "pay_amount": 5.123456,
        "actually_paid": 5.123456,
        "pay_currency": "usdttrc20",
        "order_id": "98765432",
        "order_description": "test",
        "purchase_id": "5500000000",
        "outcome_amount": 4.95,
        "outcome_currency": "usd",
    }
    base.update(overrides)
    return json.dumps(base, ensure_ascii=False).encode("utf-8")


def _make_request(body: bytes, sig: str | None) -> web.Request:
    """Build a minimal aiohttp ``Request``-lookalike. We don't need a
    real aiohttp Application/loop for these tests because
    ``payment_webhook`` only touches ``request.read()``,
    ``request.headers``, ``request.remote`` and ``request.app["bot"]``.
    """
    bot = MagicMock()
    bot.send_message = AsyncMock(return_value=None)

    request = MagicMock(spec=[])
    request.read = AsyncMock(return_value=body)
    request.headers = {} if sig is None else {"x-nowpayments-sig": sig}
    request.remote = "127.0.0.1"
    request.app = {"bot": bot}
    return request


@pytest.fixture(autouse=True)
def _ipn_secret(monkeypatch):
    """Pin a known IPN secret + bot language lookup for every test in
    this module."""
    monkeypatch.setattr(payments, "NOWPAYMENTS_IPN_SECRET", SECRET)
    payments._IPN_DROP_COUNTERS["bad_signature"] = 0
    payments._IPN_DROP_COUNTERS["bad_json"] = 0
    payments._IPN_DROP_COUNTERS["missing_payment_id"] = 0
    payments._IPN_DROP_COUNTERS["replay"] = 0


@pytest.fixture
def patched_db(monkeypatch):
    """Replace the module-level ``db`` singleton with a MagicMock whose
    coroutines return values the handler expects on the happy path."""
    db = MagicMock()
    db.record_payment_status_transition = AsyncMock(return_value=42)
    db.finalize_payment = AsyncMock(
        return_value={
            "telegram_id": 7,
            "delta_credited": 5.0,
            "amount_usd_credited": 5.0,
            "promo_bonus_credited": 0.0,
        }
    )
    db.finalize_partial_payment = AsyncMock(
        return_value={
            "telegram_id": 7,
            "amount_usd_credited": 2.5,
            "delta_credited": 2.5,
            "currency_used": "usdttrc20",
        }
    )
    db.mark_transaction_terminal = AsyncMock(
        return_value={
            "telegram_id": 7,
            "previous_status": "PENDING",
            "amount_usd_credited": 0.0,
            "currency_used": "usdttrc20",
        }
    )
    db.get_user_language = AsyncMock(return_value="en")
    monkeypatch.setattr(payments, "db", db)
    return db


# ---------------------------------------------------------------------
# Replay dedupe via payment_status_transitions
# ---------------------------------------------------------------------


async def test_webhook_records_transition_and_proceeds_on_finished(patched_db):
    body = _make_body(payment_id=11111, payment_status="finished")
    request = _make_request(body, _sign(body))

    response = await payments.payment_webhook(request)

    assert response.status == 200
    patched_db.record_payment_status_transition.assert_awaited_once()
    args, kwargs = patched_db.record_payment_status_transition.call_args
    assert args[0] == "11111"
    assert args[1] == "finished"
    assert kwargs["outcome"] == "applied"
    patched_db.finalize_payment.assert_awaited_once_with("11111", 5.0)


async def test_webhook_drops_duplicate_invoice_status_pair(patched_db):
    """``ON CONFLICT DO NOTHING`` returns None on a replay; the handler
    must bail before calling finalize_payment / finalize_partial /
    mark_terminal — that's the whole point of the dedupe table."""
    patched_db.record_payment_status_transition.return_value = None
    body = _make_body(payment_id=22222, payment_status="finished")
    request = _make_request(body, _sign(body))

    response = await payments.payment_webhook(request)

    assert response.status == 200
    patched_db.finalize_payment.assert_not_awaited()
    patched_db.finalize_partial_payment.assert_not_awaited()
    patched_db.mark_transaction_terminal.assert_not_awaited()
    assert payments.get_ipn_drop_counters()["replay"] == 1


async def test_webhook_dedupe_does_not_block_different_status_for_same_invoice(
    patched_db,
):
    """An invoice that goes PENDING → PARTIAL → SUCCESS legitimately
    fires three different ``payment_status`` IPNs; the dedupe is keyed
    on the *pair* so each one inserts a fresh row."""
    body_partial = _make_body(payment_id=33333, payment_status="partially_paid")
    request = _make_request(body_partial, _sign(body_partial))
    await payments.payment_webhook(request)

    body_finished = _make_body(payment_id=33333, payment_status="finished")
    request2 = _make_request(body_finished, _sign(body_finished))
    await payments.payment_webhook(request2)

    # Two distinct calls to record_payment_status_transition with the
    # same gateway_invoice_id but different payment_status values.
    calls = patched_db.record_payment_status_transition.call_args_list
    assert len(calls) == 2
    assert {c.args[1] for c in calls} == {"partially_paid", "finished"}


async def test_webhook_falls_open_on_dedupe_db_error(patched_db):
    """Transient asyncpg pool blip mid-record_payment_status_transition
    must not 500 back to NowPayments — the row-level guards still
    dedupe correctly downstream."""
    patched_db.record_payment_status_transition.side_effect = RuntimeError(
        "pool exhausted"
    )
    body = _make_body(payment_id=44444, payment_status="expired")
    request = _make_request(body, _sign(body))

    response = await payments.payment_webhook(request)

    assert response.status == 200
    patched_db.mark_transaction_terminal.assert_awaited_once_with(
        "44444", "EXPIRED"
    )


# ---------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------


def test_classify_actionable_statuses():
    for status in ("finished", "partially_paid", "expired", "failed", "refunded"):
        assert payments._classify_ipn_outcome(status) == "applied"


def test_classify_in_flight_statuses():
    for status in ("waiting", "confirming", "confirmed", "sending"):
        assert payments._classify_ipn_outcome(status) == "noop"


def test_classify_unknown_status_marked_unhandled():
    assert payments._classify_ipn_outcome("not-a-real-status") == "unhandled"
    assert payments._classify_ipn_outcome(None) == "unhandled"


async def test_webhook_unhandled_status_records_transition_no_state_change(
    patched_db,
):
    body = _make_body(payment_id=55555, payment_status="not-a-real-status")
    request = _make_request(body, _sign(body))

    response = await payments.payment_webhook(request)

    assert response.status == 200
    args, kwargs = patched_db.record_payment_status_transition.call_args
    assert kwargs["outcome"] == "unhandled"
    patched_db.finalize_payment.assert_not_awaited()
    patched_db.finalize_partial_payment.assert_not_awaited()
    patched_db.mark_transaction_terminal.assert_not_awaited()


# ---------------------------------------------------------------------
# Bug-fix bundle: drop counters + error-level logs for malformed IPNs
# ---------------------------------------------------------------------


async def test_webhook_bad_signature_bumps_counter_and_401(patched_db):
    body = _make_body(payment_id=66666)
    request = _make_request(body, "deadbeef")  # wrong signature

    response = await payments.payment_webhook(request)

    assert response.status == 401
    assert payments.get_ipn_drop_counters()["bad_signature"] == 1
    patched_db.record_payment_status_transition.assert_not_awaited()


async def test_webhook_bad_json_bumps_counter_and_200(patched_db):
    """A malformed body that happens to pass signature verification
    (because the verifier signs raw bytes, not parsed JSON) must drop
    cleanly with a counter bump — pre-fix this fell through to the
    outer ``except Exception`` and returned 500, which triggered
    NowPayments retries."""
    body = b"not-json-at-all"
    request = _make_request(body, _sign(body))

    response = await payments.payment_webhook(request)

    assert response.status == 200
    assert payments.get_ipn_drop_counters()["bad_json"] == 1
    patched_db.record_payment_status_transition.assert_not_awaited()


async def test_webhook_missing_payment_id_bumps_counter(patched_db, caplog):
    body = json.dumps(
        {"payment_status": "finished", "price_amount": 5}
    ).encode("utf-8")
    request = _make_request(body, _sign(body))

    with caplog.at_level("ERROR", logger="payments"):
        response = await payments.payment_webhook(request)

    assert response.status == 200
    assert payments.get_ipn_drop_counters()["missing_payment_id"] == 1
    # Pre-fix this was logged at WARNING; bug-fix bundle bumps to ERROR
    # so deploy alerts pick it up.
    assert any(
        rec.levelname == "ERROR"
        and "missing payment_id" in rec.getMessage()
        for rec in caplog.records
    )
    patched_db.record_payment_status_transition.assert_not_awaited()


def test_get_ipn_drop_counters_returns_snapshot_copy():
    """Mutating the returned dict must not mutate the module-private
    state — defense against an admin-page handler accidentally
    incrementing live counters."""
    snapshot = payments.get_ipn_drop_counters()
    snapshot["bad_signature"] = 9999
    fresh = payments.get_ipn_drop_counters()
    assert fresh["bad_signature"] != 9999


# ---------------------------------------------------------------------
# Database.record_payment_status_transition
# ---------------------------------------------------------------------


async def test_record_payment_status_transition_inserts_with_meta():
    """Pin the SQL shape: INSERT … ON CONFLICT DO NOTHING RETURNING id,
    JSONB cast on meta, six bind parameters in the documented order."""
    import database as database_module
    from tests.test_database_queries import _PoolStub, _make_conn

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=42)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    row_id = await db.record_payment_status_transition(
        "abc-123",
        "finished",
        outcome="applied",
        meta={"remote": "1.2.3.4"},
    )

    assert row_id == 42
    sql, *args = conn.fetchval.await_args.args
    assert "INSERT INTO payment_status_transitions" in sql
    assert "ON CONFLICT" in sql
    assert "(gateway_invoice_id, payment_status)" in sql
    assert "DO NOTHING" in sql
    assert args[0] == "abc-123"
    assert args[1] == "finished"
    assert args[2] == "applied"
    assert json.loads(args[3]) == {"remote": "1.2.3.4"}


async def test_record_payment_status_transition_returns_none_on_conflict():
    import database as database_module
    from tests.test_database_queries import _PoolStub, _make_conn

    conn = _make_conn()
    conn.fetchval = AsyncMock(return_value=None)
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    row_id = await db.record_payment_status_transition(
        "dup-id", "finished", outcome="applied"
    )
    assert row_id is None


async def test_list_payment_status_transitions_filters_by_invoice():
    import database as database_module
    from tests.test_database_queries import _PoolStub, _make_conn

    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_payment_status_transitions(
        gateway_invoice_id="abc-123", limit=50
    )
    sql, *args = conn.fetch.await_args.args
    assert "WHERE gateway_invoice_id = $1" in sql
    assert "ORDER BY recorded_at DESC" in sql
    assert args[0] == "abc-123"
    assert args[1] == 50


async def test_list_payment_status_transitions_no_filter_omits_where():
    import database as database_module
    from tests.test_database_queries import _PoolStub, _make_conn

    conn = _make_conn()
    conn.fetch = AsyncMock(return_value=[])
    db = database_module.Database()
    db.pool = _PoolStub(conn)

    await db.list_payment_status_transitions(limit=10)
    sql, *args = conn.fetch.await_args.args
    assert "WHERE" not in sql
    assert args[0] == 10
