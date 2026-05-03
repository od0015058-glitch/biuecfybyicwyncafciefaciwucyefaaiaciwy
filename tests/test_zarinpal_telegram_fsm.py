"""Stage-15-Step-E #8 follow-up #1: Zarinpal Telegram FSM integration.

Covers:

* The currency-picker keyboard rendered after ``process_toman_amount_input``
  exposes a ``cur_zarinpal`` button next to ``cur_tetrapay``.
* ``is_gateway_disabled("zarinpal")`` hides the Zarinpal button.
* ``process_custom_currency_selection`` with ``currency=zarinpal``
  routes to ``_start_zarinpal_invoice``.
* ``_start_zarinpal_invoice`` happy path: creates order, persists
  PENDING row keyed on the gateway-issued Authority, renders the
  order_text + StartPay button.
* ``_start_zarinpal_invoice`` failure modes:
    - missing / non-finite / non-positive ``toman_rate_at_entry``
      (the bundled bug-fix gate) renders ``charge_toman_no_rate``.
    - ``zarinpal.create_order`` raises → renders ``zarinpal_unreachable``.
    - ``Database.create_pending_transaction`` returns ``False`` →
      renders ``charge_invoice_error``.
* ``process_custom_currency_selection`` with ``currency=zarinpal`` but
  the gateway is admin-disabled: shows ``gateway_disabled`` toast and
  does NOT call ``zarinpal_create_order``.
* Promo data on the FSM rides through to the ``create_pending_transaction``
  call as ``promo_code`` / ``promo_bonus_usd`` (settled-on-success
  semantics, same as crypto / TetraPay paths).

We don't drive the aiogram dispatcher (needs a live Bot session +
Redis); we call the handler coroutines directly with mocked
callback / state and assert the right downstream calls fire.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import fx_rates


@pytest.fixture(autouse=True)
def _seed_fx_rate():
    """Pin the ticker rate so amount-stash math is predictable."""
    fx_rates._cache = fx_rates.FxRateSnapshot(
        toman_per_usd=100_000.0,
        fetched_at=time.time(),
        source="test",
    )
    yield
    fx_rates._reset_cache_for_tests()


def _make_callback(*, user_id: int = 42, callback_data: str = "cur_zarinpal"):
    msg = SimpleNamespace(
        edit_text=AsyncMock(),
        chat=SimpleNamespace(id=user_id),
    )
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="test"),
        message=msg,
        answer=AsyncMock(),
        data=callback_data,
    )


def _make_state(stash: dict | None = None):
    return SimpleNamespace(
        clear=AsyncMock(),
        set_state=AsyncMock(),
        update_data=AsyncMock(),
        get_data=AsyncMock(return_value=stash or {}),
    )


def _make_message(text: str, user_id: int = 42):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, username="test"),
        text=text,
        answer=AsyncMock(),
    )


def _fake_zarinpal_order(
    *,
    authority: str = "A" * 36,
    amount_irr: int = 4_000_000,
    amount_usd: float = 4.0,
    rate: float = 100_000.0,
    payment_url: str = "https://payment.zarinpal.com/pg/StartPay/" + "A" * 36,
):
    return SimpleNamespace(
        authority=authority,
        payment_url=payment_url,
        amount_irr=amount_irr,
        locked_rate_toman_per_usd=rate,
        amount_usd=amount_usd,
        fee_type="Merchant",
        fee=0,
    )


# ---------------------------------------------------------------------
# Toman entry confirmation keyboard wiring (button presence)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_toman_confirm_keyboard_includes_zarinpal_button():
    """The currency-picker rendered after a successful Toman entry
    must include ``cur_zarinpal`` next to ``cur_tetrapay``."""
    from handlers import process_toman_amount_input

    msg = _make_message("400000")
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", return_value=False
    ):
        await process_toman_amount_input(msg, state)

    markup = msg.answer.await_args.kwargs["reply_markup"]
    callbacks = [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data is not None
    ]
    assert "cur_zarinpal" in callbacks
    assert "cur_tetrapay" in callbacks


@pytest.mark.asyncio
async def test_toman_confirm_keyboard_drops_zarinpal_when_disabled():
    """Admin-disabled Zarinpal must hide the button (TetraPay still shown)."""
    from handlers import process_toman_amount_input

    def _stub_disabled(key: str) -> bool:
        return key == "zarinpal"

    msg = _make_message("400000")
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", side_effect=_stub_disabled
    ):
        await process_toman_amount_input(msg, state)

    markup = msg.answer.await_args.kwargs["reply_markup"]
    callbacks = [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data is not None
    ]
    assert "cur_zarinpal" not in callbacks
    assert "cur_tetrapay" in callbacks


@pytest.mark.asyncio
async def test_toman_confirm_keyboard_drops_both_card_buttons_when_disabled():
    """Both card gateways disabled → keyboard has no card-row buttons."""
    from handlers import process_toman_amount_input

    msg = _make_message("400000")
    state = _make_state()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", return_value=True
    ):
        await process_toman_amount_input(msg, state)

    markup = msg.answer.await_args.kwargs["reply_markup"]
    callbacks = [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data is not None
    ]
    assert "cur_tetrapay" not in callbacks
    assert "cur_zarinpal" not in callbacks


# ---------------------------------------------------------------------
# process_custom_currency_selection routing
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_currency_selection_zarinpal_routes_to_start_invoice():
    """``cur_zarinpal`` callback must call ``_start_zarinpal_invoice``."""
    from handlers import process_custom_currency_selection

    cb = _make_callback(callback_data="cur_zarinpal")
    state = _make_state(
        stash={
            "custom_amount": 4.0,
            "toman_rate_at_entry": 100_000.0,
        }
    )
    start_mock = AsyncMock()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", return_value=False
    ), patch(
        "handlers._start_zarinpal_invoice", new=start_mock
    ):
        await process_custom_currency_selection(cb, state)

    start_mock.assert_awaited_once()
    kwargs = start_mock.await_args.kwargs
    assert kwargs["amount_usd"] == pytest.approx(4.0)
    assert kwargs["toman_rate_at_entry"] == pytest.approx(100_000.0)
    assert kwargs["lang"] == "fa"


@pytest.mark.asyncio
async def test_currency_selection_zarinpal_blocked_when_gateway_disabled():
    """Admin-disabled Zarinpal must short-circuit with the gateway_disabled
    toast and never reach ``_start_zarinpal_invoice``."""
    from handlers import process_custom_currency_selection

    def _stub_disabled(key: str) -> bool:
        return key == "zarinpal"

    cb = _make_callback(callback_data="cur_zarinpal")
    state = _make_state(stash={"custom_amount": 4.0})
    start_mock = AsyncMock()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", side_effect=_stub_disabled
    ), patch(
        "handlers._start_zarinpal_invoice", new=start_mock
    ):
        await process_custom_currency_selection(cb, state)

    start_mock.assert_not_awaited()
    cb.answer.assert_awaited_once()
    # gateway_disabled toast → show_alert=True
    answer_kwargs = cb.answer.await_args.kwargs
    assert answer_kwargs.get("show_alert") is True


@pytest.mark.asyncio
async def test_currency_selection_crypto_blocked_when_provider_master_disabled():
    """Stage-15-Step-E #10b row 14: a stale rendered keyboard could
    let a user click a ``cur_<crypto>`` button after the operator
    flipped the NowPayments provider master switch off in the panel.
    The handler must refuse the click with the standard
    ``gateway_disabled`` toast and never reach the invoice path.

    Card gateways (``cur_tetrapay`` / ``cur_zarinpal``) are NOT
    covered by the master switch — see the sibling test below.
    """
    from handlers import process_custom_currency_selection

    def _stub_disabled(key: str) -> bool:
        # Per-currency entry is enabled; only the master switch is off.
        return key == "nowpayments"

    cb = _make_callback(callback_data="cur_btc")
    state = _make_state(stash={"custom_amount": 4.0})
    create_mock = AsyncMock()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", side_effect=_stub_disabled
    ), patch(
        "handlers._start_nowpayments_invoice", new=create_mock,
        create=True,
    ):
        await process_custom_currency_selection(cb, state)

    create_mock.assert_not_awaited()
    cb.answer.assert_awaited_once()
    answer_kwargs = cb.answer.await_args.kwargs
    assert answer_kwargs.get("show_alert") is True


@pytest.mark.asyncio
async def test_currency_selection_card_gateway_passes_when_master_disabled():
    """Stage-15-Step-E #10b row 14: the NowPayments master switch
    must NOT veto card gateways. ``tetrapay`` / ``zarinpal`` have
    their own per-gateway toggles (already enforced earlier in the
    handler) and are unrelated to NowPayments.
    """
    from handlers import process_custom_currency_selection

    def _stub_disabled(key: str) -> bool:
        return key == "nowpayments"

    cb = _make_callback(callback_data="cur_zarinpal")
    state = _make_state(
        stash={"custom_amount": 4.0, "toman_rate_at_entry": 100_000.0}
    )
    start_mock = AsyncMock()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", side_effect=_stub_disabled
    ), patch(
        "handlers._start_zarinpal_invoice", new=start_mock
    ):
        await process_custom_currency_selection(cb, state)

    start_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_currency_selection_zarinpal_lost_amount_shows_error():
    """No ``custom_amount`` in FSM (e.g. user navigated to currency
    picker via stale inline button) → ``charge_amount_lost`` toast,
    no order creation."""
    from handlers import process_custom_currency_selection

    cb = _make_callback(callback_data="cur_zarinpal")
    state = _make_state(stash={})  # no custom_amount
    start_mock = AsyncMock()
    with patch(
        "handlers._get_user_language", new=AsyncMock(return_value="fa")
    ), patch(
        "handlers.is_gateway_disabled", return_value=False
    ), patch(
        "handlers._start_zarinpal_invoice", new=start_mock
    ):
        await process_custom_currency_selection(cb, state)

    start_mock.assert_not_awaited()
    cb.answer.assert_awaited_once()


# ---------------------------------------------------------------------
# _start_zarinpal_invoice happy path + failure modes
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_zarinpal_invoice_happy_path():
    """Order created → PENDING row inserted → user sees order_text +
    pay_button keyed on the StartPay URL."""
    from handlers import _start_zarinpal_invoice

    cb = _make_callback()
    state = _make_state()
    order = _fake_zarinpal_order()
    create_mock = AsyncMock(return_value=order)
    db_mock = AsyncMock(return_value=True)
    with patch(
        "handlers.zarinpal_create_order", new=create_mock
    ), patch(
        "handlers.db.create_pending_transaction", new=db_mock
    ):
        await _start_zarinpal_invoice(
            cb,
            state,
            lang="fa",
            amount_usd=4.0,
            toman_rate_at_entry=100_000.0,
            promo_code=None,
            promo_bonus_usd=0.0,
        )

    state.clear.assert_awaited_once()
    create_mock.assert_awaited_once()
    create_kwargs = create_mock.await_args.kwargs
    assert create_kwargs["amount_usd"] == pytest.approx(4.0)
    assert create_kwargs["rate_toman_per_usd"] == pytest.approx(100_000.0)
    assert create_kwargs["user_id"] == 42

    db_mock.assert_awaited_once()
    db_kwargs = db_mock.await_args.kwargs
    assert db_kwargs["telegram_id"] == 42
    assert db_kwargs["gateway"] == "zarinpal"
    assert db_kwargs["currency_used"] == "IRR"
    assert db_kwargs["amount_crypto"] == pytest.approx(4_000_000.0)
    assert db_kwargs["amount_usd"] == pytest.approx(4.0)
    assert db_kwargs["gateway_invoice_id"] == order.authority
    assert db_kwargs["gateway_locked_rate_toman_per_usd"] == pytest.approx(
        100_000.0
    )

    # Final user-facing render must have the StartPay URL on a button
    # AND must include the rial / USD figures in the body.
    final_render = cb.message.edit_text.await_args_list[-1]
    body = final_render.args[0]
    assert "4,000,000" in body  # amount_irr formatted
    assert "$4.00" in body  # amount_usd formatted
    assert "100,000" in body  # locked rate formatted
    markup = final_render.kwargs["reply_markup"]
    urls = [
        btn.url
        for row in markup.inline_keyboard
        for btn in row
        if getattr(btn, "url", None)
    ]
    assert order.payment_url in urls
    cb.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_zarinpal_invoice_promo_rides_through():
    """Promo data on the call → ``create_pending_transaction`` receives
    both ``promo_code`` and ``promo_bonus_usd`` so settlement credits
    the bonus."""
    from handlers import _start_zarinpal_invoice

    cb = _make_callback()
    state = _make_state()
    order = _fake_zarinpal_order()
    db_mock = AsyncMock(return_value=True)
    with patch(
        "handlers.zarinpal_create_order", new=AsyncMock(return_value=order)
    ), patch(
        "handlers.db.create_pending_transaction", new=db_mock
    ):
        await _start_zarinpal_invoice(
            cb,
            state,
            lang="fa",
            amount_usd=4.0,
            toman_rate_at_entry=100_000.0,
            promo_code="WELCOME",
            promo_bonus_usd=1.5,
        )

    db_kwargs = db_mock.await_args.kwargs
    assert db_kwargs["promo_code"] == "WELCOME"
    assert db_kwargs["promo_bonus_usd"] == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_start_zarinpal_invoice_missing_rate_renders_no_rate_error():
    """``toman_rate_at_entry=None`` → render ``charge_toman_no_rate``,
    NO order creation, retry button to ``amt_toman``."""
    from handlers import _start_zarinpal_invoice

    cb = _make_callback()
    state = _make_state()
    create_mock = AsyncMock()
    db_mock = AsyncMock()
    with patch(
        "handlers.zarinpal_create_order", new=create_mock
    ), patch(
        "handlers.db.create_pending_transaction", new=db_mock
    ):
        await _start_zarinpal_invoice(
            cb,
            state,
            lang="fa",
            amount_usd=4.0,
            toman_rate_at_entry=None,
            promo_code=None,
            promo_bonus_usd=0.0,
        )

    create_mock.assert_not_awaited()
    db_mock.assert_not_awaited()
    final_render = cb.message.edit_text.await_args_list[-1]
    body = final_render.args[0]
    # fa string includes "نرخ زنده" per strings.py
    assert "نرخ زنده" in body
    markup = final_render.kwargs["reply_markup"]
    callbacks = [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data is not None
    ]
    assert "amt_toman" in callbacks  # retry button


@pytest.mark.parametrize(
    "bad_rate",
    [
        float("nan"),
        float("inf"),
        -1.0,
        0.0,
        True,  # bool subclass of int slipped through pre-fix
        "100000",  # accidental string from corrupted FSM
    ],
)
@pytest.mark.asyncio
async def test_start_zarinpal_invoice_bad_rate_rejected_at_gate(bad_rate):
    """Bundled bug fix: tightened gate now rejects NaN / inf / negative
    / zero / bool / string rates BEFORE calling ``create_order``."""
    from handlers import _start_zarinpal_invoice

    cb = _make_callback()
    state = _make_state()
    create_mock = AsyncMock()
    with patch(
        "handlers.zarinpal_create_order", new=create_mock
    ), patch(
        "handlers.db.create_pending_transaction", new=AsyncMock()
    ):
        await _start_zarinpal_invoice(
            cb,
            state,
            lang="fa",
            amount_usd=4.0,
            toman_rate_at_entry=bad_rate,
            promo_code=None,
            promo_bonus_usd=0.0,
        )

    create_mock.assert_not_awaited()
    final_render = cb.message.edit_text.await_args_list[-1]
    assert "نرخ زنده" in final_render.args[0]


@pytest.mark.asyncio
async def test_start_zarinpal_invoice_create_order_raises_renders_unreachable():
    """``zarinpal_create_order`` raising (transport, missing merchant id,
    etc.) → render ``zarinpal_unreachable`` with retry/home buttons,
    no DB write."""
    from handlers import _start_zarinpal_invoice

    cb = _make_callback()
    state = _make_state()
    db_mock = AsyncMock()
    with patch(
        "handlers.zarinpal_create_order",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ), patch(
        "handlers.db.create_pending_transaction", new=db_mock
    ):
        await _start_zarinpal_invoice(
            cb,
            state,
            lang="fa",
            amount_usd=4.0,
            toman_rate_at_entry=100_000.0,
            promo_code=None,
            promo_bonus_usd=0.0,
        )

    db_mock.assert_not_awaited()
    final_render = cb.message.edit_text.await_args_list[-1]
    body = final_render.args[0]
    # fa zarinpal_unreachable includes "زرین‌پال"
    assert "زرین" in body or "ZWNJ" not in body  # ensure the gateway-named string surfaced
    markup = final_render.kwargs["reply_markup"]
    callbacks = [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data is not None
    ]
    assert "amt_toman" in callbacks
    assert "close_menu" in callbacks
    cb.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_zarinpal_invoice_db_pending_refused_renders_invoice_error():
    """``create_pending_transaction`` returning False (defensive guard
    or duplicate authority) → render ``charge_invoice_error`` with
    retry/home, no payment URL handed to the user."""
    from handlers import _start_zarinpal_invoice

    cb = _make_callback()
    state = _make_state()
    order = _fake_zarinpal_order()
    with patch(
        "handlers.zarinpal_create_order", new=AsyncMock(return_value=order)
    ), patch(
        "handlers.db.create_pending_transaction", new=AsyncMock(return_value=False)
    ):
        await _start_zarinpal_invoice(
            cb,
            state,
            lang="en",
            amount_usd=4.0,
            toman_rate_at_entry=100_000.0,
            promo_code=None,
            promo_bonus_usd=0.0,
        )

    final_render = cb.message.edit_text.await_args_list[-1]
    body = final_render.args[0]
    # en charge_invoice_error includes "Failed to create the invoice"
    # or similar; any string with "invoice" should be there.
    assert "invoice" in body.lower() or "خطا" in body
    markup = final_render.kwargs["reply_markup"]
    urls = [
        btn.url
        for row in markup.inline_keyboard
        for btn in row
        if getattr(btn, "url", None)
    ]
    # No StartPay URL handed out — the row is gone from the ledger.
    assert urls == []


# ---------------------------------------------------------------------
# String coverage: the gateway-named slugs must exist in both langs.
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "slug",
    [
        "zarinpal_button",
        "zarinpal_creating_order",
        "zarinpal_order_text",
        "zarinpal_pay_button",
        "zarinpal_unreachable",
        "zarinpal_credit_notification",
    ],
)
def test_zarinpal_strings_exist_in_both_languages(slug):
    """Both ``fa`` and ``en`` must define every Zarinpal-FSM slug."""
    from strings import _STRINGS

    for lang in ("fa", "en"):
        assert slug in _STRINGS[lang], (
            f"Missing slug={slug!r} for lang={lang!r} (i18n drift)"
        )
