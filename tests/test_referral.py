"""Stage-13-Step-C: referral codes.

Three layers covered here:

1. **Pure config / parsing helpers in ``referral.py``** —
   ``parse_start_payload`` / ``parse_referral_payload`` (the bundled
   bug-fix that finally inspects the ``/start <payload>`` argument
   ``cmd_start`` ignored pre-PR-110), the env-var bonus knobs
   (``REFERRAL_BONUS_PERCENT`` / ``REFERRAL_BONUS_MAX_USD``) with
   defensive fallbacks for malformed input, and ``build_share_url``.

2. **Bonus computation** — :meth:`Database.compute_referral_bonus`
   under the standard inputs (10% × $20 = $2), boundary inputs (cap
   triggers, $0 input, NaN input), and the static-method invariant
   that it never returns a value above the configured cap.

3. **Wallet-screen + cmd_start wiring** — the new ``hub_invite``
   handler (renders link / no-link variants depending on
   ``BOT_USERNAME``) and ``cmd_start``'s new branch that calls
   ``db.claim_referral`` when the message carries a ``ref_<code>``
   payload.

The DB layer methods that touch a live Postgres connection
(``claim_referral`` / ``_grant_referral_in_tx``) are NOT exercised
here — they require a real DB and are covered indirectly by the
finalize-payment integration tests in CI. We DO test the
config-free static helper :meth:`Database.compute_referral_bonus`
which is the only piece of the bonus pipeline that's pure-Python.
"""

from __future__ import annotations

import math
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from database import Database


# =====================================================================
# parse_start_payload + parse_referral_payload
# =====================================================================


@pytest.mark.parametrize(
    "text, expected",
    [
        ("/start", None),
        ("/start ", None),
        ("/start   ", None),
        ("", None),
        (None, None),
        ("/start abc123", "abc123"),
        ("/start@MyBot abc123", "abc123"),
        ("/start ref_ABC12345", "ref_ABC12345"),
        # Length-bounded: anything over 64 chars is junk (Telegram's
        # own deep-link cap).
        ("/start " + "x" * 65, None),
        ("/start " + "x" * 64, "x" * 64),
        # Only the FIRST whitespace-separated token after /start is
        # taken; anything beyond that the user typed gets folded into
        # the payload (deliberate — mirrors how Telegram delivers the
        # raw text to the bot when the deep-link runs).
        ("/start foo bar baz", "foo bar baz"),
    ],
)
def test_parse_start_payload(text, expected):
    from referral import parse_start_payload

    assert parse_start_payload(text) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        # Happy path
        ("/start ref_ABC12345", "ABC12345"),
        ("/start ref_a", "a"),
        # Wrong prefix → not a referral payload
        ("/start promo_ABC", None),
        ("/start abc123", None),
        # No payload
        ("/start", None),
        ("", None),
        (None, None),
        # Malformed / empty code
        ("/start ref_", None),
        # Too long for the deep-link
        ("/start ref_" + "X" * 100, None),
        # Mixed-case / underscores / dashes accepted by the regex
        # (DB lookup will reject unknowns separately)
        ("/start ref_AbC-1_2", "AbC-1_2"),
    ],
)
def test_parse_referral_payload(text, expected):
    from referral import parse_referral_payload

    assert parse_referral_payload(text) == expected


# =====================================================================
# env-var config (REFERRAL_BONUS_PERCENT / REFERRAL_BONUS_MAX_USD)
# =====================================================================


def test_get_referral_bonus_percent_default_is_10(monkeypatch):
    monkeypatch.delenv("REFERRAL_BONUS_PERCENT", raising=False)

    import referral

    assert referral.get_referral_bonus_percent() == 10.0


def test_get_referral_bonus_percent_reads_env(monkeypatch):
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", "25")

    import referral

    assert referral.get_referral_bonus_percent() == 25.0


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "abc", "nan", "inf", "-5", "0", "-0"],
)
def test_get_referral_bonus_percent_falls_back_on_bad_input(monkeypatch, raw):
    """Malformed / non-finite / non-positive values fall back to the
    default rather than silently disabling the feature."""
    monkeypatch.setenv("REFERRAL_BONUS_PERCENT", raw)

    import referral

    assert referral.get_referral_bonus_percent() == 10.0


def test_get_referral_bonus_max_usd_default_is_5(monkeypatch):
    monkeypatch.delenv("REFERRAL_BONUS_MAX_USD", raising=False)

    import referral

    assert referral.get_referral_bonus_max_usd() == 5.0


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("3", 3.0),
        ("12.5", 12.5),
        ("", 5.0),
        ("nan", 5.0),
        ("-1", 5.0),
    ],
)
def test_get_referral_bonus_max_usd_env_handling(monkeypatch, raw, expected):
    if raw == "":
        monkeypatch.delenv("REFERRAL_BONUS_MAX_USD", raising=False)
    else:
        monkeypatch.setenv("REFERRAL_BONUS_MAX_USD", raw)

    import referral

    assert referral.get_referral_bonus_max_usd() == expected


# =====================================================================
# build_share_url
# =====================================================================


def test_build_share_url_with_bot_username(monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "Meowassist_Ai_bot")
    from referral import build_share_url

    url = build_share_url("ABC12345")
    assert url == "https://t.me/Meowassist_Ai_bot?start=ref_ABC12345"


def test_build_share_url_strips_leading_at(monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "@Meowassist_Ai_bot")
    from referral import build_share_url

    url = build_share_url("XYZ")
    assert url == "https://t.me/Meowassist_Ai_bot?start=ref_XYZ"


def test_build_share_url_returns_none_when_unset(monkeypatch):
    monkeypatch.delenv("BOT_USERNAME", raising=False)
    from referral import build_share_url

    assert build_share_url("ABC12345") is None


def test_build_share_url_returns_none_for_blank(monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "   ")
    from referral import build_share_url

    assert build_share_url("ABC12345") is None


# =====================================================================
# Database.compute_referral_bonus (pure static method)
# =====================================================================


def test_compute_referral_bonus_standard_case():
    """10% of $20 = $2, well under the $5 cap."""
    bonus = Database.compute_referral_bonus(20.0, percent=10.0, max_usd=5.0)
    assert bonus == 2.0


def test_compute_referral_bonus_caps_at_max():
    """10% of $100 would be $10 — but the $5 cap applies."""
    bonus = Database.compute_referral_bonus(100.0, percent=10.0, max_usd=5.0)
    assert bonus == 5.0


def test_compute_referral_bonus_higher_percent():
    bonus = Database.compute_referral_bonus(20.0, percent=25.0, max_usd=10.0)
    assert bonus == 5.0  # 25% of $20


def test_compute_referral_bonus_zero_amount():
    assert Database.compute_referral_bonus(0.0, percent=10.0, max_usd=5.0) == 0.0


def test_compute_referral_bonus_negative_amount_returns_zero():
    """A non-positive triggering amount must NOT credit anyone — defence
    in depth against a future caller bypassing the finalize_payment
    finite/positive guard."""
    assert Database.compute_referral_bonus(-5.0, percent=10.0, max_usd=5.0) == 0.0


def test_compute_referral_bonus_nan_amount_returns_zero():
    assert Database.compute_referral_bonus(math.nan, percent=10.0, max_usd=5.0) == 0.0


def test_compute_referral_bonus_inf_amount_returns_zero():
    assert Database.compute_referral_bonus(math.inf, percent=10.0, max_usd=5.0) == 0.0


def test_compute_referral_bonus_nan_percent_returns_zero():
    assert Database.compute_referral_bonus(20.0, percent=math.nan, max_usd=5.0) == 0.0


def test_compute_referral_bonus_negative_max_returns_zero():
    """A misconfigured negative cap shouldn't accidentally invert the
    feature into a wallet drain."""
    assert Database.compute_referral_bonus(20.0, percent=10.0, max_usd=-5.0) == 0.0


def test_compute_referral_bonus_rounds_to_4_decimals():
    """Wallet ledger column is ``DECIMAL(10, 4)``; the helper rounds to
    that precision so the DB write doesn't get rejected."""
    bonus = Database.compute_referral_bonus(
        3.33333333, percent=10.0, max_usd=100.0
    )
    # Expected: 0.333333… → rounded to 4dp = 0.3333
    assert bonus == 0.3333


# =====================================================================
# _generate_referral_code (pure)
# =====================================================================


def test_generate_referral_code_length_and_alphabet():
    """The generated code is 8 chars from the curated alphabet and is
    not predictable across calls."""
    code1 = Database._generate_referral_code()
    code2 = Database._generate_referral_code()
    assert len(code1) == Database.REFERRAL_CODE_LEN
    assert len(code2) == Database.REFERRAL_CODE_LEN
    for c in code1 + code2:
        assert c in Database.REFERRAL_CODE_ALPHABET
    # Two random 8-char draws from a 32-char alphabet are vanishingly
    # unlikely to collide, so this is a reasonable randomness pin.
    assert code1 != code2


def test_generate_referral_code_excludes_ambiguous_chars():
    """The alphabet excludes ``0`` / ``O`` and ``1`` / ``I`` so a user
    dictating a code over the phone (or visually copying it from a
    DM) can't confuse the digits with the letters. Lowercase isn't
    in the alphabet at all so the lowercase ``l``-vs-``1`` ambiguity
    is moot. Pin the curated alphabet."""
    forbidden = "01IO"
    for c in forbidden:
        assert c not in Database.REFERRAL_CODE_ALPHABET


# =====================================================================
# hub_invite_handler — wallet screen rendering
# =====================================================================


def _make_callback(user_id: int = 555) -> MagicMock:
    cb = MagicMock()
    cb.from_user = SimpleNamespace(id=user_id)
    cb.message = MagicMock()
    cb.message.edit_text = AsyncMock()
    cb.answer = AsyncMock()
    return cb


@pytest.mark.asyncio
async def test_hub_invite_handler_with_bot_username(monkeypatch):
    """When ``BOT_USERNAME`` is configured, the screen renders the
    full deep-link variant of the invite text."""
    monkeypatch.setenv("BOT_USERNAME", "Meowassist_Ai_bot")
    monkeypatch.delenv("REFERRAL_BONUS_PERCENT", raising=False)
    monkeypatch.delenv("REFERRAL_BONUS_MAX_USD", raising=False)

    import handlers

    cb = _make_callback(user_id=42)
    state = MagicMock()
    state.clear = AsyncMock()

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(
            handlers.db,
            "get_or_create_referral_code",
            AsyncMock(return_value="ABC12345"),
        ),
        patch.object(
            handlers.db,
            "get_referral_stats",
            AsyncMock(
                return_value={
                    "pending": 3,
                    "paid": 2,
                    "total_bonus_usd": 4.50,
                }
            ),
        ),
    ):
        await handlers.hub_invite_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "ABC12345" in body
    assert "https://t.me/Meowassist_Ai_bot?start=ref_ABC12345" in body
    assert "$4.50" in body  # total bonus formatted
    # Pending / paid counts present in the stats line
    assert "3" in body
    assert "2" in body


@pytest.mark.asyncio
async def test_hub_invite_handler_without_bot_username(monkeypatch):
    """When ``BOT_USERNAME`` is unset, the degraded variant renders the
    bare code with ``/start ref_<code>`` instructions."""
    monkeypatch.delenv("BOT_USERNAME", raising=False)

    import handlers

    cb = _make_callback(user_id=42)
    state = MagicMock()
    state.clear = AsyncMock()

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(
            handlers.db,
            "get_or_create_referral_code",
            AsyncMock(return_value="ABC12345"),
        ),
        patch.object(
            handlers.db,
            "get_referral_stats",
            AsyncMock(
                return_value={"pending": 0, "paid": 0, "total_bonus_usd": 0.0}
            ),
        ),
    ):
        await handlers.hub_invite_handler(cb, state)

    body = cb.message.edit_text.await_args.args[0]
    assert "ABC12345" in body
    assert "/start ref_ABC12345" in body
    # No share URL synthesised
    assert "https://t.me/" not in body


@pytest.mark.asyncio
async def test_hub_invite_handler_lazy_creates_code(monkeypatch):
    """Pin: the handler always goes through ``get_or_create_referral_code``
    (never a raw ``lookup_referral_code``) so the row is created on
    first visit if the user has never tapped the button before."""
    monkeypatch.setenv("BOT_USERNAME", "Meowassist_Ai_bot")

    import handlers

    cb = _make_callback(user_id=99)
    state = MagicMock()
    state.clear = AsyncMock()
    create_mock = AsyncMock(return_value="NEWCODE0")

    with (
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "get_or_create_referral_code", create_mock),
        patch.object(
            handlers.db,
            "get_referral_stats",
            AsyncMock(
                return_value={"pending": 0, "paid": 0, "total_bonus_usd": 0.0}
            ),
        ),
    ):
        await handlers.hub_invite_handler(cb, state)

    create_mock.assert_awaited_once_with(99)


# =====================================================================
# cmd_start — referral payload processing
# =====================================================================


@pytest.mark.asyncio
async def test_cmd_start_with_referral_payload_calls_claim_referral():
    """Pin: ``/start ref_<code>`` triggers ``db.claim_referral`` with
    the parsed code. Pre-PR-110 ``cmd_start`` ignored the payload
    entirely (the bundled bug-fix from the audit findings)."""
    import handlers

    msg = MagicMock()
    msg.from_user = SimpleNamespace(id=42, username="alice", first_name="Alice")
    msg.text = "/start ref_ABC12345"
    msg.answer = AsyncMock()
    state = MagicMock()
    state.clear = AsyncMock()
    claim_mock = AsyncMock(return_value={"status": "ok", "referrer_telegram_id": 7})

    with (
        patch.object(handlers.db, "create_user", AsyncMock()),
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "claim_referral", claim_mock),
        patch.object(handlers, "_send_hub", AsyncMock()),
    ):
        await handlers.cmd_start(msg, state)

    claim_mock.assert_awaited_once_with(
        invitee_telegram_id=42, code="ABC12345"
    )


@pytest.mark.asyncio
async def test_cmd_start_without_payload_does_not_call_claim_referral():
    """Pin: a bare ``/start`` (no payload) must NOT touch the referral
    DB layer — every existing user opens /start daily."""
    import handlers

    msg = MagicMock()
    msg.from_user = SimpleNamespace(id=42, username="alice", first_name="Alice")
    msg.text = "/start"
    msg.answer = AsyncMock()
    state = MagicMock()
    state.clear = AsyncMock()
    claim_mock = AsyncMock()

    with (
        patch.object(handlers.db, "create_user", AsyncMock()),
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(handlers.db, "claim_referral", claim_mock),
        patch.object(handlers, "_send_hub", AsyncMock()),
    ):
        await handlers.cmd_start(msg, state)

    claim_mock.assert_not_called()


@pytest.mark.asyncio
async def test_cmd_start_with_unknown_code_renders_unknown_flash():
    import handlers

    msg = MagicMock()
    msg.from_user = SimpleNamespace(id=42, username="alice", first_name="Alice")
    msg.text = "/start ref_NOSUCH"
    msg.answer = AsyncMock()
    state = MagicMock()
    state.clear = AsyncMock()

    with (
        patch.object(handlers.db, "create_user", AsyncMock()),
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(
            handlers.db,
            "claim_referral",
            AsyncMock(return_value={"status": "unknown"}),
        ),
        patch.object(handlers, "_send_hub", AsyncMock()),
    ):
        await handlers.cmd_start(msg, state)

    # Two answer() calls: the flash + the start_greeting.
    assert msg.answer.await_count >= 2
    flash_text = msg.answer.await_args_list[0].args[0]
    assert "isn't valid" in flash_text or "Welcome" in flash_text


@pytest.mark.asyncio
async def test_cmd_start_with_self_referral_renders_self_flash():
    import handlers

    msg = MagicMock()
    msg.from_user = SimpleNamespace(id=42, username="alice", first_name="Alice")
    msg.text = "/start ref_OWNCODE0"
    msg.answer = AsyncMock()
    state = MagicMock()
    state.clear = AsyncMock()

    with (
        patch.object(handlers.db, "create_user", AsyncMock()),
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(
            handlers.db,
            "claim_referral",
            AsyncMock(return_value={"status": "self"}),
        ),
        patch.object(handlers, "_send_hub", AsyncMock()),
    ):
        await handlers.cmd_start(msg, state)

    flash_text = msg.answer.await_args_list[0].args[0]
    assert "own invite code" in flash_text


@pytest.mark.asyncio
async def test_cmd_start_claim_failure_does_not_block_hub_render():
    """Pin: if ``db.claim_referral`` raises (DB unavailable, FK error,
    etc.), the user still gets the hub. A typo'd deep-link must not
    strand the user on a dead screen."""
    import handlers

    msg = MagicMock()
    msg.from_user = SimpleNamespace(id=42, username="alice", first_name="Alice")
    msg.text = "/start ref_ABC12345"
    msg.answer = AsyncMock()
    state = MagicMock()
    state.clear = AsyncMock()
    send_hub_mock = AsyncMock()

    with (
        patch.object(handlers.db, "create_user", AsyncMock()),
        patch.object(handlers, "_get_user_language", AsyncMock(return_value="en")),
        patch.object(
            handlers.db, "claim_referral", AsyncMock(side_effect=RuntimeError("DB down"))
        ),
        patch.object(handlers, "_send_hub", send_hub_mock),
    ):
        await handlers.cmd_start(msg, state)

    # Hub renders despite the claim raising
    send_hub_mock.assert_awaited_once()


# =====================================================================
# Wallet keyboard — invite button is present
# =====================================================================


# =====================================================================
# _grant_referral_in_tx — in-memory connection mock pinning the SQL
# flow + side effects.
# =====================================================================


class _FakeConn:
    """Minimal asyncpg-connection stand-in. Tracks ``execute`` /
    ``fetchrow`` calls so the test can assert against the SQL flow.
    The first ``fetchrow`` returns ``self.grant_row`` (the PENDING
    grant we expect the helper to lock); subsequent calls all
    return ``None``."""

    def __init__(self, grant_row=None):
        self.grant_row = grant_row
        self.executed: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self._fetchrow_calls = 0

    async def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        self._fetchrow_calls += 1
        if self._fetchrow_calls == 1:
            return self.grant_row
        return None

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "UPDATE 1"


@pytest.mark.asyncio
async def test_grant_referral_in_tx_no_pending_grant_returns_none():
    """No PENDING grant for the invitee → no-op, no DB writes."""
    conn = _FakeConn(grant_row=None)
    db = Database()
    result = await db._grant_referral_in_tx(
        conn,
        invitee_telegram_id=42,
        amount_usd=20.0,
        transaction_id=99,
        bonus_percent=10.0,
        bonus_max_usd=5.0,
    )
    assert result is None
    # Only the lookup happened — no UPDATEs.
    assert conn.executed == []


@pytest.mark.asyncio
async def test_grant_referral_in_tx_credits_both_wallets():
    """Pending grant exists → flip to PAID, credit both wallets, return
    info dict. Three UPDATE statements: grant row, referrer wallet,
    invitee wallet (in that order)."""
    grant_row = {
        "id": 7,
        "referrer_telegram_id": 100,
        "invitee_telegram_id": 42,
        "status": "PENDING",
    }
    conn = _FakeConn(grant_row=grant_row)
    db = Database()
    result = await db._grant_referral_in_tx(
        conn,
        invitee_telegram_id=42,
        amount_usd=20.0,
        transaction_id=99,
        bonus_percent=10.0,
        bonus_max_usd=5.0,
    )
    assert result is not None
    assert result["referrer_telegram_id"] == 100
    assert result["bonus_usd"] == 2.0  # 10% of $20
    assert result["amount_usd"] == 20.0
    assert result["grant_id"] == 7
    # Three UPDATEs in order: grant flip, referrer credit, invitee credit.
    assert len(conn.executed) == 3
    grant_update, referrer_credit, invitee_credit = conn.executed
    assert "UPDATE referral_grants" in grant_update[0]
    assert "PAID" in grant_update[0]
    assert "UPDATE users" in referrer_credit[0]
    assert "UPDATE users" in invitee_credit[0]
    # Bonus value flowed into all three queries
    assert grant_update[1][1] == 2.0  # bonus_usd_referrer
    assert grant_update[1][2] == 2.0  # bonus_usd_invitee
    assert referrer_credit[1][0] == 2.0
    assert invitee_credit[1][0] == 2.0
    # Wallets credited to the right users
    assert referrer_credit[1][1] == 100
    assert invitee_credit[1][1] == 42


@pytest.mark.asyncio
async def test_grant_referral_in_tx_caps_at_max():
    """A $100 first top-up at 10% would be $10 — but the $5 cap
    applies; only $5 credited per side."""
    grant_row = {
        "id": 7,
        "referrer_telegram_id": 100,
        "invitee_telegram_id": 42,
        "status": "PENDING",
    }
    conn = _FakeConn(grant_row=grant_row)
    db = Database()
    result = await db._grant_referral_in_tx(
        conn,
        invitee_telegram_id=42,
        amount_usd=100.0,
        transaction_id=99,
        bonus_percent=10.0,
        bonus_max_usd=5.0,
    )
    assert result["bonus_usd"] == 5.0


@pytest.mark.asyncio
async def test_grant_referral_in_tx_refuses_non_finite_amount():
    """Defence in depth: NaN / Inf amount returns None, no DB writes."""
    conn = _FakeConn(grant_row={"id": 7, "referrer_telegram_id": 100, "invitee_telegram_id": 42, "status": "PENDING"})
    db = Database()
    for bad in (math.nan, math.inf, -math.inf, -1.0, 0.0):
        conn.executed.clear()
        conn.fetchrow_calls.clear()
        conn._fetchrow_calls = 0
        result = await db._grant_referral_in_tx(
            conn,
            invitee_telegram_id=42,
            amount_usd=bad,
            transaction_id=99,
            bonus_percent=10.0,
            bonus_max_usd=5.0,
        )
        assert result is None
        # Defensive guard short-circuits BEFORE the SELECT FOR UPDATE.
        assert conn.executed == []


def test_wallet_keyboard_has_invite_button():
    """Pin: the ``hub_invite`` callback button is present on the wallet
    keyboard so users can reach the invite screen."""
    import handlers

    kb = handlers._build_wallet_keyboard("en")
    callback_data = []
    for row in kb.export():
        for btn in row:
            if hasattr(btn, "callback_data") and btn.callback_data:
                callback_data.append(btn.callback_data)
    assert "hub_invite" in callback_data
