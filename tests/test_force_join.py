"""Stage-13-Step-A: required-channel subscription gate tests.

Pin every branch the middleware can take so a future refactor can't
accidentally:

* Lock admins out of their own bot.
* Fail closed on a misconfigured channel (the "bot isn't admin yet"
  bootstrap scenario).
* Loop forever on the "✅ I've joined" callback.
* Leak ``$nan`` into the hub view (bundled bug-fix regression pin).

The tests stub ``bot.get_chat_member`` rather than spinning up a
real Telegram client. The middleware contract is defined in terms
of ``ChatMember.status`` strings, which is the shape the Bot API
itself returns.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiogram.exceptions import TelegramBadRequest

import force_join
from force_join import (
    FORCE_JOIN_CHECK_CALLBACK,
    RequiredChannelMiddleware,
    build_join_keyboard,
    build_join_url,
    force_join_check_callback,
    get_required_channel,
    get_required_channel_invite_link,
    is_joined_status,
    render_join_prompt,
    user_is_member,
)


# ---------- env-string parsing ----------


def test_get_required_channel_unset_returns_empty(monkeypatch):
    monkeypatch.delenv("REQUIRED_CHANNEL", raising=False)
    assert get_required_channel() == ""


def test_get_required_channel_empty_string_returns_empty(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "   ")
    assert get_required_channel() == ""


def test_get_required_channel_at_handle_passthrough(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@MeowAssist_Channel")
    assert get_required_channel() == "@MeowAssist_Channel"


def test_get_required_channel_bare_handle_gets_at_prefix(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "MeowAssist_Channel")
    assert get_required_channel() == "@MeowAssist_Channel"


def test_get_required_channel_numeric_id_passthrough(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "-1001234567890")
    assert get_required_channel() == "-1001234567890"


def test_get_required_channel_invite_link_default_empty(monkeypatch):
    monkeypatch.delenv("REQUIRED_CHANNEL_INVITE_LINK", raising=False)
    assert get_required_channel_invite_link() == ""


# ---------- join-URL synthesis ----------


def test_build_join_url_handle_synthesises_t_me(monkeypatch):
    monkeypatch.delenv("REQUIRED_CHANNEL_INVITE_LINK", raising=False)
    assert build_join_url("@MeowAssist") == "https://t.me/MeowAssist"


def test_build_join_url_numeric_id_returns_empty_without_override(monkeypatch):
    monkeypatch.delenv("REQUIRED_CHANNEL_INVITE_LINK", raising=False)
    assert build_join_url("-1001234567890") == ""


def test_build_join_url_override_wins_over_handle(monkeypatch):
    monkeypatch.setenv(
        "REQUIRED_CHANNEL_INVITE_LINK",
        "https://t.me/+abcdefINVITE",
    )
    assert (
        build_join_url("@PublicHandle") == "https://t.me/+abcdefINVITE"
    )


# ---------- ChatMember.status predicate ----------


@pytest.mark.parametrize(
    "status, is_member, expected",
    [
        ("creator", None, True),
        ("administrator", None, True),
        ("member", None, True),
        ("restricted", True, True),
        ("restricted", False, False),
        ("left", None, False),
        ("kicked", None, False),
        (None, None, False),
        ("unknown_status", None, False),
    ],
)
def test_is_joined_status(status, is_member, expected):
    assert is_joined_status(status, is_member) is expected


# ---------- user_is_member: API error fail-open ----------


@pytest.mark.asyncio
async def test_user_is_member_returns_true_for_member():
    bot = SimpleNamespace(
        get_chat_member=AsyncMock(
            return_value=SimpleNamespace(status="member")
        )
    )
    assert await user_is_member(bot, "@chan", 12345) is True


@pytest.mark.asyncio
async def test_user_is_member_returns_false_for_left():
    bot = SimpleNamespace(
        get_chat_member=AsyncMock(
            return_value=SimpleNamespace(status="left")
        )
    )
    assert await user_is_member(bot, "@chan", 12345) is False


@pytest.mark.asyncio
async def test_user_is_member_returns_none_on_telegram_bad_request():
    """Pre-fix the gate would crash the handler chain. Now it logs and
    returns None so the middleware can fail open."""
    bot = SimpleNamespace(
        get_chat_member=AsyncMock(
            side_effect=TelegramBadRequest(
                method=MagicMock(), message="chat not found"
            )
        )
    )
    assert await user_is_member(bot, "@chan", 12345) is None


# ---------- keyboard shape ----------


def test_build_join_keyboard_has_join_and_check_buttons():
    kb = build_join_keyboard("https://t.me/some_chan", "en")
    rows = kb.inline_keyboard
    flat = [b for row in rows for b in row]
    urls = [b.url for b in flat if b.url]
    callbacks = [b.callback_data for b in flat if b.callback_data]
    assert "https://t.me/some_chan" in urls
    assert FORCE_JOIN_CHECK_CALLBACK in callbacks


def test_build_join_keyboard_omits_join_when_no_url():
    """Numeric-id channels without an explicit invite link override
    drop the Join button — but the re-check button is always there
    so the user can self-recover after the operator fixes the
    config."""
    kb = build_join_keyboard("", "en")
    rows = kb.inline_keyboard
    flat = [b for row in rows for b in row]
    urls = [b.url for b in flat if b.url]
    callbacks = [b.callback_data for b in flat if b.callback_data]
    assert urls == []
    assert FORCE_JOIN_CHECK_CALLBACK in callbacks


# ---------- middleware: short-circuit branches ----------


@pytest.mark.asyncio
async def test_middleware_no_op_when_channel_unset(monkeypatch):
    monkeypatch.delenv("REQUIRED_CHANNEL", raising=False)
    handler = AsyncMock(return_value="ran")
    event = SimpleNamespace(
        from_user=SimpleNamespace(id=42), bot=SimpleNamespace()
    )
    mw = RequiredChannelMiddleware()
    result = await mw(handler, event, {})
    assert result == "ran"
    handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_admin_bypass(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    from admin import set_admin_user_ids

    set_admin_user_ids({99})
    handler = AsyncMock(return_value="ran")
    event = SimpleNamespace(
        from_user=SimpleNamespace(id=99), bot=SimpleNamespace()
    )
    mw = RequiredChannelMiddleware()
    result = await mw(handler, event, {})
    set_admin_user_ids(set())
    assert result == "ran"
    handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_skips_force_join_check_callback(monkeypatch):
    """The check-callback handler does the membership re-check itself;
    if the middleware also intercepted it the user could never
    exit the gate."""
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    from aiogram.types import CallbackQuery

    handler = AsyncMock(return_value="ran")
    cb = MagicMock(spec=CallbackQuery)
    cb.from_user = SimpleNamespace(id=42)
    cb.bot = SimpleNamespace()
    cb.data = FORCE_JOIN_CHECK_CALLBACK
    mw = RequiredChannelMiddleware()
    result = await mw(handler, cb, {})
    assert result == "ran"
    handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_no_op_when_from_user_is_none(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    handler = AsyncMock(return_value="ran")
    event = SimpleNamespace(from_user=None, bot=SimpleNamespace())
    mw = RequiredChannelMiddleware()
    result = await mw(handler, event, {})
    assert result == "ran"
    handler.assert_awaited_once()


# ---------- middleware: actual gate behaviour ----------


@pytest.mark.asyncio
async def test_middleware_lets_member_through(monkeypatch):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    handler = AsyncMock(return_value="ran")
    bot = SimpleNamespace(
        get_chat_member=AsyncMock(
            return_value=SimpleNamespace(status="member")
        )
    )
    event = SimpleNamespace(
        from_user=SimpleNamespace(id=42), bot=bot
    )
    mw = RequiredChannelMiddleware()
    result = await mw(handler, event, {})
    assert result == "ran"
    handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_fails_open_on_bad_request(monkeypatch):
    """If the bot isn't admin of the channel yet, every user would be
    locked out under fail-closed semantics — operator can't even
    join their own bot to fix the config. Fail open and log."""
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    handler = AsyncMock(return_value="ran")
    bot = SimpleNamespace(
        get_chat_member=AsyncMock(
            side_effect=TelegramBadRequest(
                method=MagicMock(), message="chat not found"
            )
        )
    )
    event = SimpleNamespace(
        from_user=SimpleNamespace(id=42), bot=bot
    )
    mw = RequiredChannelMiddleware()
    result = await mw(handler, event, {})
    assert result == "ran"
    handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_blocks_non_member_and_renders_prompt(
    monkeypatch,
):
    """Pre-feature any non-member could chat with the bot. Now they get
    the join screen and the underlying handler does NOT run."""
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    handler = AsyncMock(return_value="ran")
    bot = SimpleNamespace(
        get_chat_member=AsyncMock(
            return_value=SimpleNamespace(status="left")
        )
    )

    answer = AsyncMock()
    event = SimpleNamespace(
        from_user=SimpleNamespace(id=42),
        bot=bot,
        answer=answer,
    )
    # The middleware imports ``Message`` for the isinstance check;
    # patch render_join_prompt so we don't have to construct a
    # full Message instance.
    with patch("force_join.render_join_prompt", new=AsyncMock()) as render:
        mw = RequiredChannelMiddleware()
        result = await mw(handler, event, {})

    assert result is None
    handler.assert_not_awaited()
    render.assert_awaited_once()


# ---------- force_join_check callback: re-check logic ----------


@pytest.mark.asyncio
async def test_force_join_check_callback_routes_to_hub_when_joined(
    monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    cb = SimpleNamespace(
        from_user=SimpleNamespace(id=42),
        bot=SimpleNamespace(
            get_chat_member=AsyncMock(
                return_value=SimpleNamespace(status="member")
            )
        ),
        message=SimpleNamespace(edit_text=AsyncMock()),
        answer=AsyncMock(),
    )
    with patch("force_join._drop_at_hub", new=AsyncMock()) as drop:
        await force_join_check_callback(cb)
    drop.assert_awaited_once_with(cb)


@pytest.mark.asyncio
async def test_force_join_check_callback_re_renders_not_yet_when_still_left(
    monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@somechan")
    cb = SimpleNamespace(
        from_user=SimpleNamespace(id=42),
        bot=SimpleNamespace(
            get_chat_member=AsyncMock(
                return_value=SimpleNamespace(status="left")
            )
        ),
        message=SimpleNamespace(edit_text=AsyncMock()),
        answer=AsyncMock(),
    )
    with patch("force_join.render_join_prompt", new=AsyncMock()) as render:
        await force_join_check_callback(cb)
    render.assert_awaited_once()
    # not_yet=True must be passed so the user sees the clearer
    # "still not joined" flash.
    args, kwargs = render.call_args
    assert kwargs.get("not_yet") is True


# ---------- bundled bug-fix: hub_title NaN guard ----------


@pytest.mark.asyncio
async def test_hub_title_nan_balance_renders_zero_not_nan():
    """Pre-fix a corrupted ``users.balance_usd`` row (legacy NaN,
    manual SQL fix) leaked literally ``$nan`` into the hub view —
    same regression PR #101 fixed for ``wallet_text`` via
    ``format_balance_block``. The hub template was missed; this
    pin guards the fix."""
    import math
    from handlers import _hub_text_and_kb

    fake_user = {
        "active_model": "openai/gpt-4o",
        "balance_usd": math.nan,
        "memory_enabled": False,
    }
    with patch("handlers.db.get_user", new=AsyncMock(return_value=fake_user)):
        text, _kb = await _hub_text_and_kb(12345, "en")
    assert "$nan" not in text.lower()
    assert "$0.00" in text


@pytest.mark.asyncio
async def test_hub_title_inf_balance_renders_zero_not_inf():
    import math
    from handlers import _hub_text_and_kb

    fake_user = {
        "active_model": "openai/gpt-4o",
        "balance_usd": math.inf,
        "memory_enabled": False,
    }
    with patch("handlers.db.get_user", new=AsyncMock(return_value=fake_user)):
        text, _kb = await _hub_text_and_kb(12345, "en")
    assert "$inf" not in text.lower()
    assert "$0.00" in text


# ---------- render_join_prompt: Message vs CallbackQuery dispatch ----------


@pytest.mark.asyncio
async def test_render_join_prompt_message_calls_answer():
    from aiogram.types import Message

    msg = MagicMock(spec=Message)
    msg.from_user = SimpleNamespace(id=42)
    msg.answer = AsyncMock()
    with patch(
        "force_join._user_lang", new=AsyncMock(return_value="en")
    ):
        await render_join_prompt(msg, "@somechan")
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_render_join_prompt_callback_calls_edit_text():
    from aiogram.types import CallbackQuery

    cb = MagicMock(spec=CallbackQuery)
    cb.from_user = SimpleNamespace(id=42)
    cb.message = SimpleNamespace(edit_text=AsyncMock())
    cb.answer = AsyncMock()
    with patch(
        "force_join._user_lang", new=AsyncMock(return_value="en")
    ):
        await render_join_prompt(cb, "@somechan")
    cb.message.edit_text.assert_awaited_once()
    cb.answer.assert_awaited_once()


# ---------------------------------------------------------------------
# Stage-15-Step-E #10b row 5: DB-backed REQUIRED_CHANNEL override
# ---------------------------------------------------------------------
#
# Mirrors :file:`tests/test_payments_min_topup_override.py`. Same fixture
# (auto-clear cache between tests + scrub the env var), same coverage
# matrix (set / clear / get, source reporting, refresh-from-db happy /
# missing / error / none-db / malformed / out-of-range, plus the
# resolution-order pin so future refactors can't accidentally swap the
# override and env precedence).


@pytest.fixture
def _reset_required_channel_override(monkeypatch):
    force_join.clear_required_channel_override()
    monkeypatch.delenv("REQUIRED_CHANNEL", raising=False)
    yield
    force_join.clear_required_channel_override()


# ---------- _normalise_channel / _coerce_required_channel ----------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("", ""),
        ("   ", ""),
        ("@MeowAssist", "@MeowAssist"),
        ("MeowAssist", "@MeowAssist"),
        ("-1001234567890", "-1001234567890"),
        ("  @padded  ", "@padded"),
        # 65 chars > REQUIRED_CHANNEL_MAX_LENGTH (64).
        ("a" * 65, ""),
    ],
)
def test_normalise_channel_canonical_form(raw, expected):
    assert force_join._normalise_channel(raw) == expected


def test_normalise_channel_non_string_returns_empty():
    assert force_join._normalise_channel(123) == ""  # type: ignore[arg-type]
    assert force_join._normalise_channel(None) == ""  # type: ignore[arg-type]


def test_coerce_required_channel_accepts_empty_string():
    """The empty string is a VALID override value (force gate OFF)."""
    assert force_join._coerce_required_channel("") == ""
    assert force_join._coerce_required_channel("   ") == ""


def test_coerce_required_channel_accepts_handle_and_id():
    assert force_join._coerce_required_channel("@chan") == "@chan"
    assert force_join._coerce_required_channel("chan") == "@chan"
    assert (
        force_join._coerce_required_channel("-1001234567890")
        == "-1001234567890"
    )


def test_coerce_required_channel_rejects_non_string():
    assert force_join._coerce_required_channel(123) is None
    assert force_join._coerce_required_channel(None) is None
    assert force_join._coerce_required_channel(True) is None
    assert force_join._coerce_required_channel(False) is None


def test_coerce_required_channel_rejects_over_cap():
    """Raw input longer than the cap is rejected (returns None) — the
    cap check happens BEFORE canonicalisation."""
    too_long = "@" + ("x" * 64)  # 65 chars total
    assert force_join._coerce_required_channel(too_long) is None


# ---------- override set / clear / get ----------


def test_set_required_channel_override_changes_get_required_channel(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    assert force_join.get_required_channel() == "@env_chan"
    force_join.set_required_channel_override("@db_chan")
    assert force_join.get_required_channel() == "@db_chan"
    assert force_join.get_required_channel_override() == "@db_chan"


def test_set_required_channel_override_empty_string_forces_off(
    _reset_required_channel_override, monkeypatch,
):
    """The empty-string override forces the gate OFF even when env is set."""
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    force_join.set_required_channel_override("")
    assert force_join.get_required_channel() == ""
    # ``""`` is distinct from ``None`` here — there IS an override active.
    assert force_join.get_required_channel_override() == ""


def test_clear_required_channel_override_falls_back_to_env(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    force_join.set_required_channel_override("@override")
    force_join.clear_required_channel_override()
    assert force_join.get_required_channel() == "@env_chan"
    assert force_join.get_required_channel_override() is None


def test_clear_required_channel_override_returns_had_value(
    _reset_required_channel_override,
):
    force_join.set_required_channel_override("@chan")
    assert force_join.clear_required_channel_override() is True
    assert force_join.clear_required_channel_override() is False


def test_set_required_channel_override_rejects_bool(
    _reset_required_channel_override,
):
    with pytest.raises(ValueError):
        force_join.set_required_channel_override(True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        force_join.set_required_channel_override(False)  # type: ignore[arg-type]


def test_set_required_channel_override_rejects_over_cap(
    _reset_required_channel_override,
):
    too_long = "@" + ("x" * 64)
    with pytest.raises(ValueError):
        force_join.set_required_channel_override(too_long)


def test_set_required_channel_override_canonicalises_input(
    _reset_required_channel_override,
):
    """Bare handles get the ``@`` prefix; numeric IDs pass through."""
    force_join.set_required_channel_override("MyChan")
    assert force_join.get_required_channel() == "@MyChan"
    force_join.set_required_channel_override("-1001234567890")
    assert force_join.get_required_channel() == "-1001234567890"


# ---------- source reporting ----------


def test_get_required_channel_source_default_when_unset(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.delenv("REQUIRED_CHANNEL", raising=False)
    assert force_join.get_required_channel_source() == "default"


def test_get_required_channel_source_env_when_env_set(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    assert force_join.get_required_channel_source() == "env"


def test_get_required_channel_source_db_when_override_set(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    force_join.set_required_channel_override("@db_chan")
    assert force_join.get_required_channel_source() == "db"


def test_get_required_channel_source_db_for_force_off_override(
    _reset_required_channel_override, monkeypatch,
):
    """A force-OFF override (`""`) is still ``source=db`` — that's
    operator intent, not "no override"."""
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    force_join.set_required_channel_override("")
    assert force_join.get_required_channel_source() == "db"


# ---------- refresh_required_channel_override_from_db ----------


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_loads_value(
    _reset_required_channel_override,
):
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="@db_chan")
    loaded = await force_join.refresh_required_channel_override_from_db(db)
    assert loaded == "@db_chan"
    assert force_join.get_required_channel() == "@db_chan"
    db.get_setting.assert_awaited_once_with(
        force_join.REQUIRED_CHANNEL_SETTING_KEY,
    )


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_loads_force_off(
    _reset_required_channel_override, monkeypatch,
):
    """A stored empty string IS persisted as the force-OFF override."""
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    db = MagicMock()
    db.get_setting = AsyncMock(return_value="")
    loaded = await force_join.refresh_required_channel_override_from_db(db)
    assert loaded == ""
    assert force_join.get_required_channel_override() == ""
    assert force_join.get_required_channel() == ""


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_clears_when_row_missing(
    _reset_required_channel_override,
):
    force_join.set_required_channel_override("@chan")
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)
    loaded = await force_join.refresh_required_channel_override_from_db(db)
    assert loaded is None
    assert force_join.get_required_channel_override() is None


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_keeps_cache_on_error(
    _reset_required_channel_override,
):
    """A transient DB blip must NOT clear an active override."""
    force_join.set_required_channel_override("@chan")
    db = MagicMock()
    db.get_setting = AsyncMock(side_effect=RuntimeError("DB down"))
    loaded = await force_join.refresh_required_channel_override_from_db(db)
    assert loaded == "@chan"
    assert force_join.get_required_channel_override() == "@chan"


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_handles_none_db(
    _reset_required_channel_override,
):
    force_join.set_required_channel_override("@chan")
    loaded = await force_join.refresh_required_channel_override_from_db(None)
    assert loaded == "@chan"
    assert force_join.get_required_channel_override() == "@chan"


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_rejects_malformed(
    _reset_required_channel_override,
):
    """A malformed (non-string) DB row clears the override rather than
    poisoning it."""
    force_join.set_required_channel_override("@chan")
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=12345)  # not a string
    loaded = await force_join.refresh_required_channel_override_from_db(db)
    assert loaded is None
    assert force_join.get_required_channel_override() is None


@pytest.mark.asyncio
async def test_refresh_required_channel_override_from_db_rejects_over_cap(
    _reset_required_channel_override,
):
    db = MagicMock()
    too_long = "@" + ("x" * 64)
    db.get_setting = AsyncMock(return_value=too_long)
    loaded = await force_join.refresh_required_channel_override_from_db(db)
    assert loaded is None


# ---------- resolution-order pin ----------


def test_resolution_order_override_beats_env(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    force_join.set_required_channel_override("@db_chan")
    assert force_join.get_required_channel() == "@db_chan"


def test_resolution_order_env_beats_default(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.setenv("REQUIRED_CHANNEL", "@env_chan")
    assert force_join.get_required_channel() == "@env_chan"


def test_resolution_order_default_when_neither_set(
    _reset_required_channel_override, monkeypatch,
):
    monkeypatch.delenv("REQUIRED_CHANNEL", raising=False)
    assert force_join.get_required_channel() == ""
