"""Stage-13-Step-A: required-channel subscription gate.

Some operators want to require every user to be a member of an
announcement channel before the bot becomes interactive — a common
growth pattern for Telegram bots in the FA / commercial space. This
module owns that gate.

The contract:

* When ``REQUIRED_CHANNEL`` is unset (default), the gate is a no-op
  and every handler runs as before. **Full back-compat** for existing
  deploys that don't want this feature.

* When ``REQUIRED_CHANNEL`` is set to a public channel handle
  (``@MeowAssist_Channel``) or numeric chat id (``-1001234567890``),
  every incoming :class:`Message` and :class:`CallbackQuery` is
  intercepted by the :class:`RequiredChannelMiddleware` *before* the
  router dispatches it. If the user is not a member of the channel,
  the middleware sends / edits a "Please join" screen with a deep
  link to the channel and a ``✅ I've joined`` button that re-checks
  membership.

* Admins (``ADMIN_USER_IDS``) are always allowed through — operators
  must not lock themselves out of their own bot.

* The gate is *fail-open* on Telegram-API errors. If the bot is not
  yet an admin of the channel, or the channel handle is wrong, or
  the API returns ``Bad Request: chat not found`` /
  ``user not found``, we log a one-shot WARNING and let the user
  through. The alternative — failing closed — would brick every user
  on a misconfiguration the operator may not even realise they have.

* The "I've joined" button (``force_join_check`` callback) is the
  one piece of UI that bypasses the gate during dispatch — the
  middleware skips it and the dedicated handler does the membership
  re-check itself, then either drops the user at the hub or
  re-renders the join screen with the ``not_yet`` flash.

* Telegram channel-membership semantics (``ChatMember.status``):
  ``"creator"`` / ``"administrator"`` / ``"member"`` → joined.
  ``"restricted"`` is joined iff ``is_member`` is True. ``"left"``
  / ``"kicked"`` → not joined. We accept the four "joined" statuses
  and reject everything else, matching the Telegram Bot API spec.

The middleware is registered AFTER ``UserUpsertMiddleware`` so the
``users`` row exists before we render any localised text (the gate
uses ``strings.t`` which reads the user's preferred language).

Per §11 of HANDOFF.md the user wants a real bug fix bundled in every
PR; that fix lives in :mod:`handlers` (``_hub_text_and_kb`` NaN
guard) and is documented separately — this module is the feature.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    TelegramObject,
)

log = logging.getLogger("bot.force_join")


# Status values that count as "joined" per the Bot API spec.
# https://core.telegram.org/bots/api#chatmember
_JOINED_STATUSES: frozenset[str] = frozenset(
    {"creator", "administrator", "member"}
)
# ``restricted`` is only joined when ``is_member`` is True; handled
# separately because the predicate isn't a pure status compare.

# Callback data the "✅ I've joined" button posts back. Kept short
# (Telegram caps callback_data at 64 bytes) and namespaced so it
# can never collide with a future hub callback.
FORCE_JOIN_CHECK_CALLBACK = "force_join_check"

# Stage-15-Step-E #10b row 5: DB-backed override layer for
# ``REQUIRED_CHANNEL``. Same overlay shape COST_MARKUP / MIN_TOPUP_USD
# already use:
#
# 1. ``_REQUIRED_CHANNEL_OVERRIDE`` — process-local cache, populated
#    from ``system_settings.REQUIRED_CHANNEL`` via
#    :func:`refresh_required_channel_override_from_db` at boot and
#    on every ``/admin/control`` render. The web admin form writes
#    this row so an operator can re-target the force-join gate
#    without a redeploy. ``None`` means "no DB override".
# 2. ``REQUIRED_CHANNEL`` env var — same shape as before; remains the
#    fallback for staging deploys that prefer env-only config.
# 3. ``""`` (gate disabled) — compile-time fallback.
#
# The override is the canonicalised channel handle string (already run
# through :func:`_normalise_channel`), so the read path stays a plain
# attribute lookup. The empty-string sentinel inside the override slot
# means "actively force the gate off" — distinct from ``None`` ("no
# DB row, fall through to env"). That distinction lets an operator
# turn off a force-join gate that's only configured in env without
# editing the env file.
REQUIRED_CHANNEL_SETTING_KEY: str = "REQUIRED_CHANNEL"
# Cap on the channel-handle field length. Telegram public usernames
# are 5–32 chars (with the ``@``); a numeric chat id is at most ~16
# chars (``-100`` + 13-digit channel id). 64 is comfortably above
# both and matches the cap on ``set_admin_role`` notes (which the
# DB-layer NUL-strip applies the same defensive trim to).
REQUIRED_CHANNEL_MAX_LENGTH: int = 64
_REQUIRED_CHANNEL_OVERRIDE: str | None = None


def _normalise_channel(raw: str) -> str:
    """Canonicalise a ``REQUIRED_CHANNEL`` candidate.

    Pulled out so the env path, the DB path, and the web admin form
    validator all share the exact same rules. Returns ``""`` for any
    blank input — the caller decides whether ``""`` means "gate off"
    or "fall through to the next layer".

    * ``""`` / whitespace-only → ``""``.
    * ``@username`` → ``"@username"``.
    * Bare ``username`` → ``"@username"``.
    * Numeric ``-100…`` chat id → returned as-is.
    * Anything longer than :data:`REQUIRED_CHANNEL_MAX_LENGTH` is
      clamped to ``""`` (defence in depth — a 1 MB form field would
      otherwise sail through as a "valid" handle).
    """
    if not isinstance(raw, str):
        return ""
    candidate = raw.strip()
    if not candidate:
        return ""
    if len(candidate) > REQUIRED_CHANNEL_MAX_LENGTH:
        return ""
    # Numeric chat id (private channel / supergroup). Allow a leading
    # ``-`` so ``-1001234567890`` parses without forcing the operator
    # to wrap it in quotes.
    stripped = candidate.lstrip("-")
    if stripped.isdigit():
        return candidate
    if not candidate.startswith("@"):
        return "@" + candidate
    return candidate


def _coerce_required_channel(value: object) -> str | None:
    """Validate a ``REQUIRED_CHANNEL`` candidate for the override slot.

    Returns the canonical form on success, or ``None`` if the value
    is unusable (non-string, too long, blank). Never raises — the
    caller (the web admin form, the DB warm-up, the ``set_*`` helper)
    decides how to surface a rejection.

    Note the ``""`` empty-string IS a valid override value here — it
    represents "force the gate OFF even if env is set". The override
    slot stores the empty string verbatim in that case; only ``None``
    means "no override at all".
    """
    if isinstance(value, bool):
        return None
    if not isinstance(value, str):
        return None
    if len(value) > REQUIRED_CHANNEL_MAX_LENGTH:
        return None
    return _normalise_channel(value)


def set_required_channel_override(value: str) -> None:
    """Replace the in-process REQUIRED_CHANNEL override.

    Validates against :func:`_coerce_required_channel`. Refuses
    non-string / over-cap input with a :class:`ValueError` so a buggy
    caller can't smuggle in a bool or a 1 MB handle.
    """
    global _REQUIRED_CHANNEL_OVERRIDE
    coerced = _coerce_required_channel(value)
    if coerced is None:
        raise ValueError(
            f"REQUIRED_CHANNEL override {value!r} must be a string up "
            f"to {REQUIRED_CHANNEL_MAX_LENGTH} chars"
        )
    _REQUIRED_CHANNEL_OVERRIDE = coerced


def clear_required_channel_override() -> bool:
    """Drop the in-process override. Returns True if one was active."""
    global _REQUIRED_CHANNEL_OVERRIDE
    had = _REQUIRED_CHANNEL_OVERRIDE is not None
    _REQUIRED_CHANNEL_OVERRIDE = None
    return had


def get_required_channel_override() -> str | None:
    """Return the current in-process override (or ``None``)."""
    return _REQUIRED_CHANNEL_OVERRIDE


async def refresh_required_channel_override_from_db(db) -> str | None:
    """Reload the override from the ``system_settings`` overlay.

    Mirrors :func:`payments.refresh_min_topup_override_from_db`: a
    transient DB error keeps the previous cache in place so a pool
    blip can't accidentally revert to env / default mid-incident.
    A malformed stored value (non-string / over-cap) is treated as
    "no override" rather than crashing the bot.
    """
    global _REQUIRED_CHANNEL_OVERRIDE
    if db is None:
        return _REQUIRED_CHANNEL_OVERRIDE
    try:
        raw = await db.get_setting(REQUIRED_CHANNEL_SETTING_KEY)
    except Exception:
        log.exception(
            "refresh_required_channel_override_from_db: get_setting "
            "failed; keeping previous cache value=%r",
            _REQUIRED_CHANNEL_OVERRIDE,
        )
        return _REQUIRED_CHANNEL_OVERRIDE
    if raw is None:
        _REQUIRED_CHANNEL_OVERRIDE = None
        return None
    coerced = _coerce_required_channel(raw)
    if coerced is None:
        log.warning(
            "refresh_required_channel_override_from_db: rejected stored "
            "value %r; clearing override",
            raw,
        )
        _REQUIRED_CHANNEL_OVERRIDE = None
        return None
    _REQUIRED_CHANNEL_OVERRIDE = coerced
    return coerced


def get_required_channel() -> str:
    """Return the configured ``REQUIRED_CHANNEL`` value, normalised.

    Resolution order (Stage-15-Step-E #10b row 5):

    1. ``_REQUIRED_CHANNEL_OVERRIDE`` (DB ``system_settings`` row).
    2. ``REQUIRED_CHANNEL`` env var.
    3. ``""`` (gate disabled).

    * Empty / unset → ``""`` (gate disabled).
    * ``@username`` → ``"@username"`` (canonical Telegram public form).
    * Bare ``username`` → ``"@username"`` (we add the ``@`` so the
      operator doesn't have to remember the convention).
    * Numeric ``-100…`` chat id → returned as-is for use with
      ``bot.get_chat_member(chat_id=…)``.

    The override slot is the source of truth even when its value is
    the empty string — that lets an operator force the gate OFF on a
    deploy whose env var is set.

    We don't validate the value beyond canonicalisation — Telegram
    will surface a misconfiguration as a ``Bad Request`` from
    ``get_chat_member``, which the middleware turns into a logged
    fail-open.
    """
    if _REQUIRED_CHANNEL_OVERRIDE is not None:
        return _REQUIRED_CHANNEL_OVERRIDE
    return _normalise_channel(os.getenv("REQUIRED_CHANNEL", ""))


def get_required_channel_source() -> str:
    """Return ``db`` / ``env`` / ``default`` for the resolved value.

    Mirrors :func:`payments.get_min_topup_source` — the ``/admin/control``
    panel uses this badge to show operators where the live value is
    coming from.

    ``"db"`` is returned even when the override slot is the empty
    string, because that's still an operator-applied decision (force
    the gate off). ``"env"`` only fires when there's no DB row AND
    the env var resolves to a non-empty canonical value.
    """
    if _REQUIRED_CHANNEL_OVERRIDE is not None:
        return "db"
    env_value = _normalise_channel(os.getenv("REQUIRED_CHANNEL", ""))
    if env_value:
        return "env"
    return "default"


def get_required_channel_invite_link() -> str:
    """Return the operator-supplied invite URL, or ``""``.

    Used when ``REQUIRED_CHANNEL`` is a numeric id (private channel)
    — the ``Join`` button needs an explicit URL because Telegram
    can't deep-link ``-1001234567890`` directly. For public ``@handle``
    channels we synthesise ``https://t.me/handle`` automatically and
    this env var is optional.
    """
    return os.getenv("REQUIRED_CHANNEL_INVITE_LINK", "").strip()


def build_join_url(channel: str) -> str:
    """Return a clickable join URL for *channel*.

    * ``REQUIRED_CHANNEL_INVITE_LINK`` overrides everything (operator
      may want a join-request link for a private channel).
    * ``@handle`` → ``https://t.me/handle``.
    * Numeric id with no override → ``""`` (the gate falls back to
      a text-only prompt; the operator should set the override to
      restore the button).
    """
    override = get_required_channel_invite_link()
    if override:
        return override
    if channel.startswith("@"):
        return "https://t.me/" + channel[1:]
    return ""


def is_joined_status(status: str | None, is_member: bool | None) -> bool:
    """Decide whether a ``ChatMember`` status counts as joined.

    Pulled out so the middleware and the explicit re-check handler
    share the same predicate (and so tests can pin every branch
    without spinning up an aiogram event loop).
    """
    if status is None:
        return False
    if status in _JOINED_STATUSES:
        return True
    if status == "restricted":
        return bool(is_member)
    return False


async def user_is_member(bot: Any, channel: str, user_id: int) -> bool | None:
    """Return True/False if we determined membership, ``None`` if the
    Telegram API returned an error we treat as "fail open".

    The ``None`` sentinel matters: the middleware uses it to log a
    WARNING and let the user through, instead of either crashing the
    handler chain or silently rejecting every user on a
    misconfiguration. ``False`` means we got a definitive answer that
    they aren't a member; the caller is expected to render the join
    screen.
    """
    try:
        chat_member = await bot.get_chat_member(
            chat_id=channel, user_id=user_id
        )
    except TelegramBadRequest as exc:
        # ``Bad Request: chat not found`` / ``user not found`` /
        # ``PARTICIPANT_ID_INVALID`` — the operator hasn't added the
        # bot to the channel as an admin yet, or the handle is
        # wrong. Don't brick every user; log loud, fail open.
        log.warning(
            "force-join: get_chat_member failed for channel=%r user_id=%s: %s "
            "(failing OPEN — verify the bot is admin of the required channel)",
            channel,
            user_id,
            exc,
        )
        return None
    except TelegramAPIError as exc:
        # Network blip / rate limit / 5xx — same fail-open policy
        # so a transient outage doesn't lock every user out.
        log.warning(
            "force-join: get_chat_member API error for channel=%r user_id=%s: %s",
            channel,
            user_id,
            exc,
        )
        return None
    status = getattr(chat_member, "status", None)
    is_member_attr = getattr(chat_member, "is_member", None)
    return is_joined_status(status, is_member_attr)


def build_join_keyboard(
    join_url: str, lang: str
) -> InlineKeyboardMarkup:
    """Build the "Join" + "✅ I've joined" inline keyboard.

    The Join button is omitted when ``join_url`` is empty (numeric
    channel id without an explicit ``REQUIRED_CHANNEL_INVITE_LINK``
    override) — the user still gets the re-check button so they can
    proceed once they've found the channel through whatever
    operator-provided side channel the deploy uses.
    """
    # Lazy-import to avoid a circular import: ``strings`` doesn't
    # import this module, but importing at module top would force
    # ``force_join`` to be loadable before the strings table is
    # populated, which complicates the strings tests.
    from strings import t  # noqa: PLC0415

    rows: list[list[InlineKeyboardButton]] = []
    if join_url:
        rows.append([
            InlineKeyboardButton(
                text=t(lang, "btn_force_join_join"),
                url=join_url,
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text=t(lang, "btn_force_join_check"),
            callback_data=FORCE_JOIN_CHECK_CALLBACK,
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


class RequiredChannelMiddleware(BaseMiddleware):
    """Block every handler when the user hasn't joined the required
    channel.

    Registered as an *outer* middleware on both ``dp.message`` and
    ``dp.callback_query`` — same wiring shape as
    :class:`UserUpsertMiddleware`. Order matters: this middleware
    runs AFTER ``UserUpsertMiddleware`` so the ``users`` row (and
    therefore the user's preferred language) is available when we
    render the join screen.

    The middleware short-circuits when:

    1. ``REQUIRED_CHANNEL`` is unset (gate disabled).
    2. The event has no ``from_user`` (anonymous group admin /
       channel post — same edge case ``UserUpsertMiddleware``
       handles).
    3. The user is in ``ADMIN_USER_IDS`` (operator can never lock
       themselves out).
    4. The event is the ``force_join_check`` callback (handled by
       its own dedicated handler so the middleware doesn't loop).

    Otherwise it asks Telegram for the membership status, and:

    * ``True``  → call the next handler unchanged.
    * ``None``  → fail open (log warning, call the next handler).
    * ``False`` → render the join screen and STOP the chain.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        channel = get_required_channel()
        if not channel:
            return await handler(event, data)

        from_user = getattr(event, "from_user", None)
        if from_user is None or from_user.id is None:
            return await handler(event, data)

        # Operator escape hatch — admins never see the gate.
        from admin import is_admin  # noqa: PLC0415

        if is_admin(from_user.id):
            return await handler(event, data)

        # The "✅ I've joined" callback re-checks membership itself;
        # if the middleware also intercepted it, the user could never
        # exit the gate.
        if isinstance(event, CallbackQuery) and event.data == FORCE_JOIN_CHECK_CALLBACK:
            return await handler(event, data)

        bot = getattr(event, "bot", None) or data.get("bot")
        if bot is None:
            # No bot reference — the dispatcher would crash a few
            # lines later anyway. Fail open + warn so we don't lose
            # the entire update.
            log.warning(
                "force-join: no bot on event/data; failing OPEN for user_id=%s",
                from_user.id,
            )
            return await handler(event, data)

        joined = await user_is_member(bot, channel, from_user.id)
        if joined is True or joined is None:
            return await handler(event, data)

        # Definitively not a member. Render the join screen and
        # stop the chain.
        await render_join_prompt(event, channel)
        return None


async def render_join_prompt(
    event: Message | CallbackQuery,
    channel: str,
    *,
    not_yet: bool = False,
) -> None:
    """Send / edit the join screen for *event*.

    * For a :class:`Message`, sends a fresh bubble with the join
      keyboard.
    * For a :class:`CallbackQuery`, edits the message the callback
      came from so the chat history doesn't bloat with one bubble
      per re-check tap.
    * ``not_yet=True`` swaps the title for the "still not joined"
      flash so a user tapping ``✅ I've joined`` before actually
      joining gets a clearer signal than the generic prompt.
    """
    from strings import t  # noqa: PLC0415

    from_user = event.from_user
    lang = await _user_lang(from_user.id if from_user else None)

    join_url = build_join_url(channel)
    text_key = "force_join_not_yet" if not_yet else "force_join_text"
    text = t(lang, text_key, channel=channel)
    kb = build_join_keyboard(join_url, lang)

    if isinstance(event, CallbackQuery):
        # ``answer`` first so the spinner stops even if the edit
        # racing-no-ops (e.g. user double-taps and the second tap
        # arrives after we've already rendered the same prompt).
        try:
            await event.answer()
        except TelegramAPIError:
            log.debug("force-join: callback.answer() failed", exc_info=True)
        if event.message is None:
            return
        try:
            await event.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            # ``message is not modified`` — already showing the
            # exact same prompt. No-op.
            log.debug(
                "force-join: edit_text was a no-op", exc_info=True
            )
        return

    # Message branch.
    try:
        await event.answer(text, reply_markup=kb)
    except TelegramAPIError:
        log.warning(
            "force-join: failed to send join prompt to user_id=%s",
            from_user.id if from_user else None,
            exc_info=True,
        )


async def _user_lang(user_id: int | None) -> str:
    """Resolve the user's preferred language for the join screen.

    Imports lazily to dodge a circular import (``database`` itself
    is fine, but the middleware module has to be importable before
    the DB pool is open during boot — tests stub the DB rather than
    spinning one up).
    """
    from strings import DEFAULT_LANGUAGE, SUPPORTED_LANGUAGES  # noqa: PLC0415

    if user_id is None:
        return DEFAULT_LANGUAGE
    try:
        from database import db  # noqa: PLC0415

        lang = await db.get_user_language(user_id)
    except Exception:
        log.debug(
            "force-join: get_user_language failed for user_id=%s",
            user_id,
            exc_info=True,
        )
        return DEFAULT_LANGUAGE
    if lang in SUPPORTED_LANGUAGES:
        return lang
    return DEFAULT_LANGUAGE


async def force_join_check_callback(callback: CallbackQuery) -> None:
    """Handler for the ``force_join_check`` callback button.

    Re-checks membership; on success drops the user at the hub, on
    failure re-renders the join screen with the ``not_yet`` flash.

    Wired into the public router so the middleware's escape hatch
    has somewhere to land. We deliberately don't put this on the
    admin router — admins skip the gate entirely so they'd never
    see this button.
    """
    channel = get_required_channel()
    if not channel:
        # Gate was disabled between the original prompt render and
        # the re-check tap. Just drop the user at the hub.
        await _drop_at_hub(callback)
        return

    if callback.from_user is None:
        try:
            await callback.answer()
        except TelegramAPIError:
            pass
        return

    bot = callback.bot
    if bot is None:
        log.warning(
            "force-join: callback has no bot reference for user_id=%s",
            callback.from_user.id,
        )
        try:
            await callback.answer()
        except TelegramAPIError:
            pass
        return

    joined = await user_is_member(bot, channel, callback.from_user.id)
    if joined is True or joined is None:
        # Joined (or fail-open) → let them through.
        await _drop_at_hub(callback)
        return

    # Still not joined → re-render with the not-yet flash.
    await render_join_prompt(callback, channel, not_yet=True)


async def _drop_at_hub(callback: CallbackQuery) -> None:
    """Edit the join-screen bubble into the hub view.

    Imports ``handlers`` lazily — both modules can otherwise import
    each other and we'd have a circular reference at boot.
    """
    from handlers import _edit_to_hub, _get_user_language  # noqa: PLC0415

    if callback.from_user is None:
        try:
            await callback.answer()
        except TelegramAPIError:
            pass
        return

    lang = await _get_user_language(callback.from_user.id)
    try:
        await callback.answer()
    except TelegramAPIError:
        pass
    await _edit_to_hub(callback, lang)
