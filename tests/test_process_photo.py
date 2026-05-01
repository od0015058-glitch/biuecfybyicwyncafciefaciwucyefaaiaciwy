"""Stage-15-Step-E #10 second slice: photo / vision handler.

Pins:

* The handler downloads the largest ``PhotoSize``, encodes it as a
  data URI, and routes to ``chat_with_model`` with
  ``image_data_uris=[uri]``.
* Pre-flight gates fire in the right order:
  rate-limit → in-flight slot → user-row lookup → vision-capability
  pre-check → download → encode → chat_with_model.
* Each failure mode (download fail, oversize image, non-vision
  model, missing from_user, no user row) surfaces a localised
  message with **no** wallet impact.

We mock aiogram's ``Bot.get_file`` and ``Bot.download_file`` so the
tests don't touch Telegram. The image bytes used for happy-path
tests are a 14-byte JPEG-marker prefix — enough to satisfy
``vision.encode_image_data_uri``'s non-empty / size-cap checks.
"""

from __future__ import annotations

import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


_TINY_JPEG = bytes.fromhex(
    "ffd8ffe000104a46494600010101006000600000",  # JFIF header
)


def _make_photo_message(
    user_id: int = 7777,
    caption: str | None = None,
    photo_present: bool = True,
) -> MagicMock:
    """Build a minimal ``Message`` mock with a ``photo`` attribute
    so ``process_photo`` accepts it."""
    msg = MagicMock()
    msg.text = None
    msg.caption = caption
    msg.from_user = MagicMock()
    msg.from_user.id = user_id
    msg.chat = MagicMock()
    msg.chat.id = user_id
    if photo_present:
        # Two PhotoSize objects ordered smallest→largest;
        # process_photo picks the last one.
        small = MagicMock()
        small.file_id = "small_id"
        small.width = 90
        small.height = 90
        big = MagicMock()
        big.file_id = "big_id"
        big.width = 1200
        big.height = 900
        msg.photo = [small, big]
    else:
        msg.photo = []
    msg.bot = MagicMock()
    msg.bot.send_chat_action = AsyncMock()
    file_obj = MagicMock()
    file_obj.file_path = "photos/1234.jpg"
    msg.bot.get_file = AsyncMock(return_value=file_obj)

    async def _download_file(file_path, destination):
        # aiogram's API contract: write into destination (BytesIO),
        # then seek back to start.
        destination.write(_TINY_JPEG)
        destination.seek(0)
        return destination

    msg.bot.download_file = AsyncMock(side_effect=_download_file)
    msg.answer = AsyncMock()
    return msg


@pytest.fixture(autouse=True)
def _clean_inflight_slots():
    """Each test starts with an empty in-flight set."""
    from rate_limit import reset_chat_inflight_slots_for_tests

    reset_chat_inflight_slots_for_tests()
    yield
    reset_chat_inflight_slots_for_tests()


# ---------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_photo_routes_to_chat_with_model_with_image_uri():
    """Vision-capable user sends a photo with caption — handler
    must download, encode, and call ``chat_with_model`` with the
    keyword ``image_data_uris=[uri]``. The caption is the prompt."""
    import handlers

    msg = _make_photo_message(caption="what's in this picture?")
    captured: dict = {}

    async def _stub_chat(user_id, prompt, **kwargs):
        captured["user_id"] = user_id
        captured["prompt"] = prompt
        captured["image_data_uris"] = kwargs.get("image_data_uris")
        return "I see a JFIF marker."

    user_row = {
        "active_model": "openai/gpt-4o",  # vision-capable
        "language_code": "en",
    }

    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(side_effect=_stub_chat)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    assert captured["user_id"] == 7777
    assert captured["prompt"] == "what's in this picture?"
    uris = captured["image_data_uris"]
    assert isinstance(uris, list) and len(uris) == 1
    assert uris[0].startswith("data:image/jpeg;base64,")
    msg.answer.assert_awaited_once_with("I see a JFIF marker.")


@pytest.mark.asyncio
async def test_process_photo_picks_largest_photo_size():
    """The handler must call ``get_file`` with the LAST PhotoSize's
    file_id (largest variant) — pre-fix a careless `[0]` would
    have grabbed the smallest, costing image quality at no
    bandwidth saving (we're already paying for the AI call)."""
    import handlers

    msg = _make_photo_message()
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(return_value="ok")),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    msg.bot.get_file.assert_awaited_once_with("big_id")


@pytest.mark.asyncio
async def test_process_photo_no_caption_passes_empty_prompt():
    """A photo without a caption is allowed — the multimodal
    helper accepts an empty text part as long as there's at
    least one image."""
    import handlers

    msg = _make_photo_message(caption=None)
    captured: dict = {}

    async def _stub_chat(user_id, prompt, **kwargs):
        captured["prompt"] = prompt
        return "image-only reply"

    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(side_effect=_stub_chat)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    assert captured["prompt"] == ""


# ---------------------------------------------------------------------
# Pre-flight rejection paths
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_photo_drops_when_from_user_is_none():
    """Anonymous-admin or sender_chat-only forwards arrive with
    ``from_user=None`` — handler must drop silently rather than
    crashing on ``message.from_user.id``."""
    import handlers

    msg = _make_photo_message()
    msg.from_user = None
    chat_mock = AsyncMock()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", chat_mock),
    ):
        await handlers.process_photo(msg)
    chat_mock.assert_not_awaited()
    msg.answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_photo_rate_limited_replies_local_rate_limited():
    """Token bucket exhausted — user sees ``ai_local_rate_limited``
    and ``chat_with_model`` is never called."""
    import handlers

    msg = _make_photo_message()
    chat_mock = AsyncMock()
    with (
        patch(
            "handlers.consume_chat_token", AsyncMock(return_value=False)
        ),
        patch("handlers.chat_with_model", chat_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_photo(msg)
    chat_mock.assert_not_awaited()
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_photo_busy_replies_chat_busy():
    """Slot already taken — handler must reject with
    ``ai_chat_busy`` and not call ``chat_with_model``."""
    import handlers
    from rate_limit import try_claim_chat_slot

    # Pre-claim the slot to simulate a still-in-flight request.
    assert await try_claim_chat_slot(7777) is True

    msg = _make_photo_message()
    chat_mock = AsyncMock()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", chat_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_photo(msg)

    chat_mock.assert_not_awaited()
    sent = msg.answer.await_args.args[0]
    assert "still being processed" in sent.lower()


@pytest.mark.asyncio
async def test_process_photo_no_user_row_returns_no_account():
    """User who hasn't /started — handler hits the
    ``ai_no_account`` localised message and skips
    download/encode/chat_with_model."""
    import handlers

    msg = _make_photo_message()
    chat_mock = AsyncMock()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", chat_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=None)),
    ):
        await handlers.process_photo(msg)

    chat_mock.assert_not_awaited()
    msg.bot.get_file.assert_not_awaited()
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_photo_non_vision_model_short_circuits():
    """Pre-flight vision-capability check must reject a text-only
    active_model BEFORE the Telegram CDN download, so the user
    gets the actionable error immediately and we save the round-
    trip for an obviously-doomed turn."""
    import handlers

    msg = _make_photo_message()
    user_row = {
        "active_model": "openai/gpt-3.5-turbo",  # NOT vision-capable
        "language_code": "en",
    }
    chat_mock = AsyncMock()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", chat_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    # Download was NOT attempted — pre-flight gate fired.
    msg.bot.get_file.assert_not_awaited()
    msg.bot.download_file.assert_not_awaited()
    chat_mock.assert_not_awaited()
    sent = msg.answer.await_args.args[0]
    assert "vision" in sent.lower()


@pytest.mark.asyncio
async def test_process_photo_download_failure_surfaces_localised_error():
    """Telegram CDN returns no bytes (file expired, link broken).
    Handler must surface ``ai_image_download_failed`` and skip
    the OpenRouter call entirely."""
    import handlers

    msg = _make_photo_message()
    # Override download_file to write nothing (zero-byte buffer).
    async def _empty_download(file_path, destination):
        destination.seek(0)
        return destination

    msg.bot.download_file = AsyncMock(side_effect=_empty_download)
    chat_mock = AsyncMock()
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", chat_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    chat_mock.assert_not_awaited()
    msg.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_photo_oversize_image_surfaces_localised_error():
    """An image that exceeds ``vision.MAX_IMAGE_BYTES`` after
    download must be rejected with ``ai_image_oversize`` and the
    chat call skipped — no wallet impact."""
    import handlers

    msg = _make_photo_message()

    # Override download_file to write a buffer larger than the cap.
    async def _huge_download(file_path, destination):
        # Write 6 MiB — exceeds the 5 MiB default cap.
        destination.write(b"\xff" * (6 * 1024 * 1024))
        destination.seek(0)
        return destination

    msg.bot.download_file = AsyncMock(side_effect=_huge_download)
    chat_mock = AsyncMock()
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", chat_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    chat_mock.assert_not_awaited()
    sent = msg.answer.await_args.args[0]
    # The Persian / English oversize string contains a sizeable hint.
    assert "large" in sent.lower() or "بزرگ" in sent


# ---------------------------------------------------------------------
# Slot bookkeeping
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_photo_releases_slot_on_success():
    """The slot must be released after a normal handler return so
    the user can send another photo or text turn."""
    import handlers
    from rate_limit import try_claim_chat_slot

    msg = _make_photo_message()
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(return_value="ok")),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    # Slot is now free.
    assert await try_claim_chat_slot(7777) is True


@pytest.mark.asyncio
async def test_process_photo_releases_slot_on_exception():
    """If anything inside the handler's try block raises, the slot
    must still be released via the finally."""
    import handlers
    from rate_limit import try_claim_chat_slot

    msg = _make_photo_message()
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch(
            "handlers.chat_with_model",
            AsyncMock(side_effect=RuntimeError("boom")),
        ),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        with pytest.raises(RuntimeError):
            await handlers.process_photo(msg)

    assert await try_claim_chat_slot(7777) is True


# ---------------------------------------------------------------------
# Chunked reply send (mirror of process_chat behaviour)
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_photo_long_reply_is_chunked():
    """An AI reply over the per-message char cap must be split
    into multiple ``message.answer`` calls — same chunker as
    process_chat to keep behaviour aligned."""
    import handlers

    msg = _make_photo_message()
    big_reply = "a paragraph.\n\n" + ("x" * 5000)
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(return_value=big_reply)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    # At least 2 chunks for a 5000+ char reply (Telegram cap is 4096).
    assert msg.answer.await_count >= 2


@pytest.mark.asyncio
async def test_process_photo_empty_reply_falls_back_to_provider_unavailable():
    """Empty / None reply from ``chat_with_model`` (e.g. upstream
    refusal) must surface ``ai_provider_unavailable`` rather than
    sending an empty Telegram message that the API would reject."""
    import handlers

    msg = _make_photo_message()
    user_row = {
        "active_model": "openai/gpt-4o",
        "language_code": "en",
    }
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers.chat_with_model", AsyncMock(return_value="")),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.db.get_user", AsyncMock(return_value=user_row)),
    ):
        await handlers.process_photo(msg)

    msg.answer.assert_awaited_once()
    sent = msg.answer.await_args.args[0]
    assert sent  # non-empty fallback string


# ---------------------------------------------------------------------
# Helper pinning
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_download_photo_to_bytes_returns_none_on_telegram_api_error():
    """``_download_photo_to_bytes`` must catch
    :class:`aiogram.exceptions.TelegramAPIError` and return None
    so the handler can render the localised error rather than
    crashing the poller."""
    from aiogram.exceptions import TelegramAPIError

    import handlers

    msg = _make_photo_message()
    msg.bot.get_file = AsyncMock(side_effect=TelegramAPIError(method=None, message="x"))
    out = await handlers._download_photo_to_bytes(msg)
    assert out is None


@pytest.mark.asyncio
async def test_download_photo_to_bytes_returns_none_on_no_file_path():
    """Telegram occasionally returns a ``File`` with no
    ``file_path`` (file too large per Telegram API). Handler
    must treat that as a download failure."""
    import handlers

    msg = _make_photo_message()
    file_obj = MagicMock()
    file_obj.file_path = None
    msg.bot.get_file = AsyncMock(return_value=file_obj)
    out = await handlers._download_photo_to_bytes(msg)
    assert out is None


@pytest.mark.asyncio
async def test_download_photo_to_bytes_no_photo_returns_none():
    """Defense-in-depth: a Message that somehow lacks ``photo``
    (filter mismatch, future routing change) returns None
    rather than crashing."""
    import handlers

    msg = _make_photo_message(photo_present=False)
    out = await handlers._download_photo_to_bytes(msg)
    assert out is None


@pytest.mark.asyncio
async def test_download_photo_to_bytes_happy_path_returns_bytes():
    """Sanity: when Telegram cooperates we get the raw bytes that
    were written into the buffer."""
    import handlers

    msg = _make_photo_message()
    out = await handlers._download_photo_to_bytes(msg)
    assert out == _TINY_JPEG
