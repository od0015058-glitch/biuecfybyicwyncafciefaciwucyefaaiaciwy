"""Stage-15-Step-E #10 follow-up: image-as-document rejection handler.

Pins:

* ``F.document`` matches ANY document — the handler must filter
  to ``mime_type`` starting with ``image/`` and pass-through
  everything else (PDFs, archives, audio) silently so a future
  document handler can be added without colliding.
* The reply uses the localised ``ai_image_document_instruction``
  slug; the user gets the actionable "re-send as photo" hint.
* Rate-limit gate: per-user chat-token bucket exhausts to a
  silent drop (NOT a "rate-limited" reply), to avoid noise on
  top of an already-throttled chat session.
* ``from_user is None`` (anonymous-admin / channel forward edge
  cases) drops silently the same way the photo handler does.
* Defence-in-depth: ``message.document = None`` (theoretically
  not possible behind the ``F.document`` filter, but cheap to
  guard) drops silently rather than crashing.

All tests mock the network surface: no real Telegram CDN
fetch, no DB pool — just the handler logic.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_document_message(
    user_id: int = 8888,
    mime_type: str | None = "image/heic",
    file_name: str | None = "IMG_0001.HEIC",
    document_present: bool = True,
) -> MagicMock:
    """Build a minimal ``Message`` mock with a ``document`` attribute
    so ``process_image_document`` accepts it.

    ``mime_type`` defaults to HEIC (the iPhone case the handler is
    primarily designed for). Pass other mimes via the parameter to
    exercise the various branches.
    """
    msg = MagicMock()
    msg.text = None
    msg.caption = None
    msg.from_user = MagicMock()
    msg.from_user.id = user_id
    msg.chat = MagicMock()
    msg.chat.id = user_id
    if document_present:
        document = MagicMock()
        document.mime_type = mime_type
        document.file_name = file_name
        document.file_id = "doc_file_id"
        msg.document = document
    else:
        msg.document = None
    msg.bot = MagicMock()
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
# Happy path: HEIC / HEIF / WEBP / PNG / GIF / TIFF / SVG / AVIF / BMP
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "mime_type",
    [
        "image/heic",
        "image/heif",
        "image/png",
        "image/webp",
        "image/jpeg",
        "image/gif",
        "image/tiff",
        "image/svg+xml",
        "image/avif",
        "image/bmp",
        "image/x-icon",
        # Mixed-case + whitespace — Telegram has been observed to
        # send mime types verbatim from the client OS, which on
        # some Android distros uppercases the type.
        "Image/HEIC",
        "  image/png  ",
    ],
)
@pytest.mark.asyncio
async def test_process_image_document_replies_for_image_mimes(mime_type):
    """Every ``image/*`` document mime type should trigger the
    instructional reply, regardless of casing or surrounding
    whitespace."""
    import handlers

    msg = _make_document_message(mime_type=mime_type)
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_image_document(msg)
    msg.answer.assert_awaited_once()
    body = msg.answer.await_args.args[0]
    # Sanity: the message we send is the localised instruction
    # slug, not a raw key fall-through.
    assert "Photo" in body or "File" in body


@pytest.mark.asyncio
async def test_process_image_document_uses_persian_for_persian_user():
    """A user with ``language_code='fa'`` must receive the
    Persian copy of the instruction string."""
    import handlers

    msg = _make_document_message(mime_type="image/heic")
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers._get_user_language", AsyncMock(return_value="fa")),
    ):
        await handlers.process_image_document(msg)
    body = msg.answer.await_args.args[0]
    # The Persian string carries Persian characters; assert
    # we're not silently falling through to English copy.
    assert "تصویر" in body or "عکس" in body


# ---------------------------------------------------------------------
# Pass-through paths: non-image documents are NOT replied to
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "mime_type",
    [
        "application/pdf",
        "application/zip",
        "application/x-tar",
        "application/json",
        "audio/mpeg",
        "video/mp4",
        "text/plain",
        "application/octet-stream",
        "",  # Telegram occasionally sends docs with no detected mime
        None,  # ``mime_type`` field absent entirely
    ],
)
@pytest.mark.asyncio
async def test_process_image_document_passthrough_for_non_image_mimes(
    mime_type,
):
    """Non-image document mime types must NOT consume a chat
    token and must NOT send a reply — they're "pass-through" so
    a future PDF / audio handler can pick them up.

    Pre-fix this handler caught any ``F.document`` and replied
    with the image instruction even for PDFs, which would have
    been actively misleading to a user trying to send a doc to
    a future feature.
    """
    import handlers

    msg = _make_document_message(mime_type=mime_type)
    consume_mock = AsyncMock(return_value=True)
    with (
        patch("handlers.consume_chat_token", consume_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_image_document(msg)
    consume_mock.assert_not_awaited()
    msg.answer.assert_not_awaited()


# ---------------------------------------------------------------------
# Defensive paths: drop silently
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_image_document_drops_when_from_user_is_none():
    """Anonymous-admin / channel forwards arrive with
    ``from_user=None`` — handler must drop silently rather than
    crashing on ``message.from_user.id`` (mirrors the
    ``process_photo`` defensive shape)."""
    import handlers

    msg = _make_document_message(mime_type="image/heic")
    msg.from_user = None
    consume_mock = AsyncMock(return_value=True)
    with (
        patch("handlers.consume_chat_token", consume_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_image_document(msg)
    consume_mock.assert_not_awaited()
    msg.answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_image_document_drops_when_document_is_none():
    """``F.document`` should already filter this out, but if a
    future aiogram-filter regression changes the matching
    semantics, the handler must not trip on
    ``getattr(None, "mime_type", ...)``."""
    import handlers

    msg = _make_document_message(document_present=False)
    consume_mock = AsyncMock(return_value=True)
    with (
        patch("handlers.consume_chat_token", consume_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_image_document(msg)
    consume_mock.assert_not_awaited()
    msg.answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_image_document_rate_limited_drops_silently():
    """Token bucket exhausted — handler must drop silently
    (NOT send another rate-limited reply on top of an already-
    throttled chat). The user has already learned from their
    text-chat throttle that they're rate-limited; doubling up
    would just be noise."""
    import handlers

    msg = _make_document_message(mime_type="image/heic")
    with (
        patch(
            "handlers.consume_chat_token", AsyncMock(return_value=False)
        ),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_image_document(msg)
    msg.answer.assert_not_awaited()


# ---------------------------------------------------------------------
# Token-bucket interaction: the consume_chat_token gate fires
# AFTER the mime filter. A user spamming PDFs (which pass through)
# must NOT have their chat-token bucket drained.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_image_document_pdf_does_not_drain_chat_token():
    """A PDF (non-image) must pass through without consuming a
    chat token. Pre-fix, draining the token at the top of the
    handler would penalise a user trying to send a PDF to a
    future doc handler — they'd find their text-chat budget
    silently halved by their failed PDF upload."""
    import handlers

    msg = _make_document_message(mime_type="application/pdf")
    consume_mock = AsyncMock(return_value=True)
    with (
        patch("handlers.consume_chat_token", consume_mock),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
    ):
        await handlers.process_image_document(msg)
    consume_mock.assert_not_awaited()
    msg.answer.assert_not_awaited()


# ---------------------------------------------------------------------
# Caption-on-document: the user's caption is dropped along with
# the document. We don't reply to the caption text; the
# instruction reply is the only output.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_image_document_ignores_caption():
    """A user sending a HEIC file with a caption ("what's in this
    pic?") gets the instruction message — NOT a chat reply to
    the caption. Once they re-send as a photo with the same
    caption, ``process_photo`` will pick it up properly."""
    import handlers

    msg = _make_document_message(mime_type="image/heic")
    msg.caption = "what's in this picture?"
    chat_mock = AsyncMock()
    with (
        patch("handlers.consume_chat_token", AsyncMock(return_value=True)),
        patch("handlers._get_user_language", AsyncMock(return_value="en")),
        patch("handlers.chat_with_model", chat_mock),
    ):
        await handlers.process_image_document(msg)
    chat_mock.assert_not_awaited()
    msg.answer.assert_awaited_once()
    body = msg.answer.await_args.args[0]
    # Should NOT echo the caption back to the user — the reply
    # is the static instruction string only.
    assert "what's in this picture" not in body.lower()


# ---------------------------------------------------------------------
# String registration: the slug must exist in both supported
# locales so the t() lookup never falls through to a bare slug.
# ---------------------------------------------------------------------


def test_ai_image_document_instruction_string_present_in_both_locales():
    """``ai_image_document_instruction`` must be defined for
    both ``fa`` and ``en`` so a user in either locale gets a
    real translation, not the bare slug."""
    from strings import _STRINGS, SUPPORTED_LANGUAGES

    for lang in SUPPORTED_LANGUAGES:
        assert "ai_image_document_instruction" in _STRINGS[lang], (
            f"locale {lang!r} is missing the "
            f"ai_image_document_instruction slug"
        )
        body = _STRINGS[lang]["ai_image_document_instruction"]
        # Sanity: each locale's body is non-trivial and at
        # least mentions a "photo" or "عکس" concept.
        assert len(body) > 30
        assert "Photo" in body or "عکس" in body or "photo" in body
