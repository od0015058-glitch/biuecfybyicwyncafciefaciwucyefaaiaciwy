"""Vision / multimodal helpers (Stage-15-Step-E #10 first slice).

This module is the **foundation layer** for the upcoming
"image / vision model support" feature listed in the Stage-15-Step-E
roadmap (HANDOFF table row 10). It deliberately ships zero
Telegram-handler integration and zero ``ai_engine.chat_with_model``
integration in this PR — those are the two surfaces that need to
land *together* in the next slice (a partial integration would
render in production as "user sends a photo, bot ignores it"
which is worse than the current "user sends a photo, bot replies
to the caption text only" because it'd silently drop unique
information). The pure helpers here are split off so they can:

* be exercised in isolation by ``tests/test_vision.py`` (no aiogram
  test rig, no aiohttp, no DB), keeping this PR small and CI-fast;
* be imported and unit-tested by future PRs that wire the handler
  + ``ai_engine`` paths;
* document the OpenRouter multimodal payload shape inline so a
  future contributor doesn't have to re-derive it from the spec.

The five public surfaces:

* ``VisionError`` — all failures the caller cares about. Raised by
  ``encode_image_data_uri`` (oversize, bad MIME) and
  ``build_multimodal_user_message`` (empty / oversize image list).
* ``MAX_IMAGE_BYTES`` — module-level cap on the encoded payload
  size. 5 MiB by default, sized to comfortably fit a Telegram
  photo (Telegram's own limit is 10 MiB but their re-compression
  for the photo-message channel almost always lands under 1 MiB).
  Configurable via ``VISION_MAX_IMAGE_BYTES`` env at import time
  with the same per-loop ``_parse_positive_int_env``-style guard
  the other modules use; an unparseable / non-positive value is
  logged at WARNING and falls back to the 5 MiB default rather
  than crashing the import.
* ``MAX_IMAGES_PER_MESSAGE`` — cap on how many images can ride
  along with one user message. 4 by default (matches Anthropic's
  Claude limit, which is the strictest of the major vision
  providers OpenRouter routes to). Same env-overridable pattern.
* ``encode_image_data_uri(image_bytes, content_type)`` — pure
  function returning ``"data:<mime>;base64,..."``. The OpenAI
  multimodal spec accepts both bare HTTP(S) URLs and inline data
  URIs; the bot uses data URIs because (a) Telegram file-server
  URLs require the bot's own auth token to fetch and OpenRouter
  has no way to pass that, and (b) data URIs avoid any external
  dependency on Telegram's CDN being reachable from the
  OpenRouter datacentre.
* ``build_multimodal_user_message(prompt, image_data_uris)`` —
  pure function returning the OpenAI/OpenRouter multimodal user-
  message dict, ready to drop into the ``messages`` array.
* ``is_vision_capable_model(model_id)`` — heuristic check based
  on model id substrings. Conservative: any known-vision id
  pattern returns True; anything else returns False so a future
  user-with-vision-disabled-model gets a clean "this model can't
  see images" message instead of a wasted OpenRouter call that
  burns tokens producing "I'm sorry, I can't see images".

The shape returned by ``build_multimodal_user_message`` matches
the OpenAI / OpenRouter chat-completions schema exactly::

    {
        "role": "user",
        "content": [
            {"type": "text", "text": "what's in this picture?"},
            {"type": "image_url",
             "image_url": {"url": "data:image/jpeg;base64,/9j/..."}},
        ],
    }

A subsequent PR will (a) import ``encode_image_data_uri`` from
the photo-message handler, (b) call ``is_vision_capable_model``
in ``ai_engine.chat_with_model`` to short-circuit unsupported
models with a localised reply, and (c) call
``build_multimodal_user_message`` to assemble the upstream
payload when the user's active model is vision-capable.
"""

from __future__ import annotations

import base64
import logging
import os

log = logging.getLogger("bot.vision")


class VisionError(Exception):
    """Raised when the caller passes invalid input to a vision helper.

    Carrying a ``reason`` slug (machine-readable) and a
    human-readable message is the same shape the existing
    ``ZarinpalError`` / ``TetraPayError`` use, so a future caller
    that wants to bump a per-process drop counter or emit a
    Prometheus label can read ``err.reason`` directly without
    string-parsing the message.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(f"[{reason}] {message}")
        self.reason = reason
        self.message = message


# ─── Module-level limits ─────────────────────────────────────────


def _parse_positive_int_env(
    name: str, default: int, *, minimum: int = 1
) -> int:
    """Tolerantly parse a positive-int env var.

    Mirrors the pattern in ``model_discovery._parse_positive_int_env``
    /  ``pending_expiration._read_int_env`` exactly so a future
    refactor can collapse them into a shared module without
    behavioural drift. The contract: blank / unset → ``default``;
    unparseable → ``default`` with WARNING; below ``minimum`` →
    clamped to ``minimum`` with WARNING; otherwise the parsed
    value is returned.
    """
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        log.warning(
            "vision: env %s=%r is not parseable as int; "
            "falling back to default=%d",
            name, raw, default,
        )
        return default
    if parsed < minimum:
        log.warning(
            "vision: env %s=%d is below the minimum=%d; "
            "clamping to minimum",
            name, parsed, minimum,
        )
        return minimum
    return parsed


# 5 MiB. Telegram caps photo messages at ~10 MiB and
# re-compresses to <1 MiB on the photo-message channel, but a
# direct ``send_document`` of an image can ride higher. We keep
# the cap conservative because the encoded data URI grows by
# 4/3 (base64 expansion) before being shipped to OpenRouter;
# 5 MiB raw → 6.7 MiB on the wire, well under any reasonable
# upstream body limit.
MAX_IMAGE_BYTES = _parse_positive_int_env(
    "VISION_MAX_IMAGE_BYTES", default=5 * 1024 * 1024, minimum=1024,
)

# 4 images per message. OpenAI's GPT-4V allows up to 10 in
# practice; Anthropic Claude allows 5; Google Gemini allows 16;
# Mistral allows up to 8. We pick the strictest cap so any
# vision-capable model OpenRouter routes to accepts the payload.
# Override with ``VISION_MAX_IMAGES_PER_MESSAGE`` if a deploy
# only ever uses a more permissive provider.
MAX_IMAGES_PER_MESSAGE = _parse_positive_int_env(
    "VISION_MAX_IMAGES_PER_MESSAGE", default=4, minimum=1,
)


# ─── Vision-capable model detection ──────────────────────────────


# OpenRouter exposes hundreds of model ids and the set churns
# weekly. We can't enumerate every one. Instead we maintain a
# conservative list of substring patterns that map to known
# vision-capable model families. The check is case-insensitive
# and substring-based so e.g. ``openai/gpt-4-vision-preview``,
# ``openai/gpt-4o``, and ``openai/gpt-4o-mini`` all match the
# ``gpt-4o`` / ``gpt-4-vision`` patterns.
#
# Patterns are deliberately broad — false-positive (claim vision
# support, get a 400 from OpenRouter) is recoverable (the
# existing 400-handling branch in ``ai_engine`` already shows
# the user "ai_provider_unavailable" and lets them retry); but
# false-negative (refuse to send the image to a model that
# actually supports vision) is silently lossy. When in doubt,
# include the pattern.
_VISION_MODEL_PATTERNS: tuple[str, ...] = (
    # OpenAI vision tier
    "gpt-4-vision",
    "gpt-4-turbo",       # vision since 2024-04
    "gpt-4o",
    "o1",                # o1 + o1-mini both support vision
    "chatgpt-4o",
    # Anthropic Claude 3 family — every Claude 3 variant is
    # vision-capable (Haiku / Sonnet / Opus / Claude 3.5 + 3.7).
    "claude-3",
    "claude-3.5",
    "claude-3.7",
    "claude-3-haiku",
    "claude-3-sonnet",
    "claude-3-opus",
    # Google Gemini family — 1.5+ supports vision; 1.0 was text-
    # only. Match on ``gemini-1.5`` / ``gemini-pro-vision`` /
    # ``gemini-2`` / ``gemini-flash``.
    "gemini-1.5",
    "gemini-2",
    "gemini-pro-vision",
    "gemini-flash",
    # Meta Llama 3.2 vision tier (90B and 11B variants).
    "llama-3.2-vision",
    "llama-3.2-11b-vision",
    "llama-3.2-90b-vision",
    # Mistral Pixtral.
    "pixtral",
    # Qwen-VL family.
    "qwen-vl",
    "qwen2-vl",
    "qwen2.5-vl",
    # Internal/wildcard escape hatch — model ids that explicitly
    # carry "vision" in their slug. Catches future families we
    # haven't seen yet without requiring a code edit.
    "vision",
)


def is_vision_capable_model(model_id: str) -> bool:
    """Return True iff ``model_id`` is in the known-vision set.

    Case-insensitive substring match against
    ``_VISION_MODEL_PATTERNS``. Empty / non-string input returns
    False rather than raising so a corrupted ``users.active_model``
    row (NULL, whitespace, etc.) doesn't crash the upstream
    handler — the vision-only path is opted into, the text-only
    fallback is the safe default.
    """
    if not isinstance(model_id, str):
        return False
    needle = model_id.strip().lower()
    if not needle:
        return False
    return any(pattern in needle for pattern in _VISION_MODEL_PATTERNS)


# ─── Image encoding ─────────────────────────────────────────────


# Whitelist of acceptable image content types. OpenAI's
# multimodal docs list ``image/jpeg``, ``image/png``,
# ``image/gif``, ``image/webp`` as the supported set; OpenRouter
# documents the same. Other mime types (``image/heic``, SVG, AVIF)
# get rejected so we don't burn tokens on a 400 the user has no
# way to debug.
_ALLOWED_IMAGE_MIME_TYPES: frozenset[str] = frozenset({
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
})


def encode_image_data_uri(
    image_bytes: bytes, content_type: str = "image/jpeg",
) -> str:
    """Encode raw image bytes as an OpenAI-compatible data URI.

    Returns ``"data:<content_type>;base64,<b64_payload>"``.

    Raises:
        VisionError(reason="empty_image", ...): zero-length input.
        VisionError(reason="oversize_image", ...): over
            ``MAX_IMAGE_BYTES``.
        VisionError(reason="unsupported_mime", ...): the content
            type is not in the OpenAI-documented allowlist.
        VisionError(reason="invalid_input", ...): non-bytes /
            non-bytearray input.

    The size check fires on the RAW byte count, not on the
    base64-encoded length. This keeps the cap predictable from
    the caller's side (where the bytes came from a Telegram
    download); OpenRouter sees a ~33% larger payload over the
    wire, which is still well within any reasonable HTTP body
    limit.
    """
    if not isinstance(image_bytes, (bytes, bytearray)):
        raise VisionError(
            "invalid_input",
            f"image_bytes must be bytes-like, got {type(image_bytes).__name__}",
        )
    if len(image_bytes) == 0:
        raise VisionError("empty_image", "image_bytes is empty")
    if len(image_bytes) > MAX_IMAGE_BYTES:
        raise VisionError(
            "oversize_image",
            f"image_bytes is {len(image_bytes)} bytes, "
            f"max is {MAX_IMAGE_BYTES}",
        )
    mime = (content_type or "").strip().lower()
    if mime not in _ALLOWED_IMAGE_MIME_TYPES:
        raise VisionError(
            "unsupported_mime",
            f"content_type {content_type!r} is not one of "
            f"{sorted(_ALLOWED_IMAGE_MIME_TYPES)}",
        )
    encoded = base64.b64encode(bytes(image_bytes)).decode("ascii")
    return f"data:{mime};base64,{encoded}"


# ─── Multimodal message assembly ────────────────────────────────


def build_multimodal_user_message(
    prompt: str, image_data_uris: list[str],
) -> dict:
    """Assemble the OpenAI/OpenRouter multimodal user-message dict.

    Returns the exact shape OpenRouter expects::

        {
            "role": "user",
            "content": [
                {"type": "text", "text": "<prompt>"},
                {"type": "image_url",
                 "image_url": {"url": "data:image/jpeg;base64,..."}},
                ...
            ],
        }

    The text part comes first by convention — most providers
    treat ordering as informative ("here's the question, then
    here's the image to look at"), and the OpenAI docs example
    shows text-first. ``prompt`` may be empty (caller is
    expected to pass at least one image in that case); a fully
    empty content array is rejected because some providers
    return 400 on it.

    Raises:
        VisionError(reason="empty_message", ...): both prompt
            and image list are empty.
        VisionError(reason="too_many_images", ...): more than
            ``MAX_IMAGES_PER_MESSAGE`` data URIs.
        VisionError(reason="invalid_image_uri", ...): a list
            entry is not a non-empty ``data:image/...;base64,``
            URI string (caught early so a typo doesn't reach
            OpenRouter as a wasted token-burn 400).
    """
    text_part = (prompt or "").strip()
    if not isinstance(image_data_uris, list):
        raise VisionError(
            "invalid_input",
            f"image_data_uris must be a list, got "
            f"{type(image_data_uris).__name__}",
        )
    if not text_part and not image_data_uris:
        raise VisionError(
            "empty_message",
            "must provide at least one of prompt or image_data_uris",
        )
    if len(image_data_uris) > MAX_IMAGES_PER_MESSAGE:
        raise VisionError(
            "too_many_images",
            f"{len(image_data_uris)} images supplied, max is "
            f"{MAX_IMAGES_PER_MESSAGE}",
        )
    for idx, uri in enumerate(image_data_uris):
        if not isinstance(uri, str) or not uri.startswith("data:image/"):
            raise VisionError(
                "invalid_image_uri",
                f"image_data_uris[{idx}] is not a "
                f"data:image/...;base64,... URI: {uri!r:.80}",
            )
        # Cheap structural check — a base64 data URI must contain
        # ``;base64,`` after the mime type. Catches ``data:image/
        # jpeg,...`` (URL-encoded, no base64) which would also
        # 400 at OpenRouter.
        if ";base64," not in uri:
            raise VisionError(
                "invalid_image_uri",
                f"image_data_uris[{idx}] missing ;base64, "
                f"separator: {uri!r:.80}",
            )

    content: list[dict] = []
    if text_part:
        content.append({"type": "text", "text": text_part})
    for uri in image_data_uris:
        content.append({"type": "image_url", "image_url": {"url": uri}})
    return {"role": "user", "content": content}
