"""Tests for ``vision`` (Stage-15-Step-E #10 first slice).

Pure-helper coverage. No Telegram, no aiohttp, no DB — every
function under test is a regular sync function that takes bytes
or strings and returns strings or dicts. The future PR that
wires the photo handler + ``ai_engine.chat_with_model`` will
add integration tests on top.
"""

from __future__ import annotations

import base64
import importlib

import pytest

import vision


# ─── is_vision_capable_model ────────────────────────────────────


@pytest.mark.parametrize(
    "model_id",
    [
        "openai/gpt-4-vision-preview",
        "openai/gpt-4-turbo",
        "openai/gpt-4o",
        "openai/gpt-4o-mini",
        "openai/o1",
        "openai/o1-mini",
        "openai/chatgpt-4o-latest",
        "anthropic/claude-3-haiku",
        "anthropic/claude-3-sonnet",
        "anthropic/claude-3-opus",
        "anthropic/claude-3.5-sonnet",
        "anthropic/claude-3.7-sonnet",
        "google/gemini-1.5-pro",
        "google/gemini-1.5-flash",
        "google/gemini-2.0-flash-exp",
        "google/gemini-pro-vision",
        "meta-llama/llama-3.2-90b-vision-instruct",
        "meta-llama/llama-3.2-11b-vision-instruct",
        "mistralai/pixtral-12b",
        "qwen/qwen-vl-plus",
        "qwen/qwen2-vl-72b-instruct",
        "qwen/qwen2.5-vl-7b-instruct",
        # Case-insensitive — same id but uppercased.
        "OPENAI/GPT-4O",
        "Anthropic/Claude-3-Opus",
        # "vision" wildcard escape hatch — catches future slugs.
        "some-future-vendor/cool-vision-model-v9",
    ],
)
def test_is_vision_capable_model_recognises_known_vision_ids(
    model_id: str,
):
    assert vision.is_vision_capable_model(model_id) is True


@pytest.mark.parametrize(
    "model_id",
    [
        "openai/gpt-3.5-turbo",
        "openai/gpt-4",  # plain gpt-4, not -turbo / -vision / -o
        "openai/gpt-4-32k",
        "anthropic/claude-2",
        "anthropic/claude-2.1",
        "anthropic/claude-instant-1",
        "google/gemini-1.0-pro",  # only 1.5+ has vision
        "meta-llama/llama-3-70b-instruct",  # base llama-3, not 3.2-vision
        "meta-llama/llama-3.1-70b-instruct",
        "mistralai/mistral-large",
        "qwen/qwen-72b-chat",
        "deepseek/deepseek-chat",
        "x-ai/grok-2",  # text-only at the time of writing
    ],
)
def test_is_vision_capable_model_rejects_known_text_only_ids(
    model_id: str,
):
    assert vision.is_vision_capable_model(model_id) is False


@pytest.mark.parametrize(
    "bad_input",
    [
        None,
        "",
        "   ",
        123,
        [],
        {},
    ],
)
def test_is_vision_capable_model_treats_invalid_input_as_non_vision(
    bad_input,
):
    """Empty / non-string / corrupted ``users.active_model`` rows
    must NOT crash the upstream handler — return False so the
    safe text-only fallback fires."""
    assert vision.is_vision_capable_model(bad_input) is False


# ─── encode_image_data_uri ──────────────────────────────────────


def _fake_jpeg(size: int = 16) -> bytes:
    """Return ``size`` bytes of pseudo-JPEG content. Test uses a
    constant byte pattern so the base64 prefix is predictable."""
    return b"\xff\xd8\xff\xe0" + b"A" * (size - 4) if size >= 4 else b"\xff" * size


def test_encode_image_data_uri_happy_path_jpeg():
    img = _fake_jpeg(16)
    encoded = vision.encode_image_data_uri(img, "image/jpeg")
    assert encoded.startswith("data:image/jpeg;base64,")
    # Decode the payload and verify it round-trips.
    payload = encoded.removeprefix("data:image/jpeg;base64,")
    assert base64.b64decode(payload) == img


@pytest.mark.parametrize(
    "mime",
    ["image/jpeg", "image/png", "image/gif", "image/webp"],
)
def test_encode_image_data_uri_accepts_all_documented_mime_types(
    mime: str,
):
    encoded = vision.encode_image_data_uri(_fake_jpeg(64), mime)
    assert encoded.startswith(f"data:{mime};base64,")


def test_encode_image_data_uri_normalises_mime_case_and_whitespace():
    encoded = vision.encode_image_data_uri(_fake_jpeg(64), "  IMAGE/JPEG  ")
    assert encoded.startswith("data:image/jpeg;base64,")


def test_encode_image_data_uri_rejects_empty_bytes():
    with pytest.raises(vision.VisionError) as exc_info:
        vision.encode_image_data_uri(b"", "image/jpeg")
    assert exc_info.value.reason == "empty_image"


def test_encode_image_data_uri_rejects_oversize():
    """One byte over the cap → reject. Cap-exact is fine."""
    over = b"x" * (vision.MAX_IMAGE_BYTES + 1)
    with pytest.raises(vision.VisionError) as exc_info:
        vision.encode_image_data_uri(over, "image/jpeg")
    assert exc_info.value.reason == "oversize_image"


def test_encode_image_data_uri_accepts_exactly_max_bytes():
    at_cap = b"x" * vision.MAX_IMAGE_BYTES
    encoded = vision.encode_image_data_uri(at_cap, "image/jpeg")
    assert encoded.startswith("data:image/jpeg;base64,")


@pytest.mark.parametrize(
    "bad_mime",
    ["image/heic", "image/svg+xml", "image/avif", "text/plain", "", "   ", "image/jpg"],
)
def test_encode_image_data_uri_rejects_unsupported_mime(bad_mime: str):
    with pytest.raises(vision.VisionError) as exc_info:
        vision.encode_image_data_uri(_fake_jpeg(64), bad_mime)
    assert exc_info.value.reason == "unsupported_mime"


@pytest.mark.parametrize(
    "bad_input",
    [None, "not-bytes", 123, [b"x"], {"k": b"v"}],
)
def test_encode_image_data_uri_rejects_non_bytes_input(bad_input):
    with pytest.raises(vision.VisionError) as exc_info:
        vision.encode_image_data_uri(bad_input, "image/jpeg")
    assert exc_info.value.reason == "invalid_input"


def test_encode_image_data_uri_accepts_bytearray():
    """``bytearray`` is bytes-like and Telegram's
    ``Bot.download(...)`` returns it. Must accept without
    forcing the caller to ``bytes(...)``."""
    encoded = vision.encode_image_data_uri(
        bytearray(_fake_jpeg(64)), "image/png",
    )
    assert encoded.startswith("data:image/png;base64,")


# ─── build_multimodal_user_message ─────────────────────────────


def _stub_uri(idx: int = 0) -> str:
    """Return a syntactically-valid data URI suitable for the
    structural checks. Doesn't have to decode to a real image
    because the function under test only inspects the prefix."""
    payload = base64.b64encode(f"img{idx}".encode()).decode("ascii")
    return f"data:image/jpeg;base64,{payload}"


def test_build_multimodal_user_message_happy_path_text_plus_one_image():
    msg = vision.build_multimodal_user_message(
        "what's in this picture?", [_stub_uri(0)],
    )
    assert msg["role"] == "user"
    assert isinstance(msg["content"], list)
    assert len(msg["content"]) == 2
    # Text part comes first by convention.
    assert msg["content"][0] == {
        "type": "text", "text": "what's in this picture?",
    }
    # Image part second.
    assert msg["content"][1]["type"] == "image_url"
    assert msg["content"][1]["image_url"]["url"] == _stub_uri(0)


def test_build_multimodal_user_message_strips_prompt_whitespace():
    msg = vision.build_multimodal_user_message(
        "  hello  \n", [_stub_uri()],
    )
    assert msg["content"][0]["text"] == "hello"


def test_build_multimodal_user_message_text_only_no_images():
    """Empty image list with non-empty prompt is allowed —
    degrades to a text-only user message. Useful for the
    "user attached a photo with caption, then we strip the
    photo because it's a HEIC the bot can't handle" path."""
    msg = vision.build_multimodal_user_message("just text", [])
    assert msg == {"role": "user", "content": [{"type": "text", "text": "just text"}]}


def test_build_multimodal_user_message_image_only_no_prompt():
    """Empty prompt with non-empty image list is allowed — the
    user sent a photo with no caption and the model should infer
    intent."""
    msg = vision.build_multimodal_user_message("", [_stub_uri()])
    assert msg["role"] == "user"
    # No text part at all when the prompt is empty.
    assert len(msg["content"]) == 1
    assert msg["content"][0]["type"] == "image_url"


def test_build_multimodal_user_message_multiple_images_preserve_order():
    """When the user sends a multi-photo album, ordering must
    survive the call so "the first one is the cat, the second
    is the dog" still makes sense."""
    uris = [_stub_uri(i) for i in range(3)]
    msg = vision.build_multimodal_user_message("describe these", uris)
    image_parts = [c for c in msg["content"] if c["type"] == "image_url"]
    assert [p["image_url"]["url"] for p in image_parts] == uris


def test_build_multimodal_user_message_rejects_empty_message():
    with pytest.raises(vision.VisionError) as exc_info:
        vision.build_multimodal_user_message("", [])
    assert exc_info.value.reason == "empty_message"


def test_build_multimodal_user_message_rejects_whitespace_only_prompt_with_no_images():
    """``"   "`` strips to ``""`` — same as truly empty."""
    with pytest.raises(vision.VisionError) as exc_info:
        vision.build_multimodal_user_message("   \n\t", [])
    assert exc_info.value.reason == "empty_message"


def test_build_multimodal_user_message_rejects_too_many_images():
    too_many = [_stub_uri(i) for i in range(vision.MAX_IMAGES_PER_MESSAGE + 1)]
    with pytest.raises(vision.VisionError) as exc_info:
        vision.build_multimodal_user_message("hi", too_many)
    assert exc_info.value.reason == "too_many_images"


def test_build_multimodal_user_message_accepts_exactly_max_images():
    at_cap = [_stub_uri(i) for i in range(vision.MAX_IMAGES_PER_MESSAGE)]
    msg = vision.build_multimodal_user_message("hi", at_cap)
    image_parts = [c for c in msg["content"] if c["type"] == "image_url"]
    assert len(image_parts) == vision.MAX_IMAGES_PER_MESSAGE


@pytest.mark.parametrize(
    "bad_uri",
    [
        "https://example.com/foo.jpg",  # HTTP URL, not a data URI
        "data:text/plain;base64,SGVsbG8=",  # not image
        "data:image/jpeg,raw_bytes_no_base64",  # missing ;base64,
        "",
        "   ",
        None,
        123,
    ],
)
def test_build_multimodal_user_message_rejects_invalid_image_uri(bad_uri):
    with pytest.raises(vision.VisionError) as exc_info:
        vision.build_multimodal_user_message("hi", [bad_uri])
    assert exc_info.value.reason == "invalid_image_uri"


def test_build_multimodal_user_message_rejects_non_list_image_data_uris():
    with pytest.raises(vision.VisionError) as exc_info:
        # ``str`` is iterable but not a list — guard it.
        vision.build_multimodal_user_message("hi", "data:image/jpeg;base64,xx")  # type: ignore[arg-type]
    assert exc_info.value.reason == "invalid_input"


@pytest.mark.parametrize(
    "bad_prompt",
    [
        {"role": "user"},
        ["fragment", "fragment"],
        42,
        3.14,
        b"bytes-prompt",
        object(),
    ],
)
def test_build_multimodal_user_message_rejects_non_string_prompt(bad_prompt):
    """Bug fix bundle (Stage-15-Step-E #6 follow-up #2): a non-string
    truthy ``prompt`` used to slip past ``(prompt or "").strip()`` and
    crash with ``AttributeError`` instead of the documented
    ``VisionError`` contract. With the type guard at the top of the
    function, every bad shape now produces a clean
    ``VisionError(reason="invalid_input")``.
    """
    with pytest.raises(vision.VisionError) as exc_info:
        vision.build_multimodal_user_message(bad_prompt, [_stub_uri()])  # type: ignore[arg-type]
    assert exc_info.value.reason == "invalid_input"
    assert "prompt" in str(exc_info.value).lower()


def test_build_multimodal_user_message_accepts_none_prompt_with_image():
    """``None`` prompt is the documented "image-only" calling
    convention; it must keep working after the type-guard tightening.
    """
    msg = vision.build_multimodal_user_message(None, [_stub_uri()])  # type: ignore[arg-type]
    assert msg["role"] == "user"
    # No text part should be emitted for an empty / None prompt.
    assert all(part.get("type") != "text" for part in msg["content"])
    assert sum(1 for p in msg["content"] if p.get("type") == "image_url") == 1


# ─── Env-driven module-level constants ─────────────────────────


def test_max_image_bytes_env_override(monkeypatch):
    """``VISION_MAX_IMAGE_BYTES`` set at import time wins over
    the default. Re-import the module under monkeypatch'd env."""
    monkeypatch.setenv("VISION_MAX_IMAGE_BYTES", "1048576")  # 1 MiB
    reloaded = importlib.reload(vision)
    try:
        assert reloaded.MAX_IMAGE_BYTES == 1024 * 1024
    finally:
        # Reload back to defaults so subsequent tests see the
        # canonical 5 MiB cap.
        monkeypatch.delenv("VISION_MAX_IMAGE_BYTES", raising=False)
        importlib.reload(vision)


def test_max_image_bytes_unparseable_env_falls_back_to_default(
    monkeypatch, caplog,
):
    monkeypatch.setenv("VISION_MAX_IMAGE_BYTES", "not-a-number")
    with caplog.at_level("WARNING"):
        reloaded = importlib.reload(vision)
    try:
        assert reloaded.MAX_IMAGE_BYTES == 5 * 1024 * 1024
        assert any("not parseable" in r.message for r in caplog.records)
    finally:
        monkeypatch.delenv("VISION_MAX_IMAGE_BYTES", raising=False)
        importlib.reload(vision)


def test_max_image_bytes_below_minimum_clamps(monkeypatch, caplog):
    """1 KiB minimum — ``VISION_MAX_IMAGE_BYTES=512`` clamps."""
    monkeypatch.setenv("VISION_MAX_IMAGE_BYTES", "512")
    with caplog.at_level("WARNING"):
        reloaded = importlib.reload(vision)
    try:
        assert reloaded.MAX_IMAGE_BYTES == 1024
        assert any("clamping to minimum" in r.message for r in caplog.records)
    finally:
        monkeypatch.delenv("VISION_MAX_IMAGE_BYTES", raising=False)
        importlib.reload(vision)


def test_max_images_per_message_env_override(monkeypatch):
    monkeypatch.setenv("VISION_MAX_IMAGES_PER_MESSAGE", "10")
    reloaded = importlib.reload(vision)
    try:
        assert reloaded.MAX_IMAGES_PER_MESSAGE == 10
    finally:
        monkeypatch.delenv("VISION_MAX_IMAGES_PER_MESSAGE", raising=False)
        importlib.reload(vision)
