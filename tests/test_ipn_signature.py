"""Tests for the NowPayments IPN signature verifier (payments.verify_ipn_signature).

This is the first automated test in the repo. Money-touching code paths
(webhook → finalize → wallet credit) get tested first — see HANDOFF.md §8
for the rest of the queue (P3-Op-3 will scaffold pytest + CI properly).

Run from the repo root:

    pip install -r requirements-dev.txt
    pytest tests/

We intentionally don't import any aiogram / asyncpg / aiohttp internals
here so the test can run in a fresh venv without a database or a bot
token. The verifier itself only depends on hashlib + hmac + json.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys

# Make the repo root importable when pytest is invoked from a subdir.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from payments import (  # noqa: E402  (sys.path manipulation above)
    _canonicalize_ipn_body,
    _hmac_sha512_hex,
    verify_ipn_signature,
)

SECRET = "test-ipn-secret-32-bytes-padding"


def _sign(body: bytes) -> str:
    """Compute the lowercase-hex HMAC-SHA512 the same way NowPayments would."""
    return hmac.new(SECRET.encode("utf-8"), body, hashlib.sha512).hexdigest()


# A representative IPN body. Includes a non-ASCII order_description so we
# exercise the same UTF-8-vs-\uXXXX gotcha that bit us in production
# (see HANDOFF.md §6).
SAMPLE_PAYLOAD = {
    "payment_id": 1234567890,
    "payment_status": "finished",
    "pay_address": "TQrZ9wBzPvF7nPq3xY5kE2rLgM8aWvJh1d",
    "price_amount": 5,
    "price_currency": "usd",
    "pay_amount": 5.123456,
    "actually_paid": 5.123456,
    "pay_currency": "usdttrc20",
    "order_id": "98765432",
    "order_description": "شارژ کیف پول",
    "purchase_id": "5500000000",
    "outcome_amount": 4.95,
    "outcome_currency": "usd",
}


def test_raw_body_signature_verifies():
    """Stripe-style: HMAC over the exact bytes received."""
    raw = json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    sig = _sign(raw)
    assert verify_ipn_signature(raw, sig, secret=SECRET) is True


def test_canonical_body_signature_verifies():
    """NowPayments-style: HMAC over the sorted, no-whitespace canonical form."""
    canonical = _canonicalize_ipn_body(
        json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    )
    assert canonical is not None
    sig = _sign(canonical)
    # Send the un-sorted body on the wire — the verifier must still accept
    # because its second pass re-canonicalizes.
    raw_unsorted = json.dumps(
        SAMPLE_PAYLOAD, ensure_ascii=False, sort_keys=False
    ).encode("utf-8")
    assert verify_ipn_signature(raw_unsorted, sig, secret=SECRET) is True


def test_uppercase_hex_signature_verifies():
    """NowPayments lowercases its digest, but harden against a future docs change."""
    raw = json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    sig = _sign(raw).upper()
    assert verify_ipn_signature(raw, sig, secret=SECRET) is True


def test_persian_description_doesnt_inflate_canonical():
    """Regression for the prod bug.

    Pre-#39 the canonicalizer used ``ensure_ascii=True`` by default, which
    expanded each Persian char in ``order_description`` from 2 raw UTF-8
    bytes into a 6-byte ``\\uXXXX`` escape — inflating the canonical body
    by ~40 bytes vs. what NowPayments put on the wire and breaking HMAC.
    Assert the post-#39 canonicalizer keeps non-ASCII as raw UTF-8.
    """
    # Same separators NowPayments uses on the wire (no whitespace).
    raw = json.dumps(
        SAMPLE_PAYLOAD, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    canonical = _canonicalize_ipn_body(raw)
    assert canonical is not None
    # Same key set + same separators ⇒ canonical and raw differ only in key
    # ordering. Both should be the same byte length.
    assert len(canonical) == len(raw), (
        f"canonical drifted in length: len(raw)={len(raw)} "
        f"len(canonical)={len(canonical)}"
    )
    # And the canonical must NOT contain any \uXXXX escape — that's the
    # exact regression we're guarding against.
    assert b"\\u" not in canonical, "canonical body still escapes non-ASCII"
    sig = _sign(canonical)
    assert verify_ipn_signature(raw, sig, secret=SECRET) is True


def test_bad_signature_rejected():
    raw = json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    bogus = "0" * 128
    assert verify_ipn_signature(raw, bogus, secret=SECRET) is False


def test_tampered_body_rejected():
    """Signing the original body, then mutating the bytes, must fail."""
    raw = json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    sig = _sign(raw)
    tampered = json.dumps(
        {**SAMPLE_PAYLOAD, "price_amount": 999999}, ensure_ascii=False
    ).encode("utf-8")
    assert verify_ipn_signature(tampered, sig, secret=SECRET) is False


def test_missing_signature_header_rejected():
    raw = json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    assert verify_ipn_signature(raw, None, secret=SECRET) is False
    assert verify_ipn_signature(raw, "", secret=SECRET) is False


def test_missing_secret_rejected():
    """No secret configured ⇒ refuse, even if the digest matched."""
    raw = json.dumps(SAMPLE_PAYLOAD, ensure_ascii=False).encode("utf-8")
    sig = _sign(raw)
    # Pass an empty secret explicitly to bypass the env-driven default.
    assert verify_ipn_signature(raw, sig, secret="") is False


def test_invalid_json_with_matching_raw_body_still_verifies():
    """The raw-body pass doesn't need JSON; only the canonical fallback does."""
    raw = b"this is not json {][}"
    sig = _sign(raw)
    assert verify_ipn_signature(raw, sig, secret=SECRET) is True


def test_invalid_json_without_matching_raw_body_rejected():
    raw = b"this is not json {][}"
    bogus = "0" * 128
    assert verify_ipn_signature(raw, bogus, secret=SECRET) is False


def test_hmac_helper_matches_stdlib():
    """Belt-and-braces sanity check on the helper itself."""
    body = b'{"k":"v"}'
    expected = hmac.new(SECRET.encode(), body, hashlib.sha512).hexdigest()
    assert _hmac_sha512_hex(SECRET, body) == expected
