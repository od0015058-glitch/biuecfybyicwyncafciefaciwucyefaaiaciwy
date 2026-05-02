import asyncio
import hashlib
import hmac
import json
import logging
import math
import os

import aiohttp
from aiogram import Bot
from aiohttp import web

from bot_health import register_loop
from database import db
from strings import t

log = logging.getLogger("bot.payments")

NOWPAYMENTS_API_KEY = os.getenv("NOWPAYMENTS_API_KEY")
NOWPAYMENTS_IPN_SECRET = os.getenv("NOWPAYMENTS_IPN_SECRET")

# Absolute lower bound on any wallet top-up, in USD. Every supported
# currency is additionally subject to its own per-currency NowPayments
# minimum (fetched from ``/v1/min-amount``); the effective floor is
# ``max(GLOBAL_MIN_TOPUP_USD, per_currency_min_usd)``. We never accept
# a top-up below ``GLOBAL_MIN_TOPUP_USD`` even if the gateway reports
# a lower per-currency number, because sub-dollar top-ups don't cover
# our own processing overhead.
#
# Overridable via ``MIN_TOPUP_USD`` so ops can bump it without a
# redeploy if the economics change (e.g. gateway-fee schedule shifts).
try:
    GLOBAL_MIN_TOPUP_USD = max(0.0, float(os.getenv("MIN_TOPUP_USD", "2")))
except (TypeError, ValueError):
    GLOBAL_MIN_TOPUP_USD = 2.0

# How long we trust a cached min-amount lookup before re-querying the
# NowPayments API. The minimums move with the underlying network
# fee + spot price, so a stale value can falsely accept an invoice
# that NowPayments will then reject. 1h is conservative.
_MIN_AMOUNT_CACHE_TTL_SECONDS = 3600
_min_amount_cache: dict[str, tuple[float | None, float]] = {}

# Background refresher cadence. The minimums shift slowly (they track
# network-fee + spot-price, not block-by-block), so every 15 minutes
# keeps the pre-flight check responsive without hammering the API.
# A deliberately-shorter interval than the 1h cache TTL so a failed
# refresh pass doesn't strand the cache on the expired-but-still-served
# side of the TTL window.
_MIN_AMOUNT_REFRESH_INTERVAL_SECONDS = max(
    60, int(os.getenv("MIN_AMOUNT_REFRESH_INTERVAL_SECONDS", "900"))
)


class MinAmountError(Exception):
    """NowPayments rejected the invoice as below the per-currency minimum.

    Raised from :func:`create_crypto_invoice` when the API returns a
    400 with ``"amountTo is too small"`` (the only currency-specific
    BAD_REQUEST we can recover from cleanly).

    Carries the offending pay-currency symbol and, when we can fetch
    it, the current minimum in USD so the handler can render a precise
    user-facing message. ``min_usd`` is ``None`` when the min-amount
    lookup itself failed (network error, malformed response, etc.).
    """

    def __init__(self, currency: str, min_usd: float | None):
        self.currency = currency
        self.min_usd = min_usd
        msg = f"NowPayments min-amount not met for {currency!s}"
        if min_usd is not None:
            msg += f" (min ${min_usd:.2f})"
        super().__init__(msg)

# Public base URL where this bot is reachable (HTTPS in production).
# Example: https://bot.example.com  -> IPN posts to /nowpayments-webhook there.
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
if not WEBHOOK_BASE_URL:
    log.warning(
        "WEBHOOK_BASE_URL is not set; NowPayments will not be able to deliver "
        "IPN callbacks until it is configured."
    )
CALLBACK_URL = f"{WEBHOOK_BASE_URL}/nowpayments-webhook" if WEBHOOK_BASE_URL else ""


def _hmac_sha512_hex(secret: str, data: bytes) -> str:
    """Lowercase hex digest of HMAC-SHA512 over ``data`` keyed by ``secret``."""
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha512).hexdigest()


def _canonicalize_ipn_body(raw_body: bytes) -> bytes | None:
    """Re-serialize the IPN body in NowPayments' canonical form.

    Recursively sorted keys, no whitespace separators, and — critically —
    ``ensure_ascii=False`` so non-ASCII characters (e.g. the Persian
    ``order_description`` "شارژ کیف پول") stay as raw UTF-8 instead of
    being escaped into ``\\uXXXX``. The default ``ensure_ascii=True``
    inflated the canonical body by ~40 bytes vs. what NowPayments
    actually signed, which is the bug PR #39 fixed.

    Returns ``None`` if the body isn't valid JSON.
    """
    try:
        payload = json.loads(raw_body)
    except (ValueError, TypeError):
        return None
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def verify_ipn_signature(
    raw_body: bytes,
    signature_header: str | None,
    *,
    secret: str | None = None,
) -> bool:
    """Verify the x-nowpayments-sig HMAC-SHA512 signature.

    Two-pass verifier (defense in depth against canonicalization drift):

    1. **Raw-body pass.** HMAC the bytes exactly as they came off the
       wire. Mature webhook handlers (Stripe, GitHub, Paddle) all sign
       the raw request body so the receiver doesn't have to guess the
       sender's canonicalization rules. This is the path that should
       hit in production.

    2. **Canonicalized pass.** If the raw-body HMAC doesn't match, fall
       back to re-serializing the parsed JSON with sorted keys and no
       whitespace and HMAC that. This matches NowPayments' historical
       documented behaviour (their PHP example does ``ksort`` +
       ``json_encode`` before signing) and protects us if anything
       upstream of us — a reverse proxy, an HTTP client, an aiohttp
       quirk — rewrote whitespace or key order between NowPayments and
       us.

    If neither matches we log diagnostics (first/last 8 hex chars of
    each candidate digest plus body lengths — not enough to forge a
    signature, enough to tell secret-mismatch from canonicalization
    drift) and return False. The caller then 401s the request.

    The ``secret`` kwarg exists so tests can inject a known IPN secret
    without touching env vars; production callers should leave it
    unset and let it resolve to ``NOWPAYMENTS_IPN_SECRET``.
    """
    secret = secret if secret is not None else NOWPAYMENTS_IPN_SECRET
    if not secret:
        log.error("NOWPAYMENTS_IPN_SECRET is not set; refusing to process IPN.")
        return False
    if not signature_header:
        log.warning("IPN request had no x-nowpayments-sig header.")
        return False

    received = signature_header.lower()

    raw_digest = _hmac_sha512_hex(secret, raw_body)
    if hmac.compare_digest(raw_digest, received):
        return True

    canonical = _canonicalize_ipn_body(raw_body)
    canonical_digest = (
        _hmac_sha512_hex(secret, canonical) if canonical is not None else None
    )
    if canonical_digest is not None and hmac.compare_digest(
        canonical_digest, received
    ):
        return True

    # Diagnostics: which candidate were we computing, how do their
    # short prefixes compare to what NowPayments sent, and how long
    # were the bodies. Useful when the next deployment still mismatches
    # and we need to know whether it's a secret problem or a
    # canonicalization problem.
    log.warning(
        "IPN sig mismatch: raw=%s..%s canonical=%s received=%s..%s "
        "secret_len=%d body_len=%d canonical_len=%s",
        raw_digest[:8],
        raw_digest[-8:],
        f"{canonical_digest[:8]}..{canonical_digest[-8:]}"
        if canonical_digest
        else "n/a",
        received[:8],
        received[-8:],
        len(secret),
        len(raw_body),
        len(canonical) if canonical is not None else "n/a",
    )
    return False


# Backwards-compatible private alias retained for any callers that
# still reach for the old name. New code should use the public name.
_verify_ipn_signature = verify_ipn_signature


async def _query_min_amount(
    currency_from: str, currency_to: str
) -> float | None:
    """Single ``GET /v1/min-amount`` call returning ``fiat_equivalent`` (USD).

    Returns ``None`` on any failure (network error, non-2xx, malformed
    JSON, missing field). The caller decides what to do with that.
    """
    url = "https://api.nowpayments.io/v1/min-amount"
    params = {
        "currency_from": currency_from,
        "currency_to": currency_to,
        "fiat_equivalent": "usd",
    }
    headers = {"x-api-key": NOWPAYMENTS_API_KEY} if NOWPAYMENTS_API_KEY else {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    log.warning(
                        "min-amount lookup %s->%s returned %d: %s",
                        currency_from, currency_to, response.status, body,
                    )
                    return None
                data = await response.json()
    except (asyncio.TimeoutError, aiohttp.ClientError):
        log.warning(
            "min-amount lookup %s->%s timed out / network error",
            currency_from, currency_to,
        )
        return None
    except Exception:
        log.exception(
            "Unexpected error in min-amount lookup %s->%s",
            currency_from, currency_to,
        )
        return None

    fiat = data.get("fiat_equivalent")
    if fiat is None:
        return None
    try:
        value = float(fiat)
    except (TypeError, ValueError):
        return None
    # Reject ``NaN`` / ``±Infinity`` / negative values explicitly.
    # ``float("NaN")`` succeeds and returns the IEEE-754 special, then
    # the caller (:func:`get_min_amount_usd`) caches it and surfaces
    # it back through ``MinAmountError(min_usd=NaN)`` whose
    # user-facing rendering is ``f"min ${nan:.2f}"`` ⇒ ``"min $nan"``.
    # Worse, the trustworthiness filter below uses ``value <
    # attempted_usd`` which is False for NaN, so the NaN passes
    # straight through unmasked. ``max(...)`` over NaN candidates
    # is also order-dependent (``max(nan, 5)`` is ``nan`` but
    # ``max(5, nan)`` is ``5``) so a NaN here corrupts the cache in
    # an order-sensitive way. Reject upstream so the caller sees a
    # clean ``None`` and falls back to the generic "min not met"
    # branch of the UI rather than rendering nonsense.
    if not math.isfinite(value) or value < 0.0:
        return None
    return value


async def get_min_amount_usd(
    pay_currency: str, *, attempted_usd: float | None = None
) -> float | None:
    """Best-effort USD floor for an invoice paid in *pay_currency*.

    NowPayments' ``/v1/min-amount`` is asymmetric and a bit
    misleading: ``<crypto> -> usd`` typically reflects the *conversion*
    floor (~tens of cents — "smallest crypto amount we'll convert to
    USD") and NOT the floor that ``POST /v1/payment`` enforces on the
    merchant settlement side. Asking the merchant-side question
    (``usd -> <crypto>``) gives us a more honest "smallest invoice
    this provider will accept" value.

    We try both directions and return the *larger* of the two as the
    user-facing minimum. If the API returns a value that's clearly
    inconsistent with the rejection we just saw — i.e. the floor is
    *less than* the amount the user actually attempted — we treat
    the lookup as untrustworthy and return ``None`` so the handler
    falls back to the generic "min not met for <currency>" message
    instead of misleading the user with e.g. "$0.16".

    The combined result is cached per pay_currency for
    ``_MIN_AMOUNT_CACHE_TTL_SECONDS`` so re-prompts after a rejection
    don't fan out two more HTTP calls.
    """
    pay_currency = pay_currency.lower()

    def _apply_trustworthiness(value: float | None) -> float | None:
        """Suppress the floor as ``None`` when the rejection we're
        explaining clearly disagrees with the looked-up floor.

        If the user's attempted amount is *above* the floor we got
        back, then by definition that floor isn't what triggered the
        rejection — surfacing it tells the user e.g. "min $0.16" when
        their $5 invoice was rejected, which is more confusing than
        the generic "unknown min" branch of the UI. The check has to
        run on every call (not just on a fresh fetch) because
        ``attempted_usd`` is per-call: a value cached during a
        small-attempt call can be unmasked as trustworthy now and
        return correctly, while a value cached during a different
        call needs to be suppressed for *this* attempt — both
        decisions depend on the *current* ``attempted_usd``, not the
        one in effect when the cache was warmed.
        """
        if (
            value is not None
            and attempted_usd is not None
            and value < attempted_usd
        ):
            log.warning(
                "min-amount lookup for %s gave $%.2f which is below the "
                "rejected invoice amount $%.2f; suppressing as untrustworthy",
                pay_currency, value, attempted_usd,
            )
            return None
        return value

    cached = _min_amount_cache.get(pay_currency)
    if cached is not None:
        value, ts = cached
        if (asyncio.get_event_loop().time() - ts) < _MIN_AMOUNT_CACHE_TTL_SECONDS:
            # Pre-fix this returned ``value`` directly, bypassing the
            # trustworthiness filter. So a small-attempt call would
            # warm the cache with $0.16, then a follow-up $5 attempt
            # whose rejection has nothing to do with the $0.16 floor
            # would render "min $0.16" in the UI — actively misleading.
            # Apply the same filter we apply to a fresh fetch so the
            # cached value stays safe to surface across diverse
            # ``attempted_usd`` values.
            return _apply_trustworthiness(value)

    pay_side = await _query_min_amount(pay_currency, "usd")
    merchant_side = await _query_min_amount("usd", pay_currency)
    candidates = [v for v in (pay_side, merchant_side) if v is not None]
    raw_value = max(candidates) if candidates else None

    # Cache the raw (un-suppressed) value so the trustworthiness
    # check can re-evaluate against future ``attempted_usd`` values.
    # Only the *current* call's return goes through the filter.
    _min_amount_cache[pay_currency] = (
        raw_value, asyncio.get_event_loop().time()
    )
    return _apply_trustworthiness(raw_value)


def effective_min_usd(pay_currency: str) -> float:
    """Return the effective floor a top-up in ``pay_currency`` must clear.

    ``max(GLOBAL_MIN_TOPUP_USD, cached per-currency NowPayments min)``.
    A cache miss / ``None`` cached value means we have no trustworthy
    per-currency number, so we fall back to the global floor only.

    Synchronous and never does I/O — reads the in-memory cache seeded
    by :func:`get_min_amount_usd` and the background refresher. Callers
    that want an on-demand lookup with HTTP fallback should await
    :func:`get_min_amount_usd` directly.
    """
    pay_currency = pay_currency.lower()
    cached = _min_amount_cache.get(pay_currency)
    per_currency_min = cached[0] if cached is not None else None
    if per_currency_min is None:
        return GLOBAL_MIN_TOPUP_USD
    return max(GLOBAL_MIN_TOPUP_USD, float(per_currency_min))


def find_cheaper_alternative(
    requested_usd: float,
    excluded_currency: str,
    candidates: "list[tuple[str, str]]",
) -> tuple[str, str] | None:
    """Suggest an alternative currency whose effective min ≤ ``requested_usd``.

    ``candidates`` is a list of ``(label, ticker)`` pairs — same shape
    as ``handlers.SUPPORTED_PAY_CURRENCIES`` — so the caller can render
    the bot's user-facing label directly. We iterate sorted ascending
    by effective min and return the **cheapest** alternative that
    clears the requested amount. Returning the cheapest (rather than
    the first) gives users the widest future headroom if they retry
    with a slightly different amount.

    Returns ``None`` if no candidate's min covers the request (either
    the request itself is below ``GLOBAL_MIN_TOPUP_USD`` across every
    coin, or we have no cached min data at all and the global floor
    already rejected the request).
    """
    excluded = excluded_currency.lower()
    viable: list[tuple[float, str, str]] = []
    for label, ticker in candidates:
        tl = ticker.lower()
        if tl == excluded:
            continue
        eff = effective_min_usd(tl)
        if eff <= requested_usd + 1e-9:
            viable.append((eff, label, ticker))
    if not viable:
        return None
    viable.sort(key=lambda item: (item[0], item[2]))
    _, label, ticker = viable[0]
    return (label, ticker)


async def refresh_min_amounts_once(
    tickers: "list[str]", *, concurrency: int = 3
) -> None:
    """Re-query ``/v1/min-amount`` for every supplied ticker once.

    Runs with a small concurrency cap so a multi-currency refresh
    doesn't burst 18 parallel HTTP calls at NowPayments (which has
    per-IP rate limits). Best-effort — individual failures are logged
    by :func:`_query_min_amount` and leave the prior known-good cache
    entry in place (see the "cache preservation" note below).

    **Cache preservation on API outage.** The naive approach of
    ``pop → fetch`` drops the previously-cached good value the moment
    the fresh fetch comes back ``None``. NowPayments-side outages /
    transient rate-limits / DNS blips are all common enough that
    losing a perfectly valid "BTC min = $10" reading at every hiccup
    would silently collapse :func:`effective_min_usd` to the $2
    global floor mid-outage — and that in turn makes
    :func:`handlers._preflight_min_amount_check` falsely admit
    sub-min amounts that NowPayments will then reject.

    Fix: snapshot the old value, force a fresh fetch by clearing the
    entry, and if the fetch returns ``None`` while the snapshot held
    a real number, put the snapshot back (with the new timestamp so
    a reader can still tell we tried). Only a successful fresh fetch
    overwrites a known-good value.
    """
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _one(ticker: str) -> None:
        async with semaphore:
            t_lower = ticker.lower()
            prior = _min_amount_cache.get(t_lower)
            prior_value = prior[0] if prior is not None else None
            # Force a fresh lookup by expiring the cache entry first.
            _min_amount_cache.pop(t_lower, None)
            try:
                await get_min_amount_usd(ticker)
            except Exception:
                log.exception(
                    "background refresh of min-amount for %s crashed",
                    ticker,
                )
            # If the refresh couldn't produce a real number but we
            # had one previously, restore it so the pre-flight check
            # keeps working through the outage. We stamp the restored
            # entry with ``now`` so the TTL reflects the last refresh
            # attempt (not the moment the value was originally
            # observed) — readers that care can spot a stuck value by
            # watching the cache across ticks.
            post = _min_amount_cache.get(t_lower)
            post_value = post[0] if post is not None else None
            if post_value is None and prior_value is not None:
                log.info(
                    "min-amount refresh for %s returned None; "
                    "preserving prior value $%.2f",
                    ticker, prior_value,
                )
                _min_amount_cache[t_lower] = (
                    prior_value, asyncio.get_event_loop().time()
                )

    await asyncio.gather(*(_one(t) for t in tickers))


@register_loop("min_amount_refresh", cadence_seconds=900)
async def refresh_min_amounts_loop(
    tickers: "list[str]",
    *,
    interval_seconds: int | None = None,
) -> None:
    """Forever-loop wrapper around :func:`refresh_min_amounts_once`.

    Intended to be spawned as a background task from ``main.py``. The
    first refresh runs immediately so the cache is warm before the
    first user reaches the currency picker; subsequent passes wait
    ``interval_seconds`` (default
    ``_MIN_AMOUNT_REFRESH_INTERVAL_SECONDS`` = 15 min).

    Swallows every exception except ``CancelledError`` so a transient
    network hiccup doesn't take the refresher off the air for the
    remainder of the process's lifetime.
    """
    interval = interval_seconds or _MIN_AMOUNT_REFRESH_INTERVAL_SECONDS
    while True:
        try:
            await refresh_min_amounts_once(tickers)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("min-amount refresher iteration failed; retrying")
        else:
            # Stage-15-Step-A: heartbeat for the Prometheus
            # ``meowassist_min_amount_refresh_last_run_epoch`` gauge.
            # Imported lazily so a fresh test runtime that hasn't
            # imported ``metrics`` yet doesn't pay the cost on every
            # iteration of this loop.
            from metrics import record_loop_tick

            record_loop_tick("min_amount_refresh")
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise


async def create_crypto_invoice(
    telegram_id: int,
    amount_usd: float,
    currency: str,
    max_retries: int = 3,
    promo_code: str | None = None,
    promo_bonus_usd: float = 0.0,
):
    url = "https://api.nowpayments.io/v1/payment"
    headers = {
        "x-api-key": NOWPAYMENTS_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "price_amount": amount_usd,
        "price_currency": "usd",
        "pay_currency": currency,
        "order_id": str(telegram_id),
        "order_description": "شارژ کیف پول",
        "ipn_callback_url": CALLBACK_URL,
        # We deliberately DO NOT set is_fee_paid_by_user. Per
        # NowPayments' own FAQ
        # (nowpayments.io/help/payments/api/...), enabling that
        # flag automatically pins the invoice to fixed-rate mode
        # and bakes the ~0.5% gateway service fee into the
        # quoted pay_amount. The combined effect raises the
        # effective per-currency floor past $5 even on cheap
        # chains like TRX / USDT-BEP20, which is exactly the
        # rejection users were hitting.
        #
        # By leaving the flag off the merchant absorbs the
        # gateway's 0.5% service fee (~$0.025 on a $5 top-up,
        # ~$0.50 on a $100 top-up). The model-call markup more
        # than covers that, and the alternative (a $5 invoice
        # that NowPayments rejects with "amountTo is too small")
        # is much worse for retention.
    }

    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    # NowPayments returns 201 for created invoices.
                    if response.status in (200, 201):
                        data = await response.json()
                        if not (data.get("pay_address") and data.get("pay_amount")):
                            log.warning(
                                "NowPayments returned 2xx but missing fields: %r",
                                data,
                            )
                            return None

                        payment_id = data.get("payment_id")
                        if payment_id is None:
                            log.warning(
                                "NowPayments response missing payment_id; "
                                "refusing to issue invoice without an idempotency key."
                            )
                            return None

                        # Record a PENDING transaction so the IPN webhook can
                        # finalize idempotently. If we can't record PENDING,
                        # do NOT return the invoice — the webhook would
                        # refuse to credit it later.
                        try:
                            await db.create_pending_transaction(
                                telegram_id=telegram_id,
                                gateway="NowPayments",
                                currency_used=currency,
                                amount_crypto=float(data.get("pay_amount")),
                                amount_usd=float(amount_usd),
                                gateway_invoice_id=str(payment_id),
                                promo_code=promo_code,
                                promo_bonus_usd=float(promo_bonus_usd),
                            )
                        except Exception:
                            log.exception(
                                "Failed to record PENDING transaction for payment_id=%s",
                                payment_id,
                            )
                            return None

                        return data

                    error_text = await response.text()
                    log.error(
                        "NowPayments error (attempt %d/%d) for "
                        "pay_currency=%s amount_usd=$%.2f: status=%d body=%s",
                        attempt + 1,
                        max_retries,
                        currency,
                        float(amount_usd),
                        response.status,
                        error_text,
                    )

                    # Currency-specific minimum: don't waste retries on a
                    # 400 that's deterministic. Surface a structured
                    # error so the handler can show the user the actual
                    # minimum and suggest a different currency.
                    if (
                        response.status == 400
                        and "amountTo is too small" in error_text
                    ):
                        min_usd = await get_min_amount_usd(
                            currency, attempted_usd=float(amount_usd)
                        )
                        raise MinAmountError(currency=currency, min_usd=min_usd)

        except MinAmountError:
            raise
        except asyncio.TimeoutError:
            log.warning(
                "Timeout talking to NowPayments (attempt %d/%d)",
                attempt + 1,
                max_retries,
            )
        except Exception:
            log.exception(
                "Network error talking to NowPayments (attempt %d/%d)",
                attempt + 1,
                max_retries,
            )

        if attempt < max_retries - 1:
            await asyncio.sleep(2)

    return None


# IPN statuses we consider "in flight": the payment is still progressing,
# nothing to do but log. NowPayments may emit several of these per invoice.
_IN_FLIGHT_STATUSES = frozenset(
    {"waiting", "confirming", "confirmed", "sending"}
)

# IPN statuses that mean the payment will NOT settle. We mark the ledger
# with a terminal status (so retries are no-ops) and notify the user.
# Mapping: incoming IPN status -> ledger status we record.
_TERMINAL_FAILURE_STATUSES = {
    "expired": "EXPIRED",
    "failed": "FAILED",
    "refunded": "REFUNDED",
}


def _finite_positive_float(value) -> float | None:
    """Return ``float(value)`` iff it parses, is finite, and > 0.

    Why not just ``float(v) > 0``? ``float("NaN")`` returns ``nan``,
    and **every** comparison against ``nan`` returns ``False`` —
    including ``nan <= 0``. So a pre-fix check of the form::

        x = float(data["actually_paid"])
        if x <= 0:
            return None
        ...credit x to the wallet...

    silently passed ``NaN`` straight through to ``finalize_payment`` /
    ``finalize_partial_payment``, which then INSERTed ``NaN`` into the
    ``transactions.amount_usd_credited`` ``DECIMAL`` column. PostgreSQL
    accepts ``'NaN'::numeric`` (it's a defined IEEE-754 value), but
    every subsequent balance comparison against the wallet — including
    ``deduct_balance``'s ``WHERE balance_usd >= $1 RETURNING ...`` —
    becomes a silent no-op (``NaN >= x`` is always false), effectively
    bricking the user's wallet without an obvious error.

    ``math.isfinite`` returns ``False`` for ``nan``, ``+inf``, and
    ``-inf``, so the single check below is equivalent to the
    ``not (v == v) or v in (inf, -inf) or v <= 0`` pattern used
    elsewhere in the codebase but a lot easier to read.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    return v


def _compute_actually_paid_usd(data: dict) -> float | None:
    """Convert the IPN's `actually_paid` (in pay_currency) to USD.

    NowPayments quotes the conversion rate at invoice time:
        pay_amount  <crypto>  ==  price_amount  USD
    so the proportional USD value of `actually_paid` is:
        actually_paid_usd = actually_paid / pay_amount * price_amount

    Using the quoted rate (rather than fetching a fresh spot price) is
    the right defensive choice: it's the rate the user agreed to when
    they generated the invoice, so they cannot game us by paying when
    the spot price has moved against us.

    Returns None if any required field is missing, non-finite (NaN /
    Infinity), or non-positive, in which case the caller should refuse
    to credit and surface for manual reconciliation.
    """
    try:
        actually_paid_raw = data["actually_paid"]
        pay_amount_raw = data["pay_amount"]
        price_amount_raw = data["price_amount"]
    except KeyError:
        return None
    actually_paid = _finite_positive_float(actually_paid_raw)
    pay_amount = _finite_positive_float(pay_amount_raw)
    price_amount = _finite_positive_float(price_amount_raw)
    if actually_paid is None or pay_amount is None or price_amount is None:
        return None
    # Cap at price_amount as a defense-in-depth: NowPayments shouldn't fire
    # `partially_paid` for an over-payment, but if it ever did we don't want
    # to credit more than the user requested.
    usd = actually_paid / pay_amount * price_amount
    # Even with finite inputs, FP arithmetic on extreme magnitudes
    # could in principle overflow to inf — guard the output too.
    if not math.isfinite(usd) or usd <= 0:
        return None
    return min(usd, price_amount)

# Maps an IPN failure status -> (strings.py key for PENDING-row variant,
# strings.py key for PARTIAL-row variant). The actual translated text is
# looked up at notification time using the user's stored language.
_TERMINAL_FAILURE_MESSAGE_KEYS = {
    "expired": ("pay_expired_pending", "pay_expired_partial"),
    "failed": ("pay_failed_pending", "pay_failed_partial"),
    "refunded": ("pay_refunded_pending", "pay_refunded_partial"),
}

# Stage-9-Step-4: IPN statuses that trigger state mutation. The handler
# splits cleanly into three buckets — actionable (finalize_payment /
# finalize_partial_payment / mark_transaction_terminal),
# in-flight observability (already declared above as
# ``_IN_FLIGHT_STATUSES``), and unhandled (logged loudly, audited,
# no state change). Listed explicitly so a future NowPayments status
# addition lands in ``"unhandled"`` rather than being silently swallowed.
_ACTIONABLE_IPN_STATUSES = frozenset(
    {"finished", "partially_paid", "expired", "failed", "refunded"}
)

# Stage-9-Step-4 bug-fix bundle: per-process counters for IPN deliveries
# we drop *before* state mutation. Surfaced via
# ``get_ipn_drop_counters()`` so future ops dashboards can read the same
# values without scraping logs. The four reasons cover the legitimate
# fail-safe paths (bad signature, malformed body, missing payment_id,
# replayed (invoice, status) pair) — every increment is also logged at
# ``error`` level so a misconfigured sandbox hitting the prod webhook
# is immediately visible without grep-fu.
_IPN_DROP_COUNTERS: dict[str, int] = {
    "bad_signature": 0,
    "bad_json": 0,
    "missing_payment_id": 0,
    "replay": 0,
}


def _bump_ipn_drop_counter(reason: str) -> None:
    """Bump the named drop counter by 1, defensively creating the bucket
    if a future code path passes a new reason string. The dict is
    process-local — restarting the bot resets all four counters to 0."""
    _IPN_DROP_COUNTERS[reason] = _IPN_DROP_COUNTERS.get(reason, 0) + 1


def get_ipn_drop_counters() -> dict[str, int]:
    """Return a snapshot copy of the IPN drop counters so callers can
    surface them via /admin/payment_health (queued) or via tests
    without touching the module-private dict."""
    return dict(_IPN_DROP_COUNTERS)


def _classify_ipn_outcome(status: str | None) -> str:
    """Map an IPN ``payment_status`` to the audit-trail outcome we'll
    record at observation time.

    * ``"applied"`` for statuses the handler will mutate state on.
    * ``"noop"`` for in-flight / informational statuses we deliberately
      ignore.
    * ``"unhandled"`` for everything else — logged loudly so a new
      NowPayments status doesn't get lost in the void.

    The dedupe contract on ``payment_status_transitions`` is the
    ``UNIQUE(gateway_invoice_id, payment_status)`` index, NOT this
    classification; outcome is purely audit-trail enrichment.
    """
    if status in _ACTIONABLE_IPN_STATUSES:
        return "applied"
    if status in _IN_FLIGHT_STATUSES:
        return "noop"
    return "unhandled"


async def payment_webhook(request: web.Request):
    try:
        raw_body = await request.read()
        signature = request.headers.get("x-nowpayments-sig")

        if not verify_ipn_signature(raw_body, signature):
            _bump_ipn_drop_counter("bad_signature")
            log.warning(
                "IPN signature verification failed (remote=%s)", request.remote
            )
            return web.Response(status=401, text="Invalid signature")

        try:
            data = json.loads(raw_body)
        except (ValueError, TypeError):
            # Bug-fix bundle: a malformed-JSON body that happens to pass
            # signature verification (because ``verify_ipn_signature``
            # takes raw bytes) used to be swallowed by the outer
            # ``except Exception`` as a generic 500. Surface the real
            # reason — it almost always means a misconfigured sandbox
            # client posting form-encoded data to the prod webhook.
            _bump_ipn_drop_counter("bad_json")
            log.error(
                "IPN body is not valid JSON (remote=%s, len=%d); ignoring",
                request.remote,
                len(raw_body),
            )
            return web.Response(status=200, text="OK")

        status = data.get("payment_status")
        payment_id = data.get("payment_id")
        if payment_id is None:
            # Bug-fix bundle: pre-fix this was a *warning*-level log
            # with no counter, so a misconfigured sandbox hitting the
            # prod webhook was effectively invisible. Bump the level
            # to ``error`` (deploy alerts hook on ``error`` and above)
            # AND increment the per-process drop counter so
            # /admin/payment_health (queued) can surface it without
            # scraping logs.
            _bump_ipn_drop_counter("missing_payment_id")
            log.error(
                "IPN missing payment_id; ignoring (status=%s, remote=%s, "
                "body_len=%d) — likely a misconfigured NowPayments "
                "sandbox callback URL",
                status,
                request.remote,
                len(raw_body),
            )
            return web.Response(status=200, text="OK")

        # Stage-9-Step-4: schema-level replay-dedupe. Insert the
        # observed (invoice, status) pair into
        # ``payment_status_transitions``; ``ON CONFLICT DO NOTHING``
        # means a duplicate delivery returns ``None`` and we bail
        # *before* calling ``finalize_payment`` /
        # ``finalize_partial_payment`` / ``mark_transaction_terminal``.
        # The downstream row-status guards still catch out-of-order
        # deliveries against an already-terminal row (e.g. PARTIAL
        # arriving after SUCCESS), but the transitions table makes the
        # dedupe explicit and observable in /admin/audit.
        intended_outcome = _classify_ipn_outcome(status)
        try:
            transition_id = await db.record_payment_status_transition(
                str(payment_id),
                str(status) if status is not None else "",
                outcome=intended_outcome,
                meta={
                    "remote": request.remote,
                    "body_len": len(raw_body),
                },
            )
        except Exception:
            # Fail-open on a transient DB blip: we'd rather process the
            # IPN and rely on the existing row-status dedupe than 500
            # back to NowPayments and trigger their retry storm. The
            # ``except`` is narrow to keep this well-behaved — anything
            # the asyncpg pool itself raises is already in the outer
            # ``Exception`` branch below if it happens later.
            log.exception(
                "record_payment_status_transition failed for "
                "payment_id=%s status=%s; falling through to row-level "
                "dedupe",
                payment_id,
                status,
            )
            transition_id = "deferred"  # truthy sentinel so we proceed
        if transition_id is None:
            _bump_ipn_drop_counter("replay")
            log.info(
                "IPN replay dropped: payment_id=%s status=%s already "
                "observed (deduped at payment_status_transitions)",
                payment_id,
                status,
            )
            return web.Response(status=200, text="OK")

        bot: Bot = request.app["bot"]

        if status == "finished":
            # We need the full invoice price from the IPN (not the row's
            # amount_usd_credited, which is overwritten with the partial
            # already-credited amount when this payment first came in as
            # partially_paid). Without it we can't compute the remaining
            # delta to credit on a PARTIAL -> SUCCESS upgrade.
            #
            # ``_finite_positive_float`` rejects NaN / Inf in addition to
            # the obvious missing / non-numeric / non-positive cases.
            # Pre-fix a NaN ``price_amount`` slipped past
            # ``full_price_usd <= 0`` (every comparison against NaN is
            # False) and got passed to ``finalize_payment`` as the
            # credit amount — see ``_finite_positive_float`` docstring
            # for the wallet-bricking implications.
            full_price_usd = _finite_positive_float(data.get("price_amount"))
            if full_price_usd is None:
                log.error(
                    "finished IPN for payment_id=%s missing/invalid "
                    "price_amount; refusing to credit (data=%r)",
                    payment_id,
                    data,
                )
                return web.Response(status=200, text="OK")

            # Atomic in one DB transaction: flip the row (PENDING or
            # PARTIAL) to SUCCESS, and credit the wallet by however much
            # of the full invoice price hasn't been credited yet. If
            # anything fails the row stays in its previous state so a
            # webhook retry can finalize it.
            row = await db.finalize_payment(str(payment_id), full_price_usd)
            if row is None:
                # Either we've never seen this payment_id (no PENDING row
                # was created on our side) or it was already in a terminal
                # state (SUCCESS / EXPIRED / FAILED / REFUNDED). Either
                # way: do NOT credit. The whole point of the transactions
                # ledger is that a replayed or unknown IPN cannot mint money.
                log.info(
                    "Webhook for payment_id=%s ignored (unknown or already finalized)",
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            delta_credited = float(row["delta_credited"])
            total_credited = float(row["amount_usd_credited"])
            bonus_credited = float(row.get("promo_bonus_credited") or 0.0)

            # Best-effort user notification. The wallet has already been
            # credited in finalize_payment; a Telegram error must not cause us
            # to return 500 and trigger a NowPayments retry (the retry would
            # be a no-op because the row is no longer PENDING/PARTIAL).
            lang = await db.get_user_language(telegram_id)
            if delta_credited > 0:
                # Either a fresh full payment, or the remainder after a
                # partially_paid earlier credited some.
                msg = t(lang, "pay_credited_full", delta=delta_credited)
            else:
                # We've already credited the full amount earlier (e.g.
                # partially_paid covered the full price). Just close the
                # loop with the user.
                msg = t(lang, "pay_credited_total_only", total=total_credited)
            if bonus_credited > 0:
                msg = msg + "\n\n" + t(
                    lang, "pay_promo_bonus", bonus=bonus_credited
                )
            try:
                await bot.send_message(chat_id=telegram_id, text=msg)
            except Exception:
                log.exception(
                    "Failed to notify user %d about credit of $%s "
                    "(delta=$%.4f, promo_bonus=$%.4f)",
                    telegram_id,
                    total_credited,
                    delta_credited,
                    bonus_credited,
                )

        elif status in _TERMINAL_FAILURE_STATUSES:
            # Close the ledger row (works for both PENDING and PARTIAL —
            # see mark_transaction_terminal docstring) and notify the user.
            # No balance change: PARTIAL rows keep the partial credit they
            # already received.
            target_status = _TERMINAL_FAILURE_STATUSES[status]
            # Stage-12-Step-A: REFUNDED has its own dedicated DB entry
            # point so the type system can't confuse a gateway-side
            # refund (no debit, IPN-driven) with an admin-issued one
            # (debit + audit, web-panel driven). Functionally identical
            # to the previous ``mark_transaction_terminal("REFUNDED")``
            # call from the IPN's perspective — same row dict shape,
            # same idempotent retry semantics.
            if target_status == "REFUNDED":
                row = await db.mark_payment_refunded_via_ipn(str(payment_id))
            else:
                row = await db.mark_transaction_terminal(str(payment_id), target_status)
            if row is None:
                log.info(
                    "Webhook %s for payment_id=%s ignored "
                    "(unknown or already in a different terminal state)",
                    status,
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            previous_status = row["previous_status"]
            # `amount_usd_credited` semantics differ by row state: for PENDING
            # rows it's the *intended* credit (set at invoice creation), for
            # PARTIAL rows it's the cumulative amount already credited. Treat
            # PENDING as $0 actually credited so the log line and the
            # `{credited}` template var both reflect reality.
            credited_so_far = (
                float(row["amount_usd_credited"]) if previous_status == "PARTIAL" else 0.0
            )
            log.info(
                "Marked payment_id=%s as %s for user %d (was %s, credited so far $%.4f)",
                payment_id,
                target_status,
                telegram_id,
                previous_status,
                credited_so_far,
            )
            pending_key, partial_key = _TERMINAL_FAILURE_MESSAGE_KEYS[status]
            string_key = partial_key if previous_status == "PARTIAL" else pending_key
            lang = await db.get_user_language(telegram_id)
            text = t(lang, string_key, credited=credited_so_far)
            try:
                await bot.send_message(chat_id=telegram_id, text=text)
            except Exception:
                log.exception(
                    "Failed to notify user %d about %s payment %s",
                    telegram_id,
                    target_status,
                    payment_id,
                )

        elif status == "partially_paid":
            # Under-payment: the user paid some crypto, but less than the
            # invoice required. Credit the proportional USD value derived
            # from `actually_paid` (NOT the originally requested
            # price_amount, which would over-credit and let users
            # intentionally underpay to drain margin).
            actually_paid_usd = _compute_actually_paid_usd(data)
            if actually_paid_usd is None:
                # Couldn't derive a credit amount from the IPN payload.
                # Refuse to credit and log loudly so the operator can
                # reconcile by hand. The row stays PENDING.
                log.error(
                    "partially_paid IPN for payment_id=%s missing/invalid "
                    "fields needed to convert actually_paid -> USD; leaving "
                    "row PENDING for manual review (data=%r)",
                    payment_id,
                    data,
                )
                return web.Response(status=200, text="OK")

            row = await db.finalize_partial_payment(
                str(payment_id), actually_paid_usd
            )
            if row is None:
                log.info(
                    "Webhook partially_paid for payment_id=%s ignored "
                    "(unknown or already in a non-PENDING/non-PARTIAL state)",
                    payment_id,
                )
                return web.Response(status=200, text="OK")

            telegram_id = row["telegram_id"]
            total_credited = float(row["amount_usd_credited"])
            delta = float(row["delta_credited"])
            log.info(
                "partially_paid for payment_id=%s user=%d: "
                "delta_credited=$%.4f total_credited=$%.4f",
                payment_id,
                telegram_id,
                delta,
                total_credited,
            )
            # Only notify when there was actually new money to credit, so a
            # replayed IPN with the same actually_paid doesn't re-spam the user.
            if delta > 0:
                lang = await db.get_user_language(telegram_id)
                try:
                    await bot.send_message(
                        chat_id=telegram_id,
                        text=t(lang, "pay_partial", delta=delta, total=total_credited),
                    )
                except Exception:
                    log.exception(
                        "Failed to notify user %d about partial credit of $%s",
                        telegram_id,
                        delta,
                    )

        elif status in _IN_FLIGHT_STATUSES:
            log.info(
                "In-flight IPN status=%s for payment_id=%s; no-op",
                status,
                payment_id,
            )

        else:
            log.info(
                "Unhandled IPN status=%s for payment_id=%s; no-op",
                status,
                payment_id,
            )

        return web.Response(status=200, text="OK")
    except Exception:
        log.exception("Webhook handler error")
        return web.Response(status=500, text="Error")
