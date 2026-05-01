"""Auto-discover new OpenRouter models and notify admins.

Stage-10-Step-C. Every ``DISCOVERY_INTERVAL_SECONDS`` (default 6h)
the bot diffs the live OpenRouter catalog against the persistent
``seen_models`` table and sends a Telegram DM to every admin in
``ADMIN_USER_IDS`` summarising anything new. After a notification
round the new ids are written back to the table so subsequent polls
don't re-notify for the same models.

Design notes
------------

* **Bootstrap behaviour.** If ``seen_models`` is empty on the first
  call, every current catalog model is silently recorded without
  sending a DM. Otherwise a fresh deploy would flood admins with
  200+ "new model" notifications for models that have been in the
  catalog forever. Explicit first-run suppression is easier to
  reason about than a "new this week" heuristic.

* **Prominent vs. long-tail filtering.** The user specifically asked
  about new models in *our category* — the five providers we
  highlight as top-level buttons in the model picker (OpenAI,
  Anthropic, Google, xAI, DeepSeek). Long-tail providers (Meta,
  Mistral, Qwen, …) still show up under the "Others" bucket and
  benefit from the catalog refresh, but they produce too much noise
  at 50+ models each to be worth DMing admins about. If you want to
  widen the filter, bump ``ADMIN_NOTIFY_DISCOVERY_PROVIDERS`` in env.

* **Rate cap.** We cap at :data:`_MAX_NEW_MODELS_PER_NOTIFICATION`
  models per DM so a provider that ships a 20-model family in one
  drop doesn't exceed Telegram's 4 096-char message limit. If more
  than the cap appear in one refresh, the DM names the first N and
  logs a count of the overflow so ops can `/admin/models` to see
  the rest (future) or SSH into the DB.

* **Per-admin fault tolerance.** If admin A has blocked the bot
  (``TelegramForbiddenError``) we log and move on — we don't let
  that poison admin B's notification or raise out of the loop.

* **Concurrency.** Runs alongside ``refresh_min_amounts_loop`` and
  the pending-expiration reaper in ``main.py``. Holds no locks;
  collates with ``models_catalog`` via the same public
  :func:`models_catalog.get_catalog` accessor, so two refreshers
  can't double-fetch from OpenRouter.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from dataclasses import dataclass

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

from admin import get_admin_user_ids
from database import db
from models_catalog import CatalogModel, force_refresh

log = logging.getLogger("bot.model_discovery")


# Default refresh interval. 6h is a sensible balance: new models
# typically ship during business hours in one of a handful of
# timezones, and a 6h cadence means the alert lands the same day.
_DISCOVERY_INTERVAL_SECONDS: int = int(
    os.getenv("DISCOVERY_INTERVAL_SECONDS", str(6 * 60 * 60))
)

# Hard cap per DM so we never bust Telegram's 4 096-char message
# limit when a provider ships a big family drop. 10 lines of
# "<id> — <name>" comfortably fits with the header / footer.
_MAX_NEW_MODELS_PER_NOTIFICATION: int = int(
    os.getenv("DISCOVERY_MAX_MODELS_PER_DM", "10")
)


def _parse_float_env(name: str, default: float) -> float:
    """Tolerant float env parser: blank / malformed / non-finite → ``default``.

    ``float("nan")`` / ``float("inf")`` / ``float("-inf")`` parse
    successfully — but feeding any of them into a threshold check
    (``abs(delta) >= threshold``) silently disables the alert path
    because every comparison against NaN is ``False`` and nothing
    finite can exceed +Inf. The latent regression from that gap was
    a misconfigured ``PRICE_ALERT_THRESHOLD_PERCENT=nan`` quietly
    turning off the entire price-move alert system rather than
    surfacing a configuration error and falling back to the default.
    Reject non-finite values explicitly so the failure mode is loud.
    """
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        log.warning("%s=%r is not a float; using default %.2f", name, raw, default)
        return default
    if not math.isfinite(value):
        log.warning(
            "%s=%r parsed as non-finite (%s); using default %.2f",
            name, raw, value, default,
        )
        return default
    return value


# Threshold (percent) above which a per-side price move becomes an
# alert. 20% catches dishonest / silent-bump scenarios without
# firing on the small floating-point wobble OpenRouter sometimes
# emits when they re-denominate tokens-per-dollar. Per-side
# (compared independently) because a provider can cut input prices
# by 50% while raising output by 10% and we want both legs named.
_PRICE_ALERT_THRESHOLD_PERCENT: float = _parse_float_env(
    "PRICE_ALERT_THRESHOLD_PERCENT", 20.0
)


def _parse_notify_providers() -> frozenset[str]:
    """Read the env-configurable provider allowlist for notifications.

    Defaults to the same five prominent providers the model picker
    surfaces as top-level buttons. Operators who want DMs for every
    provider can set ``ADMIN_NOTIFY_DISCOVERY_PROVIDERS=*``.
    """
    raw = os.getenv("ADMIN_NOTIFY_DISCOVERY_PROVIDERS", "").strip()
    if not raw:
        return frozenset({"openai", "anthropic", "google", "x-ai", "deepseek"})
    if raw == "*":
        return frozenset()  # empty = no filter (see _is_notifiable)
    return frozenset(part.strip().lower() for part in raw.split(",") if part.strip())


_NOTIFY_PROVIDERS: frozenset[str] = _parse_notify_providers()


def _is_notifiable(model: CatalogModel) -> bool:
    """True if this model's provider is in the notify allowlist.

    Empty allowlist (``ADMIN_NOTIFY_DISCOVERY_PROVIDERS=*``) means
    notify for everything.
    """
    if not _NOTIFY_PROVIDERS:
        return True
    return model.provider.lower() in _NOTIFY_PROVIDERS


@dataclass(frozen=True)
class PriceDelta:
    """One model's price move between the prior snapshot and the live
    catalog. Only populated when at least one side moved by more than
    :data:`_PRICE_ALERT_THRESHOLD_PERCENT`.

    ``input_delta_pct`` / ``output_delta_pct`` are percent deltas
    computed as ``(new - old) / old * 100`` per side. Positive means
    the upstream price went UP; negative means it went DOWN (great
    for margin, but still worth surfacing so the operator can
    recompute their average margin).
    """

    model_id: str
    old_input_per_1m_usd: float
    new_input_per_1m_usd: float
    old_output_per_1m_usd: float
    new_output_per_1m_usd: float
    input_delta_pct: float
    output_delta_pct: float


@dataclass(frozen=True)
class DiscoveryResult:
    """Structured output of one discovery pass.

    Kept as a small dataclass rather than a tuple so the loop can
    log / test each field independently (e.g. "we recorded 200 ids
    on bootstrap but notified about 0" is a healthy first-run state;
    "we recorded 5 and notified about 0" is a red flag that the
    allowlist is filtering too aggressively).
    """

    total_live_models: int
    newly_seen_ids: frozenset[str]
    notified_models: tuple[CatalogModel, ...]
    bootstrap: bool  # True when the prior seen-set was empty.
    price_deltas: tuple[PriceDelta, ...] = ()


async def _compute_discovery(
    *, live_models: tuple[CatalogModel, ...], prior_seen: set[str]
) -> DiscoveryResult:
    """Pure function: given the live catalog and prior seen set,
    compute what's new and what to notify about.

    Split out from the loop body so tests can exercise the diff
    logic without mocking the Bot / Database.
    """
    live_ids = {m.id for m in live_models}
    newly_seen_ids = frozenset(live_ids - prior_seen)

    bootstrap = not prior_seen
    if bootstrap:
        # First ever run: treat every current model as already known
        # so we don't flood admins with the full catalog.
        return DiscoveryResult(
            total_live_models=len(live_ids),
            newly_seen_ids=newly_seen_ids,
            notified_models=(),
            bootstrap=True,
        )

    notified = tuple(
        m for m in live_models if m.id in newly_seen_ids and _is_notifiable(m)
    )
    return DiscoveryResult(
        total_live_models=len(live_ids),
        newly_seen_ids=newly_seen_ids,
        notified_models=notified,
        bootstrap=False,
    )


def _compute_price_deltas(
    *,
    live_models: tuple[CatalogModel, ...],
    prior_prices: dict[str, tuple[float, float]],
    threshold_pct: float,
) -> tuple[PriceDelta, ...]:
    """Return the subset of models whose price moved by more than
    ``threshold_pct`` on at least one side.

    Skips:

    * models that have no prior snapshot (first time we've seen them
      — those are reported via the Step-C "new model" path, not here);
    * models whose prior snapshot had a zero on the side that moved
      (percent-change is undefined for old=0; the new-model DM path
      already flagged zero-priced models if relevant);
    * non-finite / negative live prices (defensive — the catalog
      parser should already filter these out).
    """
    deltas: list[PriceDelta] = []
    for model in live_models:
        prior = prior_prices.get(model.id)
        if prior is None:
            continue
        old_input, old_output = prior
        new_input = model.price.input_per_1m_usd
        new_output = model.price.output_per_1m_usd

        input_delta_pct = 0.0
        output_delta_pct = 0.0
        input_alerted = False
        output_alerted = False

        if old_input > 0.0:
            input_delta_pct = (new_input - old_input) / old_input * 100.0
            if abs(input_delta_pct) >= threshold_pct:
                input_alerted = True
        if old_output > 0.0:
            output_delta_pct = (new_output - old_output) / old_output * 100.0
            if abs(output_delta_pct) >= threshold_pct:
                output_alerted = True

        if input_alerted or output_alerted:
            deltas.append(
                PriceDelta(
                    model_id=model.id,
                    old_input_per_1m_usd=old_input,
                    new_input_per_1m_usd=new_input,
                    old_output_per_1m_usd=old_output,
                    new_output_per_1m_usd=new_output,
                    input_delta_pct=input_delta_pct,
                    output_delta_pct=output_delta_pct,
                )
            )
    # Sort by the largest absolute side-move first so the operator
    # sees the most dramatic change at the top of the DM.
    deltas.sort(
        key=lambda d: max(abs(d.input_delta_pct), abs(d.output_delta_pct)),
        reverse=True,
    )
    return tuple(deltas)


def _format_price_delta_notification(
    deltas: tuple[PriceDelta, ...], threshold_pct: float
) -> str:
    """Render the price-delta DM. Plain text (no Markdown) so slugs
    with ``_`` / ``*`` don't need escaping."""
    head = (
        f"⚠️ {len(deltas)} OpenRouter model(s) moved price by "
        f"more than {threshold_pct:.0f}% since last check:\n\n"
    )
    shown = deltas[:_MAX_NEW_MODELS_PER_NOTIFICATION]
    lines: list[str] = []
    for d in shown:
        # A model can enter the delta tuple because only ONE side
        # moved past the threshold; the other side's pct is 0.0
        # (unchanged) or sub-threshold. Render 0.0 as a flat arrow
        # ``→`` so the operator doesn't see a misleading ``↓0.0%``
        # next to an unchanged price.
        in_arrow = (
            "↑" if d.input_delta_pct > 0
            else ("↓" if d.input_delta_pct < 0 else "→")
        )
        out_arrow = (
            "↑" if d.output_delta_pct > 0
            else ("↓" if d.output_delta_pct < 0 else "→")
        )
        lines.append(
            f"• {d.model_id}\n"
            f"    input:  ${d.old_input_per_1m_usd:.4f} → "
            f"${d.new_input_per_1m_usd:.4f} / 1M "
            f"({in_arrow}{abs(d.input_delta_pct):.1f}%)\n"
            f"    output: ${d.old_output_per_1m_usd:.4f} → "
            f"${d.new_output_per_1m_usd:.4f} / 1M "
            f"({out_arrow}{abs(d.output_delta_pct):.1f}%)"
        )
    overflow = len(deltas) - len(shown)
    footer = ""
    if overflow > 0:
        footer = f"\n\n…and {overflow} more (DB has the full list)."
    return head + "\n".join(lines) + footer


async def notify_admins_of_price_deltas(
    bot: Bot, deltas: tuple[PriceDelta, ...], threshold_pct: float
) -> int:
    """Send a price-delta DM to each admin. Returns successful sends.

    Separate from :func:`notify_admins` so the two notifications can
    be read independently. Shares the same per-admin fault isolation
    policy.
    """
    if not deltas:
        return 0
    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "Detected %d price-delta event(s) but ADMIN_USER_IDS is empty "
            "— nothing to notify. Set ADMIN_USER_IDS to receive these.",
            len(deltas),
        )
        return 0
    text = _format_price_delta_notification(deltas, threshold_pct)
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
            sent += 1
        except TelegramForbiddenError:
            log.info(
                "Admin %d blocked the bot; skipping price-delta notification",
                admin_id,
            )
        except TelegramAPIError:
            log.exception(
                "Failed to send price-delta notification to admin %d",
                admin_id,
            )
    return sent


def _format_notification(notified: tuple[CatalogModel, ...]) -> str:
    """Render the admin DM body. Plain text (no Markdown) so we don't
    have to escape model names that contain ``_`` or ``*``.

    Caps the rendered count at :data:`_MAX_NEW_MODELS_PER_NOTIFICATION`
    and appends an overflow footer naming the overflow count.
    """
    head = f"🆕 Discovered {len(notified)} new OpenRouter model(s):\n\n"
    shown = notified[:_MAX_NEW_MODELS_PER_NOTIFICATION]
    lines = [f"• {m.id} — {m.name}" for m in shown]
    overflow = len(notified) - len(shown)
    footer = ""
    if overflow > 0:
        footer = f"\n\n…and {overflow} more (DB has the full list)."
    return head + "\n".join(lines) + footer


async def notify_admins(bot: Bot, notified: tuple[CatalogModel, ...]) -> int:
    """Send a DM to each admin. Returns the number of successful sends.

    Per-admin fault isolation: a bot-blocked-by-admin or a transient
    Telegram 5xx on admin A doesn't stop admin B's notification.
    """
    if not notified:
        return 0
    admin_ids = get_admin_user_ids()
    if not admin_ids:
        log.warning(
            "Discovered %d new model(s) but ADMIN_USER_IDS is empty "
            "— nothing to notify. Set ADMIN_USER_IDS to receive these.",
            len(notified),
        )
        return 0
    text = _format_notification(notified)
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
            sent += 1
        except TelegramForbiddenError:
            # Admin blocked the bot. Logged at INFO because it's
            # a normal state (admin doesn't want DMs) — not an error.
            log.info(
                "Admin %d blocked the bot; skipping discovery notification",
                admin_id,
            )
        except TelegramAPIError:
            log.exception(
                "Failed to send discovery notification to admin %d",
                admin_id,
            )
    return sent


async def run_discovery_pass(bot: Bot) -> DiscoveryResult:
    """One discovery pass: fetch catalog, diff seen + prices, notify, persist.

    Force-refreshes the catalog (bypassing the 24h TTL) so price
    deltas are detected on the loop's cadence rather than at the
    slower catalog-TTL cadence. Extracted from the forever-loop so
    the boot path (or a manual ``/admin/refresh_models`` trigger in
    the future) can run exactly one pass on demand.

    Two independent notification streams:

    * **New-model DM** (Stage-10-Step-C) — fires when a previously
      unseen model id appears in the catalog's prominent-provider
      allowlist. Bootstrap-suppressed on first run.
    * **Price-delta DM** (Stage-10-Step-D) — fires when a previously
      snapshotted model's per-1M price moved by more than
      ``PRICE_ALERT_THRESHOLD_PERCENT`` on either side. Always
      suppressed for models we've never priced before (those are
      reported via the new-model path) so a first deploy doesn't
      fire a 200-row delta DM.
    """
    # ``models_catalog._refresh`` records the
    # ``catalog_refresh`` heartbeat on a successful OpenRouter fetch
    # (Stage-15-Step-A). We do NOT tick the ``catalog_refresh`` gauge
    # here because ``force_refresh`` falls through to the previous
    # snapshot on a failed fetch — recording the tick from the
    # caller would silently mask the staleness we want to surface.
    catalog = await force_refresh()
    prior_seen = await db.get_seen_model_ids()
    prior_prices = await db.get_model_prices()

    result = await _compute_discovery(
        live_models=catalog.models, prior_seen=prior_seen
    )
    deltas = _compute_price_deltas(
        live_models=catalog.models,
        prior_prices=prior_prices,
        threshold_pct=_PRICE_ALERT_THRESHOLD_PERCENT,
    )

    if result.notified_models:
        await notify_admins(bot, result.notified_models)
    if deltas:
        await notify_admins_of_price_deltas(
            bot, deltas, _PRICE_ALERT_THRESHOLD_PERCENT
        )

    if result.newly_seen_ids:
        inserted = await db.record_seen_models(result.newly_seen_ids)
        if result.bootstrap:
            log.info(
                "Discovery bootstrap: recorded %d model id(s) as the initial "
                "seen-set (no admin notifications sent)",
                inserted,
            )
        else:
            log.info(
                "Discovery: recorded %d newly-seen model id(s); notified admins "
                "about %d of them (the rest are filtered-out providers)",
                inserted,
                len(result.notified_models),
            )

    # Always upsert the live price snapshot — including on bootstrap
    # — so the NEXT pass has a baseline to diff against. Skipping the
    # upsert on bootstrap would postpone all price alerts by one
    # interval (6h default) for no good reason.
    current_prices = {
        m.id: (m.price.input_per_1m_usd, m.price.output_per_1m_usd)
        for m in catalog.models
    }
    if current_prices:
        processed = await db.upsert_model_prices(current_prices)
        if deltas:
            log.info(
                "Discovery: upserted %d model price snapshot(s); %d above "
                "the %.1f%% delta threshold",
                processed,
                len(deltas),
                _PRICE_ALERT_THRESHOLD_PERCENT,
            )

    return DiscoveryResult(
        total_live_models=result.total_live_models,
        newly_seen_ids=result.newly_seen_ids,
        notified_models=result.notified_models,
        bootstrap=result.bootstrap,
        price_deltas=deltas,
    )


async def discover_new_models_loop(
    bot: Bot, *, interval_seconds: int | None = None
) -> None:
    """Forever-loop wrapper around :func:`run_discovery_pass`.

    Intended to be spawned as a background task from ``main.py``.
    Swallows every exception except :class:`asyncio.CancelledError`
    so a transient OpenRouter / DB blip doesn't take the loop off
    the air.
    """
    interval = interval_seconds if interval_seconds is not None else _DISCOVERY_INTERVAL_SECONDS
    while True:
        try:
            await run_discovery_pass(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("discovery pass crashed; retrying next tick")
        else:
            # Stage-15-Step-A: heartbeat for the Prometheus
            # ``meowassist_model_discovery_last_run_epoch`` gauge.
            # ``run_discovery_pass`` itself records the
            # ``catalog_refresh`` tick after the
            # ``force_refresh`` call succeeds.
            from metrics import record_loop_tick

            record_loop_tick("model_discovery")
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
