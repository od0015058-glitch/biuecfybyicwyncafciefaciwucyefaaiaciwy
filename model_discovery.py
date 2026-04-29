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
import os
from dataclasses import dataclass

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError

from admin import get_admin_user_ids
from database import db
from models_catalog import CatalogModel, get_catalog

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
    """One discovery pass: fetch catalog, diff, notify, persist.

    Extracted from the forever-loop so the boot path (or a manual
    ``/admin/refresh_models`` trigger in the future) can run exactly
    one pass on demand.
    """
    catalog = await get_catalog()
    prior = await db.get_seen_model_ids()
    result = await _compute_discovery(
        live_models=catalog.models, prior_seen=prior
    )

    if result.notified_models:
        await notify_admins(bot, result.notified_models)

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
    return result


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
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
