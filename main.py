import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.base import BaseStorage
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
from dotenv import load_dotenv

import i18n_runtime
import strings
from admin import parse_admin_user_ids, router as admin_router
from admin_roles import ensure_env_admins_have_roles
from admin_toggles import load_disabled_gateways, load_disabled_models
from bot_commands import publish_bot_commands
from database import db
from handlers import SUPPORTED_PAY_CURRENCIES, router
from middlewares import UserUpsertMiddleware
from force_join import RequiredChannelMiddleware, get_required_channel
from fx_rates import refresh_usd_to_toman_loop
from metrics import install_metrics_route
from model_discovery import discover_new_models_loop
from payments import payment_webhook, refresh_min_amounts_loop
from tetrapay import tetrapay_webhook
from zarinpal import zarinpal_callback
from bot_health_alert import start_bot_health_alert_task
from pending_alert import start_pending_alert_task
from pending_expiration import start_pending_expiration_task
from zarinpal_backfill import start_zarinpal_backfill_task
from rate_limit import (
    install_webhook_rate_limit,
    register_rate_limited_webhook_path,
)
from telegram_webhook import (
    WebhookConfigError,
    install_telegram_webhook_healthz_route,
    install_telegram_webhook_ip_filter,
    install_telegram_webhook_route,
    is_webhook_mode_enabled,
    load_webhook_config,
    register_webhook_with_retry,
    remove_webhook_from_telegram,
)
from web_admin import setup_admin_routes

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bot.main")


async def start_webhook_server(
    bot: Bot, dp: Dispatcher | None = None
) -> web.AppRunner:
    """Spins up a background web server to listen for payment IPNs.

    Stage-15-Step-E #3: when ``dp`` is non-None **and**
    ``TELEGRAM_WEBHOOK_SECRET`` is set, also mounts the Telegram
    update endpoint at ``/telegram-webhook/<secret>`` on the same
    aiohttp app — same process, same port, same per-IP rate limiter.
    Default behaviour (``dp=None`` or env var unset) is unchanged:
    the IPN endpoints come up on their own and ``main`` continues
    with long-polling.
    """
    app = web.Application()
    # Per-IP token-bucket middleware first so a flood can't even reach
    # the JSON parsing / signature verification step. NowPayments'
    # legitimate retry rhythm is well under the cap (30 tokens, 5/sec
    # refill); see rate_limit.install_webhook_rate_limit.
    install_webhook_rate_limit(app)
    app["bot"] = bot  # Give the server access to the bot so it can send messages
    app.router.add_post("/nowpayments-webhook", payment_webhook)
    # Stage-11-Step-C: TetraPay (Rial card) settlement callback. Same
    # rate-limit middleware applies (a flood of forged callbacks can't
    # reach the JSON-parse / verify step). The handler itself is
    # responsible for parsing, dedupe, the authoritative ``/api/verify``
    # call, and the idempotent ``finalize_payment``.
    app.router.add_post("/tetrapay-webhook", tetrapay_webhook)
    # Stage-15-Step-E #8: Zarinpal (Rial card) callback. Unlike the
    # TetraPay POST webhook, Zarinpal redirects the user's browser
    # to this URL with ``?Authority=...&Status=OK|NOK`` query
    # parameters — so the route is GET, not POST. Same rate-limit
    # bucket (a refresh-loop on the callback URL can't bypass the
    # bucket; ``register_rate_limited_webhook_path`` adds it to the
    # filtered set the middleware reads). The handler is responsible
    # for the authoritative ``/v4/payment/verify.json`` call and the
    # idempotent ``finalize_payment`` — same defensive pattern as
    # TetraPay.
    app.router.add_get("/zarinpal-callback", zarinpal_callback)
    register_rate_limited_webhook_path(app, "/zarinpal-callback")

    # Stage-15-Step-A: Prometheus ``/metrics`` endpoint for internal
    # scraping. IP-allowlisted (default ``127.0.0.1,::1``) so a
    # leaked URL doesn't expose internal counters publicly. No
    # third-party ``prometheus_client`` dependency — the exposition
    # format is rendered by hand in ``metrics.render_metrics``.
    install_metrics_route(app)

    # Mount the web admin panel under /admin/. Same aiohttp app, same
    # process — one less thing to deploy. Auth is HMAC-cookie based,
    # gated by ADMIN_PASSWORD + ADMIN_SESSION_SECRET. If either is
    # unset the admin panel is unreachable (login refuses all attempts
    # and logs a WARNING on boot).
    setup_admin_routes(
        app,
        db=db,
        password=os.getenv("ADMIN_PASSWORD", ""),
        session_secret=os.getenv("ADMIN_SESSION_SECRET", ""),
        ttl_hours=int(os.getenv("ADMIN_SESSION_TTL_HOURS", "24")),
        # Default ON so cookies are HTTPS-only. Set ADMIN_COOKIE_SECURE=0
        # locally if you're running over plain HTTP for development.
        cookie_secure=os.getenv("ADMIN_COOKIE_SECURE", "1") != "0",
        # Stage-8-Part-5: the broadcast page's background worker
        # needs a ``Bot`` to send Telegram messages through. The
        # webhook handler still reads ``app["bot"]`` directly, so we
        # leave that legacy stash in place above — this kwarg just
        # routes the same instance into the admin plumbing via a
        # typed AppKey.
        bot=bot,
        # Stage-9-Step-3: optional TOTP / 2FA. Empty secret keeps the
        # password-only login. ``setup_admin_routes`` validates the
        # base32 string at boot so a typo surfaces immediately
        # instead of failing at first login.
        totp_secret=os.getenv("ADMIN_2FA_SECRET", ""),
        totp_issuer=os.getenv("ADMIN_2FA_ISSUER", "Meowassist Admin"),
    )

    # Stage-15-Step-E #3: opt-in Telegram webhook mode. Mounted on
    # the same aiohttp app + same port + same per-IP token bucket as
    # the IPN endpoints. We only mount the route when:
    #   * the caller passed a Dispatcher (``main`` does iff webhook
    #     mode is enabled — see the boot check below), and
    #   * ``load_webhook_config`` returns a non-None config.
    # When either is false we fall through and ``main`` continues
    # with the legacy ``dp.start_polling`` path.
    if dp is not None:
        webhook_config = load_webhook_config()
        if webhook_config is not None:
            install_telegram_webhook_route(
                app, dispatcher=dp, bot=bot, config=webhook_config
            )
            register_rate_limited_webhook_path(app, webhook_config.path)
            # Stage-15-Step-E #3 follow-up: opt-in IP allowlist
            # (no-op when TELEGRAM_WEBHOOK_IP_ALLOWLIST is unset)
            # and a stateless ``/telegram-webhook/healthz`` probe.
            install_telegram_webhook_ip_filter(
                app, config=webhook_config,
            )
            install_telegram_webhook_healthz_route(app, webhook_config)
            app["telegram_webhook_config"] = webhook_config

    port = int(os.getenv("WEBHOOK_PORT", "8080"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Payment webhook listening on port %d", port)
    return runner


def build_fsm_storage() -> BaseStorage:
    """Pick the FSM backing store based on env.

    If ``REDIS_URL`` is set, store FSM state in Redis so a bot restart
    mid-checkout doesn't trap users in ``waiting_custom_amount`` /
    ``waiting_promo_code``. Otherwise fall back to in-memory storage
    and log a loud warning — fine for local dev, NOT for production.

    Importing ``RedisStorage`` lazily so the bot doesn't hard-depend
    on the ``redis`` package when a deployer chooses memory.
    """
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        log.warning(
            "REDIS_URL is not set — falling back to in-memory FSM storage. "
            "A bot restart will lose mid-checkout state. Set REDIS_URL "
            "(e.g. redis://redis:6379/0) for production."
        )
        return MemoryStorage()
    from aiogram.fsm.storage.redis import RedisStorage  # noqa: PLC0415

    log.info("Using Redis FSM storage at %s", redis_url)
    return RedisStorage.from_url(redis_url)


async def main():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    dp = Dispatcher(storage=build_fsm_storage())

    # Upsert ``users`` row before every handler runs so anything that
    # FK-references ``users.telegram_id`` (e.g. ``transactions``) never
    # hits ``transactions_telegram_id_fkey`` for a Telegram client that
    # tapped a button without re-sending /start.
    upsert = UserUpsertMiddleware()
    dp.message.outer_middleware(upsert)
    dp.callback_query.outer_middleware(upsert)

    # Stage-13-Step-A: required-channel subscription gate. Registered
    # AFTER UserUpsertMiddleware so the users row (and therefore the
    # preferred-language column) is available when the gate renders
    # the join screen. When REQUIRED_CHANNEL is unset the middleware
    # short-circuits so existing deploys see no behaviour change. See
    # force_join.py for the full contract (admin escape hatch,
    # fail-open on Telegram-API errors, etc.).
    required_channel = get_required_channel()
    if required_channel:
        log.info(
            "force-join: enabled — required channel %r", required_channel
        )
        join_gate = RequiredChannelMiddleware()
        dp.message.outer_middleware(join_gate)
        dp.callback_query.outer_middleware(join_gate)
    else:
        log.info("force-join: disabled (REQUIRED_CHANNEL unset)")

    # Per-user chat rate limiting is implemented INSIDE the AI catch-all
    # handler via ``rate_limit.consume_chat_token`` — not as a
    # dispatcher-wide middleware, because a middleware on dp.message
    # would also fire for /start, waiting_custom_amount input, promo
    # code input, and reply-keyboard handlers, none of which cost
    # OpenRouter money. See handlers.process_chat.

    # Admin commands first so /admin* never falls through to the public
    # router's catch-all chat handler. Non-admin callers see a silent
    # no-op (see admin.is_admin) and the message keeps propagating.
    dp.include_router(admin_router)
    dp.include_router(router)

    await db.connect()
    log.info("Proxy bot is online.")

    # Seed the in-memory string-override cache from the bot_strings
    # table. Admin edits via /admin/strings refresh the cache after
    # each save; this seed handles the boot case (process restart,
    # rolling deploy, etc.). See strings.set_overrides.
    try:
        overrides = await db.load_all_string_overrides()
        strings.set_overrides(overrides)
        log.info("loaded %d bot_strings overrides from DB", len(overrides))
    except Exception:
        # Don't take the bot down for an override-load failure —
        # the compiled defaults are a safe fallback. Surface loudly
        # so ops investigates (vs. silently shipping stale text).
        log.exception(
            "failed to load bot_strings overrides — using compiled defaults"
        )

    # Stage-15-Step-E #7 follow-up #1: load community translations
    # from locale/<lang>/LC_MESSAGES/messages.po into the runtime
    # cache. These sit BETWEEN the admin-override cache (still top
    # priority) and the compiled-default _STRINGS table — so a
    # translator can drop an edited messages.po into the locale
    # directory and the bot picks up the new strings on the next
    # process restart without a code deploy. init_translations
    # never raises (per-locale errors are logged and the affected
    # locale falls through to its compiled default), so a missing
    # or malformed .po file can't take the bot down.
    try:
        counts = i18n_runtime.init_translations()
        log.info("loaded community translations: %s", counts)
    except Exception:
        log.exception(
            "i18n_runtime.init_translations failed — falling back "
            "to compiled defaults for every locale"
        )

    # Stage-14: warm the admin-toggle caches so disabled models and
    # gateways are filtered from the very first request.
    await load_disabled_models(db)
    await load_disabled_gateways(db)

    # Stage-15-Step-E #10b row 2: warm the COST_MARKUP override
    # cache so the very first paid request sees the operator's
    # configured markup rather than the env / compile-time default
    # for the time window between boot and the first hit on
    # ``/admin/monetization``. Best-effort — pricing.get_markup()
    # falls through to env / default if the refresh raises.
    try:
        import pricing
        await pricing.refresh_markup_override_from_db(db)
        log.info(
            "loaded COST_MARKUP override from system_settings: %s "
            "(source=%s, effective=%.4fx)",
            pricing.get_markup_override(),
            pricing.get_markup_source(),
            pricing.get_markup(),
        )
    except Exception:
        log.exception(
            "failed to load COST_MARKUP override from DB — "
            "falling through to env / compile-time default"
        )

    # Stage-15-Step-F follow-up #4: warm the bot-health threshold
    # override cache. Same shape as the markup load above.
    try:
        from bot_health import refresh_threshold_overrides_from_db
        snapshot = await refresh_threshold_overrides_from_db(db)
        if snapshot:
            log.info(
                "loaded BOT_HEALTH_* overrides from system_settings: %s",
                sorted(snapshot.keys()),
            )
    except Exception:
        log.exception(
            "failed to load BOT_HEALTH_* overrides from DB — "
            "falling through to env / compile-time default"
        )

    # Stage-15-Step-E #10b row 4: warm the MIN_TOPUP_USD override
    # cache so the very first top-up attempt is gated against the
    # operator's configured floor rather than the env / compile-time
    # default. Same fail-soft shape as the markup load above.
    try:
        import payments
        loaded = await payments.refresh_min_topup_override_from_db(db)
        log.info(
            "loaded MIN_TOPUP_USD override from system_settings: %s "
            "(source=%s, effective=$%.2f)",
            loaded,
            payments.get_min_topup_source(),
            payments.get_min_topup_usd(),
        )
    except Exception:
        log.exception(
            "failed to load MIN_TOPUP_USD override from DB — "
            "falling through to env / compile-time default"
        )

    # Stage-15-Step-E #10b row 5: warm the REQUIRED_CHANNEL override
    # cache so the very first incoming update is gated against the
    # operator's configured channel rather than the env / compile-time
    # default. Same fail-soft shape as the markup / min-topup loads
    # above. Best-effort — ``force_join.get_required_channel()`` falls
    # through to env / "" if the refresh raises.
    try:
        import force_join
        loaded_channel = await (
            force_join.refresh_required_channel_override_from_db(db)
        )
        log.info(
            "loaded REQUIRED_CHANNEL override from system_settings: %r "
            "(source=%s, effective=%r)",
            loaded_channel,
            force_join.get_required_channel_source(),
            force_join.get_required_channel(),
        )
    except Exception:
        log.exception(
            "failed to load REQUIRED_CHANNEL override from DB — "
            "falling through to env / compile-time default"
        )

    # Overwrite BotFather's cached slash-command list with the
    # canonical one. Without this, Telegram shows whatever was last
    # typed into the BotFather "Edit Commands" panel — including
    # leftover entries the bot has no handlers for. See bot_commands.
    admin_ids = parse_admin_user_ids(os.getenv("ADMIN_USER_IDS"))
    await publish_bot_commands(bot, admin_ids)

    # Stage-15-Step-E #5 follow-up #3: auto-promote env-list admins
    # to a real ``admin_roles`` row so the DB is the source of
    # truth. Best-effort — any DB failure is logged and the env-list
    # fallback in :func:`admin_roles.effective_role` keeps the legacy
    # admin surface working until the next boot.
    try:
        promote_counts = await ensure_env_admins_have_roles(
            db, admin_ids
        )
        log.info(
            "ensure_env_admins_have_roles: promoted=%d "
            "skipped_existing=%d skipped_invalid=%d errors=%d",
            promote_counts["promoted"],
            promote_counts["skipped_existing"],
            promote_counts["skipped_invalid"],
            promote_counts["errors"],
        )
    except Exception:
        log.exception(
            "ensure_env_admins_have_roles boot hook failed; "
            "continuing with env-list fallback"
        )

    # Stage-15-Step-E #3: pre-resolve the webhook config so a typo in
    # ``TELEGRAM_WEBHOOK_SECRET`` halts boot here (loud) rather than
    # silently failing every Telegram update later. When the env var
    # is unset, ``load_webhook_config`` returns None and the bot
    # continues in long-polling mode — backward-compatible default.
    if is_webhook_mode_enabled():
        try:
            webhook_config = load_webhook_config()
        except WebhookConfigError as exc:
            log.error("Telegram webhook config error: %s", exc)
            raise
    else:
        webhook_config = None

    runner = await start_webhook_server(
        bot, dp=dp if webhook_config is not None else None
    )

    # Stage-9-Step-5: background reaper for stuck PENDING transactions.
    # Wakes every PENDING_EXPIRATION_INTERVAL_MIN minutes (default 15)
    # and flips PENDING rows older than PENDING_EXPIRATION_HOURS
    # (default 24) to EXPIRED. Without this, abandoned NowPayments
    # invoices accumulate forever in the ledger. See
    # pending_expiration.py for the full contract.
    expiration_task = start_pending_expiration_task(bot)

    # Stage-15-Step-E #8 follow-up #2: Zarinpal browser-close backfill
    # reaper. Wakes every ZARINPAL_BACKFILL_INTERVAL_MIN minutes
    # (default 5) and verifies any PENDING Zarinpal row in the
    # window (min_age, max_age) — crediting orders the gateway
    # settled but whose user-redirect callback never landed
    # (browser-close race). See zarinpal_backfill.py for the full
    # contract + jurisdictional split with the expire reaper.
    zarinpal_backfill_task = start_zarinpal_backfill_task(bot)

    # Stage-12-Step-B: proactive admin alert loop for stuck PENDING
    # transactions. Wakes every PENDING_ALERT_INTERVAL_MIN minutes
    # (default 30) and DMs admins about PENDING rows older than
    # PENDING_ALERT_THRESHOLD_HOURS (default 2). Per-row dedupe is
    # by hour-bucket so the same stuck row alerts at most once per
    # crossed integer hour. See pending_alert.py for the full
    # contract + per-admin fault isolation policy.
    pending_alert_task = start_pending_alert_task(bot)

    # Stage-15-Step-F follow-up: bot-health alert loop. Wakes every
    # BOT_HEALTH_ALERT_INTERVAL_SECONDS (default 60), runs the same
    # bot_health classifier the dashboard / Prometheus / control
    # panel use, and DMs admins on transitions to DEGRADED /
    # UNDER_ATTACK / DOWN (and on recovery). Per-level dedupe with
    # an hour anchor; the loop never crashes on a single tick error.
    # See bot_health_alert.py for the full contract.
    bot_health_alert_task = start_bot_health_alert_task(bot)

    # Background refresher for NowPayments per-currency min-amounts.
    # Keeps the in-memory cache warm so the checkout pre-flight check
    # (see handlers._preflight_min_amount_check) never blocks on a
    # cold-cache HTTP call and so ops sees fresh minimums within the
    # refresh interval (15 min by default). Cheap: ~18 HTTP calls per
    # pass, gated by a concurrency=3 semaphore inside the loop.
    tickers = [ticker for _, ticker in SUPPORTED_PAY_CURRENCIES]
    min_amount_refresher = asyncio.create_task(
        refresh_min_amounts_loop(tickers),
        name="min-amount-refresher",
    )

    # Stage-10-Step-C: background loop that diffs the live OpenRouter
    # catalog against the ``seen_models`` watermark and DMs admins
    # about genuinely new models in the prominent-provider allowlist.
    # First-run deploys silently bootstrap the seen-set (no DMs) so
    # admins aren't spammed with the full catalog on day 1. See
    # ``model_discovery.py`` for the full contract.
    model_discovery_task = asyncio.create_task(
        discover_new_models_loop(bot),
        name="model-discovery",
    )

    # Stage-11-Step-A: USD→Toman FX refresher. Polls the configured
    # rate source every ``FX_REFRESH_INTERVAL_SECONDS`` (default 10
    # min), persists the snapshot to ``fx_rates_snapshot``, and DMs
    # admins on rate moves above ``FX_RATE_ALERT_THRESHOLD_PERCENT``
    # (default 10%). The wallet UI (Stage-11-Step-D) and Toman
    # top-up entry (Stage-11-Step-B) read the cache this loop
    # maintains. See ``fx_rates.py`` for the cache-preservation
    # and plausibility-bound semantics.
    fx_refresher_task = asyncio.create_task(
        refresh_usd_to_toman_loop(bot),
        name="fx-refresher",
    )

    try:
        if webhook_config is not None:
            # Stage-15-Step-E #3: webhook mode. The aiohttp app
            # (mounted in ``start_webhook_server`` above) is already
            # serving the Telegram update endpoint; tell Telegram
            # where to deliver updates and then block forever
            # waiting for shutdown. The dispatcher's startup hooks
            # were wired in by ``setup_application`` inside
            # ``install_telegram_webhook_route``.
            #
            # Stage-15-Step-E #3 follow-up: retry-with-backoff on
            # transient 5xx / network errors from the Bot API
            # (3 attempts, 1s/2s exponential backoff by default).
            # A ``TelegramBadRequest`` (HTTP 400 — typo'd URL or
            # invalid secret_token shape) is NOT retried; that's
            # a deploy-side typo and burning retries on it just
            # delays the loud failure the operator needs to fix.
            await register_webhook_with_retry(bot, webhook_config)
            log.info(
                "Bot is in webhook mode; updates flow via "
                "%s. Long-polling is suspended for the duration "
                "of this process.",
                webhook_config.url,
            )
            stop = asyncio.Event()
            try:
                await stop.wait()
            finally:
                await remove_webhook_from_telegram(bot)
        else:
            await dp.start_polling(bot)
    finally:
        fx_refresher_task.cancel()
        try:
            await fx_refresher_task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("fx-refresher loop exited with error")
        model_discovery_task.cancel()
        try:
            await model_discovery_task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("model-discovery loop exited with error")
        min_amount_refresher.cancel()
        try:
            await min_amount_refresher
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("min-amount refresher exited with error")
        expiration_task.cancel()
        try:
            await expiration_task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("pending-expiration reaper exited with error")
        zarinpal_backfill_task.cancel()
        try:
            await zarinpal_backfill_task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("zarinpal-backfill reaper exited with error")
        pending_alert_task.cancel()
        try:
            await pending_alert_task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("pending-alert loop exited with error")
        bot_health_alert_task.cancel()
        try:
            await bot_health_alert_task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("bot-health-alert loop exited with error")
        await runner.cleanup()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
