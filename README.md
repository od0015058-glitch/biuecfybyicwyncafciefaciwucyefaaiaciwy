# Meowassist AI Telegram Bot

A Persian/English Telegram bot that proxies user prompts to OpenRouter LLMs,
charges per-token cost (with markup) from a wallet, and tops up wallets via
NowPayments crypto invoices.

**Features**
- Wallet top-ups via NowPayments (BTC / ETH / LTC / TON / TRX / USDT on TRC20·ERC20·BEP20·TON).
- Per-model pricing pulled live from OpenRouter (24 h cache).
- Promo codes — admin-issued bonus % or $ applied during paid invoices.
- **Gift codes** — admin-issued codes that directly credit balance (no
  purchase required). Users redeem with `/redeem CODE`; admin manages
  them at `${WEBHOOK_BASE_URL}/admin/gifts`.
- Web admin panel at `${WEBHOOK_BASE_URL}/admin/` (login, dashboard,
  promos, gifts, users — search by id/username and credit/debit from
  the browser — broadcast with a live progress bar, and a paginated
  transactions browser with gateway / status / user filters).
- Telegram-side admin commands (`/admin`, `/admin_metrics`,
  `/admin_credit`, `/admin_broadcast`, …) for ops via DMs.
- **Canonical slash-command menu** — on every startup the bot
  publishes its user-facing command list (`/start`, `/redeem`) via
  `Bot.set_my_commands` so Telegram's `/` popup never shows stale
  entries left over from BotFather's "Edit Commands" panel. Admin
  commands are scoped per-admin via `BotCommandScopeChat` so
  non-admins don't see them.
- **Wallet-menu redemption** — alongside the existing `/redeem CODE`
  command, the wallet inline menu now exposes a "🎁 Redeem gift code"
  button that prompts for the code and reuses the same eligibility
  pipeline.
- **Hub buttons** — top-level menu has `Wallet · Models · New Chat ·
  Memory: ON/OFF · Support · Language`. Tapping "🆕 New Chat" wipes
  the conversation buffer immediately; tapping "🧠 Memory" opens
  the memory settings screen with the cost trade-off explainer.
- **Editable bot text** — every user-facing label (button, prompt,
  error message) is editable at `${WEBHOOK_BASE_URL}/admin/strings`.
  The compiled defaults in `strings.py` ship with the code; admin
  edits write a per-`(lang, key)` row to `bot_strings` and refresh
  an in-memory cache so the next message uses the new text. Reverting
  resurrects the compiled default. Missing-slug typos now log a
  WARNING instead of silently shipping the slug to the user.
- **User-field editor + admin audit log** — `/admin/users/{id}` now
  edits language, active model, memory toggle, free-message counter,
  and username (allow-listed via `Database.USER_EDITABLE_FIELDS`) in
  addition to balance credit/debit. Every admin POST (login, promo
  create/revoke, gift create/revoke, user adjust, user edit, broadcast
  start, string save/revert) writes a row to the new `admin_audit_log`
  table. View the feed at `${WEBHOOK_BASE_URL}/admin/audit` with
  optional action/actor filters. Audit writes are best-effort — a
  failed audit insert never blocks the underlying admin operation.
- **Wallet shows USD + live Toman equivalent** — Iranian users see
  their USD balance with a `≈ N تومان` annotation computed from the
  live USDT/IRR ticker (default source: `api.nobitex.ir`). The wallet
  itself stays denominated in USD — Toman is display-only — so a
  swing in the rial doesn't change the user's purchasing power. When
  the FX cache is stale (rate-source down for >40 min) the line
  suffixes a `(approx)` marker so the figure is shown as
  informational, not a quote. Cold-cache deploys silently drop the
  line until the first refresh lands, rather than rendering `≈ 0
  تومان`.
- **Required-channel subscription gate** — set `REQUIRED_CHANNEL`
  (e.g. `@MeowAssist_Channel`, or a `-100…` numeric id for a
  private channel) to force every non-admin user to join your
  announcement channel before they can use the bot. They get a
  "Please join @channel" screen with a Join button + an "✅ I've
  joined" re-check button until Telegram confirms membership.
  Admins (`ADMIN_USER_IDS`) always bypass so you can't lock
  yourself out. The bot must be added as an administrator of the
  channel — Telegram only exposes membership status for chats the
  bot administers. If the bot isn't admin yet, the gate fails OPEN
  with a logged WARNING so a misconfiguration doesn't brick every
  user. Leave `REQUIRED_CHANNEL` unset to keep the bot fully open
  (back-compat default). For private channels, also set
  `REQUIRED_CHANNEL_INVITE_LINK=https://t.me/+abcdefINVITE` so the
  Join button has somewhere to deep-link.
- **Referral codes** — every user can generate a one-tap invite
  deep-link (`/wallet → 🎁 Invite a friend`). When a friend opens
  the bot via that link and completes their first paid top-up,
  both wallets get a percentage bonus (defaults to 10% capped at
  $5 per side). Set `BOT_USERNAME` to the bot's `@handle` (without
  the `@`) so the screen renders the share URL — leaving it unset
  falls back to a copy-paste-only flow with the bare code + a
  `/start ref_<code>` instruction. Tune the economics with
  `REFERRAL_BONUS_PERCENT` / `REFERRAL_BONUS_MAX_USD`. The credit
  fires inside the same DB transaction as the triggering top-up
  (PARTIAL or SUCCESS, whichever comes first), so an IPN replay
  cannot double-credit, and an invitee can be claimed by at most
  one referrer (UNIQUE constraint on `referral_grants.invitee_telegram_id`).
- **Per-user in-flight cap on AI chat** — at most one OpenRouter
  request per Telegram user can be in flight at any moment. The
  existing per-user *token bucket* (`consume_chat_token`) gates
  sustained spend, but its 5-token capacity lets a fast burst of 5
  prompts hit OpenRouter in parallel before the bucket reacts — on
  a paid model that drains $5+ from the wallet in under a second
  before the user can react. The in-flight slot is the second
  layer: a second prompt arriving while the first is still being
  awaited gets the `ai_chat_busy` flash ("Your previous message
  is still being processed. Please wait…") instead of silently
  hitting OpenRouter again. Slot is released in a `try…finally`
  so an exception can't permanently lock the user out, and the
  set is bounded at 10 000 entries with FIFO eviction as defence
  against a slow leak.
- **TOTP / 2FA on admin login** — set `ADMIN_2FA_SECRET` to a base32
  string and `/admin/login` will require a 6-digit code from your
  authenticator app (Google Authenticator, Authy, 1Password,
  Bitwarden) in addition to `ADMIN_PASSWORD`. The check runs *after*
  the password compare so an attacker without the password can't use
  the form to brute-force the TOTP code. Provision a fresh secret at
  `${WEBHOOK_BASE_URL}/admin/enroll_2fa` (renders an inline-SVG QR
  + the manual key + an `otpauth://` URI). Leave `ADMIN_2FA_SECRET`
  unset to keep the existing password-only login flow.
- **AI model toggles** — admin can disable individual OpenRouter models
  at `${WEBHOOK_BASE_URL}/admin/models`. Disabled models disappear
  from the Telegram model picker and are refused at chat time. Models
  are grouped by provider (OpenAI, Anthropic, Google, xAI, DeepSeek)
  with a live search filter. Toggle actions are audit-logged.
- **Payment gateway toggles** — admin can disable TetraPay or any
  NowPayments crypto currency at `${WEBHOOK_BASE_URL}/admin/gateways`.
  Disabled gateways/currencies disappear from the payment picker.
  Pending invoices on a disabled gateway are not affected.
- **Multi-key OpenRouter load balancing** — set `OPENROUTER_API_KEY_1`
  through `OPENROUTER_API_KEY_10` to spread traffic across multiple
  accounts. Each user sticks to one key (`telegram_id % N`) so
  conversation context stays consistent. If only the bare
  `OPENROUTER_API_KEY` is set, all traffic goes there (backward-
  compatible). See `.env.example` for details. Keys are read
  **lazily** on the first call to `key_for_user()` / `key_count()`
  so an importer that never needs OpenRouter (a small DB-only
  script, a focused test) doesn't trigger a spurious "no keys
  configured" warning at import time.
- **Prometheus `/metrics` endpoint** — the same aiohttp server that
  hosts the IPN webhooks publishes `GET /metrics` in Prometheus
  text-exposition format for internal scraping (Prometheus,
  Grafana Agent, VictoriaMetrics — no third-party
  `prometheus_client` dependency in the bot itself). Output covers
  per-loop heartbeat epochs (FX refresh, model discovery, catalog
  refresh, NowPayments min-amount refresh, pending alert / reaper
  loops), IPN drop counters (NowPayments + TetraPay) broken down by
  reason, the in-flight chat-slot gauge, the count of
  admin-disabled models / gateways, and the size of the OpenRouter
  key pool. The endpoint is gated by `METRICS_IP_ALLOWLIST`
  (comma-separated IPs / CIDRs, default `127.0.0.1,::1`); an empty
  allowlist locks every request out (fail-closed). Sample scrape
  config:
  ```yaml
  scrape_configs:
    - job_name: meowassist
      scrape_interval: 30s
      static_configs:
        - targets: ['127.0.0.1:8080']
  ```
  Sample alert (a stuck reaper goes silent for >15 minutes):
  ```yaml
  - alert: MeowassistReaperStuck
    expr: time() - meowassist_pending_reaper_last_run_epoch > 900
    for: 5m
  ```
- **IPN-health dashboard tile** — `/admin/` shows a per-process
  drop-counter table for both NowPayments and TetraPay so an
  operator can spot a misconfigured webhook (signature mismatch,
  unknown invoice, transient verify failures) at a glance without
  shelling into the Prometheus scrape. Counters reset on every
  bot restart; for long-running history, pull `/metrics` into
  Prometheus. Gateway tiles are independently fault-isolated — a
  future regression in one accessor cannot blank the other half.
- **Conversation history export** — the memory screen now has a
  "📥 Export conversation" button that ships the user's full
  persisted buffer back as a `.txt` document (role labels +
  ISO-8601 UTC timestamps + a summary header). Hard 1 MB cap;
  oldest messages are trimmed first when a heavy buffer
  overflows, with the trim count surfaced in both the in-file
  header and the upload caption (caption used to lie pre-Step-E
  #2 — see HANDOFF §5 Step-E #2 bundled bug fix).
- **Per-user spending dashboard** — a new "📊 My usage stats"
  button on the wallet menu opens a per-user analytics screen
  showing lifetime totals (calls / tokens / spent), the same
  totals over a rolling 30-day window, and the user's top 5
  models by call count. Same SQL shape as the admin-side
  dashboard; the DB method (`Database.get_user_spending_summary`)
  hard-codes `WHERE telegram_id = $1` on every sub-query so a
  buggy caller can't leak someone else's totals. First slice of
  Stage-15-Step-E #2.
- **Opt-in Telegram webhook mode** — set `TELEGRAM_WEBHOOK_SECRET`
  to switch from long-polling to webhook delivery. The bot mounts
  a `POST /telegram-webhook/<secret>` route on the same aiohttp
  app + port as the IPN endpoints, registers the URL with
  Telegram via `Bot.set_webhook` (with the same secret as the
  `X-Telegram-Bot-Api-Secret-Token` header — both layers must
  match for an update to be accepted), and shares the per-IP
  rate-limit bucket with NowPayments / TetraPay. Reduces latency
  vs. the long-polling cycle and saves the polling task's idle
  CPU. **Backward-compatible default**: when the env var is unset,
  the bot continues to use long-polling exactly as before. See
  `.env.example` (Stage-15-Step-E #3 block) for the recovery
  procedure if you flip back.

For the full project history, file map, and roadmap **read [HANDOFF.md](./HANDOFF.md)**.

## Quick start (Docker — recommended)

```bash
git clone <repo>
cd <repo>
cp .env.example .env       # fill in BOT_TOKEN / OPENROUTER_API_KEY / NOWPAYMENTS_* / DB_PASSWORD / WEBHOOK_BASE_URL
docker compose up -d --build
docker compose logs -f bot
```

Compose boots Postgres + Redis + the bot together. The bot's `entrypoint.sh`
runs `alembic upgrade head` on every container start so schema migrations
are applied automatically (idempotent — no-op when already at head).
Redis backs aiogram's FSM so a bot restart doesn't trap users
mid-checkout. The webhook listener is published to `127.0.0.1:8080`
only — put nginx/Caddy/Cloudflare Tunnel in front for TLS so NowPayments
can reach `${WEBHOOK_BASE_URL}/nowpayments-webhook` over HTTPS.

First-time deploy on an existing prod DB (one that already has the
schema before Alembic was introduced)? Stamp it once before bringing
everything up so the auto-upgrade is a no-op:
```bash
docker compose run --rm bot alembic stamp head
docker compose up -d
```

To roll back: `docker compose down && git checkout <previous-sha> && docker compose up -d --build`.

## Quick start (manual)

1. **Clone + install**
   ```bash
   git clone <repo>
   cd <repo>
   python3.11 -m venv venv && source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **PostgreSQL**
   ```bash
   sudo -u postgres createuser botuser -P            # set DB_PASSWORD
   sudo -u postgres createdb -O botuser aibot_db
   alembic upgrade head                               # apply schema via alembic
   ```

   For an existing prod DB that pre-dates Alembic (one created before
   PR #44 — i.e. already has the bot's tables but no `alembic_version`
   row), stamp it once instead of upgrading, otherwise the first
   `alembic upgrade head` will fail with `relation users already exists`:
   ```bash
   alembic stamp head
   ```

3. **Configure** — copy `.env.example` to `.env` and fill in:
   - `BOT_TOKEN` from @BotFather
   - `OPENROUTER_API_KEY` from <https://openrouter.ai/settings/keys>
   - `NOWPAYMENTS_API_KEY` + `NOWPAYMENTS_IPN_SECRET` from your NowPayments
     store settings
   - `WEBHOOK_BASE_URL` — public HTTPS URL where this bot is reachable
     (NowPayments will POST IPNs to `${WEBHOOK_BASE_URL}/nowpayments-webhook`)
   - DB credentials, `ADMIN_USER_IDS` if you have them
   - `REDIS_URL` (e.g. `redis://localhost:6379/0`) for production. If
     unset, the bot logs a WARNING and uses in-memory FSM storage —
     fine for local dev but a bot restart loses every user's
     mid-checkout state.
   - `ADMIN_PASSWORD` + `ADMIN_SESSION_SECRET` if you want the **web
     admin panel** at `${WEBHOOK_BASE_URL}/admin/` (Stage-8). Without
     them the panel is unreachable. Generate a secret with
     `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
     Set `ADMIN_COOKIE_SECURE=0` ONLY when running over plain HTTP
     locally — the default is HTTPS-only.
   - `ADMIN_2FA_SECRET` (optional) — base32 TOTP secret. When set,
     `/admin/login` requires a 6-digit code from your authenticator
     app in addition to `ADMIN_PASSWORD`. Provision via
     `${WEBHOOK_BASE_URL}/admin/enroll_2fa` after first login.
     `ADMIN_2FA_ISSUER` (default `Meowassist Admin`) labels the entry
     in the authenticator app.
   - `TRUST_PROXY_HEADERS=1` if the bot runs behind a reverse proxy
     (Cloudflare Tunnel, nginx, Caddy). When set, the per-IP rate
     limiters (webhook + `/admin/login`) key on the leftmost
     `X-Forwarded-For` IP instead of the proxy's TCP peer address —
     so a login sprayer can't use the tunnel IP to bucket-share
     with legitimate admins. Leave unset on direct-internet deploys
     because the header can be spoofed by arbitrary clients.
   - `REQUIRED_CHANNEL` (optional, Stage-13-Step-A) — when set to a
     public `@handle` (e.g. `@MeowAssist_Channel`) or a numeric
     `-100…` chat id for a private channel, every non-admin user
     must join that channel before the bot becomes interactive.
     The bot must be added as an **administrator** of the channel
     so Telegram lets it call `getChatMember`. Pair with
     `REQUIRED_CHANNEL_INVITE_LINK` for private channels so the
     join button has a deep-link target. Leave unset to keep the
     bot fully open (default). Admins (`ADMIN_USER_IDS`) always
     bypass the gate.
   - `BOT_USERNAME` (optional, Stage-13-Step-C) — the bot's
     `@handle` without the leading `@` (e.g. `Meowassist_Ai_bot`).
     Used to synthesise the share deep-link
     `https://t.me/<handle>?start=ref_<code>` rendered on the
     `/wallet → 🎁 Invite a friend` screen. Leave unset and the
     screen falls back to a copy-paste-only flow (the bare code +
     a `/start ref_<code>` instruction); the feature still works,
     just without a one-tap link.
   - `REFERRAL_BONUS_PERCENT` (optional, default `10`) —
     percentage of the invitee's first paid top-up credited to
     **both** wallets (referrer + invitee). Capped by
     `REFERRAL_BONUS_MAX_USD`. Non-finite / non-positive values
     fall back to the default with a logged WARNING.
   - `REFERRAL_BONUS_MAX_USD` (optional, default `5`) — per-side
     cap on the referral bonus. So a $20 first-top-up triggers
     10% × $20 = $2 to each side; a $100 first-top-up triggers
     $5 (the cap), not $10.

4. **Run**
   ```bash
   python main.py
   ```

   The bot starts long-polling Telegram and a webhook listener on
   `WEBHOOK_PORT` (default `8080`). Put it behind nginx/Caddy + certbot or a
   Cloudflare tunnel so NowPayments can reach `${WEBHOOK_BASE_URL}/nowpayments-webhook`
   over HTTPS.

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/
```

## Source map

| File | Purpose |
| --- | --- |
| `main.py` | Entrypoint. Boots aiogram dispatcher, registers middleware, calls `bot_commands.publish_bot_commands` to overwrite BotFather's slash-command list, starts the IPN HTTP listener. |
| `bot_commands.py` | Canonical Telegram slash-command publisher. `PUBLIC_COMMANDS` (everyone sees) + `ADMIN_COMMANDS` (per-admin via `BotCommandScopeChat`). Idempotent; errors are logged and swallowed so a transient network blip during startup doesn't take the bot down. |
| `database.py` | asyncpg pool + every SQL query. Money methods use `SELECT … FOR UPDATE` inside connection-scoped transactions. `deduct_balance` refuses NaN / ±Infinity *and* finite-negative `cost_usd` so the canonical wallet-debit path can't be flipped into a silent un-audited credit (defense-in-depth — `pricing._apply_markup` already clamps to `[0, ∞)` upstream). `redeem_gift_code` re-checks the row's `amount_usd` is finite *inside* the open transaction so a corrupted `gift_codes` row (legacy NaN, manual SQL fix, etc.) rolls back cleanly instead of bricking the redeemer's wallet via `balance_usd + NaN`. `log_usage` mirrors the same finite + non-negative guard on `cost` so a NaN / ±Infinity / negative value can't poison `usage_logs.cost_deducted_usd` and break every dashboard aggregate (`spend_usd`, `top_models`, per-user totals). `get_system_metrics` returns `pending_payments_count` + `pending_payments_oldest_age_hours` for the Stage-9-Step-9 dashboard tile. **Stage-12-Step-A:** the terminal-state surface is split — `mark_transaction_terminal` accepts only `EXPIRED` / `FAILED` (the canonical `TERMINAL_FAILURE_STATUSES` set), the gateway-side IPN refund routes through `mark_payment_refunded_via_ipn` (no wallet write), and the admin-issued `refund_transaction(transaction_id, reason, admin_telegram_id)` is the only path that flips a SUCCESS row to `REFUNDED` *and* debits the wallet. Two-row `FOR UPDATE` lock (transactions then users), refuses non-SUCCESS rows / `admin` + `gift` gateways / would-go-negative balances, and writes the new `refunded_at` + `refund_reason` columns from alembic 0012. |
| `payments.py` | NowPayments invoice creation, IPN verification (HMAC-SHA512), idempotent finalize, partial-payment crediting. IPN replay-dedupe via `payment_status_transitions` (`UNIQUE(gateway_invoice_id, payment_status)`) — duplicate `(invoice, status)` deliveries drop with a 200 before any state mutation. Per-process drop counters (`bad_signature`, `bad_json`, `missing_payment_id`, `replay`) exposed via `get_ipn_drop_counters()` for ops dashboards. **Per-currency minimum-payment enforcement**: `GLOBAL_MIN_TOPUP_USD` (default $2, env-overridable via `MIN_TOPUP_USD`) + cached `/v1/min-amount` lookup (`get_min_amount_usd`) + background refresher (`refresh_min_amounts_loop`, default every 15 min) + `find_cheaper_alternative` so the checkout flow refuses sub-minimum amounts with "min for BTC is $10, pay $3 with USDT-TRC20 instead" instead of a generic rejection. |
| `tetrapay.py` | TetraPay (Iranian Shaparak / Rial card) gateway. Stage-11-Step-C. `create_order` POSTs to `https://tetra98.com/api/create_order` and returns a `TetraPayOrder` with the redirect `payment_url`, `Authority` (used as our `gateway_invoice_id`), `tracking_id`, and the integer `amount_irr` (rial). `verify_payment` is the *authoritative* settlement check — the user-side webhook callback is never trusted alone (a forged callback would otherwise credit a wallet without payment). `tetrapay_webhook` is mounted at `/tetrapay-webhook`: parse JSON → dedupe via `payment_status_transitions` → drop on non-"100" status → look up the locked USD figure on the PENDING row → call `verify_payment` → call `Database.finalize_payment(authority, locked_usd)`. The credit amount is the USD equivalent **locked at order creation**, never recomputed at settlement: Iranian banks regularly take minutes for Shaparak 3DS round-trips and the rial can move materially during that window. Drop counters (`bad_json`, `missing_authority`, `non_success_callback`, `unknown_invoice`, `verify_failed`, `replay`) mirror the NowPayments path's `_IPN_DROP_COUNTERS` for ops visibility. |
| `payments.py` | NowPayments invoice creation, IPN verification (HMAC-SHA512), idempotent finalize, partial-payment crediting. |
| `pending_expiration.py` | Background reaper task. Wakes every `PENDING_EXPIRATION_INTERVAL_MIN` (default 15) minutes, calls `Database.expire_stale_pending` to flip stuck `PENDING` rows older than `PENDING_EXPIRATION_HOURS` (default 24) to `EXPIRED`, drops a `payment_expired` audit row (`actor="reaper"`), and pings the affected user. `TelegramForbiddenError` / `TelegramBadRequest` are swallowed. Spawned by `main.main` after the webhook server, cancelled cleanly on shutdown. |
| `pending_alert.py` | **Stage-12-Step-B.** Background loop that *notifies* about stuck `PENDING` rows long before the reaper *closes* them. Wakes every `PENDING_ALERT_INTERVAL_MIN` (default 30) minutes, calls `Database.list_pending_payments_over_threshold(threshold_hours=PENDING_ALERT_THRESHOLD_HOURS)` (default 2 h), and DMs every `ADMIN_USER_IDS` with a "⚠️ N pending payment(s) stuck over Xh" summary (max 10 lines, overflow footer). Per-row dedupe keyed on `(transaction_id, floor(age_hours))` so the same stuck row alerts once per crossed integer-hour boundary. Per-admin fault isolation mirrors `model_discovery.notify_admins_of_price_deltas`: `TelegramForbiddenError` per admin is logged INFO; `TelegramAPIError` is logged + swallowed; the loop never crashes. Spawned by `main.main`, cancelled cleanly on shutdown. The `get_pending_alert_threshold_hours()` helper is also imported by `web_admin.dashboard` so the "Pending payments" tile shows the same overdue count the alert DM uses. |
| `handlers.py` | All aiogram handlers — `/start`, hub UI, charge flow, model picker, language picker, support. The two `edit_text` no-op silencers (`_edit_to_hub` for the universal "🏠 Back to menu" button, `_render_memory_screen` for the memory-toggle screen) wrap their calls in `except TelegramBadRequest:` only, so unrelated `TelegramForbiddenError` (bot blocked), `TelegramRetryAfter`, or aiohttp network blips propagate to logs / the dispatcher's error handler instead of being silenced as a single `log.debug` line. |
| `ai_engine.py` | OpenRouter call, cost calc, balance deduct, optional conversation memory. Defense-in-depth: a non-finite `users.balance_usd` (NaN / +Infinity from a legacy poisoned row) is treated as $0 for the insufficient-funds gate so a corrupted wallet can't silently bypass the gate and grant unlimited free chat at the bot's expense. |
| `pricing.py` | Per-model price table + `COST_MARKUP` env var (default 1.5×). |
| `models_catalog.py` | Live `/v1/models` fetch from OpenRouter with 24 h cache, provider whitelist, free/paid split. |
| `middlewares.py` | `UserUpsertMiddleware` — ensures `users` row exists before any handler runs. |
| `force_join.py` | `RequiredChannelMiddleware` + the `force_join_check` callback handler. When `REQUIRED_CHANNEL` is set, every non-admin user is gated behind a "join the channel" screen until Telegram confirms membership. Admins bypass; API errors fail open. |
| `referral.py` | Stage-13-Step-C. Env-var config (`BOT_USERNAME` / `REFERRAL_BONUS_PERCENT` / `REFERRAL_BONUS_MAX_USD`), the `/start <payload>` parser (the bundled bug-fix that finally inspects the deep-link payload `cmd_start` ignored pre-PR-110), the share-URL builder, and the thin wrapper around `Database._grant_referral_in_tx` that the finalize-payment open-TX calls. The DB-layer primitives (`get_or_create_referral_code`, `claim_referral`, `_grant_referral_in_tx`, `get_referral_stats`) live in `database.py`. |
| `rate_limit.py` | Token-bucket primitives + `consume_chat_token` (per-user throughput throttle) + `try_claim_chat_slot` / `release_chat_slot` (per-user in-flight cap) + `chat_inflight_count` (read-only accessor for the Stage-15-Step-A metrics gauge) and `webhook_rate_limit_middleware` (per-IP). Guards the AI chat path against runaway OpenRouter spend on both axes (sustained rate and burst concurrency) and the `/nowpayments-webhook` endpoint against DoS bursts. |
| `metrics.py` | Stage-15-Step-A. Prometheus `/metrics` exposition mounted on the existing aiohttp server. Process-local loop heartbeat registry (`record_loop_tick` / `get_loop_last_tick`) instrumented from every forever-loop's success-path. CIDR allowlist parser (`parse_ip_allowlist`) gated by `METRICS_IP_ALLOWLIST` (default `127.0.0.1,::1`, fail-closed on empty). No third-party `prometheus_client` dependency — exposition format rendered by hand in `render_metrics`. |
| `strings.py` | Two-locale (fa/en) compiled string table + `t(lang, key, **kwargs)` helper. Layered with a runtime override cache populated from the `bot_strings` DB table — admin edits at `/admin/strings` shadow the compiled defaults until reverted. Missing-slug lookups now log a one-shot WARNING per `(lang, key)` instead of silently returning the bare slug. |
| `wallet_display.py` | Stage-11-Step-D. `format_toman_annotation(lang, balance_usd, snap)` returns the `\n≈ N تومان` (fa) / `\n≈ N TMN` (en) line spliced onto every wallet view's `$X.YZ` figure when an FX snapshot is cached. Stale snapshots get the `(نرخ تقریبی)` / `(approx)` suffix; cold cache returns `""` so the wallet still renders without the line; non-finite balances and arithmetic-overflow products are rejected with `""` rather than rendering `≈ nan تومان`. `format_balance_block(lang, balance_usd, snap)` packages `$X.YZ` + the annotation for callers (post-credit DMs, future wallet sub-screens) that don't go through `strings.t` — and substitutes `$0.00` for the head string on a non-finite balance so a corrupted upstream can't leak `$nan` either (the annotation guard already covered the Toman line). |
| `wallet_receipts.py` | **Stage-12-Step-C.** Renders the new "🧾 Recent top-ups" wallet sub-screen. `get_receipts_page_size()` reads the `RECEIPTS_PAGE_SIZE` env var (default 5, max 20). `format_receipt_line(row, lang)` renders one row as `<status badge> — $X.YZ — <gateway label> — YYYY-MM-DD`; TetraPay rows append `(≈ N TMN)` using the per-transaction `gateway_locked_rate_toman_per_usd` (NOT the live snapshot — the user verifies against what they actually paid). Same NaN-defense policy as `wallet_display`: a non-finite `amount_usd` renders `$0.00`, a non-finite locked rate omits the Toman annotation. Backed by `Database.list_user_transactions(*, telegram_id, limit, before_id=None)`, which **hard-codes the `WHERE telegram_id = …` clause** and `raise ValueError` on a missing/zero/negative `telegram_id` — separate method from the admin-side `list_transactions` so a future buggy caller can't drop the user-scope filter and leak someone else's transactions. Cursor pagination via `before_id` over `transaction_id` (stable when fresh top-ups land mid-browse). Status whitelist is `{"SUCCESS", "PARTIAL", "REFUNDED"}` — PENDING / EXPIRED / FAILED are operational state, not user-facing receipts. |
| `admin.py` | Telegram-side admin commands gated on `ADMIN_USER_IDS`: `/admin`, `/admin_metrics`, `/admin_balance`, `/admin_credit`, `/admin_debit`, `/admin_promo_create`, `/admin_promo_list`, `/admin_promo_revoke`, `/admin_broadcast`. |
| `web_admin.py` | aiohttp + jinja2 web admin panel mounted under `/admin/` on the same web server that serves `/nowpayments-webhook`. HMAC-cookie auth via `ADMIN_PASSWORD` / `ADMIN_SESSION_SECRET`. CSRF-protected POST forms + signed flash-cookie banners. Login + dashboard + promo codes UI + gift codes UI + users UI + **per-user AI usage log browser** (`/admin/users/{id}/usage` with lifetime aggregates + paginated last-N calls) + **broadcast UI with live-progress polling** + **paginated transactions browser** + **editable bot text** (`/admin/strings`) shipped. |
| `templates/admin/` | Jinja2 templates for the web admin (login, dashboard, promos, gifts, users, user_detail, user_usage, broadcast, broadcast_detail, transactions, strings, string_detail). |
| `web_admin.py` | aiohttp + jinja2 web admin panel mounted under `/admin/` on the same web server that serves `/nowpayments-webhook`. HMAC-cookie auth via `ADMIN_PASSWORD` / `ADMIN_SESSION_SECRET`. CSRF-protected POST forms + signed flash-cookie banners. Login + dashboard + promo codes UI + gift codes UI + users UI + **broadcast UI with live-progress polling** + **paginated transactions browser with streamed CSV export** (`/admin/transactions?format=csv` honours all filters, RFC 4180 quoted, audited as `transactions_export_csv`) + **editable bot text** (`/admin/strings`) shipped. |
| `formatting.py` | Single canonical USD formatter — `format_usd(value, places=4)` returns `"$1,234.5678"` (4 decimal places, comma-grouped, leading-minus for negatives). Wired as a Jinja2 filter (`{{ value \| format_usd }}` / `{{ value \| format_usd(2) }}`) so every admin template uses the same precision, replacing the ad-hoc `:,.4f` / `:,.2f` / `:.4f` mix that pre-Step-7 made cross-page auditing painful. |
| `templates/admin/` | Jinja2 templates for the web admin (login, dashboard, promos, gifts, users, user_detail, broadcast, broadcast_detail, transactions, strings, string_detail). |
| `web_admin.py` | aiohttp + jinja2 web admin panel mounted under `/admin/` on the same web server that serves `/nowpayments-webhook`. HMAC-cookie auth via `ADMIN_PASSWORD` / `ADMIN_SESSION_SECRET`, optional TOTP / 2FA via `ADMIN_2FA_SECRET` (Stage-9-Step-3). CSRF-protected POST forms + signed flash-cookie banners. Login + dashboard + promo codes UI + gift codes UI + users UI + **broadcast UI with live-progress polling backed by the durable `broadcast_jobs` table** (links survive restarts, recent-jobs list reads from DB) + **paginated transactions browser with inline refund button** (`POST /admin/transactions/{id}/refund`, Stage-12-Step-A — CSRF + audit-logged, button only renders on SUCCESS rows from refundable gateways) + **per-code gift redemption drilldown** (`GET /admin/gifts/{code}/redemptions`, Stage-12-Step-D — paginated `gift_redemptions` rows with per-row credited USD joined from `transactions.amount_usd_credited`, aggregates above the table; backed by alembic 0013's `idx_gift_redemptions_code_redeemed_at`) + **editable bot text** (`/admin/strings`) + **audit log** + **2FA enrolment helper** (`/admin/enroll_2fa`) shipped. |
| `templates/admin/` | Jinja2 templates for the web admin (login, dashboard, promos, gifts, gift_redemptions, users, user_detail, user_usage, broadcast, broadcast_detail, transactions, strings, string_detail, audit, enroll_2fa). |
| `alembic/` | Schema migrations. `alembic upgrade head` runs idempotently in `entrypoint.sh` on every container start. New schema changes: `alembic revision -m "..."`. |

## Updating the live server

One-command update with automatic backup rotation (keeps 2 backups):

```bash
cd /opt/meowassist && sudo bash scripts/update-server.sh
```

The script:
1. Backs up the current version to `/opt/meowassist-backups/YYYY-MM-DD_HH-MM/`
2. Pulls latest code from `origin/main`
3. Rebuilds Docker containers (`docker compose up -d --build`)
4. Restarts Caddy if `docker-compose.caddy.yml` exists
5. Rotates old backups (keeps latest 2, deletes the rest)

**Your `.env` is never touched.** Database and Redis data live in Docker volumes — also untouched.

See `scripts/update-server.sh` for the full implementation, or
[HANDOFF.md](./HANDOFF.md) §Stage-15-Step-B for the design rationale.

## Roadmap

See [HANDOFF.md](./HANDOFF.md) §Stage-15 for the full queue:

| Step | Title | Status |
|------|-------|--------|
| **Stage-15-A** | Prometheus `/metrics` endpoint | shipped |
| **Stage-15-B** | Server update script with backup rotation | shipped |
| **Stage-15-C** | Logos & posters AI prompt folder | shipped |
| **Stage-15-D** | Bug-fix sweep | shipped (PRs #113–#118, all 6 candidates closed) |
| **Stage-15-E** | Future project suggestions (12 items) | **#1 MERGED** (conversation history export, first slice) · **#2 MERGED** (per-user spending dashboard, first slice) · **#3 STARTED** (opt-in webhook mode) |

## License / contributing

Internal project — see HANDOFF.md for the priority queue before opening a PR.
