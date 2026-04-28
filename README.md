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
   - `TRUST_PROXY_HEADERS=1` if the bot runs behind a reverse proxy
     (Cloudflare Tunnel, nginx, Caddy). When set, the per-IP rate
     limiters (webhook + `/admin/login`) key on the leftmost
     `X-Forwarded-For` IP instead of the proxy's TCP peer address —
     so a login sprayer can't use the tunnel IP to bucket-share
     with legitimate admins. Leave unset on direct-internet deploys
     because the header can be spoofed by arbitrary clients.

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
| `database.py` | asyncpg pool + every SQL query. Money methods use `SELECT … FOR UPDATE` inside connection-scoped transactions. |
| `payments.py` | NowPayments invoice creation, IPN verification (HMAC-SHA512), idempotent finalize, partial-payment crediting. |
| `handlers.py` | All aiogram handlers — `/start`, hub UI, charge flow, model picker, language picker, support. |
| `ai_engine.py` | OpenRouter call, cost calc, balance deduct, optional conversation memory. |
| `pricing.py` | Per-model price table + `COST_MARKUP` env var (default 1.5×). |
| `models_catalog.py` | Live `/v1/models` fetch from OpenRouter with 24 h cache, provider whitelist, free/paid split. |
| `middlewares.py` | `UserUpsertMiddleware` — ensures `users` row exists before any handler runs. |
| `rate_limit.py` | Token-bucket primitives + `ChatRateLimitMiddleware` (per-user) and `webhook_rate_limit_middleware` (per-IP). Guards `/chat` against runaway OpenRouter spend and the `/nowpayments-webhook` endpoint against DoS bursts. |
| `strings.py` | Two-locale (fa/en) compiled string table + `t(lang, key, **kwargs)` helper. Layered with a runtime override cache populated from the `bot_strings` DB table — admin edits at `/admin/strings` shadow the compiled defaults until reverted. Missing-slug lookups now log a one-shot WARNING per `(lang, key)` instead of silently returning the bare slug. |
| `admin.py` | Telegram-side admin commands gated on `ADMIN_USER_IDS`: `/admin`, `/admin_metrics`, `/admin_balance`, `/admin_credit`, `/admin_debit`, `/admin_promo_create`, `/admin_promo_list`, `/admin_promo_revoke`, `/admin_broadcast`. |
| `web_admin.py` | aiohttp + jinja2 web admin panel mounted under `/admin/` on the same web server that serves `/nowpayments-webhook`. HMAC-cookie auth via `ADMIN_PASSWORD` / `ADMIN_SESSION_SECRET`. CSRF-protected POST forms + signed flash-cookie banners. Login + dashboard + promo codes UI + gift codes UI + users UI + **broadcast UI with live-progress polling** + **paginated transactions browser** + **editable bot text** (`/admin/strings`) shipped. |
| `templates/admin/` | Jinja2 templates for the web admin (login, dashboard, promos, gifts, users, user_detail, broadcast, broadcast_detail, transactions, strings, string_detail). |
| `alembic/` | Schema migrations. `alembic upgrade head` runs idempotently in `entrypoint.sh` on every container start. New schema changes: `alembic revision -m "..."`. |

## License / contributing

Internal project — see HANDOFF.md for the priority queue before opening a PR.
