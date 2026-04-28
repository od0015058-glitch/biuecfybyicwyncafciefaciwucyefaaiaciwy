# Meowassist AI Telegram Bot

A Persian/English Telegram bot that proxies user prompts to OpenRouter LLMs,
charges per-token cost (with markup) from a wallet, and tops up wallets via
NowPayments crypto invoices.

For the full project history, file map, and roadmap **read [HANDOFF.md](./HANDOFF.md)**.

## Quick start (Docker — recommended)

```bash
git clone <repo>
cd <repo>
cp .env.example .env       # fill in BOT_TOKEN / OPENROUTER_API_KEY / NOWPAYMENTS_* / DB_PASSWORD / WEBHOOK_BASE_URL
docker compose up -d --build
docker compose logs -f bot
```

Compose boots Postgres + the bot together. Schema and `migrations/*.sql`
auto-apply on first boot of an empty Postgres volume. The bot's webhook
listener is published to `127.0.0.1:8080` only — put nginx/Caddy/Cloudflare
Tunnel in front for TLS so NowPayments can reach
`${WEBHOOK_BASE_URL}/nowpayments-webhook` over HTTPS.

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
   psql -U botuser -d aibot_db -f schema.sql
   for f in migrations/*.sql; do psql -U botuser -d aibot_db -f "$f"; done
   ```

3. **Configure** — copy `.env.example` to `.env` and fill in:
   - `BOT_TOKEN` from @BotFather
   - `OPENROUTER_API_KEY` from <https://openrouter.ai/settings/keys>
   - `NOWPAYMENTS_API_KEY` + `NOWPAYMENTS_IPN_SECRET` from your NowPayments
     store settings
   - `WEBHOOK_BASE_URL` — public HTTPS URL where this bot is reachable
     (NowPayments will POST IPNs to `${WEBHOOK_BASE_URL}/nowpayments-webhook`)
   - DB credentials, `ADMIN_USER_IDS` if you have them

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
| `main.py` | Entrypoint. Boots aiogram dispatcher, registers middleware, starts the IPN HTTP listener. |
| `database.py` | asyncpg pool + every SQL query. Money methods use `SELECT … FOR UPDATE` inside connection-scoped transactions. |
| `payments.py` | NowPayments invoice creation, IPN verification (HMAC-SHA512), idempotent finalize, partial-payment crediting. |
| `handlers.py` | All aiogram handlers — `/start`, hub UI, charge flow, model picker, language picker, support. |
| `ai_engine.py` | OpenRouter call, cost calc, balance deduct, optional conversation memory. |
| `pricing.py` | Per-model price table + `COST_MARKUP` env var (default 1.5×). |
| `models_catalog.py` | Live `/v1/models` fetch from OpenRouter with 24 h cache, provider whitelist, free/paid split. |
| `middlewares.py` | `UserUpsertMiddleware` — ensures `users` row exists before any handler runs. |
| `strings.py` | Two-locale (fa/en) string table + `t(lang, key, **kwargs)` helper. |
| `schema.sql` | Initial schema. New tables/columns go in `migrations/NNN_*.sql`. |
| `migrations/` | Numbered, append-only SQL migrations. Apply in order. |

## License / contributing

Internal project — see HANDOFF.md for the priority queue before opening a PR.
