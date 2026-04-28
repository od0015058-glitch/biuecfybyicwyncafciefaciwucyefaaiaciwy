# Project Handoff — Meowassist AI bot

**Audience:** the next AI (or human) picking this codebase up.
**Goal:** read this single file and have full context — what the project is,
what's been shipped, what direction the user pivoted to, what to do next.

> **If you are the next AI:** read §11 ("Working agreement") first — the user
> wants you to ship PRs autonomously without blocking on per-PR approval.

---

## 1. What the bot is

Telegram bot (`@Meowassist_Ai_bot`, id `8761211112`) that:

1. Lets a user pick an LLM from OpenRouter (free or paid).
2. Charges them per request — free messages while their `free_messages_left > 0`,
   then deducts USD from a wallet stored in PostgreSQL.
3. Tops the wallet up via NowPayments crypto invoices (BTC / ETH / LTC / TON /
   TRX / USDT on TRC20·ERC20·BEP20·TON — 9 supported tickers).
4. Speaks Persian by default, English on demand.

**Stack:** Python 3.11+, aiogram 3, asyncpg, aiohttp, PostgreSQL, Redis.
**Process model:** one Python process. `aiogram` long-polls Telegram; a
side-by-side `aiohttp` web server listens for NowPayments IPN POSTs on
`${WEBHOOK_BASE_URL}/nowpayments-webhook`.
**Deployment:** docker-compose (`postgres` + `redis` + `bot`). Live deploy
sits at `/root/bot_project` on the user's VPS. **Do not modify that path.**

---

## 2. File map

```
main.py             entrypoint + set_my_commands publish   ~80 LoC
bot_commands.py     canonical /-menu publisher             ~130 LoC
database.py         asyncpg pool, all SQL                 ~1100 LoC
payments.py         NowPayments invoice + IPN verify       ~630 LoC
handlers.py         every aiogram handler                 ~1660 LoC
ai_engine.py        OpenRouter call + cost settlement      ~140 LoC
pricing.py          per-model price + markup               ~110 LoC
models_catalog.py   live OpenRouter /v1/models cache       ~290 LoC
middlewares.py      user-upsert middleware                  ~60 LoC
strings.py          fa/en string table                     ~600 LoC
admin.py            Telegram-side admin commands           ~870 LoC
rate_limit.py       chat + webhook rate limiters           ~270 LoC
web_admin.py        web admin panel (aiohttp+jinja2)       ~910 LoC
templates/admin/    jinja2 templates (base, _layout, login, dashboard, promos, gifts)
alembic/            schema migrations (owns schema)
  env.py
  versions/0001_baseline.py
  versions/0002_transactions_notes.py
  versions/0003_gift_codes.py
entrypoint.sh       runs `alembic upgrade head` then exec's main.py
Dockerfile          python:3.12-slim + requirements
docker-compose.yml  postgres + redis + bot
.env.example        every required env var
tests/              pytest, ~473 cases
.github/workflows/ci.yml   3.11/3.12 matrix + alembic roundtrip + docker build
```

Total: ~6.7k LoC, 473 tests, full CI on every push.

---

## 3. Priority framework

The user's rule (overriding the original "Stage 1–8" Persian roadmap):
**money/security first, product surface last.**

- **P0** — security & money correctness. Anything that can drain the account
  or let users mint balance.
- **P1** — correctness bugs (custom amount, partial-payment crediting).
- **P2** — product surface (back buttons, i18n, model picker, promo, admin).
- **P3** — operational hardening (Dockerfile, README, tests, Redis FSM,
  Alembic, rate limiting).

The new **Stage-8** (web admin panel) belongs to P2 conceptually but is
big enough to track separately.

---

## 4. What's shipped (in merge order)

### P0 — security & money correctness
| # | What |
| --- | --- |
| #1 | `.gitignore` + `.env.example` + drop committed `payments.py.save`. |
| #2 | Per-model pricing table + `COST_MARKUP` (default 1.5×). |
| #3 | Atomic `deduct_balance` with `WHERE balance_usd >= $1 RETURNING …`. |
| #4 | Env-driven `WEBHOOK_BASE_URL`, structured logging, sanitized errors. |
| #5 | NowPayments IPN HMAC-SHA512 signature verification. |
| #6 | Idempotent payments via the `transactions` ledger (PENDING → SUCCESS in one DB tx). |

### P1 — correctness bugs
| # | What |
| --- | --- |
| #7  | `amt_custom` callback reachability (handler-ordering fix). |
| #8  | Non-`finished` IPN status handling (`expired`/`failed`/`refunded`). |
| #9  | Partial-payment crediting via `actually_paid_usd`. |
| #10 | `finished`-after-`partially_paid` credits the **delta**, not zero. |
| #11 | `mark_transaction_terminal` accepts PARTIAL ∪ PENDING. |
| #13 | Report `$0` (not invoice price) when closing PENDING terminally. |

### P2 — product surface (#12, #14–#16, #25–#37)
Hub navigation, FSM clearing, i18n, model picker filter, conversation memory,
free-models tab, NowPayments error log polish, rate-lock screen, etc.
See git log; not repeating here — they're all in `main`.

### IPN signature fix (the one the user was stuck on)
| # | What |
| --- | --- |
| #38 | Diagnostic logging on signature mismatch (`expected/received` prefixes + lengths). |
| #39 | **`json.dumps(..., ensure_ascii=False)` in canonical re-serialization.** Persian `order_description` was being escaped to `\uXXXX` (6 bytes/char) while NowPayments signs raw UTF-8 (~2 bytes/char). 40-byte length gap → HMAC mismatch. Verified against the user's `body_len=585 canonical_len=625` log. |

### P3 — operational hardening (the original stage-2 list, completed this cycle)
| PR | Title |
| --- | --- |
| [#41](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/41) | **P3-Op-1**: two-pass IPN verifier — sign raw body first, fall back to canonicalized. Stripe-style. |
| [#42](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/42) | **P3-Op-2**: Dockerfile + docker-compose. |
| [#43](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/43) | **P3-Op-3**: pytest skeleton + GitHub Actions CI matrix. |
| [#44](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/44) | **P3-Op-4**: Alembic migrations + entrypoint runs `alembic upgrade head`. |
| [#45](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/45) | **P3-Op-4-Hotfix**: URL-encode DB credentials in `alembic/env.py` (Devin Review catch). |
| [#46](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/46) | **P3-Op-5**: Redis-backed FSM + reject NaN/Inf/over-cap custom amounts. |
| [#47](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/47) | **P3-Op-6**: per-user chat + per-IP webhook rate limits + OpenRouter `aiohttp.ClientTimeout`. |
| [#48](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/48) | **P3-Op-6-Hotfix**: scope chat throttle to AI handler only (Devin Review catch). |

### Stage-7 — Telegram-side admin commands (completed)
The original "Stage 7 CLI panel" was reframed as Telegram commands (gated by
`ADMIN_USER_IDS` in env). All four sub-stages shipped:

| PR | Command surface |
| --- | --- |
| [#49](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/49) | **Stage-7-Part-1**: `is_admin` gate, `/admin` hub, `/admin_metrics`. + `message.text=None` crash fix in `process_custom_amount_input`. |
| [#50](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/50) | **Stage-7-Part-2**: `/admin_balance`, `/admin_credit`, `/admin_debit` with `transactions.notes` audit column (alembic 0002). + defensive guard for malformed OpenRouter responses. |
| [#51](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/51) | **Stage-7-Part-3**: `/admin_promo_create`, `/admin_promo_list`, `/admin_promo_revoke`. + `from_user is None` guard in `process_chat`. |
| [#52](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/52) | **Stage-7-Part-4**: `/admin_broadcast [--active=N]` with paced fan-out + progress + Markdown-escape fix for free-form `reason`/`notes`. |

### Cleanup PR ([#53](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/53))
- Deleted `schema.sql` and `migrations/*.sql` — Alembic owns schema now,
  the legacy files were stale leftovers (the docker-compose mount-list
  maintenance burden goes with them too).
- Same defensive `from_user is None` guard added to `process_promo_input`
  and `process_custom_amount_input` (the FSM-state handlers PR #51 didn't
  cover). Two new regression tests pin both guards.

### Stage-8 — Web admin panel (in progress)
| PR | Title |
| --- | --- |
| **Stage-8-Part-1** | Web admin scaffold — aiohttp+jinja2 mounted under `/admin/` on the same server as the IPN webhook. HMAC-signed-cookie auth via `ADMIN_PASSWORD` + `ADMIN_SESSION_SECRET`. Login + dashboard with system metrics. + `from_user is None` guard added to `cmd_start` and `_route_legacy_text_to_hub` (the two remaining handlers reachable from anonymous-group-admin posts). 30 new tests. |
| **Stage-8-Part-2** | Promo codes web UI — `/admin/promos` page with table view + create form + per-row revoke. CSRF-protected POSTs (HMAC tokens derived from session cookie). Signed flash-cookie banners (10s TTL) survive the post-redirect-get cycle without a server-side store. **Bug fix bundled:** `Database.create_promo_code` + `parse_promo_form` now reject `discount_amount > 999_999.9999` up-front so admins get a friendly error instead of PG `numeric field overflow` (column is `DECIMAL(10,4)`). 39 new tests. |
| **Stage-8-Part-3** | Gift codes — alembic `0003_gift_codes` (new `gift_codes` + `gift_redemptions` tables, distinct from `promo_codes`/`promo_usage`). Five new `Database` methods (`create_gift_code`, `list_gift_codes`, `revoke_gift_code`, `get_gift_redemptions`, atomic `redeem_gift_code` with `FOR UPDATE` locks). `/admin/gifts` web UI (list/create/revoke, CSRF + flash, mirroring promos). User-side `/redeem CODE` Telegram command with full localized error branches (not_found / inactive / expired / exhausted / already_redeemed / user_unknown / ok). **Bug fix bundled:** both `parse_gift_form` and `parse_promo_form` now cap `expires_in_days` at `EXPIRES_IN_DAYS_MAX = 36_500` (≈100 years) — without the cap, an admin pasting a giant integer would crash the create handler with an uncaught `OverflowError` from `timedelta(days=...)` → 500 instead of a friendly red banner. 56 new tests (286 total). |

---

## 5. The user's pivot — Stage-8: Web admin panel + gift codes

> The user explicitly asked for this on 2026-04-28, replacing the original
> "Stage 8 — webhook security" item from the Persian roadmap (already shipped
> as P0/P1/P3-Op-1). The Telegram-side `/admin_*` commands stay (still work,
> still gated), but they are no longer the primary UI.

### Why a web panel
The user found the Telegram-command UI hard to use ("typing slash commands is
painful, give me buttons in a browser"). New requirement: a real web admin
dashboard the user can open in any browser, log in, and click through.

### The new gift-code concept (distinct from existing promo codes)
| | Existing promo code | **NEW: gift code** |
| --- | --- | --- |
| Triggered by | User applying it during a paid invoice | User redeeming it standalone (no purchase required) |
| Effect | Adds bonus % or $ on top of a paid top-up | Directly credits balance |
| Cap | `max_uses` (total redemptions) | `max_uses` (total redemptions) — same shape |
| Per-user | Single redemption (`promo_usage` table) | Single redemption (`gift_redemptions` table) |
| Admin sets | Code + discount % or $ + max_uses + expiry | Code + **fixed $ amount** + max_uses + expiry |
| Schema | `promo_codes` | new `gift_codes` table (alembic 0003) |
| User flow | Apply during charge picker | New `/redeem CODE` command **or** "Redeem gift code" wallet button |

The user's exact wording (paraphrased): "I want to set 10 people and 10
people use that code to increase their balance as much as I want." → that's
a gift code with `max_uses=10`, `amount_usd=$X`, no expiry, that can be
redeemed by any 10 distinct telegram_ids.

### Stage-8 stack (decided, not asked)
- **Server-side:** mount on the **same** aiohttp app that already serves
  `/nowpayments-webhook`. Routes under `/admin/`. One process, one
  Dockerfile, one deploy. No FastAPI / no Node.
- **Templating:** `jinja2` (already a stable, well-known choice; no build
  step required).
- **Frontend interactivity:** vanilla HTML forms + a sprinkle of HTMX where
  it removes 80% of the JS we'd otherwise need (e.g. live broadcast progress).
- **Auth:** `ADMIN_PASSWORD` env var → login form sets a signed (HMAC) cookie
  with a 24h TTL. Existing `ADMIN_USER_IDS` list is still used to attribute
  "which admin" performed an action (recorded into `transactions.notes`
  via `admin_adjust_balance`). No OAuth, no third-party SSO.
- **Hosting:** the bot's aiohttp server already binds port 8080. The same
  port serves admin routes behind `/admin/`. Expose externally via the
  existing Cloudflare tunnel (or whatever the user uses for `WEBHOOK_BASE_URL`).
  Admin URL becomes e.g. `https://bot.example.com/admin/login`.

### Stage-8 PR queue
Each is a separate PR with a real bundled bug fix, HANDOFF.md + README.md
updated, full tests:

| # | Title | Status |
| --- | --- | --- |
| **Stage-8-Part-1** | Web admin scaffold — login + dashboard with system metrics. `web_admin.py` + `templates/admin/`. Auth via `ADMIN_PASSWORD` + `ADMIN_SESSION_SECRET` HMAC-signed cookie. | ✅ shipped (PR #54) |
| **Stage-8-Part-2** | Promo codes page — table view + create form + revoke action. Reuses `Database.list_promo_codes` + `create_promo_code` + `revoke_promo_code`. CSRF + flash messaging primitives added to `web_admin.py`. | ✅ shipped (PR #55) |
| **Stage-8-Part-3** | **Gift codes** — alembic 0003 (`gift_codes` + `gift_redemptions`), DB methods, `/redeem CODE` user-facing flow, admin UI for create/list/revoke. **(Wallet-menu button + redemption stats page deferred to Part-3.5 if user asks.)** | ✅ shipped (PR #56) |
| **Stage-8-Part-4** | Users page — `/admin/users` search-by-id-or-username, `/admin/users/{id}` detail page (balance, lifetime totals, last 20 transactions), credit/debit form posting to `/admin/users/{id}/adjust`. Reuses `admin_adjust_balance`; web calls pass `admin_telegram_id=0` sentinel and `[web]` -prefixed reason into `transactions.notes` for unambiguous audit trail. New `Database.search_users(query, limit)` with int-lookup / escaped ILIKE dispatch; `get_user_admin_summary` now takes `recent_tx_limit` kwarg (default 5, clamped [1..200]). **Bug fix bundled:** `Database.get_system_metrics` now excludes both `gateway='admin'` AND `gateway='gift'` from `revenue_usd` — latent since PR #56 shipped gift redemptions with `gateway='gift'`, which inflated the dashboard's "revenue" figure every time an admin minted a gift code. Regression test pins the filter. 45 new tests (331 total). | ✅ this PR |
| **Stage-8-Part-5** (this PR) | Broadcast page — `/admin/broadcast` form (text + optional `only_active_days` filter) that kicks off a background `asyncio.Task`, plus `/admin/broadcast/{job_id}` detail page with a live progress bar and a polling `/admin/broadcast/{job_id}/status` JSON endpoint (vanilla JS, no HTMX). In-memory job registry on the aiohttp app (bounded to 50 entries, never evicts live jobs). Shares `admin._do_broadcast` with the Telegram `/admin_broadcast` command via a `progress_callback` refactor, so pacing / 429 handling / error bucketing is identical for both callers. **Bug fix bundled:** `rate_limit.webhook_rate_limit_middleware` is now scoped to `/nowpayments-webhook` only — it was previously installed globally and consumed a token on **every** request, so admin-panel traffic (and the new broadcast status-polling page) would eat from the NowPayments bucket and vice-versa. 33 new tests (364 total). | ✅ this PR |
| **Stage-8-Part-6** (this PR) | Transactions browser — paginated `/admin/transactions` list, filter by gateway / status / user, link through to existing user-detail page. Read-only ledger view; credit/debit still lives on the user-detail page so the write-path audit trail has one canonical entry point. New `Database.list_transactions(gateway, status, telegram_id, page, per_page)` with allow-listed enum filters and parameterised SQL; clamps `per_page` to `TRANSACTIONS_MAX_PER_PAGE=200`. Sidebar link is now enabled — the whole Stage-8 nav is complete. **Bug fix bundled:** both `parse_broadcast_args` (Telegram) and `parse_broadcast_web_form` (web) now reject `only_active_days > 36_500` up-front — pre-fix an admin typing `--active=9999999999` would overflow PG's 32-bit-int interval column when `iter_broadcast_recipients` formatted it as `f"{N} days"`, surfacing as an opaque "DB query failed" banner instead of a friendly validation error. Defensive cap also added inside `Database.iter_broadcast_recipients` itself so a direct REPL caller can't hit the overflow either. 30 new tests (394 total). | ✅ this PR |

### Stage-9 queue (next 10 steps)
Sorted by the same §3 priority framework — money/security first, product surface last, operational hardening last. Each step is one PR with a real bundled bug fix (never invented), HANDOFF.md + README.md updated per §11.

| # | Title | Priority | Status |
| --- | --- | --- | --- |
| **Stage-9-Step-1** | Per-IP token-bucket throttle on `/admin/login`. New `install_login_rate_limit` + `consume_login_token` helpers in `rate_limit.py`; `login_post` now consumes a token BEFORE the password compare so a spraying attacker can't get constant-time feedback on every guess. Defaults: 10-token burst, 1 token / 30 s refill — combined with `ADMIN_PASSWORD` being a 32-char secret this makes brute force infeasible. **Bug fix bundled:** the existing `request.remote` keying collapses every reverse-proxy deploy onto one bucket IP, which either (a) silently disables the new login throttle (the tunnel IP is fine, bucket never drains) or (b) self-DoSes (one attacker locks every admin out of the same tunnel). New `rate_limit.client_ip_for_rate_limit(request)` helper reads `X-Forwarded-For` leftmost IP iff `TRUST_PROXY_HEADERS=1` env var is set (defaults off so direct-exposure deploys don't trust a spoofable header). Retrofitted the existing webhook middleware to use the same helper so the two limiters gain real-client granularity together. 12 new tests (406 total). | P0 security | ✅ shipped (PR #60) |
| **Stage-9-Step-1.5** | **User-side bot UX cleanup** (P2 product, taken out of order at user request 2026-04-28). New `bot_commands.py` module + boot-time `Bot.set_my_commands(...)` call so Telegram's `/` popup matches the handlers we actually ship — `/start`, `/redeem` for everyone; admin commands per-admin via `BotCommandScopeChat`. Pre-fix the bot never published its commands so the popup served whatever was last typed into BotFather's "Edit Commands" panel (the user reported `/new`, `/redo`, `/img`, `/version` ghosts). Hub keyboard split: dedicated `🆕 New Chat` button now wipes the conversation buffer immediately (free) and a separate `🧠 Memory: ON/OFF` button opens the memory settings screen with the cost trade-off explainer. Wallet keyboard gained a `🎁 Redeem gift code` button that arms a new `UserStates.waiting_gift_code` and reuses the same `_redeem_code_for_user` helper as `cmd_redeem` — gift redemption is now reachable from buttons, not just the slash command. **Bug fix bundled:** `_render_memory_screen`'s `try/except Exception` around `edit_text` was swallowing every exception including DB drops, `TelegramForbiddenError` (bot blocked), and unrelated network blips — masking real bugs as a single `log.debug`. Tightened to `except TelegramBadRequest:` so only the legitimate "message is not modified" / parse-mode no-op cases are silenced. 29 new tests (435 total). | P2 product | ✅ this PR |
| **Stage-9-Step-1.6** (this PR) | **Editable bot text** (P2 product, second of three out-of-order PRs requested by the user 2026-04-28). New `bot_strings(lang, key, value, updated_at, updated_by)` table + alembic migration `0004_bot_strings`. The runtime `t()` helper grew an in-memory override cache (`strings._OVERRIDES`) populated at boot from `Database.load_all_string_overrides` and refreshed after every successful admin write — so the next message a user receives uses the new text without any process restart. New `/admin/strings` page lists every `(lang, key)` slug with its compiled default and current override (if any), filterable by lang + free-text search. New `/admin/strings/{lang}/{key}` editor: textarea pre-filled with the override-or-default, save / revert buttons, length cap of 2 KB to keep the operator from pasting megabyte JSON into a button label, CSRF-protected. Reverting deletes the row and resurrects the compiled default. **Bug fix bundled:** `t()` previously returned the bare slug silently when a key was missing in both the requested locale and the `DEFAULT_LANGUAGE` fallback — translator typos shipped to users invisibly. Now logs a one-shot WARNING per `(lang, key)` per process so dictionary drift surfaces in ops logs. 38 new tests (473 total): 19 in `tests/test_strings_overrides.py`, 19 added to `tests/test_web_admin.py`. | P2 product | ✅ this PR |
| **Stage-9-Step-2** | `admin_audit_log` append-only table — one row per admin action (login success/fail, promo + gift create/revoke, credit/debit, broadcast start, string save/revert, user edit) with `ts, actor, action, ip, target, outcome, meta_json`. Viewable at `/admin/audit` with optional action + actor filters. `/admin/users/{id}/edit` POST now updates an allow-listed subset of user fields (`language_code`, `active_model`, `memory_enabled`, `free_messages_left`, `username`) atomically; balance still routes through `/admin/users/{id}/adjust` so every change leaves a transactions-ledger row. **Bug fix bundled:** `Database.admin_adjust_balance` no longer relies solely on the legacy `gateway_invoice_id` string encoding for the acting admin id — every new adjustment also populates a real `transactions.admin_telegram_id` column with a partial index for forensics. Audit writes are wrapped in `_record_audit_safe`, swallowing exceptions so a failed audit insert never rolls back the underlying admin operation. | P0 security | ✅ done (PR-C) |
| **Stage-9-Step-3** | TOTP / 2FA on admin login. New env var `ADMIN_2FA_SECRET` enables enforcement; enrollment via QR at `/admin/enroll_2fa`. Backwards compatible — if env var missing, login works exactly as today. **Bug fix bundled:** a config where `ADMIN_PASSWORD` is set but empty string (common deploy typo) currently refuses logins with a confusing `TypeError` under one code path; tighten the config guard to a single "must be non-empty" assertion at startup. | P0 security | ⏳ pending |
| **Stage-9-Step-4** | IPN webhook replay-dedupe. New `payment_status_transitions` table keyed by `(gateway_invoice_id, payment_status)` so a backdated PARTIAL arriving after SUCCESS is dropped rather than writing a stray ledger row. **Bug fix bundled:** `parse_ipn_body` silently drops IPNs with missing `payment_id` — log LOUDLY and expose a counter so a misconfigured sandbox hitting the prod webhook is immediately visible. | P1 correctness | ⏳ pending |
| **Stage-9-Step-5** | Background task: expire stuck `PENDING` transactions (> 24h) so the ledger doesn't accumulate dead invoices forever. `Database.expire_stale_pending()` + a `asyncio.create_task` scheduled every 15 min. **Bug fix bundled:** `mark_transaction_terminal` has a branch that silently re-updates `updated_at` on a PENDING→PENDING no-op; tighten the FSM so only real state transitions bump the timestamp. | P1 correctness | ⏳ pending |
| **Stage-9-Step-6** | Soft-cancel running broadcasts. Cancel button on `/admin/broadcast/{id}` sets `job["cancel_requested"]`; `_do_broadcast` checks between sends. **Bug fix bundled:** the in-memory job registry's 50-entry cap evicts oldest-first without checking state — an admin spamming the form could push a live `running` job off the end of the dict. Guard eviction on `state ∈ {completed, failed}`. | P1 correctness | ⏳ pending |
| **Stage-9-Step-7** (this PR) | CSV export from `/admin/transactions?format=csv` for quarterly audits. Streams via aiohttp `StreamResponse` in 500-row pages capped at 500k rows so a "everything-ever" filter can't pin the connection pool. Same filter semantics as the HTML page (gateway, status, telegram_id); pagination params are ignored — CSV is always full-result. RFC 4180 quoting (commas, quotes, newlines escape correctly). `Cache-Control: no-store` and `Content-Disposition: attachment; filename="transactions-YYYYMMDDTHHMMSSZ.csv"` so a later admin session on the same machine can't pull a cached copy. Each successful export writes one `transactions_export_csv` audit row recording row count + filters. The HTML transactions page grew an "⬇ Export CSV" link that carries the active filters into the export. **Bundled bug fix:** the admin UI used four different USD formatters — `${:,.4f}` in the transactions browser & user-detail page, `${:,.2f}` in gifts/promos lists, `${value:.4f}` (NO comma grouping) in `/admin_balance`, and `${value:.4f}` in adjust refusal messages. An auditor reconciling a single row across two pages would see ``$1,234.5678`` and ``$1234.5678`` and have to second-guess whether they were actually the same number. New `formatting.format_usd(value, places=4)` is the single canonical formatter — defaults to 4dp (matches the on-screen ledger precision), `places=2` for settlement amounts, leading minus before the dollar sign for negatives. Wired as a Jinja2 filter (`{{ value \| format_usd }}` / `{{ value \| format_usd(2) }}`) so future templates pick it up automatically. 13 new tests (520 total): 5 `format_usd` unit pins (default 4dp, 2dp variant, leading-minus negatives, `places` clamped to `[0,8]`, int input widens to float), 7 CSV-export integration pins (auth required, headers + rows shape, RFC 4180 quoting, audit row written, filters honoured, multi-page streaming, "Export CSV" button on HTML page), 1 Jinja-filter registration pin. | P2 product | ✅ this PR |
| **Stage-9-Step-8** | Per-user usage log browser — `/admin/users/{id}/usage` with last N AI calls (model, tokens, cost). **Bug fix bundled:** `usage_logs.cost_usd` is nullable for legacy reasons but modern code always populates it; add Alembic migration + backfill + NOT NULL constraint so the new report can trust the column. | P2 product | ⏳ pending |
| **Stage-9-Step-9** | Dashboard tile: "Pending payments: N (oldest: Xh ago)" so stuck invoices are visible at a glance. **Bug fix bundled:** the existing `spend_usd` dashboard tile sums the absolute value of everything — including a rare class of refund rows that were inserted with positive `amount_usd_credited` by a legacy migration; scope the sum to `amount_usd < 0`. | P2 product | ⏳ pending |
| **Stage-9-Step-10** | Durable broadcast job registry — move the in-memory `APP_KEY_BROADCAST_JOBS` dict to a `broadcast_jobs` table so a restart mid-broadcast can resume. **Bug fix bundled:** `_run_broadcast_job` catches `asyncio.CancelledError` and re-raises before setting `completed_at`; the operator sees the cancelled job with a `None` completion timestamp forever. Set the timestamp before `raise`. | P3 operational | ⏳ pending |

---

## 6. The IPN signature bug we were stuck on (kept for context)

### Symptom (from prod log 2026-04-27 16:12:01 UTC)
```
WARNING bot.payments: IPN sig mismatch:
  expected=6ac370a7..f0d64f68
  received=691300cd..b1ae2324
  secret_len=32 body_len=585 canonical_len=625
```

### Root cause
Persian `order_description = "شارژ کیف پول"` got escaped to `\uXXXX`
(6 bytes/char) by `json.dumps(ensure_ascii=True)`, while NowPayments
signs the raw UTF-8 body (~2 bytes/char). 40-byte length gap = HMAC mismatch.

### Fix (PR #39 + PR #41)
- `ensure_ascii=False` in canonical re-serialization (#39).
- Two-pass verifier — try the **raw body bytes first**, fall back to
  re-canonicalization (#41). Stripe / Paddle / GitHub all sign the raw
  body; we now do the same.

User confirmed clean log on 2026-04-28 (PR was already merged & redeployed).

---

## 7. Money-flow walkthrough (so you understand what NOT to break)

```
User taps "Charge wallet" → picks $5/$10/$25/custom → picks currency
    │
    ▼
handlers.process_charge_*  →  payments.create_crypto_invoice(...)
    │
    ├─ POST /v1/payment to NowPayments → returns {payment_id, pay_address, ...}
    ├─ db.create_pending_transaction(...)              ← PENDING row
    └─ Bot shows the user the invoice address + amount

[user pays on-chain]

NowPayments POSTs to /nowpayments-webhook with x-nowpayments-sig header
    │
    ▼
payments.payment_webhook
    ├─ _verify_ipn_signature(raw_body, header)         ← HMAC-SHA512
    │     └─ 401 if bad. STOP. No balance changes.
    │
    ├─ status == "finished":
    │     └─ db.finalize_payment(payment_id, full_price_usd)
    │            credit (full_price_usd - already_credited), set SUCCESS,
    │            consume promo (if any), all in ONE DB transaction.
    │
    ├─ status == "partially_paid":
    │     └─ actually_paid_usd = actually_paid / pay_amount * price_amount
    │        db.finalize_partial_payment(...)
    │            credit only the new delta, status PENDING|PARTIAL → PARTIAL.
    │
    ├─ status in {expired, failed, refunded}:
    │     └─ db.mark_transaction_terminal(...) — accepts PENDING ∪ PARTIAL.
    │
    ├─ status in {waiting, confirming, confirmed, sending}:    no-op.
    └─ unknown:                                               no-op.
```

**Invariants:**
1. Wallet credit + ledger row update happen in **one DB transaction**.
2. Every credit is gated on `WHERE status IN ('PENDING','PARTIAL')` so a
   replayed IPN cannot mint money.
3. `_verify_ipn_signature` returns False on missing secret / missing header /
   bad JSON / mismatched HMAC → 401. **Never** read the body without
   verifying.
4. `actually_paid` from a `partially_paid` IPN is in the **pay-currency**
   (e.g. TRX), NOT USD. Convert via `actually_paid / pay_amount * price_amount`.

---

## 8. Schema (post-cleanup)

Alembic owns the schema. Read `alembic/versions/0001_baseline.py` for the
canonical truth. Two tables are "money tables" — touch them only with
care:

- `users` — wallet, free messages remaining, language, active model.
- `transactions` — ledger. **Every** balance change writes a row.
  - `gateway` ∈ {`nowpayments`, `admin`} (admin set via `Database.admin_adjust_balance`).
  - `status` ∈ {PENDING, PARTIAL, SUCCESS, EXPIRED, FAILED, REFUNDED}.
  - `gateway_invoice_id` UNIQUE — gives a free duplicate-click guard.
  - `notes` TEXT — human-readable reason on admin adjustments (alembic 0002).
- `usage_logs` — append-only AI request log. Used for cost analytics.
- `system_settings` — singleton-ish key/value store.
- `promo_codes` + `promo_usage` — discount codes (P2-5).
- `conversation_messages` — opt-in conversation memory buffer (P3-5).

Two tables added by **Stage-8-Part-3** (alembic 0003, this PR):
- `gift_codes (code TEXT PK, amount_usd DECIMAL(10,4), max_uses INT NULL, used_count INT, expires_at TIMESTAMPTZ NULL, is_active BOOLEAN, created_at TIMESTAMPTZ)`
- `gift_redemptions (code TEXT REFERENCES gift_codes ON DELETE CASCADE, telegram_id BIGINT REFERENCES users ON DELETE CASCADE, redeemed_at TIMESTAMPTZ, transaction_id INT REFERENCES transactions(transaction_id) ON DELETE SET NULL, PRIMARY KEY (code, telegram_id))`

---

## 9. Test suite

**473 tests across 14 modules** as of Stage-9-Step-1.6:

```
tests/
├── conftest.py                            # adds repo root to sys.path
├── test_admin.py                          # 87 cases (gate, parsers, formatters, broadcast, _escape_md)
├── test_alembic_env.py                    # 12 cases (DB_URL building w/ special chars in password)
├── test_custom_amount_validation.py       # 21 cases (NaN/Inf/bounds)
├── test_database_queries.py               # 18 cases (revenue filter regression,
                                           #            search_users dispatch, summary limit clamp,
                                           #            list_transactions pagination + filter composition,
                                           #            iter_broadcast_recipients active-days cap)
├── test_fsm_storage.py                    # 3 cases (build_fsm_storage selection)
├── test_handlers_from_user_guard.py       # 4 cases (promo, custom_amount, cmd_start, _route_legacy_text_to_hub)
├── test_ipn_signature.py                  # 11 cases (raw + canonical paths, persian descr regression)
├── test_pricing.py                        # 11 cases (per-model lookup, markup, fallback)
├── test_rate_limit.py                     # 23 cases (token bucket + LRU + middleware +
                                           #            client_ip_for_rate_limit / TRUST_PROXY_HEADERS +
                                           #            login-throttle install/consume helpers)
├── test_redeem_handler.py                 # 15 cases (cmd_redeem usage / status branches)
├── test_strings_overrides.py              # 19 cases (override cache replace/clear/copy,
│                                          #          t() resolution order, missing-key WARNING
│                                          #          one-shot suppression, iter_compiled_strings
│                                          #          determinism + ignores overrides)
├── test_bot_commands.py                   # 9 cases (PUBLIC/ADMIN scope shape, set_my_commands
│                                          #          per-admin scoping, swallowed-failure semantics)
├── test_hub_ux.py                         # 20 cases (6-button hub layout, hub_newchat wipes,
│                                          #          hub_memory opens settings, wallet redeem button,
│                                          #          waiting_gift_code FSM input handler,
│                                          #          _render_memory_screen exception tightening,
│                                          #          shared _redeem_code_for_user helper status branches)
└── test_web_admin.py                      # 219 cases (cookie sign/verify, login, dashboard,
                                          #             promo + gift + user list/create/revoke,
                                          #             CSRF, flash cookies, adjust-form parser,
                                          #             credit/debit happy-path + edge cases,
                                          #             broadcast form parser, job lifecycle,
                                          #             detail page polling endpoint,
                                          #             transactions query parser + handler,
                                          #             active-days cap mirror test,
                                          #             login rate-limit + trust-proxy-headers)
```

CI runs the full suite on Python 3.11 + 3.12, plus an alembic
upgrade/downgrade roundtrip job and a docker-build smoke job. Every PR
must be green before merge.

**Rule:** never modify a test to make it pass. Fix the code or the
assumption.

---

## 10. Status of every file (post-cleanup)

| File | Status |
| --- | --- |
| `main.py` | Clean. Env-driven port, `build_fsm_storage` (Redis if `REDIS_URL` set, in-memory fallback with warning), `install_webhook_rate_limit`, admin router included BEFORE the public router. |
| `database.py` | Clean. All money-touching methods use `SELECT … FOR UPDATE`. `finalize_partial_payment` already uses `max(already_credited, actually_paid_usd)`. `admin_adjust_balance` writes `transactions` row + updates wallet in one tx with FOR UPDATE on the user row. Part-6 added `list_transactions(gateway, status, telegram_id, page, per_page)` with allow-listed enum filters (`TRANSACTIONS_GATEWAY_VALUES`, `TRANSACTIONS_STATUS_VALUES`) and `TRANSACTIONS_MAX_PER_PAGE=200`, plus `BROADCAST_ACTIVE_DAYS_MAX=36_500` defense-in-depth cap inside `iter_broadcast_recipients`. |
| `payments.py` | Clean. Two-pass IPN verifier (raw → canonical fallback). Idempotent finalize, partial-delta crediting. |
| `handlers.py` | Clean. `cmd_start`, `_route_legacy_text_to_hub`, `process_chat`, `process_promo_input`, and `process_custom_amount_input` all guard `from_user is None` and `text is None`. |
| `web_admin.py` | aiohttp+jinja2 panel mounted under `/admin/`. HMAC-signed cookies (`ADMIN_PASSWORD` + `ADMIN_SESSION_SECRET`). Login + dashboard (Part-1). Promos page with CSRF tokens + flash banners (Part-2). Gift codes page (Part-3) with `parse_gift_form` + `EXPIRES_IN_DAYS_MAX` bound. Users page + credit/debit form (Part-4) with `parse_adjust_form`, `ADJUST_MAX_USD` bound, `ADMIN_WEB_SENTINEL_ID=0` audit attribution. Broadcast page (Part-5) with in-memory job registry (`APP_KEY_BROADCAST_JOBS` + `APP_KEY_BROADCAST_TASKS`), `asyncio.create_task` background worker, JSON polling endpoint, shares `admin._do_broadcast` via `progress_callback`; `BROADCAST_ACTIVE_DAYS_MAX` cap added in Part-6. Transactions browser (Part-6) with `parse_transactions_query` + `_encode_tx_query` helpers, paginated read against `Database.list_transactions`. |
| `templates/admin/` | jinja2 templates. `base.html` = global CSS + `<head>`; `_layout.html` = sidebar shell (extended by content pages); `login.html`, `dashboard.html`, `promos.html`, `gifts.html`, `users.html`, `user_detail.html`, `broadcast.html`, `broadcast_detail.html`, `transactions.html`. |
| `ai_engine.py` | Clean. `aiohttp.ClientTimeout(total=60, connect=10, sock_read=50)` on OpenRouter. Defensive guard for malformed responses. |
| `pricing.py` | Clean. Conservative fallback for unmapped models, markup ≥ 1.0. |
| `rate_limit.py` | `consume_chat_token(user_id)` per-user (called *inside* `handlers.process_chat`, not as a `dp.message` middleware — see PR #47/#48 history). `webhook_rate_limit_middleware` per-IP — scoped to `WEBHOOK_PATH = "/nowpayments-webhook"` only (Part-5 bundled fix) so admin panel traffic doesn't eat the same bucket. Stage-9-Step-1 added `install_login_rate_limit` + `consume_login_token` for the admin-login throttle, plus `client_ip_for_rate_limit(request)` helper that reads `X-Forwarded-For` leftmost when `TRUST_PROXY_HEADERS=1` is set (defaults off; both webhook and login limiters share the helper). |
| `admin.py` | `parse_admin_user_ids`, `is_admin`, `_escape_md`, `/admin`, `/admin_metrics`, `/admin_balance`, `/admin_credit`, `/admin_debit`, `/admin_promo_create`, `/admin_promo_list`, `/admin_promo_revoke`, `/admin_broadcast`. Part-6 added `_BROADCAST_ACTIVE_DAYS_MAX=36_500` cap in `parse_broadcast_args`. 87 unit tests. |
| `alembic/` | Clean. Baseline = consolidated current schema. `env.py` URL-encodes credentials. |
| `entrypoint.sh` | Idempotent `alembic upgrade head` then `exec python -m main`. |
| `docker-compose.yml` | postgres + redis + bot. |
| `strings.py` | Clean. Every `t()` slug exists in fa + en. |
| `.env.example` | Documents every required env var including `REDIS_URL`, `ADMIN_USER_IDS`, `COST_MARKUP`. |
| `tests/` | 286 cases. Strict-warnings pytest config + 3-job CI matrix. |
| ~~`schema.sql`, `migrations/*.sql`~~ | **Deleted in cleanup PR.** Alembic owns schema. |

---

## 11. Working agreement (read this first if you're the next AI)

The user's process for this project — **do not deviate**:

1. **Push PRs autonomously, sequentially, without blocking on per-PR
   approval.** The user explicitly said (2026-04-28): *"I'm going to sleep
   so don't wait for my approval on every pull. I pull all of them when I
   wake up. Just move forward step by step. Stick to the plan."* Take this
   as standing instruction unless the user says otherwise.

2. **One PR per logical step.** Don't bundle "scaffold web admin" with
   "promo codes UI" with "gift codes". Small, reviewable PRs.

3. **Bundle a real bug fix in every PR.** The user explicitly asked for
   this. Find an actual latent bug in the codebase — defensive guards for
   edge cases, off-by-ones, race-condition fixes, etc. **Do not invent
   fake bugs.** If you genuinely cannot find one for a given PR (e.g. a
   pure deletion PR like cleanup), pair it with a small defensive
   refactor + test. Document the bug in the PR description.

4. **Update HANDOFF.md and README.md in every PR.** The user explicitly
   asked: *"remember change your read me file and your guide for next AI in
   every single merge u push. I want to have a record on what we have when
   you are finished in every step."* The HANDOFF.md you're reading right
   now is that running record. Keep it current. Future AIs / humans will
   pick the project up from this file alone.

5. **Branch naming:** `devin/$(date +%s)-<short-description>`.

6. **PR template:** call `git_pr(action="fetch_template")` then
   `git_pr(action="create")`. The create call enforces fetch first.

7. **Never modify a test to make it pass.** Fix the code or the assumption.

8. **CI must be green before reporting completion.** Run
   `git(action="pr_checks", wait_mode="all")` after creating each PR.
   If a check fails, fix it (don't report success). The user merges manually
   when they wake up — they expect green PRs ready to merge.

9. **Devin Review feedback is real.** Treat the bot's PR-review comments
   as if a senior engineer wrote them. PR #50's Devin Review caught a real
   Markdown-escape bug; PR #44's caught the URL-encode-credentials bug.
   Both became hotfix PRs. Read the review on every PR and respond to it.

10. **Respect the live deploy.** `/root/bot_project` on the user's VPS is
    sacred. Don't ssh in, don't modify it, don't reference it as anything
    other than "the live deploy".

### Branch / commit conventions
- Branch: `devin/$(date +%s)-<description>` (timestamp + kebab-case slug).
- Commit message: `<type>(<scope>): <subject> [<stage-ref>]`
  e.g. `feat(admin): broadcast + Markdown-escape fix [Stage-7-Part-4]`
- Co-author Devin on every commit (handled automatically by the tool).

### Git etiquette
- **Never** `git add .` (catches stray files). Stage explicitly.
- **Never** `--force` push to main / master.
- **Never** `--no-verify` to skip pre-commit hooks (we don't have any
  configured, but if the user adds them, respect them).
- **Never** amend a commit. Add a new commit on top.

---

## 12. Glossary / odd things

- **IPN** = Instant Payment Notification (NowPayments' webhook).
- **FSM** = aiogram per-chat finite state machine. Backed by Redis (PR #46).
- **`COST_MARKUP`** env var, default 1.5×. Multiplier on raw OpenRouter cost.
- **`MEMORY_CONTENT_MAX_CHARS`** = 8000 (per-message buffer cap).
- **`MEMORY_CONTEXT_LIMIT`** = 30 (recent turns fed back as context).
- **`partially_paid`** = NowPayments status. User paid less than invoice.
  Credit proportional USD, NOT zero.
- **`amount_usd_credited`** has dual semantics by row state:
  - PENDING: *intended* credit at invoice creation.
  - PARTIAL/SUCCESS: *cumulative* USD already credited.
- **NowPayments source IP**: `51.75.77.69` (per prod log; not stable, don't
  gate on it — gate on HMAC).
- **NowPayments docs**: <https://documenter.getpostman.com/view/7907941/2s93JusNJt#api-documentation>
- **Bot handle**: `@Meowassist_Ai_bot` (id `8761211112`).
- **Live deploy**: `/root/bot_project`. Don't touch.

---

## 13. TL;DR

1. **All P0 / P1 / P2 / P3-Op / Stage-7 + Cleanup are shipped and merged.**
2. **Stage-8 Parts 1–6 are shipped (the whole web panel is done).**
   Reachable at `${WEBHOOK_BASE_URL}/admin/login` once
   `ADMIN_PASSWORD` + `ADMIN_SESSION_SECRET` are set in the live
   deploy. Promo codes at `/admin/promos`, gift codes at
   `/admin/gifts`, users at `/admin/users` (search → detail →
   credit/debit), broadcast at `/admin/broadcast` (form + live-polling
   detail page), transactions browser at `/admin/transactions`
   (paginated, filter by gateway/status/user, link through to the
   user-detail page). Web-initiated balance adjustments are attributed
   with `admin_telegram_id=0` sentinel and a `[web]` prefix in
   `transactions.notes` so the audit trail distinguishes web vs
   Telegram-DM adjustments. Users redeem gift codes with
   `/redeem CODE` in the bot.
3. **The IPN signature bug is fixed on `main`** (PRs #39 + #41). User
   confirmed clean log on 2026-04-28.
4. **Stage-9 queue is set** — next 10 steps prioritised per §3
   (money/security first, product surface last, operational
   hardening last). See §5 "Stage-9 queue" table. Starts with
   rate-limiting `/admin/login`.
5. **Working rule:** push PRs sequentially, bundle a real bug fix in each,
   update this doc + README in each, do NOT block on user approval. The
   user merges them when they wake up.
6. **Read the §11 working agreement before doing anything.**
