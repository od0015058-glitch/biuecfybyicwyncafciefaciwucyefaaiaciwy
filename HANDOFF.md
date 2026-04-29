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
tests/              pytest, ~534 cases
.github/workflows/ci.yml   3.11/3.12 matrix + alembic roundtrip + docker build
```

Total: ~6.7k LoC, 534 tests, full CI on every push.

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
| **Stage-12-Step-C** | **User-side TetraPay receipts in `/wallet`.** Currently a TetraPay user has no in-bot way to look up "what did I top up last week, with which card?" — the data lives in `transactions` but is only exposed via the admin panel. Add a "🧾 Recent top-ups" button to the wallet keyboard that pages through the user's last N (default 5, `RECEIPTS_PAGE_SIZE` env var, capped at 20) SUCCESS / PARTIAL / REFUNDED transactions. Per row: amount in USD, gateway-friendly label (NowPayments shows `BTC` / `TRX` etc., TetraPay shows the `gateway_locked_rate_toman_per_usd` for the locked Toman amount), status badge, completed_at in user's locale. Reuses the Stage-11-Step-B amount formatter for Toman lines. **Bug-fix candidate:** `Database.list_transactions` (the admin-side helper) already has the right shape but takes `telegram_id` as one of many filters — an unauthenticated request from another user couldn't list someone else's transactions today (the panel auth catches it), but exposing this on the user side would need a **new** `Database.list_user_transactions(telegram_id, limit, before_id)` that hard-codes the user filter and refuses an unset `telegram_id` so a future caller can't accidentally drop the WHERE clause. | P2 product | ⏳ pending |
| **Stage-12-Step-D** | **Gift-code redemption stats web page.** Currently `/admin/gifts` lists codes and their `redemptions_count` cell, but there's no per-code drill-down — to see who redeemed a code an admin has to query the DB directly. New `/admin/gifts/{code}/redemptions` page lists every `gift_redemptions` row for a code (telegram_id, redeemed_at, USD credited at redemption time). Mirror the Stage-9-Step-8 user-usage browser pattern (paginated, per-page picker, prev/next). Linked from the existing gifts list ("`N` redemptions →" cell). **Bug-fix candidate:** `Database.get_gift_redemptions` may not have an index on `(code_id, redeemed_at)` — at scale (a popular code redeemed by thousands) this would be a sequential scan. Verify in the PR; if missing, add an alembic index migration like Stage-9-Step-8 did for `usage_logs`. | P3 product | ⏳ pending |

Dependency order: A is independent and gates the others (refunds is a P0 because a user dispute today has no in-product path). B can ship anytime after A (independent). C and D are independent leaves.

Deferred / explicitly out of Stage-12 scope: (a) the live TetraPay `/api/refund` call (gateway-side automated refund — Step-A.5 follow-up if user asks); (b) multi-step approval workflows on refunds (single admin's signature is fine for the bot's current scale); (c) user-initiated refund requests from the bot side (would require a dispute UX + admin queue — much larger scope).

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
7. **Stage-12 queue is set** — see new §5 "Stage-12 queue" table.
   Prioritised per §3: A (refunds/chargebacks UI, P0 correctness),
   B (stuck-payment proactive admin DM, P1 ops), C (user-side TetraPay
   receipts in `/wallet`, P2 product), D (gift-code redemption stats
   web page, P3 product). User direction 2026-04-29: do all four,
   one PR each, in that order.
8. **Working rule:** push PRs sequentially, bundle a real bug fix in each,
   update this doc + README in each, do NOT block on user approval. The
   user merges them when they wake up.
9. **Read the §11 working agreement before doing anything.**
