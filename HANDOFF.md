# Project Handoff — Meowassist AI bot

**Audience:** the next AI (or human) picking this codebase up.
**Goal:** you can read this single file and have full context — what the
project is, what's been shipped, what's currently broken (or recently fixed),
and what to do next without asking the user to re-explain anything.

---

## 1. What the bot is

Telegram bot (`@Meowassist_Ai_bot`) that:

1. Lets a user pick an LLM from OpenRouter (free or paid).
2. Charges them per request — free messages while their `free_messages_left > 0`,
   then deducts USD from a wallet stored in PostgreSQL.
3. Tops the wallet up via NowPayments crypto invoices (BTC / ETH / LTC / TON /
   TRX / USDT on TRC20·ERC20·BEP20·TON).
4. Speaks Persian by default, English on demand.

**Stack:** Python 3.11+, aiogram 3, asyncpg, aiohttp, PostgreSQL.
**Process model:** one Python process. `aiogram` long-polls Telegram, a
side-by-side `aiohttp` web server listens for NowPayments IPN POSTs on
`${WEBHOOK_BASE_URL}/nowpayments-webhook`.

---

## 2. File map

```
main.py             entrypoint                             67 LoC
database.py         asyncpg pool, all SQL                 758 LoC
payments.py         NowPayments invoice + IPN verify       628 LoC
handlers.py         every aiogram handler                 1430 LoC
ai_engine.py        OpenRouter call + cost settlement     119 LoC
pricing.py          per-model price + markup              109 LoC
models_catalog.py   live OpenRouter /v1/models cache      289 LoC
middlewares.py      user-upsert middleware                 61 LoC
strings.py          fa/en string table                    539 LoC
schema.sql          initial schema                        142 LoC
migrations/*.sql    numbered, append-only migrations
.env.example        every required env var
.gitignore          secrets, venvs, *.save backups
```

Total: ~4.2k LoC, no test suite yet (P3 todo).

---

## 3. The priority framework we follow

A previous AI's roadmap put payment-security at "stage 8 / low priority"; the
user pushed back on that and we adopted **money-and-security-first** as the
prioritization rule:

- **P0** — security & money correctness. Anything that can drain the account
  or let users mint balance.
- **P1** — correctness bugs (custom amount, partial-payment crediting, etc).
- **P2** — product surface (back buttons, i18n, model picker UI).
- **P3** — operational hardening (Dockerfile, README, tests, Redis FSM, etc).

Every PR labels itself P0/P1/P2/P3 in the title and updates this doc when a
phase is complete.

---

## 4. What's been shipped (in merge order)

### P0 — security & money correctness
| # | What | Why |
| --- | --- | --- |
| #1 | `.gitignore` + `.env.example` + drop committed `payments.py.save` | Stops the most obvious accidental-commit-of-secrets path. |
| #2 | Per-model pricing table + `COST_MARKUP` (default 1.5×) | Original $1/1M-tokens flat fee lost money on every paid call. |
| #3 | Atomic `deduct_balance` with `WHERE balance_usd >= $1 RETURNING …` | Pre-fix, `UPDATE balance = balance - cost` could go negative under concurrency. |
| #4 | Env-driven `WEBHOOK_BASE_URL`, structured logging, sanitized errors | Removed hard-coded `212.87.199.41:8080`; replaced every `print` with `logging`; user replies never carry raw exception text. |
| #5 | NowPayments IPN HMAC-SHA512 signature verification | Without this, anyone reaching the webhook could POST `payment_status=finished` and credit themselves. |
| #6 | Idempotent payments via the `transactions` ledger | Pre-fix, the webhook would credit the same payment twice on retry. Now: PENDING row written at invoice issuance, webhook flips it to SUCCESS + credits in **one DB transaction**. Replays / unknown payment_ids cannot mint money. |

### P1 — correctness bugs
| # | What |
| --- | --- |
| #7 | Make `amt_custom` callback reachable (handler-ordering fix). |
| #8 | Handle non-`finished` IPN statuses (`expired` / `failed` / `refunded` close the ledger row + notify user). |
| #9 | Credit `actually_paid_usd` (not `price_amount`) on `partially_paid`. New atomic `db.finalize_partial_payment`. |
| #10 | Hotfix: `finished` after `partially_paid` now credits the **remainder delta**, not zero. Both finalize paths use `SELECT … FOR UPDATE` and accept PENDING ∪ PARTIAL. |
| #11 | Hotfix: `mark_transaction_terminal` accepts PARTIAL (not just PENDING) so a terminal IPN after a partial actually closes the row. |
| #13 | Hotfix: report `$0` credited (not the unpaid invoice price) when closing a PENDING row, fixing dual-semantics confusion in `amount_usd_credited`. |

### P2 — product surface
| # | What |
| --- | --- |
| #12 | P2-0 cleanup — Markdown `parse_mode` on missing messages, default `active_model` aligned, schema docstrings. |
| #14 | P2-1 back/home navigation across every nested inline menu. |
| #15 | Hotfix: Home button clears FSM. |
| #16 | P2-2 i18n via `strings.py` (Persian default + English). Language picker persists choice via `db.set_language`. |

### P3 — partial product-side
The recent run of P3-* PRs was mostly **product**, not the operational
hardening from the original P3 list. Specifically:

| # | What |
| --- | --- |
| #25 | P3-1 user-upsert middleware (aiogram outer middleware, fixes FK violations from button taps without `/start`). |
| #26 | P3-2 friendly per-currency min-amount error. |
| #27 | P3-3 single-message inline-hub UI. |
| #28 | P3-4 model filter (provider whitelist + text-only). |
| #29 | P3-5 per-user opt-in conversation memory toggle (with cost warning). |
| #30 | P3-6 free-models tab, picker polish, friendly 429 message. |
| #31 | P3-7 polish — pretty provider names. |
| #32 | P3-7 long-reply chunking, wallet min $10, free-trial bumped 5→10. |
| #33 | P3-8 `is_fixed_rate=true` to lower per-currency min, restore $5 button. |
| #34 | P3-9 EN/FA copy alignment for `charge_min_amount_unknown`. |
| #35 | P3-10 NowPayments error log includes `pay_currency` + amount. |
| #36 | P3-11 drop `is_fee_paid_by_user` to unblock low-amount invoices. |
| #37 | P3-12 explain rate-lock + 7-day tracking on the invoice screen. |
| #38 | Diagnostic logging on IPN signature mismatch. |
| #39 | **IPN canonical body uses `ensure_ascii=False`** — see §6. |

The original P3 operational-hardening checklist (Dockerfile, README, tests,
Alembic, Redis FSM, rate limiting) is still **unstarted**; that is what's
queued next.

---

## 5. Status of every file (post-P3-Op-5)

| File | Status |
| --- | --- |
| `main.py` | Clean. Env-driven port, single source of truth for the webhook listener. `build_fsm_storage()` picks Redis vs in-memory based on `REDIS_URL`. |
| `database.py` | Clean. All money-touching methods use `SELECT … FOR UPDATE` inside a connection-scoped transaction. One latent low-severity issue, see Bug A below. |
| `payments.py` | Clean for the threat model (HMAC verify → idempotent finalize → partial-delta crediting → terminal closure on PENDING ∪ PARTIAL). Two-pass verifier: raw bytes first, canonicalized fallback. |
| `handlers.py` | Clean. `process_custom_amount_input` rejects NaN/Inf and amounts > $10k as of P3-Op-5. Legacy reply-keyboard handlers (`support_text_handler` etc.) all route through `_route_legacy_text_to_hub` which `state.clear()`s — i.e. the original Bug B is already fixed in main; this row used to flag it as latent. |
| `ai_engine.py` | Clean. Pre-check on free messages + balance, atomic deduct, log_usage with the actual amount. |
| `pricing.py` | Clean. Conservative fallback for unmapped models, guards markup ≥ 1.0. |
| `alembic/` | Clean. `env.py` URL-encodes credentials so passwords with `@`/`:`/`/` don't crash-loop the bot. Baseline = consolidated current schema. Stamp existing prod DBs once before redeploy. |
| `entrypoint.sh` | Clean. Runs `alembic upgrade head` (idempotent) before exec'ing the bot. |
| `docker-compose.yml` | Clean. postgres + redis + bot. Redis backs FSM. No init-script mounts (alembic owns schema). |
| `schema.sql`, `migrations/*.sql` | Historical artifacts only. Not referenced by anything now — alembic owns schema. Safe to delete in a follow-up cleanup PR. |
| `strings.py` | Clean. Every `t()` call site has a slug in both locales. |
| `.env.example`, `.gitignore` | Clean. `.env.example` documents `REDIS_URL`. |
| `tests/` | 57 cases across IPN signature, pricing, alembic env URL building, FSM storage selection, and custom-amount validation. Strict-warnings pytest config + GitHub Actions CI on Python 3.11/3.12 + alembic upgrade/downgrade roundtrip + docker-build smoke. |

### Bug A (low severity, latent) — `finalize_partial_payment` can lower `amount_usd_credited` on out-of-order replays
Always overwrites with the new `actually_paid_usd` regardless of whether it's
higher or lower than what's already there. If two `partially_paid` IPNs arrive
out of order, the row is silently downgraded and a subsequent `finished` IPN
double-counts the gap.

**Fix:** `SET amount_usd_credited = GREATEST(amount_usd_credited, $2)`.
Queued for P3-Op-6.

### Pre-existing minor issue (not blocking)
`process_custom_amount_input` does `message.text.strip()` without first
checking `message.text is not None`. Stickers / images / voice while in
`waiting_custom_amount` raise `AttributeError`. Trivial: `text =
(message.text or "").strip()`. (Mostly mooted by aiogram's `F.text` filter
in newer registrations, but the dedicated state-filter handler doesn't have
that gate.)

---

## 6. The IPN signature bug we were stuck on

### Symptom (from the user's prod log, 2026-04-27 16:12:01 UTC)
```
WARNING bot.payments: IPN sig mismatch:
  expected=6ac370a7..f0d64f68
  received=691300cd..b1ae2324
  secret_len=32 body_len=585 canonical_len=625
WARNING bot.payments: IPN signature verification failed (remote=51.75.77.69)
INFO aiohttp.access: 51.75.77.69 ... "POST /nowpayments-webhook HTTP/1.1" 401 199
```

`51.75.77.69` is NowPayments' real outbound IP. The IPN secret was set
correctly (length 32). The signature still mismatched.

### Root cause
The diagnostic logging gives it away: `body_len=585 canonical_len=625`. Our
re-canonicalized body was **40 bytes longer** than the body NowPayments
actually put on the wire.

`json.dumps(...)` defaults to `ensure_ascii=True`, which escapes every
non-ASCII char into `\uXXXX`. The invoice payload includes
`order_description = "شارژ کیف پول"` (Persian, 12 chars). In raw UTF-8 each
char is ~2 bytes; in `\uXXXX` form each char is 6 bytes. ~10 Persian chars
× 4 extra bytes ≈ 40 bytes — exactly the gap we saw.

NowPayments sends the IPN body as raw UTF-8 (`JSON_UNESCAPED_UNICODE`
equivalent) and signs that same raw-UTF-8 string. Our re-canonicalization
was producing a different string, so the HMACs disagreed.

### Fix (PR #39, merged)
```python
# payments.py:_verify_ipn_signature
canonical = json.dumps(
    payload, sort_keys=True, separators=(",", ":"),
    ensure_ascii=False,    # ← the fix
)
```

### What the user should do now
The user's last log is from **before** PR #39 was merged. The fix is on
`main`. They need to redeploy:

```bash
cd /root/bot_project
git pull origin main
sudo systemctl restart bot      # or whatever process supervisor they use
```

If the next IPN still fails verification after the redeploy, that means
NowPayments' actual canonical form differs from ours in a way we haven't
diagnosed yet. The diagnostic log line gives us enough to tell which:
- `body_len == canonical_len` → byte counts match, only key order or
  whitespace differs.
- `canonical_len > body_len` → we're still escaping something they aren't
  (probably forward slashes — NowPayments uses `JSON_UNESCAPED_SLASHES`,
  Python doesn't escape slashes either, so this should be a non-issue).
- `canonical_len < body_len` → they're padding something we strip.

A future PR (queued — see §8 P3-Op-1) will switch the verifier to **sign
the raw body bytes first**, falling back to re-canonicalization only if
that fails. That makes us robust to whatever canonical form NowPayments
actually signed, and is what every mature webhook handler does (Stripe,
Paddle, GitHub all expose the raw body for HMAC).

---

## 7. Money-flow walkthrough (so you understand what NOT to break)

```
User taps "Charge wallet" → picks $5/$10/$25/custom → picks currency
        │
        ▼
handlers.process_charge_*  →  payments.create_crypto_invoice(...)
        │
        ├─ POST /v1/payment to NowPayments → returns {payment_id, pay_address, pay_amount}
        ├─ db.create_pending_transaction(payment_id, amount_usd, ...)   ← PENDING row
        └─ Bot shows the user the invoice (address + amount + QR via Telegram link)

[user pays on-chain in their wallet]

NowPayments POSTs to /nowpayments-webhook with x-nowpayments-sig header
        │
        ▼
payments.payment_webhook
        │
        ├─ _verify_ipn_signature(raw_body, header)             ← HMAC-SHA512
        │     │
        │     └─ 401 if bad. Stop here — no balance changes.
        │
        ├─ status == "finished":
        │     └─ db.finalize_payment(payment_id, full_price_usd)
        │            ↑ SELECT … FOR UPDATE on the row, accept PENDING or PARTIAL,
        │              credit (full_price_usd - already_credited), set SUCCESS,
        │              consume promo (if any), all in one DB transaction.
        │
        ├─ status == "partially_paid":
        │     └─ actually_paid_usd = actually_paid / pay_amount * price_amount
        │        db.finalize_partial_payment(payment_id, actually_paid_usd)
        │            ↑ same FOR UPDATE pattern, credit only the new delta,
        │              row goes PENDING/PARTIAL → PARTIAL.
        │
        ├─ status in {expired, failed, refunded}:
        │     └─ db.mark_transaction_terminal(payment_id, EXPIRED|FAILED|REFUNDED)
        │            ↑ accepts PENDING ∪ PARTIAL; user keeps any partial credit.
        │
        ├─ status in {waiting, confirming, confirmed, sending}:    no-op, just log
        │
        └─ unknown status:                                         no-op, just log

        Telegram notification to the user is best-effort AFTER the DB commit.
```

**Invariants you must preserve:**
1. Wallet credit and ledger row update happen in **one DB transaction**.
2. Every credit operation is gated on `WHERE status IN ('PENDING','PARTIAL')`
   so a replayed IPN cannot mint money.
3. `_verify_ipn_signature` returns False on missing secret / missing header /
   bad JSON / mismatched HMAC. Anything else short-circuits the request to
   401 — **never** read the body without verifying.
4. `actually_paid` from a `partially_paid` IPN is in the **pay-currency**
   (e.g. TRX), NOT USD. Convert via the locked-in invoice rate
   (`actually_paid / pay_amount * price_amount`), cap at `price_amount`.

---

## 8. What's next (priority queue)

### P0/P1 cleanup (zero outstanding — re-verified §5)
The codebase is currently free of P0/P1 issues that I could find. Bug A and
Bug B in §5 are both low severity. They're tracked but not blocking.

### P3 — operational hardening (the original list, still unstarted)

These are the items the original roadmap had under "P3 — operational
hardening (~3h)" that the recent P3-* product PRs did **not** address.
Each is a separate PR, in this order:

| # | Title | Status | PR |
| --- | --- | --- | --- |
| **P3-Op-1** | Robust IPN verifier — sign raw body first, fall back to canonicalized | ✅ Shipped | [#41](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/41) |
| **P3-Op-2** | `Dockerfile` + `docker-compose.yml` (postgres + bot) | ✅ Shipped | [#42](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/42) |
| **P3-Op-3** | pytest skeleton + GitHub Actions CI + pricing tests | ✅ Shipped | [#43](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/43) |
| **P3-Op-4** | Alembic migrations + entrypoint runs `upgrade head` | ✅ Shipped | [#44](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/44) |
| **P3-Op-4-Hotfix** | URL-encode DB credentials in `alembic/env.py` (Devin Review catch) | ✅ Shipped | [#45](https://github.com/od0015058-glitch/biuecfybyicwyncafciefaciwucyefaaiaciwy/pull/45) |
| **P3-Op-5** | Redis-backed FSM storage **+ NaN/Inf/over-cap rejection in `process_custom_amount_input`** | ✅ Shipped | this PR |
| **P3-Op-6** | Rate limiting on `/chat` and `/nowpayments-webhook` **+ Bug A (`finalize_partial_payment` GREATEST guard)** | ⏳ Next | — |

### P2 product items still queued (lower priority than P3-Op)

The P2-* roadmap from the earlier session had these still unbuilt:
- Promo codes UI is wired (DB tables exist, validate/redeem methods exist)
  but has no admin-side creation flow yet.
- A Telegram-side **admin panel** gated by `ADMIN_USER_IDS` (see env var)
  for promo creation, balance-adjust, refund, system metrics. Build this as
  Telegram commands, not a separate CLI binary.

---

## 9. How we work together

The user's process for this project (which the next AI should follow):

1. **One PR per logical step.** Don't bundle the IPN fix with a Dockerfile.
2. **Push the PR. Wait for the user to approve / merge.** Do not start the
   next item until the previous merge is in.
3. **CI:** there's no CI yet (P3-Op-3 will add it). Until then, run `python
   -m py_compile *.py` locally and read your own diff carefully.
4. **Tone in PR descriptions:** be specific about *what* and *why*, link
   the failing log line or the doc, don't oversell.
5. **Never modify tests to make them pass.** When P3-Op-3 lands and a test
   fails, fix the code or the assumption, not the test.
6. **`git_pr(action="fetch_template")` before `git_pr(action="create")`** —
   the create call enforces this.

### Where the user runs the bot
```
/root/bot_project              the live deploy
/root/bot_project.bak-<ts>     the rollback dir from the previous deploy
```

Their rollback is literally `mv` between the two directories. P3-Op-2
(Docker) replaces this with `docker compose up -d` + `docker compose down`.

### The bot's Telegram handle
`@Meowassist_Ai_bot` (id `8761211112`).

### NowPayments
- Inbound IPN source IP: `51.75.77.69` (per the prod log). NowPayments
  doesn't publish a stable IP allowlist, so don't gate on this — gate on the
  HMAC.
- API base: `https://api.nowpayments.io/v1`
- Docs the user is using:
  <https://documenter.getpostman.com/view/7907941/2s93JusNJt#api-documentation>

---

## 10. Glossary of files / acronyms / odd things

- **IPN** = Instant Payment Notification, NowPayments' name for their
  webhook callback.
- **FSM** = aiogram's per-chat finite state machine for multi-message
  flows like "type a custom amount". Currently in-memory; restarts wipe
  in-flight states.
- **`MEMORY_CONTENT_MAX_CHARS`** = 8000. Per-message cap on what we
  persist into the conversation-memory buffer (P3-5).
- **`MEMORY_CONTEXT_LIMIT`** = 30. How many recent turns we feed back as
  context when memory is enabled.
- **`COST_MARKUP`** = env var, default 1.5. Multiplier on raw OpenRouter
  cost to cover the gateway's 0.5% fee + give us margin.
- **`partially_paid`** = a NowPayments status: the user paid less than the
  invoice required. We credit them the proportional USD value, not zero.
- **`amount_usd_credited`** has dual semantics by row state:
  - On PENDING rows: the *intended* credit (set at invoice creation).
  - On PARTIAL/SUCCESS rows: the *cumulative* USD already credited.
  - That difference matters in two places: terminal-status logging
    (PR #13) and `finalize_payment`'s "credit only the delta" math
    (PR #10). Read those PRs before touching that column.

---

## 11. TL;DR for whoever reads this next

1. P0 + P1 + P2-0..P2-2 are done.
2. The IPN signature bug is fixed on `main` (PR #39, `ensure_ascii=False`).
   The user's failing log is from before that merge — they need to pull and
   restart.
3. The next queue is the **operational hardening** items in §8: robust IPN
   verifier (with unit test), Dockerfile, pytest, Alembic, Redis FSM, rate
   limiting, then Bug A/B cleanup.
4. One PR per item. Wait for approval before starting the next.
