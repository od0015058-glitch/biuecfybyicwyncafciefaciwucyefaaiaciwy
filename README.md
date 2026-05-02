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
- **Monetization dashboard** at `${WEBHOOK_BASE_URL}/admin/monetization`
  surfaces the bot's revenue (gateway top-ups, with the same
  admin/gift filter as the main dashboard's "Total revenue" tile),
  wallet charges (`SUM(cost_deducted_usd)` from `usage_logs`), the
  *implied* OpenRouter cost (charges divided by the current
  `COST_MARKUP`), gross margin (charges − OpenRouter cost), and net
  profit (revenue − OpenRouter cost) — both lifetime and over a
  trailing window selectable via a `?window=7|30|90` query param
  (defaults to 30 days; the segmented pill control at the top-right
  of the "Last N days" panel switches between week / month /
  quarter views). Includes a per-model breakdown over the same
  window sorted by charged USD descending so the biggest margin
  contributors are at the top. Footnotes on the page spell out the
  assumptions (implied OR cost drifts when `COST_MARKUP` changes;
  net profit is forward-looking — assumes every credited dollar
  will eventually burn). The **"⬇ Export CSV" button** at the
  top-right (Stage-15-Step-E #9 follow-up #2) downloads the same
  data as a single CSV with a `scope` column (`lifetime` /
  `window` / `window_by_model`) so an operator can pivot it for
  monthly P&amp;L without screen-scraping; the export uses
  `MONETIZATION_CSV_TOP_MODELS_LIMIT=1000` (vs. the on-screen
  `_MONETIZATION_TOP_MODELS_LIMIT=10`) so the long-tail models are
  included for offline analysis. Each export records a
  `monetization_export_csv` audit row with the active window and
  row count. The page also renders a **"Top users by revenue" panel**
  (Stage-15-Step-E #9 follow-up #3) ranking the top-10 users by
  total gateway revenue over the same trailing window. Each row
  shows the user's `@username` (linking to `/admin/users/<telegram_id>`)
  — or the bare telegram id when the username is null — plus their
  top-up count, total revenue, and total wallet charges over the
  window so an operator can cross-reference "are big spenders also
  big consumers?" at a glance. CSV export includes the same data as
  trailing `window_top_users` rows with three new trailing columns
  (`telegram_id`, `username`, `topup_count`) appended at the end so
  existing column positions for `lifetime` / `window` /
  `window_by_model` rows don't shift; the CSV pulls
  `MONETIZATION_CSV_TOP_USERS_LIMIT=1000` rows for the long tail.
  The **"Edit markup (operator+)" details panel** (Stage-15-Step-E
  #10b row 2) lets an operator retune the global price multiplier
  `COST_MARKUP` without redeploying the bot. The panel renders the
  same "effective / db / env / default" breakdown that the
  bot-health thresholds card on `/admin/control` uses — so it's
  obvious whether the active value came from a runtime override,
  the `COST_MARKUP` env var, or the 1.5× compile-time default.
  Submitting a new value writes a row keyed `COST_MARKUP` to the
  generic `system_settings` overlay (the same DB-backed override
  table the bot-health thresholds use), refreshes the in-process
  `pricing.get_markup()` cache, and records a
  `monetization_markup_update` audit row whose `meta` carries the
  before/after diff. Submitting an empty value clears the override
  (falls through to env / default). Validators refuse anything
  below `MARKUP_MINIMUM=1.0` (operators can't accidentally charge
  less than cost) and at-or-above `MARKUP_OVERRIDE_MAXIMUM=100.0`
  (a fat-finger `150` for `1.50` is rejected rather than silently
  100× every charge).
- **Trial-allowance editor** at `/admin/wallet-config` (Stage-15-Step-E
  #10b row 6) — operator-floored editor for `FREE_MESSAGES_PER_USER`,
  the trial-message allowance granted at `/start` time. Writes a
  `free_messages_per_user` row to `system_settings`, warms an
  in-process override cache in `free_trial.py`, and `Database.create_user`
  binds the resolved allowance to the `INSERT INTO users (..., free_messages_left)`
  statement so a saved override applies to every brand-new registrant
  without a process restart. Existing users are unaffected
  (`ON CONFLICT (telegram_id) DO NOTHING`); to retroactively top up
  one user, use the balance-adjust form on `/admin/users/<id>`. Bounds
  `[0, 10_000]` — explicit `0` is allowed (closed-beta "pay-to-play
  only" path); the upper cap exists so a fat-finger can't quietly
  burn through the trial budget. Audit slug
  `wallet_config_free_messages_update` carries before/after with the
  resolution source (`db` / `env` / `default`) for clean attribution
  in the audit feed.
- **Memory-config editor** at `/admin/memory-config` (Stage-15-Step-E
  #10b row 8) — two operator-floored editors for
  `MEMORY_CONTEXT_LIMIT` (messages per turn, `[1, 500]`, default 30)
  and `MEMORY_CONTENT_MAX_CHARS` (per-message content cap, `[100,
  100_000]`, default 8 000). DB-backed override in new
  `memory_config.py`; boot warm-up; `database.py` calls
  `get_memory_context_limit()` / `get_memory_content_max_chars()`
  instead of hardcoded class attrs. Env vars are the second-priority
  fallback; compile-time defaults are last. Audit slugs
  `memory_config_context_limit_update` /
  `memory_config_content_max_update`.
- **Audit retention policy** on `/admin/audit` (Stage-15-Step-E #10b
  row 20) — configurable retention window for the `admin_audit_log`
  table. DB-backed override in new `audit_retention.py`; background
  reaper loop batch-deletes rows older than `AUDIT_RETENTION_DAYS`
  (default 90, range [7, 3 650]). Collapsible editor card with
  breakdown table (DB / env / default / effective), set/clear form.
  Audit slug `audit_retention_update`.
- **Model discovery interval editor** on `/admin/models-config`
  (Stage-15-Step-E #10b row 23) — DB-backed override for
  `DISCOVERY_INTERVAL_SECONDS` (default 21 600 s / 6 h, range
  [60, 604 800]). The background discovery loop re-reads the
  interval from the DB-backed config on every tick.
- **Admin password rotation** on `/admin/profile` (Stage-15-Step-E
  #10b row 25) — DB-backed scrypt-hashed override for the panel
  login password. Stored under `system_settings.ADMIN_PASSWORD_HASH`
  in the canonical `scrypt$N$r$p$salt$hash` format (n = 2¹⁵, r = 8,
  p = 1) so a future cost-factor bump verifies older hashes without
  a flag-day rotation. Login flow prefers the DB hash → falls back
  to the env `ADMIN_PASSWORD` plaintext → refuses every sign-in if
  neither is configured. Rotation form gated to `ROLE_SUPER` (the
  password owner) with strength gate (≥12 chars, must include
  letter + digit/symbol, refuses whitespace-only and current-equals-
  new). Bundled bug fix: `/admin/logout` now clears the signed
  view-as cookie and the flash cookie in addition to the session
  cookie — prior impl leaked the prior operator's "viewing as
  &lt;role&gt;" preview into the next sign-in on a shared workstation.
- Telegram-side admin commands (`/admin`, `/admin_metrics`,
  `/admin_credit`, `/admin_broadcast`, …) for ops via DMs.
- **Canonical slash-command menu** — on every startup the bot
  publishes its user-facing command list (`/start`, `/redeem`) via
  `Bot.set_my_commands` so Telegram's `/` popup never shows stale
  entries left over from BotFather's "Edit Commands" panel. Admin
  commands are scoped per-admin via `BotCommandScopeChat` so
  non-admins don't see them. The admin scope advertises every
  `/admin*` handler the bot ships, including the role-system
  commands (`/admin_role_grant`, `/admin_role_revoke`,
  `/admin_role_list`); a regression test scans `admin.py` and pins
  both directions (every `Command("admin_*")` handler must appear in
  the menu, and every menu entry must have a matching handler).
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
  loops), IPN drop counters (NowPayments + TetraPay + Zarinpal —
  exposed as `meowassist_ipn_drops_total`,
  `meowassist_tetrapay_drops_total`, `meowassist_zarinpal_drops_total`)
  broken down by reason, the in-flight chat-slot gauge, the count of
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
  drop-counter table for NowPayments, TetraPay, and Zarinpal so an
  operator can spot a misconfigured webhook (signature mismatch,
  unknown invoice, transient verify failures) at a glance without
  shelling into the Prometheus scrape. Counters reset on every
  bot restart; for long-running history, pull `/metrics` into
  Prometheus. Gateway tiles are independently fault-isolated — a
  future regression in one accessor cannot blank the other two.
- **Conversation history export** — the memory screen now has a
  "📥 Export conversation" button (or run `/history`) that ships
  the user's full persisted buffer back as one or more `.txt`
  documents (role labels + ISO-8601 UTC timestamps + a summary
  header). Buffers up to 1 MB land as a single file with the
  legacy filename pattern. Larger buffers are paginated into
  parts of up to 1 MB each (`-part-NN-of-M.txt` filename suffix,
  zero-padded for lex-sort order, `Part: N/M` line in each part's
  header), capped at 10 parts × 10 MB total — a heavy user with
  months of memory ON gets the full archive instead of having
  the earliest content silently trimmed away. When the buffer
  exceeds the 10 MB total budget the oldest messages are trimmed
  first (the trim count lands on part 1's header only); the
  caption count for each document matches that part's body
  (which used to lie pre-Step-E #2 — see HANDOFF §5 Step-E #2
  bundled bug fix). The `/history` slash-command surface and the
  wallet-menu callback share `_build_history_export_documents` so
  they can never drift on filename / encoding / pagination
  semantics; the slash path is rate-limited via the same chat-
  token bucket as the AI-chat handler so a user can't pivot from
  "out of AI prompts" to "spam an unbounded
  `conversation_messages` table scan". The trim loop itself runs
  in O(n) bytes processed (was O(n²) pre-Step-E #1 follow-up).
- **Per-user spending dashboard** — a new "📊 My usage stats"
  button on the wallet menu opens a per-user analytics screen
  showing lifetime totals (calls / tokens / spent), the same
  totals over a rolling 30-day window, and the user's top 5
  models by call count. Same SQL shape as the admin-side
  dashboard; the DB method (`Database.get_user_spending_summary`)
  hard-codes `WHERE telegram_id = $1` on every sub-query so a
  buggy caller can't leak someone else's totals. Stage-15-Step-E
  #2 follow-up: typed `/stats` slash command (with optional
  `/stats 7` / `/stats 90` / `/stats 365` window arg) lands as a
  fresh message bubble; an inline window selector (`7d` / `30d`
  / `90d` / `365d`) on the stats screen pivots between rolling
  windows in-place — the currently-selected window is prefixed
  with `✓` so the user can read which one they're on without
  scrolling. Stage-15-Step-E #2 follow-up #3: per-day spending
  breakdown rendered as ASCII bars in a fenced code block at the
  bottom of the stats screen (`Database.get_user_daily_spending`
  groups `usage_logs` by `date_trunc('day', created_at)` over the
  selected window). Bar widths are proportional to the day's cost
  relative to the busiest day in the window; missing-usage days
  are padded as zero-height bars so the date axis stays
  continuous from oldest → newest. Defense-in-depth: the
  formatter drops malformed rows (non-ISO date, NaN/Inf cost) and
  the snapshot dict only contains finite floats — pre-fix
  `float(Decimal('NaN') or 0)` propagated NaN through to other
  callers because `Decimal('NaN')` is truthy in Python.
  Stage-15-Step-E #2 follow-up #4: a "📤 Download usage CSV"
  button on the same stats screen (or `/usage_csv` slash) ships
  the user's full `usage_logs` history back as an RFC-4180 CSV
  document with a UTF-8 BOM (so Excel auto-detects the encoding
  instead of mojibaking Persian model names) and `\n` line
  terminators. Header row pinned to
  `id,created_at,model,prompt_tokens,completion_tokens,total_tokens,cost_usd`
  matching the admin-side `/admin/users/<id>/usage` table column
  order so a user comparing the CSV → admin screenshot doesn't
  have to mentally re-sort. Six-fractional-digit cost precision
  matches the `cost_deducted_usd DECIMAL(10,6)` column. Hard
  5 MB cap; oldest rows are trimmed first when a heavy buffer
  overflows, and the kept count is surfaced in the upload
  caption so a heavy user whose buffer was trimmed sees the
  truth (matches the conversation-export caption fix from
  Step-E #2). The DB query is clamped at 50 000 rows so a buggy
  caller can't tar-pit the connection. Both surfaces share
  `_build_usage_csv_export_document` — same shape as the
  conversation-history export pair from Step-E #1.
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
  procedure if you flip back. Stage-15-Step-E #3 follow-up:
  `set_webhook` retries up to 3 times with 1s/2s exponential
  backoff on transient `TelegramServerError` /
  `TelegramNetworkError`; opt-in IP allowlist via
  `TELEGRAM_WEBHOOK_IP_ALLOWLIST=default` (or a comma-separated
  CIDR list) layers Telegram's published delivery ranges
  (`149.154.160.0/20`, `91.108.4.0/22`) on top of the secret
  check; stateless `GET /telegram-webhook/healthz` returns 200
  with a tiny JSON body for load-balancer / k8s liveness probes
  (no secret in the response, no Telegram round-trip per probe,
  not rate-limited).
- **DB-tracked admin roles wired into every Telegram-side handler**
  — `viewer`, `operator`, and `super` roles live in the
  `admin_roles` table; the hierarchy lives in `admin_roles.py`
  (`role_at_least`, `effective_role`). Three new commands manage
  the rows: `/admin_role_grant <user_id> <role> [notes]`,
  `/admin_role_revoke <user_id>`, `/admin_role_list`. Role grants
  are audit-logged with `action=role_grant`/`role_revoke` so the
  trail surfaces in `${WEBHOOK_BASE_URL}/admin/audit`. Per-handler
  floors (Stage-15-Step-E #5 follow-up): `/admin_metrics` and
  `/admin_balance` at `viewer`; `/admin_broadcast` at `operator`;
  `/admin_credit`, `/admin_debit`, and `/admin_promo_*` at
  `super`. The `/admin_role_*` handlers stay env-list-only — a
  DB-tracked super must NOT be able to self-promote out of the
  role table. The `/admin` hub message is rendered per-actor and
  only lists rows the caller can drive (so a `viewer` typing
  `/admin` sees just `/admin_metrics` + `/admin_balance`, not
  `/admin_credit`). Backward compatible: any Telegram id in
  `ADMIN_USER_IDS` keeps `super` access through
  `admin_roles.effective_role`'s env-list fallback so legacy
  deploys don't change behaviour. **`/admin/roles` web page
  (Stage-15-Step-E #5 follow-up #2)** is the browser counterpart
  to the Telegram CLI: `GET /admin/roles` lists every DB-tracked
  grant with telegram id, role badge, granted-at, granted-by,
  and notes columns; `POST /admin/roles` writes a grant; `POST
  /admin/roles/{telegram_id}/revoke` drops the row. Same
  `ADMIN_PASSWORD`-gated cookie as the rest of the panel, both
  write paths CSRF-protected, every action audit-logged with the
  same `role_grant` / `role_revoke` slugs the Telegram side
  emits (so the `/admin/audit` filter dropdown shows web and
  Telegram grants in one feed). Notes are NUL-byte-stripped
  before INSERT (mirrors the Stage-15-Step-E #10 fix on
  `append_conversation_message`) so a clipboard paste with
  embedded `\x00` doesn't demote the whole grant to a generic
  "DB write failed" error. Web-side per-handler role gating
  remains the open follow-up; today the page lives behind the
  same single-password gate as every other admin tab.
  **First-login auto-promote (Stage-15-Step-E #5 follow-up #3)** —
  `admin_roles.ensure_env_admins_have_roles(db, admin_ids)` runs
  from the boot path and seeds a `super` row in `admin_roles` for
  every id in `ADMIN_USER_IDS` that doesn't already have one.
  Idempotent (a re-boot bumps `skipped_existing` instead of
  rewriting), defensive (never **downgrades** an existing role,
  never **escalates non-env users**, never blocks boot on a
  transient DB error). With this, the DB is the source of truth
  for the admin surface — operators reading `/admin/roles` see
  every legacy env-list admin too, and the audit trail can
  attribute role state changes that started outside the panel.
  Non-positive entries (a typo like `ADMIN_USER_IDS=123,-456` or
  a chat-id paste) are dropped at parse time so the auto-promote
  never seeds an unmatchable row.
  **Web-panel role gates + "view as <role>" toggle (Stage-15-
  Step-E #5 follow-up #4)** — the web admin panel now mirrors
  the Telegram-side per-handler floors via a new
  `_require_role(required)` decorator that wraps `_require_auth`.
  Per-route floors match the Telegram CLI: viewer-readable
  list/detail/dashboard pages stay on `_require_auth`; broadcast
  enqueue/cancel and gift / promo create+revoke require
  `operator`; user-wallet adjust, user-field edit, transaction
  refund, openrouter-keys add/toggle/delete, admin-roles
  grant/revoke, AI-model + gateway toggles, and every
  destructive `/admin/control/*` action require `super`.
  Below-floor requests 302 to `/admin/` with a flash banner
  ("That action requires super role — you are previewing as
  viewer …") and a `view_as_deny` audit row capturing
  path + method + required + view-as for forensic review. The
  layout sidebar carries a "view as <role>" `<select>`
  (`POST /admin/view-as`) that signs an HMAC-SHA256 cookie
  carrying the previewed role and reloads the page; selecting
  `super` deletes the cookie outright. Domain separation —
  view-as cookies use the HMAC prefix `viewas:` while auth
  cookies use no prefix — so a forged auth-cookie HMAC can't
  replay as a view-as override and vice-versa. The middleware
  fail-soft-degrades a malformed / tampered / unknown-role
  cookie back to `super` rather than crashing, so a stale
  cookie left in the browser after a session-secret rotation
  doesn't lock the operator out. Per-user web auth (replacing
  the single `ADMIN_PASSWORD` with telegram-id-keyed
  credentials) is the multi-week redesign called out in the
  Step-E table — out of scope for this follow-up; the toggle
  is the interim story for verifying gates without
  provisioning a second password.
- **Telethon-driven live-bot integration test suite** —
  `tests/integration/` ships a Telethon-based suite that drives a
  *live* test bot via a real Telegram user account (MTProto, not
  the Bot API — bots can't DM other bots). The suite skips itself
  cleanly when any of `TG_API_ID` / `TG_API_HASH` /
  `TG_TEST_SESSION_STRING` / `TG_TEST_BOT_USERNAME` is unset, so
  CI's `pytest -v` just emits `SKIPPED [reason]` lines and stays
  green. To run locally, set the four secrets (the throwaway
  session-string generation script lives in
  `tests/integration/conftest.py`) and point
  `TG_TEST_BOT_USERNAME` at a *dedicated test bot, not the
  production bot* — a flaky test must not be able to credit /
  refund / broadcast to real users. Coverage:
    - **Smoke tests** (`tests/integration/test_smoke.py`) — the
      `/start` greeting + hub keyboard, `/balance` rendering the
      wallet line, and the bot's resilience to unknown commands.
    - **FSM coverage** (`tests/integration/test_fsm_flows.py`) —
      `/redeem` two-step (enter FSM → bad code → reject + clean
      exit), hub-keyboard geometry pin, wallet-button
      callback-query → wallet card with `$` balance line, and
      `/redeem` → `/start` mid-FSM clears state (regression
      against pre-PR-110's `cmd_start` consuming slash commands
      as raw FSM input). The new `click_button_and_wait` helper
      taps an inline-keyboard button and waits for the bot's
      reply (handles both the "edit the same message in place"
      callback-query path and the "post a new message" path).
  Stage-15-Step-E #6 first slice + follow-up #1. **Optional CI
  workflow (Stage-15-Step-E #6 follow-up #2)** —
  `.github/workflows/integration.yml` runs the suite on a manual
  `workflow_dispatch` trigger. Operator stores the four secrets
  in repo Settings → Secrets and variables → Actions, then
  Actions tab → "Integration tests (live Telegram)" → "Run
  workflow". Manual-only because (a) PR forks can't read the
  session-string secret without leaking it, and (b) every run
  sends real Telegram messages and debits the test wallet.
  Pinned by 9 stdlib-only sanity tests in
  `tests/test_workflows.py` so a future edit can't silently
  remove the `workflow_dispatch` gate, drop a secret env binding,
  or remove the 15-minute job timeout. **Manual smoke recipe**
  (when you don't want to set up the CI workflow): from a local
  shell with the four env vars exported, run
  `pytest tests/integration/ -v` against the test bot. The suite
  finishes in <5 minutes and prints `PASSED` / `FAILED` per case
  with full Telegram round-trip output. To regenerate
  `TG_TEST_SESSION_STRING`, paste the docstring snippet from
  `tests/integration/conftest.py` into a python REPL with the
  api_id + api_hash on hand.
- **gettext `.po` round-trip + runtime lookup for community
  translations** — `i18n_po.py` exports `strings._STRINGS` to
  `locale/<lang>/LC_MESSAGES/messages.po` files (one per
  supported locale, two today: `fa`, `en`). Translators can
  open the `.po` files directly in Poedit / Crowdin / OmegaT
  and submit a PR with the diffed translation instead of
  hand-editing the 1146-line Python literal in `strings.py`.
  **`i18n_runtime.py` (Stage-15-Step-E #7 follow-up #1)** loads
  every `messages.po` into an in-memory catalog at boot and
  `strings.t()` consults it *between* the admin-override cache
  (still highest priority) and the compiled-default `_STRINGS`
  table — so a translator can drop an edited `messages.po`
  into the locale directory and the bot picks up the new
  strings on the next process restart **without a code
  deploy**. Empty `msgstr` (gettext convention for
  "untranslated") is treated as a miss so the lookup falls
  through to the compiled default. Errors are isolated
  per-locale: a malformed or missing `.po` file logs an
  exception but doesn't crash the bot — the affected locale
  just falls through to the compiled default. Workflow: edit
  `strings.py`, then `python -m i18n_po export` to regenerate
  the `.po` files, then commit both. CI gate
  `python -m i18n_po check` (also exercised by
  `tests/test_i18n_po.py`) fails the build if the on-disk
  `.po` files drift from the dict, so adding a slug without
  re-exporting is impossible to merge. msgid is the slug
  (Persian-as-msgid is awkward and length-explodes); the
  source-locale text appears as a `#.` translator comment for
  context. Stage-15-Step-E #7 first slice + follow-up #1.
  **Importer (Stage-15-Step-E #7 follow-up #2)** —
  `python -m i18n_po import <lang> <path>` bulk-loads a
  translator's `.po` into the runtime `bot_strings` table. Every
  `msgstr` is validated against `strings.validate_override`
  before being written; rows that fail (unknown slug, bad
  placeholder, malformed format syntax) are reported and skipped
  while the rest are upserted. `--dry-run` validates without
  writing — use it to preview a translator's PR before applying.
  `--updated-by NAME` tags `bot_strings.updated_by` with a
  translator name or PR number for traceability (defaults to
  `i18n_po-import`). The CLI prints a five-bucket summary
  (`upserted` / `unchanged` / `skipped_empty` /
  `skipped_unknown_slug` / `invalid` / `errors`) and exits
  non-zero if any entry hit the `invalid` or `errors` buckets,
  so CI / cron-driven imports can fail fast on bad input.
  Closes the .po round-trip: a community translator submits
  `messages.po`, the operator runs the import, and overrides go
  live without a code deploy. **Translator walkthrough
  (Stage-15-Step-E #7 follow-up #3):** see the dedicated
  ["Translating Meowassist"](#translating-meowassist-translator-workflow)
  section below for the step-by-step Poedit / Crowdin recipe,
  the code-deploy vs. hot-update paths, and the new orphan-locale
  drift gate that flags stale `.po` files for locales that have
  been removed from `strings.SUPPORTED_LANGUAGES`.
- **Per-key 429 cooldown for OpenRouter** — when OpenRouter
  returns 429 for one of the configured pool keys (the upstream
  provider rate-limited it, or the key hit its OpenRouter plan
  ceiling), `openrouter_keys` puts that key in a short cooldown
  (default 60s, honours `Retry-After`, clamped to 1h max).
  Subsequent users routed there fall through to the next
  available pool member instead of seeing "rate-limited" every
  time their sticky key is the one under pressure. When **all**
  keys are in cooldown the picker returns the sticky pick
  anyway with a warning, so the user gets at least one attempt
  rather than a hard "no service" error. **Stage-15-Step-E #4
  follow-up:** the `/admin/openrouter-keys` ops view renders one
  row per pool slot with cooldown status, remaining seconds, the
  per-process 429 count, and the per-process fallback count
  (how many times this slot absorbed a fallback after another
  slot's sticky key went hot). The matching Prometheus families
  off `/metrics` are
  `meowassist_openrouter_key_429_total{index="N"}`,
  `meowassist_openrouter_key_fallback_total{index="N"}`, and
  `meowassist_openrouter_key_cooldown_remaining_seconds{index="N"}`
  — keyed by 0-based pool index, never by api_key, so a leaked
  scrape doesn't carry the keys themselves. Counters reset on
  every deliberate `load_keys()` reload so a key rotation
  doesn't carry stale per-index meaning forward. **Stage-15-
  Step-E #4 follow-up #2:** the same `/admin/openrouter-keys`
  page is now an editable surface — operators can add new keys
  (label + plaintext stored in the `openrouter_api_keys` DB
  table), disable / re-enable keys without restarting the bot,
  and hard-delete rows. Each row shows a 4-char tail
  (`…3a4b`) instead of the plaintext key so the operator can
  identify each entry without leaking it into browser history.
  Each pool slot also surfaces a per-process **request count**
  (sticky picks + fallback picks combined) so the panel answers
  "is this key actually being used?" without needing access to
  OpenRouter's dashboard. All mutations are CSRF-protected and
  audit-logged via `record_admin_audit`. **Stage-15-Step-E #4
  follow-up #3:** every successful AI completion now also bumps
  a per-key **24h rolling buffer** (`record_key_usage`); the
  panel renders two new "24h reqs" / "24h cost" columns that
  answer "how much traffic — and how much $ — is this key
  handling right now?" over a 24-hour window that survives
  process restarts of the cumulative counters. The same hook
  bumps `last_used_at` on the DB-backed registry row (a real
  bug fix — pre-PR that column was only updated by tests, so
  the panel's "Last used" column always rendered `—` even for
  actively-used DB keys). Cross-replica cooldown coordination
  (Redis-backed) remains a follow-up.
  **Stage-15-Step-E #4 follow-up #4 — per-(key, model) cooldown:**
  OpenRouter typically 429s a specific `:free` model whose
  upstream provider is throttling, not the API key as a whole.
  The first slice cooled the *whole key* on every 429, which
  over-blocked: a user routed to that key paying for a
  paid model got an unhelpful "rate limited" reply because
  someone else had hit a free-tier limit on a different model.
  A second cooldown table keyed by `(api_key, model_id)` lives
  alongside the whole-key table. `mark_key_rate_limited(key,
  model="<slug>")` writes to the per-(key, model) table; the
  picker (`key_for_user(uid, model="<slug>")`) walks past slots
  blocked for that model while keeping slots blocked for *other*
  models on the same key. `ai_engine.chat_with_model` passes
  `model=active_model` through automatically. New Prometheus
  family `meowassist_openrouter_key_model_cooldown_remaining_seconds{
  index="N",model="<slug>"}` (only emits a row per *active*
  cooldown — no sentinel zeros for the whole key × model cross
  product). **Bundled bug fix** in this slice: the inline
  `float(retry_after)` previously only handled the delta-seconds
  form of `Retry-After`; per RFC 7231 §7.1.3 the header can also
  be an HTTP-date, and many CDNs (Cloudflare, Akamai,
  CloudFront — all of which can sit in front of OpenRouter's
  edge) emit the date form. The first slice silently fell back
  to the default 60s on the date form, throwing away a real
  upstream signal in both directions. New `_parse_retry_after`
  helper handles both forms (RFC 1123 / RFC 850 / asctime via
  `email.utils.parsedate_to_datetime`), rejects past dates / NaN
  / Inf / negative values so the caller still falls back cleanly
  to the default when the header is unusable.
  **Stage-15-Step-E #4 follow-up #5 — one-shot retry on 429:**
  pre-feature, a 429 from OpenRouter on the user's sticky key
  bounced back as `ai_rate_limited` even when a non-cooled
  alternate key in the pool would have served the request — the
  user had to retry by hand. Now: after the first 429 +
  `(key, model)` cooldown mark, `ai_engine.chat_with_model`
  asks the picker for the next key — if it's a different key
  from the first attempt, it retries the POST exactly **once**
  against the alternate. The retry is a single attempt (NOT a
  loop) so a pool-wide outage can't cascade into N retries × N
  cooldowns; latency cost is bounded to one extra round-trip on
  the 429 path. Implementation extracts a
  `_post_chat_completion(api_key, payload, timeout)` private
  helper inside `ai_engine.py` so the retry doesn't duplicate
  the 50+ lines of POST + response-parsing + error-logging the
  original inline block carried; the helper takes the api_key
  explicitly so the retry sends the alternate key in the
  `Authorization` header. Outcome tracking lives in a new
  pool-wide aggregate counter `_ONE_SHOT_RETRY_COUNTERS` in
  `openrouter_keys.py` (deliberately *not* per-key — the retry
  is a user-session-level event). Six outcome labels
  (`_ONE_SHOT_RETRY_OUTCOMES`): `attempted` (every retry — the
  denominator), `succeeded` (retry returned a 200 and the user
  got an AI reply), `second_429` (retry also rate-limited;
  surfaces "is the rate-limit pool-wide or per-key?"),
  `second_other_status` (retry returned a non-200 non-429 — 5xx,
  401, etc; user sees `ai_provider_unavailable`),
  `transport_error` (retry POST raised `aiohttp.ClientError` /
  `TimeoutError`; outer `except` surfaces `ai_transient_error`),
  `no_alternate_key` (single-key pool or all alternates already
  cooled — no retry attempted, the user gets the existing
  rate-limit reply). New Prometheus counter family
  `meowassist_openrouter_oneshot_retry_total{outcome="…"}` —
  operators alert on
  `rate(...{outcome="second_429"}[5m])
  / rate(...{outcome="attempted"}[5m]) > 0.5` ("the pool is hot
  enough that retries don't help — add another key"). The
  HELP/TYPE preamble renders even with zero outcomes recorded so
  a fresh deploy's PromQL query doesn't return a "metric does
  not exist" no-data state. After a successful retry,
  `record_key_usage` is bumped against the *alternate* key (not
  the original cooled key) so the 24h-spend dashboard credits
  the right key. Reset hooks (`reset_key_counters_for_tests`,
  `load_keys`) wipe the new counter alongside the existing
  per-key ones so test isolation and operator key rotations both
  stay clean.
- **Bot health & emergency control panel** — new `/admin/control`
  page surfaces a traffic-light status tile (idle / healthy /
  busy / degraded / under-attack / down) classified by
  `bot_health.compute_bot_status` from in-flight chat slots, IPN
  drop totals, login-throttle activity, background loop
  heartbeats and DB reachability. The same page exposes
  master kill-switches that disable every AI model or every
  payment gateway in one click (CSRF-protected, audit-logged),
  plus a **force-stop** button that sends `SIGTERM` to the bot
  process so a wedged or under-attack deploy can be cycled in
  one click — the operator's process supervisor (systemd /
  docker / pm2) restarts the bot. Tunable thresholds via
  `BOT_HEALTH_BUSY_INFLIGHT`,
  `BOT_HEALTH_LOOP_STALE_SECONDS`,
  `BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD`,
  `BOT_HEALTH_LOGIN_THROTTLE_ATTACK_KEYS`. The Prometheus
  exposition adds a `meowassist_bot_status_score` gauge (0=idle,
  5=down) so existing alerting rules can target
  `meowassist_bot_status_score >= 4` to page on under-attack /
  down. First slice of Stage-15-Step-F.
- **Tunable severity thresholds via panel (no restart)** —
  `/admin/control` now ships a "Severity thresholds" card that
  edits the four `BOT_HEALTH_*` knobs at runtime. Values are
  written to the `system_settings` table, beat env / default for
  every component (panel, `/metrics`, alert loop), propagate
  without a process restart, and audit-log a per-submission diff.
  Blank field clears the override and falls back to env / default.
  A bundled bug fix refuses `=0` values that would have
  permanently tripped the corresponding alarm (e.g.
  `BOT_HEALTH_IPN_DROP_ATTACK_THRESHOLD=0` flagged UNDER_ATTACK
  on every dashboard hit). Stage-15-Step-F follow-up #4.
- **Alert-loop audit trail** — every bot-health alert DM
  (`bot_health_alert.py`) and recovery DM now appends one row to
  `admin_audit_log`, alongside the human-admin actions on
  `/admin/control`. Filter `/admin/audit?actor=bot_health_alert`
  for the loop-driven feed. The `meta` jsonb column captures the
  entered level, the underlying classifier signals (so the audit
  row is self-contained), the recovered-from level (recovery
  only), and the per-DM delivery counts so a partial-fan-out
  incident ("0 of 2 admins reached") is visible. The
  `no_admins_reachable` and `no_admins_configured` outcomes
  surface alerts that fired but didn't reach anyone — silent
  failures the audit log was designed to catch. Bundled bug
  fix: the five control-panel action slugs shipped in
  Stage-15-Step-F first slice (force-stop, kill-switches) were
  being recorded but were missing from the `/admin/audit` filter
  dropdown — fixed in this PR. Stage-15-Step-F follow-up #3.
- **Per-loop freshness thresholds for bot-health** — the
  `bot_health.compute_bot_status` classifier now derives each
  background loop's stale threshold from a per-loop cadence map
  (`bot_health.LOOP_CADENCES`) rather than a single shared
  `BOT_HEALTH_LOOP_STALE_SECONDS=1800` knob. Long-cadence loops
  like `model_discovery` (6h) and `catalog_refresh` (24h) no
  longer trip DEGRADED on a healthy bot; short-cadence loops like
  `bot_health_alert` (60s) now correctly trip stale at 3 min
  rather than after a 30 min outage. Operators can pin a per-loop
  override via `BOT_HEALTH_LOOP_STALE_<UPPER_NAME>_SECONDS`.
  Bundled bug fix: a freshly-booted bot used to show DEGRADED for
  any loop that hadn't ticked yet — including `catalog_refresh`
  which only fires once per 24h, so a fresh deploy was DEGRADED
  for its first 24h. The classifier now grace-periods a never-
  ticked loop until `uptime > stale_threshold`. Stage-15-Step-F
  follow-up #2.
- **Cadence introspection on `/admin/control`** — the panel's
  "Background loop heartbeats" section now shows each loop's
  published cadence + per-loop stale threshold + colour-coded
  status (`fresh` / `warming up` / `overdue` / `no tick`)
  alongside the live last-tick age. Operators no longer have to
  memorise each loop's expected interval to answer "is this loop
  overdue?" — the panel reads
  `bot_health.loop_cadence_seconds(name)` and
  `bot_health.loop_stale_threshold_seconds(name)` so the panel
  and the classifier agree by construction. Bundled bug fix:
  `zarinpal_backfill` (5 min cadence) was in the heartbeat metric
  registry but missing from `LOOP_CADENCES`, so it was inheriting
  the legacy 30 min stale threshold — six missed ticks before the
  panel hinted at a problem. Now registered at its true 5 min
  cadence with a 660 s threshold; pinned by a regression test
  that asserts every name in `metrics._LOOP_METRIC_NAMES` has a
  matching `LOOP_CADENCES` entry. Stage-15-Step-F follow-up #4.
- **Cadence registration via `@register_loop` decorator** — the
  hand-maintained `bot_health.LOOP_CADENCES` dict and the
  hand-maintained `metrics._LOOP_METRIC_NAMES` tuple are now
  populated by a single decorator at each loop's definition site,
  e.g. `@register_loop("fx_refresh", cadence_seconds=600)` on
  `refresh_usd_to_toman_loop`. The two registries can no longer
  drift — adding a new loop touches one place, not two. Mismatch
  protection: re-registering the same name with a *different*
  cadence raises `RuntimeError`. Bundled bug fix:
  `openrouter_keys._read_env_keys` did not mirror `load_keys`'s
  "numbered slots win, bare ignored" semantics — with both
  `OPENROUTER_API_KEY` *and* `OPENROUTER_API_KEY_1..N` set, the
  helper returned `[BARE, *numbered]` while `load_keys`
  produced just `[*numbered]`, breaking the no-op fast path in
  `refresh_from_db` and (in the rebuild branch) duplicating the
  last numbered slot into the in-process pool. Now matches
  `load_keys` exactly, pinned by 5 new tests. Stage-15-Step-F
  follow-up #5.
- **Per-loop manual "tick now" button on `/admin/control`** —
  every background loop registers an async runner alongside its
  cadence (`@register_loop("fx_refresh", cadence_seconds=600,
  runner=_tick_fx_refresh_from_app)`), and the panel's heartbeat
  table grew an "Action" column with a per-row "Tick now" button
  that POSTs to `/admin/control/loop/<name>/tick-now`. The handler
  CSRF-guards, audit-logs `control_loop_tick_now` *before*
  invoking, and runs the runner under
  `asyncio.wait_for(_, timeout=60s)` so a wedged outbound
  connection can't tie up the request worker. Operators verifying
  a freshly-deployed loop no longer wait up to 24 h
  (`catalog_refresh`) or 6 h (`model_discovery`) before the panel
  proves the loop actually works. Heartbeat metrics update through
  the runner's normal `record_loop_tick(name)` path — the panel
  reads exactly as if the loop had naturally fired. Bundled bug
  fix: the panel rendered "(overdue by Ns)" the moment a loop's
  age passed its cadence, but the classifier's actual overdue
  threshold is ≈ 2× cadence + 60 s — so in the grace window the
  next-tick text said "overdue" while the status badge said
  "fresh", confusing during incident triage. Now classifies that
  grace window as `is_running_late` (mutually exclusive with
  `is_overdue`, more severe wins) and the template renders three
  distinct sub-text strings: `(overdue by ~Ns)`,
  `(running late ~Ns)`, `(next in ~Ns)`. Pinned by 3 new tests
  covering each grace-window state. Stage-15-Step-F follow-up #6.
- **Proactive bot-health Telegram DMs** — new `bot_health_alert.py`
  background loop wakes every `BOT_HEALTH_ALERT_INTERVAL_SECONDS`
  (default 60), runs the same `bot_health.compute_bot_status`
  classifier the panel uses, and DMs admins on transitions to
  DEGRADED / UNDER_ATTACK / DOWN — and on recovery back to
  HEALTHY. Per-level dedupe with an hour anchor avoids same-state
  spam while still re-firing immediately on level escalation
  (DEGRADED → DOWN). Per-admin fault isolation (a blocked admin
  doesn't stop notifications to the others). Bundled bug fix:
  `compute_bot_status` previously used the *since-boot* IPN drop
  total to detect UNDER_ATTACK, which would silently false-fire
  on long-running deploys that slowly accumulated bad-signature
  rows. The classifier now reads a rate-windowed
  `ipn_drops_recent` count maintained by the alert loop, so the
  panel + the gauge + the loop classify identically and
  long-uptime deploys no longer self-trip. Stage-15-Step-F
  follow-up #1.
- **Tunable bot-health alert cadence** — the
  `BOT_HEALTH_ALERT_INTERVAL_SECONDS` knob is now editable from
  `/admin/control` instead of being env-only. The alert loop
  re-reads its resolved cadence every iteration so a saved
  override takes effect on the next tick (no restart). Override
  range is bounded to `[1, 86_400]` seconds — the 24h cap on the
  override slot prevents a fat-finger like `86400000` (intended
  `60`) from silently disabling alerting for a month. Bundled
  bug fix: the panel's per-loop "stale threshold" calculation
  now follows runtime cadence updates via a new
  `bot_health.update_loop_cadence()` helper, so retuning the
  cadence at runtime no longer leaves the panel forever showing
  the alert loop as "running late". Stage-15-Step-E #10b row 21.
- **Tunable pending-PENDING expiration window** — the
  `PENDING_EXPIRATION_HOURS` knob (default 24h) is now editable
  from `/admin/control` instead of being env-only. The reaper loop
  re-reads its resolved threshold every iteration so a saved
  override takes effect on the next tick (no restart). Override
  range is bounded to `[1, 8_760]` hours — the 1-year cap on the
  override slot prevents a fat-finger like `876000` (intended
  `168`) from silently disabling the reaper for the rest of the
  deploy lifetime. Bundled bug fixes: (1) `_record_expiration_audit`
  now logs `threshold_hours_used` in `meta` so investigators can
  later reconcile EXPIRED rows against the operator's
  `control_expiration_hours_update` audit trail (pre-fix the audit
  row carried no threshold metadata, so "did we expire a paid
  invoice because the window was set too aggressively?" was
  unanswerable weeks after the fact); (2) the manual "Tick now"
  button on `/admin/control` now routes through
  `pending_expiration.get_pending_expiration_hours()` instead of
  reading the env var directly, so it respects saved DB overrides
  and agrees with the loop's iteration-time behaviour.
  Stage-15-Step-E #10b row 9.
- **Tunable stuck-PENDING alert threshold** — the
  `PENDING_ALERT_THRESHOLD_HOURS` knob (default 2h) is now editable
  from `/admin/control` instead of being env-only. The pending-alert
  loop re-reads its resolved threshold every iteration so a saved
  override takes effect on the next tick (no restart). The dashboard
  tile, the panel, and the alert DM body all pull from the same
  resolver, so they cannot disagree about "what counts as overdue".
  Override range is bounded to `[1, 8_760]` hours; an explicit `bool`
  rejection in the coercer prevents a stored `"true"` row from
  coercing to `1` and shrinking the threshold to "anything PENDING
  for an hour is suspicious", paging admins constantly. Bundled
  defensive measure: `_alert_loop` wraps the iteration-time re-read
  in a `try/except` that falls back to the previous threshold (logged
  at ERROR) rather than letting a transient resolver blip propagate
  up and starve the loop. Stage-15-Step-E #10b row 10.
- **Tunable per-loop stale thresholds** — the
  `BOT_HEALTH_LOOP_STALE_<NAME>_SECONDS` knobs (default
  `2 × cadence + 60s`) are now editable per-loop from
  `/admin/control` instead of being env-only. A new "⏱ Per-loop
  stale thresholds" card renders one row per registered loop with
  effective / source / cadence / cadence-derived / DB / env columns
  and inline Save+Clear forms. Saved overrides take effect on the
  next panel render and next classifier read (no restart) — DB
  beats env so a saved override cannot be silently shadowed by a
  stale env left behind from a previous deploy. Useful when a
  slow-syncing gateway is legitimately late and falsely tripping
  DEGRADED on the panel for `zarinpal_backfill`, or when the
  `2 × cadence + 60s` default isn't right for a long-cadence job.
  Override range bounded to `[1, 604_800]` seconds (1 week); an
  explicit `bool` rejection in the coercer prevents a stored
  `"true"` row from shrinking every loop's freshness window to 1s
  and painting the whole panel red; the POST handler validates
  `loop_name` against `metrics._LOOP_METRIC_NAMES` so a typo can't
  write a row no real loop reads. Bundled bug fix:
  `refresh_threshold_overrides_from_db` previously raised
  `AttributeError` on a non-string-non-None row in `system_settings`
  (e.g. an int from a future schema change), poisoning the whole
  load and reverting every other override; the refresh now coerces
  via `_coerce_setting_to_str` so a single garbage row only drops
  itself. Stage-15-Step-E #10b row 11.
- **Markup history & per-era revenue attribution on
  `/admin/monetization`** — the page used to apply *today's*
  `COST_MARKUP` uniformly to every historical `usage_logs` row when
  computing implied OpenRouter cost, which lied about lifetime
  margin if you'd ever changed the markup. Two new cards now
  surface the missing breakdown. **"Markup change history"** lists
  the most recent `monetization_markup_update` audit rows with
  timestamp / actor / kind / before / after / IP, decoded from
  `admin_audit_log.meta`. **"Markup eras — revenue attribution"**
  splits charged-USD into eras at each markup change and divides
  each era by *its own* markup, so changing `1.5×` → `2.0×` and
  back tells you honestly which week was more profitable. Both
  cards fail-soft to empty placeholder text on a DB blip — the
  headline summary still renders. Bundled bug fix: a new
  `_finite_float_or_none` helper rejects `bool` (so a `True`
  meta value can't sneak through as `1.0` and corrupt the markup
  column) and treats `NaN` / `±Inf` as `None` rather than letting
  them propagate into the per-era SQL where they'd render as
  `nan×`; `get_markup_eras` clamps a tampered `markup=0` audit row
  to `openrouter_cost_usd=0` rather than dividing by zero.
  Stage-15-Step-E #10b row 12.

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

## Translating Meowassist (translator workflow)

Meowassist ships two locales today (`fa` Persian as the source-of-truth, `en`
English as a fallback). Translators don't need to touch Python — every
user-facing string lives in standard gettext `.po` files at
`locale/<lang>/LC_MESSAGES/messages.po`, editable in
[Poedit](https://poedit.net/) or any
[Crowdin](https://crowdin.com/) project (or a plain text editor at a pinch).
Stage-15-Step-E #7 follow-up #3 — the round-trip foundation, runtime lookup,
and DB importer are already in place; this section is the step-by-step
recipe a translator follows.

### What the runtime does with your `.po`

```
                   admin override (DB row)
edit messages.po ─►─►─►─►─►─►─►─►─►──────────────────────►─► strings.t()
       │           │   2. compiled-default fallback
       │           └── messages.po (in-memory catalog)
       │
       ▼
   PR / commit
```

Two paths to ship a translation, both supported:

1. **Code-deploy path** (recommended for new languages and bulk edits). You
   submit a PR with the edited `messages.po`, it merges, the next bot
   release loads the new strings into the in-memory catalog at boot via
   `i18n_runtime.init_translations` and `strings.t()` consults it
   automatically — no DB write, no operator step.
2. **Hot-update path** (for surgical fixes that can't wait for a release).
   The operator runs `python -m i18n_po import <lang> <path-to-your.po>` to
   bulk-load every validated `msgstr` into the `bot_strings` DB table, which
   wins over the compiled defaults *and* over the on-disk `.po` (admin
   overrides are highest priority). Translators don't run this themselves;
   you ask the operator to apply your `.po` once it's been reviewed.

### Step-by-step in Poedit

1. **Install Poedit.** Download from <https://poedit.net/download> (free,
   cross-platform). The Pro edition isn't required for `.po` editing.
2. **Get the file.** Either fork+clone the repo and open
   `locale/fa/LC_MESSAGES/messages.po` (or `en` for English), or download
   just the file from GitHub's "Raw" link if you only want to look. Editing
   in Poedit and submitting a PR is the supported flow.
3. **Open in Poedit.** `File → Open` and pick the `.po` file. Poedit
   reads the gettext header and the per-entry `#.` translator comments
   (which show the source-locale rendering for context) and gives you a
   two-column view: the slug on the left (the `msgid`), your translation
   on the right (the `msgstr`).
4. **Translate.** Click an empty entry, type the translation, hit `Tab` to
   advance. Save with `Ctrl/Cmd-S`. Poedit preserves the entry order, the
   header, and the `#.` comments so a `git diff` against the original
   shows only your `msgstr` edits. Don't change `msgid`s — those are the
   programmatic slugs the runtime keys off.
5. **Mind the placeholders.** `msgstr "Your balance is {balance:.2f}"` —
   the `{balance:.2f}` part is a Python format placeholder. You can
   reorder it (e.g. `"{balance:.2f} موجودی"`) but you can't drop it or
   rename it; the runtime feeds the same kwargs to your translation as
   to the compiled default. Removing a placeholder renders without it
   (fine), introducing a new one breaks. The validator described under
   "Hot-update path" rejects mismatched placeholders before they reach
   users.
6. **Submit a PR.** `git checkout -b translation/<your-name>`,
   `git commit -am "i18n(<lang>): batch from <date>"`, push, open a PR.
   CI runs `python -m i18n_po check` which fails on any drift between the
   `.po` files and `strings._STRINGS` — if you've only edited `msgstr`s,
   it passes. CI also fails on a stale `.po` file for a locale that was
   removed from `strings.SUPPORTED_LANGUAGES` (Stage-15-Step-E #7
   follow-up #3), so PRs that remove a locale must remove the directory
   in the same change.
7. **A maintainer reviews and merges.** Once merged, the next deploy
   ships your translation through the code-deploy path. If you need it
   live before the next release, ping the operator and reference your
   PR — they'll run the importer below.

### Step-by-step in Crowdin

1. **Get added to the Crowdin project.** Crowdin doesn't auto-create the
   project; a maintainer sets up an organisation and shares an invite
   link. There's no public Crowdin URL today — Stage-15-Step-E #7 #4
   tracks integrating one with auto-PR-on-merge.
2. **Upload the source `.po`.** A maintainer uses Crowdin's "upload to
   source" flow on each language's `messages.po`. The `.po` format is
   first-class in Crowdin so headers, comments, and placeholders all
   round-trip cleanly.
3. **Translate in the web UI.** Crowdin's editor surfaces the
   per-entry `#.` translator-comment so you see the source-locale
   rendering next to the slug.
4. **Crowdin exports back to the repo.** When the maintainer pulls the
   translated `.po` from Crowdin, they overwrite the on-disk file and
   open a PR. From here it's identical to step 6+ of the Poedit flow.

### Hot-update path (operator only)

The on-disk-`.po` flow ships at the next deploy. For a faster turnaround
the operator can bulk-load your `.po` into the `bot_strings` DB table:

```bash
# Preview what would change (validates every msgstr without writing).
python -m i18n_po import fa /path/to/translated.po --dry-run

# Apply for real, tagging the audit column with the PR number.
python -m i18n_po import fa /path/to/translated.po \
  --updated-by "crowdin-pr-241"
```

The CLI prints a five-bucket summary (`upserted` / `unchanged` /
`skipped_empty` / `skipped_unknown_slug` / `invalid` / `errors`) and exits
non-zero on any `invalid` or `errors`. `invalid` collects msgstrs that
fail `strings.validate_override` (unknown placeholder, malformed format
syntax, etc.) — the operator forwards the list to the translator with a
"please retry these in your `.po`". `errors` collects DB / I/O problems
that aren't the translator's fault.

Once the import succeeds, the `bot_strings` rows take precedence over
both the in-memory `.po` catalog and the compiled defaults; the
translation is live on the next request.

### Adding a new language

1. Add the locale code (e.g. `'tr'`) to `strings.SUPPORTED_LANGUAGES` in
   `strings.py`.
2. Add a translation column to the `_STRINGS` dict for every existing
   slug (Persian source-of-truth → your new locale).
3. Run `python -m i18n_po export` to generate
   `locale/tr/LC_MESSAGES/messages.po`.
4. Run `python -m i18n_po check` to confirm the export round-trips.
5. Run `pytest tests/test_i18n_po.py tests/test_i18n_runtime.py` to
   confirm the parametrised round-trip + drift tests still pass under
   the new locale.
6. Submit a PR.

A new locale is a code change, not a translation change — translators
don't need to do this themselves. Once the locale is wired up, the
`.po`-only flow above carries every subsequent edit.

### Common pitfalls

* **Don't edit `msgid`.** It's the slug; the runtime keys off it. Edit
  only `msgstr`.
* **Don't reformat the file in your editor.** Poedit preserves layout.
  Other editors may "fix" line wrapping or reorder entries, which makes
  the PR diff unreviewable. Use Poedit if you can.
* **Empty `msgstr` means "untranslated".** The runtime treats an empty
  translation as a miss and falls back to the compiled default. That's
  the right behaviour for a partial translation in progress; commit
  what you have.
* **NUL bytes (`\x00`) get stripped.** Some Crowdin export pipelines
  emit them inside multi-line `msgstr`s. The DB importer strips them
  before insertion (Postgres `TEXT` rejects NUL). You'll see a `WARN`
  in the logs but no row is dropped.
* **Format-spec kwargs count too.** `{amount:.{precision}f}` requires
  both `amount` and `precision` to exist as call-site kwargs. Don't
  introduce a placeholder in the format spec that wasn't in the
  compiled default; the validator catches this and rejects the entry.

## Source map

| File | Purpose |
| --- | --- |
| `main.py` | Entrypoint. Boots aiogram dispatcher, registers middleware, calls `bot_commands.publish_bot_commands` to overwrite BotFather's slash-command list, starts the IPN HTTP listener. |
| `bot_commands.py` | Canonical Telegram slash-command publisher. `PUBLIC_COMMANDS` (everyone sees) + `ADMIN_COMMANDS` (per-admin via `BotCommandScopeChat`). Idempotent; errors are logged and swallowed so a transient network blip during startup doesn't take the bot down. |
| `database.py` | asyncpg pool + every SQL query. Money methods use `SELECT … FOR UPDATE` inside connection-scoped transactions. `deduct_balance` refuses NaN / ±Infinity *and* finite-negative `cost_usd` so the canonical wallet-debit path can't be flipped into a silent un-audited credit (defense-in-depth — `pricing._apply_markup` already clamps to `[0, ∞)` upstream). `redeem_gift_code` re-checks the row's `amount_usd` is finite *inside* the open transaction so a corrupted `gift_codes` row (legacy NaN, manual SQL fix, etc.) rolls back cleanly instead of bricking the redeemer's wallet via `balance_usd + NaN`. `log_usage` mirrors the same finite + non-negative guard on `cost` so a NaN / ±Infinity / negative value can't poison `usage_logs.cost_deducted_usd` and break every dashboard aggregate (`spend_usd`, `top_models`, per-user totals). `get_system_metrics` returns `pending_payments_count` + `pending_payments_oldest_age_hours` for the Stage-9-Step-9 dashboard tile. **Stage-12-Step-A:** the terminal-state surface is split — `mark_transaction_terminal` accepts only `EXPIRED` / `FAILED` (the canonical `TERMINAL_FAILURE_STATUSES` set), the gateway-side IPN refund routes through `mark_payment_refunded_via_ipn` (no wallet write), and the admin-issued `refund_transaction(transaction_id, reason, admin_telegram_id)` is the only path that flips a SUCCESS row to `REFUNDED` *and* debits the wallet. Two-row `FOR UPDATE` lock (transactions then users), refuses non-SUCCESS rows / `admin` + `gift` gateways / would-go-negative balances, and writes the new `refunded_at` + `refund_reason` columns from alembic 0012. **Stage-15-Step-E #10 integration slice:** `append_conversation_message` now strips U+0000 NUL bytes before the INSERT (Postgres TEXT rejects `\x00`; every other Unicode code point is accepted, so a targeted `.replace("\x00", "")` preserves the user's content with maximum fidelity). Telegram clients allow U+0000 (paste from a binary file, certain Android emoji-keyboard bugs); pre-fix a NUL-bearing prompt or reply would fail the INSERT and lose the memory turn — even though PR #129 wraps the upstream call in a defensive try/except so the AI reply isn't lost, the memory buffer was still developing gaps. Loud-and-once WARN log when the strip fires. **Stage-15-Step-E #10 follow-up #2:** `append_conversation_message` accepts a keyword-only `image_data_uris: list[str] \| None = None`; non-empty lists are JSON-encoded and INSERTed via `$N::jsonb` into the new nullable `conversation_messages.image_data_uris` JSONB column added by alembic `0018_image_data_uris`. `get_recent_messages` reads the column and reconstructs the OpenAI/OpenRouter multimodal user-message shape (`content` is a list of `{type:"text"}` / `{type:"image_url"}` parts) for non-null vision rows; text-only rows return the legacy plain-string content shape so the prompt-assembly path is unchanged. New `_decode_jsonb_str_list` sibling helper to `_decode_jsonb_meta` — fail-soft on malformed JSONB so a single poisoned row doesn't blank the whole memory buffer. |
| `payments.py` | NowPayments invoice creation, IPN verification (HMAC-SHA512), idempotent finalize, partial-payment crediting. IPN replay-dedupe via `payment_status_transitions` (`UNIQUE(gateway_invoice_id, payment_status)`) — duplicate `(invoice, status)` deliveries drop with a 200 before any state mutation. Per-process drop counters (`bad_signature`, `bad_json`, `missing_payment_id`, `replay`) exposed via `get_ipn_drop_counters()` for ops dashboards. **Per-currency minimum-payment enforcement**: `GLOBAL_MIN_TOPUP_USD` (default $2, env-overridable via `MIN_TOPUP_USD`) + cached `/v1/min-amount` lookup (`get_min_amount_usd`) + background refresher (`refresh_min_amounts_loop`, default every 15 min) + `find_cheaper_alternative` so the checkout flow refuses sub-minimum amounts with "min for BTC is $10, pay $3 with USDT-TRC20 instead" instead of a generic rejection. |
| `tetrapay.py` | TetraPay (Iranian Shaparak / Rial card) gateway. Stage-11-Step-C. `create_order` POSTs to `https://tetra98.com/api/create_order` and returns a `TetraPayOrder` with the redirect `payment_url`, `Authority` (used as our `gateway_invoice_id`), `tracking_id`, and the integer `amount_irr` (rial). `verify_payment` is the *authoritative* settlement check — the user-side webhook callback is never trusted alone (a forged callback would otherwise credit a wallet without payment). `tetrapay_webhook` is mounted at `/tetrapay-webhook`: parse JSON → dedupe via `payment_status_transitions` → drop on non-"100" status → look up the locked USD figure on the PENDING row → call `verify_payment` → call `Database.finalize_payment(authority, locked_usd)`. The credit amount is the USD equivalent **locked at order creation**, never recomputed at settlement: Iranian banks regularly take minutes for Shaparak 3DS round-trips and the rial can move materially during that window. Drop counters (`bad_json`, `missing_authority`, `non_success_callback`, `unknown_invoice`, `verify_failed`, `replay`) mirror the NowPayments path's `_IPN_DROP_COUNTERS` for ops visibility. |
| `zarinpal.py` | Zarinpal (Iranian Shaparak / Rial card) gateway, alternative to TetraPay. Stage-15-Step-E #8. `create_order` POSTs to `https://payment.zarinpal.com/pg/v4/payment/request.json` with `{merchant_id, amount, currency: "IRR", description, callback_url, metadata}` and returns a `ZarinpalOrder` with the StartPay `payment_url`, `authority` (used as our `gateway_invoice_id`), and the integer `amount_irr`. Unlike TetraPay (POST webhook), **Zarinpal redirects the user's browser** back to `${WEBHOOK_BASE_URL}/zarinpal-callback?Authority=…&Status=OK\|NOK` — `zarinpal_callback` is mounted as a GET route. `verify_payment(authority, amount_irr)` POSTs to `/v4/payment/verify.json` with the locked rial figure read from our ledger (defense against a tampered redirect: a malicious user can't override the amount via a query param) and treats both `code=100` (settled now) and `code=101` (already verified) as success. The credit amount is the same locked-USD invariant as TetraPay — locked at order creation, never recomputed at settlement. Drop counters (`missing_authority`, `non_success_callback`, `unknown_invoice`, `verify_failed`, `replay`) mirror TetraPay's. Telegram FSM integration (the "💳 پرداخت با زرین‌پال" button next to the existing TetraPay button on the Toman-entry currency picker) shipped in Stage-15-Step-E #8 follow-up #1: `handlers._start_zarinpal_invoice` mirrors `_start_tetrapay_invoice` 1:1 — reads the locked `toman_rate_at_entry` from FSM, calls `zarinpal.create_order`, persists a PENDING transaction keyed on the `Authority`, and renders an inline keyboard with the gateway-issued StartPay URL. |
| `payments.py` | NowPayments invoice creation, IPN verification (HMAC-SHA512), idempotent finalize, partial-payment crediting. |
| `memory_config.py` | DB-backed override layer for `MEMORY_CONTEXT_LIMIT` and `MEMORY_CONTENT_MAX_CHARS`. Same pattern as `free_trial.py`: module-level caches, coercion validators, set/clear/get/refresh-from-DB helpers, public lookup with resolution order (override → env → default), source reporting (`db`/`env`/`default`). Web editor at `/admin/memory-config`. |
| `audit_retention.py` | DB-backed override layer for `AUDIT_RETENTION_DAYS` + background reaper loop. Same pattern as `free_trial.py`: module-level cache, coercion validator, set/clear/get/refresh-from-DB helpers, resolution order (override → env → default 90 days). The reaper loop wakes every `AUDIT_RETENTION_INTERVAL_HOURS` (default 24) and batch-deletes `admin_audit_log` rows older than the retention window. Per-process counters: `ticks`, `total_deleted`, `last_run_epoch`. Editor on `/admin/audit`. |
| `pending_expiration.py` | Background reaper task. Wakes every `PENDING_EXPIRATION_INTERVAL_MIN` (default 15) minutes, calls `Database.expire_stale_pending` to flip stuck `PENDING` rows older than `PENDING_EXPIRATION_HOURS` (default 24) to `EXPIRED`, drops a `payment_expired` audit row (`actor="reaper"`), and pings the affected user. `TelegramForbiddenError` / `TelegramBadRequest` are swallowed. Spawned by `main.main` after the webhook server, cancelled cleanly on shutdown. |
| `zarinpal_backfill.py` | **Stage-15-Step-E #8 follow-up #2.** Background backfill reaper for Zarinpal browser-close races. Zarinpal's `?Authority=…&Status=OK` callback is a USER-AGENT redirect (not a server-to-server webhook); if the user closes the browser before the redirect lands, the gateway has the order settled but our ledger never gets the success signal. The reaper wakes every `ZARINPAL_BACKFILL_INTERVAL_MIN` (default 5) minutes, fetches PENDING Zarinpal rows in the window `(min_age, max_age)` (default 5min — 23h), calls `zarinpal.verify_payment` (the same authoritative gateway check the redirect callback would have made), then `Database.finalize_payment` (idempotent — FOR UPDATE + status check guards against double-credit if the user reopens their tab while the reaper is mid-tick), sends the same `zarinpal_credit_notification` DM, and writes a `zarinpal_backfill_credited` audit row marked `actor="zarinpal_backfill"` so forensics can distinguish backfill credits from callback credits. Per-process counters: `rows_examined`, `credited`, `verify_failed`, `transport_error`, `finalize_noop`, `audit_failed`. Heartbeat exposed as `meowassist_zarinpal_backfill_last_run_epoch`. Jurisdictional split with the expire reaper: backfill owns `(min_age, max_age * 3600)`; expire owns everything older — keep `ZARINPAL_BACKFILL_MAX_AGE_HOURS < PENDING_EXPIRATION_HOURS` to avoid races. TetraPay doesn't need this because its callback is a server-to-server POST that retries on 5xx. |
| `model_discovery_config.py` | DB-backed override layer for `DISCOVERY_INTERVAL_SECONDS`. Same pattern as `free_trial.py`: module-level cache, coercion validator, set/clear/get/refresh-from-DB helpers, resolution order (override → env → default 21 600 s). Editor on `/admin/models-config`. |
| `pending_alert.py` | **Stage-12-Step-B.** Background loop that *notifies* about stuck `PENDING` rows long before the reaper *closes* them. Wakes every `PENDING_ALERT_INTERVAL_MIN` (default 30) minutes, calls `Database.list_pending_payments_over_threshold(threshold_hours=PENDING_ALERT_THRESHOLD_HOURS)` (default 2 h), and DMs every `ADMIN_USER_IDS` with a "⚠️ N pending payment(s) stuck over Xh" summary (max 10 lines, overflow footer). Per-row dedupe keyed on `(transaction_id, floor(age_hours))` so the same stuck row alerts once per crossed integer-hour boundary. Per-admin fault isolation mirrors `model_discovery.notify_admins_of_price_deltas`: `TelegramForbiddenError` per admin is logged INFO; `TelegramAPIError` is logged + swallowed; the loop never crashes. Spawned by `main.main`, cancelled cleanly on shutdown. The `get_pending_alert_threshold_hours()` helper is also imported by `web_admin.dashboard` so the "Pending payments" tile shows the same overdue count the alert DM uses. |
| `handlers.py` | All aiogram handlers — `/start`, hub UI, charge flow, model picker, language picker, support. The two `edit_text` no-op silencers (`_edit_to_hub` for the universal "🏠 Back to menu" button, `_render_memory_screen` for the memory-toggle screen) wrap their calls in `except TelegramBadRequest:` only, so unrelated `TelegramForbiddenError` (bot blocked), `TelegramRetryAfter`, or aiohttp network blips propagate to logs / the dispatcher's error handler instead of being silenced as a single `log.debug` line. **Stage-15-Step-E #10 integration:** new `process_photo` handler at `@router.message(F.photo)` mirrors `process_chat`'s shape (rate-limit → in-flight slot → typing → reply chunking) and wires the `vision` module end-to-end: `_download_photo_to_bytes` picks the largest `PhotoSize`, downloads via `bot.download_file` to a `BytesIO`, returns None on download failure (broadened to `except Exception` in Step-E #10 follow-up #1 — covers `asyncio.TimeoutError` from aiogram's request-timeout, `aiohttp.ClientConnectionError` / `ClientPayloadError` from the streaming download, `ConnectionResetError` from a peer reset, plus the original `TelegramAPIError` set) so a flaky CDN never bubbles a stack trace out to the poller; the handler then encodes via `vision.encode_image_data_uri(..., "image/jpeg")` (Telegram serves photos as JPEG; non-JPEG arrives as `document`) and calls `chat_with_model(user_id, caption_or_empty, image_data_uris=[uri])`. Pre-flight vision-capability check fires *before* the Telegram CDN download to skip the round-trip when the active model is text-only. **Stage-15-Step-E #10 follow-up #2 bundled bug fix:** the pre-flight now routes through `ai_engine._resolve_active_model` so a NULL / blank / whitespace-only `users.active_model` row resolves to the same fallback `chat_with_model` would use (`openai/gpt-3.5-turbo` — text-only) and the rejection lands at the gate rather than after a wasted CDN download + base64 encode. `VisionError` from the encoder is mapped to localised slugs (`ai_image_oversize` / `ai_image_unsupported_format` / `ai_image_too_many` / `ai_image_download_failed`) with no wallet impact. **Stage-15-Step-E #10 follow-up #1:** sibling `process_image_document` handler at `@router.message(F.document)` intercepts image-as-document uploads (HEIC / HEIF / PNG / WEBP / TIFF / SVG / AVIF / BMP — anything `mime_type.startswith("image/")`) and replies with the localised `ai_image_document_instruction` slug ("send as Photo, not File"). iPhone's default photo format is HEIC and Telegram's "Send as File" attach mode bypasses photo compression; both arrive as `message.document` (not `message.photo`) so `process_photo` never sees them — pre-fix the bot silently ignored the upload. Non-image documents (PDFs, archives, audio) pass through silently so a future doc handler can be added without colliding. The mime filter fires *before* the `consume_chat_token` gate so a user sending a PDF doesn't have their chat-token budget penalised; the rate-limit gate's exhausted branch drops silently rather than send a "rate-limited" reply on top of an already-throttled session. Server-side HEIC conversion (Pillow + `pillow-heif`) deliberately not taken: doubles the install footprint, adds a CPU-bound memory-heavy operation per upload, and Telegram's "Photo" attach mode already converts client-side to JPEG for free. |
| `ai_engine.py` | OpenRouter call, cost calc, balance deduct, optional conversation memory. Defense-in-depth: a non-finite `users.balance_usd` (NaN / +Infinity from a legacy poisoned row) is treated as $0 for the insufficient-funds gate so a corrupted wallet can't silently bypass the gate and grant unlimited free chat at the bot's expense. **Stage-15-Step-E #10:** the post-settlement `append_conversation_message` calls (memory-enabled users only) are wrapped in a local try/except so a persistence failure (Postgres NUL-byte rejection, transient DB hiccup, FK violation if the user row was concurrently deleted) doesn't bubble out to the outer broad except and lose the AI reply *after* the wallet had already been debited — the wallet debit stands, the reply is delivered, and the persistence failure is logged loud-and-once at ERROR level so ops can spot the row corruption. **Stage-15-Step-E #10 integration slice:** `chat_with_model` now accepts a keyword-only `image_data_uris: list[str] | None = None`; when non-empty AND `vision.is_vision_capable_model(active_model)` the payload assembles the multimodal user-message dict via `vision.build_multimodal_user_message`; non-empty AND text-only model returns `t(lang, "ai_model_no_vision")` *before* any wallet debit or OpenRouter call; the keyword-only signature preserves backward-compat for the 19+ positional-arg call sites. |
| `vision.py` | **Stage-15-Step-E #10.** Pure helpers for the image / vision feature: `is_vision_capable_model(model_id)` (case-insensitive substring match against a known-vision pattern set covering OpenAI gpt-4-vision/4-turbo/4o/o1, Anthropic Claude 3 family + 3.5 + 3.7, Google Gemini 1.5+ + Gemini 2 + gemini-pro-vision + gemini-flash, Meta Llama 3.2 vision, Mistral Pixtral, Qwen-VL family, plus a "vision" wildcard escape hatch for future slugs); `encode_image_data_uri(image_bytes, content_type)` (returns `data:image/jpeg;base64,...`, validates non-empty + ≤ `MAX_IMAGE_BYTES` (default 5 MiB, env `VISION_MAX_IMAGE_BYTES`) + mime in `{jpeg,png,gif,webp}`); `build_multimodal_user_message(prompt, image_data_uris)` (returns the OpenAI/OpenRouter chat-completions multimodal user-message dict, with text-first ordering by convention, ≤ `MAX_IMAGES_PER_MESSAGE` (default 4, env `VISION_MAX_IMAGES_PER_MESSAGE`)). `VisionError` carries a machine-readable `reason` slug. No third-party deps. Wired into `handlers.process_photo` (download → encode → chat_with_model) and `ai_engine.chat_with_model` (vision-capability gate, multimodal payload assembly) by the integration slice. |
| `pricing.py` | Per-model price table + `COST_MARKUP` env var (default 1.5×). **Stage-15-Step-E #10b row 2:** module-level `_MARKUP_OVERRIDE` cache populated from `system_settings.COST_MARKUP` via `refresh_markup_override_from_db(db)`. Resolution order is in-process override → `COST_MARKUP` env → 1.5× compile-time default. The `monetization_markup_post` handler on `/admin/monetization` writes the override row, refreshes the cache, and records a `monetization_markup_update` audit row whose `meta` carries the before/after diff so an analyst can correlate revenue moves with pricing changes. Validators (`set_markup_override`, `_coerce_markup`) refuse non-finite, below-`MARKUP_MINIMUM=1.0`, at-or-above-`MARKUP_OVERRIDE_MAXIMUM=100.0`, and `bool` values defensively so a malformed DB row or fat-finger POST can't poison every paid request. `get_markup_source()` reports `db` / `env` / `default` for the panel's source badge. |
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
| **Stage-15-E** | Future project suggestions (12 items) | **#1 MERGED** (conversation history export, first slice) · **#2 MERGED** (per-user spending dashboard, first slice) · **#3 MERGED** (opt-in webhook mode) · **#4 STARTED** (per-key 429 cooldown) |

## License / contributing

Internal project — see HANDOFF.md for the priority queue before opening a PR.
