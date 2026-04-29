import logging
import math
import os

import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

log = logging.getLogger("bot.database")


def _is_finite_amount(value) -> bool:
    """Defense-in-depth: return ``True`` iff *value* is a finite real
    number (not ``NaN``, ``+Infinity``, or ``-Infinity``).

    Why this exists at the DB layer in addition to the upstream form
    parsers: NaN and Infinity slipping into a money-handling SQL path
    is *silent* — PostgreSQL accepts ``'NaN'::numeric`` (it's a valid
    IEEE-754 value) and INSERTs it without error, but every subsequent
    comparison on the wallet column (``balance_usd >= $1``,
    ``balance_usd < 0``, etc.) becomes a no-op (every comparison
    against ``NaN`` returns ``False`` in SQL just as in Python) which
    effectively bricks the user's wallet without any obvious error in
    logs. PR #75 closed the same hole at the IPN layer; this helper is
    the matching belt-and-suspenders at the DB layer so any future
    caller that bypasses the form parsers (a new internal call site, a
    refactor, a test stub) still can't quietly poison a row.
    """
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Initializes the connection pool to PostgreSQL."""
        self.pool = await asyncpg.create_pool(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            min_size=1,  # Minimum concurrent connections
            max_size=10  # Maximum concurrent connections to prevent RAM overload
        )
        log.info("Database connection pool established")

    async def close(self):
        """Closes the connection pool securely."""
        if self.pool:
            await self.pool.close()

    async def get_user(self, telegram_id: int):
        """Fetches a user from the database."""
        query = "SELECT * FROM users WHERE telegram_id = $1"
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, telegram_id)

    async def create_user(self, telegram_id: int, username: str):
        """Creates a new user with default 5 free messages."""
        query = """
            INSERT INTO users (telegram_id, username) 
            VALUES ($1, $2) 
            ON CONFLICT (telegram_id) DO NOTHING
        """
        async with self.pool.acquire() as connection:
            await connection.execute(query, telegram_id, username)

    async def set_language(self, telegram_id: int, language_code: str) -> bool:
        """Sets the user's preferred language. Returns True iff a row was updated."""
        query = """
            UPDATE users
            SET language_code = $1
            WHERE telegram_id = $2
            RETURNING telegram_id
        """
        async with self.pool.acquire() as connection:
            result = await connection.fetchval(query, language_code, telegram_id)
        return result is not None

    async def get_user_language(self, telegram_id: int) -> str | None:
        """Returns the user's stored language_code, or None if the user doesn't exist."""
        query = "SELECT language_code FROM users WHERE telegram_id = $1"
        async with self.pool.acquire() as connection:
            return await connection.fetchval(query, telegram_id)

    # ------------------------------------------------------------------
    # P3-5 conversation memory.
    #
    # The toggle is a single boolean column on users; the running buffer
    # is conversation_messages (one row per turn, role=user|assistant).
    # ai_engine only reads/writes the buffer when memory_enabled=TRUE.
    # ------------------------------------------------------------------
    # Soft cap on per-message stored content. Telegram caps user messages
    # at 4096 chars but model replies can be much longer (and we feed
    # them back into the next turn). Truncating before insert keeps row
    # sizes bounded; the UI displays the original full reply, only the
    # *retained-as-context* snapshot is trimmed.
    MEMORY_CONTENT_MAX_CHARS = 8000
    # Default number of most-recent messages to feed back as context.
    MEMORY_CONTEXT_LIMIT = 30

    async def get_memory_enabled(self, telegram_id: int) -> bool:
        """Returns True iff the user opted into conversation memory.

        Returns False for unknown users (caller is responsible for the
        upsert-via-middleware contract — if we get None back, we treat
        it as 'no memory' which is the safe default).
        """
        query = "SELECT memory_enabled FROM users WHERE telegram_id = $1"
        async with self.pool.acquire() as connection:
            value = await connection.fetchval(query, telegram_id)
        return bool(value) if value is not None else False

    async def set_memory_enabled(self, telegram_id: int, enabled: bool) -> bool:
        """Flips the memory toggle. Returns True iff a row was updated."""
        query = """
            UPDATE users
            SET memory_enabled = $1
            WHERE telegram_id = $2
            RETURNING telegram_id
        """
        async with self.pool.acquire() as connection:
            result = await connection.fetchval(query, enabled, telegram_id)
        return result is not None

    async def append_conversation_message(
        self, telegram_id: int, role: str, content: str
    ) -> None:
        """Persist one turn (user prompt or assistant reply).

        Caller is responsible for only invoking this when memory is
        enabled — we don't re-check the flag here so the FK violation
        (no users row) is the only thing the DB will reject.
        """
        if role not in ("user", "assistant"):
            raise ValueError(f"invalid role: {role}")
        if len(content) > self.MEMORY_CONTENT_MAX_CHARS:
            content = content[: self.MEMORY_CONTENT_MAX_CHARS]
        query = """
            INSERT INTO conversation_messages (telegram_id, role, content)
            VALUES ($1, $2, $3)
        """
        async with self.pool.acquire() as connection:
            await connection.execute(query, telegram_id, role, content)

    async def get_recent_messages(
        self, telegram_id: int, limit: int | None = None
    ) -> list[dict]:
        """Return the user's last <limit> messages in chronological order.

        Returns an empty list if memory is disabled OR the buffer is
        empty. The list is ready to drop into an OpenAI-style
        ``messages`` array (each element has ``role`` and ``content``).
        """
        cap = limit if limit is not None else self.MEMORY_CONTEXT_LIMIT
        # Pull newest-first from the index-friendly side, then reverse
        # client-side so the oldest message comes first in the array.
        query = """
            SELECT role, content
              FROM conversation_messages
             WHERE telegram_id = $1
             ORDER BY created_at DESC, id DESC
             LIMIT $2
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(query, telegram_id, cap)
        return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

    async def clear_conversation(self, telegram_id: int) -> int:
        """Delete every conversation_message for the user.

        Returns the number of rows deleted (used for the 'cleared N
        messages' confirmation in the UI).
        """
        query = "DELETE FROM conversation_messages WHERE telegram_id = $1"
        async with self.pool.acquire() as connection:
            result = await connection.execute(query, telegram_id)
        # asyncpg returns "DELETE <count>"; parse the integer suffix.
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0

    async def set_active_model(self, telegram_id: int, model_id: str) -> bool:
        """Updates the user's active OpenRouter model id.

        Returns True iff a row was updated. The caller is responsible for
        validating that the model exists in the catalog before calling.
        """
        query = """
            UPDATE users
            SET active_model = $1
            WHERE telegram_id = $2
            RETURNING telegram_id
        """
        async with self.pool.acquire() as connection:
            result = await connection.fetchval(query, model_id, telegram_id)
        return result is not None

    async def decrement_free_message(self, telegram_id: int):
        """Safely deducts 1 free message using an atomic update."""
        query = """
            UPDATE users 
            SET free_messages_left = free_messages_left - 1 
            WHERE telegram_id = $1 AND free_messages_left > 0
            RETURNING free_messages_left
        """
        async with self.pool.acquire() as connection:
            return await connection.fetchval(query, telegram_id)

    async def deduct_balance(self, telegram_id: int, cost_usd: float) -> bool:
        """Atomically deducts USD cost from the wallet.

        Returns True iff the user had enough balance and the row was updated.
        Returns False if the user does not exist or has insufficient funds;
        in that case no balance change is made.

        Defense-in-depth: refuses to attempt the SQL when ``cost_usd`` is
        ``NaN`` / ``±Infinity`` or strictly negative. The pre-fix WHERE
        clause ``balance_usd >= $1`` is already a silent no-op for
        ``NaN`` (every comparison against ``NaN`` is ``False``), but a
        negative-Infinity cost would *match* for any finite balance and
        try to write ``balance_usd - (-inf) = inf`` into the row,
        bricking the wallet the same way PR #75 prevented at the IPN
        layer. A *finite* negative ``cost_usd`` is just as bad — the
        WHERE clause ``balance_usd >= -5`` is True for every solvent
        wallet, and ``SET balance_usd = balance_usd - (-5)`` then
        silently credits $5 without writing a ``transactions`` ledger
        row. Today the only caller (``ai_engine.chat_with_model`` →
        ``pricing._apply_markup``) clamps the cost to ``[0, ∞)`` via
        ``max(raw * markup, 0.0)`` before it gets here, so a sign-
        flipped price (negative ``input_per_1m_usd`` from a
        misconfigured model row) currently rounds to a $0 free reply
        rather than a credit. But that clamp lives one module away,
        and a future caller / refactor / test stub bypassing it would
        re-open the hole — we already paid the cost of having
        ``transactions`` be the canonical ledger, so any wallet
        movement that doesn't go through it is an audit-trail
        regression. Refuse non-finite OR negative ``cost_usd`` here so
        the only way money flows into a user's wallet is through
        ``finalize_payment`` / ``admin_adjust_balance`` / gift / promo
        — every one of which writes a ``transactions`` row in the
        same DB transaction.

        Returning ``False`` mirrors the "insufficient funds" path the
        caller already handles.
        """
        if not _is_finite_amount(cost_usd):
            log.error(
                "deduct_balance refused for telegram_id=%s: non-finite "
                "cost_usd=%r (NaN / Infinity)",
                telegram_id,
                cost_usd,
            )
            return False
        if cost_usd < 0:
            log.error(
                "deduct_balance refused for telegram_id=%s: negative "
                "cost_usd=%r — would silently credit the wallet without "
                "a transactions ledger row.",
                telegram_id,
                cost_usd,
            )
            return False
        query = """
            UPDATE users
            SET balance_usd = balance_usd - $1
            WHERE telegram_id = $2 AND balance_usd >= $1
            RETURNING balance_usd
        """
        async with self.pool.acquire() as connection:
            new_balance = await connection.fetchval(query, cost_usd, telegram_id)
        return new_balance is not None

    async def log_usage(self, telegram_id: int, model: str, prompt_tokens: int, completion_tokens: int, cost: float):
        """Logs the exact token usage for accounting.

        Defense-in-depth: refuses non-finite (``NaN`` / ``±Infinity``)
        or negative ``cost`` and skips the INSERT with a logged error.
        ``usage_logs.cost_deducted_usd`` is ``DECIMAL(10,6) NOT NULL``
        with no CHECK constraint, and PostgreSQL ``NUMERIC`` accepts
        ``'NaN'::numeric`` happily; once stored, every aggregate
        (``SUM(cost_deducted_usd)`` for the dashboard's spend tile,
        per-model totals in ``top_models``, per-user totals in
        ``get_user_usage_aggregates``) propagates the NaN and bricks
        the figure. The only present caller (``chat_with_model``)
        clamps cost via ``pricing._apply_markup``'s
        ``max(raw * markup, 0.0)`` so a sign-flipped per-1M price
        currently rounds to $0 — but the clamp lives one module away
        from the SQL. A future caller (a refactor that drops the
        clamp, a stub ``ModelPrice`` in a test, a new internal
        billing path) bypassing it would silently poison the table.
        Refusing here keeps the only paths that can put a row into
        ``usage_logs`` finite-and-non-negative.

        Skipping vs raising: the call is fire-and-forget from
        ``chat_with_model`` (return value unused) and the user has
        already received their reply by this point. Skipping with a
        log line preserves the user's reply without poisoning the
        table; raising would either crash the handler (bad UX for
        a "should never happen" assertion) or be swallowed silently
        by an outer ``except`` (worse). The error log is the right
        signal for ops.
        """
        if not _is_finite_amount(cost) or cost < 0:
            log.error(
                "log_usage refused for telegram_id=%s model=%r: bad "
                "cost=%r (must be a finite non-negative number). "
                "Row NOT inserted; investigate the caller.",
                telegram_id, model, cost,
            )
            return
        query = """
            INSERT INTO usage_logs (telegram_id, model_used, prompt_tokens, completion_tokens, cost_deducted_usd)
            VALUES ($1, $2, $3, $4, $5)
        """
        async with self.pool.acquire() as connection:
            await connection.execute(query, telegram_id, model, prompt_tokens, completion_tokens, cost)

    async def create_pending_transaction(
        self,
        telegram_id: int,
        gateway: str,
        currency_used: str,
        amount_crypto: float,
        amount_usd: float,
        gateway_invoice_id: str,
        promo_code: str | None = None,
        promo_bonus_usd: float = 0.0,
    ) -> bool:
        """Records a payment as PENDING. Returns True iff a new row was inserted.

        ON CONFLICT on the unique gateway_invoice_id makes this safe to retry;
        a duplicate invoice id will not create a second row.

        ``promo_code`` / ``promo_bonus_usd`` attach an already-validated
        promo redemption to the transaction. The bonus is only credited
        on the SUCCESS transition (see :meth:`finalize_payment`); if the
        invoice ends up EXPIRED / FAILED / REFUNDED, the promo is not
        consumed (its used_count is incremented in the same DB tx as
        the SUCCESS credit, not here).
        """
        query = """
            INSERT INTO transactions (
                telegram_id, gateway, currency_used,
                amount_crypto_or_rial, amount_usd_credited,
                status, gateway_invoice_id,
                promo_code_used, promo_bonus_usd
            )
            VALUES ($1, $2, $3, $4, $5, 'PENDING', $6, $7, $8)
            ON CONFLICT (gateway_invoice_id) DO NOTHING
            RETURNING transaction_id
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                query,
                telegram_id, gateway, currency_used,
                amount_crypto, amount_usd, gateway_invoice_id,
                promo_code, promo_bonus_usd,
            )
        return row is not None

    # Stage-9-Step-5: explicit allow-list for terminal-failure statuses.
    # Lifted out of the function body so callers (and tests) can refer to
    # the canonical set without grepping. SUCCESS is its own ledger
    # status reached via ``finalize_payment``, NOT
    # ``mark_transaction_terminal``.
    TERMINAL_FAILURE_STATUSES: frozenset[str] = frozenset(
        {"EXPIRED", "FAILED", "REFUNDED"}
    )

    async def mark_transaction_terminal(
        self, gateway_invoice_id: str, new_status: str
    ):
        """Atomically close a PENDING or PARTIAL transaction with a terminal
        failure status (EXPIRED / FAILED / REFUNDED).

        The user's balance is **not** modified — even when transitioning from
        PARTIAL. The semantics are:
          - PENDING -> terminal: user paid nothing, nothing to do but log.
          - PARTIAL -> terminal: user paid less than the invoice required and
            we already credited that partial amount when partially_paid first
            arrived. They keep the partial credit; we just close the ledger
            row. (We do NOT debit them — NowPayments did receive the
            underpayment, and reversing would require a counter-payment we
            cannot orchestrate.)

        Returns a row dict (telegram_id, currency_used, amount_usd_credited,
        previous_status) if the close happened, or None if the row was
        unknown or already in a different terminal state. Idempotent against
        retries via the WHERE-status guard.

        Stage-9-Step-5 bug-fix bundle: ``new_status`` MUST be in
        ``TERMINAL_FAILURE_STATUSES`` — passing anything else (including
        the row's own current status, e.g. PENDING -> PENDING) raises
        ``ValueError`` at the API surface. Pre-fix, a same-status call
        would silently bump ``completed_at`` on a row that hadn't actually
        transitioned, polluting forensics queries. The UPDATE's WHERE
        clause now also checks ``status != $2`` as belt-and-suspenders so
        a future caller sneaking past the entry guard still can't bump
        the timestamp on a real no-op.
        """
        if new_status not in self.TERMINAL_FAILURE_STATUSES:
            raise ValueError(
                f"new_status must be one of {sorted(self.TERMINAL_FAILURE_STATUSES)}; "
                f"got {new_status!r}"
            )
        # We need the *previous* status to let the caller choose the right
        # user notification text (a PARTIAL -> terminal close means the user
        # already received some credit). Wrap the read-then-update in one DB
        # transaction with FOR UPDATE so concurrent webhooks serialize.
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT telegram_id, status, currency_used, amount_usd_credited
                    FROM transactions
                    WHERE gateway_invoice_id = $1
                    FOR UPDATE
                    """,
                    gateway_invoice_id,
                )
                if row is None or row["status"] not in ("PENDING", "PARTIAL"):
                    return None
                previous_status = row["status"]
                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = $2, completed_at = CURRENT_TIMESTAMP
                    WHERE gateway_invoice_id = $1
                      AND status != $2
                    """,
                    gateway_invoice_id,
                    new_status,
                )
                return {
                    "telegram_id": row["telegram_id"],
                    "currency_used": row["currency_used"],
                    "amount_usd_credited": row["amount_usd_credited"],
                    "previous_status": previous_status,
                }

    async def expire_stale_pending(
        self,
        *,
        threshold_hours: int = 24,
        limit: int = 1000,
    ) -> list[dict]:
        """Atomically mark stuck PENDING transactions as EXPIRED.

        Used by the background reaper task (see
        ``pending_expiration.start_pending_expiration_task``) to flush
        invoices the user abandoned mid-checkout — without it the
        ledger accumulates dead PENDING rows forever, polluting
        ``/admin/transactions`` and the dashboard "pending payments"
        tile.

        Only PENDING rows older than ``threshold_hours`` are touched;
        PARTIAL rows are left alone because the user actually paid
        something and the IPN may still upgrade them to SUCCESS via
        ``finalize_payment``. NowPayments invoices time out at
        20-30 minutes by default but operators may legitimately leave
        a long-tail open for high-value payments — 24 h is the
        documented default and configurable via
        ``PENDING_EXPIRATION_HOURS``.

        Returns a list of expired rows (telegram_id, currency_used,
        amount_usd_credited, gateway_invoice_id, created_at-ish) so
        the caller can fire user notifications and audit-log entries.
        ``limit`` caps the number of rows returned to avoid an
        unbounded UPDATE on a backlog (the reaper runs every 15 min
        so leftovers get caught the next tick anyway).

        Idempotent: rerunning is a no-op once the backlog is drained.
        Concurrency-safe: the UPDATE … WHERE status='PENDING' guard
        means two reapers running in parallel can't double-process the
        same row (the second one's WHERE returns 0 rows).
        """
        if threshold_hours <= 0:
            raise ValueError("threshold_hours must be positive")
        if limit <= 0:
            raise ValueError("limit must be positive")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                UPDATE transactions
                SET status = 'EXPIRED',
                    completed_at = CURRENT_TIMESTAMP
                WHERE transaction_id IN (
                    SELECT transaction_id
                    FROM transactions
                    WHERE status = 'PENDING'
                      AND created_at < NOW() - ($1 || ' hours')::interval
                    ORDER BY created_at
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING transaction_id,
                          telegram_id,
                          currency_used,
                          amount_usd_credited,
                          gateway_invoice_id,
                          created_at
                """,
                str(int(threshold_hours)),
                int(limit),
            )
        return [
            {
                "transaction_id": int(r["transaction_id"]),
                "telegram_id": r["telegram_id"],
                "currency_used": r["currency_used"],
                "amount_usd_credited": float(r["amount_usd_credited"]),
                "gateway_invoice_id": r["gateway_invoice_id"],
                "created_at": (
                    r["created_at"].isoformat()
                    if r["created_at"] is not None
                    else None
                ),
            }
            for r in rows
        ]

    async def finalize_partial_payment(
        self, gateway_invoice_id: str, actually_paid_usd: float
    ):
        """Atomically finalize / top-up an under-paid (partially_paid) transaction.

        Accepts both PENDING (first partially_paid IPN) and PARTIAL
        (a follow-up partially_paid IPN where the user paid more but
        still less than the full invoice). NowPayments reports the
        cumulative actually_paid in each IPN, so on a follow-up we credit
        only the delta between the new total and what we've already
        credited.

        Either way the row ends up with status='PARTIAL' and
        amount_usd_credited equal to the new cumulative credited total.
        The status flip, the row update and the wallet credit all happen
        in one DB transaction, so a crash anywhere in the middle leaves
        the row recoverable on retry.

        Returns a row dict (telegram_id, currency_used, amount_usd_credited,
        delta_credited) on success, or None if the transaction is unknown
        or already in a non-PENDING/non-PARTIAL terminal state. Idempotent:
        a replayed IPN with the same actually_paid_usd will return the row
        with delta_credited == 0.

        Defense-in-depth: refuses (returns ``None``) when
        ``actually_paid_usd`` is ``NaN`` / ``±Infinity`` / non-positive.
        The IPN handler already filters these out via
        ``payments._finite_positive_float`` (PR #75), but the DB layer
        re-checks so a future internal caller / test stub / refactor that
        bypasses the IPN path can't poison a wallet either. ``max(0.0,
        NaN - x)`` is undefined behaviour in CPython (it depends on
        argument order) and a finite-but-Infinity input would propagate
        straight into the wallet UPDATE.
        """
        if not _is_finite_amount(actually_paid_usd) or actually_paid_usd <= 0:
            log.error(
                "finalize_partial_payment refused for invoice=%s: "
                "non-finite or non-positive actually_paid_usd=%r",
                gateway_invoice_id,
                actually_paid_usd,
            )
            return None
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT telegram_id, status, currency_used, amount_usd_credited
                    FROM transactions
                    WHERE gateway_invoice_id = $1
                    FOR UPDATE
                    """,
                    gateway_invoice_id,
                )
                if row is None or row["status"] not in ("PENDING", "PARTIAL"):
                    return None

                already_credited = (
                    float(row["amount_usd_credited"])
                    if row["status"] == "PARTIAL"
                    else 0.0
                )
                # max(0, ...) so a replay with a smaller actually_paid_usd
                # cannot debit the user.
                delta = max(0.0, actually_paid_usd - already_credited)

                # The new cumulative-credited value is the larger of what
                # we previously credited and what's just landed.
                # `already_credited` is already the right base on both
                # paths (0 for PENDING, the row's stored cumulative for
                # PARTIAL), so this max() correctly:
                #   - PENDING → PARTIAL: stamps the actually_paid_usd we
                #     just credited (NOT the stale "intended" amount the
                #     PENDING row was holding).
                #   - PARTIAL → PARTIAL upgrade: writes the higher number.
                #   - PARTIAL → PARTIAL out-of-order replay: keeps the
                #     stored higher value, so a subsequent `finished` IPN
                #     can correctly compute its remainder.
                # An equivalent SQL expression (CASE on status, GREATEST
                # only on PARTIAL rows) was rejected because the row's
                # PRE-update status is gone the moment the SET runs;
                # computing in Python where we still have the previous
                # status is clearer and equally atomic under FOR UPDATE.
                new_credited = max(already_credited, actually_paid_usd)
                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = 'PARTIAL',
                        amount_usd_credited = $2,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE gateway_invoice_id = $1
                    """,
                    gateway_invoice_id,
                    new_credited,
                )
                if delta > 0:
                    await connection.execute(
                        """
                        UPDATE users
                        SET balance_usd = balance_usd + $1
                        WHERE telegram_id = $2
                        """,
                        delta,
                        row["telegram_id"],
                    )
                return {
                    "telegram_id": row["telegram_id"],
                    "currency_used": row["currency_used"],
                    "amount_usd_credited": new_credited,
                    "delta_credited": delta,
                }

    async def finalize_payment(
        self, gateway_invoice_id: str, full_price_usd: float
    ):
        """Atomically mark a PENDING / PARTIAL transaction SUCCESS *and* credit
        any remaining USD owed to the user's wallet, in one DB transaction.

        Accepts:
          - PENDING: first delivery of a 'finished' IPN. Credits the full
            invoice amount.
          - PARTIAL: follow-up 'finished' IPN that arrives after a
            partially_paid IPN already credited some of the funds. Credits
            only the delta between the full invoice and what was already
            credited, so the user is made whole without double-crediting.

        Returns a row dict (telegram_id, amount_usd_credited, delta_credited)
        on success, or None if the transaction is unknown or already in a
        terminal non-PENDING/non-PARTIAL state. Idempotent against replays
        via the FOR UPDATE lock + status check.

        The status flip and the wallet credit must happen in the same DB
        transaction: otherwise a crash or DB error between them would mark
        the ledger SUCCESS but leave the user uncredited, and webhook
        retries would forever skip crediting (status is no longer PENDING).

        Defense-in-depth: refuses (returns ``None``) when
        ``full_price_usd`` is ``NaN`` / ``±Infinity`` / non-positive.
        Same rationale as ``finalize_partial_payment``: PR #75 closes
        the hole at the IPN layer, this is the matching DB-layer guard.
        """
        if not _is_finite_amount(full_price_usd) or full_price_usd <= 0:
            log.error(
                "finalize_payment refused for invoice=%s: non-finite or "
                "non-positive full_price_usd=%r",
                gateway_invoice_id,
                full_price_usd,
            )
            return None
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT transaction_id, telegram_id, status,
                           amount_usd_credited,
                           promo_code_used, promo_bonus_usd
                    FROM transactions
                    WHERE gateway_invoice_id = $1
                    FOR UPDATE
                    """,
                    gateway_invoice_id,
                )
                if row is None or row["status"] not in ("PENDING", "PARTIAL"):
                    return None

                already_credited = (
                    float(row["amount_usd_credited"])
                    if row["status"] == "PARTIAL"
                    else 0.0
                )
                # max(0, ...) is defense in depth: if a buggy IPN reports
                # full_price_usd below the already-credited amount, we must
                # not debit the user.
                delta = max(0.0, full_price_usd - already_credited)

                # New cumulative-credited value, computed in Python with
                # the pre-update status still in scope. See the parallel
                # comment in finalize_partial_payment for why this is
                # safer than `GREATEST(amount_usd_credited, $2)` in SQL —
                # the PENDING row's amount_usd_credited carries the
                # *intended* amount, not what's been credited, so a
                # blind GREATEST would lock the row at the intended value
                # and starve subsequent finalize calls of any delta.
                new_credited = max(already_credited, full_price_usd)
                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = 'SUCCESS',
                        amount_usd_credited = $2,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE gateway_invoice_id = $1
                    """,
                    gateway_invoice_id,
                    new_credited,
                )

                # Promo bonus is unlocked only on the SUCCESS transition.
                # We record the redemption (promo_usage row +
                # promo_codes.used_count++) in the SAME DB transaction so
                # a crash between credit and bookkeeping leaves nothing
                # half-done. We also re-check max_uses under the FOR
                # UPDATE lock on promo_codes so two parallel finishing
                # invoices can't each take the last seat of a 1-use code.
                promo_code = row["promo_code_used"]
                bonus_usd = float(row["promo_bonus_usd"] or 0.0)
                bonus_credited = 0.0
                if promo_code and bonus_usd > 0:
                    bonus_credited = await self._consume_promo_in_tx(
                        connection,
                        promo_code=promo_code,
                        telegram_id=row["telegram_id"],
                        transaction_id=row["transaction_id"],
                        bonus_usd=bonus_usd,
                    )

                total_credit = delta + bonus_credited
                if total_credit > 0:
                    await connection.execute(
                        """
                        UPDATE users
                        SET balance_usd = balance_usd + $1
                        WHERE telegram_id = $2
                        """,
                        total_credit,
                        row["telegram_id"],
                    )
                return {
                    "telegram_id": row["telegram_id"],
                    "amount_usd_credited": new_credited,
                    "delta_credited": delta,
                    "promo_bonus_credited": bonus_credited,
                }

    # ----------------------------------------------------------------- #
    # Promo codes (P2-5)
    # ----------------------------------------------------------------- #

    async def validate_promo_code(
        self, code: str, telegram_id: int
    ):
        """Look up a promo code and check eligibility for *telegram_id*.

        Returns a dict ``{code, discount_percent, discount_amount}`` if
        the code can be applied, else a string error key (one of
        ``"unknown"``, ``"inactive"``, ``"expired"``, ``"exhausted"``,
        ``"already_used"``) the caller can pass to :func:`strings.t`.

        Codes are uppercased before lookup so the UI promise of
        case-insensitivity holds even for codes inserted by raw SQL
        in mixed case (defense in depth — :meth:`create_promo_code`
        also uppercases on insert).

        This is an *advisory* check at UI time — the authoritative gate
        runs again under FOR UPDATE inside the SUCCESS transaction
        (see :meth:`_consume_promo_in_tx`) so two parallel invoices
        can't both take the last seat of a single-use code.
        """
        code = code.upper()
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT code, discount_percent, discount_amount,
                       max_uses, used_count, expires_at, is_active
                FROM promo_codes
                WHERE code = $1
                """,
                code,
            )
            if row is None:
                return "unknown"
            if not row["is_active"]:
                return "inactive"
            expires_at = row["expires_at"]
            if expires_at is not None:
                # asyncpg returns timezone-aware datetimes for
                # TIMESTAMP WITH TIME ZONE columns; compare with NOW().
                now = await connection.fetchval("SELECT NOW()")
                if expires_at <= now:
                    return "expired"
            if row["max_uses"] is not None and row["used_count"] >= row["max_uses"]:
                return "exhausted"
            already_used = await connection.fetchval(
                """
                SELECT 1 FROM promo_usage
                WHERE promo_code = $1 AND telegram_id = $2
                """,
                code,
                telegram_id,
            )
            if already_used:
                return "already_used"
        return {
            "code": row["code"],
            "discount_percent": row["discount_percent"],
            "discount_amount": float(row["discount_amount"])
            if row["discount_amount"] is not None
            else None,
        }

    @staticmethod
    def compute_promo_bonus(
        amount_usd: float,
        *,
        discount_percent: int | None,
        discount_amount: float | None,
    ) -> float:
        """USD bonus to credit on top of *amount_usd* for the given promo.

        Exactly one of ``discount_percent`` / ``discount_amount`` must be
        non-None (enforced at the DB layer, but defensive here too).
        Bonuses are clamped to *amount_usd* — a $5 fixed-discount code
        on a $2 top-up only credits $2 of bonus, never more than the
        invoice itself.
        """
        if discount_percent is not None:
            bonus = amount_usd * (discount_percent / 100.0)
        elif discount_amount is not None:
            bonus = float(discount_amount)
        else:
            return 0.0
        # Round to 4 decimals (matches DECIMAL(10,4)) and clamp.
        bonus = max(0.0, min(bonus, amount_usd))
        return round(bonus, 4)

    async def _consume_promo_in_tx(
        self,
        connection,
        *,
        promo_code: str,
        telegram_id: int,
        transaction_id: int,
        bonus_usd: float,
    ) -> float:
        """Atomically consume one redemption of *promo_code*.

        Called from inside :meth:`finalize_payment`'s open transaction.
        Locks the promo_codes row, re-validates is_active / expires_at /
        max_uses, then bumps used_count and inserts a promo_usage row.
        On any failure (already used, exhausted, expired, deactivated
        between picker and webhook), returns 0.0 — the SUCCESS still
        commits, just without the bonus. We do **not** raise here:
        a stale promo state is a UX issue, not a payment failure, and
        we don't want webhook retries to compound.

        Codes are uppercased on entry to match :meth:`create_promo_code`
        and :meth:`validate_promo_code`. This shouldn't matter in
        practice because the code on the transactions row was stamped
        from the validate result (already uppercase), but defending
        the boundary is cheaper than chasing a mismatch later.

        Returns the bonus actually credited.
        """
        promo_code = promo_code.upper()
        promo = await connection.fetchrow(
            """
            SELECT max_uses, used_count, expires_at, is_active
            FROM promo_codes
            WHERE code = $1
            FOR UPDATE
            """,
            promo_code,
        )
        if promo is None:
            log.warning(
                "Promo code %r vanished between invoice creation and SUCCESS "
                "(transaction_id=%d, telegram_id=%d). Crediting without bonus.",
                promo_code, transaction_id, telegram_id,
            )
            return 0.0
        if not promo["is_active"]:
            log.warning(
                "Promo code %r deactivated before SUCCESS for txn %d.",
                promo_code, transaction_id,
            )
            return 0.0
        if promo["expires_at"] is not None:
            now = await connection.fetchval("SELECT NOW()")
            if promo["expires_at"] <= now:
                log.warning(
                    "Promo code %r expired before SUCCESS for txn %d.",
                    promo_code, transaction_id,
                )
                return 0.0
        if (
            promo["max_uses"] is not None
            and promo["used_count"] >= promo["max_uses"]
        ):
            log.warning(
                "Promo code %r exhausted before SUCCESS for txn %d.",
                promo_code, transaction_id,
            )
            return 0.0

        # ON CONFLICT DO NOTHING handles webhook replay: a second
        # 'finished' IPN for the same invoice would otherwise try to
        # insert a duplicate (promo_code, telegram_id) and re-credit.
        # The composite PK refuses, so we silently no-op and return 0.
        inserted = await connection.fetchval(
            """
            INSERT INTO promo_usage (
                promo_code, telegram_id, transaction_id, bonus_usd
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (promo_code, telegram_id) DO NOTHING
            RETURNING 1
            """,
            promo_code, telegram_id, transaction_id, bonus_usd,
        )
        if inserted is None:
            log.warning(
                "Promo redemption already recorded for code=%r telegram_id=%d "
                "(replayed IPN for txn %d). Skipping double-credit.",
                promo_code, telegram_id, transaction_id,
            )
            return 0.0
        await connection.execute(
            """
            UPDATE promo_codes
            SET used_count = used_count + 1
            WHERE code = $1
            """,
            promo_code,
        )
        return bonus_usd

    async def create_promo_code(
        self,
        code: str,
        *,
        discount_percent: int | None = None,
        discount_amount: float | None = None,
        max_uses: int | None = None,
        expires_at=None,
    ) -> bool:
        """Insert a new promo code. Returns False on conflict.

        Mainly intended for admin use (P2-6 will add a Telegram-side
        admin command). Validates the percent/amount XOR client-side
        for a friendlier error than the DB CHECK constraint.

        The code is uppercased before insert so the user-facing case-
        insensitive promise (the UI uppercases what users type, see
        :func:`handlers.process_promo_input`) holds for codes created
        through this method. Admins can pass ``"summer20"`` and users
        can type ``"summer20"`` / ``"Summer20"`` / ``"SUMMER20"`` and
        all three resolve to the same row.
        """
        code = code.upper()
        if (discount_percent is None) == (discount_amount is None):
            raise ValueError(
                "Exactly one of discount_percent / discount_amount must be set"
            )
        if discount_percent is not None and not (1 <= discount_percent <= 100):
            raise ValueError("discount_percent must be between 1 and 100")
        if discount_amount is not None:
            # Defense-in-depth: refuse non-finite values BEFORE the
            # range checks below. Both the web admin form
            # (``parse_promo_form``) and the Telegram-side parser
            # (``parse_promo_create_args``) already reject NaN /
            # ±Infinity, but PostgreSQL ``NUMERIC`` happily stores
            # ``'NaN'::numeric`` and the comparison guards we use
            # downstream (``discount_amount <= 0``,
            # ``discount_amount > 999_999.9999``) are silent no-ops
            # against NaN — every comparison returns ``False`` — so
            # a NaN slipping past them would land in the column,
            # then propagate into ``promos.compute_promo_bonus``
            # (which multiplies by NaN) and ultimately into the
            # ``balance_usd + bonus`` SQL on invoice finalize,
            # bricking the wallet exactly the way PR #75 prevented
            # at the IPN layer. ``+Infinity`` is caught by the
            # ``> 999_999.9999`` check below, but a NaN is not — so
            # we filter both up-front using the same
            # ``_is_finite_amount`` helper that already guards
            # ``deduct_balance`` / ``finalize_payment`` /
            # ``finalize_partial_payment`` / ``admin_adjust_balance``.
            if not _is_finite_amount(discount_amount):
                raise ValueError(
                    "discount_amount must be a finite number "
                    "(NaN / ±Infinity rejected)"
                )
            if discount_amount <= 0:
                raise ValueError("discount_amount must be positive")
            # ``discount_amount`` is stored as ``DECIMAL(10,4)`` (alembic
            # 0001) — max representable value is 999_999.9999. Anything
            # bigger would crash the INSERT with PG ``numeric field
            # overflow``. Reject up-front with a friendly message so
            # both the Telegram-side ``/admin_promo_create`` command and
            # the web admin form (``web_admin.parse_promo_form``) get a
            # consistent error rather than a 500.
            if discount_amount > 999_999.9999:
                raise ValueError(
                    "discount_amount must be at most 999999.9999 "
                    "(DECIMAL(10,4) column limit)"
                )
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                """
                INSERT INTO promo_codes (
                    code, discount_percent, discount_amount,
                    max_uses, expires_at
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (code) DO NOTHING
                RETURNING code
                """,
                code, discount_percent, discount_amount,
                max_uses, expires_at,
            )
        return row is not None

    # Defense-in-depth cap for ``only_active_days``. The ``admin.py``
    # / ``web_admin.py`` parsers already reject anything over
    # ``_BROADCAST_ACTIVE_DAYS_MAX`` before they get here, but this
    # second line of defence keeps a bogus call (e.g. a direct REPL
    # invocation during an outage) from formatting an interval string
    # that PostgreSQL refuses.
    BROADCAST_ACTIVE_DAYS_MAX: int = 36_500

    async def iter_broadcast_recipients(
        self, *, only_active_days: int | None = None
    ) -> list[int]:
        """Return telegram_ids the admin can broadcast to.

        ``only_active_days`` filters to users who logged AI usage in
        the last N days (joins on ``usage_logs``). ``None`` returns
        every user. Sorted ascending so the broadcaster paginates
        deterministically and a crash mid-broadcast doesn't skip
        people on retry.

        Returns plain ``list[int]`` rather than a streaming cursor —
        the user table is small (sub-100k expected) and the broadcast
        coroutine throttles its own send rate, so the memory cost is
        trivial and we get a simple "took a snapshot at T0" semantic.

        Raises :class:`ValueError` when ``only_active_days`` is not
        a positive integer or exceeds
        ``BROADCAST_ACTIVE_DAYS_MAX`` — would otherwise overflow
        PG's interval column and surface as an opaque DB error.
        """
        if only_active_days is not None:
            days = int(only_active_days)
            if days <= 0 or days > self.BROADCAST_ACTIVE_DAYS_MAX:
                raise ValueError(
                    f"only_active_days out of range: {only_active_days!r} "
                    f"(must be in [1, {self.BROADCAST_ACTIVE_DAYS_MAX}])"
                )
        async with self.pool.acquire() as connection:
            if only_active_days is None:
                rows = await connection.fetch(
                    "SELECT telegram_id FROM users ORDER BY telegram_id ASC"
                )
            else:
                rows = await connection.fetch(
                    """
                    SELECT DISTINCT u.telegram_id
                    FROM users u
                    JOIN usage_logs l ON l.telegram_id = u.telegram_id
                    WHERE l.created_at >= NOW() - $1::interval
                    ORDER BY u.telegram_id ASC
                    """,
                    f"{int(only_active_days)} days",
                )
        return [int(r["telegram_id"]) for r in rows]

    async def list_promo_codes(self, *, limit: int = 20) -> list[dict]:
        """Return up to ``limit`` most recently created promo codes
        for ``/admin_promo_list``.

        Each row contains the public-facing code metadata + usage
        counters. ``is_active`` is included so the admin can tell
        revoked codes apart from active ones.
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT code, discount_percent, discount_amount,
                       max_uses, used_count, expires_at,
                       is_active, created_at
                FROM promo_codes
                ORDER BY created_at DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [
            {
                "code": r["code"],
                "discount_percent": (
                    int(r["discount_percent"])
                    if r["discount_percent"] is not None else None
                ),
                "discount_amount": (
                    float(r["discount_amount"])
                    if r["discount_amount"] is not None else None
                ),
                "max_uses": (
                    int(r["max_uses"]) if r["max_uses"] is not None else None
                ),
                "used_count": int(r["used_count"]),
                "expires_at": (
                    r["expires_at"].isoformat()
                    if r["expires_at"] is not None else None
                ),
                "is_active": bool(r["is_active"]),
                "created_at": (
                    r["created_at"].isoformat()
                    if r["created_at"] is not None else None
                ),
            }
            for r in rows
        ]

    async def revoke_promo_code(self, code: str) -> bool:
        """Mark a promo code as inactive (soft-delete).

        Returns True iff a row was flipped from active to inactive.
        Returns False if the code doesn't exist OR was already
        inactive — the caller can disambiguate by re-querying. A
        hard DELETE would orphan rows in promo_usage (FK), so we
        soft-delete instead; ``validate_promo_code`` already filters
        on ``is_active = TRUE``.
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                """
                UPDATE promo_codes
                SET is_active = FALSE
                WHERE code = $1 AND is_active = TRUE
                RETURNING code
                """,
                code.upper(),
            )
        return row is not None

    # ---------------------------------------------------------------
    # Gift codes (Stage-8-Part-3)
    # ---------------------------------------------------------------
    #
    # Distinct from promo codes — gift codes credit balance directly
    # (no purchase required). One row per (code, telegram_id) pair so
    # each user can redeem each code at most once. The ``redeem``
    # method is the only money-touching primitive here; it locks the
    # gift_codes row + user row, runs the eligibility checks, inserts
    # the redemption + transaction, and bumps balance — all in one tx.

    GIFT_AMOUNT_MAX = 999_999.9999  # DECIMAL(10,4) cap

    async def create_gift_code(
        self,
        code: str,
        *,
        amount_usd: float,
        max_uses: int | None = None,
        expires_at=None,
    ) -> bool:
        """Insert a new gift code. Returns False on duplicate.

        Validates the discount cap up-front (DECIMAL(10,4) max is
        999_999.9999) so admins get a friendly ValueError instead of
        a PG ``numeric field overflow`` on the INSERT.
        """
        code = code.upper()
        if amount_usd is None:
            raise ValueError("amount_usd must be positive")
        # Defense-in-depth: refuse non-finite values BEFORE the
        # ordering checks below. ``parse_gift_form`` already rejects
        # NaN / ±Infinity, but the DB layer is the only line of
        # defence against a hypothetical caller bypassing it (a
        # future JSON API, a refactor that drops the parser, a test
        # stub). PostgreSQL ``NUMERIC`` happily stores
        # ``'NaN'::numeric`` and ``amount_usd <= 0`` /
        # ``amount_usd > GIFT_AMOUNT_MAX`` are both silent no-ops
        # for NaN (every comparison returns ``False``), so a NaN
        # would land in ``gift_codes.amount_usd`` and brick the
        # next redeemer's wallet via ``balance_usd + NaN`` in
        # ``redeem_gift_code``. ``+Infinity`` is caught by the
        # ``> GIFT_AMOUNT_MAX`` check below, but NaN is not.
        if not _is_finite_amount(amount_usd):
            raise ValueError(
                "amount_usd must be a finite number "
                "(NaN / ±Infinity rejected)"
            )
        if amount_usd <= 0:
            raise ValueError("amount_usd must be positive")
        if amount_usd > self.GIFT_AMOUNT_MAX:
            raise ValueError(
                f"amount_usd must be at most {self.GIFT_AMOUNT_MAX} "
                "(DECIMAL(10,4) column limit)"
            )
        if max_uses is not None and max_uses <= 0:
            raise ValueError("max_uses must be positive (or None for unlimited)")
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                """
                INSERT INTO gift_codes (
                    code, amount_usd, max_uses, expires_at
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (code) DO NOTHING
                RETURNING code
                """,
                code, amount_usd, max_uses, expires_at,
            )
        return row is not None

    async def list_gift_codes(self, *, limit: int = 100) -> list[dict]:
        """Return up to ``limit`` most recently created gift codes."""
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT code, amount_usd, max_uses, used_count,
                       expires_at, is_active, created_at
                FROM gift_codes
                ORDER BY created_at DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [
            {
                "code": r["code"],
                "amount_usd": float(r["amount_usd"]),
                "max_uses": (
                    int(r["max_uses"]) if r["max_uses"] is not None else None
                ),
                "used_count": int(r["used_count"]),
                "expires_at": (
                    r["expires_at"].isoformat()
                    if r["expires_at"] is not None else None
                ),
                "is_active": bool(r["is_active"]),
                "created_at": (
                    r["created_at"].isoformat()
                    if r["created_at"] is not None else None
                ),
            }
            for r in rows
        ]

    async def revoke_gift_code(self, code: str) -> bool:
        """Soft-delete a gift code. Returns True iff flipped active→inactive."""
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                """
                UPDATE gift_codes
                SET is_active = FALSE
                WHERE code = $1 AND is_active = TRUE
                RETURNING code
                """,
                code.upper(),
            )
        return row is not None

    async def get_gift_redemptions(
        self, code: str, *, limit: int = 100
    ) -> list[dict]:
        """Return who redeemed *code*, newest first."""
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT r.telegram_id, r.redeemed_at, r.transaction_id,
                       u.username
                FROM gift_redemptions r
                LEFT JOIN users u ON u.telegram_id = r.telegram_id
                WHERE r.code = $1
                ORDER BY r.redeemed_at DESC
                LIMIT $2
                """,
                code.upper(), int(limit),
            )
        return [
            {
                "telegram_id": int(r["telegram_id"]),
                "redeemed_at": (
                    r["redeemed_at"].isoformat()
                    if r["redeemed_at"] is not None else None
                ),
                "transaction_id": (
                    int(r["transaction_id"])
                    if r["transaction_id"] is not None else None
                ),
                "username": r["username"],
            }
            for r in rows
        ]

    async def redeem_gift_code(
        self, code: str, telegram_id: int
    ) -> dict:
        """Atomically redeem *code* for *telegram_id*.

        Returns a dict shaped::

            {
              "status": "ok" | "not_found" | "inactive" | "expired"
                        | "exhausted" | "already_redeemed" | "user_unknown",
              "amount_usd": float | None,   # only on "ok"
              "new_balance_usd": float | None,
              "transaction_id": int | None,
            }

        All eligibility checks happen *inside* the transaction with
        ``SELECT ... FOR UPDATE`` so concurrent redemptions race
        deterministically — the (n+1)th caller of a max_uses=n code
        sees ``"exhausted"``, not a quietly-overflowed counter.
        """
        code = code.upper()
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT amount_usd, max_uses, used_count,
                           expires_at, is_active
                    FROM gift_codes
                    WHERE code = $1
                    FOR UPDATE
                    """,
                    code,
                )
                if row is None:
                    return {
                        "status": "not_found", "amount_usd": None,
                        "new_balance_usd": None, "transaction_id": None,
                    }
                if not bool(row["is_active"]):
                    return {
                        "status": "inactive", "amount_usd": None,
                        "new_balance_usd": None, "transaction_id": None,
                    }
                if row["expires_at"] is not None:
                    expired = await connection.fetchval(
                        "SELECT $1 < NOW()", row["expires_at"]
                    )
                    if expired:
                        return {
                            "status": "expired", "amount_usd": None,
                            "new_balance_usd": None, "transaction_id": None,
                        }
                if (
                    row["max_uses"] is not None
                    and int(row["used_count"]) >= int(row["max_uses"])
                ):
                    return {
                        "status": "exhausted", "amount_usd": None,
                        "new_balance_usd": None, "transaction_id": None,
                    }

                # Per-user uniqueness — duplicate ⇒ already_redeemed.
                already = await connection.fetchval(
                    """
                    SELECT 1 FROM gift_redemptions
                    WHERE code = $1 AND telegram_id = $2
                    """,
                    code, telegram_id,
                )
                if already:
                    return {
                        "status": "already_redeemed", "amount_usd": None,
                        "new_balance_usd": None, "transaction_id": None,
                    }

                # User row must exist. We don't auto-create — the
                # UserUpsertMiddleware fires on every Telegram update,
                # so by the time /redeem reaches here the row should
                # already be there. If it isn't, signal it cleanly
                # rather than crashing on the FK insert below.
                user_exists = await connection.fetchval(
                    "SELECT 1 FROM users WHERE telegram_id = $1 FOR UPDATE",
                    telegram_id,
                )
                if not user_exists:
                    return {
                        "status": "user_unknown", "amount_usd": None,
                        "new_balance_usd": None, "transaction_id": None,
                    }

                amount = float(row["amount_usd"])
                # Defense-in-depth: refuse to credit a non-finite
                # amount even if a corrupted ``gift_codes`` row
                # somehow holds NaN / ±Infinity. ``create_gift_code``
                # rejects non-finite ``amount_usd`` at write time
                # (PR #86), but a row predating that guard, a manual
                # SQL fix, a future migration mishap, or any other
                # path that bypasses the create method could still
                # leave a ``'NaN'::numeric`` in the column — and
                # PostgreSQL stores it without complaint. Crediting
                # it here would run ``balance_usd + NaN`` in the
                # ``UPDATE users`` below and brick the wallet exactly
                # the way PR #75 / #77 prevented at the IPN layer.
                # We raise inside the open transaction so it rolls
                # back cleanly (no transactions ledger row, no
                # gift_redemptions row, no counter bump); the caller
                # in ``handlers.py`` already wraps this call in
                # ``try / except`` and returns the generic
                # ``redeem_error`` string, which is the right UX for
                # a "should never happen" DB-corruption case.
                if not _is_finite_amount(amount):
                    log.error(
                        "redeem_gift_code refused for code=%r telegram_id=%d: "
                        "non-finite amount_usd in gift_codes row (%r). "
                        "Rolling back; investigate row corruption.",
                        code, telegram_id, row["amount_usd"],
                    )
                    raise ValueError(
                        "gift_codes.amount_usd must be a finite number "
                        "(NaN / ±Infinity rejected)"
                    )

                # Bump the gift_codes counter first so a concurrent
                # transaction blocked on FOR UPDATE will see the new
                # value when it re-reads.
                await connection.execute(
                    """
                    UPDATE gift_codes
                    SET used_count = used_count + 1
                    WHERE code = $1
                    """,
                    code,
                )

                # Audit ledger row first so we have the id to FK from
                # the redemption record. status=SUCCESS is correct:
                # the user actually got the credit.
                tx_id = await connection.fetchval(
                    """
                    INSERT INTO transactions (
                        telegram_id, gateway, currency_used,
                        amount_crypto_or_rial, amount_usd_credited,
                        status, gateway_invoice_id, completed_at, notes
                    ) VALUES (
                        $1, 'gift', 'USD',
                        NULL, $2,
                        'SUCCESS', $3, NOW(), $4
                    )
                    RETURNING transaction_id
                    """,
                    telegram_id, amount,
                    f"gift:{code}:{telegram_id}",
                    f"redeemed gift code '{code}'",
                )

                # Insert redemption record (FK to transactions).
                await connection.execute(
                    """
                    INSERT INTO gift_redemptions (
                        code, telegram_id, transaction_id
                    ) VALUES ($1, $2, $3)
                    """,
                    code, telegram_id, tx_id,
                )

                # Credit balance.
                new_balance = await connection.fetchval(
                    """
                    UPDATE users
                    SET balance_usd = balance_usd + $1
                    WHERE telegram_id = $2
                    RETURNING balance_usd
                    """,
                    amount, telegram_id,
                )

                return {
                    "status": "ok",
                    "amount_usd": amount,
                    "new_balance_usd": float(new_balance),
                    "transaction_id": int(tx_id),
                }

    async def admin_adjust_balance(
        self,
        telegram_id: int,
        delta_usd: float,
        reason: str,
        admin_telegram_id: int,
    ) -> dict | None:
        """Admin-issued credit (positive ``delta_usd``) or debit
        (negative ``delta_usd``) of a user's wallet.

        Wraps the wallet update + ``transactions`` ledger row in one
        DB transaction with ``SELECT ... FOR UPDATE`` so concurrent
        ``deduct_balance`` calls can't sneak in between the read and
        the write. A debit that would take the balance below zero is
        refused (returns ``None``).

        Returns ``None`` if the user does not exist OR if a debit
        exceeds the available balance. Returns a dict shaped
        ``{"new_balance": float, "transaction_id": int, "delta": float}``
        on success.

        The ``reason`` is stored in ``transactions.notes`` for audit;
        the admin's telegram id lands in two places for redundancy:
        a dedicated ``admin_telegram_id`` column (Stage-9-Step-2 —
        forensics queries can do
        ``WHERE admin_telegram_id IS NOT NULL`` cleanly) and as part
        of the ``gateway_invoice_id`` string
        ``admin-<admin_id>-<timestamp>-<rand>``. The legacy encoding
        is kept because (a) the UNIQUE constraint on
        ``gateway_invoice_id`` doubles as a duplicate-click guard
        and (b) older rows from before the column existed only have
        the encoded form, so any forensics tool needs to know to
        check both.
        """
        if delta_usd == 0:
            raise ValueError("delta_usd must be non-zero")
        # Defense-in-depth: refuse NaN / ±Infinity. Both the web admin
        # form parser (``parse_adjust_form``) and the Telegram parser
        # (``parse_balance_args``) already reject these, but a future
        # internal caller / refactor / test stub could bypass them. A
        # ``NaN`` delta would slip past the ``new_balance < 0`` check
        # (``NaN < 0`` is ``False``) and write ``NaN`` straight into
        # ``users.balance_usd``, bricking the wallet exactly the way
        # PR #75 prevented at the IPN layer.
        if not _is_finite_amount(delta_usd):
            raise ValueError(
                f"delta_usd must be finite (got NaN or Infinity: {delta_usd!r})"
            )
        import secrets
        import time

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Lock the user row so a concurrent deduct_balance
                # can't race us between the balance check and the
                # update below.
                row = await connection.fetchrow(
                    "SELECT balance_usd FROM users "
                    "WHERE telegram_id = $1 FOR UPDATE",
                    telegram_id,
                )
                if row is None:
                    return None
                current = float(row["balance_usd"])
                new_balance = current + delta_usd
                if new_balance < 0:
                    # Insufficient funds for the debit. Don't write
                    # anything — caller will surface a friendly error.
                    return None
                await connection.execute(
                    "UPDATE users SET balance_usd = $1 "
                    "WHERE telegram_id = $2",
                    new_balance, telegram_id,
                )
                invoice_id = (
                    f"admin-{admin_telegram_id}-{int(time.time() * 1000)}"
                    f"-{secrets.token_hex(4)}"
                )
                tx_id = await connection.fetchval(
                    """
                    INSERT INTO transactions (
                        telegram_id, gateway, currency_used,
                        amount_crypto_or_rial, amount_usd_credited,
                        status, gateway_invoice_id, completed_at,
                        notes, admin_telegram_id
                    )
                    VALUES (
                        $1, 'admin', 'USD',
                        NULL, $2,
                        'SUCCESS', $3, NOW(),
                        $4, $5
                    )
                    RETURNING transaction_id
                    """,
                    telegram_id, delta_usd, invoice_id, reason,
                    admin_telegram_id,
                )
        return {
            "new_balance": new_balance,
            "transaction_id": int(tx_id),
            "delta": delta_usd,
        }

    async def get_user_admin_summary(
        self, telegram_id: int, *, recent_tx_limit: int = 5
    ) -> dict | None:
        """Read-only snapshot of a user's wallet for ``/admin_balance``.

        Returns a dict shaped::

            {
              "telegram_id": int,
              "username": str | None,
              "balance_usd": float,
              "free_messages_left": int,
              "active_model": str,
              "language_code": str,
              "memory_enabled": bool,
              "total_credited_usd": float,   # sum SUCCESS|PARTIAL tx, signed
              "total_spent_usd": float,      # sum usage_logs.cost
              "recent_transactions": [
                {"id": int, "gateway": str, "currency": str,
                 "amount_usd": float, "status": str, "created_at": iso str,
                 "notes": str | None}
              ],
            }

        Returns ``None`` if the user doesn't exist.
        """
        async with self.pool.acquire() as connection:
            user_row = await connection.fetchrow(
                """
                SELECT telegram_id, username, balance_usd,
                       free_messages_left, active_model, language_code,
                       memory_enabled
                FROM users WHERE telegram_id = $1
                """,
                telegram_id,
            )
            if user_row is None:
                return None
            credited = await connection.fetchval(
                """
                SELECT COALESCE(SUM(amount_usd_credited), 0)
                FROM transactions
                WHERE telegram_id = $1
                  AND status IN ('SUCCESS', 'PARTIAL')
                """,
                telegram_id,
            )
            spent = await connection.fetchval(
                """
                SELECT COALESCE(SUM(cost_deducted_usd), 0)
                FROM usage_logs WHERE telegram_id = $1
                """,
                telegram_id,
            )
            # Cap the ``recent_tx_limit`` defensively — callers that
            # forget to bound it shouldn't be able to stream the entire
            # ledger for one user in a single query.
            limit = max(1, min(int(recent_tx_limit), 200))
            recent = await connection.fetch(
                """
                SELECT transaction_id, gateway, currency_used,
                       amount_usd_credited, status, created_at, notes
                FROM transactions WHERE telegram_id = $1
                ORDER BY transaction_id DESC LIMIT $2
                """,
                telegram_id, limit,
            )
        return {
            "telegram_id": int(user_row["telegram_id"]),
            "username": user_row["username"],
            "balance_usd": float(user_row["balance_usd"]),
            "free_messages_left": int(user_row["free_messages_left"]),
            "active_model": user_row["active_model"],
            "language_code": user_row["language_code"],
            "memory_enabled": bool(user_row["memory_enabled"]),
            "total_credited_usd": float(credited or 0),
            "total_spent_usd": float(spent or 0),
            "recent_transactions": [
                {
                    "id": int(r["transaction_id"]),
                    "gateway": r["gateway"],
                    "currency": r["currency_used"],
                    "amount_usd": float(r["amount_usd_credited"]),
                    "status": r["status"],
                    "created_at": (
                        r["created_at"].isoformat()
                        if r["created_at"] is not None else None
                    ),
                    "notes": r["notes"],
                }
                for r in recent
            ],
        }

    async def search_users(
        self, query: str, *, limit: int = 20
    ) -> list[dict]:
        """Search the ``users`` table for admin lookup.

        Dispatches on the shape of *query*:

        * If *query* parses as an integer, look up by ``telegram_id``
          (exact match — Telegram ids are unique and there's no
          business need to prefix-match a numeric id).
        * Otherwise, treat *query* as a username fragment and do a
          case-insensitive ``ILIKE`` match. The literal ``@`` prefix
          (as seen in Telegram mentions) is stripped so admins can
          paste either ``kashlev`` or ``@kashlev``.
        * Empty / whitespace-only *query* returns ``[]`` — the caller
          (web handler) uses that to distinguish "no search yet" from
          "searched and got nothing" in the UI.

        Returns a list of dicts shaped::

            {
              "telegram_id": int,
              "username": str | None,
              "balance_usd": float,
              "free_messages_left": int,
              "language_code": str,
            }

        Results are ordered by ``telegram_id DESC`` so the most
        recently-registered user matching the query comes first. The
        *limit* is clamped to ``[1, 100]`` to guard against a UI bug
        that sends a huge value.
        """
        q = (query or "").strip().lstrip("@").strip()
        if not q:
            return []
        effective_limit = max(1, min(int(limit), 100))
        async with self.pool.acquire() as connection:
            try:
                user_id = int(q)
            except ValueError:
                # Username search. Use ILIKE with a trailing wildcard
                # so admins get prefix-match-style results; escape
                # SQL-LIKE metacharacters (``%`` / ``_``) in the
                # user-supplied fragment so a username fragment like
                # ``bob_`` doesn't silently match ``bobX``.
                safe = (
                    q.replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_")
                )
                rows = await connection.fetch(
                    """
                    SELECT telegram_id, username, balance_usd,
                           free_messages_left, language_code
                    FROM users
                    WHERE username ILIKE $1 ESCAPE '\\'
                    ORDER BY telegram_id DESC
                    LIMIT $2
                    """,
                    f"%{safe}%", effective_limit,
                )
            else:
                rows = await connection.fetch(
                    """
                    SELECT telegram_id, username, balance_usd,
                           free_messages_left, language_code
                    FROM users
                    WHERE telegram_id = $1
                    LIMIT $2
                    """,
                    user_id, effective_limit,
                )
        return [
            {
                "telegram_id": int(r["telegram_id"]),
                "username": r["username"],
                "balance_usd": float(r["balance_usd"]),
                "free_messages_left": int(r["free_messages_left"]),
                "language_code": r["language_code"],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Stage-8-Part-6: paginated transactions browser
    # ------------------------------------------------------------------

    # The known set of gateway tokens the ledger actually stores.
    # Used to reject bogus filter values at the ``list_transactions``
    # boundary rather than at the SQL layer — keeps PostgreSQL from
    # ever seeing an arbitrary admin-supplied string.
    TRANSACTIONS_GATEWAY_VALUES: frozenset[str] = frozenset(
        {"nowpayments", "admin", "gift"}
    )
    # Mirror for ``status`` column — the state-machine values the
    # codebase uses anywhere. ``PARTIAL`` is the NowPayments
    # partially_paid path, ``SUCCESS`` the terminal credit, and the
    # three failure states come from ``mark_transaction_terminal``.
    TRANSACTIONS_STATUS_VALUES: frozenset[str] = frozenset(
        {"PENDING", "PARTIAL", "SUCCESS", "EXPIRED", "FAILED", "REFUNDED"}
    )
    # Per-page cap for the paginated /admin/transactions browser.
    # 200 is enough to fit a quarterly audit export on one page
    # without streaming, small enough that a misconfigured page
    # request can't blow up admin memory.
    TRANSACTIONS_MAX_PER_PAGE: int = 200

    async def list_transactions(
        self,
        *,
        gateway: str | None = None,
        status: str | None = None,
        telegram_id: int | None = None,
        page: int = 1,
        per_page: int = 50,
    ) -> dict:
        """Paginated read of ``transactions`` for the admin browser.

        All filters are optional and combine with AND. Unknown
        ``gateway`` / ``status`` values raise :class:`ValueError` — we
        don't silently return an empty result because that would
        mask a typo in the filter form. The caller (web_admin) is
        responsible for mapping those enum strings before handing
        them to us; an attacker-supplied arbitrary string cannot
        reach the SQL layer.

        ``per_page`` is clamped to ``[1, TRANSACTIONS_MAX_PER_PAGE]``
        and ``page`` is clamped to ``>= 1``. Sort is
        ``transaction_id DESC`` which — since ``transaction_id`` is
        ``SERIAL`` — is identical to ``created_at DESC`` for any real
        deploy, and strictly monotonic (i.e. no tie-breaking needed).

        Returns a dict shaped::

            {
              "rows": [
                {"id": int, "telegram_id": int | None,
                 "gateway": str, "currency": str,
                 "amount_crypto_or_rial": float | None,
                 "amount_usd": float, "status": str,
                 "gateway_invoice_id": str | None,
                 "created_at": iso str | None,
                 "completed_at": iso str | None,
                 "notes": str | None},
                ...
              ],
              "total": int,         # full filter match count
              "page": int,          # clamped page number
              "per_page": int,      # clamped per-page
              "total_pages": int,   # 0 when total == 0
            }
        """
        if gateway is not None and gateway not in self.TRANSACTIONS_GATEWAY_VALUES:
            raise ValueError(f"unknown gateway filter: {gateway!r}")
        if status is not None and status not in self.TRANSACTIONS_STATUS_VALUES:
            raise ValueError(f"unknown status filter: {status!r}")

        per_page = max(1, min(int(per_page), self.TRANSACTIONS_MAX_PER_PAGE))
        page = max(1, int(page))

        where_clauses: list[str] = []
        params: list = []
        if gateway is not None:
            params.append(gateway)
            where_clauses.append(f"gateway = ${len(params)}")
        if status is not None:
            params.append(status)
            where_clauses.append(f"status = ${len(params)}")
        if telegram_id is not None:
            params.append(int(telegram_id))
            where_clauses.append(f"telegram_id = ${len(params)}")

        where_sql = (
            "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        )
        count_sql = f"SELECT COUNT(*) FROM transactions {where_sql}"
        # LIMIT / OFFSET come last so they're always the two trailing
        # placeholders regardless of which filters ran above.
        limit_param = len(params) + 1
        offset_param = len(params) + 2
        list_sql = f"""
            SELECT transaction_id, telegram_id, gateway, currency_used,
                   amount_crypto_or_rial, amount_usd_credited, status,
                   gateway_invoice_id, created_at, completed_at, notes
            FROM transactions
            {where_sql}
            ORDER BY transaction_id DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
        """

        async with self.pool.acquire() as connection:
            total = await connection.fetchval(count_sql, *params)
            rows = await connection.fetch(
                list_sql,
                *params,
                per_page,
                (page - 1) * per_page,
            )

        total = int(total or 0)
        total_pages = (total + per_page - 1) // per_page

        return {
            "rows": [
                {
                    "id": int(r["transaction_id"]),
                    "telegram_id": (
                        int(r["telegram_id"])
                        if r["telegram_id"] is not None else None
                    ),
                    "gateway": r["gateway"],
                    "currency": r["currency_used"],
                    "amount_crypto_or_rial": (
                        float(r["amount_crypto_or_rial"])
                        if r["amount_crypto_or_rial"] is not None else None
                    ),
                    "amount_usd": float(r["amount_usd_credited"]),
                    "status": r["status"],
                    "gateway_invoice_id": r["gateway_invoice_id"],
                    "created_at": (
                        r["created_at"].isoformat()
                        if r["created_at"] is not None else None
                    ),
                    "completed_at": (
                        r["completed_at"].isoformat()
                        if r["completed_at"] is not None else None
                    ),
                    "notes": r["notes"],
                }
                for r in rows
            ],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }

    USAGE_LOGS_MAX_PER_PAGE: int = 200

    async def list_user_usage_logs(
        self,
        *,
        telegram_id: int,
        page: int = 1,
        per_page: int = 50,
    ) -> dict:
        """Stage-9-Step-8: paginated read of ``usage_logs`` for one user.

        Backs ``GET /admin/users/{id}/usage`` — last N AI calls with
        model, token counts, and per-call cost. Indexed by
        ``idx_usage_logs_telegram_created`` (added in migration
        ``0006_usage_logs_indexes``); without that index this query
        was a sequential scan.

        ``per_page`` is clamped to ``[1, USAGE_LOGS_MAX_PER_PAGE]``;
        ``page`` to ``>= 1``. Sort is ``log_id DESC`` which on a
        ``SERIAL`` column is functionally identical to
        ``created_at DESC`` and avoids a tie-break for rows with the
        same second-resolution ``created_at``.

        Returns a dict shaped::

            {
              "rows": [
                {"id": int, "model": str,
                 "prompt_tokens": int, "completion_tokens": int,
                 "total_tokens": int, "cost_usd": float,
                 "created_at": iso str | None},
                ...
              ],
              "total": int, "page": int, "per_page": int,
              "total_pages": int,
            }
        """
        per_page = max(1, min(int(per_page), self.USAGE_LOGS_MAX_PER_PAGE))
        page = max(1, int(page))
        tid = int(telegram_id)

        async with self.pool.acquire() as connection:
            total = await connection.fetchval(
                "SELECT COUNT(*) FROM usage_logs WHERE telegram_id = $1",
                tid,
            )
            rows = await connection.fetch(
                """
                SELECT log_id, model_used,
                       prompt_tokens, completion_tokens,
                       cost_deducted_usd, created_at
                FROM usage_logs
                WHERE telegram_id = $1
                ORDER BY log_id DESC
                LIMIT $2 OFFSET $3
                """,
                tid,
                per_page,
                (page - 1) * per_page,
            )

        total = int(total or 0)
        total_pages = (total + per_page - 1) // per_page
        return {
            "rows": [
                {
                    "id": int(r["log_id"]),
                    "model": r["model_used"],
                    "prompt_tokens": int(r["prompt_tokens"]),
                    "completion_tokens": int(r["completion_tokens"]),
                    "total_tokens": (
                        int(r["prompt_tokens"]) + int(r["completion_tokens"])
                    ),
                    "cost_usd": float(r["cost_deducted_usd"]),
                    "created_at": (
                        r["created_at"].isoformat()
                        if r["created_at"] is not None else None
                    ),
                }
                for r in rows
            ],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }

    async def get_user_usage_aggregates(self, telegram_id: int) -> dict:
        """Stage-9-Step-8: lightweight aggregates rendered above the
        per-user usage log table.

        Returns ``{"total_calls": int, "total_tokens": int,
        "total_cost_usd": float}`` — the user's lifetime AI usage at a
        glance. Cheap on the new index (range over a single
        ``telegram_id`` partition).
        """
        tid = int(telegram_id)
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT COUNT(*) AS calls,
                       COALESCE(SUM(prompt_tokens + completion_tokens), 0)
                           AS tokens,
                       COALESCE(SUM(cost_deducted_usd), 0) AS cost
                FROM usage_logs
                WHERE telegram_id = $1
                """,
                tid,
            )
        if row is None:
            return {"total_calls": 0, "total_tokens": 0, "total_cost_usd": 0.0}
        return {
            "total_calls": int(row["calls"] or 0),
            "total_tokens": int(row["tokens"] or 0),
            "total_cost_usd": float(row["cost"] or 0),
        }

    async def get_system_metrics(self) -> dict:
        """Aggregate counters for the admin metrics panel.

        Single round trip via ``acquire`` → multiple ``fetch*`` calls
        on the same connection. We don't bother wrapping in a
        transaction because the values are all snapshot-style and
        slight drift between the queries is tolerable for an admin
        dashboard.

        Returns a dict shaped like::

            {
              "users_total":     int,
              "users_active_7d": int,
              "revenue_usd":     float,   # sum amount_usd_credited where status IN (SUCCESS, PARTIAL)
              "spend_usd":       float,   # sum cost_deducted_usd from usage_logs
              "top_models":      [
                {"model": str, "count": int, "cost_usd": float},
                ...  # up to 5 rows, by call count over last 30d
              ],
              "pending_payments_count":            int,           # transactions.status='PENDING'
              "pending_payments_oldest_age_hours": float | None,  # NULL when zero pending
            }
        """
        async with self.pool.acquire() as connection:
            users_total = await connection.fetchval(
                "SELECT COUNT(*) FROM users"
            )
            # "Active" = sent at least one prompt in the last 7 days.
            # usage_logs has the right granularity for that — the user
            # may have a balance and have done /start without ever
            # invoking the model.
            users_active_7d = await connection.fetchval(
                """
                SELECT COUNT(DISTINCT telegram_id) FROM usage_logs
                WHERE created_at >= NOW() - INTERVAL '7 days'
                """
            )
            # NB: filter out gateway='admin' AND gateway='gift' so
            # internal ledger adjustments (admin credit/debit, gift-code
            # redemptions) don't pollute the revenue figure. "Revenue"
            # here means money that flowed in from a real payment
            # gateway, i.e. NowPayments. Gift redemptions are free
            # credit issued from nowhere — adding them to revenue would
            # make the dashboard look like we earned money every time
            # an admin mints a gift code. (Latent since PR #56 shipped
            # gateway='gift' rows; fixed in Stage-8-Part-4.)
            revenue_usd = await connection.fetchval(
                """
                SELECT COALESCE(SUM(amount_usd_credited), 0)
                FROM transactions
                WHERE status IN ('SUCCESS', 'PARTIAL')
                  AND gateway NOT IN ('admin', 'gift')
                """
            )
            spend_usd = await connection.fetchval(
                "SELECT COALESCE(SUM(cost_deducted_usd), 0) FROM usage_logs"
            )
            top_rows = await connection.fetch(
                """
                SELECT model_used AS model,
                       COUNT(*)::int AS count,
                       COALESCE(SUM(cost_deducted_usd), 0) AS cost_usd
                FROM usage_logs
                WHERE created_at >= NOW() - INTERVAL '30 days'
                GROUP BY model_used
                ORDER BY count DESC
                LIMIT 5
                """
            )
            # Stage-9-Step-9: dashboard pending-payments tile.
            # A spike of stuck PENDING rows (gateway flap, mis-issued
            # invoice, IPN delivery delay, NowPayments outage) is the
            # earliest signal that money flow has broken — every
            # PENDING is a user who paid but hasn't been credited yet.
            # The reaper (``pending_expiration``) sweeps rows older
            # than 24h to EXPIRED, so a steadily climbing count below
            # that threshold means active inflow that's not landing.
            # We surface (a) the count and (b) the oldest age so the
            # admin can tell "5 fresh invoices waiting for IPN" from
            # "5 invoices stuck for 23h about to be reaped".
            pending_row = await connection.fetchrow(
                """
                SELECT
                    COUNT(*)::int AS count,
                    EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) / 3600.0
                        AS oldest_age_hours
                FROM transactions
                WHERE status = 'PENDING'
                """
            )
        # ``MIN(created_at)`` is NULL when zero PENDING rows exist;
        # ``EXTRACT(EPOCH FROM (NOW() - NULL))`` is NULL too, so the
        # branch surfaces ``None`` cleanly without a second query.
        pending_count = int(pending_row["count"] or 0) if pending_row else 0
        oldest_age_raw = pending_row["oldest_age_hours"] if pending_row else None
        pending_oldest_age_hours = (
            float(oldest_age_raw) if oldest_age_raw is not None else None
        )
        return {
            "users_total": int(users_total or 0),
            "users_active_7d": int(users_active_7d or 0),
            "revenue_usd": float(revenue_usd or 0),
            "spend_usd": float(spend_usd or 0),
            "top_models": [
                {
                    "model": r["model"],
                    "count": int(r["count"]),
                    "cost_usd": float(r["cost_usd"]),
                }
                for r in top_rows
            ],
            "pending_payments_count": pending_count,
            "pending_payments_oldest_age_hours": pending_oldest_age_hours,
        }

    # ---- bot_strings (Stage-9-Step-1.6) ---------------------------
    # Per-(lang, key) overrides for the compiled string table in
    # ``strings.py``. Surface area is intentionally small: full-table
    # load on boot, single-row upsert on admin save, single-row delete
    # on revert. The runtime ``t()`` helper consults an in-memory cache
    # populated by ``strings.set_overrides`` so we don't round-trip
    # the DB on every message.

    async def load_all_string_overrides(self) -> dict[tuple[str, str], str]:
        """Snapshot every (lang, key) override → value pair.

        Called once at startup to seed the in-memory cache, and again
        after each admin write to refresh the cache. The table is
        bounded by the size of ``strings._STRINGS`` × 2 langs (~600
        rows max), so ``SELECT *`` is cheap.
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT lang, key, value FROM bot_strings"
            )
        return {(r["lang"], r["key"]): r["value"] for r in rows}

    async def list_string_overrides(self, *, limit: int = 200) -> list[dict]:
        """Return the most recently edited overrides for the admin
        "What did the team change?" view. Each row carries lang, key,
        the override value, who set it, and when."""
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT lang, key, value, updated_at, updated_by
                FROM bot_strings
                ORDER BY updated_at DESC
                LIMIT $1
                """,
                int(limit),
            )
        return [
            {
                "lang": r["lang"],
                "key": r["key"],
                "value": r["value"],
                "updated_at": (
                    r["updated_at"].isoformat()
                    if r["updated_at"] is not None else None
                ),
                "updated_by": r["updated_by"],
            }
            for r in rows
        ]

    async def upsert_string_override(
        self,
        lang: str,
        key: str,
        value: str,
        *,
        updated_by: str | None,
    ) -> None:
        """Insert-or-replace a single override. The ``updated_by``
        field is freeform text — typically the admin's telegram id
        for /admin_* slash commands or ``"web"`` for browser edits."""
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO bot_strings (lang, key, value, updated_at, updated_by)
                VALUES ($1, $2, $3, NOW(), $4)
                ON CONFLICT (lang, key) DO UPDATE
                  SET value = EXCLUDED.value,
                      updated_at = NOW(),
                      updated_by = EXCLUDED.updated_by
                """,
                lang,
                key,
                value,
                updated_by,
            )

    async def delete_string_override(self, lang: str, key: str) -> bool:
        """Revert to the compiled default for *(lang, key)*. Returns
        True iff a row was deleted (False = there was no override
        to begin with — caller may want to flash that as info)."""
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                "DELETE FROM bot_strings WHERE lang = $1 AND key = $2",
                lang,
                key,
            )
        # ``DELETE 0`` vs ``DELETE 1`` — asyncpg returns the raw
        # status string. Anything other than ``"DELETE 0"`` means at
        # least one row went.
        return not result.endswith(" 0")

    # ------------------------------------------------------------------
    # Stage-9-Step-2: admin audit log + per-user-field editor.
    # ------------------------------------------------------------------

    # Allow-list of user fields the admin /admin/users/{id} editor can
    # touch. Anything outside this set is rejected — we don't want a
    # malformed POST to be able to set arbitrary columns (e.g.
    # ``balance_usd`` should ONLY change through ``admin_adjust_balance``
    # so the change shows up in the transactions ledger).
    USER_EDITABLE_FIELDS = (
        "language_code",
        "active_model",
        "memory_enabled",
        "free_messages_left",
        "username",
    )

    async def record_admin_audit(
        self,
        actor: str,
        action: str,
        *,
        target: str | None = None,
        ip: str | None = None,
        outcome: str = "ok",
        meta: dict | None = None,
    ) -> int | None:
        """Append one row to ``admin_audit_log``. Returns the new id.

        Best-effort: callers should wrap the call in their own
        try/except so an audit-write failure never blocks the
        underlying admin operation. We log the exception here as well
        so the failure is double-visible in ops logs.
        """
        import json as _json
        meta_json = _json.dumps(meta) if meta is not None else None
        try:
            async with self.pool.acquire() as connection:
                row_id = await connection.fetchval(
                    """
                    INSERT INTO admin_audit_log
                        (actor, action, target, ip, outcome, meta)
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                    RETURNING id
                    """,
                    actor,
                    action,
                    target,
                    ip,
                    outcome,
                    meta_json,
                )
            return int(row_id) if row_id is not None else None
        except Exception:
            log.exception(
                "record_admin_audit failed actor=%s action=%s",
                actor, action,
            )
            return None

    async def list_admin_audit_log(
        self,
        *,
        limit: int = 200,
        action: str | None = None,
        actor: str | None = None,
    ) -> list[dict]:
        """Most recent audit rows, newest first. Optional filters
        narrow by action slug or actor."""
        clauses: list[str] = []
        params: list[object] = []
        if action:
            params.append(action)
            clauses.append(f"action = ${len(params)}")
        if actor:
            params.append(actor)
            clauses.append(f"actor = ${len(params)}")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(int(limit))
        sql = f"""
            SELECT id, ts, actor, action, target, ip, outcome, meta
              FROM admin_audit_log
              {where}
             ORDER BY ts DESC, id DESC
             LIMIT ${len(params)}
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(sql, *params)
        return [
            {
                "id": int(r["id"]),
                "ts": r["ts"].isoformat() if r["ts"] is not None else None,
                "actor": r["actor"],
                "action": r["action"],
                "target": r["target"],
                "ip": r["ip"],
                "outcome": r["outcome"],
                "meta": dict(r["meta"]) if r["meta"] is not None else None,
            }
            for r in rows
        ]

    async def record_payment_status_transition(
        self,
        gateway_invoice_id: str,
        payment_status: str,
        *,
        outcome: str,
        meta: dict | None = None,
    ) -> int | None:
        """Append one row to ``payment_status_transitions``, deduping on
        ``(gateway_invoice_id, payment_status)``.

        Returns the new ``id`` if the insert took, or ``None`` if the
        row already existed (i.e. this exact ``(invoice, status)`` pair
        was previously observed — the caller should treat that as a
        replayed IPN and bail before mutating state).

        ``outcome`` should be one of:
          * ``"applied"`` — handler actually mutated state in response
          * ``"replay"`` — handler observed but bailed early because
            the row dedupe upstream had already finalized the invoice
          * ``"noop"`` — handler intentionally did nothing (e.g.
            ``confirming`` / ``waiting`` informational IPN)

        ``meta`` is free-form structured detail stored as JSONB.

        Best-effort: any DB exception is logged and re-raised — the
        caller MUST decide whether to fail-closed (e.g. signature
        verification path) or fail-open (e.g. a transient pool blip
        for an idempotent IPN).
        """
        import json as _json
        meta_json = _json.dumps(meta) if meta is not None else None
        async with self.pool.acquire() as connection:
            row_id = await connection.fetchval(
                """
                INSERT INTO payment_status_transitions
                    (gateway_invoice_id, payment_status, outcome, meta)
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (gateway_invoice_id, payment_status)
                DO NOTHING
                RETURNING id
                """,
                gateway_invoice_id,
                payment_status,
                outcome,
                meta_json,
            )
        return int(row_id) if row_id is not None else None

    async def list_payment_status_transitions(
        self,
        *,
        limit: int = 200,
        gateway_invoice_id: str | None = None,
    ) -> list[dict]:
        """Most recent IPN transitions, newest first. Optional filter
        narrows to a single invoice (forensics: "show me everything we
        observed for this invoice")."""
        params: list[object] = []
        where = ""
        if gateway_invoice_id is not None:
            params.append(gateway_invoice_id)
            where = f"WHERE gateway_invoice_id = ${len(params)}"
        params.append(int(limit))
        sql = f"""
            SELECT id, gateway_invoice_id, payment_status,
                   recorded_at, outcome, meta
              FROM payment_status_transitions
              {where}
             ORDER BY recorded_at DESC, id DESC
             LIMIT ${len(params)}
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(sql, *params)
        return [
            {
                "id": int(r["id"]),
                "gateway_invoice_id": r["gateway_invoice_id"],
                "payment_status": r["payment_status"],
                "recorded_at": (
                    r["recorded_at"].isoformat()
                    if r["recorded_at"] is not None
                    else None
                ),
                "outcome": r["outcome"],
                "meta": dict(r["meta"]) if r["meta"] is not None else None,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Stage-9-Step-10: durable broadcast job registry
    # ------------------------------------------------------------------
    #
    # Mirrors the in-memory ``APP_KEY_BROADCAST_JOBS`` dict used by
    # ``web_admin._run_broadcast_job``. Persistence is write-through:
    # the worker keeps writing to the in-memory dict for cheap
    # progress polling, and mirrors every state transition + throttled
    # progress tick to this table so a process restart leaves a
    # forensic trail rather than orphaning the job. Reads from
    # ``broadcast_get`` / ``broadcast_detail_get`` come straight from
    # the DB (with the in-memory dict layered on top for live
    # progress numbers on an active job that the throttled writer
    # may not have flushed yet).
    #
    # Terminal states: ``completed`` / ``failed`` / ``cancelled`` /
    # ``interrupted``. The ``interrupted`` state is set by the
    # startup orphan sweep below (any row left in ``queued`` or
    # ``running`` from before the restart).

    BROADCAST_JOB_STATES: frozenset[str] = frozenset(
        {
            "queued", "running",
            "completed", "failed", "cancelled", "interrupted",
        }
    )
    BROADCAST_JOB_TERMINAL_STATES: frozenset[str] = frozenset(
        {"completed", "failed", "cancelled", "interrupted"}
    )
    BROADCAST_JOB_LIST_DEFAULT_LIMIT: int = 50
    BROADCAST_JOB_LIST_MAX_LIMIT: int = 200

    @staticmethod
    def _broadcast_job_row_to_dict(row) -> dict:
        """Coerce an asyncpg ``Record`` into the dict shape
        ``web_admin`` consumes (matches the in-memory ``job`` dict
        keys: ``sent`` / ``blocked`` / ``failed`` rather than the
        ``_count``-suffixed column names — the suffix is purely a
        SQL-side disambiguation against ``state="failed"``).
        """
        return {
            "id": row["job_id"],
            "text_preview": row["text_preview"],
            "full_text_len": int(row["full_text_len"]),
            "only_active_days": (
                int(row["only_active_days"])
                if row["only_active_days"] is not None else None
            ),
            "state": row["state"],
            "total": int(row["total"]),
            "sent": int(row["sent_count"]),
            "blocked": int(row["blocked_count"]),
            "failed": int(row["failed_count"]),
            "i": int(row["i"]),
            "error": row["error"],
            "cancel_requested": bool(row["cancel_requested"]),
            "created_at": (
                row["created_at"].isoformat()
                if row["created_at"] is not None else None
            ),
            "started_at": (
                row["started_at"].isoformat()
                if row["started_at"] is not None else None
            ),
            "completed_at": (
                row["completed_at"].isoformat()
                if row["completed_at"] is not None else None
            ),
        }

    async def insert_broadcast_job(
        self,
        *,
        job_id: str,
        text_preview: str,
        full_text_len: int,
        only_active_days: int | None,
        state: str = "queued",
    ) -> None:
        """Insert a freshly-created job row in its initial state.

        Called once from ``broadcast_post`` right after the
        in-memory dict is populated; subsequent updates flow
        through ``update_broadcast_job``. Fails loudly on
        duplicate ``job_id`` — the caller's
        ``secrets.token_urlsafe(6)`` collision rate is one in
        2**48 so this is a real bug if it ever fires.
        """
        if state not in self.BROADCAST_JOB_STATES:
            raise ValueError(
                f"invalid broadcast job state {state!r}; "
                f"expected one of {sorted(self.BROADCAST_JOB_STATES)}"
            )
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO broadcast_jobs
                    (job_id, text_preview, full_text_len,
                     only_active_days, state)
                VALUES ($1, $2, $3, $4, $5)
                """,
                job_id, text_preview, int(full_text_len),
                only_active_days, state,
            )

    async def update_broadcast_job(
        self,
        job_id: str,
        *,
        state: str | None = None,
        total: int | None = None,
        sent: int | None = None,
        blocked: int | None = None,
        failed: int | None = None,
        i: int | None = None,
        error: str | None = None,
        cancel_requested: bool | None = None,
        started_at_now: bool = False,
        completed_at_now: bool = False,
    ) -> bool:
        """Patch one or more fields on a broadcast job row.

        Each parameter is opt-in — passing only ``state="running"``
        sets just the state column. ``started_at_now`` /
        ``completed_at_now`` are flag-shaped (rather than accepting
        a caller-supplied timestamp) so the wall-clock value is
        the DB's ``NOW()`` and we can't write ``None`` accidentally.

        Returns ``True`` if a row was updated, ``False`` if no row
        matched (already-evicted history, or the caller passed a
        bad ``job_id``).
        """
        if state is not None and state not in self.BROADCAST_JOB_STATES:
            raise ValueError(
                f"invalid broadcast job state {state!r}; "
                f"expected one of {sorted(self.BROADCAST_JOB_STATES)}"
            )
        set_clauses: list[str] = []
        params: list[object] = []
        if state is not None:
            params.append(state)
            set_clauses.append(f"state = ${len(params)}")
        if total is not None:
            params.append(int(total))
            set_clauses.append(f"total = ${len(params)}")
        if sent is not None:
            params.append(int(sent))
            set_clauses.append(f"sent_count = ${len(params)}")
        if blocked is not None:
            params.append(int(blocked))
            set_clauses.append(f"blocked_count = ${len(params)}")
        if failed is not None:
            params.append(int(failed))
            set_clauses.append(f"failed_count = ${len(params)}")
        if i is not None:
            params.append(int(i))
            set_clauses.append(f"i = ${len(params)}")
        if error is not None:
            params.append(error)
            set_clauses.append(f"error = ${len(params)}")
        if cancel_requested is not None:
            params.append(bool(cancel_requested))
            set_clauses.append(f"cancel_requested = ${len(params)}")
        if started_at_now:
            set_clauses.append("started_at = NOW()")
        if completed_at_now:
            set_clauses.append("completed_at = NOW()")
        if not set_clauses:
            # No-op patch — explicitly OK rather than a SQL error.
            return True
        params.append(job_id)
        sql = (
            f"UPDATE broadcast_jobs SET {', '.join(set_clauses)} "
            f"WHERE job_id = ${len(params)} "
            f"RETURNING job_id"
        )
        async with self.pool.acquire() as connection:
            row_id = await connection.fetchval(sql, *params)
        return row_id is not None

    async def get_broadcast_job(self, job_id: str) -> dict | None:
        """Read a single job row by id; ``None`` if not found."""
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT job_id, text_preview, full_text_len,
                       only_active_days, state, total,
                       sent_count, blocked_count, failed_count,
                       i, error, cancel_requested,
                       created_at, started_at, completed_at
                  FROM broadcast_jobs
                 WHERE job_id = $1
                """,
                job_id,
            )
        if row is None:
            return None
        return self._broadcast_job_row_to_dict(row)

    async def list_broadcast_jobs(
        self, *, limit: int | None = None
    ) -> list[dict]:
        """Most recent broadcast jobs, newest first.

        ``limit`` defaults to ``BROADCAST_JOB_LIST_DEFAULT_LIMIT``
        and is clamped to ``[1, BROADCAST_JOB_LIST_MAX_LIMIT]``
        defensively — the recent-jobs page only renders 50ish
        anyway, and we don't want a UI bug to stream the entire
        table back into the template.
        """
        if limit is None:
            limit = self.BROADCAST_JOB_LIST_DEFAULT_LIMIT
        effective = max(1, min(int(limit), self.BROADCAST_JOB_LIST_MAX_LIMIT))
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT job_id, text_preview, full_text_len,
                       only_active_days, state, total,
                       sent_count, blocked_count, failed_count,
                       i, error, cancel_requested,
                       created_at, started_at, completed_at
                  FROM broadcast_jobs
                 ORDER BY created_at DESC, job_id DESC
                 LIMIT $1
                """,
                effective,
            )
        return [self._broadcast_job_row_to_dict(r) for r in rows]

    async def mark_orphan_broadcast_jobs_interrupted(self) -> int:
        """Startup orphan sweep — flip every row left in
        ``queued`` / ``running`` from before the restart to
        ``interrupted`` with ``completed_at = NOW()`` and a
        canned error message.

        Returns the number of rows updated. Idempotent: a second
        call after the same restart returns 0 because the rows
        are now ``interrupted``.

        Called from ``setup_admin_routes`` on app start so a
        restart mid-broadcast leaves a clean audit trail rather
        than a phantom "running" job whose worker task no longer
        exists.
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                UPDATE broadcast_jobs
                   SET state = 'interrupted',
                       completed_at = NOW(),
                       error = COALESCE(error,
                           'Job was running when the bot process '
                           'restarted; no recipients sent after this '
                           'point. Re-run the broadcast manually if '
                           'needed.')
                 WHERE state IN ('queued', 'running')
                RETURNING job_id
                """
            )
        return len(rows)

    async def get_seen_model_ids(self) -> set[str]:
        """Return the full set of previously-observed OpenRouter model ids.

        Used by :mod:`model_discovery` to diff against the live
        catalog. Returns a :class:`set` so the caller can compute
        ``live_ids - seen_ids`` in O(n). Empty set on first run
        (migration just created the table). Callers must NOT treat
        the empty case as "every current model is new" — the
        discovery loop's bootstrap path handles first-run suppression.
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch("SELECT model_id FROM seen_models")
        return {row["model_id"] for row in rows}

    async def record_seen_models(self, model_ids) -> int:
        """Insert any new ``model_ids`` into ``seen_models``.

        Uses ``ON CONFLICT DO NOTHING`` so concurrent calls (e.g.
        two worker processes racing the discovery loop) can't crash
        on duplicate keys. Accepts any iterable — caller typically
        passes the set difference from :meth:`get_seen_model_ids`.

        Returns the number of rows actually inserted (useful for tests
        and operator logging — zero means every id was already known).
        A unit-test caller that wants to re-seed a deterministic set
        should clear the table between runs rather than relying on
        this method's insert count.
        """
        ids = [str(m) for m in model_ids if m]
        if not ids:
            return 0
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                INSERT INTO seen_models (model_id)
                     SELECT unnest($1::text[])
                ON CONFLICT (model_id) DO NOTHING
                  RETURNING model_id
                """,
                ids,
            )
        return len(rows)

    async def update_user_admin_fields(
        self,
        telegram_id: int,
        *,
        fields: dict,
    ) -> dict | None:
        """Update an allow-listed subset of user fields atomically.

        ``fields`` is a dict whose keys must be in
        ``USER_EDITABLE_FIELDS``. Any key outside that allow-list is
        a programming error and is rejected with ``ValueError``
        before we touch the DB — defense in depth, the caller is
        already supposed to clamp the form input to this set.

        Returns:
            * ``None`` if no row matched ``telegram_id``
            * ``{"changed": {field: new_value, ...}}`` on success
              (echoing what was actually written so the caller can
              flash a precise summary)
        """
        if not fields:
            raise ValueError("fields must be non-empty")
        for k in fields:
            if k not in self.USER_EDITABLE_FIELDS:
                raise ValueError(f"field {k!r} is not user-editable")
        # Build a positional UPDATE — asyncpg can't bind an arbitrary
        # column-name list, so we string-format the (allow-listed)
        # column names directly. NEVER do this with caller-supplied
        # column names; the allow-list above is the only thing
        # keeping this safe.
        set_clauses: list[str] = []
        params: list[object] = []
        for col, value in fields.items():
            params.append(value)
            set_clauses.append(f"{col} = ${len(params)}")
        params.append(telegram_id)
        sql = (
            f"UPDATE users SET {', '.join(set_clauses)} "
            f"WHERE telegram_id = ${len(params)} RETURNING telegram_id"
        )
        async with self.pool.acquire() as connection:
            result = await connection.fetchval(sql, *params)
        if result is None:
            return None
        return {"changed": dict(fields)}


# Export a single instance to be used across the app
db = Database()