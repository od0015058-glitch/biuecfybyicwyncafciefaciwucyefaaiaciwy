import logging
import os

import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

log = logging.getLogger("bot.database")

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
        """
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
        """Logs the exact token usage for accounting."""
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
        """
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
        """
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
        """
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
        """
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
        if amount_usd is None or amount_usd <= 0:
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
                       u.first_name, u.username
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
                "first_name": r["first_name"],
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
        the admin's telegram id is encoded into ``gateway_invoice_id``
        as ``admin-<admin_id>-<timestamp>-<rand>`` (UNIQUE on the
        column means we get a free duplicate-click guard).
        """
        if delta_usd == 0:
            raise ValueError("delta_usd must be non-zero")
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
                        status, gateway_invoice_id, completed_at, notes
                    )
                    VALUES (
                        $1, 'admin', 'USD',
                        NULL, $2,
                        'SUCCESS', $3, NOW(), $4
                    )
                    RETURNING transaction_id
                    """,
                    telegram_id, delta_usd, invoice_id, reason,
                )
        return {
            "new_balance": new_balance,
            "transaction_id": int(tx_id),
            "delta": delta_usd,
        }

    async def get_user_admin_summary(self, telegram_id: int) -> dict | None:
        """Read-only snapshot of a user's wallet for ``/admin_balance``.

        Returns a dict shaped::

            {
              "telegram_id": int,
              "username": str | None,
              "balance_usd": float,
              "free_messages_left": int,
              "active_model": str,
              "language_code": str,
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
                       free_messages_left, active_model, language_code
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
            recent = await connection.fetch(
                """
                SELECT transaction_id, gateway, currency_used,
                       amount_usd_credited, status, created_at, notes
                FROM transactions WHERE telegram_id = $1
                ORDER BY transaction_id DESC LIMIT 5
                """,
                telegram_id,
            )
        return {
            "telegram_id": int(user_row["telegram_id"]),
            "username": user_row["username"],
            "balance_usd": float(user_row["balance_usd"]),
            "free_messages_left": int(user_row["free_messages_left"]),
            "active_model": user_row["active_model"],
            "language_code": user_row["language_code"],
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

    async def get_system_metrics(self) -> dict:
        """Aggregate counters for the admin metrics panel.

        Single round trip via ``acquire`` → multiple ``fetch*`` calls
        on the same connection. We don't bother wrapping in a
        transaction because the values are all snapshot-style and
        slight drift between the four queries is tolerable for an
        admin dashboard.

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
            # NB: filter out gateway='admin' so admin debits/credits
            # don't pollute the revenue figure — those are internal
            # ledger adjustments, not gateway income.
            revenue_usd = await connection.fetchval(
                """
                SELECT COALESCE(SUM(amount_usd_credited), 0)
                FROM transactions
                WHERE status IN ('SUCCESS', 'PARTIAL')
                  AND gateway <> 'admin'
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
        }


# Export a single instance to be used across the app
db = Database()