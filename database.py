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

        This is an *advisory* check at UI time — the authoritative gate
        runs again under FOR UPDATE inside the SUCCESS transaction
        (see :meth:`_consume_promo_in_tx`) so two parallel invoices
        can't both take the last seat of a single-use code.
        """
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

        Returns the bonus actually credited.
        """
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
        """
        if (discount_percent is None) == (discount_amount is None):
            raise ValueError(
                "Exactly one of discount_percent / discount_amount must be set"
            )
        if discount_percent is not None and not (1 <= discount_percent <= 100):
            raise ValueError("discount_percent must be between 1 and 100")
        if discount_amount is not None and discount_amount <= 0:
            raise ValueError("discount_amount must be positive")
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


# Export a single instance to be used across the app
db = Database()