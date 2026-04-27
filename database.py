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
    ) -> bool:
        """Records a payment as PENDING. Returns True iff a new row was inserted.

        ON CONFLICT on the unique gateway_invoice_id makes this safe to retry;
        a duplicate invoice id will not create a second row.
        """
        query = """
            INSERT INTO transactions (
                telegram_id, gateway, currency_used,
                amount_crypto_or_rial, amount_usd_credited,
                status, gateway_invoice_id
            )
            VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)
            ON CONFLICT (gateway_invoice_id) DO NOTHING
            RETURNING transaction_id
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                query,
                telegram_id, gateway, currency_used,
                amount_crypto, amount_usd, gateway_invoice_id,
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
                    SELECT telegram_id, status, amount_usd_credited
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
                    "amount_usd_credited": new_credited,
                    "delta_credited": delta,
                }

# Export a single instance to be used across the app
db = Database()