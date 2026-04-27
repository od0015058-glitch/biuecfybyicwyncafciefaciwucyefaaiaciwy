import asyncpg
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
        print("✅ Database connection pool established.")

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

    async def add_balance(self, telegram_id: int, amount_usd: float):
        """Safely adds USD to the user's wallet after a successful crypto payment."""
        query = "UPDATE users SET balance_usd = balance_usd + $1 WHERE telegram_id = $2"
        async with self.pool.acquire() as connection:
            await connection.execute(query, amount_usd, telegram_id)

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
        """Atomically flip a PENDING transaction to a terminal failure status.

        Used for IPN statuses that mean the user will NOT be credited
        (EXPIRED / FAILED / REFUNDED). The user's balance is not touched.

        Returns the row (with telegram_id, currency_used, amount_usd_credited)
        if the flip happened, or None if the transaction was unknown or
        already in a non-PENDING state. Idempotent against retries.
        """
        query = """
            UPDATE transactions
            SET status = $2, completed_at = CURRENT_TIMESTAMP
            WHERE gateway_invoice_id = $1 AND status = 'PENDING'
            RETURNING telegram_id, currency_used, amount_usd_credited
        """
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, gateway_invoice_id, new_status)

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

                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = 'PARTIAL',
                        amount_usd_credited = $2,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE gateway_invoice_id = $1
                    """,
                    gateway_invoice_id,
                    actually_paid_usd,
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
                    "amount_usd_credited": actually_paid_usd,
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

                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = 'SUCCESS',
                        amount_usd_credited = $2,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE gateway_invoice_id = $1
                    """,
                    gateway_invoice_id,
                    full_price_usd,
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
                    "amount_usd_credited": full_price_usd,
                    "delta_credited": delta,
                }

# Export a single instance to be used across the app
db = Database()