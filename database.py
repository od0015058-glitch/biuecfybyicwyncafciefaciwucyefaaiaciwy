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

    async def deduct_balance(self, telegram_id: int, cost_usd: float):
        """Safely deducts USD cost from the wallet."""
        query = "UPDATE users SET balance_usd = balance_usd - $1 WHERE telegram_id = $2"
        async with self.pool.acquire() as connection:
            await connection.execute(query, cost_usd, telegram_id)

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

    async def complete_transaction(self, gateway_invoice_id: str):
        """Marks a PENDING transaction SUCCESS. Returns the row dict if the
        update happened, or None if the transaction was already finalized,
        not found, or in a non-PENDING state.

        The atomic UPDATE ... WHERE status = 'PENDING' guarantees idempotency:
        replayed webhooks for the same payment will see no row to update and
        will return None, so the caller must skip crediting.
        """
        query = """
            UPDATE transactions
            SET status = 'SUCCESS', completed_at = CURRENT_TIMESTAMP
            WHERE gateway_invoice_id = $1 AND status = 'PENDING'
            RETURNING telegram_id, amount_usd_credited
        """
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, gateway_invoice_id)

# Export a single instance to be used across the app
db = Database()