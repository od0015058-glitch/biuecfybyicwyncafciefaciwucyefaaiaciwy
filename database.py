import json
import logging
import math
import os

import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

log = logging.getLogger("bot.database")


def _decode_jsonb_meta(value: object) -> dict | None:
    """Coerce an asyncpg-fetched JSONB column value into a ``dict``.

    asyncpg returns JSONB columns as raw ``str`` by default (no codec
    is registered on the pool — see the audit + payment-status
    transitions writers, which all hand-cast ``$N::jsonb`` from a
    ``json.dumps``-rendered string). On the read side, the historical
    code path was ``dict(r["meta"]) if r["meta"] is not None else
    None``, which works fine in tests (asyncpg-Record-like dict
    fixtures) but raises ``ValueError`` in production for any
    non-empty meta because ``dict("...JSON string...")`` interprets
    the string as a sequence of 2-tuples. The audit page handler
    swallowed the exception and rendered "Database query failed" so
    the regression was silent in production.

    This helper accepts the union of shapes asyncpg / future codecs
    might return:

    * ``None`` → ``None``.
    * ``dict`` (a future ``set_type_codec`` registration would produce
      this) → defensive shallow copy so the caller can mutate freely.
    * ``str`` / ``bytes`` / ``bytearray`` (asyncpg's default) →
      ``json.loads`` into a dict.
    * Anything else (or a ``json.loads`` that returns a non-dict) →
      ``None`` with a logged WARNING. Defense-in-depth: a buggy SQL
      INSERT writing ``'"oops"'::jsonb`` shouldn't crash every
      subsequent read of the table.

    Decoding errors are logged and demoted to ``None``: a single
    poisoned row should not blank the entire feed.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (str, bytes, bytearray)):
        try:
            decoded = json.loads(value)
        except (ValueError, TypeError) as exc:
            log.warning("could not decode JSONB meta value: %s", exc)
            return None
        if isinstance(decoded, dict):
            return decoded
        log.warning(
            "JSONB meta value decoded to non-dict (%s); treating as null",
            type(decoded).__name__,
        )
        return None
    log.warning(
        "unexpected JSONB meta value type %s; treating as null",
        type(value).__name__,
    )
    return None


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

        Stage-15-Step-E #10 root-cause fix: strip the U+0000 NUL byte
        before INSERT. Postgres TEXT rejects ``\\x00`` outright with
        ``invalid byte sequence for encoding "UTF8": 0x00`` — every
        other Unicode code point (including the rest of the C0 /
        C1 control range) is accepted, so a targeted strip preserves
        the user's content with maximum fidelity. The previous PR
        (#129 / Stage-15-Step-E #10 first slice) wrapped the call
        site in ``ai_engine.chat_with_model`` in a defensive try/
        except so a NUL-bearing prompt wouldn't *lose the AI reply*
        and silently double-bill on retry — that fix handles the
        symptom (the broad exception path) but the underlying memory
        turn is still discarded, so the user's conversation buffer
        develops gaps every time a NUL slips through. Telegram
        clients DO let users send U+0000 (paste from a binary file,
        certain emoji-keyboard bugs on Android), so this isn't
        theoretical. Stripping at the DB layer means the buffer
        stays intact and the retrying defensive wrap upstream
        becomes a backstop for the *other* failure modes
        (transient disconnect, deadlock, FK violation on
        concurrent user-row delete) it was originally designed to
        cover. We log loud-and-once when the strip actually fires,
        so ops can investigate the source of the NUL.
        """
        if role not in ("user", "assistant"):
            raise ValueError(f"invalid role: {role}")
        if "\x00" in content:
            stripped = content.replace("\x00", "")
            log.warning(
                "append_conversation_message: stripping %d NUL byte(s) "
                "from %s message for user %d (Postgres TEXT rejects "
                "\\x00); preserving the rest of the content",
                content.count("\x00"), role, telegram_id,
            )
            content = stripped
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

    async def get_full_conversation(
        self, telegram_id: int
    ) -> list[dict]:
        """Return every persisted message for the user, ordered
        oldest-first, with the ``created_at`` timestamp included.

        Stage-15-Step-E #1 (conversation history export, first
        slice): the running-window read path
        (:meth:`get_recent_messages`) drops the timestamp because
        the LLM doesn't need it. Export needs it. This method is
        the export-specific counterpart — same table, same filter,
        no ``LIMIT``, ``ASC`` order, and the timestamp column
        included.

        Returns an empty list for a user with no buffer (no
        special-case for ``memory_enabled`` because exporting
        history that *was* recorded before the toggle was flipped
        off is a legitimate use case — the user owns the data
        even after they disable the feature).
        """
        query = """
            SELECT role, content, created_at
              FROM conversation_messages
             WHERE telegram_id = $1
             ORDER BY created_at ASC, id ASC
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(query, telegram_id)
        return [
            {
                "role": r["role"],
                "content": r["content"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

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
        gateway_locked_rate_toman_per_usd: float | None = None,
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

        ``gateway_locked_rate_toman_per_usd`` (Stage-11-Step-C) is the
        USD→Toman rate captured at order-creation time for the
        TetraPay Rial gateway. ``finalize_payment`` does NOT consult
        this column — the locked USD figure is already in
        ``amount_usd_credited`` and that's what gets credited. The
        rate is stored for forensic / audit purposes ("what rate did
        we promise the user?"). NULL for crypto rows.

        Bundled bug fix (Stage-11-Step-C): refuse non-finite
        ``amount_usd`` / ``amount_crypto`` / ``promo_bonus_usd`` /
        ``gateway_locked_rate_toman_per_usd`` *before* INSERT.
        Pre-fix, ``create_pending_transaction`` had NO finite-amount
        guard — the matching defense lives on every *write* site
        (``deduct_balance``, ``redeem_gift_code``, ``log_usage``,
        ``admin_adjust_balance``) and every *settle* site
        (``finalize_payment``, ``finalize_partial_payment``), but the
        *create* site relied entirely on its callers being
        well-behaved. A buggy / future caller (or an admin tool that
        pre-stages PENDING rows from CSV) passing ``float('nan')``
        would happily INSERT a poisoned row: PostgreSQL's NUMERIC
        accepts ``'NaN'::numeric`` without complaint and there's no
        CHECK constraint on the column. ``finalize_payment`` then
        refuses to credit (its own NaN guard fires), leaving the
        invoice eternally PENDING until the reaper (Stage-9-Step-9)
        sweeps it ~24h later — but with the user already having
        paid the gateway. This guard fails fast at the INSERT instead.
        """
        if not _is_finite_amount(amount_usd) or amount_usd <= 0:
            log.error(
                "create_pending_transaction refused for invoice=%s "
                "gateway=%s: non-finite or non-positive amount_usd=%r",
                gateway_invoice_id, gateway, amount_usd,
            )
            return False
        if not _is_finite_amount(amount_crypto) or amount_crypto <= 0:
            log.error(
                "create_pending_transaction refused for invoice=%s "
                "gateway=%s: non-finite or non-positive amount_crypto=%r",
                gateway_invoice_id, gateway, amount_crypto,
            )
            return False
        # Bonus may legitimately be 0 (no promo) so we only refuse on
        # non-finite or *negative* values — a positive 0 is fine.
        if not _is_finite_amount(promo_bonus_usd) or promo_bonus_usd < 0:
            log.error(
                "create_pending_transaction refused for invoice=%s "
                "gateway=%s: non-finite or negative promo_bonus_usd=%r",
                gateway_invoice_id, gateway, promo_bonus_usd,
            )
            return False
        if gateway_locked_rate_toman_per_usd is not None:
            if (
                not _is_finite_amount(gateway_locked_rate_toman_per_usd)
                or gateway_locked_rate_toman_per_usd <= 0
            ):
                log.error(
                    "create_pending_transaction refused for invoice=%s "
                    "gateway=%s: non-finite or non-positive "
                    "gateway_locked_rate_toman_per_usd=%r",
                    gateway_invoice_id, gateway,
                    gateway_locked_rate_toman_per_usd,
                )
                return False

        query = """
            INSERT INTO transactions (
                telegram_id, gateway, currency_used,
                amount_crypto_or_rial, amount_usd_credited,
                status, gateway_invoice_id,
                promo_code_used, promo_bonus_usd,
                gateway_locked_rate_toman_per_usd
            )
            VALUES ($1, $2, $3, $4, $5, 'PENDING', $6, $7, $8, $9)
            ON CONFLICT (gateway_invoice_id) DO NOTHING
            RETURNING transaction_id
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchval(
                query,
                telegram_id, gateway, currency_used,
                amount_crypto, amount_usd, gateway_invoice_id,
                promo_code, promo_bonus_usd,
                gateway_locked_rate_toman_per_usd,
            )
        return row is not None

    # Stage-9-Step-5: explicit allow-list for terminal-failure statuses.
    # Lifted out of the function body so callers (and tests) can refer to
    # the canonical set without grepping. SUCCESS is its own ledger
    # status reached via ``finalize_payment``, NOT
    # ``mark_transaction_terminal``.
    #
    # Stage-12-Step-A bug-fix: REFUNDED used to live in this set, which
    # meant ``mark_transaction_terminal("...", "REFUNDED")`` would flip
    # a row to REFUNDED *without* debiting the user — a future caller
    # using the helper for a SUCCESS-row refund would silently mint
    # money (user keeps the credit AND the gateway returned the funds).
    # REFUNDED is now its own state with its own entry points:
    #   * :meth:`refund_transaction` — admin-issued refund, debits the
    #     wallet by the credited USD amount inside the same DB tx.
    #   * :meth:`mark_payment_refunded_via_ipn` — gateway-side refund
    #     (NowPayments ``refunded`` IPN); does not debit, mirrors the
    #     PARTIAL-credit-stays-with-the-user limitation documented on
    #     ``mark_transaction_terminal``.
    TERMINAL_FAILURE_STATUSES: frozenset[str] = frozenset(
        {"EXPIRED", "FAILED"}
    )
    # Refund states have their own canonical set so the type/value
    # system catches a future caller passing REFUNDED to
    # ``mark_transaction_terminal`` (which never debits) when they
    # actually meant ``refund_transaction`` (which does).
    REFUND_STATUSES: frozenset[str] = frozenset({"REFUNDED"})

    async def mark_transaction_terminal(
        self, gateway_invoice_id: str, new_status: str
    ):
        """Atomically close a PENDING or PARTIAL transaction with a terminal
        failure status (EXPIRED / FAILED).

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

        Stage-12-Step-A: REFUNDED is no longer accepted here — see the
        ``REFUND_STATUSES`` docstring above and the dedicated
        :meth:`refund_transaction` / :meth:`mark_payment_refunded_via_ipn`
        entry points. A caller that needs to record a refund must pick
        one of those depending on whether the wallet should be debited.
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

    # Stage-12-Step-A: gateway-side refund (NowPayments ``refunded`` IPN).
    # Carved out of ``mark_transaction_terminal`` so the type system
    # makes "refund" a deliberate, separate API surface — the wallet
    # debit / no-debit decision is no longer hidden behind a status
    # string parameter.
    async def mark_payment_refunded_via_ipn(self, gateway_invoice_id: str):
        """Atomically close a PENDING / PARTIAL row as REFUNDED in
        response to a gateway-side refund (today: NowPayments
        ``refunded`` IPN).

        The user's wallet is **not** debited — for a PARTIAL row the
        partial credit they already received stays put, mirroring the
        documented limitation on :meth:`mark_transaction_terminal`
        (we cannot programmatically reverse a partial-credit on the
        wallet side because the gateway has already returned the
        crypto / fiat to the buyer; the operator handles any
        correction off-ledger). For a PENDING row there is nothing
        to debit — no credit was issued.

        Returns the same row-dict shape as :meth:`mark_transaction_terminal`
        on success, or ``None`` if the row was unknown or already
        terminal. Idempotent against retries via the WHERE-status
        guard.

        Admin-issued refunds of SUCCESS rows go through
        :meth:`refund_transaction` instead, which DOES debit the
        wallet — the two flows are intentionally distinct entry
        points so a misrouted call can't silently mint money.
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
                previous_status = row["status"]
                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = 'REFUNDED',
                        completed_at = CURRENT_TIMESTAMP,
                        refunded_at = CURRENT_TIMESTAMP,
                        refund_reason = $2
                    WHERE gateway_invoice_id = $1
                      AND status != 'REFUNDED'
                    """,
                    gateway_invoice_id,
                    "[ipn] gateway reported refunded",
                )
                return {
                    "telegram_id": row["telegram_id"],
                    "currency_used": row["currency_used"],
                    "amount_usd_credited": row["amount_usd_credited"],
                    "previous_status": previous_status,
                }

    # Stage-12-Step-A: admin-issued bookkeeping refund.
    REFUND_REASON_MAX_LEN: int = 500
    # Distinguishable failure shapes returned by ``refund_transaction``
    # so the caller can render a friendly banner without a second DB
    # round-trip to figure out *why* we said no. ``None`` is reserved
    # for "row no longer exists" — a benign race where the row was
    # deleted between the user clicking refund and the POST landing.
    REFUND_REFUSAL_NOT_FOUND = "not_found"
    REFUND_REFUSAL_NOT_SUCCESS = "not_success"
    REFUND_REFUSAL_GATEWAY_NOT_REFUNDABLE = "gateway_not_refundable"
    REFUND_REFUSAL_INSUFFICIENT_BALANCE = "insufficient_balance"
    # The set of gateways the admin refund flow knows how to reverse.
    # ``admin`` and ``gift`` rows are reversed via
    # :meth:`admin_adjust_balance` (which writes its own ledger row)
    # so the refund flow refuses them — there's no underlying gateway
    # transaction to mark refunded.
    REFUNDABLE_GATEWAYS: frozenset[str] = frozenset({"nowpayments", "tetrapay"})

    async def refund_transaction(
        self,
        *,
        transaction_id: int,
        reason: str,
        admin_telegram_id: int,
    ) -> dict | None:
        """Admin-issued refund of a SUCCESS gateway transaction.

        Atomically:
          1. Lock the row with ``SELECT ... FOR UPDATE``.
          2. Refuse if it's not SUCCESS, not from a refundable gateway,
             or has already been refunded.
          3. Lock the user row and refuse if the wallet doesn't have
             enough balance to absorb the debit (the operator gets a
             friendly "user spent it; debit manually first" banner).
          4. Debit the wallet by ``amount_usd_credited`` (the original
             credit USD figure, NOT recomputed at refund time — for
             TetraPay this preserves the locked-rate semantics from
             Stage-11-Step-C; for NowPayments this is the same USD
             we credited at finalize time).
          5. Flip ``status`` -> REFUNDED, write ``refunded_at`` and
             ``refund_reason``.

        Returns a dict on success::

            {
              "transaction_id": int,
              "telegram_id": int,
              "amount_refunded_usd": float,
              "new_balance_usd": float,
            }

        On a refusal returns a dict shaped
        ``{"error": <REFUND_REFUSAL_*>, "current_status": str | None,
           "balance_usd": float | None, "amount_usd": float | None}``.

        Returns ``None`` only when the row genuinely does not exist
        (the rare race where it was deleted between the operator
        clicking the button and the POST landing).

        ``reason`` is mandatory, capped at ``REFUND_REASON_MAX_LEN``
        chars, and stored verbatim on ``transactions.refund_reason``
        for the forensic record. ``admin_telegram_id`` is the
        operator's id (the web panel passes its sentinel
        ``ADMIN_WEB_SENTINEL_ID``); it lands on the audit log row
        recorded by the caller (this method does not write the audit
        row itself — the web layer does, so a DB blip writing the
        audit row never blocks the refund itself, mirroring the
        ``_record_audit_safe`` pattern used everywhere else).

        NowPayments has no programmatic refund API for confirmed
        crypto payments — this method is a **bookkeeping** refund
        only. The operator separately settles the user off-chain /
        off-card; the row's REFUNDED state records that we did.
        TetraPay's ``/api/refund`` endpoint is documented; calling
        it inline is a Stage-12-Step-A.5 follow-up if the user asks.
        """
        if not isinstance(transaction_id, int) or transaction_id <= 0:
            raise ValueError(
                f"transaction_id must be a positive int; got {transaction_id!r}"
            )
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        reason = reason.strip()
        if len(reason) > self.REFUND_REASON_MAX_LEN:
            raise ValueError(
                f"reason longer than REFUND_REASON_MAX_LEN "
                f"({self.REFUND_REASON_MAX_LEN}); got {len(reason)}"
            )

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Lock the transactions row first so a concurrent IPN
                # cannot flip status out from under us between the
                # eligibility check and the UPDATE.
                tx_row = await connection.fetchrow(
                    """
                    SELECT transaction_id, telegram_id, gateway,
                           amount_usd_credited, status
                    FROM transactions
                    WHERE transaction_id = $1
                    FOR UPDATE
                    """,
                    transaction_id,
                )
                if tx_row is None:
                    return None
                if tx_row["status"] != "SUCCESS":
                    return {
                        "error": self.REFUND_REFUSAL_NOT_SUCCESS,
                        "current_status": tx_row["status"],
                        "balance_usd": None,
                        "amount_usd": float(tx_row["amount_usd_credited"]),
                    }
                if tx_row["gateway"] not in self.REFUNDABLE_GATEWAYS:
                    return {
                        "error": self.REFUND_REFUSAL_GATEWAY_NOT_REFUNDABLE,
                        "current_status": tx_row["status"],
                        "balance_usd": None,
                        "amount_usd": float(tx_row["amount_usd_credited"]),
                    }

                telegram_id = int(tx_row["telegram_id"])
                amount_usd = float(tx_row["amount_usd_credited"])
                # Defense-in-depth: refuse non-finite or non-positive
                # amounts. ``finalize_payment`` already guards the
                # credit side, but a corrupted row from a manual SQL
                # fix could still poison the refund path.
                if not _is_finite_amount(amount_usd) or amount_usd <= 0:
                    log.error(
                        "refund_transaction: non-finite or non-positive "
                        "amount_usd_credited=%r on transaction_id=%d; "
                        "refusing refund",
                        amount_usd, transaction_id,
                    )
                    return {
                        "error": self.REFUND_REFUSAL_INSUFFICIENT_BALANCE,
                        "current_status": tx_row["status"],
                        "balance_usd": None,
                        "amount_usd": amount_usd,
                    }

                # Lock the user row so a concurrent ``deduct_balance`` /
                # ``admin_adjust_balance`` can't race the balance check.
                user_row = await connection.fetchrow(
                    "SELECT balance_usd FROM users "
                    "WHERE telegram_id = $1 FOR UPDATE",
                    telegram_id,
                )
                if user_row is None:
                    # The user row is gone but the transaction row
                    # still references it — extremely unlikely with
                    # the FK in place, but treat as "not found" for
                    # the operator banner.
                    return None
                current_balance = float(user_row["balance_usd"])
                if not _is_finite_amount(current_balance):
                    log.error(
                        "refund_transaction: non-finite balance_usd=%r on "
                        "user %d; refusing refund",
                        current_balance, telegram_id,
                    )
                    return {
                        "error": self.REFUND_REFUSAL_INSUFFICIENT_BALANCE,
                        "current_status": tx_row["status"],
                        "balance_usd": current_balance,
                        "amount_usd": amount_usd,
                    }
                new_balance = current_balance - amount_usd
                if new_balance < 0:
                    return {
                        "error": self.REFUND_REFUSAL_INSUFFICIENT_BALANCE,
                        "current_status": tx_row["status"],
                        "balance_usd": current_balance,
                        "amount_usd": amount_usd,
                    }

                await connection.execute(
                    "UPDATE users SET balance_usd = $1 "
                    "WHERE telegram_id = $2",
                    new_balance, telegram_id,
                )
                await connection.execute(
                    """
                    UPDATE transactions
                    SET status = 'REFUNDED',
                        refunded_at = CURRENT_TIMESTAMP,
                        refund_reason = $2
                    WHERE transaction_id = $1
                      AND status = 'SUCCESS'
                    """,
                    transaction_id, reason,
                )
                log.info(
                    "refund_transaction: tx=%d user=%d amount=$%.4f "
                    "admin=%d reason=%r",
                    transaction_id, telegram_id, amount_usd,
                    admin_telegram_id, reason,
                )
                return {
                    "transaction_id": transaction_id,
                    "telegram_id": telegram_id,
                    "amount_refunded_usd": amount_usd,
                    "new_balance_usd": new_balance,
                }

    async def get_pending_invoice_amount_usd(
        self, gateway_invoice_id: str
    ) -> float | None:
        """Look up the locked USD figure for a PENDING / PARTIAL invoice.

        Returns the value of ``amount_usd_credited`` cast to ``float``,
        or ``None`` if the invoice is unknown or already in a terminal
        status (SUCCESS / EXPIRED / FAILED / REFUNDED).

        Stage-11-Step-C. Used by the TetraPay webhook to read the
        at-creation-time locked USD amount before passing it to
        :meth:`finalize_payment`. The NowPayments path doesn't use
        this helper because the IPN itself carries ``price_amount``
        (NowPayments quotes the USD figure on every IPN); TetraPay's
        callback only carries ``{status, hash_id, authority}``, so
        we have to read our own ledger to recover the locked figure.

        Note: this helper does NOT take ``FOR UPDATE``. The
        subsequent ``finalize_payment`` call re-reads the same row
        under ``FOR UPDATE`` and re-checks the status, so the only
        lossy race is "row was finalized between our read and the
        UPDATE" — which ``finalize_payment`` itself handles by
        returning ``None``. Holding the lock across the verify HTTP
        call would be much worse (a slow TetraPay verify would
        starve any concurrent reads of the same row).
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT status, amount_usd_credited
                FROM transactions
                WHERE gateway_invoice_id = $1
                """,
                gateway_invoice_id,
            )
        if row is None:
            return None
        if row["status"] not in ("PENDING", "PARTIAL"):
            return None
        return float(row["amount_usd_credited"])

    async def get_pending_invoice_amount_irr(
        self, gateway_invoice_id: str
    ) -> int | None:
        """Look up the integer rial figure for a PENDING / PARTIAL invoice.

        Returns ``transactions.amount_crypto_or_rial`` as ``int``,
        or ``None`` if the invoice is unknown or already in a
        terminal status.

        Stage-15-Step-E #8. Used by the Zarinpal callback handler to
        recover the original rial amount before passing it to
        ``zarinpal.verify_payment``. Zarinpal's verify endpoint
        requires the SAME amount that was sent on ``create_order``
        (server-side mismatch defense against a tampered redirect),
        so we have to read the locked figure back from our own
        ledger rather than trusting any field that arrived in the
        URL query string.

        Cast to ``int`` because the rial figure is always an integer
        — Shaparak doesn't settle fractional rials. The DB column is
        ``DECIMAL`` so legacy crypto rows have a fractional part; we
        ``round()`` defensively before the cast so a hand-edited
        legacy row can't trip a ``ValueError``. Returns ``None`` on
        a non-finite value (legacy poisoned row); the caller surfaces
        that as a "refusing to verify" branch.

        Same race-tolerance contract as
        :meth:`get_pending_invoice_amount_usd`: no ``FOR UPDATE``,
        because the verify HTTP call is too slow to hold a row lock
        across.
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT status, amount_crypto_or_rial
                FROM transactions
                WHERE gateway_invoice_id = $1
                """,
                gateway_invoice_id,
            )
        if row is None:
            return None
        if row["status"] not in ("PENDING", "PARTIAL"):
            return None
        raw = row["amount_crypto_or_rial"]
        if raw is None:
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if not _is_finite_amount(value) or value <= 0:
            return None
        return int(round(value))

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

    async def list_pending_payments_over_threshold(
        self,
        *,
        threshold_hours: int,
        limit: int = 500,
    ) -> list[dict]:
        """List stuck PENDING transactions older than ``threshold_hours``.

        Stage-12-Step-B: read-only companion to :meth:`expire_stale_pending`.
        The reaper *closes* rows older than the terminal 24 h
        threshold; this method *surfaces* rows that crossed a much
        earlier alert threshold (``PENDING_ALERT_THRESHOLD_HOURS``,
        default 2 h) so the background :func:`pending_alert.run_pending_alert_pass`
        can DM admins while the invoice is still well inside the
        reaper's grace period.

        Returns one dict per row, sorted oldest first, including the
        ``age_hours`` computed server-side (authoritative clock is
        Postgres, not the Python host) so the dedupe bucketing in the
        alert loop is consistent across restarts and across replicas.

        ``limit`` is a safety cap so a runaway backlog can't produce
        a 10-row DM with 500 line items — the alert body itself will
        also truncate, but the DB side bounds the transfer size.

        Never raises for a zero-row result: callers iterate the empty
        list and are done. Raises :class:`ValueError` only for
        invalid bounds (threshold_hours <= 0, limit <= 0).
        """
        if threshold_hours <= 0:
            raise ValueError("threshold_hours must be positive")
        if limit <= 0:
            raise ValueError("limit must be positive")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT transaction_id,
                       telegram_id,
                       gateway,
                       currency_used,
                       amount_usd_credited,
                       gateway_invoice_id,
                       created_at,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600.0
                           AS age_hours
                FROM transactions
                WHERE status = 'PENDING'
                  AND created_at < NOW() - ($1 || ' hours')::interval
                ORDER BY created_at
                LIMIT $2
                """,
                str(int(threshold_hours)),
                int(limit),
            )
        return [
            {
                "transaction_id": int(r["transaction_id"]),
                "telegram_id": (
                    int(r["telegram_id"])
                    if r["telegram_id"] is not None
                    else None
                ),
                "gateway": r["gateway"],
                "currency_used": r["currency_used"],
                "amount_usd_credited": (
                    float(r["amount_usd_credited"])
                    if r["amount_usd_credited"] is not None
                    else 0.0
                ),
                "gateway_invoice_id": r["gateway_invoice_id"],
                "created_at": (
                    r["created_at"].isoformat()
                    if r["created_at"] is not None
                    else None
                ),
                "age_hours": (
                    float(r["age_hours"])
                    if r["age_hours"] is not None
                    else 0.0
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
                # Stage-13-Step-C: invitee's first paid top-up
                # unlocks the referral bonus for both sides.
                # ``_grant_referral_in_tx`` no-ops if there's no
                # pending grant for this user, so this is safe to
                # call on every credit. Inside the same TX as the
                # wallet credit so a crash mid-flow either commits
                # both legs or rolls back both — critical because
                # the grant row's ``status='PAID'`` flip is the
                # idempotency key against IPN replays.
                referral_credit = None
                if delta > 0:
                    from referral import (
                        grant_referral_after_credit as _grant_after,
                    )
                    referral_credit = await _grant_after(
                        self,
                        connection,
                        invitee_telegram_id=row["telegram_id"],
                        amount_usd=delta,
                        transaction_id=None,
                    )
                return {
                    "telegram_id": row["telegram_id"],
                    "currency_used": row["currency_used"],
                    "amount_usd_credited": new_credited,
                    "delta_credited": delta,
                    "referral_credit": referral_credit,
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
                # Stage-13-Step-C: same hook
                # ``finalize_partial_payment`` calls. ``delta`` is the
                # USD that just landed in the wallet on this exact
                # transition (excluding the promo bonus, which is
                # NOT a real top-up — referral economics need to
                # track actually-paid value, not gifts on top).
                # ``transaction_id`` available here from the FOR
                # UPDATE row, so we can pin the grant to its
                # triggering transaction in the audit trail.
                referral_credit = None
                if delta > 0:
                    from referral import (
                        grant_referral_after_credit as _grant_after,
                    )
                    referral_credit = await _grant_after(
                        self,
                        connection,
                        invitee_telegram_id=row["telegram_id"],
                        amount_usd=delta,
                        transaction_id=row["transaction_id"],
                    )
                return {
                    "telegram_id": row["telegram_id"],
                    "amount_usd_credited": new_credited,
                    "delta_credited": delta,
                    "promo_bonus_credited": bonus_credited,
                    "referral_credit": referral_credit,
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

    async def get_gift_code(self, code: str) -> dict | None:
        """Fetch one gift code by *code* (uppercased on lookup).

        Returns the same dict shape as :meth:`list_gift_codes` rows,
        or ``None`` if no row matches. Used by Stage-12-Step-D's
        ``/admin/gifts/{code}/redemptions`` browser to render the
        per-code header (amount + status + cap) above the
        per-redemption table; cheap PK lookup.
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT code, amount_usd, max_uses, used_count,
                       expires_at, is_active, created_at
                FROM gift_codes
                WHERE code = $1
                """,
                code.upper(),
            )
        if row is None:
            return None
        return {
            "code": row["code"],
            "amount_usd": float(row["amount_usd"]),
            "max_uses": (
                int(row["max_uses"]) if row["max_uses"] is not None else None
            ),
            "used_count": int(row["used_count"]),
            "expires_at": (
                row["expires_at"].isoformat()
                if row["expires_at"] is not None else None
            ),
            "is_active": bool(row["is_active"]),
            "created_at": (
                row["created_at"].isoformat()
                if row["created_at"] is not None else None
            ),
        }

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

    # ------------------------------------------------------------------
    # Stage-12-Step-D: paginated per-code redemption drilldown.
    # ``get_gift_redemptions`` above is the legacy unpaginated helper
    # used by the Telegram-side ``/admin_gift_status`` slash command;
    # we keep it for backward compat. ``list_gift_code_redemptions``
    # below is the new paginated companion for the web admin browser.
    # ------------------------------------------------------------------
    GIFT_REDEMPTIONS_MAX_PER_PAGE: int = 200

    async def list_gift_code_redemptions(
        self,
        *,
        code: str,
        page: int = 1,
        per_page: int = 50,
    ) -> dict:
        """Stage-12-Step-D: paginated per-code redemption browser.

        Backs ``GET /admin/gifts/{code}/redemptions`` — for one code,
        list everybody who redeemed it (telegram_id + username) plus
        the per-redemption USD figure (joined from ``transactions``)
        and the linked transaction id. Indexed by the new
        ``idx_gift_redemptions_code_redeemed_at`` (alembic 0013) —
        without that index the query would hit the ``(code, telegram_id)``
        PK partition and then sort the partition in memory by time,
        which scales poorly once a code has thousands of redemptions.

        ``per_page`` is clamped to ``[1, GIFT_REDEMPTIONS_MAX_PER_PAGE]``;
        ``page`` to ``>= 1``. Sort is ``redeemed_at DESC`` — the index
        is built in that direction so a forward scan returns rows in
        display order without an extra sort step. ``code`` is
        upper-cased to match the ``gift_codes.code`` storage convention.

        ``transaction_id`` is nullable on the schema (alembic 0003 sets
        ``ON DELETE SET NULL`` so a manual ``transactions`` cleanup
        doesn't cascade-delete the redemption record). When non-null
        we LEFT JOIN ``transactions`` to surface the
        ``amount_usd_credited`` actually credited to the user, which
        is what the admin wants to see — *not* the gift_codes row's
        amount_usd at the time of the page render (a code can be
        edited / re-priced after redemptions land, but the credit
        landed at the original price). For NULL transaction_id rows
        (orphaned redemption — the underlying transaction row was
        cleaned up) ``amount_usd_credited`` falls back to ``None``;
        the template renders that as a dash.

        Returns::

            {
              "rows": [
                {"telegram_id": int, "username": str | None,
                 "redeemed_at": iso str | None,
                 "transaction_id": int | None,
                 "amount_usd_credited": float | None},
                ...
              ],
              "total": int, "page": int, "per_page": int,
              "total_pages": int,
            }
        """
        per_page = max(
            1, min(int(per_page), self.GIFT_REDEMPTIONS_MAX_PER_PAGE)
        )
        page = max(1, int(page))
        code_upper = code.upper()

        async with self.pool.acquire() as connection:
            total = await connection.fetchval(
                "SELECT COUNT(*) FROM gift_redemptions WHERE code = $1",
                code_upper,
            )
            rows = await connection.fetch(
                """
                SELECT r.telegram_id,
                       r.redeemed_at,
                       r.transaction_id,
                       u.username,
                       t.amount_usd_credited
                FROM gift_redemptions r
                LEFT JOIN users u ON u.telegram_id = r.telegram_id
                LEFT JOIN transactions t
                       ON t.transaction_id = r.transaction_id
                WHERE r.code = $1
                ORDER BY r.redeemed_at DESC
                LIMIT $2 OFFSET $3
                """,
                code_upper, per_page, (page - 1) * per_page,
            )

        total = int(total or 0)
        total_pages = (total + per_page - 1) // per_page
        return {
            "rows": [
                {
                    "telegram_id": int(r["telegram_id"]),
                    "username": r["username"],
                    "redeemed_at": (
                        r["redeemed_at"].isoformat()
                        if r["redeemed_at"] is not None else None
                    ),
                    "transaction_id": (
                        int(r["transaction_id"])
                        if r["transaction_id"] is not None else None
                    ),
                    "amount_usd_credited": (
                        float(r["amount_usd_credited"])
                        if r["amount_usd_credited"] is not None else None
                    ),
                }
                for r in rows
            ],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }

    async def get_gift_code_redemption_aggregates(
        self, code: str
    ) -> dict:
        """Stage-12-Step-D: lightweight aggregates rendered above the
        per-code redemption table.

        Returns::

            {"total_redemptions": int,
             "total_credited_usd": float,
             "first_redeemed_at": iso str | None,
             "last_redeemed_at": iso str | None}

        ``total_credited_usd`` sums the *actual credited* USD figures
        from the linked ``transactions`` rows (mirrors the row-level
        accuracy described on :meth:`list_gift_code_redemptions`),
        falling back to 0 for NULL ``transaction_id`` rows.

        Cheap on the new index — three aggregate functions in one
        query, full per-code partition scan but indexed.
        """
        code_upper = code.upper()
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT COUNT(*)                           AS n,
                       COALESCE(SUM(t.amount_usd_credited), 0) AS sum_usd,
                       MIN(r.redeemed_at)                 AS first_at,
                       MAX(r.redeemed_at)                 AS last_at
                FROM gift_redemptions r
                LEFT JOIN transactions t
                       ON t.transaction_id = r.transaction_id
                WHERE r.code = $1
                """,
                code_upper,
            )
        if row is None:
            return {
                "total_redemptions": 0,
                "total_credited_usd": 0.0,
                "first_redeemed_at": None,
                "last_redeemed_at": None,
            }
        return {
            "total_redemptions": int(row["n"] or 0),
            "total_credited_usd": float(row["sum_usd"] or 0),
            "first_redeemed_at": (
                row["first_at"].isoformat()
                if row["first_at"] is not None else None
            ),
            "last_redeemed_at": (
                row["last_at"].isoformat()
                if row["last_at"] is not None else None
            ),
        }

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
        {"nowpayments", "tetrapay", "admin", "gift"}
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

    # Stage-12-Step-C: per-user paginated receipts surfaced via the
    # bot's /wallet menu. Status whitelist is the *user-visible*
    # set — PENDING / EXPIRED / FAILED rows are kept off the user
    # surface (a stuck PENDING is operational state, not a receipt;
    # an EXPIRED row is "you didn't actually pay"; a FAILED row is
    # an internal error). REFUNDED is included because the user
    # *should* see "we refunded you on date X" — otherwise the
    # refund is invisible to the only person who cares about it.
    USER_RECEIPTS_STATUS_VALUES: frozenset[str] = frozenset(
        {"SUCCESS", "PARTIAL", "REFUNDED"}
    )
    # Hard cap on how many receipts a single page can return.
    # Protects against a malicious / typo'd ``limit=999999`` query
    # on the user surface independently of the env-driven default.
    USER_RECEIPTS_MAX_PER_PAGE: int = 20

    async def list_user_transactions(
        self,
        *,
        telegram_id: int,
        limit: int = 5,
        before_id: int | None = None,
    ) -> dict:
        """Stage-12-Step-C: per-user receipt feed.

        Hard-coded ``telegram_id`` filter — this method exists
        specifically so a future caller on the user-side surface
        can't accidentally drop the ``WHERE telegram_id = …`` clause
        and expose somebody else's transactions. ``telegram_id`` is
        positional-only via the keyword form, must be an integer,
        and must be > 0; we ``raise ValueError`` rather than
        silently returning everything (or nothing) for a missing /
        zero / negative value, so the bug surfaces at the call site.

        Status filter is locked to :attr:`USER_RECEIPTS_STATUS_VALUES`
        — PENDING / EXPIRED / FAILED are not user-visible (a PENDING
        row is operational state, not a paid receipt). The caller
        cannot expand or restrict the set; receipts are always the
        same shape regardless of where they're rendered.

        Cursor pagination via ``before_id`` (returns rows with
        ``transaction_id < before_id``). We deliberately don't use
        page/offset semantics: a new top-up landing while the user
        is browsing would shift every page and surface duplicates.
        Cursor pagination is stable.

        Returns::

            {
              "rows": [
                {"id": int, "gateway": str, "currency": str,
                 "amount_crypto_or_rial": float | None,
                 "amount_usd": float, "status": str,
                 "gateway_invoice_id": str | None,
                 "created_at": iso str | None,
                 "completed_at": iso str | None,
                 "refunded_at": iso str | None,
                 "gateway_locked_rate_toman_per_usd": float | None},
                ...
              ],
              "has_more": bool,    # True iff a next page exists
              "next_before_id": int | None,  # cursor for the next page
            }

        ``next_before_id`` is the smallest ``id`` in the current
        page (so the caller doesn't recompute it). ``None`` when
        ``has_more`` is False.
        """
        if not isinstance(telegram_id, int) or telegram_id <= 0:
            # Refuse silent zero/missing filter — the whole point of
            # this method is to *guarantee* the user filter is
            # present. A buggy caller passing 0 or None must crash
            # loudly, not list everything.
            raise ValueError(
                "telegram_id is required and must be a positive integer; "
                f"got {telegram_id!r}"
            )
        limit = max(1, min(int(limit), self.USER_RECEIPTS_MAX_PER_PAGE))
        if before_id is not None:
            if not isinstance(before_id, int) or before_id <= 0:
                raise ValueError(
                    "before_id must be a positive integer or None; "
                    f"got {before_id!r}"
                )

        # ANY($2::text[]) is the canonical asyncpg pattern for an IN
        # over a fixed enum without splatting per-value placeholders.
        # Sorted for deterministic test pinning.
        statuses = sorted(self.USER_RECEIPTS_STATUS_VALUES)

        # We fetch ``limit + 1`` rows: if the (limit+1)-th row exists,
        # there's a next page; otherwise this is the last. The extra
        # row is then trimmed before returning.
        fetch_limit = int(limit) + 1

        if before_id is None:
            sql = """
                SELECT transaction_id, gateway, currency_used,
                       amount_crypto_or_rial, amount_usd_credited,
                       status, gateway_invoice_id,
                       created_at, completed_at, refunded_at,
                       gateway_locked_rate_toman_per_usd
                FROM transactions
                WHERE telegram_id = $1
                  AND status = ANY($2::text[])
                ORDER BY transaction_id DESC
                LIMIT $3
            """
            params = (int(telegram_id), statuses, fetch_limit)
        else:
            sql = """
                SELECT transaction_id, gateway, currency_used,
                       amount_crypto_or_rial, amount_usd_credited,
                       status, gateway_invoice_id,
                       created_at, completed_at, refunded_at,
                       gateway_locked_rate_toman_per_usd
                FROM transactions
                WHERE telegram_id = $1
                  AND status = ANY($2::text[])
                  AND transaction_id < $3
                ORDER BY transaction_id DESC
                LIMIT $4
            """
            params = (
                int(telegram_id), statuses, int(before_id), fetch_limit,
            )

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(sql, *params)

        has_more = len(rows) > limit
        page_rows = list(rows[:limit])
        next_before_id: int | None = None
        if has_more and page_rows:
            # Last (oldest-shown) row's id is the next-page cursor.
            next_before_id = int(page_rows[-1]["transaction_id"])

        def _normalise(r) -> dict:
            return {
                "id": int(r["transaction_id"]),
                "gateway": r["gateway"],
                "currency": r["currency_used"],
                "amount_crypto_or_rial": (
                    float(r["amount_crypto_or_rial"])
                    if r["amount_crypto_or_rial"] is not None else None
                ),
                "amount_usd": float(r["amount_usd_credited"] or 0),
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
                "refunded_at": (
                    r["refunded_at"].isoformat()
                    if r["refunded_at"] is not None else None
                ),
                "gateway_locked_rate_toman_per_usd": (
                    float(r["gateway_locked_rate_toman_per_usd"])
                    if r["gateway_locked_rate_toman_per_usd"] is not None
                    else None
                ),
            }

        return {
            "rows": [_normalise(r) for r in page_rows],
            "has_more": has_more,
            "next_before_id": next_before_id,
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

    # ------------------------------------------------------------------
    # Stage-15-Step-E #2: per-user spending analytics (first slice).
    #
    # The admin-side ``get_system_metrics`` already aggregates global
    # spend + top models for the dashboard tile; this method is the
    # per-user counterpart for the new ``hub_stats`` wallet sub-screen.
    # Single connection round-trip with three fetches against the
    # ``usage_logs`` index — same shape as ``get_system_metrics`` so a
    # future caller building a richer "stats v2" surface (per-day
    # graphs, weekly bars) can extend the same dict without breaking
    # the first-slice consumer.
    # ------------------------------------------------------------------
    USER_STATS_TOP_MODELS_LIMIT: int = 5
    USER_STATS_WINDOW_DAYS_DEFAULT: int = 30
    USER_STATS_WINDOW_DAYS_MAX: int = 365

    async def get_user_spending_summary(
        self,
        telegram_id: int,
        *,
        window_days: int | None = None,
        top_models_limit: int | None = None,
    ) -> dict:
        """Stage-15-Step-E #2: per-user spending dashboard data.

        Returns the snapshot rendered by the new ``hub_stats``
        wallet sub-screen — lifetime totals, a recent-window total,
        and the user's top models by call count over the same
        window. Shape::

            {
              "lifetime": {
                "total_calls": int,
                "total_tokens": int,
                "total_cost_usd": float,
              },
              "window_days": int,
              "window": {
                "total_calls": int,
                "total_tokens": int,
                "total_cost_usd": float,
              },
              "top_models": [
                {"model": str, "calls": int, "cost_usd": float},
                ...  # up to ``top_models_limit`` rows
              ],
            }

        Hard-codes the ``WHERE telegram_id = …`` filter on every
        sub-query — same defensive shape as
        :meth:`list_user_transactions`. A buggy caller passing a
        non-positive id raises ``ValueError`` rather than silently
        returning everyone's data.

        ``window_days`` defaults to
        :attr:`USER_STATS_WINDOW_DAYS_DEFAULT` (30) and is clamped
        to ``[1, USER_STATS_WINDOW_DAYS_MAX]``. The "window"
        bucket is the most-actionable horizon for a user
        wondering "how much did I spend recently?" — lifetime is
        kept separate so a fresh-but-active user doesn't see
        their first month's spend reported as both numbers.

        ``top_models_limit`` defaults to
        :attr:`USER_STATS_TOP_MODELS_LIMIT` (5) and is clamped to
        ``[1, USER_STATS_TOP_MODELS_LIMIT]`` — a Telegram message
        is the render target so a long list isn't useful.

        Empty-data case: a user with zero ``usage_logs`` rows
        gets all-zero scalars + an empty ``top_models`` list.
        """
        if not isinstance(telegram_id, int) or telegram_id <= 0:
            raise ValueError(
                "telegram_id is required and must be a positive integer; "
                f"got {telegram_id!r}"
            )
        if window_days is None:
            window_days = self.USER_STATS_WINDOW_DAYS_DEFAULT
        window_days = max(
            1, min(int(window_days), self.USER_STATS_WINDOW_DAYS_MAX)
        )
        if top_models_limit is None:
            top_models_limit = self.USER_STATS_TOP_MODELS_LIMIT
        top_models_limit = max(
            1, min(int(top_models_limit), self.USER_STATS_TOP_MODELS_LIMIT)
        )
        tid = int(telegram_id)

        # Single ``acquire`` for all three reads — they're snapshot-
        # style aggregates and the admin-dashboard pattern in
        # ``get_system_metrics`` already does the same. Slight drift
        # between the queries (a row landing mid-fetch) is tolerable
        # for a stats screen.
        async with self.pool.acquire() as connection:
            lifetime = await connection.fetchrow(
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
            window = await connection.fetchrow(
                """
                SELECT COUNT(*) AS calls,
                       COALESCE(SUM(prompt_tokens + completion_tokens), 0)
                           AS tokens,
                       COALESCE(SUM(cost_deducted_usd), 0) AS cost
                FROM usage_logs
                WHERE telegram_id = $1
                  AND created_at >= NOW() - ($2 || ' days')::interval
                """,
                tid,
                str(int(window_days)),
            )
            top_rows = await connection.fetch(
                """
                SELECT model_used AS model,
                       COUNT(*)::int AS calls,
                       COALESCE(SUM(cost_deducted_usd), 0) AS cost
                FROM usage_logs
                WHERE telegram_id = $1
                  AND created_at >= NOW() - ($2 || ' days')::interval
                GROUP BY model_used
                ORDER BY calls DESC, cost DESC
                LIMIT $3
                """,
                tid,
                str(int(window_days)),
                top_models_limit,
            )

        def _agg_row(row) -> dict:
            if row is None:
                return {
                    "total_calls": 0,
                    "total_tokens": 0,
                    "total_cost_usd": 0.0,
                }
            return {
                "total_calls": int(row["calls"] or 0),
                "total_tokens": int(row["tokens"] or 0),
                "total_cost_usd": float(row["cost"] or 0),
            }

        return {
            "lifetime": _agg_row(lifetime),
            "window_days": window_days,
            "window": _agg_row(window),
            "top_models": [
                {
                    "model": r["model"],
                    "calls": int(r["calls"]),
                    "cost_usd": float(r["cost"] or 0),
                }
                for r in top_rows
            ],
        }

    async def get_system_metrics(
        self, *, pending_alert_threshold_hours: int = 2
    ) -> dict:
        """Aggregate counters for the admin metrics panel.

        Single round trip via ``acquire`` → multiple ``fetch*`` calls
        on the same connection. We don't bother wrapping in a
        transaction because the values are all snapshot-style and
        slight drift between the queries is tolerable for an admin
        dashboard.

        ``pending_alert_threshold_hours`` defines the boundary for
        the Stage-12-Step-B "overdue" counter so the dashboard tile
        and the proactive :func:`pending_alert.run_pending_alert_pass`
        DM stay in sync — an admin who sees "3 overdue" on the
        dashboard at 15:00 and a DM at 15:30 saying "3 overdue" should
        be seeing the same underlying set of rows, not two drifting
        numbers computed from two differently-defined thresholds.
        Default mirrors ``PENDING_ALERT_THRESHOLD_HOURS``'s env
        default (2 h).

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
              "pending_payments_over_threshold_count": int,       # Stage-12-Step-B — PENDING rows older than pending_alert_threshold_hours
              "pending_alert_threshold_hours":        int,        # Stage-12-Step-B — the threshold the above count was computed against
            }
        """
        if pending_alert_threshold_hours <= 0:
            raise ValueError(
                "pending_alert_threshold_hours must be positive"
            )
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
                        AS oldest_age_hours,
                    COUNT(*) FILTER (
                        WHERE created_at
                              < NOW() - ($1 || ' hours')::interval
                    )::int AS over_threshold_count
                FROM transactions
                WHERE status = 'PENDING'
                """,
                str(int(pending_alert_threshold_hours)),
            )
        # ``MIN(created_at)`` is NULL when zero PENDING rows exist;
        # ``EXTRACT(EPOCH FROM (NOW() - NULL))`` is NULL too, so the
        # branch surfaces ``None`` cleanly without a second query.
        pending_count = int(pending_row["count"] or 0) if pending_row else 0
        oldest_age_raw = pending_row["oldest_age_hours"] if pending_row else None
        pending_oldest_age_hours = (
            float(oldest_age_raw) if oldest_age_raw is not None else None
        )
        pending_over_threshold = (
            int(pending_row["over_threshold_count"] or 0)
            if pending_row
            else 0
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
            # Stage-12-Step-B: the "overdue" count — PENDING rows
            # older than the alert threshold the caller cares about.
            # Shipped together with the proactive admin-DM loop so
            # the dashboard tile and the DM body reference the same
            # number, not one computed from MIN(created_at) and the
            # other from a COUNT(…) OVER threshold.
            "pending_payments_over_threshold_count": pending_over_threshold,
            "pending_alert_threshold_hours": int(
                pending_alert_threshold_hours
            ),
        }

    async def get_monetization_summary(
        self,
        *,
        window_days: int = 30,
        top_models_limit: int = 10,
    ) -> dict:
        """Stage-15-Step-E #9 — bot monetization rollup.

        ``get_system_metrics`` already exposes raw revenue (gateway
        top-ups) and raw spend (sum of ``cost_deducted_usd`` from
        ``usage_logs``), but a single number doesn't tell the operator
        whether the markup is paying for OpenRouter calls. This method
        is the dedicated "is the bot profitable?" rollup the
        ``/admin/monetization`` page renders.

        Two scopes computed in a single round trip:

        * **Lifetime** — ``revenue_usd_total`` (gateway-only, mirrors
          ``get_system_metrics`` filter: NowPayments / TetraPay /
          Zarinpal etc., NOT ``admin`` / ``gift``), and
          ``charged_usd_total`` (sum of ``cost_deducted_usd`` across
          ALL of ``usage_logs``).
        * **Window** — same two figures over the trailing
          ``window_days`` (default 30 — the same horizon
          ``get_system_metrics`` uses for the top-models tile, so the
          "Top models" panel and this one stay aligned).

        For each scope we derive the OpenRouter cost via the current
        markup (``charged / markup``). The markup is global — the same
        multiplier applies to every model — so the gross margin
        *percentage* is just ``(markup - 1) / markup`` regardless of
        which model. The interesting figure is the *absolute* margin,
        which scales with usage. The per-model breakdown surfaces
        which models are pulling that margin (a model with 10 calls
        × $0.05 charge each is more profitable than one with 1 call
        × $0.40 even though the percentage is identical).

        Caveats — surfaced in the ``meta`` block of the return value
        so the template can footnote them:

        * The OpenRouter cost is *implied* — derived from the current
          markup, not from the per-call OpenRouter spend. If the
          operator changes ``COST_MARKUP`` mid-deploy the historical
          rows in ``usage_logs`` were charged at a different markup,
          but we apply the *current* markup uniformly. The
          alternative (storing per-row markup or per-row implied
          OpenRouter cost) is a schema change and out of scope for
          this slice; the dashboard footnotes the assumption.
        * Net profit (``revenue - implied_openrouter_cost``) is a
          *forward-looking* figure that assumes every dollar credited
          will eventually be consumed. A user who tops up $20 and
          never sends a prompt has contributed $20 to revenue and
          $0 to spend — net looks great until the credits start
          burning. Operators reading this dashboard should treat net
          as "how much we'd have left if the wallet were drained
          tomorrow at the current markup", not as realised profit.

        Returns a dict shaped like::

            {
              "markup": float,              # current pricing.get_markup()
              "lifetime": {
                "revenue_usd": float,       # gateway-only
                "charged_usd": float,       # SUM cost_deducted_usd
                "openrouter_cost_usd": float,   # charged / markup
                "gross_margin_usd": float,  # charged - openrouter_cost
                "gross_margin_pct": float,  # (markup - 1) / markup * 100; 0 when markup<=1
                "net_profit_usd": float,    # revenue - openrouter_cost
              },
              "window": {
                "days": int,                # echoes window_days
                ...same five figures over the trailing window
              },
              "by_model": [
                {"model": str, "requests": int,
                 "charged_usd": float, "openrouter_cost_usd": float,
                 "gross_margin_usd": float},
                ...  # up to top_models_limit, sorted by charged DESC
              ],
            }

        ``by_model`` rows are sorted by ``charged_usd`` descending —
        we want the biggest-revenue contributors at the top, not the
        most-frequently-called (which ``get_system_metrics.top_models``
        already shows). A model with 1 expensive call beats 1000
        cheap ones for "where is the margin coming from".
        """
        if not isinstance(window_days, int) or window_days <= 0:
            raise ValueError(
                f"window_days must be a positive integer; got {window_days!r}"
            )
        if not isinstance(top_models_limit, int) or top_models_limit <= 0:
            raise ValueError(
                "top_models_limit must be a positive integer; "
                f"got {top_models_limit!r}"
            )

        # Imported lazily to avoid a circular import at module-load
        # time (``pricing`` doesn't import ``database`` today, but
        # the lazy form keeps it that way under future refactors).
        from pricing import get_markup
        markup = float(get_markup())

        # Format the window as a Postgres interval literal. We use
        # ``$N::interval`` rather than string-substitution so a
        # bogus ``window_days`` can't reach the SQL — but the
        # ValueError above already refuses non-positive integers,
        # so this is purely defense-in-depth.
        window_interval = f"{int(window_days)} days"

        async with self.pool.acquire() as connection:
            revenue_total = await connection.fetchval(
                """
                SELECT COALESCE(SUM(amount_usd_credited), 0)
                FROM transactions
                WHERE status IN ('SUCCESS', 'PARTIAL')
                  AND gateway NOT IN ('admin', 'gift')
                """
            )
            charged_total = await connection.fetchval(
                "SELECT COALESCE(SUM(cost_deducted_usd), 0) FROM usage_logs"
            )
            revenue_window = await connection.fetchval(
                """
                SELECT COALESCE(SUM(amount_usd_credited), 0)
                FROM transactions
                WHERE status IN ('SUCCESS', 'PARTIAL')
                  AND gateway NOT IN ('admin', 'gift')
                  AND COALESCE(completed_at, created_at)
                      >= NOW() - $1::interval
                """,
                window_interval,
            )
            charged_window = await connection.fetchval(
                """
                SELECT COALESCE(SUM(cost_deducted_usd), 0)
                FROM usage_logs
                WHERE created_at >= NOW() - $1::interval
                """,
                window_interval,
            )
            by_model_rows = await connection.fetch(
                """
                SELECT model_used AS model,
                       COUNT(*)::int AS requests,
                       COALESCE(SUM(cost_deducted_usd), 0) AS charged_usd
                FROM usage_logs
                WHERE created_at >= NOW() - $1::interval
                GROUP BY model_used
                ORDER BY charged_usd DESC
                LIMIT $2
                """,
                window_interval,
                int(top_models_limit),
            )

        revenue_total_f = float(revenue_total or 0)
        charged_total_f = float(charged_total or 0)
        revenue_window_f = float(revenue_window or 0)
        charged_window_f = float(charged_window or 0)

        # ``markup <= 1`` means "no profit assumed" — we still want
        # a numeric answer rather than NaN / div-by-zero. Per
        # ``pricing.get_markup`` the value is clamped to >= 1.0
        # before it reaches us, but markup == 1.0 (== "no markup")
        # is a legitimate config (the operator could be running at
        # cost). In that case OpenRouter cost == charged, margin is
        # zero, and the percentage is zero. ``markup_for_div`` keeps
        # the math defined under both branches.
        markup_for_div = markup if markup >= 1.0 else 1.0
        openrouter_cost_total = charged_total_f / markup_for_div
        openrouter_cost_window = charged_window_f / markup_for_div
        gross_margin_total = charged_total_f - openrouter_cost_total
        gross_margin_window = charged_window_f - openrouter_cost_window
        if markup_for_div > 1.0:
            gross_margin_pct = (markup_for_div - 1.0) / markup_for_div * 100.0
        else:
            gross_margin_pct = 0.0

        by_model: list[dict] = []
        for row in by_model_rows:
            charged = float(row["charged_usd"] or 0)
            or_cost = charged / markup_for_div
            by_model.append(
                {
                    "model": row["model"],
                    "requests": int(row["requests"] or 0),
                    "charged_usd": charged,
                    "openrouter_cost_usd": or_cost,
                    "gross_margin_usd": charged - or_cost,
                }
            )

        return {
            "markup": markup,
            "lifetime": {
                "revenue_usd": revenue_total_f,
                "charged_usd": charged_total_f,
                "openrouter_cost_usd": openrouter_cost_total,
                "gross_margin_usd": gross_margin_total,
                "gross_margin_pct": gross_margin_pct,
                "net_profit_usd": revenue_total_f - openrouter_cost_total,
            },
            "window": {
                "days": int(window_days),
                "revenue_usd": revenue_window_f,
                "charged_usd": charged_window_f,
                "openrouter_cost_usd": openrouter_cost_window,
                "gross_margin_usd": gross_margin_window,
                "gross_margin_pct": gross_margin_pct,
                "net_profit_usd": revenue_window_f - openrouter_cost_window,
            },
            "by_model": by_model,
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
        for /admin_* slash commands or ``"web"`` for browser edits.

        Bug fix bundle (Stage-15-Step-E #7 follow-up #2): strip
        NUL bytes from *value* and *updated_by* before insertion.
        Postgres ``TEXT`` rejects NUL with
        ``invalid byte sequence for encoding "UTF8": 0x00`` which
        crashes the upsert and bubbles up to the caller. Pre-fix,
        the new ``i18n_po import`` CLI would crash mid-batch on a
        translator's ``.po`` containing a stray NUL (some Crowdin
        export pipelines emit them inside multi-line msgstrs); the
        web admin editor would 500 on the same input. The
        defensive strip is consistent with
        :meth:`set_admin_role`'s NUL-byte handling for the
        ``notes`` column.
        """
        if "\x00" in value:
            log.warning(
                "upsert_string_override: stripped NUL byte from value "
                "for %s:%s (likely artifact from translator's editor)",
                lang, key,
            )
            value = value.replace("\x00", "")
        if updated_by is not None and "\x00" in updated_by:
            log.warning(
                "upsert_string_override: stripped NUL byte from "
                "updated_by tag for %s:%s",
                lang, key,
            )
            updated_by = updated_by.replace("\x00", "")
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
                "meta": _decode_jsonb_meta(r["meta"]),
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
                "meta": _decode_jsonb_meta(r["meta"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Stage-15-Step-E #5: admin role primitives
    # ------------------------------------------------------------------
    #
    # Schema lives in alembic ``0016_admin_roles.py``. The table has a
    # CHECK constraint on the role column so a buggy caller that reaches
    # past this allow-list still can't poison a row.

    ADMIN_ROLE_VALUES: frozenset[str] = frozenset(
        {"viewer", "operator", "super"}
    )

    async def get_admin_role(self, telegram_id: int) -> str | None:
        """Return the DB-tracked role for *telegram_id* or ``None`` if
        the user has no row in ``admin_roles``.

        Does NOT consult ``ADMIN_USER_IDS``: backward-compat fallback
        to env-list admins is the caller's job (see
        :func:`admin_roles.effective_role`) so the helper stays
        honest about what's in the DB vs what's inferred from env.
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT role FROM admin_roles WHERE telegram_id = $1",
                int(telegram_id),
            )
        return row["role"] if row is not None else None

    async def set_admin_role(
        self,
        telegram_id: int,
        role: str,
        *,
        granted_by: int | None = None,
        notes: str | None = None,
    ) -> str:
        """UPSERT a role row. Returns the role string that was stored.

        Validates *role* against :data:`ADMIN_ROLE_VALUES` *before*
        hitting the DB so a typo gets a clean ``ValueError`` with the
        offending value rather than the asyncpg
        ``CheckViolationError`` that the SQL CHECK would raise on the
        wire (which is harder for upstream callers to discriminate
        from a transient DB error).

        The ``granted_at`` column is reset to ``NOW()`` on every
        UPSERT so the value always reflects the most recent change —
        that keeps the audit trail / list view honest about when the
        role last moved, which matters more than preserving the
        first-grant timestamp.
        """
        normalized = role.strip().lower() if isinstance(role, str) else ""
        if normalized not in self.ADMIN_ROLE_VALUES:
            raise ValueError(
                f"role must be one of {sorted(self.ADMIN_ROLE_VALUES)}; "
                f"got {role!r}"
            )
        # Strip U+0000 NUL bytes from ``notes`` before INSERT. Postgres
        # TEXT rejects ``\x00`` outright with ``invalid byte sequence
        # for encoding "UTF8": 0x00`` — same regression class
        # ``append_conversation_message`` documented in
        # Stage-15-Step-E #10 (PR #128). The new ``/admin/roles`` web
        # form exposes ``notes`` as a free-form textarea that an
        # operator might paste binary-y content into; strip-and-warn
        # at the DB layer so the audit trail keeps the rest of the
        # note text instead of demoting the whole grant to a
        # misleading "DB write failed" error. Telegram is the other
        # surface (the ``/admin_role_grant`` handler routes notes
        # through here too) — same fix protects both.
        clean_notes = notes
        if isinstance(notes, str) and "\x00" in notes:
            clean_notes = notes.replace("\x00", "")
            log.warning(
                "set_admin_role: stripping %d NUL byte(s) from notes "
                "for telegram_id=%d (Postgres TEXT rejects \\x00); "
                "preserving the rest of the text",
                notes.count("\x00"), int(telegram_id),
            )
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO admin_roles
                    (telegram_id, role, granted_by, notes, granted_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (telegram_id) DO UPDATE
                  SET role       = EXCLUDED.role,
                      granted_by = EXCLUDED.granted_by,
                      notes      = EXCLUDED.notes,
                      granted_at = NOW()
                """,
                int(telegram_id),
                normalized,
                int(granted_by) if granted_by is not None else None,
                clean_notes,
            )
        return normalized

    async def delete_admin_role(self, telegram_id: int) -> bool:
        """Remove the DB-tracked role row for *telegram_id*.

        Returns ``True`` iff a row was deleted. ``False`` for a
        not-found target lets the caller distinguish "you revoked a
        role" from "that user wasn't in the table" without a second
        round-trip.
        """
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                "DELETE FROM admin_roles WHERE telegram_id = $1",
                int(telegram_id),
            )
        # asyncpg's `execute` returns the command tag, e.g. "DELETE 1".
        try:
            tag, _, count = result.partition(" ")
            return tag == "DELETE" and int(count) >= 1
        except (ValueError, AttributeError):
            return False

    async def list_admin_roles(
        self, *, limit: int = 200,
    ) -> list[dict]:
        """Return every DB-tracked admin role, newest grants first.

        Caller-side sort key: ``granted_at DESC`` so the freshest
        changes float to the top of any /admin_role_list output.
        Limited to *limit* rows (clamped [1..1000]) so a future
        regression that floods the table can't OOM the formatter.
        """
        capped = max(1, min(int(limit), 1000))
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT telegram_id, role, granted_at, granted_by, notes
                  FROM admin_roles
                 ORDER BY granted_at DESC, telegram_id ASC
                 LIMIT $1
                """,
                capped,
            )
        return [
            {
                "telegram_id": int(r["telegram_id"]),
                "role": r["role"],
                "granted_at": (
                    r["granted_at"].isoformat()
                    if r["granted_at"] is not None
                    else None
                ),
                "granted_by": (
                    int(r["granted_by"])
                    if r["granted_by"] is not None
                    else None
                ),
                "notes": r["notes"],
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

    async def get_model_prices(self) -> dict[str, tuple[float, float]]:
        """Return the last-known OpenRouter prices per model.

        Result is ``{model_id: (input_per_1m_usd, output_per_1m_usd)}``.
        Empty dict on first run before the discovery loop has written
        anything. Used by :mod:`model_discovery` to diff against the
        live catalog and raise alerts when any side moves by more than
        ``PRICE_ALERT_THRESHOLD_PERCENT``.
        """
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT model_id, input_per_1m_usd, output_per_1m_usd
                  FROM model_prices
                """
            )
        return {
            row["model_id"]: (
                float(row["input_per_1m_usd"]),
                float(row["output_per_1m_usd"]),
            )
            for row in rows
        }

    async def upsert_model_prices(
        self, prices: dict[str, tuple[float, float]]
    ) -> int:
        """Bulk upsert the given prices into ``model_prices``.

        ``prices`` maps model_id to ``(input_per_1m_usd,
        output_per_1m_usd)``. Uses a single INSERT … ON CONFLICT DO
        UPDATE so we don't pay N round-trips for a 200-model catalog.
        Always bumps ``last_seen_at`` to ``NOW()`` so the operator can
        eyeball "when did we last observe this model's price" even if
        the value didn't move.

        Returns the number of rows processed (for test / log
        observability — ``None`` -> empty input short-circuits).
        """
        if not prices:
            return 0
        model_ids: list[str] = []
        input_prices: list[float] = []
        output_prices: list[float] = []
        for model_id, (in_price, out_price) in prices.items():
            model_ids.append(model_id)
            input_prices.append(float(in_price))
            output_prices.append(float(out_price))
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO model_prices (
                    model_id, input_per_1m_usd, output_per_1m_usd, last_seen_at
                )
                SELECT
                    unnest($1::text[]),
                    unnest($2::double precision[]),
                    unnest($3::double precision[]),
                    NOW()
                ON CONFLICT (model_id) DO UPDATE
                   SET input_per_1m_usd  = EXCLUDED.input_per_1m_usd,
                       output_per_1m_usd = EXCLUDED.output_per_1m_usd,
                       last_seen_at      = EXCLUDED.last_seen_at
                """,
                model_ids,
                input_prices,
                output_prices,
            )
        return len(model_ids)

    async def get_fx_snapshot(self) -> tuple[float, "datetime.datetime"] | None:
        """Return the single-row FX snapshot as ``(toman_per_usd,
        fetched_at)`` or ``None`` if the table is empty (first boot
        before any refresh).

        Used by :mod:`fx_rates` to warm the in-memory cache on
        process start so the wallet UI and top-up path have a rate
        to convert with before the first refresh completes.
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT toman_per_usd, fetched_at FROM fx_rates_snapshot WHERE id = 1"
            )
        if row is None:
            return None
        return (float(row["toman_per_usd"]), row["fetched_at"])

    async def upsert_fx_snapshot(
        self, *, toman_per_usd: float, source: str
    ) -> None:
        """Overwrite the single-row FX snapshot.

        We only track "the latest value", so the table is keyed by
        the literal ``id=1`` and every refresh upserts that row.
        ``ON CONFLICT DO UPDATE`` so the very first write (fresh
        migration, no row yet) and all subsequent updates go through
        the same SQL — saves a row-exists probe.
        """
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO fx_rates_snapshot (id, toman_per_usd, source, fetched_at)
                     VALUES (1, $1, $2, NOW())
                ON CONFLICT (id) DO UPDATE
                   SET toman_per_usd = EXCLUDED.toman_per_usd,
                       source        = EXCLUDED.source,
                       fetched_at    = EXCLUDED.fetched_at
                """,
                float(toman_per_usd),
                str(source),
            )

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

    # ------------------------------------------------------------------
    # Stage-13-Step-C: referral codes
    # ------------------------------------------------------------------
    #
    # Two tables, one verb each:
    #
    # * ``get_or_create_referral_code(owner_id)`` — look up the user's
    #   code, generating + storing one on first call. The code is a
    #   short ASCII alphanumeric string the user can comfortably DM
    #   to a friend; collisions are vanishingly rare at the chosen
    #   length but ``ON CONFLICT (code) DO NOTHING`` + retry covers
    #   the worst case.
    #
    # * ``claim_referral(invitee_id, code)`` — invitee taps the
    #   referrer's deep link. Inserts a PENDING grant row; first
    #   referral wins (UNIQUE on ``invitee_telegram_id``), self-
    #   referral and unknown codes return a non-OK status string.
    #   Idempotent: a replay returns the same status the first call
    #   would have, no double-create.
    #
    # * ``_grant_referral_in_tx(connection, invitee_id, amount,
    #   transaction_id)`` — INTERNAL, called from inside the open
    #   ``finalize_payment`` / ``finalize_partial_payment``
    #   transaction the moment the invitee crosses their first paid
    #   credit. Locks the grant row FOR UPDATE so a concurrent IPN
    #   replay can't double-credit; flips PENDING → PAID and credits
    #   both wallets in the same TX as the original payment.
    #
    # No public "list grants" reader yet — that's the next sub-step
    # (a "/wallet → invite stats" view). v1 just wires the credit
    # path; the data is queryable by raw SQL for debugging.

    REFERRAL_CODE_LEN: int = 8
    # ASCII alphanumeric, excluding the visually-ambiguous
    # ``0/O`` / ``1/I/l``. Same alphabet the gift-code generator
    # uses one module over (see ``web_admin.parse_gift_form``).
    REFERRAL_CODE_ALPHABET: str = (
        "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    )

    @classmethod
    def _generate_referral_code(cls) -> str:
        """Random 8-char code from the curated alphabet.

        Not cryptographic — just unique enough at deployment scale
        (32**8 ≈ 1.1e12 codes). The DB ``UNIQUE`` constraint on
        ``code`` is the source of truth; collision retry handled by
        the caller.
        """
        import secrets
        return "".join(
            secrets.choice(cls.REFERRAL_CODE_ALPHABET)
            for _ in range(cls.REFERRAL_CODE_LEN)
        )

    async def get_or_create_referral_code(self, owner_telegram_id: int) -> str:
        """Return the user's referral code, generating + persisting one
        on first call. Idempotent: subsequent calls return the same
        code.

        Race: two parallel "/wallet → invite friend" taps from the
        same user could each generate a fresh code; ``ON CONFLICT
        (owner_telegram_id) DO NOTHING`` lets the loser silently fall
        back to the winner's code on the SELECT-after-insert.

        Collision: the random code might collide with an existing
        row (vanishingly rare). The UNIQUE constraint on ``code``
        rejects the duplicate; we re-roll up to a few times before
        giving up. In practice the first attempt always wins.
        """
        async with self.pool.acquire() as connection:
            existing = await connection.fetchval(
                "SELECT code FROM referral_codes WHERE owner_telegram_id = $1",
                owner_telegram_id,
            )
            if existing:
                return existing
            for _attempt in range(8):
                code = self._generate_referral_code()
                row = await connection.fetchrow(
                    """
                    INSERT INTO referral_codes (owner_telegram_id, code)
                         VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    RETURNING code
                    """,
                    owner_telegram_id,
                    code,
                )
                if row is not None:
                    return row["code"]
                # Two ways the insert can no-op:
                #   1. owner already has a row (lost the race) — read it back.
                #   2. code collided with a different owner — re-roll.
                already = await connection.fetchval(
                    "SELECT code FROM referral_codes "
                    "WHERE owner_telegram_id = $1",
                    owner_telegram_id,
                )
                if already:
                    return already
            raise RuntimeError(
                "could not generate a unique referral code after 8 attempts"
            )

    async def lookup_referral_code(
        self, code: str
    ) -> "asyncpg.Record | None":
        """Resolve a referral code to its owner row, or ``None`` if the
        code doesn't exist. Codes are stored verbatim (mixed case
        possible if a future migration loosens the alphabet); we
        match exactly to avoid a false-positive on a typo'd code.
        """
        if not code or len(code) > 64:
            # Defensive bound — way over the configured length, just
            # so an attacker-supplied huge string can't pin an
            # unindexed scan. The PRIMARY/UNIQUE indexes handle the
            # well-formed lookup.
            return None
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(
                """
                SELECT rc.code, rc.owner_telegram_id, rc.created_at
                  FROM referral_codes rc
                 WHERE rc.code = $1
                """,
                code,
            )

    async def claim_referral(
        self, *, invitee_telegram_id: int, code: str
    ) -> dict:
        """Try to attach *invitee* to the referrer who owns *code*.

        Returns a dict ``{"status": "..."}`` — one of:

        * ``"ok"`` — fresh PENDING grant created (referrer_id in result).
        * ``"unknown"`` — code does not exist.
        * ``"self"`` — owner of the code IS the invitee (self-referral).
        * ``"already_claimed"`` — the invitee already has a grant
          (PENDING or PAID), even from a different code.
        * ``"unknown_invitee"`` — no users row for the invitee
          (shouldn't happen because UserUpsertMiddleware runs first,
          but the FK would raise so we surface a typed error).
        """
        owner_row = await self.lookup_referral_code(code)
        if owner_row is None:
            return {"status": "unknown"}
        owner_id = int(owner_row["owner_telegram_id"])
        if owner_id == invitee_telegram_id:
            return {"status": "self"}
        async with self.pool.acquire() as connection:
            user_exists = await connection.fetchval(
                "SELECT 1 FROM users WHERE telegram_id = $1",
                invitee_telegram_id,
            )
            if not user_exists:
                return {"status": "unknown_invitee"}
            existing = await connection.fetchrow(
                """
                SELECT id, status, referrer_telegram_id
                  FROM referral_grants
                 WHERE invitee_telegram_id = $1
                """,
                invitee_telegram_id,
            )
            if existing is not None:
                return {
                    "status": "already_claimed",
                    "existing_status": existing["status"],
                    "referrer_telegram_id": int(
                        existing["referrer_telegram_id"]
                    ),
                }
            try:
                await connection.execute(
                    """
                    INSERT INTO referral_grants (
                        referrer_telegram_id, invitee_telegram_id,
                        code, status
                    ) VALUES ($1, $2, $3, 'PENDING')
                    """,
                    owner_id,
                    invitee_telegram_id,
                    str(owner_row["code"]),
                )
            except asyncpg.UniqueViolationError:
                # Lost a race against another concurrent claim for
                # the same invitee — surface the same status the
                # winner would have seen on a replay.
                return {"status": "already_claimed"}
            except asyncpg.exceptions.CheckViolationError:
                # Self-referral protection at the DB layer (we already
                # check above; this is defence in depth in case a
                # future caller bypasses the application check).
                return {"status": "self"}
        return {
            "status": "ok",
            "referrer_telegram_id": owner_id,
        }

    @staticmethod
    def compute_referral_bonus(
        amount_usd: float,
        *,
        percent: float,
        max_usd: float,
    ) -> float:
        """Bonus to credit each side. Percentage of the triggering
        top-up, capped at ``max_usd``. Defensive against non-finite
        inputs (returns 0.0)."""
        if not _is_finite_amount(amount_usd) or amount_usd <= 0:
            return 0.0
        if not _is_finite_amount(percent) or percent <= 0:
            return 0.0
        if not _is_finite_amount(max_usd) or max_usd <= 0:
            return 0.0
        bonus = amount_usd * (percent / 100.0)
        if bonus > max_usd:
            bonus = max_usd
        return round(bonus, 4)

    async def _grant_referral_in_tx(
        self,
        connection,
        *,
        invitee_telegram_id: int,
        amount_usd: float,
        transaction_id: int | None,
        bonus_percent: float,
        bonus_max_usd: float,
    ) -> dict | None:
        """If *invitee* has a PENDING referral grant, flip it to PAID
        and credit both wallets. Returns the bonus + referrer info
        on credit, or ``None`` if nothing to do.

        Called from inside an already-open transaction (the same one
        that's crediting the invitee's top-up). Locks the grant row
        ``FOR UPDATE`` so a concurrent IPN replay can't double-credit
        — only the first writer to flip PENDING → PAID actually
        credits the wallets.

        Defence in depth: refuses (returns ``None``) for non-finite
        / non-positive ``amount_usd``. The callers
        (``finalize_payment`` / ``finalize_partial_payment``) already
        guard at entry, but a future caller bypassing that path can't
        poison either wallet here.
        """
        if not _is_finite_amount(amount_usd) or amount_usd <= 0:
            return None
        bonus = self.compute_referral_bonus(
            amount_usd, percent=bonus_percent, max_usd=bonus_max_usd
        )
        if bonus <= 0:
            return None
        grant = await connection.fetchrow(
            """
            SELECT id, referrer_telegram_id, invitee_telegram_id, status
              FROM referral_grants
             WHERE invitee_telegram_id = $1
               AND status = 'PENDING'
             FOR UPDATE
            """,
            invitee_telegram_id,
        )
        if grant is None:
            return None
        referrer_id = int(grant["referrer_telegram_id"])
        await connection.execute(
            """
            UPDATE referral_grants
               SET status = 'PAID',
                   paid_at = NOW(),
                   bonus_usd_referrer = $2,
                   bonus_usd_invitee = $3,
                   triggering_transaction_id = $4,
                   triggering_amount_usd = $5
             WHERE id = $1
            """,
            grant["id"],
            bonus,
            bonus,
            transaction_id,
            amount_usd,
        )
        await connection.execute(
            """
            UPDATE users
               SET balance_usd = balance_usd + $1
             WHERE telegram_id = $2
            """,
            bonus,
            referrer_id,
        )
        await connection.execute(
            """
            UPDATE users
               SET balance_usd = balance_usd + $1
             WHERE telegram_id = $2
            """,
            bonus,
            invitee_telegram_id,
        )
        return {
            "grant_id": int(grant["id"]),
            "referrer_telegram_id": referrer_id,
            "bonus_usd": bonus,
            "amount_usd": amount_usd,
        }

    async def get_referral_stats(self, owner_telegram_id: int) -> dict:
        """Counts of PENDING / PAID grants + total bonus earned.

        Used by the "/wallet → invite stats" screen so the user can
        see how many friends are still considering the offer vs
        already converted. Single round trip; the partial index on
        ``referrer_telegram_id, status`` keeps this cheap.
        """
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                    COUNT(*) FILTER (WHERE status = 'PAID')    AS paid,
                    COALESCE(
                        SUM(bonus_usd_referrer) FILTER (WHERE status = 'PAID'),
                        0
                    ) AS total_bonus_usd
                  FROM referral_grants
                 WHERE referrer_telegram_id = $1
                """,
                owner_telegram_id,
            )
        return {
            "pending": int(row["pending"] or 0),
            "paid": int(row["paid"] or 0),
            "total_bonus_usd": float(row["total_bonus_usd"] or 0.0),
        }

    # ------------------------------------------------------------------
    # Disabled models / gateways (Stage-14)
    # ------------------------------------------------------------------

    async def get_disabled_models(self) -> set[str]:
        """Return the set of model ids currently disabled by the admin."""
        rows = await self.pool.fetch(
            "SELECT model_id FROM disabled_models"
        )
        return {r["model_id"] for r in rows}

    async def disable_model(self, model_id: str, *, actor: str = "web") -> bool:
        """Disable a model. Returns True if it was newly disabled."""
        result = await self.pool.execute(
            "INSERT INTO disabled_models (model_id, disabled_by) "
            "VALUES ($1, $2) ON CONFLICT (model_id) DO NOTHING",
            model_id, actor,
        )
        return result.endswith("1")

    async def enable_model(self, model_id: str) -> bool:
        """Re-enable a model. Returns True if it was previously disabled."""
        result = await self.pool.execute(
            "DELETE FROM disabled_models WHERE model_id = $1",
            model_id,
        )
        return result.endswith("1")

    async def get_disabled_gateways(self) -> set[str]:
        """Return the set of gateway keys currently disabled by the admin."""
        rows = await self.pool.fetch(
            "SELECT gateway_key FROM disabled_gateways"
        )
        return {r["gateway_key"] for r in rows}

    async def disable_gateway(self, gateway_key: str, *, actor: str = "web") -> bool:
        """Disable a gateway/currency. Returns True if newly disabled."""
        result = await self.pool.execute(
            "INSERT INTO disabled_gateways (gateway_key, disabled_by) "
            "VALUES ($1, $2) ON CONFLICT (gateway_key) DO NOTHING",
            gateway_key, actor,
        )
        return result.endswith("1")

    async def enable_gateway(self, gateway_key: str) -> bool:
        """Re-enable a gateway/currency. Returns True if previously disabled."""
        result = await self.pool.execute(
            "DELETE FROM disabled_gateways WHERE gateway_key = $1",
            gateway_key,
        )
        return result.endswith("1")


# Export a single instance to be used across the app
db = Database()