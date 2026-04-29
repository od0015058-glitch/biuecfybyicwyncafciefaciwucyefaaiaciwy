"""broadcast_jobs: durable registry for admin broadcast jobs

Stage-9-Step-10. Pre-this-migration, the admin-panel broadcast page
kept its job registry in a process-local dict (``APP_KEY_BROADCAST_JOBS``
in ``web_admin.py``). That registry is fine for the live-progress
rendering of an active job, but every job — including completed ones
the operator might want to revisit — is lost on bot restart. Worse, a
bot crash mid-broadcast leaves no forensic record at all: the
``broadcast_start`` row in ``admin_audit_log`` is the only trace, and
it carries no progress counters.

This migration introduces ``broadcast_jobs`` as the durable mirror.
``web_admin._run_broadcast_job`` writes to it on every state
transition (queued → running → completed / failed / cancelled /
interrupted) and on throttled progress ticks. ``broadcast_get`` and
``broadcast_detail_get`` read recent / single rows from this table
rather than the in-memory dict so a restart doesn't orphan history.
On startup, ``setup_admin_routes`` calls
``Database.mark_orphan_broadcast_jobs_interrupted`` — any row left in
``queued`` / ``running`` from before the restart is flipped to
``interrupted`` with ``completed_at = NOW()`` so the UI doesn't
forever show a "running" job whose worker task no longer exists.

Schema:

* ``broadcast_jobs``
    - ``job_id TEXT PRIMARY KEY`` — the existing
      ``secrets.token_urlsafe(6)`` identifier the URL routes use, kept
      verbatim so a `/admin/broadcast/{job_id}` link in an audit row
      still resolves after restart.
    - ``text_preview TEXT NOT NULL`` — first ~120 chars of the message,
      with ``"…"`` suffix when truncated. The full text is intentionally
      NOT stored — admin broadcasts can be 3 500+ chars, the page only
      needs the preview to render the recent-jobs list, and we don't
      want operator-typed PII / wallet addresses sitting in the table
      indefinitely. Re-running the broadcast is an explicit operator
      action (paste the text again).
    - ``full_text_len INTEGER NOT NULL`` — character count of the full
      text, surfaced on the recent-jobs list so an operator can tell at
      a glance whether a big or small broadcast was run.
    - ``only_active_days INTEGER NULL`` — recipient filter from the
      form (``NULL`` = "all users").
    - ``state TEXT NOT NULL`` — one of ``queued``, ``running``,
      ``completed``, ``failed``, ``cancelled``, ``interrupted``. Stored
      verbatim (no enum) so adding a new terminal state in code
      doesn't require a migration.
    - ``total INTEGER NOT NULL DEFAULT 0`` — recipient count once the
      query returns; 0 while still queued.
    - ``sent_count`` / ``blocked_count`` / ``failed_count``
      ``INTEGER NOT NULL DEFAULT 0`` — running counters mirrored from
      the in-memory job dict's ``sent`` / ``blocked`` / ``failed``
      keys. Renamed with ``_count`` suffix so a future caller writing
      raw SQL can't confuse them with the ``state="failed"`` row.
    - ``i INTEGER NOT NULL DEFAULT 0`` — recipients attempted so far
      (``sent + blocked + failed`` for a cancelled run, ``total`` for
      a completed one). Surfaced as the progress numerator on the
      detail page.
    - ``error TEXT NULL`` — short human-readable failure reason for
      ``failed`` / ``interrupted`` rows.
    - ``cancel_requested BOOLEAN NOT NULL DEFAULT FALSE`` — soft-cancel
      flag mirrored from the in-memory job. Persisted so an operator
      can see in the recent-jobs list that a job was cancelled
      (vs. self-completed).
    - ``created_at`` / ``started_at`` / ``completed_at TIMESTAMPTZ`` —
      lifecycle timestamps. ``created_at`` defaults to ``NOW()`` so a
      crash between row insert and the worker's first state write
      still leaves a sensible audit trail; ``started_at`` /
      ``completed_at`` are NULL until the worker reaches those
      transitions.

Indexed on:

* ``created_at DESC`` — covers the recent-jobs list query
  (``ORDER BY created_at DESC LIMIT N``) without a sort step.
* ``state`` — covers the startup orphan sweep
  (``WHERE state IN ('queued', 'running')``).

Both ``CREATE TABLE IF NOT EXISTS`` and ``CREATE INDEX IF NOT EXISTS``
so the migration is idempotent (matches the project convention from
``0005_admin_audit_log`` / ``0006_payment_status_transitions``).

Revision ID: 0007_broadcast_jobs
Revises: 0006_usage_logs_indexes

The parent revision is the ``0006_usage_logs_indexes`` migration —
the second of the two 0006-prefixed migrations linearized in that
file's docstring. Nothing here touches ``usage_logs`` and that
migration didn't touch ``broadcast_jobs``, so the order is arbitrary;
chaining off ``0006_usage_logs_indexes`` keeps the linear graph
intact.
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0007_broadcast_jobs"
down_revision = "0006_usage_logs_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS broadcast_jobs (
            job_id            TEXT        PRIMARY KEY,
            text_preview      TEXT        NOT NULL,
            full_text_len     INTEGER     NOT NULL,
            only_active_days  INTEGER     NULL,
            state             TEXT        NOT NULL,
            total             INTEGER     NOT NULL DEFAULT 0,
            sent_count        INTEGER     NOT NULL DEFAULT 0,
            blocked_count     INTEGER     NOT NULL DEFAULT 0,
            failed_count      INTEGER     NOT NULL DEFAULT 0,
            i                 INTEGER     NOT NULL DEFAULT 0,
            error             TEXT        NULL,
            cancel_requested  BOOLEAN     NOT NULL DEFAULT FALSE,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at        TIMESTAMPTZ NULL,
            completed_at      TIMESTAMPTZ NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broadcast_jobs_created
            ON broadcast_jobs (created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broadcast_jobs_state
            ON broadcast_jobs (state)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_broadcast_jobs_state")
    op.execute("DROP INDEX IF EXISTS idx_broadcast_jobs_created")
    op.execute("DROP TABLE IF EXISTS broadcast_jobs")
