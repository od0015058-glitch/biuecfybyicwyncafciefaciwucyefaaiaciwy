"""SQL-shape tests for the Stage-16 row 19 migration.

The CI matrix runs the real ``alembic upgrade head`` /
``alembic downgrade base`` round-trip against Postgres
(``.github/workflows/alembic-roundtrip.yml``); these tests pin
the *static* properties of the migration script that don't need
a live DB to verify:

* idempotency guards (``IF NOT EXISTS`` / ``IF EXISTS``) on
  every DDL statement;
* the upgrade adds the column nullable (so the migration is
  online — no exclusive lock for the duration of the row
  rewrite);
* the CHECK constraint has the documented allowed-value set;
* the partial index uses ``WHERE feedback IS NOT NULL`` so
  rated rows are densely indexed and unrated rows skip the
  index altogether;
* the down_revision pointer matches the previous migration
  (``0019_disabled_model_per_gateway``) — so a stale Alembic
  branch label can't accidentally divert head to a different
  parent.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "alembic" / "versions" / "0020_usage_logs_feedback.py"
)


@pytest.fixture(scope="module")
def migration_module():
    spec = importlib.util.spec_from_file_location(
        "alembic_versions_0020", MIGRATION_PATH
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_revision_pointers(migration_module):
    assert migration_module.revision == "0020_usage_logs_feedback"
    assert (
        migration_module.down_revision == "0019_disabled_model_per_gateway"
    )
    assert migration_module.branch_labels is None
    assert migration_module.depends_on is None


def test_upgrade_adds_column_with_idempotency_guard(migration_module):
    sql = migration_module.UPGRADE_SQL.lower()
    assert "alter table usage_logs" in sql
    assert "add column if not exists feedback text" in sql
    # NULL allowed — keeps the migration online (no rewrite).
    assert "feedback text null" in sql or "feedback text\n" in sql


def test_upgrade_adds_named_check_constraint(migration_module):
    sql = migration_module.UPGRADE_SQL.lower()
    # Drop-then-add pattern so a partially-applied upgrade can be
    # re-run without the ADD raising "constraint already exists".
    assert "drop constraint if exists usage_logs_feedback_check" in sql
    assert "add constraint usage_logs_feedback_check" in sql
    assert "check (feedback is null or feedback in ('positive', 'negative'))" in sql


def test_upgrade_creates_partial_covering_index(migration_module):
    sql = migration_module.UPGRADE_SQL.lower()
    assert "create index if not exists idx_usage_logs_feedback_model_created" in sql
    # Composite key on (model, time DESC) so the dissatisfaction
    # aggregate range-scans forward from the cutoff.
    assert "(model_used, created_at desc)" in sql
    # Partial — only rated rows occupy the index.
    assert "where feedback is not null" in sql


def test_downgrade_drops_index_constraint_column_in_safe_order(
    migration_module,
):
    sql = migration_module.DOWNGRADE_SQL.lower()
    # Order matters: the index references the column, the
    # constraint references the column; both must drop first.
    drop_index = sql.find("drop index if exists idx_usage_logs_feedback_model_created")
    drop_constraint = sql.find("drop constraint if exists usage_logs_feedback_check")
    drop_column = sql.find("drop column if exists feedback")
    assert drop_index < drop_constraint < drop_column, (
        "downgrade must drop dependents before the column"
    )
    # All three guards present.
    assert drop_index >= 0
    assert drop_constraint >= 0
    assert drop_column >= 0


def test_upgrade_and_downgrade_call_op_execute(migration_module, monkeypatch):
    """No ``op.add_column`` / ``op.drop_index`` Pythonic calls —
    the file uses raw ``op.execute`` so the SQL constants are
    the single source of truth (and trivially diff-readable).

    Stub ``alembic.op.execute`` and assert each migration
    function called it once with the corresponding SQL string.
    """
    import alembic

    seen: list[str] = []

    def _record(sql, *_args, **_kwargs):
        # Convert ``TextClause`` -> ``str`` defensively so future
        # refactors that wrap the SQL in ``sa.text(...)`` keep
        # passing.
        seen.append(str(sql))

    monkeypatch.setattr(alembic.op, "execute", _record)

    migration_module.upgrade()
    migration_module.downgrade()

    assert len(seen) == 2
    assert migration_module.UPGRADE_SQL in seen
    assert migration_module.DOWNGRADE_SQL in seen
