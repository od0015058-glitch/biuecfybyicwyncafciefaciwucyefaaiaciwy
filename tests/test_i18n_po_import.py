"""Tests for ``i18n_po import`` (Stage-15-Step-E #7 follow-up #2).

Pins the importer-side of the .po round-trip:

* Validates every msgstr through :func:`strings.validate_override`.
* Buckets entries into ``upserted`` / ``unchanged`` /
  ``skipped_empty`` / ``skipped_unknown_slug`` / ``invalid``.
* Honors ``--dry-run`` (no UPSERT, but the report still tells the
  operator what *would* change).
* Surfaces DB errors as ``errors`` without aborting the whole run.

The DB is mocked with a duck-typed stub class because the importer
only touches two methods (``load_all_string_overrides`` +
``upsert_string_override``).
"""

from __future__ import annotations

import pathlib
import sys

import pytest

import i18n_po
import strings


REAL_FA_KEY = next(iter(strings._STRINGS["fa"]))


def _msgid_block(msgid: str, msgstr: str) -> str:
    """Render a minimal valid msgid/msgstr block for tests."""
    escaped_msgid = msgid.replace("\\", "\\\\").replace('"', '\\"')
    escaped_msgstr = msgstr.replace("\\", "\\\\").replace('"', '\\"')
    return (
        f'msgid "{escaped_msgid}"\n'
        f'msgstr "{escaped_msgstr}"\n'
    )


def _po_with_header(*entries: str) -> str:
    """Wrap raw msgid/msgstr blocks in a valid .po header."""
    header = (
        'msgid ""\n'
        'msgstr ""\n'
        '"Content-Type: text/plain; charset=UTF-8\\n"\n'
    )
    return header + "\n" + "\n".join(entries) + "\n"


class StubDb:
    """Minimal duck-typed Database stand-in for importer tests."""

    def __init__(self, existing_overrides=None, raise_on_upsert=False):
        self.existing = dict(existing_overrides or {})
        self.upserts: list[tuple[str, str, str, str | None]] = []
        self.raise_on_upsert = raise_on_upsert
        self.snapshot_calls = 0

    async def load_all_string_overrides(self):
        self.snapshot_calls += 1
        return dict(self.existing)

    async def upsert_string_override(
        self, lang: str, key: str, value: str, *, updated_by: str | None,
    ) -> None:
        if self.raise_on_upsert:
            raise RuntimeError("simulated DB hiccup")
        self.upserts.append((lang, key, value, updated_by))


# ---------------------------------------------------------------------
# import_po_into_db — happy paths
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_importer_upserts_a_valid_translation():
    db = StubDb()
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, "ترجمه‌ی نمونه"))
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert report.upserted == [REAL_FA_KEY]
    assert not report.has_failures
    assert db.upserts == [
        ("fa", REAL_FA_KEY, "ترجمه‌ی نمونه", "i18n_po-import"),
    ]


@pytest.mark.asyncio
async def test_importer_dry_run_does_not_call_upsert():
    db = StubDb()
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, "بدون نوشتن در DB"))
    report = await i18n_po.import_po_into_db(
        db, "fa", po_text, dry_run=True,
    )
    # The operator's preview still shows what *would* land.
    assert report.upserted == [REAL_FA_KEY]
    # But the DB was never touched.
    assert db.upserts == []


@pytest.mark.asyncio
async def test_importer_skips_empty_msgstr():
    db = StubDb()
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, ""))
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert report.skipped_empty == [REAL_FA_KEY]
    assert report.upserted == []
    assert db.upserts == []


@pytest.mark.asyncio
async def test_importer_skips_unknown_slug():
    db = StubDb()
    po_text = _po_with_header(
        _msgid_block("not_a_real_slug_in_strings_py", "value")
    )
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert report.skipped_unknown_slug == ["not_a_real_slug_in_strings_py"]
    assert report.upserted == []


@pytest.mark.asyncio
async def test_importer_buckets_unchanged_when_value_matches_existing():
    db = StubDb(existing_overrides={("fa", REAL_FA_KEY): "همان"})
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, "همان"))
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert report.unchanged == [REAL_FA_KEY]
    assert report.upserted == []
    # Idempotent re-runs don't generate a write.
    assert db.upserts == []


@pytest.mark.asyncio
async def test_importer_uses_supplied_existing_overrides_to_skip_db_query():
    db = StubDb()
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, "x"))
    await i18n_po.import_po_into_db(
        db,
        "fa",
        po_text,
        existing_overrides={("fa", REAL_FA_KEY): "x"},
    )
    # Caller already had a snapshot — the importer doesn't re-fetch.
    assert db.snapshot_calls == 0


@pytest.mark.asyncio
async def test_importer_passes_updated_by_to_upsert():
    db = StubDb()
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, "label"))
    await i18n_po.import_po_into_db(
        db, "fa", po_text, updated_by="crowdin-pr-241",
    )
    assert db.upserts[0][3] == "crowdin-pr-241"


# ---------------------------------------------------------------------
# import_po_into_db — error paths
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_importer_rejects_unknown_lang():
    db = StubDb()
    with pytest.raises(ValueError, match="unsupported lang"):
        await i18n_po.import_po_into_db(db, "klingon", "")


@pytest.mark.asyncio
async def test_importer_invalid_placeholder_is_bucketed_not_raised():
    """A typo'd placeholder would crash the runtime ``t()`` call —
    the validator catches it pre-write and the importer reports it
    without aborting the rest of the file."""
    # Find a slug with a placeholder we can mistype.
    target = None
    for key in strings._STRINGS["fa"]:
        if "{" in strings._STRINGS["fa"][key]:
            target = key
            break
    if target is None:
        pytest.skip("no placeholder slugs in fa locale")

    db = StubDb()
    po_text = _po_with_header(
        _msgid_block(target, "خطای جایگذاری {totally_unknown_kw}")
    )
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert len(report.invalid) == 1
    assert report.invalid[0][0] == target
    assert "totally_unknown_kw" in report.invalid[0][1]
    assert db.upserts == []


@pytest.mark.asyncio
async def test_importer_unparseable_po_yields_single_error():
    db = StubDb()
    # Unterminated quoted string — load_po raises ValueError.
    bad_po = (
        'msgid "fine"\n'
        f'msgstr "broken\n'  # missing closing quote
    )
    report = await i18n_po.import_po_into_db(db, "fa", bad_po)
    assert len(report.errors) == 1
    assert report.errors[0][0] == "<file>"
    assert report.upserted == []


@pytest.mark.asyncio
async def test_importer_db_error_is_reported_per_key_not_aborted():
    db = StubDb(raise_on_upsert=True)
    po_text = _po_with_header(
        _msgid_block(REAL_FA_KEY, "ولی DB می‌ترکه"),
    )
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert len(report.errors) == 1
    assert report.errors[0][0] == REAL_FA_KEY
    assert "simulated DB hiccup" in report.errors[0][1]


@pytest.mark.asyncio
async def test_importer_handles_db_snapshot_error():
    """If we can't even snapshot the table, abort cleanly with one
    error — no per-key noise."""

    class BrokenSnapshotDb(StubDb):
        async def load_all_string_overrides(self):
            raise RuntimeError("pool is closed")

    db = BrokenSnapshotDb()
    po_text = _po_with_header(_msgid_block(REAL_FA_KEY, "x"))
    report = await i18n_po.import_po_into_db(db, "fa", po_text)
    assert len(report.errors) == 1
    assert "<bot_strings snapshot>" in report.errors[0][0]
    assert report.upserted == []


# ---------------------------------------------------------------------
# ImportReport.render() — operator-facing summary
# ---------------------------------------------------------------------


def test_report_render_includes_all_buckets():
    r = i18n_po.ImportReport()
    r.upserted = ["a", "b"]
    r.unchanged = ["c"]
    r.skipped_empty = ["d"]
    r.skipped_unknown_slug = ["e"]
    r.invalid = [("f", "bad placeholder {x}")]
    r.errors = [("g", "DB down")]
    text = r.render()
    assert "upserted                : 2" in text
    assert "unchanged (already set) : 1" in text
    assert "skipped (empty msgstr)  : 1" in text
    assert "skipped (unknown slug)  : 1" in text
    assert "invalid (bad msgstr)    : 1" in text
    assert "errors (db / io)        : 1" in text
    assert "f: bad placeholder {x}" in text
    assert "g: DB down" in text


def test_report_has_failures_only_for_invalid_or_errors():
    r = i18n_po.ImportReport()
    assert not r.has_failures
    r.upserted = ["x"]
    r.skipped_empty = ["y"]
    r.skipped_unknown_slug = ["z"]
    r.unchanged = ["w"]
    assert not r.has_failures, (
        "skipped / upserted / unchanged are not failures"
    )
    r.invalid = [("k", "v")]
    assert r.has_failures
    r.invalid = []
    r.errors = [("k", "v")]
    assert r.has_failures


def test_report_total_seen_sums_all_buckets():
    r = i18n_po.ImportReport()
    r.upserted = ["a", "b"]
    r.unchanged = ["c", "d", "e"]
    r.skipped_empty = ["f"]
    r.skipped_unknown_slug = ["g"]
    r.invalid = [("h", "x"), ("i", "y")]
    r.errors = [("j", "z")]
    assert r.total_seen == 10


# ---------------------------------------------------------------------
# CLI argument parsing — ``python -m i18n_po import ...``
# ---------------------------------------------------------------------


def test_cli_import_help_lists_required_args(capsys):
    with pytest.raises(SystemExit) as exc:
        i18n_po.main(["import", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    # Must surface the four user-facing pieces of info.
    assert "lang" in out
    assert "po_path" in out
    assert "--dry-run" in out
    assert "--updated-by" in out


def test_cli_import_missing_args_exits_nonzero():
    with pytest.raises(SystemExit) as exc:
        i18n_po.main(["import"])
    assert exc.value.code != 0


def test_cli_import_nonexistent_path_returns_2(capsys, tmp_path):
    missing = tmp_path / "does-not-exist.po"
    rc = i18n_po.main(["import", "fa", str(missing)])
    assert rc == 2
    captured = capsys.readouterr()
    assert "not found" in captured.err.lower()


# ---------------------------------------------------------------------
# Bug fix bundle: upsert_string_override strips NUL bytes
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_string_override_strips_nul_bytes_from_value(
    monkeypatch, caplog,
):
    """Real bundled bug fix: a translator's ``.po`` containing a
    stray NUL byte (some Crowdin export pipelines emit them in
    multi-line msgstrs) used to crash
    ``upsert_string_override`` with
    ``invalid byte sequence for encoding "UTF8": 0x00``. The
    defensive strip preserves the override (minus the NUL) and
    logs a WARNING. Same pattern as ``set_admin_role`` for
    ``notes``.
    """
    import database

    # Capture the args that hit asyncpg.
    captured: dict = {}

    class _StubConn:
        async def execute(self, query, *args):
            captured["args"] = args

    class _StubAcquire:
        async def __aenter__(self):
            return _StubConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _StubPool:
        def acquire(self):
            return _StubAcquire()

    db = database.Database()
    db.pool = _StubPool()
    with caplog.at_level("WARNING", logger="bot.database"):
        await db.upsert_string_override(
            "fa", "hub_text", "ho\x00la", updated_by="op",
        )
    # NUL stripped from the value before reaching asyncpg.
    assert "\x00" not in captured["args"][2]
    assert captured["args"][2] == "hola"
    # Warning logged so ops can see the corruption was caught.
    assert any(
        "stripped NUL byte" in record.message for record in caplog.records
    ), [r.message for r in caplog.records]


@pytest.mark.asyncio
async def test_upsert_string_override_strips_nul_bytes_from_updated_by(
    caplog,
):
    """Same defence on the audit column — a fuzzed CLI invocation
    or a rogue script could feed a NUL into ``updated_by``."""
    import database

    captured: dict = {}

    class _StubConn:
        async def execute(self, query, *args):
            captured["args"] = args

    class _StubAcquire:
        async def __aenter__(self):
            return _StubConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _StubPool:
        def acquire(self):
            return _StubAcquire()

    db = database.Database()
    db.pool = _StubPool()
    with caplog.at_level("WARNING", logger="bot.database"):
        await db.upsert_string_override(
            "fa", "hub_text", "valid", updated_by="op\x00name",
        )
    assert captured["args"][3] == "opname"
    assert any(
        "stripped NUL byte" in record.message
        and "updated_by" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_upsert_string_override_passes_through_clean_input():
    """Regression pin: clean input (no NUL) must not be mutated and
    must not log a warning."""
    import database

    captured: dict = {}

    class _StubConn:
        async def execute(self, query, *args):
            captured["args"] = args

    class _StubAcquire:
        async def __aenter__(self):
            return _StubConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _StubPool:
        def acquire(self):
            return _StubAcquire()

    db = database.Database()
    db.pool = _StubPool()
    await db.upsert_string_override(
        "fa", "hub_text", "kept verbatim", updated_by="op",
    )
    assert captured["args"] == (
        "fa", "hub_text", "kept verbatim", "op",
    )
