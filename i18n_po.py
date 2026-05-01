"""GNU gettext ``.po`` round-trip for the bilingual string table.

Stage-15-Step-E #7 first slice. The bot's UI strings live in
:data:`strings._STRINGS` (a Python dict). This module ships a
narrow ``.po`` exporter / parser pair so:

* Translators can use Poedit / Crowdin / OmegaT (which all speak
  ``.po``) instead of editing a Python file.
* Diffing translations is easier — ``.po`` is a per-string format,
  not a giant Python literal where every change shifts line numbers.
* Pluralization (``ngettext``) is the natural next step; ``.po``
  is the file format the stdlib ``gettext`` runtime expects.

The runtime keeps reading ``strings._STRINGS`` for now. The
``.po`` files under ``locale/<lang>/LC_MESSAGES/messages.po`` are a
*derived* artifact: the Python dict is the source of truth, and
``python -m i18n_po export`` regenerates the on-disk files. A CI
test (``tests/test_i18n_po.py``) asserts the on-disk files match
the current dict — adding a new slug without re-exporting fails CI.

Format conventions
==================

* **msgid is the slug.** Standard gettext uses the source-language
  string as ``msgid``, but the bot's source language is Persian, and
  Persian is awkward to use as a key (RTL, length explosions for
  long messages). Slug-as-msgid is supported by Poedit / Crowdin
  and matches the dict's actual key shape.
* **msgstr is the translation in the locale's language.** For
  ``locale/fa/LC_MESSAGES/messages.po`` it's the Persian text;
  for ``locale/en/.../messages.po`` it's the English text.
* **The default-locale text appears as a translator comment**
  (``#. <text>``) for context — handy when translating into a
  non-default locale and you want to see what the slug means.
* **Multi-line strings** use the standard ``""<NL>"line"`` 
  continuation. The bot keeps the literal ``\\n`` inside; the .po
  encoding escapes it as ``\\n`` per gettext's escape rules.
* **No msgctxt.** A single global namespace fits the bot's flat
  slug naming scheme (e.g. ``hub_text``, ``redeem_usage``). If a
  future PR introduces context disambiguation, add ``msgctxt`` then.

CLI
===

``python -m i18n_po export`` — write every supported locale's .po
file under ``locale/<lang>/LC_MESSAGES/messages.po``.

``python -m i18n_po check`` — exit non-zero if any on-disk .po
file differs from the dict export. Used by CI.

``python -m i18n_po import <lang> <path>`` — bulk-load a
translator's ``.po`` file into the ``bot_strings`` runtime
override table (Stage-15-Step-E #7 follow-up #2). Every
``msgstr`` is validated against
:func:`strings.validate_override` before being written; entries
that fail (unknown slug, bad placeholder, malformed syntax) are
reported and skipped — the rest are upserted. ``--dry-run``
validates without writing. Use ``--updated-by`` to tag the
``bot_strings.updated_by`` audit column with a translator name
or PR number; defaults to ``"i18n_po-import"``.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import strings

if TYPE_CHECKING:  # pragma: no cover
    from database import Database

log = logging.getLogger("bot.i18n_po")

# --------------------------------------------------------------------- #
# Output paths                                                          #
# --------------------------------------------------------------------- #

REPO_ROOT = Path(__file__).resolve().parent
LOCALE_DIR = REPO_ROOT / "locale"


def po_path(lang: str) -> Path:
    """Return the on-disk path for *lang*'s ``messages.po`` file."""
    return LOCALE_DIR / lang / "LC_MESSAGES" / "messages.po"


# --------------------------------------------------------------------- #
# Escaping / line wrapping                                              #
# --------------------------------------------------------------------- #


def _escape_po_string(s: str) -> str:
    """Escape *s* for a ``.po`` ``msgid`` / ``msgstr`` literal.

    The gettext escape rules are a subset of Python's: backslash,
    double-quote, newline, and tab. Anything else (including the
    full Persian Unicode range) goes through verbatim — the .po
    file is UTF-8 encoded, so RTL marks etc. don't need escaping.
    """
    return (
        s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


def _unescape_po_string(s: str) -> str:
    """Inverse of :func:`_escape_po_string`."""
    out: list[str] = []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "\\" and i + 1 < len(s):
            nxt = s[i + 1]
            if nxt == "n":
                out.append("\n")
            elif nxt == "t":
                out.append("\t")
            elif nxt == "\\":
                out.append("\\")
            elif nxt == "\"":
                out.append("\"")
            else:
                # Unknown escape — preserve the backslash and the
                # following char rather than swallowing it. Matches
                # Python's behaviour for unknown ``\\X`` sequences in
                # raw input.
                out.append(ch)
                out.append(nxt)
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _format_po_value(label: str, value: str) -> str:
    """Render ``<label> "..."`` (single-line) or the multi-line
    ``<label> ""`` + ``"line\\n"`` form when *value* contains
    newlines or is empty."""
    if "\n" not in value and len(value) <= 76:
        return f'{label} "{_escape_po_string(value)}"'
    # Multi-line form: every newline anchors a continuation line.
    # Standard idiom: an empty leading ``""`` makes the diff for a
    # template-prefix change small (only the changed line moves).
    parts = [f'{label} ""']
    chunks = value.split("\n")
    for i, chunk in enumerate(chunks):
        # Every chunk except the last logically ends in \n.
        if i < len(chunks) - 1:
            chunk_with_nl = chunk + "\n"
        else:
            chunk_with_nl = chunk
        parts.append(f'"{_escape_po_string(chunk_with_nl)}"')
    return "\n".join(parts)


# --------------------------------------------------------------------- #
# Export                                                                #
# --------------------------------------------------------------------- #


def dump_po(
    lang: str,
    *,
    strings_table: dict[str, dict[str, str]] | None = None,
    default_lang: str | None = None,
    project_id_version: str = "meowassist 1.0",
    revision_date: str | None = None,
) -> str:
    """Render the full ``.po`` body for *lang*.

    *strings_table* defaults to :data:`strings._STRINGS`; pass an
    explicit dict in tests to avoid coupling round-trip tests to the
    full live table. *default_lang* sets which locale's compiled
    default appears as the translator-comment; defaults to
    :data:`strings.DEFAULT_LANGUAGE`. *revision_date* is the
    ``PO-Revision-Date`` header — defaults to ``"YEAR-MO-DA HO:MI+ZONE"``
    (the gettext placeholder) so the file is byte-stable across
    re-exports; tests can pass a real date when checking header
    rendering, but the stable placeholder is what gets committed
    to disk.
    """
    if strings_table is None:
        strings_table = strings._STRINGS
    if default_lang is None:
        default_lang = strings.DEFAULT_LANGUAGE
    if lang not in strings_table:
        raise ValueError(
            f"unknown locale {lang!r}; "
            f"known: {sorted(strings_table.keys())}"
        )
    locale_table = strings_table[lang]
    default_table = strings_table.get(default_lang, {})

    # Stage-15-Step-E #7 follow-up #1 bundled bug fix: escape the
    # caller-supplied ``project_id_version`` and ``revision_date``
    # before splicing them into the header literal. Pre-fix the
    # function pasted those strings raw into the f-string, so a
    # value containing ``"`` (quote) or ``\\`` (backslash) — both
    # legal characters in real-world ``Project-Id-Version`` strings
    # like ``"meowassist 1.0 \"beta\""`` — broke the surrounding
    # quoted-string literal and produced an unparseable ``.po``
    # file. ``load_po`` would then either raise ``unterminated
    # quoted string`` or, worse, silently mis-parse later entries
    # because the quote-balance was off. The drift-gate
    # (``i18n_po check``) would catch the divergence on the next
    # CI run, but only AFTER the broken file had been committed.
    safe_project_id_version = _escape_po_string(project_id_version)
    safe_revision_date = _escape_po_string(
        revision_date or "YEAR-MO-DA HO:MI+ZONE"
    )
    header_msgstr = (
        f"Project-Id-Version: {safe_project_id_version}\\n"
        f"Report-Msgid-Bugs-To: \\n"
        f"PO-Revision-Date: {safe_revision_date}\\n"
        f"Last-Translator: \\n"
        f"Language-Team: \\n"
        f"Language: {lang}\\n"
        f"MIME-Version: 1.0\\n"
        f"Content-Type: text/plain; charset=UTF-8\\n"
        f"Content-Transfer-Encoding: 8bit\\n"
    )

    parts: list[str] = []
    # Standard gettext header entry — a blank msgid with the
    # metadata in msgstr. Poedit / Crowdin populate the visible
    # metadata UI from this block.
    parts.append(
        "# Translations for the bot's user-facing strings.\n"
        "# Source of truth: strings.py in the repo root.\n"
        f"# Regenerate: python -m i18n_po export\n"
        f"#\n"
        'msgid ""\n'
        f'msgstr ""\n'
        f'"{header_msgstr}"'
    )

    for key in sorted(locale_table):
        msgstr = locale_table[key]
        comment_lines: list[str] = []
        if lang != default_lang and key in default_table:
            # Translator comment showing the default-locale rendering.
            # Multi-line defaults get one ``#.`` line per source line.
            for src_line in default_table[key].split("\n"):
                comment_lines.append(f"#. {src_line}")
        # ``#:`` reference points back to the slug definition. Slugs
        # don't have line numbers so we use the slug itself — matches
        # the convention used by some keyword-based gettext tools.
        comment_lines.append(f"#: strings.py:{key}")
        block_lines: list[str] = list(comment_lines)
        block_lines.append(_format_po_value("msgid", key))
        block_lines.append(_format_po_value("msgstr", msgstr))
        parts.append("\n".join(block_lines))

    # Trailing newline keeps `git diff` from showing a "no newline
    # at end of file" on the last entry.
    return "\n\n".join(parts) + "\n"


# --------------------------------------------------------------------- #
# Parse                                                                 #
# --------------------------------------------------------------------- #


def load_po(text: str) -> dict[str, str]:
    """Parse a ``.po`` file body into ``{slug: msgstr}``.

    The header entry (``msgid ""``) is silently skipped — its
    metadata is for translators / tools, not the bot.

    Tolerant of:

    * Comment lines (``#``, ``#.``, ``#:``, ``#,``).
    * Blank lines between entries.
    * Multi-line ``msgid`` / ``msgstr`` (``""<NL>"line\\n"``).

    Strict about:

    * Each entry must have a ``msgid`` and a ``msgstr``.
    * ``msgctxt`` is rejected — the bot doesn't use context
      disambiguation, and a stray ``msgctxt`` from a future feature
      we haven't designed yet should be loud, not silently ignored.
    """
    result: dict[str, str] = {}
    current_msgid: str | None = None
    current_msgstr: str | None = None
    # State tracking so multi-line continuations append to the
    # right field. Possible states: ``None`` (between entries),
    # ``"msgid"``, ``"msgstr"``.
    last_field: str | None = None

    def _flush() -> None:
        nonlocal current_msgid, current_msgstr, last_field
        if current_msgid is None and current_msgstr is None:
            return
        if current_msgid is None or current_msgstr is None:
            raise ValueError(
                "incomplete .po entry — "
                f"msgid={current_msgid!r}, msgstr={current_msgstr!r}"
            )
        if current_msgid == "":
            # Header entry — skip.
            pass
        else:
            if current_msgid in result:
                raise ValueError(
                    f"duplicate msgid {current_msgid!r} in .po file"
                )
            result[current_msgid] = current_msgstr
        current_msgid = None
        current_msgstr = None
        last_field = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r")
        if not line.strip():
            _flush()
            continue
        if line.startswith("#"):
            # Comment / reference / flag — ignored by the parser.
            continue
        if line.startswith("msgctxt"):
            raise ValueError(
                "msgctxt is not supported in this codebase yet — "
                f"line: {line!r}"
            )
        if line.startswith("msgid "):
            _flush()
            current_msgid = _parse_quoted_value(line[len("msgid "):])
            last_field = "msgid"
            continue
        if line.startswith("msgstr "):
            current_msgstr = _parse_quoted_value(line[len("msgstr "):])
            last_field = "msgstr"
            continue
        if line.startswith("\""):
            # Continuation of the previous field.
            cont = _parse_quoted_value(line)
            if last_field == "msgid":
                current_msgid = (current_msgid or "") + cont
            elif last_field == "msgstr":
                current_msgstr = (current_msgstr or "") + cont
            else:
                raise ValueError(
                    f"orphan continuation line (no preceding msgid/msgstr): "
                    f"{line!r}"
                )
            continue
        raise ValueError(f"unrecognised .po line: {line!r}")
    _flush()
    return result


def _parse_quoted_value(s: str) -> str:
    """Extract the content of a ``"..."`` segment and unescape it.

    Tolerant of trailing whitespace / comments; rejects unterminated
    quotes."""
    s = s.strip()
    if not s.startswith("\""):
        raise ValueError(f"expected quoted string, got {s!r}")
    if not s.endswith("\""):
        raise ValueError(f"unterminated quoted string {s!r}")
    return _unescape_po_string(s[1:-1])


# --------------------------------------------------------------------- #
# CLI                                                                   #
# --------------------------------------------------------------------- #


def _write_locale_files(*, dry_run: bool = False) -> list[Path]:
    """Write every supported locale's ``.po`` file. Returns the paths
    written. With ``dry_run=True`` it returns the would-write paths
    without touching disk."""
    written: list[Path] = []
    for lang in strings.SUPPORTED_LANGUAGES:
        body = dump_po(lang)
        path = po_path(lang)
        written.append(path)
        if dry_run:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return written


def _check_locale_files() -> int:
    """Compare on-disk ``.po`` files against the dict export.

    Returns ``0`` when every locale matches, ``1`` otherwise. Used
    by CI to flag drift when a developer adds / changes a slug
    without re-exporting.
    """
    drift = False
    for lang in strings.SUPPORTED_LANGUAGES:
        path = po_path(lang)
        expected = dump_po(lang)
        if not path.exists():
            print(
                f"DRIFT: {path} does not exist. "
                f"Run: python -m i18n_po export"
            )
            drift = True
            continue
        actual = path.read_text(encoding="utf-8")
        if actual != expected:
            print(
                f"DRIFT: {path} differs from strings._STRINGS export. "
                f"Run: python -m i18n_po export"
            )
            drift = True
    return 1 if drift else 0


# --------------------------------------------------------------------- #
# Import (Stage-15-Step-E #7 follow-up #2)                              #
# --------------------------------------------------------------------- #


# Public so callers (tests, future operator tooling) can introspect
# what the import actually changed without re-parsing the report
# strings.
class ImportReport:
    """Tally of what an :func:`import_po_into_db` run did.

    Counters are tracked in five buckets so the operator can tell
    at a glance whether the import was clean (only ``upserted`` or
    ``unchanged``), partially clean (some ``invalid``), or had a
    DB-side problem (``errors``).

    ``invalid`` collects ``(key, reason)`` so the operator can fix
    the offending msgstrs in the source .po and re-run; ``errors``
    collects ``(key, reason)`` for I/O / DB problems that aren't
    the translator's fault. The two are kept separate so a
    network blip doesn't masquerade as a translation problem.
    """

    __slots__ = (
        "upserted",
        "unchanged",
        "skipped_empty",
        "skipped_unknown_slug",
        "invalid",
        "errors",
    )

    def __init__(self) -> None:
        self.upserted: list[str] = []
        self.unchanged: list[str] = []
        self.skipped_empty: list[str] = []
        self.skipped_unknown_slug: list[str] = []
        self.invalid: list[tuple[str, str]] = []
        self.errors: list[tuple[str, str]] = []

    @property
    def total_seen(self) -> int:
        return (
            len(self.upserted)
            + len(self.unchanged)
            + len(self.skipped_empty)
            + len(self.skipped_unknown_slug)
            + len(self.invalid)
            + len(self.errors)
        )

    @property
    def has_failures(self) -> bool:
        return bool(self.invalid or self.errors)

    def render(self) -> str:
        """Operator-facing summary block."""
        lines = [
            f"  total entries seen      : {self.total_seen}",
            f"  upserted                : {len(self.upserted)}",
            f"  unchanged (already set) : {len(self.unchanged)}",
            f"  skipped (empty msgstr)  : {len(self.skipped_empty)}",
            f"  skipped (unknown slug)  : {len(self.skipped_unknown_slug)}",
            f"  invalid (bad msgstr)    : {len(self.invalid)}",
            f"  errors (db / io)        : {len(self.errors)}",
        ]
        if self.skipped_unknown_slug:
            lines.append("")
            lines.append("Skipped — slug not in strings.py:")
            for key in self.skipped_unknown_slug:
                lines.append(f"  - {key}")
        if self.invalid:
            lines.append("")
            lines.append("Invalid — msgstr failed validation:")
            for key, reason in self.invalid:
                lines.append(f"  - {key}: {reason}")
        if self.errors:
            lines.append("")
            lines.append("Errors — DB / I/O problems:")
            for key, reason in self.errors:
                lines.append(f"  - {key}: {reason}")
        return "\n".join(lines)


async def import_po_into_db(
    db: "Database",
    lang: str,
    po_text: str,
    *,
    dry_run: bool = False,
    updated_by: str = "i18n_po-import",
    existing_overrides: dict[tuple[str, str], str] | None = None,
) -> ImportReport:
    """Validate every ``(msgid, msgstr)`` and UPSERT the survivors
    into ``bot_strings``.

    *db* is an opened :class:`database.Database` (its ``pool`` is
    the only attribute we touch — the test harness passes a
    duck-typed stub).

    *lang* must be one of :data:`strings.SUPPORTED_LANGUAGES`. We
    don't auto-detect from the .po metadata because gettext lets
    translators write whatever they want into the ``Language:``
    header; explicit is safer.

    *po_text* is the raw .po body. ``load_po`` parses it.

    *dry_run* — when ``True`` we run all the same validation but
    skip the actual UPSERT call. Caller still gets the same
    report counters so they can preview the change.

    *updated_by* — string that lands in
    ``bot_strings.updated_by``. Defaults to ``"i18n_po-import"``;
    the CLI accepts a ``--updated-by`` flag so an operator can
    write something like ``"crowdin-pr-241"`` for traceability.

    *existing_overrides* — optional pre-loaded snapshot of the
    ``bot_strings`` table. If supplied we use it to bucket
    "unchanged" rows (msgstr matches what's already in the DB)
    so the report's ``upserted`` count reflects only *real*
    changes. If omitted we fetch it from the DB once.

    Returns the :class:`ImportReport` regardless of failures —
    callers decide whether ``has_failures`` should escalate to a
    non-zero exit code.
    """
    if lang not in strings.SUPPORTED_LANGUAGES:
        raise ValueError(
            f"unsupported lang {lang!r}; "
            f"known: {list(strings.SUPPORTED_LANGUAGES)}"
        )

    report = ImportReport()

    try:
        catalog = load_po(po_text)
    except ValueError as exc:
        # The whole file is unparseable — surface as a single
        # error so the operator knows nothing landed.
        report.errors.append(("<file>", f"failed to parse .po: {exc}"))
        return report

    # Snapshot existing overrides once so we can bucket
    # "unchanged" entries without re-querying per-key. On a typical
    # 160-slug .po this is a single query of <100 rows.
    if existing_overrides is None:
        try:
            existing_overrides = await db.load_all_string_overrides()
        except Exception as exc:  # noqa: BLE001  — surface as report
            log.exception(
                "i18n_po import: failed to snapshot bot_strings"
            )
            report.errors.append(
                ("<bot_strings snapshot>", f"{type(exc).__name__}: {exc}")
            )
            return report

    for key in sorted(catalog):
        msgstr = catalog[key]
        if not msgstr:
            # Empty msgstr is the gettext "untranslated" marker;
            # bot_strings is for *non*-empty overrides only. Empty
            # entries are not stored — the runtime falls through to
            # the .po-runtime layer or the compiled default.
            report.skipped_empty.append(key)
            continue
        if strings.get_compiled_default(lang, key) is None:
            # Unknown slug — translator is on a stale strings.py
            # snapshot, or there's a typo. Don't poison the table
            # with it; tell the operator so they can update the .po.
            report.skipped_unknown_slug.append(key)
            continue
        validation_error = strings.validate_override(lang, key, msgstr)
        if validation_error is not None:
            report.invalid.append((key, validation_error))
            continue
        if existing_overrides.get((lang, key)) == msgstr:
            # Idempotent re-runs are common (translator pushes
            # again after a tiny edit) — don't count rows that
            # didn't actually change.
            report.unchanged.append(key)
            continue
        if dry_run:
            # In dry-run we still bucket as "would upsert" so the
            # operator gets accurate counts.
            report.upserted.append(key)
            continue
        try:
            await db.upsert_string_override(
                lang, key, msgstr, updated_by=updated_by
            )
        except Exception as exc:  # noqa: BLE001 — surface as report
            log.exception(
                "i18n_po import: failed upsert for %s:%s", lang, key
            )
            report.errors.append(
                (key, f"upsert failed: {type(exc).__name__}: {exc}")
            )
            continue
        report.upserted.append(key)

    return report


async def _import_cli_async(
    lang: str,
    po_path_arg: Path,
    *,
    dry_run: bool,
    updated_by: str,
) -> int:
    """CLI entry point body. Connects to the DB, runs the import,
    prints the report, returns the exit code.

    The caller is expected to have already validated that
    ``po_path_arg`` exists — :func:`main` does that pre-flight
    before kicking off ``asyncio.run`` so a missing-file exit
    doesn't spin up (and tear down) a fresh event loop. Tearing
    down the default loop has been observed to contaminate
    downstream tests that use :func:`asyncio.get_event_loop`
    afterward.
    """
    po_text = po_path_arg.read_text(encoding="utf-8")

    # Late import — keeps the export / check codepaths free of any
    # asyncpg dependency. The DB module is a heavy import (it pulls
    # in dozens of asyncpg helpers) so we only pay the cost on the
    # `import` subcommand.
    from database import Database

    db = Database()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        # Make the DB connection error obvious — most likely cause
        # is missing DB_USER / DB_PASSWORD / DB_NAME / DB_HOST /
        # DB_PORT env vars when running locally.
        print(
            f"ERROR: failed to connect to the database: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        print(
            "Set DB_USER / DB_PASSWORD / DB_NAME / DB_HOST / "
            "DB_PORT in the environment, or use --dry-run to "
            "skip the DB connection (validates only).",
            file=sys.stderr,
        )
        return 3

    try:
        report = await import_po_into_db(
            db,
            lang,
            po_text,
            dry_run=dry_run,
            updated_by=updated_by,
        )
    finally:
        try:
            await db.close()
        except Exception:  # noqa: BLE001  — never mask the report
            log.exception("i18n_po import: db.close() failed")

    print(
        f"i18n_po import (lang={lang}, file={po_path_arg}, "
        f"dry_run={dry_run}, updated_by={updated_by!r}):"
    )
    print(report.render())

    return 1 if report.has_failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m i18n_po",
        description=(
            "Round-trip strings.py <-> locale/<lang>/LC_MESSAGES/messages.po. "
            "Stage-15-Step-E #7."
        ),
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("export", help="Write the .po files.")
    sub.add_parser("check", help="Compare on-disk .po files against strings.py.")

    import_p = sub.add_parser(
        "import",
        help=(
            "Bulk-load a translator's .po file into the bot_strings "
            "DB table. Validates every msgstr first."
        ),
    )
    import_p.add_argument(
        "lang",
        help=(
            "Locale code — must be in strings.SUPPORTED_LANGUAGES "
            "(today: fa, en)."
        ),
    )
    import_p.add_argument(
        "po_path",
        type=Path,
        help="Path to the translator's messages.po file.",
    )
    import_p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate every entry but skip the DB UPSERT. Use this "
            "to preview a translator's PR before applying."
        ),
    )
    import_p.add_argument(
        "--updated-by",
        default="i18n_po-import",
        help=(
            "Tag the bot_strings.updated_by audit column. Use a "
            "translator name or PR number for traceability. "
            "Defaults to 'i18n_po-import'."
        ),
    )

    args = parser.parse_args(argv)

    if args.cmd == "export":
        written = _write_locale_files()
        for path in written:
            print(f"wrote {path}")
        return 0
    if args.cmd == "check":
        return _check_locale_files()
    if args.cmd == "import":
        # File-existence pre-flight before spinning up an event
        # loop. Avoids tearing down the default loop just to bail
        # out on a missing file (which contaminates downstream
        # ``asyncio.get_event_loop`` calls in some test runners).
        if not args.po_path.is_file():
            print(
                f"ERROR: .po file not found: {args.po_path}",
                file=sys.stderr,
            )
            return 2
        return asyncio.run(
            _import_cli_async(
                args.lang,
                args.po_path,
                dry_run=args.dry_run,
                updated_by=args.updated_by,
            )
        )
    parser.error(f"unknown command {args.cmd!r}")
    return 2  # unreachable, parser.error exits


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
