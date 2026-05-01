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
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

import strings

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

    header_msgstr = (
        f"Project-Id-Version: {project_id_version}\\n"
        f"Report-Msgid-Bugs-To: \\n"
        f"PO-Revision-Date: {revision_date or 'YEAR-MO-DA HO:MI+ZONE'}\\n"
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
    args = parser.parse_args(argv)

    if args.cmd == "export":
        written = _write_locale_files()
        for path in written:
            print(f"wrote {path}")
        return 0
    if args.cmd == "check":
        return _check_locale_files()
    parser.error(f"unknown command {args.cmd!r}")
    return 2  # unreachable, parser.error exits


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
