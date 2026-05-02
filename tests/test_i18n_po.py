"""Tests for :mod:`i18n_po` — gettext .po round-trip.

Stage-15-Step-E #7 first slice. Pins:

* :func:`i18n_po.dump_po` is byte-stable for a given dict.
* :func:`i18n_po.load_po` is the inverse of :func:`i18n_po.dump_po`
  for every key in the live ``strings._STRINGS`` table.
* The on-disk ``locale/<lang>/LC_MESSAGES/messages.po`` files match
  the dict export — drift fails CI so adding a slug without
  re-exporting is impossible to merge.
* Edge cases for the .po format itself: multi-line strings, escape
  sequences, blank values, ``msgctxt`` rejection, tolerant comment
  parsing.
"""

from __future__ import annotations

import pytest

import i18n_po
import strings


# ---------------------------------------------------------------------
# Round-trip — every slug survives dump → load
# ---------------------------------------------------------------------


@pytest.mark.parametrize("lang", strings.SUPPORTED_LANGUAGES)
def test_round_trip_every_slug_survives(lang):
    body = i18n_po.dump_po(lang)
    parsed = i18n_po.load_po(body)
    expected = strings._STRINGS[lang]
    # No missing / extra / mismatched keys — full bidirectional
    # equality. The set comparison gives a clear diff if it ever
    # regresses.
    assert set(parsed.keys()) == set(expected.keys()), (
        f"missing or extra slugs in round-trip for {lang}"
    )
    for key, value in expected.items():
        assert parsed[key] == value, (
            f"value drift for {lang}/{key}:\n"
            f"  expected: {value!r}\n"
            f"  got:      {parsed[key]!r}"
        )


def test_round_trip_preserves_multiline_with_embedded_newlines():
    # The greeting + hub messages are multi-line — make sure the
    # multi-line .po form ("" + continuation) survives intact.
    table = {
        "fa": {"greeting": "خط اول\nخط دوم\nخط سوم"},
        "en": {"greeting": "Line one\nLine two\nLine three"},
    }
    for lang in ("fa", "en"):
        body = i18n_po.dump_po(lang, strings_table=table, default_lang="fa")
        parsed = i18n_po.load_po(body)
        assert parsed["greeting"] == table[lang]["greeting"]


def test_round_trip_preserves_embedded_quotes_and_backslashes():
    table = {
        "fa": {"k": 'has "quotes" and \\backslash\\ chars'},
    }
    body = i18n_po.dump_po("fa", strings_table=table, default_lang="fa")
    parsed = i18n_po.load_po(body)
    assert parsed["k"] == 'has "quotes" and \\backslash\\ chars'


def test_round_trip_preserves_tab_character():
    # Tab is one of the four characters .po escapes; pin the encode
    # / decode so a future refactor doesn't drop it.
    table = {"fa": {"k": "col1\tcol2"}}
    body = i18n_po.dump_po("fa", strings_table=table, default_lang="fa")
    parsed = i18n_po.load_po(body)
    assert parsed["k"] == "col1\tcol2"


def test_round_trip_empty_string_value():
    # An empty msgstr is legal gettext (means "untranslated"). We
    # store empty strings in the dict in a few places (legacy
    # placeholder slugs that haven't been wired yet).
    table = {"fa": {"placeholder_unset": ""}}
    body = i18n_po.dump_po("fa", strings_table=table, default_lang="fa")
    parsed = i18n_po.load_po(body)
    assert parsed == {"placeholder_unset": ""}


def test_round_trip_persian_rtl_unicode_passthrough():
    # Persian + Arabic RTL mark + ZWNJ — make sure the UTF-8
    # passthrough doesn't mangle non-ASCII codepoints (regression
    # pin: a future refactor that adds ``s.encode('ascii')`` would
    # silently lose every Persian string).
    persian = "سلام\u200c{user}\u200f."
    table = {"fa": {"greet": persian}}
    body = i18n_po.dump_po("fa", strings_table=table, default_lang="fa")
    parsed = i18n_po.load_po(body)
    assert parsed["greet"] == persian


# ---------------------------------------------------------------------
# Dump output stability — byte-identical re-exports
# ---------------------------------------------------------------------


@pytest.mark.parametrize("lang", strings.SUPPORTED_LANGUAGES)
def test_dump_po_is_deterministic_across_calls(lang):
    """Two consecutive calls must produce byte-identical output.

    Catches accidental dependence on dict iteration order or
    timestamp-style fields. The CI drift gate relies on this — if
    ``dump_po`` weren't deterministic, the on-disk .po file would
    spuriously fail ``check``.
    """
    a = i18n_po.dump_po(lang)
    b = i18n_po.dump_po(lang)
    assert a == b


def test_dump_po_revision_date_uses_placeholder_by_default():
    # The default placeholder keeps the file byte-stable so re-
    # running the export doesn't show a header-only diff.
    body = i18n_po.dump_po("en", strings_table={"en": {"k": "v"}})
    assert "PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE" in body


def test_dump_po_revision_date_can_be_overridden_for_test():
    body = i18n_po.dump_po(
        "en",
        strings_table={"en": {"k": "v"}},
        revision_date="2026-04-30 12:00+0000",
    )
    assert "PO-Revision-Date: 2026-04-30 12:00+0000" in body


def test_dump_po_unknown_locale_raises_value_error():
    with pytest.raises(ValueError, match="unknown locale"):
        i18n_po.dump_po(
            "klingon",
            strings_table={"fa": {"k": "v"}, "en": {"k": "v"}},
            default_lang="fa",
        )


def test_dump_po_includes_default_locale_as_translator_comment():
    """Translators reading the .po file in their tool of choice see
    the source-locale text as a ``#.`` comment. Source-locale .po
    files don't get the comment (would be redundant)."""
    table = {
        "fa": {"k": "خط فارسی"},
        "en": {"k": "Source line"},
    }
    fa_body = i18n_po.dump_po("fa", strings_table=table, default_lang="fa")
    en_body = i18n_po.dump_po("en", strings_table=table, default_lang="fa")
    # English is non-default → comment shows the Persian default.
    assert "#. خط فارسی" in en_body
    # Persian is default → no #. comment for the same slug.
    assert "#. " not in fa_body.split('msgid "k"')[0].split("msgid \"\"")[1]


# ---------------------------------------------------------------------
# Parse — strict + tolerant
# ---------------------------------------------------------------------


def test_load_po_skips_header_entry():
    """The leading ``msgid ""`` block is metadata only — must not
    appear in the parsed dict."""
    body = (
        'msgid ""\n'
        'msgstr "Project-Id-Version: x\\n"\n'
        '\n'
        'msgid "real_slug"\n'
        'msgstr "real_value"\n'
    )
    parsed = i18n_po.load_po(body)
    assert parsed == {"real_slug": "real_value"}
    assert "" not in parsed


def test_load_po_tolerates_blank_lines_and_comments():
    body = (
        '# top-level comment\n'
        '\n'
        'msgid ""\n'
        'msgstr ""\n'
        '\n'
        '# slug a\n'
        '#. translator note\n'
        '#: source.py:42\n'
        '#, fuzzy\n'
        'msgid "a"\n'
        'msgstr "alpha"\n'
        '\n\n\n'
        'msgid "b"\n'
        'msgstr "beta"\n'
    )
    assert i18n_po.load_po(body) == {"a": "alpha", "b": "beta"}


def test_load_po_msgctxt_is_rejected():
    body = (
        'msgctxt "menu"\n'
        'msgid "Save"\n'
        'msgstr "ذخیره"\n'
    )
    with pytest.raises(ValueError, match="msgctxt is not supported"):
        i18n_po.load_po(body)


def test_load_po_rejects_duplicate_msgid():
    body = (
        'msgid "a"\n'
        'msgstr "first"\n'
        '\n'
        'msgid "a"\n'
        'msgstr "second"\n'
    )
    with pytest.raises(ValueError, match="duplicate msgid"):
        i18n_po.load_po(body)


def test_load_po_rejects_orphan_continuation_line():
    body = (
        '"orphan continuation"\n'
        'msgid "x"\n'
        'msgstr "y"\n'
    )
    with pytest.raises(ValueError, match="orphan continuation"):
        i18n_po.load_po(body)


def test_load_po_rejects_unterminated_quote():
    body = (
        'msgid "good"\n'
        'msgstr "missing closing quote\n'
    )
    with pytest.raises(ValueError, match="unterminated"):
        i18n_po.load_po(body)


def test_load_po_unknown_escape_sequence_passes_through():
    """gettext doesn't support \\u escapes — Python's ``\\u200c``
    written verbatim should round-trip as ``\\u200c`` characters
    (the four ASCII bytes), not be mistakenly decoded."""
    body = 'msgid "k"\nmsgstr "alpha\\u200c"\n'
    # Backslash + 'u' isn't a known escape, so the parser must
    # keep both characters. The \\u200c in the original source
    # appears verbatim as 4 ASCII chars after the alpha.
    parsed = i18n_po.load_po(body)
    assert parsed == {"k": "alpha\\u200c"}


# ---------------------------------------------------------------------
# CI drift gate — the on-disk .po files must match the dict export
# ---------------------------------------------------------------------


def test_on_disk_po_files_match_strings_dict():
    """Adding a slug to ``strings._STRINGS`` without re-exporting the
    .po files must fail CI.

    Run ``python -m i18n_po export`` to regenerate the .po files
    after editing ``strings.py``. The exit code of ``i18n_po check``
    is what CI consumes; this test exercises the same logic so a
    drift surfaces in pytest output rather than only in the .po
    workflow.
    """
    rc = i18n_po._check_locale_files()
    assert rc == 0, (
        "On-disk locale/<lang>/LC_MESSAGES/messages.po files differ from "
        "strings._STRINGS. Run: python -m i18n_po export"
    )


@pytest.mark.parametrize("lang", strings.SUPPORTED_LANGUAGES)
def test_on_disk_po_file_round_trips_to_strings_dict(lang):
    """Belt-and-suspenders for the drift gate: read the on-disk
    file (not the dict export) and assert it parses back to
    ``_STRINGS[lang]``. If a non-developer edits the .po file
    manually (via Poedit / Crowdin) and then someone runs
    ``i18n_po`` import in a follow-up, the parse must succeed."""
    path = i18n_po.po_path(lang)
    body = path.read_text(encoding="utf-8")
    parsed = i18n_po.load_po(body)
    assert parsed == strings._STRINGS[lang]


# ---------------------------------------------------------------------
# Bundled bug fix: extract_format_fields descends into format spec
# ---------------------------------------------------------------------


class TestExtractFormatFieldsNestedSpec:
    """Pre-fix, ``extract_format_fields`` ignored the format-spec
    portion of every placeholder, so a nested kwarg like
    ``{amount:.{precision}f}`` was reported as ``{"amount"}`` only.

    The follow-on impact: ``validate_override`` accepted the
    override (no "extra placeholder" error because the spec kwarg
    didn't appear in the extracted set), but the runtime
    ``template.format(**kwargs)`` then raised ``KeyError`` for the
    nested kwarg, falling through to the bare-slug fallback so the
    operator's override silently never rendered.

    Fix: descend into ``format_spec`` recursively and extract its
    placeholders too.
    """

    def test_nested_simple_placeholder_is_extracted(self):
        assert strings.extract_format_fields("{x:{w}}") == {"x", "w"}

    def test_decimal_precision_via_spec_is_extracted(self):
        # The realistic case: an admin override that wants
        # variable precision, e.g. for displaying a balance.
        assert strings.extract_format_fields(
            "Balance: ${amount:.{precision}f}"
        ) == {"amount", "precision"}

    def test_double_nested_placeholder_is_extracted(self):
        # Pathological but legal: ``{x:{w:{n}}}`` — gettext / Poedit
        # may eventually emit such templates if a translator wants
        # full control. Confirm the recursion handles arbitrary
        # depth.
        assert strings.extract_format_fields("{x:{w:{n}}}") == {
            "x", "w", "n",
        }

    def test_mixed_top_level_and_nested_placeholders(self):
        assert strings.extract_format_fields(
            "{a} and {b:.{p}f} and {c}"
        ) == {"a", "b", "c", "p"}

    def test_indexed_field_with_nested_spec(self):
        # ``{items[0]:.{precision}f}`` — top-level kwarg is
        # ``items`` (the index access doesn't change which kwarg
        # the caller has to pass), and the nested spec adds
        # ``precision``.
        assert strings.extract_format_fields(
            "{items[0]:.{precision}f}"
        ) == {"items", "precision"}

    def test_validate_override_now_rejects_nested_unknown_placeholder(
        self, monkeypatch
    ):
        """End-to-end pin: the validator now catches nested-spec
        kwargs that aren't in the compiled default's placeholder
        set. Pre-fix the validator silently accepted the override
        and the runtime fell back to the bare slug."""
        # Stub a compiled default that only declares ``balance``.
        monkeypatch.setitem(
            strings._STRINGS["fa"], "_test_slug", "{balance:.2f}"
        )
        try:
            err = strings.validate_override(
                "fa",
                "_test_slug",
                # Override tries to thread a 'precision' kwarg
                # through the nested spec — but the call site
                # only passes 'balance'.
                "{balance:.{precision}f}",
            )
            assert err is not None
            assert "precision" in err
        finally:
            del strings._STRINGS["fa"]["_test_slug"]

    def test_malformed_nested_syntax_does_not_break_outer_extract(self):
        """If the nested spec itself contains a positional placeholder
        (which the inner recursion rejects with ValueError), the
        outer extract still returns the top-level fields. The
        runtime ``template.format`` will raise the clean error at
        render time, which gives a more useful traceback than a
        swallowed inner ValueError."""
        # ``{x:{0}}`` — outer is valid (spec is the literal ``{0}``
        # opaque string); inner recursion sees ``{0}``, rejects it
        # as a positional placeholder. The try/except in
        # extract_format_fields swallows the inner error and we
        # return just the outer field.
        result = strings.extract_format_fields("{x:{0}}")
        # Outer field is the strict guarantee; inner is best-effort.
        assert result == {"x"}

    def test_malformed_nested_syntax_with_named_inner_recovers_field(self):
        """The other malformed case: ``{x:{}}`` — outer is valid
        (spec is literal ``{}``), inner recursion sees ``{}`` and
        rejects it as a positional auto-numbered placeholder.
        Same try/except path; outer field still extracted."""
        result = strings.extract_format_fields("{x:{}}")
        assert result == {"x"}


# ---------------------------------------------------------------------
# Bundled bug fix (Stage-15-Step-E #7 follow-up #3): orphan-locale
# detection in the .po drift gate
# ---------------------------------------------------------------------


def _stage_supported_locales(locale_dir):
    """Helper: write a clean .po for every supported locale into
    *locale_dir* so the per-supported-locale loop passes and only
    the orphan-detection path can trip the gate."""
    for lang in strings.SUPPORTED_LANGUAGES:
        d = locale_dir / lang / "LC_MESSAGES"
        d.mkdir(parents=True, exist_ok=True)
        (d / "messages.po").write_text(
            i18n_po.dump_po(lang), encoding="utf-8"
        )


class TestOrphanLocaleDetection:
    """Pre-fix, ``_check_locale_files`` only iterated
    :data:`strings.SUPPORTED_LANGUAGES`, so a stale
    ``locale/<lang>/LC_MESSAGES/messages.po`` for a locale that had
    been removed from the supported set lingered on disk forever.
    Translators on Crowdin still saw the file, but
    :func:`i18n_runtime.init_translations` doesn't load locales
    outside the supported set, so any edits silently never reached
    users. The fix adds an orphan scan that flags any such file
    with a clear remediation hint.
    """

    def test_clean_locale_dir_is_not_drift(self, tmp_path):
        _stage_supported_locales(tmp_path)
        rc = i18n_po._check_locale_files(locale_dir=tmp_path)
        assert rc == 0

    def test_orphan_locale_with_po_file_is_drift(self, tmp_path, capsys):
        _stage_supported_locales(tmp_path)
        # Stage an orphan locale that is *not* in
        # SUPPORTED_LANGUAGES — pick a code that's clearly outside
        # the today's set ('de' for German).
        orphan_lang = "de"
        assert orphan_lang not in strings.SUPPORTED_LANGUAGES
        orphan_dir = tmp_path / orphan_lang / "LC_MESSAGES"
        orphan_dir.mkdir(parents=True)
        (orphan_dir / "messages.po").write_text(
            'msgid ""\nmsgstr ""\n',
            encoding="utf-8",
        )
        rc = i18n_po._check_locale_files(locale_dir=tmp_path)
        assert rc == 1
        out = capsys.readouterr().out
        # The remediation hint must name both the orphan locale
        # and the surface the developer should edit.
        assert "orphan locale 'de'" in out
        assert "SUPPORTED_LANGUAGES" in out
        assert "messages.po" in out

    def test_orphan_locale_directory_without_po_is_ignored(
        self, tmp_path
    ):
        """An empty `locale/<lang>` directory (no `.po` file inside)
        is ignored — that's the harmless leftover of a `mkdir -p`
        that didn't get a write. We only flag the case where a
        translator could actually save edits into a dead file."""
        _stage_supported_locales(tmp_path)
        # Make a directory but no .po inside.
        (tmp_path / "ru" / "LC_MESSAGES").mkdir(parents=True)
        rc = i18n_po._check_locale_files(locale_dir=tmp_path)
        assert rc == 0

    def test_loose_files_at_locale_root_are_ignored(self, tmp_path):
        """Files like `locale/README.md` or `locale/.gitkeep` aren't
        directories, so the orphan scan doesn't try to interpret
        them as locale folders."""
        _stage_supported_locales(tmp_path)
        (tmp_path / "README.md").write_text("# locale\n")
        (tmp_path / ".gitkeep").write_text("")
        rc = i18n_po._check_locale_files(locale_dir=tmp_path)
        assert rc == 0

    def test_orphan_drift_is_orthogonal_to_per_locale_drift(
        self, tmp_path, capsys
    ):
        """Both classes of drift are reported in the same run —
        fixing the orphan first shouldn't surprise the developer
        with a second drift error after they re-run the gate."""
        # Stage supported locales correctly *except* one — make en
        # drift. Both errors should be in the output simultaneously.
        for lang in strings.SUPPORTED_LANGUAGES:
            d = tmp_path / lang / "LC_MESSAGES"
            d.mkdir(parents=True, exist_ok=True)
            if lang == "en":
                # Intentional drift: stale content.
                (d / "messages.po").write_text(
                    "# stale\n", encoding="utf-8"
                )
            else:
                (d / "messages.po").write_text(
                    i18n_po.dump_po(lang), encoding="utf-8"
                )
        # Add an orphan.
        orphan_dir = tmp_path / "de" / "LC_MESSAGES"
        orphan_dir.mkdir(parents=True)
        (orphan_dir / "messages.po").write_text(
            'msgid ""\nmsgstr ""\n', encoding="utf-8"
        )
        rc = i18n_po._check_locale_files(locale_dir=tmp_path)
        assert rc == 1
        out = capsys.readouterr().out
        assert "differs from strings._STRINGS export" in out
        assert "orphan locale 'de'" in out

    def test_default_locale_dir_still_clean_in_repo(self):
        """Belt-and-suspenders: the existing on-disk locale dir
        passes the new gate with no orphans (the repo wouldn't
        otherwise pass CI). Pins that the orphan scan doesn't
        flag the supported `fa` / `en` directories themselves."""
        rc = i18n_po._check_locale_files()
        assert rc == 0
