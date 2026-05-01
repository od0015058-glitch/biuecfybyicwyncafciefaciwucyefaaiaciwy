"""Tests for the runtime gettext lookup layer
(Stage-15-Step-E #7 follow-up #1).

Layered tests:

* :func:`i18n_runtime.gettext_lookup` semantics in isolation.
* :func:`i18n_runtime.init_translations` error paths (missing .po,
  malformed .po, unknown locale, ``msgstr ""``).
* End-to-end ``strings.t()`` lookup chain — verifies that the new
  ``.po`` layer slots in correctly between the admin-override cache
  and the compiled-default ``_STRINGS`` table.
* Bundled bug-fix coverage for ``i18n_po.dump_po``: caller-supplied
  ``project_id_version`` and ``revision_date`` are now escaped
  before being spliced into the header literal.
"""

from __future__ import annotations

import os
import pytest

import i18n_po
import i18n_runtime
import strings


# ---------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_runtime():
    """Drop the i18n_runtime cache between every test so state
    doesn't leak."""
    i18n_runtime.reset_translations()
    yield
    i18n_runtime.reset_translations()


def _write_po(tmp_path, lang: str, body: str) -> None:
    """Write *body* to ``tmp_path/locale/<lang>/LC_MESSAGES/messages.po``."""
    po_dir = tmp_path / "locale" / lang / "LC_MESSAGES"
    po_dir.mkdir(parents=True, exist_ok=True)
    (po_dir / "messages.po").write_text(body, encoding="utf-8")


def _minimal_po(entries: dict[str, str]) -> str:
    """Render a minimal .po body with the given ``{msgid: msgstr}``
    entries, suitable for feeding into ``init_translations``."""
    parts = [
        'msgid ""\n'
        'msgstr ""\n'
        '"Project-Id-Version: test 1.0\\n"\n'
        '"Language: en\\n"\n'
        '"MIME-Version: 1.0\\n"\n'
        '"Content-Type: text/plain; charset=UTF-8\\n"\n'
        '"Content-Transfer-Encoding: 8bit\\n"',
    ]
    for msgid, msgstr in entries.items():
        # We use the dump_po helpers via load_po round-trip — but
        # here we just hand-write minimal entries with safe ASCII.
        m_id = msgid.replace("\\", "\\\\").replace('"', '\\"')
        m_str = msgstr.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'msgid "{m_id}"\nmsgstr "{m_str}"')
    return "\n\n".join(parts) + "\n"


# ---------------------------------------------------------------------
# gettext_lookup — basic semantics
# ---------------------------------------------------------------------


def test_lookup_returns_none_when_uninitialised():
    assert not i18n_runtime.is_initialized()
    assert i18n_runtime.gettext_lookup("en", "any_key") is None


def test_lookup_returns_none_for_unknown_locale(tmp_path):
    _write_po(tmp_path, "en", _minimal_po({"k": "v"}))
    i18n_runtime.init_translations(
        tmp_path / "locale", languages=["en", "fa"]
    )
    assert i18n_runtime.is_initialized()
    # 'de' was not in `languages` → not loaded → None.
    assert i18n_runtime.gettext_lookup("de", "k") is None


def test_lookup_returns_translation_when_present(tmp_path):
    _write_po(tmp_path, "en", _minimal_po({"hello": "Hello!"}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    assert i18n_runtime.gettext_lookup("en", "hello") == "Hello!"


def test_lookup_returns_none_for_missing_key(tmp_path):
    _write_po(tmp_path, "en", _minimal_po({"hello": "Hello!"}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    assert i18n_runtime.gettext_lookup("en", "absent") is None


def test_lookup_treats_empty_msgstr_as_miss(tmp_path):
    """Per the gettext spec, an empty ``msgstr`` means
    'untranslated — fall back to source'. The runtime layer must
    return ``None`` so the caller falls through to the compiled
    default — NOT return the empty string and silently render
    blank UI."""
    _write_po(tmp_path, "en", _minimal_po({"hello": ""}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    assert i18n_runtime.gettext_lookup("en", "hello") is None


def test_lookup_handles_unicode_and_persian(tmp_path):
    """Persian RTL text + ZWNJ + Arabic-presentation forms must
    round-trip cleanly through the .po → in-memory cache."""
    _write_po(
        tmp_path,
        "fa",
        _minimal_po({"greeting": "سلام، خوش\u200cآمدی!"}),
    )
    i18n_runtime.init_translations(tmp_path / "locale", languages=["fa"])
    assert (
        i18n_runtime.gettext_lookup("fa", "greeting")
        == "سلام، خوش\u200cآمدی!"
    )


# ---------------------------------------------------------------------
# init_translations — error paths
# ---------------------------------------------------------------------


def test_init_with_missing_po_file_uses_empty_catalog(tmp_path):
    """A locale without a .po file should still be loaded (with an
    empty catalog) — not crash the bot."""
    counts = i18n_runtime.init_translations(
        tmp_path / "locale", languages=["en", "fa"]
    )
    assert counts == {"en": 0, "fa": 0}
    assert i18n_runtime.is_initialized()
    assert i18n_runtime.gettext_lookup("en", "k") is None


def test_init_with_malformed_po_does_not_crash(tmp_path):
    """A malformed .po file logs an exception but doesn't take down
    the bot — the affected locale just falls through to compiled
    defaults."""
    _write_po(tmp_path, "en", "this is not a valid .po file\n")
    counts = i18n_runtime.init_translations(
        tmp_path / "locale", languages=["en"]
    )
    assert counts == {"en": 0}
    assert i18n_runtime.is_initialized()
    assert i18n_runtime.gettext_lookup("en", "any") is None


def test_init_partial_per_locale(tmp_path):
    """One locale's .po file failing must not leak into another
    locale's catalog."""
    _write_po(tmp_path, "en", _minimal_po({"hello": "Hello!"}))
    _write_po(tmp_path, "fa", "broken po file\n")
    counts = i18n_runtime.init_translations(
        tmp_path / "locale", languages=["en", "fa"]
    )
    assert counts == {"en": 1, "fa": 0}
    assert i18n_runtime.gettext_lookup("en", "hello") == "Hello!"
    assert i18n_runtime.gettext_lookup("fa", "hello") is None


def test_init_excludes_empty_msgstrs_from_count(tmp_path):
    """Empty msgstrs are filtered at load time; the count returned
    by init_translations reflects the *translatable* entries only."""
    _write_po(
        tmp_path,
        "en",
        _minimal_po({"a": "A", "b": "", "c": "C"}),
    )
    counts = i18n_runtime.init_translations(
        tmp_path / "locale", languages=["en"]
    )
    assert counts == {"en": 2}


def test_init_default_locale_dir_uses_module_relative(tmp_path, monkeypatch):
    """When ``locale_dir=None``, the default is ``./locale``
    relative to the i18n_runtime module file."""
    # We don't actually exercise the real locale dir here (it lives
    # at the repo root and is committed); this test just verifies
    # the default-resolution code path doesn't raise.
    counts = i18n_runtime.init_translations(languages=["en", "fa"])
    # The repo ships .po files for both locales, so both should
    # have a positive entry count.
    assert "en" in counts
    assert "fa" in counts


def test_init_is_idempotent_overwrites_cache(tmp_path):
    """Calling init_translations twice with different inputs must
    overwrite the cache, not merge."""
    _write_po(tmp_path, "en", _minimal_po({"a": "A"}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    assert i18n_runtime.gettext_lookup("en", "a") == "A"

    _write_po(tmp_path, "en", _minimal_po({"b": "B"}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    assert i18n_runtime.gettext_lookup("en", "a") is None
    assert i18n_runtime.gettext_lookup("en", "b") == "B"


def test_reset_translations_clears_cache(tmp_path):
    _write_po(tmp_path, "en", _minimal_po({"a": "A"}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    assert i18n_runtime.is_initialized()
    i18n_runtime.reset_translations()
    assert not i18n_runtime.is_initialized()
    assert i18n_runtime.gettext_lookup("en", "a") is None


# ---------------------------------------------------------------------
# strings.t() — end-to-end lookup chain
# ---------------------------------------------------------------------


_STABLE_SLUG = "kbd_wallet"  # known to exist in both en + fa.


def test_t_uses_gettext_layer_when_initialised(tmp_path, monkeypatch):
    """A .po-loaded translation should win over the compiled default
    when no admin override is set."""
    assert _STABLE_SLUG in strings._STRINGS["en"]
    compiled_default = strings._STRINGS["en"][_STABLE_SLUG]

    _write_po(
        tmp_path,
        "en",
        _minimal_po({_STABLE_SLUG: "OVERRIDE_FROM_PO"}),
    )
    # Ensure no admin override is active for this slug.
    strings.set_overrides({})
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])

    assert strings.t("en", _STABLE_SLUG) == "OVERRIDE_FROM_PO"
    assert compiled_default != "OVERRIDE_FROM_PO"


def test_admin_override_wins_over_po(tmp_path):
    """The admin-override cache must take precedence over the
    .po layer — admins should be able to hot-patch a translator's
    bad string without redeploying."""
    _write_po(
        tmp_path,
        "en",
        _minimal_po({_STABLE_SLUG: "FROM_PO"}),
    )
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    strings.set_overrides({("en", _STABLE_SLUG): "FROM_ADMIN_OVERRIDE"})

    try:
        assert strings.t("en", _STABLE_SLUG) == "FROM_ADMIN_OVERRIDE"
    finally:
        strings.set_overrides({})


def test_t_falls_back_to_compiled_default_when_po_empty(tmp_path):
    """An empty msgstr in the .po must NOT mask the compiled
    default."""
    _write_po(tmp_path, "en", _minimal_po({_STABLE_SLUG: ""}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    strings.set_overrides({})
    expected = strings._STRINGS["en"][_STABLE_SLUG]
    assert strings.t("en", _STABLE_SLUG) == expected


def test_t_falls_back_to_default_locale_via_po(tmp_path):
    """When the requested locale has no entry for the key, the
    lookup must fall through to the default locale's .po (not
    just the default locale's _STRINGS)."""
    # ``fa`` is the default locale; ``en`` is the requested.
    # We put a custom translation in the FA .po only, then ask
    # for an EN string for a slug that's missing from EN's
    # _STRINGS table — the lookup should fall back to FA, and
    # FA's .po override should win over FA's _STRINGS.
    fake_slug = "stage_15_step_e_7_test_only_slug"
    monkey_strings = strings._STRINGS
    # Patch _STRINGS in-place: add the slug to FA only (NOT EN).
    monkey_strings["fa"][fake_slug] = "FA_COMPILED_DEFAULT"
    try:
        _write_po(tmp_path, "fa", _minimal_po({fake_slug: "FA_FROM_PO"}))
        i18n_runtime.init_translations(
            tmp_path / "locale", languages=["en", "fa"]
        )
        strings.set_overrides({})
        # Ask for EN — it'll miss EN _STRINGS, miss EN .po, then
        # try FA override (none), FA .po (found "FA_FROM_PO").
        assert strings.t("en", fake_slug) == "FA_FROM_PO"
    finally:
        del monkey_strings["fa"][fake_slug]


def test_t_unchanged_when_runtime_not_initialised():
    """When init_translations was never called, t() must behave
    exactly as it did pre-#7-#1."""
    assert not i18n_runtime.is_initialized()
    strings.set_overrides({})
    expected = strings._STRINGS["en"][_STABLE_SLUG]
    assert strings.t("en", _STABLE_SLUG) == expected


def test_t_format_kwargs_still_work_through_po_layer(tmp_path):
    """Format placeholders in a .po-loaded string must still
    resolve through ``str.format(**kwargs)``."""
    fake_slug = "stage_15_step_e_7_test_format_slug"
    strings._STRINGS["en"][fake_slug] = "Default {x}"
    try:
        _write_po(
            tmp_path,
            "en",
            _minimal_po({fake_slug: "From PO {x}!"}),
        )
        i18n_runtime.init_translations(
            tmp_path / "locale", languages=["en"]
        )
        strings.set_overrides({})
        assert strings.t("en", fake_slug, x="hi") == "From PO hi!"
    finally:
        del strings._STRINGS["en"][fake_slug]


# ---------------------------------------------------------------------
# Debug / introspection
# ---------------------------------------------------------------------


def test_debug_snapshot_initial_state():
    snap = i18n_runtime._debug_snapshot()
    assert snap["initialized"] is False
    assert snap["locale_dir"] is None
    assert snap["entry_counts"] == {}


def test_debug_snapshot_after_init(tmp_path):
    _write_po(tmp_path, "en", _minimal_po({"a": "A", "b": "B"}))
    i18n_runtime.init_translations(tmp_path / "locale", languages=["en"])
    snap = i18n_runtime._debug_snapshot()
    assert snap["initialized"] is True
    assert snap["locale_dir"] == str(tmp_path / "locale")
    assert snap["entry_counts"] == {"en": 2}


# ---------------------------------------------------------------------
# Bundled bug-fix coverage: i18n_po.dump_po header escaping
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "project_id_version,expected_substring",
    [
        # Plain ASCII — no escaping needed, no change.
        ("meowassist 1.0", "Project-Id-Version: meowassist 1.0\\n"),
        # Embedded double-quote — must be escaped to \" so the
        # surrounding "..." literal stays valid.
        (
            'meowassist "beta" 1.0',
            'Project-Id-Version: meowassist \\"beta\\" 1.0\\n',
        ),
        # Embedded backslash — must be escaped to \\.
        (
            "meowassist\\nightly",
            "Project-Id-Version: meowassist\\\\nightly\\n",
        ),
        # Embedded newline — must be escaped to \n (and NOT split
        # the header across two .po lines).
        (
            "meowassist\nbeta",
            "Project-Id-Version: meowassist\\nbeta\\n",
        ),
        # Embedded tab.
        (
            "meowassist\tbeta",
            "Project-Id-Version: meowassist\\tbeta\\n",
        ),
    ],
)
def test_dump_po_escapes_project_id_version(
    project_id_version, expected_substring
):
    """Stage-15-Step-E #7 follow-up #1 bundled bug fix: caller-
    supplied ``project_id_version`` must be escaped before being
    spliced into the header literal. Pre-fix a quote, backslash,
    newline, or tab in the value broke the ``"..."`` quoted-string
    around the header msgstr and produced an unparseable .po file."""
    table = {"en": {"k": "v"}}
    body = i18n_po.dump_po(
        "en",
        strings_table=table,
        project_id_version=project_id_version,
    )
    assert expected_substring in body
    # The output must round-trip through load_po cleanly — pre-fix
    # an embedded quote would raise "unterminated quoted string"
    # OR silently swallow part of the file.
    parsed = i18n_po.load_po(body)
    assert parsed == {"k": "v"}


@pytest.mark.parametrize(
    "revision_date,expected_substring",
    [
        # Empty / None → uses the gettext placeholder unchanged
        # (the placeholder itself contains no special chars).
        (None, "PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\\n"),
        ("", "PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\\n"),
        # Real ISO date — passes through.
        (
            "2024-03-15 10:00+0000",
            "PO-Revision-Date: 2024-03-15 10:00+0000\\n",
        ),
        # Embedded quote must be escaped.
        (
            'date with "quotes"',
            'PO-Revision-Date: date with \\"quotes\\"\\n',
        ),
    ],
)
def test_dump_po_escapes_revision_date(revision_date, expected_substring):
    table = {"en": {"k": "v"}}
    body = i18n_po.dump_po(
        "en",
        strings_table=table,
        revision_date=revision_date,
    )
    assert expected_substring in body
    parsed = i18n_po.load_po(body)
    assert parsed == {"k": "v"}


def test_dump_po_with_quote_in_pidversion_round_trips():
    """End-to-end: pre-fix this would have produced a malformed
    .po body that load_po raises on. Post-fix the round-trip
    survives."""
    table = {"en": {"slug_a": "value A", "slug_b": "value B"}}
    body = i18n_po.dump_po(
        "en",
        strings_table=table,
        project_id_version='meowassist "v1.0"',
        revision_date='2024 "Q1"',
    )
    parsed = i18n_po.load_po(body)
    assert parsed == {"slug_a": "value A", "slug_b": "value B"}
