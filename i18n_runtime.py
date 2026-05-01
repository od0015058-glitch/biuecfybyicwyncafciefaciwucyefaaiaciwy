"""Runtime gettext-style lookup layer (Stage-15-Step-E #7 follow-up #1).

The first slice of Stage-15-Step-E #7 (PR #125) shipped the ``.po``
round-trip foundation: ``i18n_po.dump_po`` exports
``strings._STRINGS`` to ``locale/<lang>/LC_MESSAGES/messages.po`` and
``i18n_po.load_po`` parses it back. The runtime kept reading
``strings._STRINGS`` directly; the ``.po`` files were a *derived
artifact* for community translators (Poedit / Crowdin / OmegaT) to
edit but did not feed the bot's ``t()`` calls.

This module closes that loop. ``init_translations(locale_dir)`` loads
every ``messages.po`` into an in-memory catalog and ``gettext_lookup``
exposes a Pythonic ``Optional[str]`` lookup that ``strings.t()``
consults *between* the admin-override cache and the compiled-default
fallback. A translator can now drop an edited ``messages.po`` into
``locale/<lang>/LC_MESSAGES/`` and the bot picks up the new strings
on the next process restart **without a code deploy**.

The lookup chain in ``strings.t()`` becomes (highest priority first):

1. ``_OVERRIDES[(lang, key)]`` — admin-set override (DB-backed).
2. ``i18n_runtime.gettext_lookup(lang, key)`` — community translation
   from ``locale/<lang>/LC_MESSAGES/messages.po`` (this layer).
3. ``strings._STRINGS[lang][key]`` — compiled default (the source of
   truth that the ``.po`` is exported from).
4. Default-locale fallbacks (override → ``.po`` → compiled).
5. The bare slug + a one-shot WARNING.

Why parse ``.po`` directly instead of compiling to ``.mo`` and using
``gettext.GNUTranslations``?

* Zero deploy-time deps: ``msgfmt`` (the GNU compiler) isn't in
  Python's standard library; we'd have to either ship our own
  binary-format writer or shell out to a system tool. Both add
  friction the first slice deliberately avoided.
* The ``.po`` parser already exists (``i18n_po.load_po``) and is
  battle-tested by 22 round-trip tests + 9 nested-spec tests + the
  CI drift gate.
* Loading is one-time at startup; after that, lookup is a
  ``dict.get`` (no measurable runtime overhead vs. ``_STRINGS``).
* ``stdlib`` ``gettext.GNUTranslations`` doesn't surface a clean
  "translation missing" signal — its ``gettext()`` returns the
  ``msgid`` unchanged on miss, which conflates with a legitimate
  one-to-one translation. The dict-based catalog lets us return
  ``None`` for a miss so the caller can fall through cleanly.

Empty ``msgstr`` semantics
==========================

A ``.po`` entry with an empty ``msgstr`` (``msgstr ""``) is the
gettext convention for "untranslated — fall back to source". This
module treats an empty ``msgstr`` as a *miss* (returns ``None``) so
the caller falls through to the compiled default. Without that
treatment, an untranslated entry would silently render as an empty
string in the bot UI.
"""

from __future__ import annotations

import logging
from pathlib import Path

import i18n_po

log = logging.getLogger(__name__)


# Module-private state. Tests that need a clean slate call
# :func:`reset_translations` before re-initialising.
_TRANSLATIONS: dict[str, dict[str, str]] = {}
_LOCALE_DIR: Path | None = None
_INITIALIZED: bool = False


def init_translations(
    locale_dir: Path | str | None = None,
    *,
    languages: list[str] | tuple[str, ...] | None = None,
) -> dict[str, int]:
    """Load every ``locale/<lang>/LC_MESSAGES/messages.po`` into the
    runtime cache.

    *locale_dir* defaults to ``./locale`` relative to this module.
    *languages* defaults to :data:`strings.SUPPORTED_LANGUAGES`. A
    locale whose ``.po`` file is missing is loaded with an empty
    catalog (so :func:`gettext_lookup` always returns ``None`` for
    that locale and ``strings.t()`` falls through to the compiled
    default).

    Returns a ``{lang: entry_count}`` dict for logging / health
    surfaces. Entries with empty ``msgstr`` are excluded from the
    count because they're treated as misses by ``gettext_lookup``.

    Safe to call multiple times — subsequent calls overwrite the
    cache. Callers must hold a process-level lock if they want
    stable iteration during a reload (the bot doesn't today; an
    init-then-serve flow is enough).
    """
    global _LOCALE_DIR, _TRANSLATIONS, _INITIALIZED

    # Late import to avoid a circular dependency: ``strings`` doesn't
    # depend on this module at import time. ``strings`` only calls
    # ``gettext_lookup`` from ``t()``, which always runs after import
    # is complete.
    import strings

    resolved_dir: Path
    if locale_dir is None:
        resolved_dir = Path(__file__).resolve().parent / "locale"
    else:
        resolved_dir = Path(locale_dir)

    target_languages: tuple[str, ...]
    if languages is None:
        target_languages = tuple(strings.SUPPORTED_LANGUAGES)
    else:
        target_languages = tuple(languages)

    new_cache: dict[str, dict[str, str]] = {}
    counts: dict[str, int] = {}
    for lang in target_languages:
        po_path = resolved_dir / lang / "LC_MESSAGES" / "messages.po"
        if not po_path.is_file():
            log.info(
                "i18n_runtime: no .po file for %r at %s — translations "
                "for that locale will fall through to compiled defaults",
                lang, po_path,
            )
            new_cache[lang] = {}
            counts[lang] = 0
            continue
        try:
            text = po_path.read_text(encoding="utf-8")
            catalog = i18n_po.load_po(text)
        except Exception:
            log.exception(
                "i18n_runtime: failed to load .po for %r at %s — "
                "translations for that locale will fall through to "
                "compiled defaults",
                lang, po_path,
            )
            new_cache[lang] = {}
            counts[lang] = 0
            continue
        # Filter empty msgstrs out of the cache up front. An entry
        # with ``msgstr ""`` is the gettext convention for
        # "untranslated"; we want ``gettext_lookup`` to return
        # ``None`` for those (so the caller falls through to the
        # compiled default), and the cleanest way to enforce that
        # is to drop them at load time.
        translated = {k: v for k, v in catalog.items() if v}
        new_cache[lang] = translated
        counts[lang] = len(translated)

    _LOCALE_DIR = resolved_dir
    _TRANSLATIONS = new_cache
    _INITIALIZED = True
    return counts


def gettext_lookup(lang: str, key: str) -> str | None:
    """Return the translation for *key* in *lang*, or ``None`` if
    the runtime cache hasn't been initialised, the locale is
    unknown, or the entry is missing / empty.

    Empty ``msgstr`` is treated as a miss (returns ``None``) per
    the gettext convention for "untranslated — fall back to source"
    — see the module docstring for the rationale.
    """
    if not _INITIALIZED:
        return None
    cat = _TRANSLATIONS.get(lang)
    if cat is None:
        return None
    value = cat.get(key)
    if not value:
        # Covers both ``None`` (key not in catalog) and ``""``
        # (defensive — empty msgstrs are filtered at load time so
        # this branch is mostly belt-and-braces, but keeps the
        # contract honest if a future caller pre-populates the
        # cache directly).
        return None
    return value


def is_initialized() -> bool:
    """Has :func:`init_translations` been called successfully?"""
    return _INITIALIZED


def reset_translations() -> None:
    """Drop the cache so the next :func:`init_translations` starts
    fresh. Test helper — production code never calls this."""
    global _TRANSLATIONS, _LOCALE_DIR, _INITIALIZED
    _TRANSLATIONS = {}
    _LOCALE_DIR = None
    _INITIALIZED = False


def _debug_snapshot() -> dict[str, object]:
    """Return a JSON-serialisable view of the cache for ops health
    checks. Used by ``/admin/strings`` to surface the loaded-from-
    disk translation counts alongside the in-memory override
    counts. Safe to expose: returns counts, not the actual
    translated strings."""
    return {
        "initialized": _INITIALIZED,
        "locale_dir": str(_LOCALE_DIR) if _LOCALE_DIR is not None else None,
        "entry_counts": {lang: len(cat) for lang, cat in _TRANSLATIONS.items()},
    }


__all__ = [
    "_debug_snapshot",
    "gettext_lookup",
    "init_translations",
    "is_initialized",
    "reset_translations",
]
