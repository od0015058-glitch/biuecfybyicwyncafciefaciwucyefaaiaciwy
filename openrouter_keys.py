"""Multi-key OpenRouter load balancing.

Stage-14-Step-C. Supports up to 10 API keys via numbered env vars:

    OPENROUTER_API_KEY=sk-or-...          # legacy single-key (always works)
    OPENROUTER_API_KEY_1=sk-or-...        # numbered slots 1-10
    OPENROUTER_API_KEY_2=sk-or-...
    ...

Keys are loaded **lazily** on the first call to :func:`key_for_user`
or :func:`key_count` (Stage-15-Step-D #2). Earlier revisions ran
``load_keys()`` at module import time; that was harmless in
production but had two latent issues for tests / scripts that
import ``openrouter_keys`` without the OpenRouter env vars set:

* Every cold import emitted a spurious
  ``"No OPENROUTER_API_KEY* env vars found."`` WARNING into the log
  even when the importer never intended to call OpenRouter.
* Tests that ``monkeypatch.setenv("OPENROUTER_API_KEY", ...)`` had
  to manually call ``openrouter_keys.load_keys()`` after the
  monkeypatch to repopulate ``_keys``, because the eager-at-import
  load had already snapshotted the env *before* the monkeypatch
  ran.

Lazy initialisation fixes both: tests can monkeypatch and call
``key_for_user`` directly, and an importer that never needs an
OpenRouter key (e.g. a small DB-only script) gets a silent import.

Loading rules:

* Keys with a numbered suffix (``_1`` through ``_10``) are added
  first in order.
* If no numbered keys exist, the bare ``OPENROUTER_API_KEY`` is
  used as a single-element list (backward-compatible).
* If both exist, the bare key is ignored and a warning is logged —
  the numbered keys take precedence.

Key selection for a given user is **sticky**: the user's Telegram
id is hashed into a pool index, so the same user always routes to
the same key. This keeps conversation context (on OpenRouter's
side) consistent and avoids mid-conversation key switches.

Public surface:

* :func:`load_keys` — re-read the env and refill ``_keys``. Tests
  and operators with hot-reload tooling can call this; production
  paths don't need to.
* :func:`key_for_user` — returns the API key string for a given
  ``telegram_id``. Lazy-loads on first call. Raises
  ``RuntimeError`` if no keys are configured at first-call time.
* :func:`key_count` — number of keys in the pool. Lazy-loads on
  first call.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger("bot.openrouter_keys")

_keys: list[str] = []
_loaded: bool = False


def load_keys() -> None:
    """(Re-)load API keys from the environment.

    Resets the lazy-load flag so subsequent calls to
    :func:`key_for_user` / :func:`key_count` see the freshly
    populated ``_keys`` list (i.e. tests calling ``load_keys()``
    after a ``monkeypatch.setenv`` see the new keys immediately).
    """
    global _keys, _loaded
    numbered: list[str] = []
    for i in range(1, 11):
        val = os.getenv(f"OPENROUTER_API_KEY_{i}", "").strip()
        if val:
            numbered.append(val)

    bare = os.getenv("OPENROUTER_API_KEY", "").strip()

    if numbered:
        if bare:
            log.warning(
                "Both OPENROUTER_API_KEY and numbered OPENROUTER_API_KEY_N "
                "vars found. Using the %d numbered key(s); ignoring "
                "OPENROUTER_API_KEY.",
                len(numbered),
            )
        _keys = numbered
        log.info("Loaded %d numbered OpenRouter API key(s).", len(_keys))
    elif bare:
        _keys = [bare]
        log.info("Using single OPENROUTER_API_KEY.")
    else:
        _keys = []
        log.warning("No OPENROUTER_API_KEY* env vars found.")
    _loaded = True


def _ensure_loaded() -> None:
    """Trigger lazy-load if it hasn't run yet this process lifetime."""
    if not _loaded:
        load_keys()


def key_for_user(telegram_id: int) -> str:
    """Return the sticky API key for *telegram_id*.

    Raises ``RuntimeError`` if no keys are configured.
    """
    _ensure_loaded()
    if not _keys:
        raise RuntimeError(
            "No OpenRouter API keys configured. Set OPENROUTER_API_KEY "
            "or OPENROUTER_API_KEY_1..10 in your environment."
        )
    idx = telegram_id % len(_keys)
    return _keys[idx]


def key_count() -> int:
    """Number of keys in the pool."""
    _ensure_loaded()
    return len(_keys)
