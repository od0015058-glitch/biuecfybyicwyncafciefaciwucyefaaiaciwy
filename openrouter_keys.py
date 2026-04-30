"""Multi-key OpenRouter load balancing.

Stage-14-Step-C. Supports up to 10 API keys via numbered env vars:

    OPENROUTER_API_KEY=sk-or-...          # legacy single-key (always works)
    OPENROUTER_API_KEY_1=sk-or-...        # numbered slots 1-10
    OPENROUTER_API_KEY_2=sk-or-...
    ...

At boot the module loads every non-empty key into a list:

* Keys with a numbered suffix (``_1`` through ``_10``) are added first
  in order.
* If no numbered keys exist, the bare ``OPENROUTER_API_KEY`` is used
  as a single-element list (backward-compatible).
* If both exist, the bare key is ignored and a warning is logged —
  the numbered keys take precedence.

Key selection for a given user is **sticky**: the user's Telegram id
is hashed into a pool index, so the same user always routes to the
same key. This keeps conversation context (on OpenRouter's side)
consistent and avoids mid-conversation key switches.

Public surface:

* :func:`load_keys` — called once at import time. Re-callable for
  tests.
* :func:`key_for_user` — returns the API key string for a given
  ``telegram_id``. Raises ``RuntimeError`` if no keys are configured.
* :func:`key_count` — number of keys in the pool.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger("bot.openrouter_keys")

_keys: list[str] = []


def load_keys() -> None:
    """(Re-)load API keys from the environment."""
    global _keys
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


def key_for_user(telegram_id: int) -> str:
    """Return the sticky API key for *telegram_id*.

    Raises ``RuntimeError`` if no keys are configured.
    """
    if not _keys:
        raise RuntimeError(
            "No OpenRouter API keys configured. Set OPENROUTER_API_KEY "
            "or OPENROUTER_API_KEY_1..10 in your environment."
        )
    idx = telegram_id % len(_keys)
    return _keys[idx]


def key_count() -> int:
    """Number of keys in the pool."""
    return len(_keys)


# Eagerly load on import so the legacy ``OPENROUTER_API_KEY`` still
# works without any change to ``main.py``.
load_keys()
