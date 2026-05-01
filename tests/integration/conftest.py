"""pytest fixtures for the opt-in Telethon integration suite.

The suite drives the **live** bot via a real Telegram user account
(MTProto, not the Bot API — bots can't DM other bots). It is gated
behind four env vars; when any are missing every test in
``tests/integration/`` is skipped at collection time, so CI's
``pytest -v`` just emits ``SKIPPED [reason]`` lines and stays green.

Required env vars
=================

* ``TG_API_ID`` / ``TG_API_HASH`` — Telegram developer credentials.
  Register a (free) Telegram developer at https://my.telegram.org →
  "API development tools". The api_id is an integer; api_hash is a
  32-char hex string. **These are per-account.** Never reuse the
  production operator's credentials in CI.

* ``TG_TEST_SESSION_STRING`` — a Telethon ``StringSession`` for a
  Telegram user account that will *act as the test client*. Generate
  it once with this throwaway script::

      python -c "
      import asyncio
      from telethon import TelegramClient
      from telethon.sessions import StringSession

      API_ID = int(input('api_id: '))
      API_HASH = input('api_hash: ').strip()

      async def main():
          async with TelegramClient(StringSession(), API_ID, API_HASH) as c:
              print('Session string (paste into TG_TEST_SESSION_STRING):')
              print(c.session.save())

      asyncio.run(main())
      "

  The script will prompt for the phone number + login code on first
  run; subsequent uses of the printed string log in silently. **Treat
  the session string like a password** — anyone holding it can
  impersonate the user account.

* ``TG_TEST_BOT_USERNAME`` — the bot's @username (without the @).
  The integration tests target this username. **Strongly recommend a
  dedicated test bot, not the production bot**, so a flaky test
  can't credit / refund / broadcast to real users.

Optional env vars
=================

* ``TG_TEST_TIMEOUT_SECONDS`` (default ``15``) — how long to wait
  for a bot reply before failing the test.

* ``TG_TEST_SETTLE_SECONDS`` (default ``0.5``) — small delay between
  sending the message and starting to poll for the reply. The bot's
  long-polling loop has its own latency floor; without a settle
  delay we'd hammer the message log before the bot has had a chance
  to write its reply.

How the fixtures compose
========================

* ``integration_secrets`` — dict of the four required vars; calls
  ``pytest.skip`` if any is missing. Session-scoped so the skip
  happens once per pytest run, not per test.

* ``telegram_client`` — connected, logged-in ``TelegramClient``
  yielded for the test, then disconnected on teardown. Session-scoped
  so we don't burn a fresh MTProto handshake per test.

* ``send_and_wait`` — small helper bound to the bot's username; sends
  a message, waits for the next reply from the bot (after the send
  cursor), returns the ``Message`` object. The polling loop is
  bounded by ``TG_TEST_TIMEOUT_SECONDS`` so a misconfigured /
  offline bot fails fast rather than hanging.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

log = logging.getLogger("tests.integration")

_SECRET_VARS = (
    "TG_API_ID",
    "TG_API_HASH",
    "TG_TEST_SESSION_STRING",
    "TG_TEST_BOT_USERNAME",
)


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s=%r is not an int; using default %d", name, raw, default)
        return default
    if value <= 0:
        log.warning("%s=%d is not positive; using default %d", name, value, default)
        return default
    return value


def _read_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        log.warning("%s=%r is not a float; using default %.2f", name, raw, default)
        return default
    if not (value >= 0.0):  # NaN safe — NaN comparison is False on both sides
        log.warning("%s=%r is not >= 0; using default %.2f", name, raw, default)
        return default
    return value


@pytest.fixture(scope="session")
def integration_secrets() -> dict[str, str]:
    """Collect the four required secrets, or skip the whole suite.

    Skips at *fixture-resolution* time, not import time, so the
    integration test files still get *collected* (and lint / static
    analysis still see them) even when secrets are missing — only
    the actual run is short-circuited.
    """
    missing = [v for v in _SECRET_VARS if not os.getenv(v)]
    if missing:
        pytest.skip(
            "integration suite skipped: missing env var(s) "
            + ", ".join(missing)
            + " — see tests/integration/conftest.py for setup"
        )
    return {var: os.environ[var] for var in _SECRET_VARS}


@pytest.fixture(scope="session")
def integration_timeouts() -> dict[str, float]:
    return {
        "reply_seconds": float(_read_int_env("TG_TEST_TIMEOUT_SECONDS", 15)),
        "settle_seconds": _read_float_env("TG_TEST_SETTLE_SECONDS", 0.5),
    }


@pytest.fixture(scope="session")
async def telegram_client(
    integration_secrets: dict[str, str],
) -> AsyncIterator[Any]:
    """Connected, logged-in Telethon client. Session-scoped."""
    # Imported inside the fixture so the module is only required when
    # the suite actually runs (i.e. the secrets gate above passes).
    # Without this, every CI run would need ``telethon`` even though
    # it never executes the integration tests there. The dev
    # requirements file installs telethon for all sessions, but
    # being defensive about the import keeps the gate honest if the
    # operator runs ``pytest`` against the production requirements
    # set (no dev deps).
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:  # pragma: no cover — production requirements path
        pytest.skip(
            "integration suite skipped: telethon not installed "
            "(pip install -r requirements-dev.txt)"
        )

    api_id = int(integration_secrets["TG_API_ID"])
    api_hash = integration_secrets["TG_API_HASH"]
    session_string = integration_secrets["TG_TEST_SESSION_STRING"]

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        pytest.skip(
            "integration suite skipped: TG_TEST_SESSION_STRING is not "
            "an authorised session — regenerate it (see conftest)"
        )
    try:
        yield client
    finally:
        await client.disconnect()


@pytest.fixture
async def send_and_wait(
    telegram_client: Any,
    integration_secrets: dict[str, str],
    integration_timeouts: dict[str, float],
) -> Callable[..., Awaitable[Any]]:
    """Return ``async send_and_wait(text, *, predicate=None) -> Message``.

    Sends ``text`` to the bot and polls iter_messages for the next
    reply *strictly after the send id*. The optional ``predicate``
    lets a test wait for a specific reply when the bot DMs multiple
    times in succession (e.g. an FSM step that posts a confirmation
    AND a follow-up keyboard).
    """
    bot_username = integration_secrets["TG_TEST_BOT_USERNAME"].lstrip("@")

    async def _impl(
        text: str,
        *,
        predicate: Callable[[Any], bool] | None = None,
        timeout_seconds: float | None = None,
    ) -> Any:
        deadline = (
            timeout_seconds
            if timeout_seconds is not None
            else integration_timeouts["reply_seconds"]
        )
        sent = await telegram_client.send_message(bot_username, text)
        await asyncio.sleep(integration_timeouts["settle_seconds"])

        loop = asyncio.get_event_loop()
        end = loop.time() + deadline
        seen_ids: set[int] = set()
        while loop.time() < end:
            async for msg in telegram_client.iter_messages(
                bot_username, min_id=sent.id, limit=20
            ):
                if msg.id in seen_ids:
                    continue
                seen_ids.add(msg.id)
                if msg.out:
                    # Skip our own outgoing copy.
                    continue
                if predicate is None or predicate(msg):
                    return msg
            await asyncio.sleep(0.5)
        raise asyncio.TimeoutError(
            f"timed out after {deadline:.1f}s waiting for a reply from "
            f"@{bot_username} to {text!r}"
        )

    return _impl
