#!/bin/sh
# Container entrypoint.
#
# 1. Apply outstanding Alembic migrations against the configured DB.
#    Idempotent — `alembic upgrade head` is a no-op if nothing's pending.
# 2. Hand off to whatever command the image was started with (default
#    `python main.py`, but `docker compose run` can pass anything).

set -eu

echo "[entrypoint] running alembic upgrade head ..."
alembic upgrade head
echo "[entrypoint] migrations done; exec: $*"

exec "$@"
