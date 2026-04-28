"""Alembic runtime env.

Builds the SQLAlchemy URL from the same ``DB_*`` env vars the bot uses
(see ``database.Database.connect``) so migrations and the running app
always point at the same database without anyone duplicating
configuration.

We intentionally use the *sync* psycopg2 driver here instead of
asyncpg: alembic itself is sync, only runs at deploy time, and adding
async would just add complexity for zero runtime benefit.
"""

from __future__ import annotations

import os
from logging.config import fileConfig
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import engine_from_config, pool

# Alembic Config object — gives access to alembic.ini values.
config = context.config

# Plumb logging from alembic.ini.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _build_url() -> str:
    """Assemble the connection URL from the bot's DB_* env vars.

    User and password are URL-encoded with ``quote_plus`` so values
    containing characters that are meaningful in a URL (``@``, ``/``,
    ``:``, ``%``, ``#``, ``?``, …) don't corrupt the connection string
    and crash-loop the bot container at startup. Anything in DB_HOST /
    DB_PORT / DB_NAME stays raw — those are operator-supplied
    hostnames and identifiers.
    """
    user = quote_plus(os.getenv("DB_USER", "botuser"))
    password = quote_plus(os.getenv("DB_PASSWORD", ""))
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "aibot_db")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


# Inject the runtime URL so alembic.ini doesn't need it.
config.set_main_option("sqlalchemy.url", _build_url())


# We don't have SQLAlchemy ORM models — the bot uses asyncpg + raw SQL.
# Set target_metadata to None; that disables --autogenerate (which we
# don't want anyway). Migrations are written by hand.
target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — emit SQL to stdout."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode — open a real connection and apply."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
