# Database migrations (Alembic)

The bot uses [Alembic](https://alembic.sqlalchemy.org/) for schema
migrations as of P3-Op-4.

## Apply migrations

```bash
# Local (with .env in repo root and DB_* vars set):
alembic upgrade head

# Inside docker-compose (the bot's entrypoint already runs this on
# every start, so usually you don't need to do this manually):
docker compose exec bot alembic upgrade head
```

## Existing production deployments

If your DB was created before P3-Op-4 landed (one created via raw
`psql -f schema.sql` + the numbered SQL migrations), the schema is
identical to the post-baseline state but Alembic doesn't know that
yet. **Stamp the DB** at the baseline once, then `upgrade head` will
become a no-op:

```bash
docker compose run --rm bot alembic stamp head
```

After that, future deploys (`docker compose up -d --build`) apply only
new migrations.

## Add a new migration

```bash
alembic revision -m "add_users_phone_number"
# Edit alembic/versions/<rev>_add_users_phone_number.py
# Apply:
alembic upgrade head
```

`target_metadata = None` in `env.py` so `--autogenerate` is intentionally
disabled — we write upgrades and downgrades by hand using `op.execute`,
`op.create_table`, etc. Hand-written migrations are explicit about what
they touch and keep the SQL close to what the bot's raw asyncpg code
expects.

## Removed: legacy `schema.sql` and `migrations/*.sql`

The repo previously kept `schema.sql` and three numbered SQL migration
files (`001_promo_codes.sql`, `002_conversation_memory.sql`,
`003_bump_free_messages_to_10.sql`) as historical reference. They were
deleted in the post-Stage-7 cleanup PR — Alembic owns the schema now,
and the baseline migration (`0001_baseline.py`) carries the exact SQL
that the legacy files would have produced. If you need to inspect the
pre-Alembic state, check out the `main` branch at the merge commit of
PR #44 or earlier.
