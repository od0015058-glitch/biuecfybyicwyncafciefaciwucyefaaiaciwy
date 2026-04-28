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

If your DB was created from `schema.sql` + `migrations/*.sql` *before*
P3-Op-4 landed, the schema is identical to the post-baseline state but
Alembic doesn't know that yet. **Stamp the DB** at the baseline once,
then `upgrade head` will become a no-op:

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

## Legacy `schema.sql` and `migrations/*.sql`

`schema.sql` and `migrations/001_*.sql` ··· `003_*.sql` are kept in the
repo as historical reference but are **no longer applied automatically**
by `docker-compose.yml`. The Alembic baseline migration
(`0001_baseline.py`) carries the same SQL, so a fresh DB built via
Alembic ends up structurally identical.
