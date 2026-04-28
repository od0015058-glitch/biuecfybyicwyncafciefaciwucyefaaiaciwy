# syntax=docker/dockerfile:1.7
#
# Production image for the Meowassist Telegram bot.
#
# Two stage:
#   1. `builder` installs Python deps into a virtualenv so we can copy
#      just /opt/venv into the runtime image (no pip / build cache
#      bloat in production).
#   2. `runtime` is a slim base + the venv + the source. Runs as a
#      non-root user.
#
# Build:    docker build -t meowassist-bot .
# Run:      docker run --env-file .env -p 8080:8080 meowassist-bot
# Compose:  docker compose up -d  (boots Postgres + bot together)

# ---- builder ----------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Create the venv first so the wheel install layer caches independently
# of source changes.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt ./
RUN pip install -r requirements.txt

# ---- runtime ----------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Non-root user. Bots listen on 8080 (unprivileged) so we don't need
# any extra capabilities.
RUN groupadd --system --gid 1000 bot \
 && useradd  --system --uid 1000 --gid 1000 --home /app --shell /usr/sbin/nologin bot

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=bot:bot . /app

USER bot

# Webhook listener. The aiogram long-poll loop does not bind a port.
EXPOSE 8080

CMD ["python", "main.py"]
