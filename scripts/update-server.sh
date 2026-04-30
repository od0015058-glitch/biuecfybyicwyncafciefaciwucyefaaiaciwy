#!/usr/bin/env bash
# ── Meowassist server update with backup rotation ──
#
# Usage:
#   cd /opt/meowassist && sudo bash scripts/update-server.sh
#
# What it does:
#   1. Backs up the current version to /opt/meowassist-backups/YYYY-MM-DD_HH-MM/
#      (everything EXCEPT .env, docker volumes, and __pycache__)
#   2. Pulls the latest code from origin/main
#   3. Rebuilds Docker images and restarts containers
#   4. Rotates backups — keeps the 2 most recent, deletes older ones
#
# Your .env is NEVER touched — it stays in place across updates.
# The database lives in a Docker volume — also untouched.
#
# Backup structure after a few updates:
#   /opt/meowassist-backups/
#   ├── 2026-05-02_14-30/    ← most recent (before this update)
#   └── 2026-05-01_09-15/    ← previous
#   (older backups are automatically deleted)
#
# Override defaults with env vars:
#   PROJECT_DIR=/opt/meowassist BACKUP_ROOT=/opt/meowassist-backups KEEP_BACKUPS=2

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/opt/meowassist}"
BACKUP_ROOT="${BACKUP_ROOT:-/opt/meowassist-backups}"
KEEP_BACKUPS="${KEEP_BACKUPS:-2}"

TIMESTAMP=$(date +%F_%H-%M)
BACKUP_DIR="$BACKUP_ROOT/$TIMESTAMP"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  Meowassist update — $TIMESTAMP         ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── 0. Sanity checks ──
if [ ! -d "$PROJECT_DIR/.git" ]; then
    echo "ERROR: $PROJECT_DIR does not look like a git repo."
    echo "       Expected to find $PROJECT_DIR/.git"
    exit 1
fi

if [ ! -f "$PROJECT_DIR/docker-compose.yml" ]; then
    echo "ERROR: $PROJECT_DIR/docker-compose.yml not found."
    exit 1
fi

# ── 1. Back up current version ──
echo "→ Creating backup at $BACKUP_DIR …"
mkdir -p "$BACKUP_DIR"
rsync -a \
    --exclude='.env' \
    --exclude='__pycache__' \
    --exclude='.git' \
    --exclude='*.pyc' \
    --exclude='.mypy_cache' \
    --exclude='.pytest_cache' \
    "$PROJECT_DIR/" "$BACKUP_DIR/"
echo "  ✓ Backup created: $BACKUP_DIR"

# ── 2. Rotate old backups (keep only $KEEP_BACKUPS most recent) ──
echo "→ Rotating backups (keeping $KEEP_BACKUPS) …"
cd "$BACKUP_ROOT"
# shellcheck disable=SC2012
ls -dt */ 2>/dev/null | tail -n +$((KEEP_BACKUPS + 1)) | xargs -r rm -rf
echo "  ✓ Old backups cleaned"

# ── 3. Pull latest code ──
echo "→ Pulling latest code …"
cd "$PROJECT_DIR"
BEFORE_SHA=$(git rev-parse --short HEAD)
git fetch origin main
git checkout main
git reset --hard origin/main
AFTER_SHA=$(git rev-parse --short HEAD)
echo "  ✓ Updated: $BEFORE_SHA → $AFTER_SHA"

# ── 4. Rebuild & restart containers ──
echo "→ Rebuilding containers …"
docker compose up -d --build
echo "  ✓ Bot containers rebuilt and restarted"

# ── 5. Restart Caddy if its compose file exists ──
if [ -f "$PROJECT_DIR/docker-compose.caddy.yml" ]; then
    echo "→ Restarting Caddy …"
    docker compose -f docker-compose.caddy.yml up -d
    echo "  ✓ Caddy restarted"
fi

# ── 6. Summary ──
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  Update complete!                            ║"
echo "║                                              ║"
echo "║  Previous version backed up to:              ║"
echo "║    $BACKUP_DIR"
echo "║                                              ║"
echo "║  Verify health:                              ║"
echo "║    docker compose logs -f bot                ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
