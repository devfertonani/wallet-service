#!/usr/bin/env bash
# Recreates the wallet database and empties the queues, keeping the rest of
# the stack (Keycloak, MiniStack, volumes of other services) as is.
#
# Runs do not need a reset: each one seeds fresh wallets and the journal
# check covers only those. Reset when the database grew too big or a run left
# it in a state you do not want to carry over.
#
# The ledger is append-only even for the schema owner (TRUNCATE and DELETE
# are refused by triggers), so the reset drops and recreates the database
# instead of truncating, then applies the migrations again.
#
# Usage: loadtest/reset.sh --yes
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ "${1:-}" != "--yes" ]]; then
  echo "loadtest/reset.sh deletes ALL data in the wallet database. Confirm with: loadtest/reset.sh --yes" >&2
  exit 2
fi
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi
export LOADGEN_UID LOADGEN_GID
LOADGEN_UID=$(id -u)
LOADGEN_GID=$(id -g)
compose=(docker compose -f docker-compose.yml -f loadtest/compose.load.yml)
# Keep the limits of a stack started with LIMITS=1 loadtest/run.sh.
if [[ -n "${LIMITS:-}" ]]; then
  compose+=(-f loadtest/compose.limits.yml)
fi
compose+=(--profile load)
log() { printf '[reset.sh %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

log "stopping the instances"
"${compose[@]}" stop wallet-1 wallet-2 wallet-3 postgres-exporter >/dev/null 2>&1 || true

log "recreating the wallet database (same roles and privileges as deploy/postgres/init.sql)"
"${compose[@]}" exec -T postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q <<'SQL'
DROP DATABASE IF EXISTS wallet WITH (FORCE);
CREATE DATABASE wallet OWNER wallet_owner;
REVOKE ALL ON DATABASE wallet FROM PUBLIC;
GRANT CONNECT ON DATABASE wallet TO wallet_app;
SQL

log "applying the migrations"
"${compose[@]}" run --rm -T migrate

log "purging the queues"
"${compose[@]}" --profile loadgen run --rm -T loadgen purge

log "starting the instances"
"${compose[@]}" up -d
log "done"
