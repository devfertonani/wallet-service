#!/usr/bin/env bash
# Fault injection during a load run (opt-in: CHAOS=1 loadtest/run.sh steady).
# Waits for the run to start (journal.ndjson appears), then follows a fixed
# timeline relative to that instant and logs every action to events.ndjson:
#
#   t=30s  kill  wallet-2 (SIGKILL, no graceful shutdown)   t=50s  start wallet-2
#   t=65s  stop  wallet-3 (SIGTERM, graceful shutdown)      t=80s  start wallet-3
#   t=90s  pause postgres for 8 s (above lock_timeout and the request deadline)
#   t=105s pause ministack for 10 s (SQS and SNS unreachable, state kept)
#
# Needs a run of at least 2 minutes (the steady default). MiniStack is paused,
# never restarted: it keeps queues in memory and a restart would lose them.
#
# Usage: loadtest/chaos.sh <run directory>
set -euo pipefail

cd "$(dirname "$0")/.."
out=${1:?usage: loadtest/chaos.sh <run directory>}
compose=(docker compose -f docker-compose.yml -f loadtest/compose.load.yml --profile load)
events="$out/events.ndjson"

for _ in $(seq 1 600); do
  [[ -f "$out/journal.ndjson" ]] && break
  sleep 1
done
[[ -f "$out/journal.ndjson" ]] || { echo "chaos.sh: run never started" >&2; exit 1; }
t0=$(date +%s)

at() { # at <offset seconds> <description> <command...>
  local offset=$1 what=$2
  shift 2
  local now
  now=$(date +%s)
  if ((t0 + offset > now)); then sleep $((t0 + offset - now)); fi
  local rc=0
  "$@" >/dev/null 2>&1 || rc=$?
  printf '{"at":"%s","offsetSec":%d,"action":"%s","exit":%d}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" $(($(date +%s) - t0)) "$what" "$rc" >>"$events"
  echo "[chaos.sh +$(($(date +%s) - t0))s] $what (exit $rc)"
}

at 30 "kill wallet-2" "${compose[@]}" kill wallet-2
at 50 "start wallet-2" "${compose[@]}" start wallet-2
at 65 "stop wallet-3 (graceful)" "${compose[@]}" stop wallet-3
at 80 "start wallet-3" "${compose[@]}" start wallet-3
at 90 "pause postgres" "${compose[@]}" pause postgres
at 98 "unpause postgres" "${compose[@]}" unpause postgres
at 105 "pause ministack" "${compose[@]}" pause ministack
at 115 "unpause ministack" "${compose[@]}" unpause ministack
