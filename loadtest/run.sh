#!/usr/bin/env bash
# Runs one load scenario end to end and writes the report:
#   stack up -> ready -> seed -> run (+ chaos) -> drain + verify -> report
#
# Usage (from anywhere):
#   loadtest/run.sh <scenario> [extra "loadgen run" flags...]
#   loadtest/run.sh steady -rate 300 -duration 3m
#   loadtest/run.sh baseline            # MiniStack ceiling alone, no wallet logic
#
# Environment:
#   CHAOS=1            run loadtest/chaos.sh during the load (steady, >= 2 min)
#   SEED_FLAGS="..."   extra "loadgen seed" flags, e.g. "-wallets 10000"
#   VERIFY_FLAGS="..." extra "loadgen verify" flags, e.g. "-reconcile 200"
#   NO_UP=1            do not run "docker compose up" (stack already running)
#   LOADGEN_CPUS=2     CPU cap of the generator container
#   LIMITS=1           cap CPU and memory of the instances and Postgres
#                      (loadtest/compose.limits.yml; sizes in LIMIT_* there)
#
# Exit code: 0 PASS, 1 FAIL or INVALID, other values for setup errors.
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

scenario=${1:-}
if [[ -z "$scenario" ]]; then
  echo "usage: loadtest/run.sh <baseline|smoke|steady|hot-wallet|duplicates|reversal-first|ramp> [loadgen flags...]" >&2
  exit 2
fi
shift

export LOADGEN_UID LOADGEN_GID
LOADGEN_UID=$(id -u)
LOADGEN_GID=$(id -g)
compose=(docker compose -f docker-compose.yml -f loadtest/compose.load.yml)
if [[ -n "${LIMITS:-}" ]]; then
  compose+=(-f loadtest/compose.limits.yml)
fi
compose+=(--profile load)
loadgen=("${compose[@]}" --profile loadgen run --rm -T loadgen)

log() { printf '[run.sh %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# CPU and memory of every container of the project, every ~2 s, labelled by
# Compose service, so the report can say which component was saturated.
project=${COMPOSE_PROJECT_NAME:-wallet-service}
sample_stats() { # sample_stats <file>
  # Best effort: a container that exits between "ps" and "stats" (the
  # generator) must not stop the sampling, so no errexit/pipefail here.
  set +e +o pipefail
  while :; do
    local ts map
    ts=$(date +%s)
    map=$(docker ps --filter "label=com.docker.compose.project=${project}" --format '{{.Names}},{{.Label "com.docker.compose.service"}}')
    # shellcheck disable=SC2046
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' $(cut -d, -f1 <<<"$map") 2>/dev/null |
      awk -F, -v ts="$ts" 'NR == FNR { svc[$1] = $2; next } { print ts "," svc[$1] "," $2 "," $3 }' <(printf '%s\n' "$map") - >>"$1"
    sleep 1
  done
}
# CPU and memory limits as Docker applies them (not as the Compose files say),
# plus OOM kills and restarts, so a run with NO_UP=1 still records the truth.
limits_of() { # limits_of <service...>
  local svc id
  for svc in "$@"; do
    id=$("${compose[@]}" ps -q "$svc" 2>/dev/null) || continue
    [[ -n "$id" ]] || continue
    docker inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}} {{.RestartCount}} {{.State.OOMKilled}}' "$id" |
      awk -v svc="$svc" '{
        cpu = $1 ? sprintf("%.2f", $1 / 1e9) : "unlimited"
        mem = $2 ? sprintf("%d MiB", $2 / 1048576) : "unlimited"
        printf "  %s cpus=%s mem=%s restarts=%s oom_killed=%s\n", svc, cpu, mem, $3, $4
      }'
  done
}
limited_services=(wallet-1 wallet-2 wallet-3 postgres)

stats_pid=""
stop_stats() {
  if [[ -n "$stats_pid" ]]; then
    kill "$stats_pid" 2>/dev/null || true
  fi
  stats_pid=""
}
trap stop_stats EXIT

name="$(date -u +%Y%m%dT%H%M%SZ)-${scenario}${LIMITS:+-limits}"
out="loadtest/results/${name}"
in="/results/${name}"
mkdir -p "$out"
{
  printf 'loadtest/run.sh %s' "$scenario"
  for a in "$@"; do printf ' %q' "$a"; done
  echo
  if [[ -n "${CHAOS:-}" ]]; then echo "# with CHAOS=${CHAOS}"; fi
  if [[ -n "${LIMITS:-}" ]]; then echo "# with LIMITS=${LIMITS}"; fi
  if [[ -n "${SEED_FLAGS:-}" ]]; then echo "# with SEED_FLAGS=\"${SEED_FLAGS}\""; fi
  if [[ -n "${VERIFY_FLAGS:-}" ]]; then echo "# with VERIFY_FLAGS=\"${VERIFY_FLAGS}\""; fi
} >"$out/command.txt"

if [[ -z "${NO_UP:-}" ]]; then
  log "starting the stack with the load profile (the first time can take a few minutes)"
  "${compose[@]}" up -d --build
fi
log "building the generator image"
"${compose[@]}" --profile loadgen build loadgen >/dev/null

log "waiting for the 3 instances, Keycloak and the queues"
"${loadgen[@]}" wait-ready

log "collecting the environment fingerprint in $out/env.txt"
psql() { "${compose[@]}" exec -T postgres psql -U postgres -d wallet -qAtX "$@"; }
{
  echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)$(git diff --quiet 2>/dev/null || echo ' (working tree dirty)')"
  echo "host: $(uname -srm)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | xargs || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown) x $(getconf _NPROCESSORS_ONLN)"
  echo "ram_host: $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo 2>/dev/null || echo unknown)"
  echo "docker: $(docker version --format '{{.Server.Version}}' 2>/dev/null) ($(docker info --format '{{.OperatingSystem}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes for containers' 2>/dev/null))"
  echo "compose: $(docker compose version --short 2>/dev/null)"
  echo "generator_cpus: ${LOADGEN_CPUS:-2}"
  if [[ -n "${LIMITS:-}" ]]; then echo "limits: on (loadtest/compose.limits.yml)"; else echo "limits: off"; fi
  limits_of "${limited_services[@]}" ministack keycloak
  echo "db_max_conns_per_instance: ${DB_MAX_CONNS:-10 (default)}"
  echo "postgres: $(psql -c 'SHOW server_version')"
  echo "postgres_settings: $(psql -c "SELECT string_agg(name || '=' || setting, ' ') FROM pg_settings WHERE name IN ('fsync','synchronous_commit','full_page_writes','shared_buffers','max_connections','wal_level')")"
  echo "images:"
  "${compose[@]}" images --format '{{.Service}} {{.Repository}}:{{.Tag}} {{.ID}}' 2>/dev/null | sed 's/^/  /' ||
    "${compose[@]}" images | sed 's/^/  /'
} >"$out/env.txt" 2>&1 || log "warning: incomplete fingerprint"

if [[ "$scenario" == baseline ]]; then
  log "measuring the MiniStack ceiling alone (SQS and SNS, without the service)"
  sample_stats "$out/docker_stats.csv" &
  stats_pid=$!
  "${loadgen[@]}" baseline -out "$in" "$@"
  stop_stats
  "${loadgen[@]}" report -out "$in" || true
  log "report: $out/report.md"
  exit 0
fi

# Nobody consumes the audit queue outside a run, and MiniStack slows down as a
# queue grows, so every run starts with it empty (the generator also drains it
# during the run, like a downstream consumer).
log "purging wallet-events-audit.fifo"
"${loadgen[@]}" purge -queues wallet-events-audit.fifo

psql -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; SELECT pg_stat_statements_reset();" >/dev/null 2>&1 ||
  log "warning: pg_stat_statements unavailable (was the stack started without loadtest/compose.load.yml?)"

log "seeding wallets through the API"
# shellcheck disable=SC2086
"${loadgen[@]}" seed -scenario "$scenario" -out "$in" ${SEED_FLAGS:-}

chaos_pid=""
if [[ -n "${CHAOS:-}" ]]; then
  log "chaos on: loadtest/chaos.sh in parallel"
  loadtest/chaos.sh "$out" &
  chaos_pid=$!
fi

limits_of "${limited_services[@]}" >"$out/containers_before.txt"
sample_stats "$out/docker_stats.csv" &
stats_pid=$!
log "generating load: scenario $scenario"
# --use-aliases: Prometheus scrapes the generator as loadgen:9100.
run_rc=0
"${compose[@]}" --profile loadgen run --rm -T --use-aliases loadgen run -scenario "$scenario" -out "$in" "$@" || run_rc=$?
if [[ -n "$chaos_pid" ]]; then
  wait "$chaos_pid" || log "warning: chaos.sh exited with an error"
fi
if [[ $run_rc -ne 0 ]]; then
  log "load generation failed (code $run_rc); verifying anyway"
fi

log "waiting for the drain and checking invariants"
verify_rc=0
# shellcheck disable=SC2086
"${loadgen[@]}" verify -out "$in" ${VERIFY_FLAGS:-} || verify_rc=$?
stop_stats

psql -c "SELECT round(total_exec_time::numeric, 1) AS total_ms, calls, round(mean_exec_time::numeric, 3) AS mean_ms,
                left(regexp_replace(query, '\s+', ' ', 'g'), 140) AS query
         FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'wallet')
         ORDER BY total_exec_time DESC LIMIT 10" -F ' | ' >"$out/pg_stat_statements.txt" 2>/dev/null || rm -f "$out/pg_stat_statements.txt"

# A restart or OOM kill during the run is not a correctness failure by itself
# (the invariants decide that), but it has to be visible next to the numbers.
# The counters live as long as the container, so compare with the start.
limits_of "${limited_services[@]}" >"$out/containers_after.txt"
if ! diff -q "$out/containers_before.txt" "$out/containers_after.txt" >/dev/null; then
  log "warning: a container restarted or was OOM-killed during the run:"
  diff "$out/containers_before.txt" "$out/containers_after.txt" | grep '^>' >&2 || true
fi

report_rc=0
"${loadgen[@]}" report -out "$in" || report_rc=$?
log "report: $out/report.md"
if [[ $verify_rc -eq 3 || $run_rc -ne 0 ]]; then
  exit 3
fi
exit "$report_rc"
