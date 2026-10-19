#!/usr/bin/env bash
# The example, unattended: start the stack, capture the 20,000 existing orders,
# run the workload, kill the Worker while it runs, start it again, compare
# ClickHouse with PostgreSQL, then retire the job and its replication slot and
# tear everything down. About three minutes.
#
#   ./run.sh              # leaves nothing running
#   KEEP_UP=1 ./run.sh    # leave the stack up afterwards to look around
#   ./run.sh --self-test  # check the verifier's comparison, without Docker
#
# Every wait is bounded and waits for a condition, never a duration.
set -euo pipefail
cd "$(dirname "$0")"

if [ "${1:-}" = "--self-test" ]; then
    exec ./scripts/verify.py --self-test
fi

COORD="http://localhost:${CLINK_HTTP_PORT:-8081}"
CH="http://localhost:${CLICKHOUSE_HTTP_PORT:-8123}"
WORKLOAD_SECONDS="${WORKLOAD_SECONDS:-60}"

say() { printf '\n==> %s\n' "$*"; }

# poll <seconds> <what> <command...>: retry the command until it succeeds or
# the deadline passes; on the deadline, fail loudly.
poll() {
    local deadline=$(( $(date +%s) + $1 )) what=$2
    shift 2
    until "$@" >/dev/null 2>&1; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "run.sh: gave up waiting for $what" >&2
            return 1
        fi
        sleep 1
    done
}

ch() { curl -sf -u clink:clink "$CH/" --data-binary "$1"; }
pg() { docker compose exec -T postgres psql -U postgres -d shop -tAc "$1"; }
job_running() { curl -sf "$COORD/api/v1/jobs" | grep -q '"status":"RUNNING"'; }
worker_lost() { curl -sf "$COORD/api/v1/cluster" | grep -q '"lost":true'; }
snapshot_landed() { [ "$(ch "SELECT countIf(__op = 'snapshot') FROM orders_changes" 2>/dev/null || echo 0)" -ge 20000 ]; }
streaming_landed() { [ "$(ch "SELECT countIf(__op != 'snapshot') FROM orders_changes" 2>/dev/null || echo 0)" -ge 1000 ]; }
# The Worker keeps its log across a kill and a start, so the restarted
# source's open line is the second one.
source_opens() { docker compose logs --no-color worker 2>&1 | grep -c 'postgres cdc source open:' || true; }
reopened() { [ "$(source_opens)" -ge 2 ]; }

diagnostics() {
    echo
    echo "run.sh: FAILED - diagnostics follow" >&2
    docker compose ps -a || true
    for s in submit coordinator worker postgres; do
        echo "----- docker compose logs $s (tail)"
        docker compose logs --no-color --tail 60 "$s" 2>&1 | cut -c1-240 || true
    done
    echo "----- the CDC source's own lines"
    docker compose logs --no-color worker 2>&1 | grep -E 'source.postgres_cdc' | cut -c1-300 | tail -20 || true
    echo "----- coordinator jobs"; curl -s "$COORD/api/v1/jobs" || true; echo
    echo "----- replication slot"; pg "SELECT slot_name, active, confirmed_flush_lsn, wal_status FROM pg_replication_slots" || true
    echo "----- ClickHouse"; ch "SELECT __op, count() FROM orders_changes GROUP BY __op FORMAT PrettyCompact" || true
}

WORKLOAD=
cleanup() {
    local rc=$?
    [ -n "$WORKLOAD" ] && kill "$WORKLOAD" 2>/dev/null || true
    if [ "$rc" -ne 0 ]; then diagnostics; fi
    if [ "${KEEP_UP:-0}" = "1" ]; then
        echo; echo "run.sh: KEEP_UP=1, leaving the stack running (./scripts/cleanup.sh removes it)"
    else
        say "retiring the job and its replication slot, removing the stack"
        ./scripts/cleanup.sh >/dev/null 2>&1 || docker compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    exit "$rc"
}
trap cleanup EXIT

say "starting from a clean slate"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true

say "starting PostgreSQL (20,000 orders), ClickHouse and clink; submitting pipeline.sql"
docker compose up -d
poll 90 "the job to be RUNNING" job_running
curl -s "$COORD/api/v1/jobs"; echo

say "waiting for the initial snapshot to reach ClickHouse"
poll 120 "the 20,000 snapshot rows in ClickHouse" snapshot_landed
docker compose logs --no-color worker 2>&1 | grep -oE 'snapshot complete: .*' | tail -1

say "running the workload for ${WORKLOAD_SECONDS} s (in the background)"
./scripts/workload.sh "$WORKLOAD_SECONDS" &
WORKLOAD=$!

say "waiting for streamed changes to land"
poll 120 "1,000 streamed changes in ClickHouse" streaming_landed
echo "changes in ClickHouse so far: $(ch "SELECT countIf(__op != 'snapshot') FROM orders_changes")"

say "killing the Worker (SIGKILL) while the workload is still committing"
docker compose kill worker
poll 60 "the Coordinator to declare the Worker lost" worker_lost
echo "the Coordinator has declared the Worker lost"

say "starting the Worker again"
docker compose start worker
poll 90 "the job to be RUNNING again" job_running
poll 90 "the CDC source to open again" reopened
echo "the restarted source's $(docker compose logs --no-color worker 2>&1 | grep -oE 'plan: .*' | tail -1)"

say "waiting for the workload to finish"
wait "$WORKLOAD"
WORKLOAD=

say "comparing ClickHouse with PostgreSQL"
./scripts/verify.py

say "what the Coordinator logged about the recovery"
docker compose logs --no-color coordinator 2>&1 | grep -iE 'lost|restart|restor' | cut -c1-200 | tail -8 || true

say "the replication slot, confirmed up to the last completed checkpoint"
pg "SELECT slot_name, active, confirmed_flush_lsn, wal_status FROM pg_replication_slots"

say "PASS"
