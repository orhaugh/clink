#!/usr/bin/env bash
# The tutorial, unattended: start the stack, stream the readings, kill the
# Worker part-way through, start it again, verify ClickHouse against the
# independently computed expectation, look at the recovered state, and tear
# everything down. About three minutes.
#
#   ./run.sh              # leaves nothing running
#   KEEP_UP=1 ./run.sh    # leave the stack up afterwards to look around
#
# This is also what CI runs (.github/workflows/ci.yml), so the commands below
# are the tutorial's own commands rather than a separate representation of
# them. Every wait is bounded and waits for a condition, never a duration.
set -euo pipefail
cd "$(dirname "$0")"

COORD="http://localhost:${CLINK_HTTP_PORT:-8081}"
CH="http://localhost:${CLICKHOUSE_HTTP_PORT:-8123}"
# Kill the Worker once this many (sensor, window) rows have reached
# ClickHouse: four complete window sets, so the pipeline is demonstrably in
# the middle of its work, with open windows in state and more input to come.
KILL_AFTER_WINDOWS="${KILL_AFTER_WINDOWS:-32}"

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
windows_in_clickhouse() { ch "SELECT uniqExact((sensor_id, window_start)) FROM sensor_window_stats" 2>/dev/null || echo 0; }
job_running() { curl -sf "$COORD/api/v1/jobs" | grep -q '"status":"RUNNING"'; }
worker_lost() { curl -sf "$COORD/api/v1/cluster" | grep -q '"lost":true'; }
enough_windows() { [ "$(windows_in_clickhouse)" -ge "$KILL_AFTER_WINDOWS" ]; }

diagnostics() {
    echo
    echo "run.sh: FAILED - diagnostics follow" >&2
    docker compose ps -a || true
    for s in submit coordinator worker kafka-init; do
        echo "----- docker compose logs $s (tail)"; docker compose logs --no-color --tail 80 "$s" 2>&1 | cut -c1-220 || true
    done
    echo "----- coordinator jobs"; curl -s "$COORD/api/v1/jobs" || true; echo
    # Per-operator record counts localise a stall in one line: zero at the
    # source means nothing is being read from Kafka; non-zero everywhere but
    # the sink means ClickHouse is refusing the inserts.
    echo "----- per-operator records_out"
    curl -s "$COORD/api/v1/jobs/1/operators" 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    for o in d.get('operators', []):
        print('  %-28s out=%s' % (o['op_type'], o['records_out']))
except Exception as e:
    print('  (no operator stats: %s)' % e)
" || true
    echo "----- ClickHouse"; ch "SELECT count() AS rows, uniqExact((sensor_id, window_start)) AS windows FROM sensor_window_stats FORMAT JSONEachRow" || true
    echo "----- state directory"; docker compose exec -T coordinator sh -c 'ls /state/checkpoints/_jobs/*/ 2>/dev/null | tail -5; ls /state/checkpoints/v1/*/ 2>/dev/null' || true
}

PRODUCER=
cleanup() {
    local rc=$?
    [ -n "$PRODUCER" ] && kill "$PRODUCER" 2>/dev/null || true
    if [ "$rc" -ne 0 ]; then diagnostics; fi
    if [ "${KEEP_UP:-0}" = "1" ]; then
        echo; echo "run.sh: KEEP_UP=1, leaving the stack running (docker compose down -v to remove it)"
    else
        say "removing the stack"
        docker compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    exit "$rc"
}
trap cleanup EXIT

say "starting from a clean slate"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true

say "starting Kafka, ClickHouse and clink; submitting pipeline.sql"
# Plain `up -d`: it already waits for every depends_on condition before
# starting the dependants. `--wait` would fail the moment the one-shot
# `submit` container exits, even though exiting 0 is exactly its job.
docker compose up -d
poll 60 "the job to be RUNNING" job_running
curl -s "$COORD/api/v1/jobs"; echo

say "streaming the readings (in the background)"
./scripts/produce_events.py &
PRODUCER=$!

say "waiting for the first $KILL_AFTER_WINDOWS windows to land in ClickHouse"
poll 120 "$KILL_AFTER_WINDOWS windows in ClickHouse" enough_windows
echo "windows in ClickHouse: $(windows_in_clickhouse)"

say "killing the Worker (SIGKILL) while the stream is still arriving"
docker compose kill worker
poll 60 "the Coordinator to declare the Worker lost" worker_lost
echo "the Coordinator has declared the Worker lost:"
curl -s "$COORD/api/v1/cluster"; echo

say "starting the Worker again"
docker compose start worker
poll 60 "the job to be RUNNING again" job_running

say "waiting for the producer to finish"
wait "$PRODUCER"
PRODUCER=

say "verifying ClickHouse against the recomputed expectation"
./scripts/verify.py

say "what the Coordinator logged about the recovery"
docker compose logs --no-color coordinator 2>&1 | grep -iE 'lost|restart|restor|redeploy|recover' | cut -c1-200 | tail -12 || true

say "the job's live keyed state, queried through the Coordinator"
docker compose exec -T -e CLINK_LOG_LEVEL=off coordinator \
    clink state-query --job=1 --coordinator=coordinator:8081 \
    --sql="SELECT slot, COUNT(*) AS entries FROM state GROUP BY slot"

say "PASS"
