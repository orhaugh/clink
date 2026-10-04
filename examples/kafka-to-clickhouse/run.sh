#!/usr/bin/env bash
# The tutorial, unattended: start the stack, stream the readings, kill the
# Worker part-way through, start it again, verify ClickHouse against the
# independently computed expectation, look at the recovered state, and tear
# everything down. About three minutes.
#
#   ./run.sh              # leaves nothing running
#   KEEP_UP=1 ./run.sh    # leave the stack up afterwards to look around
#   ./run.sh --self-test  # check the sink check itself, without Docker
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

# Which sink ran. pipeline.sql asks for the native sink when its ClickHouse
# table sets insert_format = 'native', and for the JSONEachRow sink otherwise;
# an image without the native sink would quietly run the other one. The
# Coordinator names the sink that limits the job's guarantee, and the native
# sink reports itself on every open, with the server line it was tested
# against and the table's effective async_insert.
expected_sink_op() {  # <pipeline.sql text>
    if printf '%s\n' "$1" | grep -qiE "insert_format[[:space:]]*=[[:space:]]*'native'"; then
        echo clickhouse_native_sink
    else
        echo clickhouse_sink
    fi
}
# check_sink <pipeline.sql text> <coordinator log> <worker log> <native opens needed>
check_sink() {
    local want opens n
    want="$(expected_sink_op "$1")"
    if ! printf '%s\n' "$2" | grep -q "limited by sink '${want}'"; then
        echo "run.sh: the Coordinator does not name sink '${want}', which pipeline.sql asks for" >&2
        return 1
    fi
    [ "${want}" = clickhouse_native_sink ] || return 0
    opens="$(printf '%s\n' "$3" | grep -E 'clickhouse native sink open: .*factory=clickhouse_native_sink' || true)"
    n="$(printf '%s\n' "${opens}" | grep -c 'factory=' || true)"
    if [ "${n}" -lt "$4" ]; then
        echo "run.sh: ${n} native sink open report(s) in the Worker log, expected at least $4" >&2
        return 1
    fi
    if printf '%s\n' "${opens}" | grep -v ' (tested)' | grep -q 'factory='; then
        echo "run.sh: the native sink opened against a server line it was not tested on" >&2
        return 1
    fi
    if printf '%s\n' "${opens}" | grep -v 'async_insert=0 (' | grep -q 'factory='; then
        echo "run.sh: the native sink opened against a table whose async_insert is not 0" >&2
        return 1
    fi
}
sink_ran() {  # <native opens needed>
    check_sink "$(cat pipeline.sql)" "$(docker compose logs --no-color coordinator 2>&1)" \
        "$(docker compose logs --no-color worker 2>&1)" "$1"
}

self_test() {
    local native legacy coord_native coord_legacy open_ok fails=0
    native="CREATE TABLE out (a BIGINT) WITH (connector = 'clickhouse', insert_format = 'native');"
    legacy="CREATE TABLE out (a BIGINT) WITH (connector = 'clickhouse', format = 'json');"
    coord_native="[coordinator.guarantee] [info] job delivery guarantee: AT_LEAST_ONCE (limited by sink 'clickhouse_native_sink')"
    coord_legacy="[coordinator.guarantee] [info] job delivery guarantee: AT_LEAST_ONCE (limited by sink 'clickhouse_sink')"
    open_ok="[sink.clickhouse] [info] clickhouse native sink open: subtask=0/1 factory=clickhouse_native_sink mode=append delivery=at_least_once table=\`default\`.\`t\` engine=MergeTree server=26.8.15 (tested) endpoint=clickhouse:9000 tls=off async_insert=0 (table setting) columns=7"
    expect() {  # <pass|fail> <name> <check_sink args...>
        local want="$1" name="$2" got
        shift 2
        if check_sink "$@" 2>/dev/null; then got=pass; else got=fail; fi
        if [ "${got}" = "${want}" ]; then
            echo "self-test: ${name}: ${got}, as it should"
        else
            echo "self-test: ${name}: ${got}, expected ${want}" >&2
            fails=$((fails + 1))
        fi
    }
    expect fail "native pipeline, Coordinator names the JSONEachRow sink" "${native}" "${coord_legacy}" "" 1
    expect pass "native pipeline, native sink opened" "${native}" "${coord_native}" "${open_ok}" 1
    expect fail "native pipeline, no open report" "${native}" "${coord_native}" "" 1
    expect fail "native pipeline, one open where two are needed" "${native}" "${coord_native}" "${open_ok}" 2
    expect fail "native pipeline, untested server line" "${native}" "${coord_native}" \
        "${open_ok/ (tested)/ (accepted, untested)}" 1
    expect fail "native pipeline, async_insert on" "${native}" "${coord_native}" \
        "${open_ok/async_insert=0 (/async_insert=1 (}" 1
    expect pass "JSONEachRow pipeline, JSONEachRow sink" "${legacy}" "${coord_legacy}" "" 1
    expect fail "JSONEachRow pipeline, Coordinator names the native sink" "${legacy}" "${coord_native}" "" 1
    # The tutorial's own pipeline.sql, whichever sink it asks for, against
    # logs that name that sink: the check the end-to-end run makes.
    expect pass "this pipeline.sql against its own sink" "$(cat pipeline.sql)" \
        "${coord_legacy/clickhouse_sink/$(expected_sink_op "$(cat pipeline.sql)")}" "${open_ok}" 1
    [ "${fails}" -eq 0 ]
}
if [ "${1:-}" = "--self-test" ]; then
    self_test
    exit $?
fi

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
    # A native sink that refused to open names the reason as a code
    # (clickhouse.target_async_insert, clickhouse.column_plan, ...).
    echo "----- ClickHouse sink reports and refusals"
    docker compose logs --no-color worker 2>&1 | grep -E 'clickhouse native sink|clickhouse\.[a-z_]+' | cut -c1-300 | tail -12 || true
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

say "checking the sink that runs is the one pipeline.sql asks for: $(expected_sink_op "$(cat pipeline.sql)")"
poll 60 "the $(expected_sink_op "$(cat pipeline.sql)") to be reported" sink_ran 1
docker compose logs --no-color coordinator 2>&1 | grep -o "job delivery guarantee: .*" | tail -1
docker compose logs --no-color worker 2>&1 | grep -o "clickhouse native sink open: .*" | cut -c1-240 | tail -1 || true

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
poll 60 "the restarted $(expected_sink_op "$(cat pipeline.sql)") to be reported" sink_ran 2

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
