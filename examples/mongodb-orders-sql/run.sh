#!/usr/bin/env bash
# Run the MongoDB orders example end to end and check its output:
#
#   ./run.sh                              # clink from CLINK_IMAGE, in Docker
#   CLINK_BIN_DIR=../../build ./run.sh    # clink from a local build, on this host
#   CLEANUP=1 ./run.sh                    # and remove everything at the end
#
# Steps: start a single-member MongoDB replica set; start a Coordinator and a
# Worker; submit pipeline.sql (two jobs, each reading shop.orders' change
# stream); once both streams are open, run scripts/load.js (300 orders placed,
# 30 updated, 12 deleted); then run scripts/verify.js until it passes or a
# minute goes by. It checks every customer total and every logged change
# against the workload's own record, reading MongoDB directly.
#
# CLINK_IMAGE must carry the MongoDB SQL connector (a runtime image built after
# it was added). CLINK_BIN_DIR needs clink_node and clink built there.
set -euo pipefail

cd "$(dirname "$0")"
EXAMPLE_DIR="$(pwd)"
COMPOSE=(docker compose --project-directory "${EXAMPLE_DIR}" -p clink-mongodb-orders)
MONGODB_HOST_PORT="${MONGODB_HOST_PORT:-27018}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-8081}"
export MONGODB_HOST_PORT CLINK_HTTP_PORT

say() { printf '\n==> %s\n' "$*"; }
mongosh() { "${COMPOSE[@]}" exec -T mongodb mongosh --quiet "$@"; }

host_mode=0
if [ -n "${CLINK_BIN_DIR:-}" ]; then
    host_mode=1
    BIN="$(cd "${CLINK_BIN_DIR}" && pwd)"
    for b in clink_node clink; do
        [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
    done
    STATE="$(mktemp -d "${TMPDIR:-/tmp}/clink-mongodb-orders.XXXXXX")"
fi

coordinator_pid=""
worker_pid=""
cleanup() {
    if [ "${host_mode}" = 1 ]; then
        for pid in ${worker_pid} ${coordinator_pid}; do
            kill "${pid}" 2>/dev/null || true
        done
        wait 2>/dev/null || true
    fi
    if [ "${CLEANUP:-0}" = 1 ]; then
        "${COMPOSE[@]}" --profile image down -v >/dev/null 2>&1 || true
        if [ "${host_mode}" = 1 ]; then
            rm -rf "${STATE}"
        fi
    fi
}
trap cleanup EXIT

await_http() {  # <url> <what>
    for _ in $(seq 1 60); do
        curl -fsS "$1" >/dev/null 2>&1 && return 0
        sleep 1
    done
    echo "run.sh: $2 did not come up" >&2
    exit 1
}

worker_log() {
    if [ "${host_mode}" = 1 ]; then
        cat "${STATE}/worker.log"
    else
        "${COMPOSE[@]}" logs --no-color worker
    fi
}

say "Starting a single-member MongoDB replica set"
"${COMPOSE[@]}" up -d --wait mongodb

say "Starting a Coordinator and a Worker, and submitting pipeline.sql"
if [ "${host_mode}" = 1 ]; then
    "${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port=16123 \
        --http-port="${CLINK_HTTP_PORT}" --ha-dir="${STATE}/ha" >"${STATE}/coordinator.log" 2>&1 &
    coordinator_pid=$!
    await_http "http://127.0.0.1:${CLINK_HTTP_PORT}/api/v1/health" "the Coordinator"
    MONGODB_URI="mongodb://127.0.0.1:${MONGODB_HOST_PORT}/?directConnection=true" \
        "${BIN}/clink_node" --role=worker --id=worker-1 --coordinator-host=127.0.0.1 \
        --coordinator-port=16123 --port=16124 --data-host=127.0.0.1 --slots=16 \
        --http-port=18082 >"${STATE}/worker.log" 2>&1 &
    worker_pid=$!
    await_http http://127.0.0.1:18082/api/v1/health "the Worker"
    "${BIN}/clink" run pipeline.sql --coordinator-host=127.0.0.1 \
        --coordinator-port="${CLINK_HTTP_PORT}" --name=mongodb-orders \
        --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000
else
    "${COMPOSE[@]}" --profile image up -d --wait coordinator worker
    "${COMPOSE[@]}" --profile image run --rm submit
fi

say "Waiting for both jobs to open their change streams"
# A change stream delivers what happens after it opens, so the workload waits.
for i in $(seq 1 90); do
    n="$(worker_log | grep -c 'change stream open on shop.orders' || true)"
    [ "${n}" -ge 2 ] && break
    [ "${i}" = 90 ] && { echo "run.sh: the change streams did not open" >&2; worker_log | tail -20 >&2; exit 1; }
    sleep 1
done

say "Placing, updating and deleting orders (scripts/load.js)"
mongosh /scripts/load.js

say "Checking clink's output against the workload (scripts/verify.js)"
for i in $(seq 1 30); do
    if mongosh /scripts/verify.js >"${TMPDIR:-/tmp}/clink-mongodb-orders-verify.txt" 2>&1; then
        cat "${TMPDIR:-/tmp}/clink-mongodb-orders-verify.txt"
        exit 0
    fi
    sleep 2
done
cat "${TMPDIR:-/tmp}/clink-mongodb-orders-verify.txt"
exit 1
