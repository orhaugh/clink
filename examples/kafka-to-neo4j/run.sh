#!/usr/bin/env bash
# Run the Kafka -> clink -> Neo4j example end to end, a Worker kill included,
# and check the graph against the workload:
#
#   ./run.sh                          # clink from CLINK_IMAGE, in Docker
#   CLINK_BIN_DIR=../../build ./run.sh   # clink from a local build, on this host
#   CLEANUP=1 ./run.sh                # and remove everything at the end
#
# Steps: start Kafka and Neo4j; apply schema.cypher; load the reference
# devices (scripts/seed_reference.py); start a Coordinator and a Worker; submit
# pipeline.sql (two jobs); send the first half of the transfers; once some are
# in the graph and a checkpoint has completed, SIGKILL the Worker and start it
# again; send the rest; then scripts/verify.py checks every account, transfer,
# relationship and signal, exactly once, each signal's device context, and
# that the reference data is unchanged.
#
# CLINK_IMAGE must carry the Neo4j sink and lookup (a runtime image built after
# they were added). CLINK_BIN_DIR needs clink_node and clink built there.
set -euo pipefail

cd "$(dirname "$0")"
EXAMPLE_DIR="$(pwd)"
COMPOSE=(docker compose --project-directory "${EXAMPLE_DIR}")
NEO4J_HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-29092}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-8081}"
export NEO4J_HTTP="http://localhost:${NEO4J_HTTP_PORT}"
export NEO4J_PASSWORD=clink-example-pw
export NEO4J_HTTP_PORT KAFKA_HOST_PORT CLINK_HTTP_PORT

say() { printf '\n==> %s\n' "$*"; }

host_mode=0
if [ -n "${CLINK_BIN_DIR:-}" ]; then
    host_mode=1
    BIN="$(cd "${CLINK_BIN_DIR}" && pwd)"
    for b in clink_node clink; do
        [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
    done
    STATE="$(mktemp -d "${TMPDIR:-/tmp}/clink-neo4j-example.XXXXXX")"
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
        "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
        [ "${host_mode}" = 1 ] && rm -rf "${STATE}"
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

start_worker() {
    if [ "${host_mode}" = 1 ]; then
        "${BIN}/clink_node" --role=worker --id=worker-1 --coordinator-host=127.0.0.1 \
            --coordinator-port=16123 --port=16124 --data-host=127.0.0.1 --slots=16 \
            --http-port=18082 >>"${STATE}/worker.log" 2>&1 &
        worker_pid=$!
        await_http http://127.0.0.1:18082/api/v1/health "the Worker"
    else
        "${COMPOSE[@]}" up -d --wait worker
    fi
}

say "Starting Kafka and Neo4j"
"${COMPOSE[@]}" up -d --wait kafka neo4j
"${COMPOSE[@]}" up kafka-init >/dev/null

say "Applying schema.cypher (uniqueness constraints)"
for i in $(seq 1 60); do
    python3 scripts/cypher.py --file schema.cypher 2>/dev/null && break
    [ "${i}" = 60 ] && { echo "run.sh: Neo4j's Query API did not answer" >&2; exit 1; }
    sleep 2
done

say "Loading the reference data (synthetic devices each account has used)"
python3 scripts/seed_reference.py

say "Starting a Coordinator and a Worker, and submitting pipeline.sql"
if [ "${host_mode}" = 1 ]; then
    "${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port=16123 \
        --http-port="${CLINK_HTTP_PORT}" --ha-dir="${STATE}/ha" >"${STATE}/coordinator.log" 2>&1 &
    coordinator_pid=$!
    await_http "http://127.0.0.1:${CLINK_HTTP_PORT}/api/v1/health" "the Coordinator"
    start_worker
    # The same pipeline, reaching Kafka and Neo4j through their host ports.
    sed -e "s#kafka:9092#localhost:${KAFKA_HOST_PORT}#g" \
        -e "s#http://neo4j:7474#http://localhost:${NEO4J_HTTP_PORT}#g" pipeline.sql >"${STATE}/pipeline.sql"
    "${BIN}/clink" run "${STATE}/pipeline.sql" --coordinator-host=127.0.0.1 \
        --coordinator-port="${CLINK_HTTP_PORT}" --name=transfers-to-neo4j \
        --checkpoint-dir="${STATE}/checkpoints" --checkpoint-interval-ms=2000
else
    "${COMPOSE[@]}" up -d --wait coordinator worker
    "${COMPOSE[@]}" run --rm submit
fi

say "Sending the first half of the transfers"
python3 scripts/produce_events.py --part first

say "Waiting for transfers in the graph and a completed checkpoint"
for _ in $(seq 1 60); do
    n="$(python3 scripts/cypher.py 'MATCH (t:Transfer) RETURN count(t)' 2>/dev/null || echo 0)"
    [ "${n}" -gt 50 ] 2>/dev/null && break
    sleep 1
done
sleep 5
echo "transfers in the graph before the kill: ${n}"

say "Killing the Worker (SIGKILL), then starting it again"
if [ "${host_mode}" = 1 ]; then
    kill -9 "${worker_pid}"
    wait "${worker_pid}" 2>/dev/null || true
else
    "${COMPOSE[@]}" kill -s SIGKILL worker
fi
sleep 2
start_worker

say "Sending the rest of the transfers"
python3 scripts/produce_events.py --part second

say "Checking the graph against the workload"
python3 scripts/verify.py --wait 300

say "Signals, their device context, and the transfers that explain the first one"
python3 scripts/cypher.py "MATCH (s:Signal)-[:FLAGS]->(a:Account) RETURN s.id, s.distinct_senders, s.transfers ORDER BY s.id"
python3 scripts/cypher.py "MATCH (s:Signal) RETURN s.account, s.device_evidence_found, s.devices, s.shared_device_accounts, s.device_evidence, toString(s.context_observed_at) ORDER BY s.id"
python3 scripts/cypher.py "MATCH (s:Signal) WITH s ORDER BY s.id LIMIT 1 MATCH (f:Account)-[:SENT]->(t:Transfer) WHERE t.event_id IN s.event_ids RETURN t.event_id, f.id, t.amount ORDER BY t.event_id"
echo
echo "More queries in queries.cypher; Neo4j Browser at ${NEO4J_HTTP} (neo4j / ${NEO4J_PASSWORD})."
if [ "${CLEANUP:-0}" != 1 ]; then
    echo "Remove everything with: docker compose down -v"
fi
