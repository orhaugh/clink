#!/usr/bin/env bash
# The server's own insert ceiling, measured before any sink target is held to it
# (docs/clickhouse-sink-plan.md 3.14): clickhouse-client sending Native,
# LZ4-compressed, from PARALLELISM clients at once, each as a series of INSERTs
# of INSERT_ROWS rows, which is the shape the sink sends: one INSERT per subtask
# per batch interval. One large INSERT instead would let the server form parts
# of a million rows and overstate the ceiling a sink can approach. A target
# above 70% of this ceiling is restated before clink is measured, never after.
#
#   ./calibrate.sh                                   # local smoke run
#   CLICKHOUSE_DOCKER_HOST=ssh://root@<server> CLICKHOUSE_HOST=<server ip> \
#     ROWS=10000000 ./calibrate.sh                   # a rig, from the clink host
#
# INSERT_ROWS defaults to 62,500, the B1 rate target's shape: 500,000 rows/s from
# 8 subtasks each closing an INSERT a second. Set it to a campaign's measured
# native mean rows per INSERT to calibrate at what the sink actually sent. On a
# rig the server runs on the server host's Docker (CLICKHOUSE_DOCKER_HOST, the
# repository checked out at the same path there, under run.sh's project name),
# and the clients run here, so the figure includes the network between them.
# The input files are generated with clickhouse-local before the timed window.
# The result, with its premise, goes to results/calibration-<utc>.json, which
# run.sh takes as CALIBRATION.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
ROWS="${ROWS:-1000000}"                 # per client
PARALLELISM="${PARALLELISM:-8}"
INSERT_ROWS="${INSERT_ROWS:-62500}"
KEEP_UP="${KEEP_UP:-0}"
CLICKHOUSE_DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST:-}"
DB="clink_bench"
TABLE="calibration"

compose() { docker compose -p chbench -f compose.yml "$@"; }
client() { compose exec -T client "$@"; }
if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
    : "${CLICKHOUSE_HOST:?set CLICKHOUSE_HOST to the server host the clients connect to}"
    HOST="${CLICKHOUSE_HOST}"
    PORT="${CLICKHOUSE_PORT:-19208}"     # what compose.yml publishes
    server_compose() { DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker compose -p chbench-server -f compose.yml "$@"; }
else
    HOST="clickhouse"
    PORT="9000"
    server_compose() { compose "$@"; }
fi
inserts=$(( (ROWS + INSERT_ROWS - 1) / INSERT_ROWS ))

mkdir -p data results
server_compose up -d --wait clickhouse >/dev/null
compose up -d client >/dev/null
cleanup() {
    rm -rf data/calibration
    if [ "${KEEP_UP}" != "1" ]; then
        compose down -v >/dev/null 2>&1 || true
        if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
            server_compose down -v >/dev/null 2>&1 || true
        fi
    fi
}
# shellcheck disable=SC2329 # invoked by the trap
trap cleanup EXIT

query() { client clickhouse-client --host "${HOST}" --port "${PORT}" --query "$1"; }

query "DROP DATABASE IF EXISTS ${DB} SYNC"
query "CREATE DATABASE ${DB}"
query "$(sed -e "s/{db}/${DB}/g" -e "s/{table}/${TABLE}/g" table.sql)"

# One clickhouse-local per client writes that client's INSERTs as numbered
# files, INSERT_ROWS rows each, through a partitioned file() write.
echo "calibrate: generating ${PARALLELISM} x ${ROWS} rows as INSERTs of ${INSERT_ROWS}"
rm -rf data/calibration
mkdir -p data/calibration
gen_pids=()
for c in $(seq 0 $((PARALLELISM - 1))); do
    offset=$((c * ROWS))
    sql="INSERT INTO FUNCTION file('/data/calibration/c${c}-{_partition_id}.native', 'Native') \
PARTITION BY intDiv(k - ${offset}, ${INSERT_ROWS}) SELECT * FROM ($(sed -e "s/{offset}/${offset}/" -e "s/{rows}/${ROWS}/" rows.sql)
)"
    client clickhouse-local --max_partitions_per_insert_block 0 --query "${sql}" &
    gen_pids+=($!)
done
for p in "${gen_pids[@]}"; do
    wait "${p}"
done

settings="--compression 1 --async_insert 0 --input_format_native_allow_types_conversion 0"
echo "calibrate: inserting"
start_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"
pids=()
for c in $(seq 0 $((PARALLELISM - 1))); do
    # One session per client, its INSERTs in order: separate queries, so each
    # forms its own part, as each of the sink's INSERTs does.
    client sh -c "ls /data/calibration/c${c}-*.native | sort -t- -k2 -n | \
while read -r f; do echo \"INSERT INTO ${DB}.${TABLE} FROM INFILE '\$f' FORMAT Native;\"; done | \
clickhouse-client --host ${HOST} --port ${PORT} ${settings} --multiquery" &
    pids+=($!)
done
for p in "${pids[@]}"; do
    wait "${p}"
done
end_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"

landed="$(query "SELECT count() FROM ${DB}.${TABLE}")"
expected=$((PARALLELISM * ROWS))
if [ "${landed}" != "${expected}" ]; then
    echo "calibrate: ${landed} rows landed, expected ${expected}" >&2
    exit 1
fi
query "SYSTEM FLUSH LOGS"
sent="$(query "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, '${DB}.${TABLE}')")"
row_bytes="$(query "SELECT round(sum(data_uncompressed_bytes) / sum(rows), 1) FROM system.parts WHERE database = '${DB}' AND table = '${TABLE}' AND active")"
server_version="$(query "SELECT version()")"
image="$(sed -n 's/^ *image: \(clickhouse.*\)$/\1/p' compose.yml | head -1)"
client_id="$(docker info --format '{{.ID}}')"
server_id="$(if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker info --format '{{.ID}}'; else docker info --format '{{.ID}}'; fi)"
python3 - "$start_ns" "$end_ns" "$expected" "$row_bytes" "$server_version" "$image" \
    "$PARALLELISM" "$ROWS" "$INSERT_ROWS" "$inserts" "$sent" "${CLICKHOUSE_HOST:-local}" \
    "$client_id" "$server_id" <<'PY'
import json, os, platform, sys, time
(start, end, rows, row_bytes, version, image, par, per, insert_rows, inserts, sent, host,
 client_id, server_id) = sys.argv[1:]
secs = (int(end) - int(start)) / 1e9
rate = int(rows) / secs
result = {
    "kind": "clickhouse_server_ceiling",
    "rows_per_second": round(rate),
    "seconds": round(secs, 3),
    "rows": int(rows),
    "premise": {
        "client": "clickhouse-client, Native, LZ4, INSERT ... FROM INFILE",
        "parallelism": int(par),
        "rows_per_client": int(per),
        "insert_rows": int(insert_rows),
        "inserts_per_client": int(inserts),
        "inserts_acknowledged": int(sent),
        "mean_row_bytes_uncompressed": float(row_bytes),
        "server_version": version,
        "server_image": image,
        "server_host": host,
        "client_machine": platform.platform(),
        "client_cpus": os.cpu_count(),
        # The same Docker daemon for clients and server is one machine.
        "local_smoke_run": client_id == server_id,
    },
}
name = time.strftime("results/calibration-%Y%m%dT%H%M%SZ.json", time.gmtime())
with open(name, "w") as f:
    json.dump(result, f, indent=2)
print(f"calibrate: {rate:,.0f} rows/s ({rows} rows in {secs:.2f} s, {sent} INSERTs); premise in {name}")
PY
