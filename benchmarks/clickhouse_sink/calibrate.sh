#!/usr/bin/env bash
# The server's own insert ceiling, measured before any sink target is held to it
# (docs/clickhouse-sink-plan.md 3.14): clickhouse-client sending Native,
# LZ4-compressed, at the sink's batch size, from PARALLELISM clients at once,
# into the benchmark table. A target above 70% of this ceiling is restated
# before clink is measured, never after.
#
#   ./calibrate.sh                                   # local smoke run
#   CLICKHOUSE_HOST=10.0.0.2 ROWS=10000000 ./calibrate.sh   # a rig's server
#
# Locally the server and the client share one machine, so the figure checks the
# harness and is not a premise. On a rig the client service runs on the client
# host and CLICKHOUSE_HOST names the server host. The input files are generated
# with clickhouse-local before the timed window. The result, with its premise,
# goes to results/calibration-<utc>.json.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
ROWS="${ROWS:-1000000}"                 # per client
PARALLELISM="${PARALLELISM:-8}"
BATCH_ROWS="${BATCH_ROWS:-1048449}"     # the sink's default batch_rows
HOST="${CLICKHOUSE_HOST:-clickhouse}"
PORT="${CLICKHOUSE_PORT:-9000}"
KEEP_UP="${KEEP_UP:-0}"
DB="clink_bench"
TABLE="calibration"

compose() { docker compose -f compose.yml "$@"; }
client() { compose exec -T client "$@"; }

mkdir -p data results
if [ -z "${CLICKHOUSE_HOST:-}" ]; then
    compose up -d --wait clickhouse client
else
    compose up -d client
fi
cleanup() {
    rm -f data/part-*.native
    if [ "${KEEP_UP}" != "1" ]; then
        compose down -v >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

query() { client clickhouse-client --host "${HOST}" --port "${PORT}" --query "$1"; }

query "DROP DATABASE IF EXISTS ${DB} SYNC"
query "CREATE DATABASE ${DB}"
query "$(sed -e "s/{db}/${DB}/g" -e "s/{table}/${TABLE}/g" table.sql)"

echo "calibrate: generating ${PARALLELISM} x ${ROWS} rows"
for c in $(seq 0 $((PARALLELISM - 1))); do
    sql="$(sed -e "s/{offset}/$((c * ROWS))/" -e "s/{rows}/${ROWS}/" rows.sql)"
    client sh -c "clickhouse-local --query \"${sql//\"/\\\"} FORMAT Native\" > /data/part-${c}.native"
done

settings="--compression 1 --max_insert_block_size ${BATCH_ROWS} --async_insert 0 \
--input_format_native_allow_types_conversion 0"
echo "calibrate: inserting"
start_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"
pids=()
for c in $(seq 0 $((PARALLELISM - 1))); do
    client sh -c "clickhouse-client --host ${HOST} --port ${PORT} ${settings} \
--query 'INSERT INTO ${DB}.${TABLE} FORMAT Native' < /data/part-${c}.native" &
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
row_bytes="$(query "SELECT round(sum(data_uncompressed_bytes) / sum(rows), 1) FROM system.parts WHERE database = '${DB}' AND table = '${TABLE}' AND active")"
server_version="$(query "SELECT version()")"
image="$(sed -n 's/^ *image: \(clickhouse.*\)$/\1/p' compose.yml | head -1)"
python3 - "$start_ns" "$end_ns" "$expected" "$row_bytes" "$server_version" "$image" \
    "$PARALLELISM" "$ROWS" "$BATCH_ROWS" "${CLICKHOUSE_HOST:-local}" <<'PY'
import json, os, platform, sys, time
start, end, rows, row_bytes, version, image, par, per, batch, host = sys.argv[1:]
secs = (int(end) - int(start)) / 1e9
rate = int(rows) / secs
result = {
    "kind": "clickhouse_server_ceiling",
    "rows_per_second": round(rate),
    "seconds": round(secs, 3),
    "rows": int(rows),
    "premise": {
        "client": "clickhouse-client, Native, LZ4",
        "parallelism": int(par),
        "rows_per_client": int(per),
        "batch_rows": int(batch),
        "mean_row_bytes_uncompressed": float(row_bytes),
        "server_version": version,
        "server_image": image,
        "server_host": host,
        "client_machine": platform.platform(),
        "client_cpus": os.cpu_count(),
        "local_smoke_run": host == "local",
    },
}
name = time.strftime("results/calibration-%Y%m%dT%H%M%SZ.json", time.gmtime())
with open(name, "w") as f:
    json.dump(result, f, indent=2)
print(f"calibrate: {rate:,.0f} rows/s ({rows} rows in {secs:.2f} s); premise in {name}")
PY
