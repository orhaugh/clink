#!/usr/bin/env bash
# B1 for the ClickHouse sinks (premise.md is the contract; read it first): the
# native sink, the same pipeline into a blackhole, and the legacy JSONEachRow
# sink, each on a freshly composed stack, TRIALS times, at PARALLELISM 8 with
# a 10 s checkpoint interval. Every sink run is gated on the landed table
# (verify.py) and its rate taken at the server; a run whose gate fails has no
# figure. The campaign's result, premise attached, goes to results/<campaign>/.
#
#   ./run.sh                                         # local smoke run
#   TRIALS=1 ROWS=8000000 CELLS="native" ./run.sh    # a quicker one
#   PREPARE_ONLY=1 ROWS=360000000 ./run.sh           # write the input, then stop
#   CLICKHOUSE_DOCKER_HOST=ssh://root@<server> CLICKHOUSE_HOST=<server ip> \
#     CLINK_IMAGE=ghcr.io/orhaugh/clink-runtime:sha-<12> ROWS=360000000 \
#     WARMUP_S=60 COOLDOWN_S=30 RIG="<machine types, network>" ./run.sh   # a rig
#
# Locally ClickHouse and clink share one machine, so the result is marked
# local_smoke_run and checks the harness, not the targets. On a rig this runs
# on the clink host, drives the server host's Docker over CLICKHOUSE_DOCKER_HOST
# (the repository checked out at the same path there), and the Worker connects
# to CLICKHOUSE_HOST:CLICKHOUSE_PORT.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

CELLS="${CELLS:-native blackhole legacy}"
TRIALS="${TRIALS:-3}"
PARALLELISM="${PARALLELISM:-8}"
ROWS="${ROWS:-16000000}"                      # total input rows; rounded down to whole files
FILES_PER_SUBTASK="${FILES_PER_SUBTASK:-2}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}"
BATCH_ROWS="${BATCH_ROWS:-1048449}"           # the native sink's defaults, stated
BATCH_BYTES="${BATCH_BYTES:-67108864}"
BATCH_INTERVAL_MS="${BATCH_INTERVAL_MS:-1000}"
LEGACY_BATCH_ROWS="${LEGACY_BATCH_ROWS:-}"    # default: this campaign's native mean rows per INSERT
WARMUP_S="${WARMUP_S:-5}"
COOLDOWN_S="${COOLDOWN_S:-2}"
MIN_WINDOW_S="${MIN_WINDOW_S:-600}"           # the plan's ten minutes of steady state
MAX_RUNTIME_S="${MAX_RUNTIME_S:-7200}"
CLINK_IMAGE="${CLINK_IMAGE:-clink-runtime:latest}"
CLICKHOUSE_DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST:-}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-19208}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-19281}"
RIG="${RIG:-}"
D2_ACCEPTED="${D2_ACCEPTED:-0}"
GEN_JOBS="${GEN_JOBS:-4}"
KEEP_UP="${KEEP_UP:-0}"
CAMPAIGN="${CAMPAIGN:-$(date -u +%Y%m%dT%H%M%SZ)}"

DB="clink_bench"
TABLE="events"
PROJECT="chbench"
export CLINK_IMAGE CLINK_HTTP_PORT

if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
    : "${CLICKHOUSE_HOST:?set CLICKHOUSE_HOST to the server host the Worker connects to}"
    ch_host="${CLICKHOUSE_HOST}"
    ch_port="${CLICKHOUSE_PORT}"
    local_smoke=false
else
    ch_host="clickhouse"
    ch_port="9000"
    local_smoke=true
fi

say() { echo "run: $*"; }
sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }

# Locally one project holds ClickHouse and clink, so the Worker reaches the
# server by service name. On a rig the server's compose runs on the server
# host's Docker.
clink_compose() {
    if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
        docker compose -p "${PROJECT}" -f clink.yml "$@"
    else
        docker compose -p "${PROJECT}" -f compose.yml -f clink.yml "$@"
    fi
}
ch_compose() {
    if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
        # Its own project name, so the two halves never see each other's
        # containers as orphans, even when both daemons are one machine's.
        DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker compose -p "${PROJECT}-server" -f compose.yml "$@"
    else
        docker compose -p "${PROJECT}" -f compose.yml -f clink.yml "$@"
    fi
}
ch_query() { ch_compose exec -T clickhouse clickhouse-client --query "$1"; }
usage_usec() { awk '$1 == "usage_usec" { print $2 }'; }
clink_cpu_usec() {
    local c w
    c="$(clink_compose exec -T clink-coordinator cat /sys/fs/cgroup/cpu.stat | usage_usec)"
    w="$(clink_compose exec -T clink-worker cat /sys/fs/cgroup/cpu.stat | usage_usec)"
    echo $((c + w))
}
server_cpu_usec() { ch_compose exec -T clickhouse cat /sys/fs/cgroup/cpu.stat | usage_usec; }

teardown() {
    clink_compose --profile submit down -v --remove-orphans >/dev/null 2>&1 || true
    if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
        ch_compose down -v >/dev/null 2>&1 || true
    fi
}
cleanup() {
    if [ "${KEEP_UP}" != "1" ]; then
        teardown
    fi
}
# shellcheck disable=SC2329 # invoked by the trap
trap cleanup EXIT

# --- preflight ---------------------------------------------------------------

for v in ${CELLS}; do
    case "${v}" in
        native|blackhole|legacy) ;;
        *) echo "run: unknown cell '${v}' (native, blackhole, legacy)" >&2; exit 2 ;;
    esac
done
ch_image="$(sed -n 's/^ *image: \(clickhouse.*\)$/\1/p' compose.yml | head -1)"

files=$((PARALLELISM * FILES_PER_SUBTASK))
per_file=$((ROWS / files))
rows=$((per_file * files))
[ "${per_file}" -gt 0 ] || { echo "run: ROWS=${ROWS} is fewer than one row per file (${files} files)" >&2; exit 2; }

# --- the input -----------------------------------------------------------------

# Written once per size with clickhouse-local from rows.sql, so the input and
# the gate's expected side come from the same definition. ts goes out as
# epoch-millisecond text and lc as a plain String (source.sql says why). The
# leading null event_time is the column clink's Parquet row source reads every
# file's event time from; it refuses a file without one.
gen_select="SELECT CAST(NULL, 'Nullable(Int64)') AS event_time, k, a, b, i, f1, f2, toString(toUnixTimestamp64Milli(ts)) AS ts, d, s_low, s_mid, s_high, lc"
dataset="data/rows-${rows}x${files}"
stamp="$( { cat rows.sql; echo "${gen_select} ${rows} ${files} ${ch_image}"; } | sha256 | cut -c1-16)"
if [ "$(cat "${dataset}/.complete" 2>/dev/null || true)" != "${stamp}" ]; then
    say "writing ${files} Parquet files of ${per_file} rows to ${dataset}"
    rm -rf "${dataset}"
    mkdir -p "${dataset}"
    gen_one() {
        local f="$1" sql
        sql="${gen_select} FROM ($(sed -e "s/{offset}/$((f * per_file))/" -e "s/{rows}/${per_file}/" rows.sql)
) INTO OUTFILE '/data/$(basename "${dataset}")/part-$(printf '%05d' "${f}").parquet' FORMAT Parquet"
        docker run --rm -v "${PWD}/data:/data" --entrypoint clickhouse-local "${ch_image}" \
            --output_format_parquet_string_as_string 1 --output_format_parquet_compression_method lz4 \
            --query "${sql}"
    }
    pids=()
    for f in $(seq 0 $((files - 1))); do
        gen_one "${f}" &
        pids+=($!)
        if [ "${#pids[@]}" -ge "${GEN_JOBS}" ]; then
            wait "${pids[0]}"
            pids=("${pids[@]:1}")
        fi
    done
    for p in ${pids[@]+"${pids[@]}"}; do
        wait "${p}"
    done
    say "computing the expected checksums from rows.sql"
    docker run --rm --entrypoint clickhouse-local "${ch_image}" \
        --query "$(python3 verify.py checksum-sql --rows "${rows}")" > "${dataset}/oracle.json"
    echo "${stamp}" > "${dataset}/.complete"
fi
if [ "${PREPARE_ONLY:-0}" = "1" ]; then
    say "input ready in ${dataset} (PREPARE_ONLY)"
    exit 0
fi

# --- the image and the premise -------------------------------------------------------

results="results/${CAMPAIGN}"
mkdir -p "${results}"
premise="${results}/premise.json"
say "campaign ${CAMPAIGN}: cells '${CELLS}', ${TRIALS} trial(s), ${rows} rows, parallelism ${PARALLELISM}"

docker image inspect "${CLINK_IMAGE}" >/dev/null 2>&1 || {
    echo "run: no image ${CLINK_IMAGE}; build it (docker/Dockerfile.runtime) or pull a :sha- tag" >&2
    exit 2
}
# The image under test: its commit, that it carries the native sink, and the
# clickhouse-cpp it was built against (the build script builds Release, stamped).
docker run --rm "${CLINK_IMAGE}" clink --capabilities-json > "${results}/capabilities.json"
python3 - "${results}/capabilities.json" <<'PY'
import json, sys
caps = json.load(open(sys.argv[1]))
text = json.dumps(caps)
if "clickhouse_native" not in text:
    sys.exit("run: the image's capabilities do not list clickhouse_native")
PY
cpp_stamp="$(docker run --rm --entrypoint sh "${CLINK_IMAGE}" -c 'ls /usr/local/.clink-clickhouse-cpp-* 2>/dev/null || true')"
cpp_version="${cpp_stamp##*.clink-clickhouse-cpp-}"
cpp_build_type="unknown"
if [ -n "${cpp_stamp}" ]; then
    cpp_build_type="Release"   # scripts/build-clickhouse-cpp.sh builds Release whatever the toolchain's type
fi
# One machine is a smoke run whatever the endpoints say: two daemons with one
# ID are one host (a rig pointed at the wrong server host, or a simulation).
if [ -n "${CLICKHOUSE_DOCKER_HOST}" ] &&
    [ "$(docker info --format '{{.ID}}')" = "$(DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker info --format '{{.ID}}')" ]; then
    say "the server's Docker is this machine's: marking the campaign a local smoke run"
    local_smoke=true
fi
docker image inspect -f '{{.Id}} {{.Architecture}}' "${CLINK_IMAGE}" > "${results}/image.txt"
docker info --format '{{json .}}' > "${results}/clink-host-docker.json"

python3 measure.py premise --out "${premise}" \
    --set campaign="${CAMPAIGN}" \
    --set local_smoke_run="${local_smoke}" \
    --set d2_targets_accepted="$([ "${D2_ACCEPTED}" = "1" ] && echo true || echo false)" \
    --set rig="${RIG:-local machine}" \
    --set cells="${CELLS}" --set trials="${TRIALS}" --set parallelism="${PARALLELISM}" \
    --set rows="${rows}" --set files="${files}" --set rows_per_file="${per_file}" \
    --set checkpoint_interval_ms="${CHECKPOINT_INTERVAL_MS}" \
    --set native.batch_rows="${BATCH_ROWS}" --set native.batch_bytes="${BATCH_BYTES}" \
    --set native.batch_interval_ms="${BATCH_INTERVAL_MS}" --set native.compression=lz4 \
    --set warmup_s="${WARMUP_S}" --set cooldown_s="${COOLDOWN_S}" --set min_window_s="${MIN_WINDOW_S}" \
    --set source="parquet directory, ${files} files of ${per_file} rows, file i read by subtask i % ${PARALLELISM}" \
    --set clink_image="${CLINK_IMAGE}" --text clink_image_id="${results}/image.txt" \
    --json clink_capabilities="${results}/capabilities.json" \
    --set clickhouse_cpp.version="${cpp_version:-unknown}" --set clickhouse_cpp.build_type="${cpp_build_type}" \
    --set clickhouse_image="${ch_image}" \
    --set clickhouse_endpoint="${ch_host}:${ch_port}"
python3 - "${results}/clink-host-docker.json" "${premise}" <<'PY'
import json, sys
info, premise = json.load(open(sys.argv[1])), sys.argv[2]
p = json.load(open(premise))
p["clink_host"] = {k: info.get(k) for k in ("NCPU", "MemTotal", "Architecture", "OperatingSystem", "KernelVersion", "ServerVersion")}
json.dump(p, open(premise, "w"), indent=2)
PY

python3 measure.py premise --out "${premise}" \
    --set input_bytes="$(du -sk "${dataset}" | awk '{print $1 * 1024}')"

# --- one trial -------------------------------------------------------------------

fresh_stack() {
    teardown
    ch_compose up -d --wait clickhouse >/dev/null
    clink_compose up -d --wait clink-coordinator clink-worker >/dev/null
    ch_query "CREATE DATABASE ${DB}"
    ch_query "$(sed -e "s/{db}/${DB}/g" -e "s/{table}/${TABLE}/g" table.sql)"
}

record_server_premise() {
    # Once per campaign, from the first fresh server: what the plan asks the
    # premise to carry about the table and its settings.
    local d="${results}/server"
    mkdir -p "${d}"
    ch_query "SELECT version() AS version FORMAT JSONEachRow" > "${d}/version.json"
    ch_query "SHOW CREATE TABLE ${DB}.${TABLE} FORMAT TSVRaw" > "${d}/ddl.sql"
    ch_query "SELECT name, value, changed FROM system.merge_tree_settings WHERE name LIKE '%fsync%' OR name IN ('parts_to_delay_insert', 'parts_to_throw_insert', 'async_insert', 'min_rows_for_wide_part', 'min_bytes_for_wide_part') ORDER BY name FORMAT JSONEachRow" > "${d}/merge_tree_settings.json"
    ch_query "SELECT name, value, changed FROM system.settings WHERE name IN ('async_insert', 'wait_for_async_insert', 'max_insert_block_size', 'min_insert_block_size_rows', 'min_insert_block_size_bytes', 'max_insert_threads', 'insert_deduplicate', 'max_threads') ORDER BY name FORMAT JSONEachRow" > "${d}/settings.json"
    if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
        DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker info --format '{{json .}}' > "${d}/host-docker.json"
    else
        cp "${results}/clink-host-docker.json" "${d}/host-docker.json"
    fi
    python3 measure.py premise --out "${premise}" \
        --json server.version="${d}/version.json" --text server.table_ddl="${d}/ddl.sql" \
        --json server.merge_tree_settings="${d}/merge_tree_settings.json" \
        --json server.settings="${d}/settings.json"
    python3 - "${d}/host-docker.json" "${premise}" <<'PY'
import json, sys
info, premise = json.load(open(sys.argv[1])), sys.argv[2]
p = json.load(open(premise))
p["server"]["host"] = {k: info.get(k) for k in ("NCPU", "MemTotal", "Architecture", "OperatingSystem", "KernelVersion")}
json.dump(p, open(premise, "w"), indent=2)
PY
}

failures=0
run_trial() {
    local variant="$1" trial="$2" batch_rows="$3" dir sql c0 c1 s0 s1 gate_arg="" inserts_arg="" row_bytes=""
    dir="${results}/${variant}-t${trial}"
    mkdir -p "${dir}"
    sql="${dir}/pipeline.sql"
    cat sql/source.sql "sql/sink-${variant}.sql" sql/insert.sql | sed \
        -e "s|{data_dir}|/data/$(basename "${dataset}")|" \
        -e "s|{ch_host}|${ch_host}|" -e "s|{ch_port}|${ch_port}|" \
        -e "s|{ch_database}|${DB}|" -e "s|{ch_table}|${TABLE}|" \
        -e "s|{batch_rows}|${batch_rows}|" -e "s|{batch_bytes}|${BATCH_BYTES}|" \
        -e "s|{batch_interval_ms}|${BATCH_INTERVAL_MS}|" > "${sql}"

    say "${variant} t${trial}: fresh stack"
    fresh_stack
    if [ ! -f "${results}/server/version.json" ]; then
        record_server_premise
    fi

    c0="$(clink_cpu_usec)"
    s0="$(server_cpu_usec)"
    clink_compose --profile submit run --rm -T clink-submit run "/${sql}" \
        --coordinator-host=clink-coordinator --coordinator-port=8081 --name="${variant}-t${trial}" \
        --parallelism="${PARALLELISM}" --checkpoint-dir=/state/checkpoints \
        --checkpoint-interval-ms="${CHECKPOINT_INTERVAL_MS}" > "${dir}/submit.log" 2>&1 || {
        echo "run: submit failed:" >&2
        cat "${dir}/submit.log" >&2
        exit 1
    }
    python3 measure.py sample --base "http://127.0.0.1:${CLINK_HTTP_PORT}" --out "${dir}/samples.json" \
        --max-runtime "${MAX_RUNTIME_S}" || true
    c1="$(clink_cpu_usec)"
    s1="$(server_cpu_usec)"
    clink_compose logs --no-color clink-worker > "${dir}/worker.log" 2>&1 || true

    if [ "${variant}" != "blackhole" ]; then
        ch_query "SYSTEM FLUSH LOGS"
        ch_query "$(python3 measure.py inserts-sql --table "${DB}.${TABLE}")" > "${dir}/inserts.ndjson"
        ch_query "$(python3 verify.py checksum-sql --table "${DB}.${TABLE}")" > "${dir}/landed.json"
        python3 verify.py gate --expected "${dataset}/oracle.json" --actual "${dir}/landed.json" \
            --rows "${rows}" --out "${dir}/gate.json" || true
        row_bytes="$(ch_query "SELECT round(sum(data_uncompressed_bytes) / sum(rows), 1) FROM system.parts WHERE database = '${DB}' AND table = '${TABLE}' AND active")"
        gate_arg="--gate ${dir}/gate.json"
        inserts_arg="--inserts ${dir}/inserts.ndjson"
    fi
    # shellcheck disable=SC2086 # the optional arguments split on purpose
    python3 measure.py record --variant "${variant}" --trial "${trial}" --rows "${rows}" \
        --samples "${dir}/samples.json" ${gate_arg} ${inserts_arg} ${row_bytes:+--row-bytes "${row_bytes}"} \
        --clink-cpu-usec "${c0}" "${c1}" --server-cpu-usec "${s0}" "${s1}" \
        --batch-rows "${batch_rows}" --batch-bytes "${BATCH_BYTES}" --batch-interval-ms "${BATCH_INTERVAL_MS}" \
        --warmup "${WARMUP_S}" --cooldown "${COOLDOWN_S}" --out "${dir}/trial.json"
    if ! python3 -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1]))["gate"]["passed"] else 1)' "${dir}/trial.json"; then
        failures=$((failures + 1))
    fi
}

# --- the cells ---------------------------------------------------------------------

# Native first: the legacy cell's batch_rows is the native cell's measured mean
# rows per INSERT (premise.md), unless LEGACY_BATCH_ROWS fixes it.
for variant in native blackhole legacy; do
    case " ${CELLS} " in *" ${variant} "*) ;; *) continue ;; esac
    batch_rows="${BATCH_ROWS}"
    if [ "${variant}" = "legacy" ]; then
        batch_rows="${LEGACY_BATCH_ROWS:-$(python3 measure.py legacy-batch-rows --campaign "${results}")}"
        python3 measure.py premise --out "${premise}" \
            --set legacy.batch_rows="${batch_rows}" --set legacy.batch_interval_ms="${BATCH_INTERVAL_MS}" \
            --set legacy.batch_rows_from="$([ -n "${LEGACY_BATCH_ROWS}" ] && echo LEGACY_BATCH_ROWS || echo "native cell mean rows per INSERT")"
    fi
    for trial in $(seq 1 "${TRIALS}"); do
        run_trial "${variant}" "${trial}" "${batch_rows}"
    done
done

python3 measure.py summarise --campaign "${results}"
if [ "${failures}" -ne 0 ]; then
    echo "run: ${failures} trial(s) failed the gate; their figures are excluded" >&2
    exit 1
fi
