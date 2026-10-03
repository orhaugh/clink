#!/usr/bin/env bash
# B1 for the ClickHouse sinks (premise.md is the contract; read it first): the
# native sink, the same pipeline into a blackhole, and the legacy JSONEachRow
# sink, each on a freshly composed stack, TRIALS times, at PARALLELISM 8 with
# a 10 s checkpoint interval. Every sink run is gated on the landed table
# (verify.py) and its rate taken at the server; a run whose gate fails has no
# figure. The campaign's result, premise attached, goes to results/<campaign>/,
# and its summary says whether it stands as a B1 measurement and, if not, why.
#
#   ./run.sh                                         # local smoke run
#   TRIALS=1 ROWS=8000000 CELLS="native" ./run.sh    # a quicker one
#   PREPARE_ONLY=1 ROWS=1200000000 ./run.sh          # write the input, then stop
#   CLICKHOUSE_DOCKER_HOST=ssh://root@<server> CLICKHOUSE_HOST=<server ip> \
#     CALIBRATION=results/calibration-<utc>.json \
#     CLINK_IMAGE=ghcr.io/orhaugh/clink-runtime:sha-<12> ROWS=<sized from the ceiling> \
#     WARMUP_S=60 COOLDOWN_S=30 RIG="<machine types, network>" ./run.sh   # a rig
#
# Locally ClickHouse and clink share one machine, so the result is marked
# local_smoke_run and checks the harness, not the targets. On a rig this runs
# on the clink host, drives the server host's Docker over CLICKHOUSE_DOCKER_HOST
# (the repository checked out at the same path there), and the Worker connects
# to CLICKHOUSE_HOST:CLICKHOUSE_PORT. A rig campaign is refused unless ROWS
# gives every sink cell ten minutes of steady state at the calibrated ceiling.
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
CLINK_IMAGE="${CLINK_IMAGE:-clink-runtime:latest}"
CALIBRATION="${CALIBRATION:-}"
CLICKHOUSE_DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST:-}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-19208}"
CLINK_HTTP_PORT="${CLINK_HTTP_PORT:-19281}"
RIG="${RIG:-}"
D2_ACCEPTED="${D2_ACCEPTED:-0}"
GEN_JOBS="${GEN_JOBS:-4}"
KEEP_UP="${KEEP_UP:-0}"
RESUME="${RESUME:-0}"
CAMPAIGN="${CAMPAIGN:-$(date -u +%Y%m%dT%H%M%SZ)}"

MIN_WINDOW_S=600       # the plan's ten minutes of steady state (measure.py holds the same)
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
die() { echo "run: $*" >&2; exit 1; }
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

# A container's CPU so far, from inside it. Fails loudly rather than read as 0:
# the container must still be running, and be in its own cgroup v2 namespace,
# or /sys/fs/cgroup is not its cgroup and the figure would be someone else's.
read_usage() {
    local out first usec
    out="$("$1" exec -T "$2" sh -c 'cat /proc/self/cgroup; cat /sys/fs/cgroup/cpu.stat')" ||
        die "cannot read the cgroup of $2 (has it exited?)"
    first="$(printf '%s\n' "${out}" | head -1)"
    [ "${first}" = "0::/" ] ||
        die "$2 is not in its own cgroup v2 namespace (/proc/self/cgroup: ${first}); its CPU cannot be read from inside"
    usec="$(printf '%s\n' "${out}" | awk '$1 == "usage_usec" { print $2 }')"
    case "${usec}" in
        ''|*[!0-9]*) die "no usage_usec in the cgroup of $2" ;;
    esac
    echo "${usec}"
}
clink_cpu_usec() {
    local c w
    c="$(read_usage clink_compose clink-coordinator)" || exit 1
    w="$(read_usage clink_compose clink-worker)" || exit 1
    echo $((c + w))
}
server_cpu_usec() { read_usage ch_compose clickhouse; }

teardown() {
    clink_compose --profile submit down -v --remove-orphans >/dev/null 2>&1 || true
    if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
        ch_compose down -v >/dev/null 2>&1 || true
    fi
}
results=""
summarised=0
cleanup() {
    # A campaign that stops part-way still gets a summary of the trials it ran.
    if [ "${summarised}" = "0" ] && [ -n "${results}" ] && ls "${results}"/*/trial.json >/dev/null 2>&1; then
        python3 measure.py summarise --campaign "${results}" || true
    fi
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
        *) die "unknown cell '${v}' (native, blackhole, legacy)" ;;
    esac
done
ch_image="$(sed -n 's/^ *image: \(clickhouse.*\)$/\1/p' compose.yml | head -1)"

files=$((PARALLELISM * FILES_PER_SUBTASK))
per_file=$((ROWS / files))
rows=$((per_file * files))
[ "${per_file}" -gt 0 ] || die "ROWS=${ROWS} is fewer than one row per file (${files} files)"
# A hung job is cut off at a floor of 25,000 rows/s, slower than any cell runs.
MAX_RUNTIME_S="${MAX_RUNTIME_S:-$((rows / 25000 + 900))}"

# One machine is a smoke run whatever the endpoints say: two daemons with one
# ID are one host (a rig pointed at the wrong server host, or a simulation).
if [ -n "${CLICKHOUSE_DOCKER_HOST}" ] &&
    [ "$(docker info --format '{{.ID}}')" = "$(DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker info --format '{{.ID}}')" ]; then
    say "the server's Docker is this machine's: marking the campaign a local smoke run"
    local_smoke=true
fi

# A rig campaign must give every sink cell ten minutes of steady state, and no
# sink can land rows faster than the server's own ceiling, so size from it with
# room to spare; a smaller input would spend the rig on a window that cannot count.
if [ "${local_smoke}" = "false" ] && [ "${PREPARE_ONLY:-0}" != "1" ]; then
    [ -n "${CALIBRATION}" ] || die "a rig campaign needs CALIBRATION=<calibrate.sh result>, measured first"
    ceiling="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["rows_per_second"])' "${CALIBRATION}")"
    need=$(( (MIN_WINDOW_S + WARMUP_S + COOLDOWN_S) * ceiling * 3 / 2 ))
    [ "${rows}" -ge "${need}" ] ||
        die "ROWS=${rows} is short: ten minutes of steady state at the ceiling of ${ceiling} rows/s, with half again in hand, needs ROWS=${need}"
fi

# --- the input -----------------------------------------------------------------

# Written once per size with clickhouse-local from rows.sql, so the input and
# the gate's expected side come from the same definition. ts goes out as
# epoch-millisecond text and lc as a plain String (source.sql says why). The
# leading null event_time is the column clink's Parquet row source reads every
# file's event time from; it refuses a file without one.
gen_select="SELECT CAST(NULL, 'Nullable(Int64)') AS event_time, k, a, b, i, f1, f2, toString(toUnixTimestamp64Milli(ts)) AS ts, d, s_low, s_mid, s_high, lc"
gen_flags="--output_format_parquet_string_as_string 1 --output_format_parquet_compression_method lz4"
dataset="data/rows-${rows}x${files}"
# Everything the input and its expected side are made from: a change to any of
# them writes both again rather than gate against a stale oracle.
stamp="$( { cat rows.sql; echo "${gen_select} ${gen_flags} ${rows} ${files} ${ch_image}";
            python3 verify.py checksum-sql --rows "${rows}"; } | sha256 | cut -c1-16)"
if [ "$(cat "${dataset}/.complete" 2>/dev/null || true)" != "${stamp}" ]; then
    # About 85 bytes a row on disk at this dataset; refuse rather than fill the disk.
    mkdir -p data
    need_kb=$((rows * 110 / 1024))
    free_kb="$(df -Pk data | awk 'NR == 2 { print $4 }')"
    [ "${free_kb}" -ge "${need_kb}" ] ||
        die "the input needs about $((need_kb / 1048576)) GiB under data/, $((free_kb / 1048576)) GiB is free"
    say "writing ${files} Parquet files of ${per_file} rows to ${dataset}"
    rm -rf "${dataset}"
    mkdir -p "${dataset}"
    gen_one() {
        local f="$1" sql
        sql="${gen_select} FROM ($(sed -e "s/{offset}/$((f * per_file))/" -e "s/{rows}/${per_file}/" rows.sql)
) INTO OUTFILE '/data/$(basename "${dataset}")/part-$(printf '%05d' "${f}").parquet' FORMAT Parquet"
        # shellcheck disable=SC2086 # the flags split on purpose
        docker run --rm -v "${PWD}/data:/data" --entrypoint clickhouse-local "${ch_image}" ${gen_flags} \
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
if [ -e "${results}" ] && [ "${RESUME}" != "1" ]; then
    results=""
    die "results/${CAMPAIGN} exists; name a new CAMPAIGN, or RESUME=1 to add trials under the same premise"
fi
mkdir -p "${results}"
premise="${results}/premise.json"
say "campaign ${CAMPAIGN}: cells '${CELLS}', ${TRIALS} trial(s), ${rows} rows, parallelism ${PARALLELISM}"

docker image inspect "${CLINK_IMAGE}" >/dev/null 2>&1 ||
    die "no image ${CLINK_IMAGE}; build it (docker/Dockerfile.runtime) or pull a :sha- tag"
# The image under test: its commit, that it carries the native sink, and the
# clickhouse-cpp it was built against (the build script builds Release, stamped).
docker run --rm "${CLINK_IMAGE}" clink --capabilities-json > "${results}/capabilities.json"
python3 - "${results}/capabilities.json" <<'PY'
import json, sys
if "clickhouse_native" not in json.dumps(json.load(open(sys.argv[1]))):
    sys.exit("run: the image's capabilities do not list clickhouse_native")
PY
cpp_stamp="$(docker run --rm --entrypoint sh "${CLINK_IMAGE}" -c 'ls /usr/local/.clink-clickhouse-cpp-* 2>/dev/null || true')"
cpp_version="${cpp_stamp##*.clink-clickhouse-cpp-}"
cpp_build_type="unknown"
if [ -n "${cpp_stamp}" ]; then
    cpp_build_type="Release"   # scripts/build-clickhouse-cpp.sh builds Release whatever the toolchain's type
fi
image_id="$(docker image inspect -f '{{.Id}}' "${CLINK_IMAGE}")"
docker image inspect -f '{{.Id}} {{.Architecture}}' "${CLINK_IMAGE}" > "${results}/image.txt"
docker info --format '{{json .}}' > "${results}/clink-host-docker.json"
# The harness behind the figures, and the commit the image must be.
harness_head="$(git rev-parse HEAD)"
harness_dirty="$([ -n "$(git status --porcelain -- .)" ] && echo true || echo false)"
UNDER_TEST="${UNDER_TEST:-${harness_head}}"

python3 measure.py premise --out "${premise}" \
    --set campaign="${CAMPAIGN}" \
    --set local_smoke_run="${local_smoke}" \
    --set d2_targets_accepted="$([ "${D2_ACCEPTED}" = "1" ] && echo true || echo false)" \
    --set rig="${RIG}" \
    --set harness.head="${harness_head}" --set harness.dirty="${harness_dirty}" \
    --set under_test="${UNDER_TEST}" \
    --set cells="${CELLS}" --set trials="${TRIALS}" --set parallelism="${PARALLELISM}" \
    --set rows="${rows}" --set files="${files}" --set rows_per_file="${per_file}" \
    --set checkpoint_interval_ms="${CHECKPOINT_INTERVAL_MS}" \
    --set native.batch_rows="${BATCH_ROWS}" --set native.batch_bytes="${BATCH_BYTES}" \
    --set native.batch_interval_ms="${BATCH_INTERVAL_MS}" --set native.compression=lz4 \
    --set warmup_s="${WARMUP_S}" --set cooldown_s="${COOLDOWN_S}" --set max_runtime_s="${MAX_RUNTIME_S}" \
    --set source="parquet directory, ${files} files of ${per_file} rows, file i read by subtask i % ${PARALLELISM}" \
    --set clink_image="${CLINK_IMAGE}" --set clink_image_id="${image_id}" \
    --json clink_capabilities="${results}/capabilities.json" \
    --set clickhouse_cpp.version="${cpp_version:-unknown}" --set clickhouse_cpp.build_type="${cpp_build_type}" \
    --set clickhouse_image="${ch_image}" \
    --set clickhouse_endpoint="${ch_host}:${ch_port}" \
    --set input_bytes="$(du -sk "${dataset}" | awk '{print $1 * 1024}')"
if [ -n "${CALIBRATION}" ]; then
    python3 measure.py premise --out "${premise}" --json calibration="${CALIBRATION}"
fi
python3 - "${results}/clink-host-docker.json" "${premise}" <<'PY'
import json, sys
info, premise = json.load(open(sys.argv[1])), sys.argv[2]
p = json.load(open(premise))
p["clink_host"] = {k: info.get(k) for k in ("NCPU", "MemTotal", "Architecture", "OperatingSystem", "KernelVersion", "ServerVersion")}
json.dump(p, open(premise, "w"), indent=2)
PY

# --- one trial -------------------------------------------------------------------

fresh_stack() {
    teardown
    ch_compose up -d --wait clickhouse >/dev/null
    clink_compose up -d --wait clink-coordinator clink-worker >/dev/null
    ch_query "CREATE DATABASE ${DB}"
    ch_query "$(sed -e "s/{db}/${DB}/g" -e "s/{table}/${TABLE}/g" table.sql)"
    # The table at about 127 bytes a row uncompressed, with merge headroom.
    local free need
    free="$(ch_query "SELECT free_space FROM system.disks WHERE name = 'default'")"
    need=$((rows * 128))
    [ "${free}" -ge "${need}" ] ||
        die "the server has $((free / 1073741824)) GiB free, the table needs about $((need / 1073741824)) GiB"
}

record_server_premise() {
    # Once per campaign, from the first fresh server: what the plan asks the
    # premise to carry about the table and its settings. The INSERTs' own
    # effective settings come per trial from query_log; these are the server's
    # and the session's defaults.
    local d="${results}/server"
    mkdir -p "${d}"
    ch_query "SELECT version() AS version FORMAT JSONEachRow" > "${d}/version.json"
    ch_query "SHOW CREATE TABLE ${DB}.${TABLE} FORMAT TSVRaw" > "${d}/ddl.sql"
    ch_query "SELECT name, value, changed FROM system.merge_tree_settings WHERE name LIKE '%fsync%' OR name IN ('parts_to_delay_insert', 'parts_to_throw_insert', 'async_insert', 'min_rows_for_wide_part', 'min_bytes_for_wide_part') ORDER BY name FORMAT JSONEachRow" > "${d}/merge_tree_settings.json"
    ch_query "SELECT name, value, changed FROM system.settings WHERE name IN ('async_insert', 'wait_for_async_insert', 'max_insert_block_size', 'min_insert_block_size_rows', 'min_insert_block_size_bytes', 'max_insert_threads', 'insert_deduplicate', 'max_threads') ORDER BY name FORMAT JSONEachRow" > "${d}/session_default_settings.json"
    if [ -n "${CLICKHOUSE_DOCKER_HOST}" ]; then
        DOCKER_HOST="${CLICKHOUSE_DOCKER_HOST}" docker info --format '{{json .}}' > "${d}/host-docker.json"
    else
        cp "${results}/clink-host-docker.json" "${d}/host-docker.json"
    fi
    python3 measure.py premise --out "${premise}" \
        --json server.version="${d}/version.json" --text server.table_ddl="${d}/ddl.sql" \
        --json server.merge_tree_settings="${d}/merge_tree_settings.json" \
        --json server.session_default_settings="${d}/session_default_settings.json"
    python3 - "${d}/host-docker.json" "${premise}" <<'PY'
import json, sys
info, premise = json.load(open(sys.argv[1])), sys.argv[2]
p = json.load(open(premise))
p["server"]["host"] = {k: info.get(k) for k in ("ID", "NCPU", "MemTotal", "Architecture", "OperatingSystem", "KernelVersion")}
json.dump(p, open(premise, "w"), indent=2)
PY
}

failures=0
run_trial() {
    local variant="$1" trial="$2" batch_rows="$3" dir sql c0 c1 s0 s1 row_bytes=""
    local -a extra=()
    dir="${results}/${variant}-t${trial}"
    [ ! -e "${dir}/trial.json" ] || { say "${variant} t${trial}: already recorded, kept"; return 0; }
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
        cat "${dir}/submit.log" >&2
        die "submit failed"
    }
    python3 measure.py sample --base "http://127.0.0.1:${CLINK_HTTP_PORT}" --out "${dir}/samples.json" \
        --max-runtime "${MAX_RUNTIME_S}" || true
    c1="$(clink_cpu_usec)"
    s1="$(server_cpu_usec)"
    clink_compose logs --no-color clink-worker > "${dir}/worker.log" 2>&1 || true

    if [ "${variant}" != "blackhole" ]; then
        ch_query "SYSTEM FLUSH LOGS"
        ch_query "$(python3 measure.py inserts-sql --table "${DB}.${TABLE}")" > "${dir}/inserts.ndjson"
        ch_query "$(python3 measure.py insert-settings-sql --table "${DB}.${TABLE}")" > "${dir}/insert-settings.json"
        ch_query "$(python3 verify.py checksum-sql --table "${DB}.${TABLE}")" > "${dir}/landed.json"
        python3 verify.py gate --expected "${dataset}/oracle.json" --actual "${dir}/landed.json" \
            --rows "${rows}" --out "${dir}/gate.json" || true
        row_bytes="$(ch_query "SELECT round(sum(data_uncompressed_bytes) / sum(rows), 1) FROM system.parts WHERE database = '${DB}' AND table = '${TABLE}' AND active")"
        extra=(--gate "${dir}/gate.json" --inserts "${dir}/inserts.ndjson"
               --insert-settings "${dir}/insert-settings.json" --row-bytes "${row_bytes}")
    fi
    python3 measure.py record --variant "${variant}" --trial "${trial}" --rows "${rows}" \
        --image-id "${image_id}" --samples "${dir}/samples.json" ${extra[@]+"${extra[@]}"} \
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
        if [ -n "${LEGACY_BATCH_ROWS}" ]; then
            batch_rows="${LEGACY_BATCH_ROWS}"
            from="LEGACY_BATCH_ROWS"
        else
            batch_rows="$(python3 measure.py legacy-batch-rows --campaign "${results}")" ||
                die "no passed native trial to take the legacy batch_rows from; set LEGACY_BATCH_ROWS"
            from="native cell mean rows per INSERT"
        fi
        python3 measure.py premise --out "${premise}" \
            --set legacy.batch_rows="${batch_rows}" --set legacy.batch_interval_ms="${BATCH_INTERVAL_MS}" \
            --set legacy.batch_rows_from="${from}"
    fi
    for trial in $(seq 1 "${TRIALS}"); do
        run_trial "${variant}" "${trial}" "${batch_rows}"
    done
done

summarised=1
python3 measure.py summarise --campaign "${results}"
if [ "${failures}" -ne 0 ]; then
    die "${failures} trial(s) failed the gate; their figures are excluded"
fi
