#!/usr/bin/env bash
# QUAL-07's runner: ONE canonical dataset, every declared query on both
# engines, drained outputs judged under the class queries.json declares.
#
# This harness makes NO performance claims: both engines run to
# completion on the same box against the same topics and only the drained
# CONTENT is compared. clink runs the working tree's host build; the
# reference engine runs containerised from the nexmark harness's compose
# file, which pins its version.
#
#   ./run.sh                          # every query in queries.json
#   QUERIES="q4 q18" ./run.sh         # a subset
#   EVENTS=200000 PARALLELISM=2 ./run.sh
#
# Per query: submit on clink, wait until the whole pipeline stops moving
# (source-only progress is not enough - a windowed query's panes fire
# after the source drains, and cancelling early leaves a PREFIX of the
# answer, which for a top-N reads exactly like an engine that
# undercounts), settle, cancel; same on the reference engine; drain both
# output topics; judge. append queries drain every message value;
# materialised queries reduce the upsert topic to final state first
# (read_upsert_topic.py --json). A submit or drain failure records a
# NOT-GATED verdict - never a pass, never a silent skip.
set -uo pipefail

cd "$(dirname "$0")"
NX="$(cd ../nexmark_compare && pwd)"
CLINK_ROOT="$(cd ../.. && pwd)"
BUILD_DIR="${BUILD_DIR:-$CLINK_ROOT/build}"
PY="$NX/../flink_compare/.venv/bin/python"
[ -x "$PY" ] || PY=python3

EVENTS="${EVENTS:-500000}"
TPS="${TPS:-1000}"       # low tps -> datetime spans many windows (windowed queries fire mid-stream)
PAR="${PARALLELISM:-4}"
RESULTS="${OUT:-results}"
DATA_DIR="${DATA_DIR:-/tmp/nxsemantic-data}"
SETTLE_S="${SETTLE_S:-8}"
PROJECT=nxsemantic
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
FLINK_JM="${PROJECT}-flink-jobmanager-1"
FLINK_REST=8081          # compose maps the reference engine's REST here
JM_HTTP=8095             # host clink coordinator http (8081 is taken above)
COORD_PORT=7105

QUERIES="${QUERIES:-$(python3 -c '
import json, pathlib
d = json.loads(pathlib.Path("queries.json").read_text())
print(" ".join(k for k in d if not k.startswith("_")))')}"

CLINK_PIDS=()
cleanup() {
    [ "${#CLINK_PIDS[@]}" -gt 0 ] && { kill "${CLINK_PIDS[@]}" 2>/dev/null; sleep 1
        kill -9 "${CLINK_PIDS[@]}" 2>/dev/null; }
    if [ -n "${KEEP_UP:-}" ]; then
        echo "  (KEEP_UP set: stack and topics left running)"
        return
    fi
    docker compose -p "$PROJECT" -f "$NX/docker-compose.yml" --profile flink down -v \
        --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

step() { printf '\n=== %s ===\n' "$*"; }
decl() {  # query field default -> value from queries.json
    python3 -c "
import json, pathlib, sys
d = json.loads(pathlib.Path('queries.json').read_text())
print(d[sys.argv[1]].get(sys.argv[2], sys.argv[3]))" "$1" "$2" "${3:-}"
}
recreate_topic() {  # name
    $KEX --delete --topic "$1" >/dev/null 2>&1
    while $KEX --list 2>/dev/null | grep -qx "$1"; do sleep 1; done
    $KEX --create --topic "$1" --partitions "$PAR" --replication-factor 1 >/dev/null 2>&1
}

mkdir -p "$RESULTS" "$DATA_DIR"
rm -f "$RESULTS"/*.json "$RESULTS"/*.txt

echo "semantic comparison: $(echo "$QUERIES" | wc -w | tr -d ' ') queries, $EVENTS events, par=$PAR"

step "1. Build clink (node, submit_sql, nexmark_dump)"
cmake --build "$BUILD_DIR" --target clink_node clink_submit_sql nexmark_dump --parallel 10 \
    >/dev/null 2>&1 || { echo "clink build failed"; exit 1; }

step "2. Build the reference engine's SQL runner jar"
( cd "$NX/flink-job" && { mvn -q -o -DskipTests package 2>/dev/null || mvn -q -DskipTests package; } ) \
    || { echo "jar build failed"; exit 1; }

step "3. Bring up Kafka + the reference engine"
docker compose -p "$PROJECT" -f "$NX/docker-compose.yml" --profile flink up -d >/dev/null 2>&1
for _ in $(seq 1 45); do
    docker exec ${PROJECT}-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 \
        >/dev/null 2>&1 && break
    sleep 2
done
for _ in $(seq 1 30); do docker exec "$FLINK_JM" flink list >/dev/null 2>&1 && break; sleep 2; done
docker cp "$NX/flink-job/target/nexmark-sql.jar" "$FLINK_JM:/tmp/nexmark-sql.jar" >/dev/null 2>&1

step "4. Generate + load the dataset ($EVENTS events, tps=$TPS, $PAR partitions)"
"$BUILD_DIR/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR" | tail -1
recreate_topic nx-person
recreate_topic nx-auction
recreate_topic nx-bid
"$PY" "$NX/driver/load_ndjson.py" --dir "$DATA_DIR" --bootstrap localhost:9092 --prefix nx- \
    2>/dev/null | tail -1

step "5. Start the clink cluster (host build, http $JM_HTTP)"
slots=$(( PAR * 12 )); [ "$slots" -lt 8 ] && slots=8
"$BUILD_DIR/clink_node" --role=coordinator --port=$COORD_PORT --http-port=$JM_HTTP \
    >"$RESULTS/clink-coordinator.log" 2>&1 &
CLINK_PIDS+=($!)
sleep 2
for i in 1 2 3 4; do
    "$BUILD_DIR/clink_node" --role=worker --coordinator-host=127.0.0.1 \
        --coordinator-port=$COORD_PORT --id=worker-$i --slots="$slots" \
        >"$RESULTS/clink-worker-$i.log" 2>&1 &
    CLINK_PIDS+=($!)
done
sleep 3

# Whole-pipeline settle for a clink job: total records_out across every
# operator, polled until stable.
settle_clink() {  # job-id
    local prev=-1 cur
    for _ in $(seq 1 60); do
        cur=$(curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$1/operators" 2>/dev/null \
              | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(sum(int(o.get("records_out",0) or 0) for o in d.get("operators",[])))' 2>/dev/null || echo 0)
        [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && return 0
        prev=$cur; sleep 2
    done
    return 1
}
settle_flink() {  # job-id
    local prev=-1 cur
    for _ in $(seq 1 90); do
        cur=$(curl -fsS "http://127.0.0.1:$FLINK_REST/jobs/$1" 2>/dev/null \
              | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(max([int(v.get("metrics",{}).get("write-records",0) or 0) for v in d.get("vertices",[])] or [0]))' 2>/dev/null || echo 0)
        [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && return 0
        prev=$cur; sleep 2
    done
    return 1
}

run_query() {  # query
    local q=$1 variant not_gated=""
    variant=$(decl "$q" variant kafka)
    local suffix=""; [ "$variant" = "upsert" ] && suffix="_up"
    local ct="sem-$q-clink" ft="sem-$q-flink"
    recreate_topic "$ct"
    recreate_topic "$ft"

    # --- clink ---
    sed -e "s#__BROKERS__#localhost:9092#" -e "s#__OUT__#$ct#" \
        "$NX/queries/clink/${q}${suffix}.tmpl.sql" > "$DATA_DIR/$q-clink.sql"
    local cjid
    cjid=$("$BUILD_DIR/clink_submit_sql" --file "$DATA_DIR/$q-clink.sql" \
            --coordinator-host 127.0.0.1 --coordinator-port "$JM_HTTP" \
            --name "sem_$q" --parallelism "$PAR" 2>/dev/null \
          | python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except Exception: pass')
    if [ -z "$cjid" ]; then
        not_gated="clink submit failed"
    else
        settle_clink "$cjid" || not_gated="clink pipeline never settled"
        sleep "$SETTLE_S"
        curl -fsS -X POST "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$cjid/cancel" >/dev/null 2>&1 || true
    fi

    # --- the reference engine ---
    sed "s#__OUT__#$ft#" "$NX/flink-job/queries/${q}${suffix}.tmpl.sql" > "$DATA_DIR/$q-flink.sql"
    docker cp "$DATA_DIR/$q-flink.sql" "$FLINK_JM:/tmp/sem.sql" >/dev/null 2>&1
    docker exec "$FLINK_JM" flink run -d -p "$PAR" /tmp/nexmark-sql.jar /tmp/sem.sql \
        > "$DATA_DIR/$q-flink-submit.err" 2>&1
    local fjid
    fjid=$(grep -oE '[0-9a-f]{32}' "$DATA_DIR/$q-flink-submit.err" | head -1)
    if [ -z "$fjid" ]; then
        not_gated="${not_gated:+$not_gated; }reference submit failed: $(sed -n '1p' "$DATA_DIR/$q-flink-submit.err")"
    else
        settle_flink "$fjid" || not_gated="${not_gated:+$not_gated; }reference pipeline never settled"
        sleep "$SETTLE_S"
        docker exec "$FLINK_JM" flink cancel "$fjid" >/dev/null 2>&1 || true
    fi

    # --- drain + judge ---
    local ca="$RESULTS/$q-clink.out" fb="$RESULTS/$q-flink.out"
    if [ "$variant" = "upsert" ]; then
        "$PY" "$NX/driver/read_upsert_topic.py" --bootstrap localhost:9092 --topic "$ct" --json \
            > "$ca" 2>/dev/null || echo "{\"topic\":\"$ct\",\"error\":\"drain failed\"}" > "$ca"
        "$PY" "$NX/driver/read_upsert_topic.py" --bootstrap localhost:9092 --topic "$ft" --json \
            > "$fb" 2>/dev/null || echo "{\"topic\":\"$ft\",\"error\":\"drain failed\"}" > "$fb"
    else
        "$PY" "$NX/driver/read_upsert_topic.py" --bootstrap localhost:9092 --topic "$ct" --values \
            > "$ca" 2>/dev/null || not_gated="${not_gated:+$not_gated; }clink drain failed"
        "$PY" "$NX/driver/read_upsert_topic.py" --bootstrap localhost:9092 --topic "$ft" --values \
            > "$fb" 2>/dev/null || not_gated="${not_gated:+$not_gated; }reference drain failed"
    fi
    python3 judge.py --query "$q" --a "$ca" --b "$fb" \
        --out "$RESULTS/$q-verdict.json" ${not_gated:+--not-gated "$not_gated"}
}

for q in $QUERIES; do
    step "$q"
    run_query "$q"
done

step "Verdict"
python3 - "$RESULTS" <<'PY'
import json, pathlib, sys
res = pathlib.Path(sys.argv[1])
verdicts = sorted(res.glob("*-verdict.json"))
agree = [v for v in verdicts if json.loads(v.read_text()).get("equal")]
diverge = [v for v in verdicts
           if (d := json.loads(v.read_text())) and d.get("gated") and not d.get("equal")]
ungated = [v for v in verdicts if not json.loads(v.read_text()).get("gated")]
print(f"{len(agree)}/{len(verdicts)} agree, {len(diverge)} diverge, {len(ungated)} not gated")
for v in diverge + ungated:
    d = json.loads(v.read_text())
    print(f"  {d['query']}: {'DIVERGE' if d.get('gated') else 'NOT GATED'} - {d['detail']}")
sys.exit(0 if len(agree) == len(verdicts) and verdicts else 1)
PY
