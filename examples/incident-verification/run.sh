#!/usr/bin/env bash
# The incident-verification example, and the workflow's gate.
#
#   CLINK_BIN_DIR=../../build ./run.sh
#
# Runs a captured SQL job on a local cluster (a Coordinator and two Workers),
# kills the Worker running it right after it publishes a snapshot, lets the job
# recover on the other Worker, then:
#   1. collects the incident's evidence into a bundle and moves it elsewhere;
#   2. verifies the bundle offline, with network access denied;
#   3. replays the job's window operator across every captured epoch and
#      compares it with an oracle that reads nothing the job wrote;
#   4. freezes the restored run's first epoch as a regression test and runs it;
#   5. shows an incomplete bundle giving a partial result and a corrupted one
#      giving a failure;
#   6. measures collection, verification and the capture's overhead.
#
# Needs clink_node and clink built in CLINK_BIN_DIR (default ../../build, with
# fault injection compiled in, as a build with tests has it), python3 and curl.
# Everything runs on this machine on 127.0.0.1. WORK names the directory it
# writes to (default a new temporary one); results/ inside it keeps the
# reports, the logs and summary.txt. Exits 0 only when every step shows what
# it should.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$(cd "${CLINK_BIN_DIR:-${HERE}/../../build}" && pwd)"
WORK="${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/clink-incident.XXXXXX")}"
RECORDS="${RECORDS:-24000}"
RATE="${RATE:-3000}"
KILL_AT="${KILL_AT:-10}"               # the publish after which worker-1 exits
INTERVAL_MS="${INTERVAL_MS:-500}"
FEED_PORT="${FEED_PORT:-28190}"
COORD_PORT="${COORD_PORT:-26190}"
COORD_HTTP="${COORD_HTTP:-28191}"
for b in clink_node clink; do
    [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
"${BIN}/clink" --capabilities 2>/dev/null | grep -q "fault injection compiled in *yes" || {
    echo "run.sh: ${BIN}/clink has no fault injection compiled in (build with tests)" >&2; exit 1; }

JOB="${WORK}/job"           # everything the running job writes
R="${WORK}/results"
mkdir -p "${JOB}"/{ha,checkpoints,capture,traces} "${R}/logs" "${WORK}/tmp"
# The trace validator keeps its working directory when it rejects a trace, so
# its TLC log can be read; keep that inside WORK. Nothing writes to the
# source tree.
export TMPDIR="${WORK}/tmp"
export PYTHONDONTWRITEBYTECODE=1
pids=()
cleanup() {
    for p in ${pids[@]+"${pids[@]}"}; do kill "${p}" 2>/dev/null || true; done
    wait 2>/dev/null || true
}
trap cleanup EXIT

say() { printf '\n==> %s\n' "$*" | tee -a "${R}/summary.txt"; }
note() { printf '    %s\n' "$*" | tee -a "${R}/summary.txt"; }
fail() { printf 'run.sh: FAILED: %s\n' "$*" | tee -a "${R}/summary.txt" >&2; exit 1; }
now() { python3 -c 'import time; print(time.time())'; }
since() { python3 -c "import sys, time; print('%.2f' % (time.time() - float(sys.argv[1])))" "$1"; }
await_http() {
    for _ in $(seq 1 100); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done
    fail "$2 did not come up"
}
# The verdict of check $2 in JSON report $1.
verdict() { python3 -c "import json, sys
r = json.load(open(sys.argv[1]))
print(next((c['verdict'] for c in r['checks'] if c['id'] == sys.argv[2]), 'absent'))" "$1" "$2"; }
# Verify, with the network denied where this machine can deny it.
offline() {
    if [ "$(uname)" = Darwin ]; then
        sandbox-exec -p '(version 1)(allow default)(deny network*)' "$@"
    elif unshare -rn true 2>/dev/null; then
        unshare -rn "$@"
    else
        "$@"
    fi
}

say "Starting the feed (${RECORDS} records at ${RATE}/s), a Coordinator and worker-1"
python3 "${HERE}/feed.py" --port="${FEED_PORT}" --records="${RECORDS}" --rate="${RATE}" \
    >"${R}/logs/feed.log" 2>&1 &
pids+=($!)
export CLINK_PROTOCOL_TRACE_DIR="${JOB}/traces"
"${BIN}/clink_node" --role=coordinator --bind-host=127.0.0.1 --port="${COORD_PORT}" \
    --http-port="${COORD_HTTP}" --ha-dir="${JOB}/ha" >"${R}/logs/coordinator.log" 2>&1 &
pids+=($!)
await_http "http://127.0.0.1:${COORD_HTTP}/api/v1/health" "the Coordinator"
# worker-1 exits right after its ${KILL_AT}th snapshot publish. Both Workers
# keep every completed checkpoint, so each captured epoch keeps its starting
# state for the replay.
CLINK_FAULT_INJECT="checkpoint.after_publish=exit:1@${KILL_AT}" \
    "${BIN}/clink_node" --role=worker --id=worker-1 --coordinator-host=127.0.0.1 \
    --coordinator-port="${COORD_PORT}" --port=$((COORD_PORT + 1)) --data-host=127.0.0.1 \
    --slots=16 --http-port=$((COORD_HTTP + 1)) --checkpoint-num-retained=100000 \
    >"${R}/logs/worker-1.log" 2>&1 &
worker1=$!
pids+=("${worker1}")
await_http "http://127.0.0.1:$((COORD_HTTP + 1))/api/v1/health" "worker-1"
sed -e "s|@FEED_URL@|http://127.0.0.1:${FEED_PORT}|" -e "s|@OUT@|${JOB}/out.jsonl|" \
    "${HERE}/job.sql.in" >"${JOB}/job.sql"

say "Submitting the job with the flight recorder and checkpoints every ${INTERVAL_MS} ms"
"${BIN}/clink" run "${JOB}/job.sql" --coordinator-host=127.0.0.1 --coordinator-port="${COORD_HTTP}" \
    --name=incident-demo --checkpoint-dir="${JOB}/checkpoints" \
    --checkpoint-interval-ms="${INTERVAL_MS}" --capture-dir="${JOB}/capture" \
    --capture-records=1000000 >"${R}/logs/submit.log" 2>&1 || fail "the submission was refused"
grep -q '"ok":true' "${R}/logs/submit.log" || fail "the submission was refused"
# worker-2 joins only now, so the job starts on worker-1 and fails over here.
"${BIN}/clink_node" --role=worker --id=worker-2 --coordinator-host=127.0.0.1 \
    --coordinator-port="${COORD_PORT}" --port=$((COORD_PORT + 2)) --data-host=127.0.0.1 \
    --slots=16 --http-port=$((COORD_HTTP + 2)) --checkpoint-num-retained=100000 \
    >"${R}/logs/worker-2.log" 2>&1 &
pids+=($!)

say "Waiting for worker-1 to exit at the fault point"
set +e
wait "${worker1}"
code=$?
set -e
[ "${code}" = 1 ] || fail "worker-1 exited ${code}, not at the fault point"
grep -q "fired: checkpoint.after_publish" "${R}/logs/worker-1.log" ||
    fail "worker-1 exited without the fault firing"
markers="${JOB}/checkpoints/_jobs/1"
completed=$(ls "${markers}" | sed -n 's/^COMPLETED-\([0-9]*\)$/\1/p' | sort -n | tail -1)
torn=$(find "${JOB}/checkpoints/_state" -name 'checkpoint-*.snap' | sed 's/.*checkpoint-\([0-9]*\)\.snap/\1/' | sort -n | tail -1)
[ -n "${completed}" ] && [ "${completed}" -ge 1 ] || fail "no checkpoint completed before the kill"
[ "${torn}" = $((completed + 1)) ] && [ ! -e "${markers}/COMPLETED-${torn}" ] ||
    fail "the kill did not land inside checkpoint $((completed + 1)) (newest snapshot ${torn})"
note "worker-1 exited right after publishing a snapshot of checkpoint ${torn},"
note "which never completed; the newest completed checkpoint was ${completed}"
ls "${markers}" >"${R}/markers-at-kill.txt"

say "Waiting for the job to recover on worker-2 and close every window"
expected_rows=$(python3 "${HERE}/oracle.py" --records="${RECORDS}" | wc -l | tr -d ' ')
for i in $(seq 1 240); do
    [ -f "${JOB}/out.jsonl" ] && [ "$(sort -u "${JOB}/out.jsonl" | wc -l | tr -d ' ')" -ge "${expected_rows}" ] && break
    [ "${i}" = 240 ] && fail "the job did not produce its ${expected_rows} rows"
    sleep 0.5
done
restored=$(tr '\n' ' ' <"${markers}/RESTORED")
note "RESTORED: ${restored}"
next=$(sed -n 's/^next=//p' "${markers}/RESTORED")
restore=$(sed -n 's/^restore=//p' "${markers}/RESTORED")
[ "${restore}" = "${completed}" ] || fail "the job restored from ${restore}, not ${completed}"

say "Collecting the evidence while the job runs"
bundle_src="${WORK}/collected/incident-1"
mkdir -p "${WORK}/collected"
attempt=0
start=$(now)
while :; do
    attempt=$((attempt + 1))
    if "${BIN}/clink" incident-collect --checkpoint-dir="${JOB}/checkpoints" --job=1 \
        --ha-dir="${JOB}/ha" --capture-dir="${JOB}/capture" --trace-dir="${JOB}/traces" \
        --include-state --include-captures --epochs=1-1000000 --out="${bundle_src}" --json \
        >"${R}/collect.json" 2>"${R}/collect.err"; then
        break
    fi
    note "attempt ${attempt} refused: $(cat "${R}/collect.err")"
    [ "${attempt}" -lt 10 ] || fail "collection never completed"
done
collect_s=$(since "${start}")
"${BIN}/clink" incident-collect --checkpoint-dir="${JOB}/checkpoints" --job=1 \
    --ha-dir="${JOB}/ha" --capture-dir="${JOB}/capture" --include-captures \
    --out="${WORK}/collected/incident-1-partial" >/dev/null 2>"${R}/collect-partial.err" ||
    fail "the partial collection failed: $(cat "${R}/collect-partial.err")"
ls "${bundle_src}" >/dev/null
[ -z "$(ls -a "${WORK}/collected" | grep incomplete || true)" ] || fail "a temporary directory was left behind"
note "collected in ${collect_s} s after ${attempt} attempt(s): $(python3 -c "import json,sys; r=json.load(open(sys.argv[1])); print('%d files, %d bytes, checkpoint %d' % (r['files'], r['bytes'], r['checkpoint']))" "${R}/collect.json")"

say "Checking the job's own output against the oracle"
# The delivery level the Coordinator declared at submission, and the one the
# bundle recomputed from the job's graph: they must agree.
declared=$(sed -n 's/.*job delivery guarantee: \([A-Z_]*\).*/\1/p' "${R}/logs/coordinator.log" | head -1)
bundled=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['delivery']['level'])" "${bundle_src}/manifest.json")
[ -n "${declared}" ] && [ "${declared}" = "${bundled}" ] ||
    fail "the bundle's delivery level ${bundled} is not the Coordinator's ${declared}"
note "delivery level, declared and in the bundle: ${declared}"
python3 "${HERE}/oracle.py" --records="${RECORDS}" | sort >"${R}/oracle-sink.ndjson"
python3 - "${JOB}/out.jsonl" "${R}/oracle-sink.ndjson" "${declared}" "${restore}" \
    "${R}/sink-repeats.txt" <<'PY' | tee -a "${R}/summary.txt"
import collections, json, sys
got = collections.Counter(json.dumps(json.loads(l), sort_keys=True) for l in open(sys.argv[1]) if l.strip())
want = collections.Counter(l.strip() for l in open(sys.argv[2]) if l.strip())
extra = got - want
missing = sum((want - got).values())
unexpected = sum(n for row, n in extra.items() if row not in want)
repeats = sum(extra.values()) - unexpected
print("    sink rows %d, expected %d: missing %d, unexpected %d, repeated %d"
      % (sum(got.values()), sum(want.values()), missing, unexpected, repeats))
open(sys.argv[5], "w").write("%d\n" % repeats)
# Nothing may be lost or invented; repeats only where the output guarantee is
# at least once.
ok = missing == 0 and unexpected == 0 and (repeats == 0 or "OUTPUT_AT_LEAST_ONCE" in sys.argv[3])
if repeats:
    print("    the repeats are rows written after checkpoint %s and written again by the "
          "restored run, which %s permits" % (sys.argv[4], sys.argv[3]))
sys.exit(0 if ok else 1)
PY

say "Stopping the job and moving the bundles away from everything it wrote"
"${BIN}/clink" cancel --job-id=1 --coordinator-host=127.0.0.1 --coordinator-port="${COORD_PORT}" \
    >"${R}/logs/cancel.log" 2>&1 || true
cleanup
pids=()
mkdir -p "${WORK}/elsewhere"
mv "${bundle_src}" "${WORK}/elsewhere/incident-1"
mv "${WORK}/collected/incident-1-partial" "${WORK}/elsewhere/incident-1-partial"
mv "${JOB}" "${WORK}/job.moved-away"
B="${WORK}/elsewhere/incident-1"

window_op=$(python3 - "${B}" <<'PY'
import glob, json, os, sys
for f in glob.glob(os.path.join(sys.argv[1], "capture", "op-*", "subtask-*", "op.json")):
    if json.load(open(f))["op_type"] == "tumbling_window_row":
        print(f.split(os.sep)[-3][3:]); break
PY
)
[ -n "${window_op}" ] || fail "the bundle holds no capture of the window operator"
python3 "${HERE}/oracle.py" --records="${RECORDS}" --shape=window >"${R}/oracle-window.ndjson"

say "Verifying offline with the network denied (profile full)"
if [ "$(uname)" = Darwin ]; then
    if sandbox-exec -p '(version 1)(allow default)(deny network*)' \
        curl -fsS -m 2 "http://127.0.0.1:${COORD_HTTP}/" >/dev/null 2>&1; then
        fail "the sandbox does not deny the network"
    fi
    note "network denied with sandbox-exec"
elif unshare -rn true 2>/dev/null; then
    note "network denied with unshare -rn"
else
    note "no way to deny the network here; verifying without the sandbox"
fi
start=$(now)
set +e
offline "${BIN}/clink" incident-verify "${B}" --profile=full --op="${window_op}" \
    --expected="${R}/oracle-window.ndjson" --formal-dir="${HERE}/../../formal" \
    --report="${R}/verify-full.json" >"${R}/verify-full.txt" 2>&1
code=$?
set -e
verify_s=$(since "${start}")
cat "${R}/verify-full.txt" >>"${R}/summary.txt"
for id in bundle.manifest bundle.artifacts bundle.completeness checkpoint.marker \
    checkpoint.restore_rule checkpoint.participants state.decode protocol.trace_structure \
    replay.determinism replay.recorded_output replay.expected_output; do
    v=$(verdict "${R}/verify-full.json" "${id}")
    [ "${v}" = passed ] || fail "${id} is ${v}"
done
conformance=$(verdict "${R}/verify-full.json" protocol.conformance)
case "${conformance}:${code}" in
    passed:0) note "every check passed, the TLC trace validator included" ;;
    unavailable:3) note "every check passed but protocol.conformance, unavailable here: a partial result (exit 3)" ;;
    *) fail "protocol.conformance is ${conformance} with exit ${code}" ;;
esac
note "verified in ${verify_s} s"
python3 - "${R}/verify-full.json" "${restore}" "${next}" "$(cat "${R}/sink-repeats.txt")" <<'PY' | tee -a "${R}/summary.txt"
import json, re, sys
report = json.load(open(sys.argv[1]))
restore, nxt, repeats = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
det = next(c for c in report["checks"] if c["id"] == "replay.determinism")
discarded = {}
for d in det["details"]:
    m = re.search(r"epoch (\d+): passed - (\d+) emissions", d)
    if m and restore < int(m.group(1)) < nxt:
        discarded[int(m.group(1))] = int(m.group(2))
print("    the restore discarded epoch(s) %s, whose replay emits %d row(s); the sink holds %d "
      "repeated row(s)" % (", ".join(map(str, sorted(discarded))) or "none",
                           sum(discarded.values()), repeats))
PY

if [ "${conformance}" = passed ]; then
    say "A protocol trace the specification forbids fails conformance"
    # The same bundle, with the Coordinator's redeploy rewritten to restore
    # from the checkpoint that never completed, and its digest updated as if
    # it had been collected that way.
    cp -R "${B}" "${WORK}/elsewhere/incident-1-bad-trace"
    python3 - "${WORK}/elsewhere/incident-1-bad-trace" "${torn}" <<'PY'
import glob, hashlib, json, os, re, sys
b, torn = sys.argv[1], sys.argv[2]
f = glob.glob(os.path.join(b, "traces", "coordinator-*.ndjson"))[0]
lines = [re.sub(r'"restore":\d+', '"restore":%s' % torn, l) if '"Redeploy"' in l else l
         for l in open(f).read().splitlines()]
open(f, "w").write("\n".join(lines) + "\n")
m = json.load(open(os.path.join(b, "manifest.json")))
for a in m["artifacts"]:
    if a["path"] == os.path.relpath(f, b):
        data = open(f, "rb").read()
        a["bytes"], a["sha256"] = len(data), hashlib.sha256(data).hexdigest()
json.dump(m, open(os.path.join(b, "manifest.json"), "w"), indent=2)
PY
    set +e
    offline "${BIN}/clink" incident-verify "${WORK}/elsewhere/incident-1-bad-trace" --profile=full \
        --formal-dir="${HERE}/../../formal" --report="${R}/verify-bad-trace.json" \
        >"${R}/verify-bad-trace.txt" 2>&1
    code=$?
    set -e
    [ "${code}" = 1 ] && [ "$(verdict "${R}/verify-bad-trace.json" protocol.conformance)" = failed ] ||
        fail "a redeploy from checkpoint ${torn} was not refused (exit ${code})"
    note "exit 1: $(grep -m1 'DIVERGES' "${R}/verify-bad-trace.txt" | sed 's/^ *- formal-check: *//' | cut -c1-160)"
fi

say "Freezing the restored run's first epoch (${next}) as a regression test"
"${BIN}/clink" incident-regress "${B}" --op="${window_op}" --epoch="${next}" \
    --out="${WORK}/regression" | tee -a "${R}/summary.txt"
"${BIN}/clink" replay --run-test="${WORK}/regression" | tee -a "${R}/summary.txt"
python3 - "${WORK}/regression/golden.ndjson" <<'PY'
import sys
p = sys.argv[1]
lines = open(p).read().splitlines()
lines[0] = lines[0].replace('"total":', '"total":1', 1)
open(p, "w").write("\n".join(lines) + "\n")
PY
if "${BIN}/clink" replay --run-test="${WORK}/regression" >"${R}/regression-doctored.txt" 2>&1; then
    fail "a doctored golden still passed"
fi
note "with its golden doctored it fails: $(head -c 300 "${R}/regression-doctored.txt")"

say "An incomplete bundle (no snapshots) gives a partial result"
set +e
offline "${BIN}/clink" incident-verify "${WORK}/elsewhere/incident-1-partial" --profile=replay \
    --report="${R}/verify-partial.json" >"${R}/verify-partial.txt" 2>&1
code=$?
set -e
[ "${code}" = 3 ] || fail "the incomplete bundle exited ${code}, not 3"
note "exit 3: state.decode $(verdict "${R}/verify-partial.json" state.decode), replay.determinism $(verdict "${R}/verify-partial.json" replay.determinism)"

say "A corrupted bundle gives a failure"
cp -R "${B}" "${WORK}/elsewhere/incident-1-corrupted"
victim=$(find "${WORK}/elsewhere/incident-1-corrupted/capture" -name 'epoch-*.cap' | head -1)
python3 - "${victim}" <<'PY'
import sys
p = sys.argv[1]
b = bytearray(open(p, "rb").read())
b[len(b) // 2] ^= 0x5A
open(p, "wb").write(bytes(b))
PY
set +e
offline "${BIN}/clink" incident-verify "${WORK}/elsewhere/incident-1-corrupted" --profile=replay \
    --report="${R}/verify-corrupted.json" >"${R}/verify-corrupted.txt" 2>&1
code=$?
set -e
[ "${code}" = 1 ] || fail "the corrupted bundle exited ${code}, not 1"
[ "$(verdict "${R}/verify-corrupted.json" bundle.artifacts)" = failed ] || fail "bundle.artifacts did not fail"
note "exit 1: bundle.artifacts failed; the replay checks are $(verdict "${R}/verify-corrupted.json" replay.determinism)"

say "Measuring the capture's overhead on a bounded job"
python3 "${HERE}/measure.py" --bin="${BIN}" --work="${WORK}/measure" | tee -a "${R}/summary.txt"

bundle_bytes=$(du -sk "${B}" | cut -f1)
python3 - "${R}/measurements.json" "${collect_s}" "${verify_s}" "${bundle_bytes}" "${B}" <<'PY'
import json, os, sys
files = sum(len(f) for _, _, f in os.walk(sys.argv[5]))
json.dump({"collect_seconds": float(sys.argv[2]), "verify_full_seconds": float(sys.argv[3]),
           "bundle_kib": int(sys.argv[4]), "bundle_files": files}, open(sys.argv[1], "w"), indent=2)
PY
say "Done: every step showed what it should. Results in ${R}"
