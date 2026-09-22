#!/usr/bin/env bash
# Model-check the exactly-once protocol specification (design record 012).
#
#   scripts/formal-check.sh                 # every model under formal/models/
#   scripts/formal-check.sh MC_KafkaSmall   # one model
#   scripts/formal-check.sh --mutants       # every mutant under formal/mutants/
#                                           # must be REFUTED by TLC
#   scripts/formal-check.sh --trace PATH... # validate recorded protocol traces
#                                           # (files, run directories, or a
#                                           # directory of runs) against the spec
#
# Fetches the TLA+ tools pinned in formal/tools.env (SHA-256 verified, cached
# in CLINK_FORMAL_TOOLS_DIR) and runs TLC on each configuration. A model is
# green when TLC finds no invariant violation, no deadlock and no liveness
# violation within the configuration's bounds. A mutant is judged against
# formal/mutants/expected.txt: one marked `refuted` is green only when TLC
# DOES find a violation (a mutant TLC accepts means the model can no longer
# see the defect it re-introduces, which is a regression in the model, not a
# pass); one marked `accepted` records a rule that a later rule now guards as
# well, and is green only while TLC still accepts it (the day it is refuted,
# the other guard has gone and the record is wrong). Unlisted mutants are
# expected refuted.
#
# Knobs: CHECK_JOBS (models/mutants run this many at a time, default 1),
# TRACE_JOBS (the same for traces), TLC_WORKERS (default auto, or 1 per run
# when several run side by side), TLC_HEAP (default 2g), TLC_EXTRA (extra TLC
# flags), CLINK_FORMAL_TOOLS_DIR (jar cache).
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
# shellcheck source=../formal/tools.env
. "$ROOT/formal/tools.env"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

fetch_jar() {
    local name="$1" url="$2" want="$3"
    local path="$CLINK_FORMAL_TOOLS_DIR/$name"
    if [ -f "$path" ] && [ "$(sha256_of "$path")" = "$want" ]; then
        echo "$path"
        return
    fi
    mkdir -p "$CLINK_FORMAL_TOOLS_DIR"
    echo "formal-check: fetching $name" >&2
    curl -sSfL --retry 3 -o "$path.tmp" "$url"
    local got
    got="$(sha256_of "$path.tmp")"
    if [ "$got" != "$want" ]; then
        rm -f "$path.tmp"
        echo "formal-check: $name checksum mismatch: expected $want, got $got" >&2
        echo "formal-check: refusing to run an unverified model checker; update formal/tools.env deliberately" >&2
        exit 1
    fi
    mv "$path.tmp" "$path"
    echo "$path"
}

if ! command -v java >/dev/null 2>&1; then
    echo "formal-check: java not found; TLC needs a Java 11+ runtime" >&2
    exit 1
fi

TLA_JAR="$(fetch_jar "tla2tools-$TLA_TOOLS_VERSION.jar" "$TLA_TOOLS_URL" "$TLA_TOOLS_SHA256")"
CM_JAR="$(fetch_jar "CommunityModules-deps-$COMMUNITY_MODULES_VERSION.jar" "$COMMUNITY_MODULES_URL" "$COMMUNITY_MODULES_SHA256")"

MODE=models
if [ "${1:-}" = "--mutants" ]; then
    MODE=mutants
    shift
elif [ "${1:-}" = "--trace" ]; then
    MODE=trace
    shift
fi

# The CommunityModules jar is compiled against a newer TLC than the release
# jar and shadows classes in it, so it goes on the classpath only for the
# models that import a community module (the trace validator does).
WITH_CM=""
if grep -qsE '^EXTENDS.*\b(Json|IOUtils|SequencesExt|FiniteSetsExt)\b' "$ROOT"/formal/*.tla "$ROOT"/formal/*/*.tla 2>/dev/null; then
    WITH_CM=1
fi

# --trace: validate recorded protocol traces against the specification
# (design record 012, increment 4). Each argument is a merged .ndjson trace,
# a directory of per-process trace files (one run), or a directory of such
# directories (the recorded set under formal/traces, or what a test run
# left behind). The trace module follows the events in order; TLC either
# consumes the whole trace or deadlocks at the first event no allowed step
# produces, and that event is reported.
if [ "$MODE" = trace ]; then
    [ $# -gt 0 ] || { echo "formal-check: --trace needs a trace file or directory" >&2; exit 2; }
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/clink-formal.XXXXXX")"
    trap 'rm -rf "$WORK"' EXIT
    runs=()
    for arg in "$@"; do
        if [ -f "$arg" ]; then
            runs+=("$arg")
        elif [ -d "$arg" ]; then
            if compgen -G "$arg/*.ndjson" >/dev/null; then
                runs+=("$arg")
            else
                for sub in "$arg"/*/; do
                    [ -d "$sub" ] && compgen -G "$sub/*.ndjson" >/dev/null && runs+=("${sub%/}")
                done
            fi
        else
            echo "formal-check: no such trace: $arg" >&2
            exit 2
        fi
    done
    [ ${#runs[@]} -gt 0 ] || { echo "formal-check: no traces found under: $*" >&2; exit 2; }
    # One trace: merge, decide scope, run TLC. Writes $WORK/result-N (the lines to
    # print, in order) and $WORK/status-N (accepted | skipped | failed). Traces run
    # TRACE_JOBS at a time (default 1; the CI job sets it to its core count) and
    # their reports are printed in trace order once all have finished, so the
    # output reads the same at any parallelism.
    validate_one() {
        local n="$1" run="$2" name merged events start rc secs at line
        name="$(basename "$run" .ndjson)"
        merged="$WORK/trace-$n.ndjson"
        {
            mkdir -p "$WORK/trace-$n"
            if ! python3 "$ROOT/scripts/protocol-trace-merge.py" --out "$merged" \
                    --constants "$WORK/trace-$n/TraceConstants.tla" "$run" 2>"$WORK/merge-$n.log"; then
                echo "formal-check: trace $name: $(cat "$WORK/merge-$n.log")"
                echo failed >"$WORK/status-$n"
                return
            fi
            events=$(wc -l <"$merged" | tr -d ' ')
            # A run that rescaled an operator is outside the specification's
            # scope (formal/trace/events.txt, `outside Rescale`): the model keys
            # a sink by its subtask index and fixes the set and its hosts for
            # the run, and a rescale changes both. Skipped, and said so; not a
            # divergence and not a pass.
            if grep -q '"event":"Rescale"' "$merged"; then
                echo "formal-check: trace/$name ($events events): SKIPPED, the run rescaled an operator; outside the specification until the model covers rescale"
                echo skipped >"$WORK/status-$n"
                return
            fi
            echo "formal-check: TLC trace/$name ($events events)"
            start=$(date +%s)
            set +e
            # From the work directory: whatever TLC drops beside a failing run
            # (trace-exploration files) lands there, not in the tree.
            # One worker: the trace module keeps its progress in a TLC register,
            # and the state graph is a few hundred states, so parallelism buys
            # nothing here. Deadlock checking is off (a hidden-step branch that
            # dies out is not a verdict); the postcondition TraceAccepted is.
            # The library path carries the generated TraceConstants beside the
            # specification; see the trace module's header.
            (cd "$WORK" && CLINK_TRACE_FILE="$merged" java -XX:+UseParallelGC "-Xmx${TLC_HEAP:-2g}" \
                "-DTLA-Library=$ROOT/formal:$WORK/trace-$n" -cp "$TLA_JAR:$CM_JAR" tlc2.TLC \
                -workers 1 -noGenerateSpecTE -metadir "$WORK/trace-$n.states" \
                -config "$ROOT/formal/trace/TraceExactlyOnce.cfg" ${TLC_EXTRA:-} \
                "$ROOT/formal/trace/TraceExactlyOnce.tla") >"$WORK/trace-$n.log" 2>&1
            rc=$?
            set -e
            secs=$(( $(date +%s) - start ))
            if [ $rc -eq 0 ]; then
                echo "formal-check:   accepted in ${secs}s: every event is a step the specification allows"
                echo accepted >"$WORK/status-$n"
            elif grep -q '"divergence"' "$WORK/trace-$n.log"; then
                # The postcondition printed <<"divergence", index, event>>, which
                # TLC pretty-prints one element per line: the index is on the
                # line after the marker.
                at="$(grep -A1 '"divergence"' "$WORK/trace-$n.log" | sed -n '2p' | tr -dc '0-9' || true)"
                line="$( { [ -n "$at" ] && sed -n "${at}p" "$merged"; } || true)"
                echo "formal-check:   DIVERGES after ${secs}s at event ${at:-?}: ${line:-(unknown)}"
                echo "formal-check:   no step of the specification produces this event from any state the trace reached; TLC log: $WORK/trace-$n.log"
                echo failed >"$WORK/status-$n"
            else
                echo "formal-check:   ERROR (TLC exit $rc) after ${secs}s"
                sed -n '1,200p' "$WORK/trace-$n.log"
                echo failed >"$WORK/status-$n"
            fi
        } >"$WORK/result-$n" 2>&1
    }
    jobs_max="${TRACE_JOBS:-1}"
    n=0
    for run in "${runs[@]}"; do
        n=$((n + 1))
        if [ "$jobs_max" -gt 1 ]; then
            while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$jobs_max" ]; do
                sleep 1
            done
            validate_one "$n" "$run" &
        else
            validate_one "$n" "$run"
        fi
    done
    wait
    failed=()
    accepted=0
    skipped=0
    for i in $(seq 1 "$n"); do
        cat "$WORK/result-$i"
        case "$(cat "$WORK/status-$i" 2>/dev/null)" in
            accepted) accepted=$((accepted + 1)) ;;
            skipped) skipped=$((skipped + 1)) ;;
            *) failed+=("$(basename "${runs[$((i - 1))]}" .ndjson)") ;;
        esac
    done
    if [ ${#failed[@]} -ne 0 ]; then
        trap - EXIT  # keep the work dir for the TLC logs named above
        echo "formal-check: FAILED: ${failed[*]}" >&2
        exit 1
    fi
    echo "formal-check: all ${accepted} validated trace(s) accepted; ${skipped} skipped as outside the specification"
    exit 0
fi

DIR="$ROOT/formal/$MODE"
if [ $# -gt 0 ]; then
    CFGS=()
    for m in "$@"; do
        CFGS+=("$DIR/$m.cfg")
    done
else
    CFGS=("$DIR"/*.cfg)
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/clink-formal.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

# One configuration: run TLC and judge it. Writes $WORK/result-N (the lines to
# print, in order) and $WORK/status-N (ok | failed). Configurations run
# CHECK_JOBS at a time and their reports are printed in configuration order
# once all have finished, so the output reads the same at any parallelism.
check_one() {
    local n="$1" cfg="$2" name tla log start rc secs states depth expect inv
    name="$(basename "$cfg" .cfg)"
    tla="$DIR/$name.tla"
    log="$WORK/$name.log"
    {
        if [ ! -f "$tla" ]; then
            echo "formal-check: $tla missing for $cfg"
            echo failed >"$WORK/status-$n"
            return
        fi
        echo "formal-check: TLC $MODE/$name"
        start=$(date +%s)
        set +e
        # -DTLA-Library lets the model modules under formal/models find
        # ExactlyOnce.tla one directory up. Deadlock checking stays ON: a state
        # with no enabled step that is not the run's quiescent end is a wedge.
        (cd "$WORK" && java -XX:+UseParallelGC "-Xmx${TLC_HEAP:-2g}" "-DTLA-Library=$ROOT/formal" \
            -cp "$TLA_JAR${WITH_CM:+:$CM_JAR}" tlc2.TLC \
            -workers "$WORKERS" -noGenerateSpecTE -metadir "$WORK/$name.states" \
            -config "$cfg" ${TLC_EXTRA:-} "$tla") >"$log" 2>&1
        rc=$?
        set -e
        secs=$(( $(date +%s) - start ))
        states="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 || true)"
        depth="$(grep -oE 'depth of the complete state graph search is [0-9]+' "$log" | tail -1 | awk '{print $NF}' || true)"
        if [ "$MODE" = models ]; then
            if [ $rc -eq 0 ]; then
                echo "formal-check:   ok in ${secs}s (${states:-?}, depth ${depth:-?})"
                echo ok >"$WORK/status-$n"
            else
                echo "formal-check:   FAILED (TLC exit $rc) after ${secs}s"
                sed -n '1,200p' "$log"
                echo failed >"$WORK/status-$n"
            fi
        else
            expect="$(awk -v n="$name" '$1 == n {print $2}' "$ROOT/formal/mutants/expected.txt" 2>/dev/null)"
            expect="${expect:-refuted}"
            # TLC exit 12 = invariant violated, 13 = liveness violated, 11 = deadlock.
            # A deadlock refutes a mutant too: the model's only quiescent state is
            # the run's clean end, so a stuck state is a protocol that wedged.
            if [ $rc -eq 12 ] || [ $rc -eq 13 ] || [ $rc -eq 11 ]; then
                inv="$(grep -oE 'Invariant [A-Za-z]+ is violated|Temporal properties were violated|Deadlock reached' "$log" | head -1 || true)"
                if [ "$expect" = refuted ]; then
                    echo "formal-check:   refuted in ${secs}s (${inv:-violation}, ${states:-?})"
                    echo ok >"$WORK/status-$n"
                else
                    echo "formal-check:   REFUTED but expected accepted after ${secs}s (${inv:-violation}): the rule this mutant disables has become load-bearing on its own; update formal/mutants/expected.txt and the published page"
                    echo failed >"$WORK/status-$n"
                fi
            elif [ $rc -eq 0 ]; then
                if [ "$expect" = accepted ]; then
                    echo "formal-check:   accepted as recorded in ${secs}s (${states:-?}): guarded by a later rule, see formal/README.md"
                    echo ok >"$WORK/status-$n"
                else
                    echo "formal-check:   NOT REFUTED after ${secs}s (${states:-?}): the model no longer sees this defect"
                    echo failed >"$WORK/status-$n"
                fi
            else
                echo "formal-check:   ERROR (TLC exit $rc) after ${secs}s"
                sed -n '1,200p' "$log"
                echo failed >"$WORK/status-$n"
            fi
        fi
    } >"$WORK/result-$n" 2>&1
}

jobs_max="${CHECK_JOBS:-1}"
# One TLC worker per configuration when several run side by side: TLC scales
# sublinearly across workers, so N independent checks beat one N-way check on
# the same cores. A TLC_WORKERS set by hand still wins.
if [ -n "${TLC_WORKERS:-}" ]; then
    WORKERS="$TLC_WORKERS"
elif [ "$jobs_max" -gt 1 ]; then
    WORKERS=1
else
    WORKERS=auto
fi

n=0
for cfg in "${CFGS[@]}"; do
    n=$((n + 1))
    if [ "$jobs_max" -gt 1 ]; then
        while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$jobs_max" ]; do
            sleep 1
        done
        check_one "$n" "$cfg" &
    else
        check_one "$n" "$cfg"
    fi
done
wait

failed=()
for i in $(seq 1 "$n"); do
    cat "$WORK/result-$i"
    if [ "$(cat "$WORK/status-$i" 2>/dev/null)" != ok ]; then
        failed+=("$(basename "${CFGS[$((i - 1))]}" .cfg)")
    fi
done

if [ ${#failed[@]} -ne 0 ]; then
    trap - EXIT  # keep the work dir for the TLC logs named above
    echo "formal-check: FAILED: ${failed[*]}" >&2
    exit 1
fi
echo "formal-check: all $MODE green"
