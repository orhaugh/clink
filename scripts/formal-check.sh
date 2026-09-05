#!/usr/bin/env bash
# Model-check the exactly-once protocol specification (design record 012).
#
#   scripts/formal-check.sh                 # every model under formal/models/
#   scripts/formal-check.sh MC_KafkaSmall   # one model
#   scripts/formal-check.sh --mutants       # every mutant under formal/mutants/
#                                           # must be REFUTED by TLC
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
# Knobs: TLC_WORKERS (default auto), TLC_HEAP (default 2g), TLC_EXTRA (extra
# TLC flags), CLINK_FORMAL_TOOLS_DIR (jar cache).
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
fi

# The CommunityModules jar is compiled against a newer TLC than the release
# jar and shadows classes in it, so it goes on the classpath only for the
# models that import a community module (the trace validator does).
WITH_CM=""
if grep -qsE '^EXTENDS.*\b(Json|IOUtils|SequencesExt|FiniteSetsExt)\b' "$ROOT"/formal/*.tla "$ROOT"/formal/*/*.tla 2>/dev/null; then
    WITH_CM=1
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

failed=()
for cfg in "${CFGS[@]}"; do
    name="$(basename "$cfg" .cfg)"
    tla="$DIR/$name.tla"
    [ -f "$tla" ] || { echo "formal-check: $tla missing for $cfg" >&2; exit 1; }
    log="$WORK/$name.log"
    echo "formal-check: TLC $MODE/$name"
    start=$(date +%s)
    set +e
    # -DTLA-Library lets the model modules under formal/models find
    # ExactlyOnce.tla one directory up. Deadlock checking stays ON: a state
    # with no enabled step that is not the run's quiescent end is a wedge.
    java -XX:+UseParallelGC "-Xmx${TLC_HEAP:-2g}" "-DTLA-Library=$ROOT/formal" \
        -cp "$TLA_JAR${WITH_CM:+:$CM_JAR}" tlc2.TLC \
        -workers "${TLC_WORKERS:-auto}" -metadir "$WORK/$name.states" \
        -config "$cfg" ${TLC_EXTRA:-} "$tla" >"$log" 2>&1
    rc=$?
    set -e
    secs=$(( $(date +%s) - start ))
    states="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 || true)"
    depth="$(grep -oE 'depth of the complete state graph search is [0-9]+' "$log" | tail -1 | awk '{print $NF}' || true)"
    if [ "$MODE" = models ]; then
        if [ $rc -eq 0 ]; then
            echo "formal-check:   ok in ${secs}s (${states:-?}, depth ${depth:-?})"
        else
            echo "formal-check:   FAILED (TLC exit $rc) after ${secs}s"
            sed -n '1,200p' "$log"
            failed+=("$name")
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
            else
                echo "formal-check:   REFUTED but expected accepted after ${secs}s (${inv:-violation}): the rule this mutant disables has become load-bearing on its own; update formal/mutants/expected.txt and the published page"
                failed+=("$name")
            fi
        elif [ $rc -eq 0 ]; then
            if [ "$expect" = accepted ]; then
                echo "formal-check:   accepted as recorded in ${secs}s (${states:-?}): guarded by a later rule, see formal/README.md"
            else
                echo "formal-check:   NOT REFUTED after ${secs}s (${states:-?}): the model no longer sees this defect"
                failed+=("$name")
            fi
        else
            echo "formal-check:   ERROR (TLC exit $rc) after ${secs}s"
            sed -n '1,200p' "$log"
            failed+=("$name")
        fi
    fi
done

if [ ${#failed[@]} -ne 0 ]; then
    echo "formal-check: FAILED: ${failed[*]}" >&2
    exit 1
fi
echo "formal-check: all $MODE green"
