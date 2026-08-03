#!/usr/bin/env bash
# Run a clink fuzz target.
#
#   scripts/fuzz.sh                      # every target, 60s each
#   scripts/fuzz.sh cluster_frame        # one target, 60s
#   scripts/fuzz.sh cluster_frame 3600   # one target, an hour
#   scripts/fuzz.sh --minimise           # shrink the local corpora
#
# What to do when it finds something:
#
#   1. libFuzzer writes the input as fuzz/corpus/<target>/crash-<sha1>.
#   2. `git add` it. Reproducers are tracked; discovered coverage inputs
#      are not (see .gitignore for why).
#   3. tests/test_fuzz_corpus.cpp now replays it in the ordinary test
#      suite, on every platform, forever - including builds that cannot
#      run a fuzzer. That is what turns one finding into a regression test.
#   4. Fix the defect. The committed input is the proof it stays fixed.
#
# Nothing here gates CI. An unbounded search is not a gate, and pretending
# otherwise would mean either a flaky required check or a time limit so
# short it finds nothing. The corpus replay is the part that gates.
set -euo pipefail

cd "$(dirname "$0")/.."

BUILD_DIR="${FUZZ_BUILD_DIR:-build-fuzz}"
CORPUS_ROOT="fuzz/corpus"

# Apple clang does not ship libFuzzer; Homebrew LLVM does. Prefer an
# explicit CXX, then Homebrew, and say so plainly if neither works rather
# than failing inside CMake.
pick_compiler() {
    if [[ -n "${CXX:-}" ]]; then
        echo "$CXX"
        return
    fi
    for candidate in "$(brew --prefix llvm 2>/dev/null)/bin/clang++" \
                     /usr/lib/llvm-19/bin/clang++ \
                     clang++-19 \
                     clang++; do
        if [[ -x "$candidate" ]] || command -v "$candidate" >/dev/null 2>&1; then
            echo "$candidate"
            return
        fi
    done
    echo ""
}

configure() {
    local cxx
    cxx="$(pick_compiler)"
    if [[ -z "$cxx" ]]; then
        echo "▶ no C++ compiler found; set CXX to a clang that ships libFuzzer" >&2
        exit 1
    fi
    echo "▶ configuring $BUILD_DIR with $cxx"
    # SQL and the impls are off: they multiply build time and none of the
    # current targets need them. Turn CLINK_BUILD_SQL on to get the
    # sql_parse target.
    cmake -S . -B "$BUILD_DIR" \
        -DCLINK_BUILD_FUZZERS=ON \
        -DCLINK_BUILD_TESTS=OFF \
        -DCLINK_BUILD_EXAMPLES=OFF \
        -DCLINK_BUILD_IMPLS=OFF \
        -DCLINK_BUILD_SQL="${FUZZ_WITH_SQL:-OFF}" \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_CXX_COMPILER="$cxx" \
        >/dev/null
}

targets() {
    find "$BUILD_DIR/fuzz" -maxdepth 1 -name 'clink_fuzz_*' -perm -u+x 2>/dev/null \
        | grep -v 'clink_fuzz_seeds$' \
        | sed 's|.*/clink_fuzz_||' \
        | sort
}

if [[ "${1:-}" == "--minimise" ]]; then
    # Coverage-equivalent shrink. Worth doing before a long campaign so the
    # fuzzer is not re-reading redundant inputs, and before deciding whether
    # a corpus is small enough to be worth publishing.
    for t in $(targets); do
        tmp="$(mktemp -d)"
        "$BUILD_DIR/fuzz/clink_fuzz_$t" -merge=1 "$tmp" "$CORPUS_ROOT/$t" 2>&1 | tail -1
        rm -f "$CORPUS_ROOT/$t"/* 2>/dev/null || true
        cp "$tmp"/* "$CORPUS_ROOT/$t/" 2>/dev/null || true
        touch "$CORPUS_ROOT/$t/.gitkeep"
        rm -rf "$tmp"
        echo "  $t -> $(ls "$CORPUS_ROOT/$t" | wc -l | tr -d ' ') files"
    done
    exit 0
fi

TARGET="${1:-}"
SECONDS_PER="${2:-60}"

if [[ ! -d "$BUILD_DIR" ]]; then
    configure
fi
cmake --build "$BUILD_DIR" --parallel "${BUILD_JOBS:-10}" >/dev/null

if [[ -n "$TARGET" ]]; then
    RUN_LIST="$TARGET"
else
    RUN_LIST="$(targets)"
fi

status=0
for t in $RUN_LIST; do
    bin="$BUILD_DIR/fuzz/clink_fuzz_$t"
    if [[ ! -x "$bin" ]]; then
        echo "▶ no such target: $t (have: $(targets | tr '\n' ' '))" >&2
        exit 1
    fi
    mkdir -p "$CORPUS_ROOT/$t"
    echo "▶ fuzzing $t for ${SECONDS_PER}s"
    # -artifact_prefix puts a reproducer straight into the corpus directory,
    # so the only step left is `git add`.
    if ! "$bin" \
            -max_total_time="$SECONDS_PER" \
            -print_final_stats=1 \
            -artifact_prefix="$CORPUS_ROOT/$t/" \
            "$CORPUS_ROOT/$t" 2>&1 | grep -E 'DONE|ERROR|SUMMARY|Done .* runs'; then
        status=1
    fi
done

if [[ $status -ne 0 ]]; then
    echo
    echo "▶ a target reported a finding. The reproducer is in $CORPUS_ROOT/<target>/;"
    echo "  git add it so the corpus replay keeps it forever, then fix the defect."
fi
exit $status
