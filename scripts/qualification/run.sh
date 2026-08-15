#!/usr/bin/env bash
# Qualify a supplied git ref: build it from a clean detached worktree,
# run the selected test suite, and record full provenance - the exact
# SHA, build flags, compiler, dependency pins, connector capability
# manifest, host configuration and timestamps - into a per-run evidence
# directory. The same script must work against main, a release tag, or
# any future candidate; the production-readiness verdict always refers
# to the immutable SHA this records.
#
# Usage:
#   scripts/qualification/run.sh --ref <git-ref> [--suite smoke|sql|full]
#                                [--out <dir>] [--keep-worktree] [--jobs N]
#
# Suites:
#   smoke  build + ctest -L core                    (default)
#   sql    build + ctest -L core + the SQL frontend suite
#   full   build + the whole default ctest set
#
# Evidence layout (retained; never overwritten - the run id embeds the
# UTC timestamp and the SHA):
#   qualification-results/<run_id>/provenance.json
#   qualification-results/<run_id>/capabilities.json
#   qualification-results/<run_id>/results.json
#   qualification-results/<run_id>/logs/{configure,build,ctest}.log
set -euo pipefail

REF=""
SUITE="smoke"
OUT_ROOT=""
KEEP_WORKTREE=0
JOBS=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ref) REF="$2"; shift 2 ;;
        --suite) SUITE="$2"; shift 2 ;;
        --out) OUT_ROOT="$2"; shift 2 ;;
        --keep-worktree) KEEP_WORKTREE=1; shift ;;
        --jobs) JOBS="$2"; shift 2 ;;
        -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

[[ -n "$REF" ]] || { echo "run.sh: --ref <git-ref> is required" >&2; exit 2; }
case "$SUITE" in smoke|sql|full) ;; *) echo "run.sh: unknown suite '$SUITE'" >&2; exit 2 ;; esac

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

SHA="$(git rev-parse --verify "${REF}^{commit}")"
SHORT_SHA="${SHA:0:12}"
STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="qual-$(date -u +%Y%m%d-%H%M%S)-${SHORT_SHA}"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/qualification-results}"
OUT_DIR="$OUT_ROOT/$RUN_ID"
LOG_DIR="$OUT_DIR/logs"
WORKTREE="$OUT_DIR/worktree"
BUILD_DIR="$WORKTREE/build-qual"
mkdir -p "$LOG_DIR"

echo "run.sh: qualification run $RUN_ID"
echo "run.sh: ref '$REF' -> $SHA"

# An immutable checkout: a detached worktree at the exact SHA, so the
# working tree someone is editing can never leak into the qualified
# build. Removed at the end unless --keep-worktree.
git worktree add --detach "$WORKTREE" "$SHA" >"$LOG_DIR/worktree.log" 2>&1
cleanup() {
    if [[ "$KEEP_WORKTREE" -eq 0 ]]; then
        git worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# Configure and build failures must not abort the run before evidence
# is written - a failed qualification is still a result, and the first
# real defect this runner found (worktree builds dying in GitHooks.cmake)
# was initially invisible because set -e skipped the provenance stage.
BUILD_OK=1
echo "run.sh: configuring (log: $LOG_DIR/configure.log)"
cmake -S "$WORKTREE" -B "$BUILD_DIR" >"$LOG_DIR/configure.log" 2>&1 || BUILD_OK=0

if [[ "$BUILD_OK" -eq 1 ]]; then
    echo "run.sh: building with $JOBS jobs (log: $LOG_DIR/build.log)"
    cmake --build "$BUILD_DIR" --parallel "$JOBS" >"$LOG_DIR/build.log" 2>&1 || BUILD_OK=0
else
    echo "run.sh: CONFIGURE FAILED - see $LOG_DIR/configure.log" >&2
fi

CTEST_OK=""
CTEST_SUMMARY=""
if [[ "$BUILD_OK" -eq 1 ]]; then
    CTEST_ARGS=(--test-dir "$BUILD_DIR" -j8 --output-on-failure)
    case "$SUITE" in
        smoke) CTEST_ARGS+=(-L core) ;;
        sql)   CTEST_ARGS+=(-L '^(core|sql)$') ;;
        full)  ;;
    esac
    echo "run.sh: running suite '$SUITE' (log: $LOG_DIR/ctest.log)"
    if ctest "${CTEST_ARGS[@]}" >"$LOG_DIR/ctest.log" 2>&1; then
        CTEST_OK=true
    else
        CTEST_OK=false
    fi
    CTEST_SUMMARY="$(grep -E 'tests passed|tests failed' "$LOG_DIR/ctest.log" | tail -1 || true)"
else
    echo "run.sh: BUILD FAILED - see $LOG_DIR/build.log" >&2
    CTEST_OK=false
    CTEST_SUMMARY="build failed; suite not run"
fi

# Connector capability manifest from the binary just built - what THIS
# build can genuinely do, not what the project supports somewhere. The
# manifest is part of the evidence a verdict is scoped to, so its absence
# is reported rather than passed over: the first run wrote no manifest at
# all because the CLI is at build/clink, and nothing said so.
CAPABILITIES_OK=false
if [[ "$BUILD_OK" -eq 1 ]]; then
    for candidate in "$BUILD_DIR/clink" "$BUILD_DIR/tools/clink"; do
        if [[ -x "$candidate" ]] && "$candidate" --capabilities-json \
                >"$OUT_DIR/capabilities.json" 2>/dev/null; then
            CAPABILITIES_OK=true
            break
        fi
    done
    if [[ "$CAPABILITIES_OK" != "true" ]]; then
        rm -f "$OUT_DIR/capabilities.json"
        echo "run.sh: WARNING - no capability manifest captured (clink CLI not found or failed)" >&2
    fi
fi

FINISHED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Provenance. Every field is read off the artefacts, never asserted.
python3 - "$OUT_DIR" <<PYEOF
import json, os, platform, subprocess, sys

out_dir = sys.argv[1]
worktree = os.path.join(out_dir, "worktree")
build_dir = os.path.join(worktree, "build-qual")

def sh(*cmd, cwd=None):
    try:
        return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True).stdout.strip()
    except Exception:
        return ""

cache = {}
cache_path = os.path.join(build_dir, "CMakeCache.txt")
if os.path.exists(cache_path):
    for line in open(cache_path):
        line = line.strip()
        if "=" not in line or line.startswith(("#", "//")):
            continue
        key, value = line.split("=", 1)
        name = key.split(":", 1)[0]
        if name.startswith("CLINK_") or name in (
            "CMAKE_BUILD_TYPE", "CMAKE_CXX_COMPILER", "CMAKE_CXX_FLAGS"):
            cache[name] = value

versions = {}
versions_path = os.path.join(worktree, "scripts", "versions.env")
if os.path.exists(versions_path):
    for line in open(versions_path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            versions[key] = value

compiler = cache.get("CMAKE_CXX_COMPILER", "")
compiler_version = sh(compiler, "--version").splitlines()[0] if compiler else ""

provenance = {
    "run_id": "$RUN_ID",
    "ref": "$REF",
    "git_sha": "$SHA",
    "suite": "$SUITE",
    "started_utc": "$STARTED_UTC",
    "finished_utc": "$FINISHED_UTC",
    "host": {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "hostname": platform.node(),
    },
    "compiler": compiler_version,
    "build_flags": cache,
    "dependency_pins": versions,
    "cloud": None,
    "cluster": None,
}
json.dump(provenance, open(os.path.join(out_dir, "provenance.json"), "w"), indent=2)

results = {
    "run_id": "$RUN_ID",
    "build_ok": "$BUILD_OK" == "1",
    "ctest_ok": "$CTEST_OK" == "true",
    "ctest_summary": """$CTEST_SUMMARY""",
    "capability_manifest_captured": "$CAPABILITIES_OK" == "true",
}
json.dump(results, open(os.path.join(out_dir, "results.json"), "w"), indent=2)
PYEOF

echo "run.sh: evidence in $OUT_DIR"
echo "run.sh: $CTEST_SUMMARY"
[[ "$BUILD_OK" -eq 1 && "$CTEST_OK" == "true" ]] || exit 1
echo "run.sh: PASS at $SHA"
