#!/usr/bin/env bash
# The upgrade validation workflow's qualification: every scenario in
# upgrade_scenarios.py, against this build and the pinned prior release.
#
#   examples/upgrade-check/run.sh                      every scenario
#   QUICK=1 examples/upgrade-check/run.sh              the bounded subset CI runs
#   ONLY=control,schema examples/upgrade-check/run.sh  named scenarios
#
# CLINK_BIN_DIR (default build/) must hold clink, clink_node and
# examples/upgrade_check_job*.so. The prior release is v0.15.0 at the commit
# pinned below, built from this repository's own tag (never downloaded) into
# CLINK_UC_PRIOR_DIR (default build/upgrade-baselines/v0.15.0) when absent; the
# tag must resolve to the pinned commit or nothing is built.
# WORK (default a new temporary directory) keeps everything: results/summary.txt,
# results/scenarios.json, results/costs.json, every report, run and log.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
bin=${CLINK_BIN_DIR:-$repo/build}
prior_tag=v0.15.0
prior_commit=bbd63094f3dbc97d5c4457ed53f205b238090537
prior=${CLINK_UC_PRIOR_DIR:-$repo/build/upgrade-baselines/$prior_tag}
export WORK=${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/clink-upgrade-check.XXXXXX")}

python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' ||
    { echo "run.sh: Python 3.10 or later is needed" >&2; exit 2; }
for f in clink clink_node examples/upgrade_check_job.so examples/upgrade_check_job_schema.so; do
    [ -e "$bin/$f" ] || { echo "run.sh: $bin/$f is missing; build the tree first" >&2; exit 2; }
done

build_prior() {
    local have
    have=$(git -C "$repo" rev-parse "$prior_tag^{commit}" 2>/dev/null || true)
    if [ "$have" != "$prior_commit" ]; then
        echo "run.sh: $prior_tag resolves to '${have:-nothing}', not the pinned $prior_commit; not building it" >&2
        return 1
    fi
    echo "==> Building the prior release $prior_tag ($prior_commit) into $prior"
    mkdir -p "$prior"
    [ -d "$prior/src/.git" ] || git clone --quiet --no-checkout "$repo" "$prior/src"
    git -C "$prior/src" checkout --quiet --detach "$prior_commit"
    cmake -S "$prior/src" -B "$prior/build" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCLINK_BUILD_TESTS=OFF -DCLINK_BUILD_EXAMPLES=OFF -DCLINK_BUILD_IMPLS=ON \
        -DCLINK_BUILD_SQL=OFF -DCMAKE_INSTALL_PREFIX="$prior/prefix" >"$prior/configure.log" 2>&1
    cmake --build "$prior/build" --parallel 8 >"$prior/build.log" 2>&1
    cmake --install "$prior/build" >"$prior/install.log" 2>&1
    cmake -S "$here/sdk" -B "$prior/job" -DCMAKE_PREFIX_PATH="$prior/prefix" \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo >"$prior/job-configure.log" 2>&1
    cmake --build "$prior/job" --parallel 4 >"$prior/job-build.log" 2>&1
}

if [ "${QUICK:-0}" != 1 ]; then
    if [ ! -x "$prior/prefix/bin/clink_node" ] || [ ! -e "$prior/job/upgrade_check_job.so" ]; then
        build_prior || true
    fi
    if [ -x "$prior/prefix/bin/clink_node" ] && [ -e "$prior/job/upgrade_check_job.so" ] &&
       [ "$(git -C "$prior/src" rev-parse HEAD 2>/dev/null)" = "$prior_commit" ]; then
        export CLINK_UC_PRIOR_DIR=$prior
    fi
fi

export CLINK_BIN_DIR=$bin
echo "==> Qualifying upgrade validation; everything is kept under $WORK"
python3 "$here/upgrade_scenarios.py"
