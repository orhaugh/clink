#!/usr/bin/env bash
# The destination-reconciliation example and its qualification run: a Kafka
# topic, a record-wise SQL job on a local Coordinator and two Workers from the
# build, and ClickHouse, with every scenario README.md lists reconciled by
# `clink incident-reconcile` and checked by an independent reference that
# reads the source back from Kafka.
#
#   examples/destination-reconciliation/run.sh                 # every scenario
#   ONLY=healthy,corruption examples/destination-reconciliation/run.sh
#
# Needs Docker, Python 3, and clink_node and clink built with tests in
# CLINK_BIN_DIR (default ../../build): the scenarios drive fault points, which
# only a test build compiles in. Kafka and ClickHouse run from the images
# docker-compose.yml pins, on 127.0.0.1 only, under a compose project of their
# own that is removed, volumes included, when the run ends (KEEP=1 keeps it).
#
# Everything the run produces is under WORK (default a new temporary
# directory), which is the qualification bundle: results/ holds the summary,
# every check's verdict (scenarios.json), the commands run (commands.json), the
# versions, the reconciliation reports, the expected rows each one exported,
# the independent check's results and the measurements; bundles/ the incident
# bundles; <scenario>/ each cluster's checkpoints, captures, job and logs.
#
# Exits 0 only when every check of every scenario that ran passed, and says
# QUALIFIED only when every scenario ran; 1 otherwise.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
BIN="$(cd "${CLINK_BIN_DIR:-${REPO}/build}" && pwd)"
WORK="${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/clink-reconcile-demo.XXXXXX")}"
mkdir -p "${WORK}/results"
for b in clink_node clink; do
    [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
"${BIN}/clink" --capabilities 2>/dev/null | grep -q "fault injection compiled in *yes" || {
    echo "run.sh: ${BIN}/clink has no fault injection compiled in (build with tests)" >&2; exit 1; }
command -v docker >/dev/null || { echo "run.sh: Docker is needed for Kafka and ClickHouse" >&2; exit 1; }
export PYTHONDONTWRITEBYTECODE=1

{
    echo "commit: $(git -C "${REPO}" rev-parse HEAD)$(git -C "${REPO}" diff --quiet HEAD -- || echo ' (with uncommitted changes)')"
    echo "host: $(uname -srm)"
    echo "binaries: ${BIN}"
    "${BIN}/clink" --capabilities 2>/dev/null | sed -n '1,12p'
} >"${WORK}/results/build.txt"
cp "${HERE}/docker-compose.yml" "${HERE}/job.sql.in" "${WORK}/results/"

args=(--bin-dir="${BIN}" --work="${WORK}")
[ -n "${ONLY:-}" ] && args+=(--only="${ONLY}")
[ "${KEEP:-0}" = 1 ] && args+=(--keep)
set +e
python3 "${HERE}/reconcile_demo.py" "${args[@]}"
code=$?
set -e
if [ "${code}" = 0 ] && [ -z "${ONLY:-}" ]; then
    echo "QUALIFIED: every scenario ran and every check passed. Results in ${WORK}" | tee -a "${WORK}/results/summary.txt"
elif [ "${code}" = 0 ]; then
    echo "every check of the scenarios run passed (ONLY=${ONLY}), which is not a qualification. Results in ${WORK}" | tee -a "${WORK}/results/summary.txt"
else
    echo "NOT QUALIFIED: a check failed (results/scenarios.json). Results in ${WORK}" | tee -a "${WORK}/results/summary.txt" >&2
fi
exit "${code}"
