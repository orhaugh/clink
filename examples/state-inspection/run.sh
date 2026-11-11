#!/usr/bin/env bash
# The state-inspection example and its qualification run: the example job
# (accounts_job.cpp) at parallelism 2 on a local Coordinator and Workers from
# the build, against Kafka on compose, with every scenario README.md lists:
# completed checkpoints exported as datasets with `clink state-export
# --checkpoint-dir`, read with pyarrow, DuckDB and `clink state-query`,
# compared with `clink state-diff`, and checked against the state this
# example computes on its own from the records read back from Kafka.
#
#   examples/state-inspection/run.sh                       # every scenario
#   ONLY=inspect-running examples/state-inspection/run.sh
#
# Needs Docker, Python 3.10+ (and pip, for pyarrow and duckdb in a virtual
# environment under WORK), and clink, clink_node, clink_submit_sql and the
# example job module (examples/state_inspection_job.so) built in CLINK_BIN_DIR
# (default ../../build). Kafka runs from the image
# ../destination-reconciliation/docker-compose.yml pins, on 127.0.0.1 only,
# under a compose project of its own that is removed when the run ends
# (KEEP=1 keeps it). The offline commands run under the platform's network
# isolation, which the run proves first.
#
# Everything the run produces is under WORK (default a new temporary
# directory), which is the qualification bundle: results/ holds the summary,
# every check's verdict (scenarios.json), the commands run (commands.json),
# the versions, the independent checks, the diffs and queries, and the costs;
# <scenario>/ each cluster's checkpoints, logs and datasets.
#
# Exits 0 only when every check of every scenario that ran passed, and says
# QUALIFIED only when every scenario ran; 1 otherwise.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
BIN="$(cd "${CLINK_BIN_DIR:-${REPO}/build}" && pwd)"
WORK="${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/clink-state-inspection.XXXXXX")}"
mkdir -p "${WORK}/results"
for b in clink_node clink clink_submit_sql examples/state_inspection_job.so; do
    [ -e "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
command -v docker >/dev/null || { echo "run.sh: Docker is needed for Kafka" >&2; exit 1; }
export PYTHONDONTWRITEBYTECODE=1

{
    echo "commit: $(git -C "${REPO}" rev-parse HEAD)$(git -C "${REPO}" diff --quiet HEAD -- || echo ' (with uncommitted changes)')"
    echo "host: $(uname -srm)"
    echo "binaries: ${BIN}"
    "${BIN}/clink" --capabilities 2>/dev/null | sed -n '1,12p'
} >"${WORK}/results/build.txt"
cp "${HERE}/accounts_job.cpp" "${REPO}/examples/destination-reconciliation/docker-compose.yml" "${WORK}/results/"

# The two readers outside clink, apart from the system's Python.
python3 -m venv "${WORK}/venv"
"${WORK}/venv/bin/pip" install -q "pyarrow==${PYARROW_VERSION:-22.0.0}" "duckdb==${DUCKDB_VERSION:-1.4.1}"

args=(--bin-dir="${BIN}" --work="${WORK}")
[ -n "${ONLY:-}" ] && args+=(--only="${ONLY}")
[ "${KEEP:-0}" = 1 ] && args+=(--keep)
set +e
"${WORK}/venv/bin/python" "${HERE}/inspect_demo.py" "${args[@]}"
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
