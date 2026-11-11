#!/usr/bin/env bash
# The audit-runner example and its qualification run: the destination-
# reconciliation example's record-wise job (Kafka -> clink -> ClickHouse) on a
# local Coordinator and two Workers from the build, kept producing while
# `clink-audit` (python/clink-audit) audits its completed checkpoints, with
# every scenario README.md lists: a steady run checked against an independent
# read of the source and the destination, a transformation defect,
# destination-only corruption, a runner killed and started again, a
# destination outage, bounds that cannot be met, latest-only selection,
# checkpoints purged before collection, expired source history, and the cost.
#
#   examples/audit-runner/run.sh                    # every scenario
#   ONLY=runner-steady examples/audit-runner/run.sh
#
# Needs Docker, Python 3.10+ (and pip, to install python/clink-audit,
# python/clink-reference and its Kafka client into a virtual environment under
# WORK), and clink_node and clink built in CLINK_BIN_DIR (default ../../build).
# Offline verification runs under the platform's network isolation, which the
# runner proves first (sandbox-exec on macOS, unshare -rn on Linux): where it
# cannot run, every audit is partial and the run is not a qualification. Kafka
# and ClickHouse run from the images ../destination-reconciliation/docker-compose.yml
# pins, on 127.0.0.1 only, under a compose project of their own that is
# removed, volumes included, when the run ends (KEEP=1 keeps it).
#
# Everything the run produces is under WORK (default a new temporary
# directory), which is the qualification bundle: results/ holds the summary,
# every check's verdict (scenarios.json), the commands run (commands.json), the
# versions, the independent checks and the measurements; <scenario>/ each
# cluster's checkpoints, captures, job, logs, and the runner's configuration
# and state directory (its reports, status, events and retained evidence).
#
# Exits 0 only when every check of every scenario that ran passed, and says
# QUALIFIED only when every scenario ran; 1 otherwise.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
BIN="$(cd "${CLINK_BIN_DIR:-${REPO}/build}" && pwd)"
WORK="${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/clink-audit-runner.XXXXXX")}"
mkdir -p "${WORK}/results"
for b in clink_node clink; do
    [ -x "${BIN}/${b}" ] || { echo "run.sh: ${BIN}/${b} is not built" >&2; exit 1; }
done
command -v docker >/dev/null || { echo "run.sh: Docker is needed for Kafka and ClickHouse" >&2; exit 1; }
export PYTHONDONTWRITEBYTECODE=1

{
    echo "commit: $(git -C "${REPO}" rev-parse HEAD)$(git -C "${REPO}" diff --quiet HEAD -- || echo ' (with uncommitted changes)')"
    echo "host: $(uname -srm)"
    echo "binaries: ${BIN}"
    "${BIN}/clink" --capabilities 2>/dev/null | sed -n '1,12p'
} >"${WORK}/results/build.txt"
cp "${REPO}/examples/destination-reconciliation/job.sql.in" \
    "${REPO}/examples/destination-reconciliation/docker-compose.yml" "${WORK}/results/"

# The runner, the reference's library and its Kafka client, apart from the
# system's Python.
python3 -m venv "${WORK}/venv"
"${WORK}/venv/bin/pip" install -q "${REPO}/python/clink-audit" "${REPO}/python/clink-reference" \
    "confluent-kafka==2.16.1"
"${WORK}/venv/bin/python" -c 'import clink_reference as r, clink_audit as a; print("clink-audit %s; confluent-kafka %s, librdkafka %s" % ((a.__version__,) + r.client_versions()))' \
    >>"${WORK}/results/build.txt"

args=(--bin-dir="${BIN}" --work="${WORK}" --reference-python="${WORK}/venv/bin/python"
      --audit-python="${WORK}/venv/bin/python")
[ -n "${ONLY:-}" ] && args+=(--only="${ONLY}")
[ "${KEEP:-0}" = 1 ] && args+=(--keep)
set +e
python3 "${HERE}/audit_demo.py" "${args[@]}"
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
