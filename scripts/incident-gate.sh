#!/usr/bin/env bash
# The incident-verification workflow's gate (design record 014): the bundle,
# collection and replay suites, each required to have run in full with nothing
# skipped, then examples/incident-verification/run.sh end to end - a Worker
# killed at an asserted point, the incident collected, moved, and verified with
# the network denied, against an independent oracle.
#
#   scripts/incident-gate.sh [build-dir]
#
# The build needs its tests (which compile fault injection in) and the SQL
# frontend. Everything runs on 127.0.0.1. Exits 0 only when every suite and
# every step of the example passed with every verification isolated from the
# network, proven on this machine (scripts/network-isolation.sh); the
# example's results/ directory is kept and named at the end. A partial result
# from the example (exit 3: a check it needs was unavailable, so steps did not
# run) fails the gate. So does a machine that cannot prove isolation: the gate
# takes no development override, and exits 2 when one is set.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"

for override in ALLOW_UNISOLATED CLINK_ISOLATION CLINK_ISOLATION_COMMAND CLINK_ISOLATION_OS \
    CLINK_ISOLATION_PROBE_URL; do
    if [ -n "${!override:-}" ]; then
        echo "incident gate: ${override} is set; the gate qualifies only under the platform's" \
            "own network isolation, proven here, and takes no override" >&2
        exit 2
    fi
done
. "${here}/scripts/network-isolation.sh"
echo "==> Network isolation for the offline verification"
if ! isolation_establish; then
    echo "incident gate: FAILED: network isolation is ${ISOLATION_STATE}: ${ISOLATION_DETAIL}" >&2
    exit 1
fi
echo "    ${ISOLATION_DETAIL}"

build="$(cd "${1:-${here}/build}" && pwd)"
out="$(mktemp -d "${TMPDIR:-/tmp}/clink-incident-gate.XXXXXX")"

echo "==> The bundle and collection suite"
set +e
"${build}/tests/clink_core_tests" \
    --gtest_filter='IncidentBundleTest.*:IncidentEpochs.*:IncidentSelection.*:ResolutionFixture.TheRestorePointOnDisk*' \
    --gtest_output="json:${out}/core.json" >"${out}/core.log" 2>&1
echo "==> The replay suite"
"${build}/tests/clink_sql_tests" --gtest_filter='IncidentReplayTest.*' \
    --gtest_output="json:${out}/sql.json" >"${out}/sql.log" 2>&1
set -e
python3 "${here}/scripts/check-gtest-reports.py" \
    --expect 'IncidentBundleTest.*=35' --expect 'IncidentEpochs.*=1' \
    --expect 'IncidentSelection.*=3' --expect 'ResolutionFixture.TheRestorePointOnDisk*=1' \
    --expect 'IncidentReplayTest.*=20' \
    "${out}/core.json" "${out}/sql.json"

echo "==> The example, end to end"
set +e
WORK="${out}/example" CLINK_BIN_DIR="${build}" "${here}/examples/incident-verification/run.sh"
code=$?
set -e
if [ "${code}" != 0 ]; then
    echo "incident gate: FAILED: the example exited ${code}$([ "${code}" = 3 ] && echo \
        ': its steps passed, but it is not a qualification (its summary says why)')" >&2
    exit 1
fi
echo "incident gate: passed (reports and the example's results under ${out})"
