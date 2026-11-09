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
# every step of the example passed; the example's results/ directory is kept
# and named at the end.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
build="$(cd "${1:-${here}/build}" && pwd)"
out="$(mktemp -d "${TMPDIR:-/tmp}/clink-incident-gate.XXXXXX")"

echo "==> The bundle and collection suite"
set +e
"${build}/tests/clink_core_tests" \
    --gtest_filter='IncidentBundleTest.*:IncidentEpochs.*:ResolutionFixture.TheRestorePointOnDisk*' \
    --gtest_output="json:${out}/core.json" >"${out}/core.log" 2>&1
echo "==> The replay suite"
"${build}/tests/clink_sql_tests" --gtest_filter='IncidentReplayTest.*' \
    --gtest_output="json:${out}/sql.json" >"${out}/sql.log" 2>&1
set -e
python3 "${here}/scripts/check-gtest-reports.py" \
    --expect 'IncidentBundleTest.*=27' --expect 'IncidentEpochs.*=1' \
    --expect 'ResolutionFixture.TheRestorePointOnDisk*=1' --expect 'IncidentReplayTest.*=9' \
    "${out}/core.json" "${out}/sql.json"

echo "==> The example, end to end"
WORK="${out}/example" CLINK_BIN_DIR="${build}" "${here}/examples/incident-verification/run.sh"
echo "incident gate: passed (reports and the example's results under ${out})"
