#!/usr/bin/env bash
# The upgrade validation gate: the workflow's own tests, then its example
# (examples/upgrade-check/run.sh), which records evidence with a baseline and
# checks candidates against it, every build process under proven
# loopback-only isolation.
#
#   scripts/upgrade-gate.sh           the bounded subset (what CI runs)
#   scripts/upgrade-gate.sh --full    every scenario, the pinned prior release
#                                     included (built from its tag if absent)
#
# CLINK_BIN_DIR (default build/) must hold clink, clink_node and the example's
# job modules. Exit 0 only when every expectation was met.
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
full=0
for a in "$@"; do
    case "$a" in
        --full) full=1 ;;
        *) echo "usage: scripts/upgrade-gate.sh [--full]" >&2; exit 2 ;;
    esac
done

echo "==> The workflow's tests"
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s "$repo/python/clink-audit/tests" \
    -p 'test_upgrade.py'

echo "==> The example"
if [ "$full" = 1 ]; then
    "$repo/examples/upgrade-check/run.sh"
else
    QUICK=1 "$repo/examples/upgrade-check/run.sh"
fi
