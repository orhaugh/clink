#!/usr/bin/env bash
# Teardown for qualification rigs. With a run id, delegates to the
# label-driven destroy script; with --check, asserts NOTHING labelled
# qual=1 survives anywhere in the project - the standing end-of-session
# fence, same discipline as the benchmark rig's teardown.
#
#   ./teardown.sh <run-id>      delete that run's resources (asks destroy.sh --yes)
#   ./teardown.sh --check       fail loudly if any qual-labelled resource exists
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

if [ "${1:-}" = "--check" ]; then
    leftover=0
    for kind in server volume load-balancer firewall network; do
        while IFS= read -r line; do
            [ -n "$line" ] || continue
            echo "LEFTOVER $kind: $line" >&2
            leftover=1
        done < <(hcloud "$kind" list -l qual=1 -o noheader -o columns=id,name 2>/dev/null || true)
    done
    if [ "$leftover" -eq 1 ]; then
        echo "teardown --check: QUAL RESOURCES SURVIVE - they bill until destroyed." >&2
        exit 1
    fi
    echo "teardown --check: clean."
    exit 0
fi

RUN_ID="${1:?usage: teardown.sh <run-id> | --check}"
"$REPO_ROOT/scripts/qualification/destroy.sh" "$RUN_ID" --yes
