#!/usr/bin/env bash
# Destroy every cloud resource a qualification run provisioned, by
# label. Orphaned infrastructure is a failed test: every resource a
# campaign creates MUST carry the label qual-run=<run-id> (and qual=1),
# and this script is the single teardown path. Local evidence under
# qualification-results/ is deliberately NOT deleted - evidence is
# retained; pass --local-evidence to remove it too.
#
# Usage:
#   scripts/qualification/destroy.sh <run-id> [--yes] [--local-evidence]
#
# Without --yes the script only LISTS what it would delete.
set -euo pipefail

RUN_ID="${1:-}"
[[ -n "$RUN_ID" ]] || { echo "destroy.sh: usage: destroy.sh <run-id> [--yes] [--local-evidence]" >&2; exit 2; }
shift
YES=0
LOCAL_EVIDENCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --yes) YES=1; shift ;;
        --local-evidence) LOCAL_EVIDENCE=1; shift ;;
        *) echo "destroy.sh: unknown argument: $1" >&2; exit 2 ;;
    esac
done

SELECTOR="qual-run=$RUN_ID"

if command -v hcloud >/dev/null 2>&1; then
    # Deletion order matters: servers first (they hold volume
    # attachments and network memberships), then volumes, then the
    # network-level resources.
    for kind in server volume load-balancer firewall network ssh-key; do
        # `hcloud <kind> list -l <selector>` prints a header line even
        # when empty; -o noheader keeps the loop clean.
        while IFS= read -r id; do
            [[ -n "$id" ]] || continue
            if [[ "$YES" -eq 1 ]]; then
                echo "destroy.sh: deleting $kind $id ($SELECTOR)"
                hcloud "$kind" delete "$id"
            else
                echo "destroy.sh: WOULD delete $kind $id ($SELECTOR) - pass --yes to do it"
            fi
        done < <(hcloud "$kind" list -l "$SELECTOR" -o noheader -o columns=id 2>/dev/null || true)
    done
else
    echo "destroy.sh: hcloud CLI not found; no cloud resources to sweep on this machine" >&2
fi

if [[ "$LOCAL_EVIDENCE" -eq 1 ]]; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
    TARGET="$REPO_ROOT/qualification-results/$RUN_ID"
    if [[ -d "$TARGET" ]]; then
        if [[ "$YES" -eq 1 ]]; then
            echo "destroy.sh: removing local evidence $TARGET"
            rm -rf "$TARGET"
        else
            echo "destroy.sh: WOULD remove local evidence $TARGET - pass --yes to do it"
        fi
    fi
fi

echo "destroy.sh: done ($SELECTOR)"
