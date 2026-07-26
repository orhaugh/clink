#!/usr/bin/env bash
# Destroy every Hetzner resource this benchmark created. Written BEFORE anything
# is provisioned, deliberately: a teardown that only exists once the cluster is
# up is a teardown you do not have when it matters.
#
# Everything provision.sh creates carries the label purpose=clink-bench, so this
# deletes by label rather than by a remembered list of names - nothing survives
# because a name drifted or a step half-failed.
#
#   ./teardown.sh            delete everything labelled purpose=clink-bench
#   ./teardown.sh --check    list what exists, delete nothing
#
# Servers bill until destroyed. Run --check after any session, and check the
# Hetzner console too; do not take this script's word as the only evidence.
set -uo pipefail

LABEL="purpose=clink-bench"
CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

command -v hcloud >/dev/null 2>&1 || { echo "hcloud CLI not found"; exit 2; }

echo "context: $(hcloud context active 2>/dev/null || echo '(none)')"
echo

show() {
    local kind=$1
    local n
    # grep -c . not wc -l: hcloud prints a single newline for an EMPTY list, so
    # wc -l reports 1 and the script would claim a resource exists (and then fail
    # its own clean check) when nothing does.
    n=$(hcloud "$kind" list -l "$LABEL" -o noheader 2>/dev/null | grep -c . || true)
    echo "  ${kind}: ${n}"
    [ "$n" != "0" ] && hcloud "$kind" list -l "$LABEL" 2>/dev/null | sed 's/^/      /'
    return 0
}

echo "Resources labelled ${LABEL}:"
for kind in server network firewall ssh-key volume; do show "$kind"; done
echo

if [ "$CHECK_ONLY" = "1" ]; then
    echo "--check: nothing deleted."
    exit 0
fi

# Servers first: a network cannot be deleted while a server is attached to it.
delete_all() {
    local kind=$1
    local names
    names=$(hcloud "$kind" list -l "$LABEL" -o noheader -o columns=name 2>/dev/null | grep . || true)
    [ -z "$names" ] && return 0
    while IFS= read -r n; do
        [ -z "$n" ] && continue
        echo "  deleting ${kind} ${n}"
        hcloud "$kind" delete "$n" >/dev/null 2>&1 || echo "    WARNING: failed to delete ${kind} ${n}"
    done <<< "$names"
}

for kind in server network firewall ssh-key volume; do delete_all "$kind"; done

echo
echo "Remaining after teardown:"
remaining=0
for kind in server network firewall ssh-key volume; do
    n=$(hcloud "$kind" list -l "$LABEL" -o noheader 2>/dev/null | grep -c . || true)
    echo "  ${kind}: ${n}"
    [ "$n" != "0" ] && remaining=1
done

if [ "$remaining" != "0" ]; then
    echo
    echo "NOT CLEAN - something survived and is still billing. Delete it in the"
    echo "Hetzner console, or re-run this script."
    exit 1
fi
echo
echo "Clean: nothing labelled ${LABEL} remains."
