#!/usr/bin/env bash
# Verify the qualification harness before spending anything on a rig.
#
# The engine these campaigns qualify has two thousand tests. The harness had
# none, so every one of its defects - faults that never landed, an oracle that
# could not judge, a fault generator that died unnoticed, a leftover process
# that hijacked the next run's topic, a coordinator kill that decapitated the
# cluster - was found by paying for a provision, an image build and an hour of
# eight instances, one defect per cycle.
#
# These run in seconds, on this machine, against simulated infrastructure.
# Run them before any campaign.
#
#   ./run-all.sh            # everything that needs nothing external
#   ./run-all.sh --with-docker   # also the oracle tests that need a container
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
FAILED=0

run() {  # name, command...
    echo
    echo "=== $1"
    shift
    if "$@"; then echo "--- ok"; else echo "--- FAILED"; FAILED=$((FAILED+1)); fi
}

run "chaos controller (fault application, blast radius, failure handling)" \
    python3 "$HERE/../chaos/test_chaos.py"

run "campaign driver (the verification gate, end to end, no cloud)" \
    bash "$HERE/test_campaign.sh"

# Interface drift between a campaign and the shared chaos controller dies
# at argparse ~25 paid minutes into a rig (QUAL-02 carried two renamed
# flags). Checked statically for every campaign at once.
run "chaos interface (every campaign's invocation parses)" \
    python3 "$HERE/test_chaos_interface.py"

run "QUAL-02 driver parses" \
    bash -n "$HERE/../qual02/campaign.sh"

run "QUAL-02 summariser result logic (PASS is earned, never assumed)" \
    python3 "$HERE/test_summarise2.py"

run "QUAL-03 driver parses" \
    bash -n "$HERE/../qual03/campaign.sh"

run "QUAL-03 summariser result logic (PASS is earned, never assumed)" \
    python3 "$HERE/test_summarise3.py"

# QUAL-03's oracle test is container-free (the store is a duck-typed
# fake), so unlike QUAL-02's it runs on every invocation - including the
# paginated-listing race and MinIO's prefix-blind upload listing, which
# no healthy live run would ever exercise on purpose.
run "QUAL-03 oracle (must name every injected defect)" \
    python3 "$HERE/../qual03/test_oracle.py"

run "QUAL-04 driver parses" \
    bash -n "$HERE/../qual04/campaign.sh"

run "QUAL-04 summariser result logic (a small run must never pass as a large one)" \
    python3 "$HERE/test_summarise4.py"

if [ "${1:-}" = "--with-docker" ]; then
    # The QUAL-02 oracle is the one component that was tested offline from the
    # start, and it found a real defect in itself on the first attempt - a
    # malformed identifier made its judging query throw, which the live
    # verifier would have swallowed as a database blip and retried forever
    # while writing a verdict with no findings. That is the standard the rest
    # of this directory is trying to reach.
    run "QUAL-02 oracle (must name every injected defect)" \
        python3 "$HERE/../qual02/test_oracle.py"
fi

echo
if [ "$FAILED" -eq 0 ]; then
    echo "harness verified: safe to spend money on a rig"
else
    echo "$FAILED suite(s) FAILED - fix the harness before provisioning anything"
fi
exit "$FAILED"
