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

run "QUAL-05 driver parses" \
    bash -n "$HERE/../qual05/campaign.sh"

# The two shapes this campaign turns on: a state curve still climbing must
# never pass, and neither must a flat one whose CONTROL arm did not grow -
# a workload that never accumulated anything is also flat.
run "QUAL-05 summariser result logic (a climbing curve, or an ungrown control, must never pass)" \
    python3 "$HERE/test_summarise5.py"

# The generator spec is shared by every campaign, so QUAL-05's key-epoch
# mode has to be additive: a byte-for-byte identical default path, or
# QUAL-01 to QUAL-04's oracles silently change what they expect.
run "QUAL-05 detspec key epochs (default path must be byte-identical)" \
    python3 "$HERE/test_detspec_epochs.py"

run "QUAL-05 state instrument (must refuse rather than report zero)" \
    python3 "$HERE/test_ckptsize.py"

# Container-free: psycopg2 is stubbed, so the oracle is judged on every
# invocation rather than only under --with-docker.
run "QUAL-05 oracle (must name every injected defect)" \
    python3 "$HERE/../qual05/test_oracle.py"

# The QUAL-06 width claim is by construction, so the construction gets the
# gate: generated SQL must compile to exactly the operator count the
# campaign quotes, with one consumer group per branch (the shared-group
# trap loses most of the stream by construction). Needs the host build,
# same as every campaign's SUBMIT_BIN prerequisite.
run "QUAL-06 DAG generator (width against the real planner)" \
    python3 "$HERE/test_dag_gen.py"

run "QUAL-06 driver parses" \
    bash -n "$HERE/../qual06/campaign.sh"

# The scale-campaign essence: a rung that fails to deploy is a measured
# boundary and must not fail the campaign; a deployed width disagreeing
# with the generator's arithmetic must.
run "QUAL-06 summariser result logic (a boundary is not a failure; a wrong width is)" \
    python3 "$HERE/test_summarise6.py"

run "QUAL-08 driver parses" \
    bash -n "$HERE/../qual08/campaign.sh"

# The infra faults are the first whose leftovers poison everything after
# them: these pin the revert registry (drained on any exit), the loud
# skip list, and the per-fault engagement evidence.
run "QUAL-09 chaos controller (infra faults, reverts, skips, engagement)" \
    python3 "$HERE/test_chaos_infra.py"

run "QUAL-09 driver parses" \
    bash -n "$HERE/../qual09/campaign.sh"

# The infra-campaign essence: coverage credit requires engagement, the
# environment split is loud, a failed revert is a dirty rig.
run "QUAL-09 summariser result logic (engagement gates; the environment split)" \
    python3 "$HERE/test_summarise9.py"

# QUAL-07's comparator is its false-pass surface: every normalisation
# rule needs a negative case or a mutation passes everything silently.
run "QUAL-07 comparator (each rule maps only true equivalents)" \
    python3 "$HERE/test_semantic_normaliser.py"

run "QUAL-07 runner parses" \
    bash -n "$HERE/../../benchmarks/semantic_compare/run.sh"

# The semantic-campaign essence: a divergence is the campaign working
# (FAIL, named); anything short of full gated coverage - not-gated,
# silent absence, undeclared verdict, declaration drift - is
# INCONCLUSIVE, never PASS.
run "QUAL-07 summariser result logic (coverage and drift gates)" \
    python3 "$HERE/test_summarise7.py"

# The schema-evolution campaign's essence: a refused check does not
# deploy (INCONCLUSIVE), a check that ACCEPTS the deliberately broken
# job is a FAIL (the instrument is inert), state that resets across the
# boundary is loss, and no keys across the boundary is no evidence.
run "QUAL-11 driver parses" \
    bash -n "$HERE/../qual11/campaign.sh"

# Gate 4 reads the migrated STATE rather than the output stream, because
# the stream-side signal can be destroyed by an unrelated fault (an
# at-least-once sink's buffer discarded by a worker kill). Every case is
# a byte-level statement about what the migration wrote.
run "QUAL-11 migration-effect gate (read from the savepoints)" \
    python3 "$HERE/test_migration_effect.py"

run "QUAL-11 summariser result logic (the four evolution gates)" \
    python3 "$HERE/test_summarise11.py"

# The upgrade-campaign essence: every failure of the savepoint -> swap ->
# restore sequence is a FAIL of the campaign's subject, never an
# infrastructure inconclusive - and a same-image run must be labelled a
# smoke so it can never be mistaken for an upgrade claim.
run "QUAL-08 summariser result logic (the upgrade sequence is the subject)" \
    python3 "$HERE/test_summarise8.py"

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
