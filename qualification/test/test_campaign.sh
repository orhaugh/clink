#!/usr/bin/env bash
# Functional verification of the campaign driver, with no cloud and no cluster.
#
# Every defect this driver has had cost a provision-build-run cycle on paid
# hardware to find, one per cycle, because a campaign is a serial pipeline:
# you only reach step N+1 once step N works. The engine it qualifies has two
# thousand tests. The driver had none.
#
# This runs the real campaign.sh against a simulated rig (see rigsim.sh) by
# putting fake ssh, scp, curl and hcloud ahead of the real ones on PATH. The
# scenarios are the failures that actually happened, so a regression in the
# gate shows up here rather than at forty minutes and eight instances a go.
#
#   ./test_campaign.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
PASS=0; FAIL=0

report() {  # name, ok, detail
    if [ "$2" = "1" ]; then PASS=$((PASS+1)); echo "  PASS $1"
    else FAIL=$((FAIL+1)); echo "  FAIL $1${3:+ - $3}"; fi
}

# Run campaign.sh against a fresh simulated rig.
#   run_campaign <workdir> <scenario lines...>
run_campaign() {
    local work="$1"; shift
    export FAKERIG="$work/rig"
    mkdir -p "$FAKERIG"
    cp "$HERE/rigsim.sh" "$work/rigsim.sh"
    : > "$FAKERIG/scenario"
    for line in "$@"; do echo "$line" >> "$FAKERIG/scenario"; done

    python3 - "$FAKERIG/servers.json" <<'PY'
import json, sys
roles = [("ops", 1), ("coordinator", 2), ("worker", 3), ("worker", 4),
         ("worker", 5), ("broker", 6), ("broker", 7), ("broker", 8)]
out, seen = [], {}
for role, n in roles:
    seen[role] = seen.get(role, 0) + 1
    suffix = role if seen[role] == 1 or role in ("ops", "coordinator") else f"{role}{seen[role]}"
    out.append({"name": f"qual-test-{suffix}",
                "public_net": {"ipv4": {"ip": f"10.0.0.{n}"}},
                "private_net": [{"ip": f"10.20.1.{n}"}]})
json.dump(out, open(sys.argv[1], "w"))
PY

    # Fakes first, and a stub build/ so the driver finds clink_submit_sql.
    mkdir -p "$work/build"
    cp "$HERE/fakebin/clink_submit_sql" "$work/build/clink_submit_sql"
    PATH="$HERE/fakebin:$PATH" \
    RUN_ID="camptest" SKIP_PROVISION=1 DURATION_H=0 \
    DURATION_S="${SIM_DURATION_S:-0}" WATCH_MAX_LOOPS="${SIM_WATCH_LOOPS:-0}" \
    JOB_PROBE_INTERVAL_S=0 \
    CLINK_IMAGE="clink:test" SSH_KEY_FILE="$work/key" \
    SUBMIT_BIN="$work/build/clink_submit_sql" \
    CHECKPOINT_INTERVAL_MS=10000 \
        bash "$REPO_ROOT/qualification/qual01/campaign.sh" > "$work/out.log" 2>&1
    echo $? > "$work/rc"
}

# DURATION_H=0 makes the soak loop exit immediately, so a run reaches the
# gate, then drains and summarises - the whole driver, in seconds.
scenario() {  # name, workdir-suffix, expect_rc, expect_grep, scenario lines...
    local name="$1" sfx="$2" want_rc="$3" want="$4"; shift 4
    local work; work="$(mktemp -d)/$sfx"; mkdir -p "$work"
    touch "$work/key"
    run_campaign "$work" "$@"
    local rc; rc=$(cat "$work/rc")
    local ok=1 detail=""
    if [ -n "$want_rc" ] && [ "$rc" != "$want_rc" ]; then
        ok=0; detail="rc=$rc want=$want_rc"
    fi
    if [ -n "$want" ] && ! grep -q "$want" "$work/out.log"; then
        ok=0; detail="$detail; missing '$want'"
        tail -4 "$work/out.log" | sed 's/^/      /'
    fi
    report "$name" "$ok" "$detail"
    LAST_WORK="$work"
}

echo "campaign driver functional tests (no cloud, no cluster)"

# 1. The happy path must reach the gate and pass it.
scenario "a healthy rig passes the verification gate" happy "" \
    "VERIFICATION PASSED" \
    "job_status=RUNNING" "workers_lost=1"

# 2. The fabricated-fault defect: a recorded fault is not a fault. If the
#    engine never noticed a worker loss, the gate must refuse to soak.
scenario "a fault the engine never saw fails the gate" nofault 3 \
    "coordinator has lost no worker" \
    "job_status=RUNNING" "workers_lost=0"

# 3. Unreachable metrics must not be read as zero - that was the same silent
#    assumption in a different coat.
scenario "unreadable metrics fail the gate rather than defaulting" nometrics 3 \
    "cannot read the coordinator's metrics" \
    "job_status=RUNNING" "metrics_unreachable=1"

# 4. A campaign whose chaos controller applied nothing proves nothing.
scenario "no faults at all fails the gate" quiet 3 \
    "applied no fault" \
    "job_status=RUNNING" "workers_lost=1" "no_faults=1"

# 4b. A second active job means something else is feeding this run's topics
#     (run e: the previous run's job resurrected from a stale HA store, every
#     window committed twice). The gate must refuse before the soak spends.
scenario "a second job on the coordinator fails the gate" zombiejob 3 \
    "expected exactly one job" \
    "job_status=RUNNING" "workers_lost=1" "extra_job=1"

# 5. A job that never comes back after the first fault must fail the gate.
# The job must be RUNNING when the gate first looks and gone only after the
# fault - a job already dead at step 2 fails a different check, which is what
# the first version of this scenario actually exercised.
scenario "a job that does not recover from the fault fails the gate" norecover 3 \
    "did not recover" \
    "job_status=RUNNING" "workers_lost=1" "job_dies_after=1"

# 6. The job-gone latch needs SIX consecutive non-RUNNING probes. A probe
#    landing in a legitimate coordinator-fault window - three status polls
#    fail, then recovery answers - must NOT latch: a single-shot probe used
#    to, and the latch poisons the summary's read of every later window.
#    (Polls 1 and 2 are the verification gate's own status reads; the watch
#    probe starts at poll 3.)
SIM_DURATION_S=3600 SIM_WATCH_LOOPS=1 \
scenario "a transient status outage does not latch job-gone" flapgone "" \
    "VERIFICATION PASSED" \
    "job_status=RUNNING" "workers_lost=1" \
    "job_gone_from_poll=3" "job_gone_until_poll=5"
if grep -q "no longer RUNNING" "$LAST_WORK/out.log"; then
    report "a transient outage leaves the latch alone" 0 "latched on a 3-poll outage"
else
    report "a transient outage leaves the latch alone" 1 ""
fi

# 7. A job that STAYS gone must still latch - the latch exists so the
#    summary can separate "no engine" from "the engine lost data".
SIM_DURATION_S=3600 SIM_WATCH_LOOPS=1 \
scenario "a persistently gone job latches job-gone" staygone "" \
    "no longer RUNNING" \
    "job_status=RUNNING" "workers_lost=1" \
    "job_gone_from_poll=3" "job_gone_until_poll=999"

# 8. The finish phase must WAIT for the verifier's final verdict before its
#    kill sweep. The real verifier's stop path evaluates every pending
#    window before writing final=true - minutes at multi-hour scale - and
#    sweeping after a fixed 20s killed it mid-finalise on qual01-20260818c,
#    turning a 751/751-clean campaign INCONCLUSIVE. The fake verifier here
#    refuses to finalise for three polls; the campaign must keep waiting.
#    Since qual01-20260818e this scenario also pins the DELIVERY: the sim's
#    pkill -INT is inert (matching the real spawn discipline, which starts
#    both processes with SIGINT ignored), so the fake verifier finalises
#    only if the campaign touched /qual/verdict.json.stop. A campaign that
#    still relies on the signal fails here.
scenario "the finish waits for the verifier's final verdict" finalwait "" \
    "verifier finalised after 30s" \
    "job_status=RUNNING" "workers_lost=1" \
    "verifier_final_after_polls=3"

# 9. A verifier whose finalisation genuinely wedges must not hold the
#    finish phase for the full hard cap: the progress-aware wait gives up
#    after FINAL_STALL_S without movement, says so, and the summary reads
#    the run as incomplete - loudly INCONCLUSIVE, never a silent hang.
FINAL_WAIT_S=90 FINAL_STALL_S=30 \
scenario "a wedged finalisation is a loud stall, not a hang" finalstall "" \
    "made no finalisation progress" \
    "job_status=RUNNING" "workers_lost=1" \
    "verifier_never_finalises=1"
if grep -q "did not finalise" "$LAST_WORK/out.log"; then
    report "the stalled run is still declared incomplete" 1 ""
else
    report "the stalled run is still declared incomplete" 0 "missing the incomplete warning"
fi

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
