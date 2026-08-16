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

# 5. A job that never comes back after the first fault must fail the gate.
# The job must be RUNNING when the gate first looks and gone only after the
# fault - a job already dead at step 2 fails a different check, which is what
# the first version of this scenario actually exercised.
scenario "a job that does not recover from the fault fails the gate" norecover 3 \
    "did not recover" \
    "job_status=RUNNING" "workers_lost=1" "job_dies_after=1"

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
