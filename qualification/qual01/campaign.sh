#!/usr/bin/env bash
# QUAL-01: Kafka exactly-once campaign driver.
#
# Brings up the rig, deploys the stack, starts the generator and the
# independent verifier on the ops host, submits the pipeline, runs the
# chaos controller for the campaign duration, then collects evidence and
# writes the standard summary. Teardown is the caller's responsibility
# (scripts/qualification/destroy.sh <run-id> --yes) so a FAILED campaign
# can be inspected before its rig disappears - but the run id is printed
# on every exit path so nothing is left unfindable.
#
#   RUN_ID=qual01-20260815 DURATION_H=168 ./campaign.sh
#   RUN_ID=... SKIP_PROVISION=1 ./campaign.sh     # reuse a live rig
#   RUN_ID=... DURATION_H=2 PROFILE=aggressive ./campaign.sh   # shakedown
#
# The shakedown (DURATION_H=2) is not optional before a multi-day run: it
# proves the oracle, the chaos controller and the evidence pipeline on the
# real rig for the price of two hours, and every long campaign that
# skipped it has instead discovered its harness bugs on day three.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID (e.g. qual01-20260815)}"
DURATION_H="${DURATION_H:-168}"
PROFILE="${PROFILE:-steady}"
RATE="${RATE:-2000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-100000}"
SEED="${SEED:-20260815}"
WINDOW_MS="${WINDOW_MS:-10000}"
MAX_JITTER_MS="${MAX_JITTER_MS:-1500}"
# The watermark lag must STRICTLY exceed the generator's jitter or
# in-window records are legitimately late and the "expected" set the
# oracle computes is not the set the engine is obliged to produce. This
# is the campaign's central premise, so it is asserted, not assumed.
WM_LAG_MS="${WM_LAG_MS:-4000}"
if [ "$WM_LAG_MS" -le "$MAX_JITTER_MS" ]; then
    echo "campaign: WM_LAG_MS ($WM_LAG_MS) must exceed MAX_JITTER_MS ($MAX_JITTER_MS)" >&2
    exit 2
fi
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}"
# The worker-loss restart budget is a LIFETIME cap that is never reset,
# and its default resolves to 10. A campaign that kills a worker every
# few minutes for days would spend it in the first hour and then measure
# a stopped job, so it is set deliberately and generously here.
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
mkdir -p "$OUT_DIR"

SSH_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() { ssh "${SSH_OPTS[@]}" "root@$1" "$2"; }
to_host()  { scp "${SSH_OPTS[@]}" -q "$2" "root@$1:$3"; }

echo "campaign: QUAL-01 run $RUN_ID, ${DURATION_H}h, profile=$PROFILE"

if [ "${SKIP_PROVISION:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" "$REPO_ROOT/qualification/infra/provision.sh"
    echo "campaign: waiting for cloud-init on every host"
fi

# Inventory, read from the API by label - never hand-maintained.
hcloud server list -l "qual-run=${RUN_ID}" -o json | python3 -c "
import json, sys
servers = json.load(sys.stdin)
hosts = []
for s in servers:
    name = s['name']
    role = ('ops' if name.endswith('-ops') else
            'coordinator' if name.endswith('-coordinator') else
            'worker' if '-worker' in name else
            'broker' if '-broker' in name else 'unknown')
    hosts.append({
        'name': name,
        'role': role,
        'public_ip': s['public_net']['ipv4']['ip'],
        'private_ip': (s.get('private_net') or [{}])[0].get('ip', ''),
    })
json.dump({'run_id': '${RUN_ID}', 'hosts': sorted(hosts, key=lambda h: h['name'])},
          open('${OUT_DIR}/inventory.json', 'w'), indent=2)
print('campaign: inventory ->', '${OUT_DIR}/inventory.json')
"

read_inv() { python3 -c "
import json
inv = json.load(open('${OUT_DIR}/inventory.json'))
print(*[h['$2'] for h in inv['hosts'] if h['role'] == '$1'])
"; }

OPS_PUB=$(read_inv ops public_ip)
COORD_PUB=$(read_inv coordinator public_ip)
COORD_PRIV=$(read_inv coordinator private_ip)
WORKER_PUBS=$(read_inv worker public_ip)
BROKER_PRIVS=$(read_inv broker private_ip)
BROKER_LIST=$(for ip in $BROKER_PRIVS; do printf "%s:9092," "$ip"; done | sed 's/,$//')
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV ops=$OPS_PUB"

for h in $OPS_PUB $COORD_PUB $WORKER_PUBS; do
    until on_host "$h" "docker info >/dev/null 2>&1"; do
        echo "campaign: waiting for docker on $h"; sleep 15
    done
done

# Shared checkpoint state, mounted from the ops host's NFS export.
# Checkpoint state is per-subtask (<dir>/v1/<subtask_idx>/), resolved on
# each node's own filesystem, so a killed worker's subtask redeployed to
# a different host would find nothing and the restore would refuse. On
# per-host local disks this campaign would measure failed restores
# rather than recovery.
OPS_PRIV=$(read_inv ops private_ip)
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h - refusing to run a" >&2
             echo "  worker-kill campaign on per-host local state; recovery would fail by" >&2
             echo "  construction and the result would be meaningless." >&2; exit 2; }
done
# Prove it is genuinely shared, not just mounted.
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done
echo "campaign: shared state verified on the coordinator and every worker"

# --- stack -------------------------------------------------------------
echo "campaign: deploying stack"
i=0
for bp in $(read_inv broker public_ip); do
    i=$((i+1))
    priv=$(echo "$BROKER_PRIVS" | cut -d' ' -f$i)
    on_host "$bp" "mkdir -p /qual"
    to_host "$bp" "$HERE/../infra/broker.yml" /qual/broker.yml
    on_host "$bp" "cd /qual && NODE_ID=$i PRIVATE_IP=$priv SEEDS='$BROKER_LIST' docker compose -f broker.yml up -d"
done

on_host "$COORD_PUB" "mkdir -p /qual"
to_host "$COORD_PUB" "$HERE/../infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "cd /qual && CLINK_IMAGE=$CLINK_IMAGE CONTROL_IP=$COORD_PRIV docker compose -f coordinator.yml up -d"

wid=0
for wp in $WORKER_PUBS; do
    wid=$((wid+1))
    wpriv=$(read_inv worker private_ip | cut -d' ' -f$wid)
    on_host "$wp" "mkdir -p /qual /qual/state"
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "cd /qual && CLINK_IMAGE=$CLINK_IMAGE CONTROL_IP=$COORD_PRIV WORKER_ID=w$wid WORKER_IP=$wpriv docker compose -f worker.yml up -d"
done

# The deployed build's own capability manifest, captured as evidence
# BEFORE the campaign runs: it is what the verdict's claims are scoped
# to, and it is where the fault-injection surface is asserted rather
# than assumed. A `twopc` profile against a build with no fault points
# would arm CLINK_FAULT_INJECT into a binary that compiles every fault
# point to nothing - reporting faults it never injected.
until on_host "$COORD_PUB" "docker exec clink-coordinator clink --capabilities-json" \
        > "$OUT_DIR/capabilities.json" 2>/dev/null; do
    echo "campaign: waiting for the coordinator to answer --capabilities-json"; sleep 10
done
FAULT_SURFACE=$(python3 -c "
import json; d=json.load(open('$OUT_DIR/capabilities.json'))
print('yes' if (d.get('build') or d).get('fault_injection') else 'no')")
echo "campaign: fault-injection surface: $FAULT_SURFACE"
if [ "$PROFILE" = "twopc" ] && [ "$FAULT_SURFACE" != "yes" ]; then
    echo "campaign: profile 'twopc' needs a build with CLINK_ENABLE_FAULT_INJECTION=ON;" >&2
    echo "  the deployed image has no fault surface, so the targeted 2PC-window faults" >&2
    echo "  would be silently skipped. Build a qualification image:" >&2
    echo "  docker build --build-arg CLINK_ENABLE_FAULT_INJECTION=ON -f docker/Dockerfile.runtime ." >&2
    exit 2
fi

# Topics: partition count is load-bearing. Source parallelism equals it
# (the configuration the per-partition watermark work proved exact), and
# the oracle's per-partition sequences assume exactly these partitions.
BROKER1_PUB=$(read_inv broker public_ip | cut -d' ' -f1)
on_host "$BROKER1_PUB" "docker exec redpanda rpk topic create qual01-in -p $PARTITIONS -r 3 || true"
on_host "$BROKER1_PUB" "docker exec redpanda rpk topic create qual01-out -p $PARTITIONS -r 3 || true"

# --- ops host: generator + verifier + chaos -----------------------------
on_host "$OPS_PUB" "mkdir -p /qual"
for f in detspec.py generator.py verifier.py; do to_host "$OPS_PUB" "$HERE/$f" "/qual/$f"; done
to_host "$OPS_PUB" "$HERE/../chaos/chaos.py" /qual/chaos.py
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
to_host "$OPS_PUB" "$KEY_FILE" /root/.ssh/id_ed25519
on_host "$OPS_PUB" "chmod 600 /root/.ssh/id_ed25519 && pip3 install --break-system-packages -q confluent-kafka || pip3 install -q confluent-kafka"

# A fixed event-time base makes the whole campaign reproducible: the
# oracle's window boundaries do not depend on when it was started.
BASE_MS=$(( $(date +%s) * 1000 ))
echo "$BASE_MS" > "$OUT_DIR/base_ms"

on_host "$OPS_PUB" "cd /qual && nohup python3 generator.py --brokers '$BROKER_LIST' \
    --topic qual01-in --rate $RATE --partitions $PARTITIONS --keys $KEYS \
    --seed $SEED --base-ms $BASE_MS --max-jitter-ms $MAX_JITTER_MS \
    --window-ms $WINDOW_MS --progress /qual/progress.json \
    > /qual/generator.log 2>&1 &"
echo "campaign: generator started"

# --- pipeline -----------------------------------------------------------
sed -e "s|__BROKERS__|$BROKER_LIST|g" -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
    -e "s|__TXN_ID__|qual01-${RUN_ID}|g" "$HERE/pipeline.sql.tmpl" > "$OUT_DIR/pipeline.sql"

"$REPO_ROOT/build/tools/clink_submit_sql" \
    --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 6123 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir /qual/state \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --max-restarts-on-worker-loss "$MAX_RESTARTS" \
    | tee "$OUT_DIR/submit.log"
JOB_ID=$(python3 "$REPO_ROOT/benchmarks/nexmark_compare/cloud/job_id.py" < "$OUT_DIR/submit.log" 2>/dev/null \
         || grep -oE 'job[ _]?id[ =:]+[0-9]+' "$OUT_DIR/submit.log" | grep -oE '[0-9]+$' | head -1)
[ -n "$JOB_ID" ] || { echo "campaign: could not determine job id - see $OUT_DIR/submit.log" >&2; exit 1; }
echo "campaign: job $JOB_ID submitted"

on_host "$OPS_PUB" "cd /qual && nohup python3 verifier.py --brokers '$BROKER_LIST' \
    --topic qual01-out --spec /qual/progress.json.spec --progress /qual/progress.json \
    --verdict /qual/verdict.json > /qual/verifier.log 2>&1 &"
echo "campaign: verifier started"

# --- functional verification --------------------------------------------
#
# Prove the campaign is actually WORKING before leaving it to soak.
#
# Every failure this harness has had so far was silent: a chaos gate that
# could never fire, a verifier that read a partial topic, a job that
# could not checkpoint. All of them would have run for days and produced
# a confident-looking page of nulls. So nothing proceeds to the soak
# until each link in the chain is observed doing its job, and any link
# that cannot be observed aborts the run while the rig is still cheap to
# inspect.
echo "campaign: functional verification (nothing soaks until this passes)"
verify_fail() { echo "campaign: VERIFICATION FAILED - $1" >&2
                echo "  rig is still up for inspection; tear down with:" >&2
                echo "  scripts/qualification/destroy.sh $RUN_ID --yes" >&2
                exit 3; }

# 1. Input is flowing: the generator's progress must advance.
sleep 30
P1=$(on_host "$OPS_PUB" "python3 -c \"import json;print(sum(json.load(open('/qual/progress.json'))['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 30
P2=$(on_host "$OPS_PUB" "python3 -c \"import json;print(sum(json.load(open('/qual/progress.json'))['produced_high'].values()))\"" 2>/dev/null || echo 0)
[ "${P2:-0}" -gt "${P1:-0}" ] || verify_fail "the generator is not producing (progress stuck at ${P1:-unknown})"
echo "campaign: input flowing ($P1 -> $P2 events)"

# 2. The job is RUNNING and CHECKPOINTING. Without completed checkpoints
#    the transactional sink never commits, so the campaign would measure
#    a pipeline that cannot be exactly-once by construction.
JOB_JSON=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "cannot read job status"
echo "$JOB_JSON" > "$OUT_DIR/job-status.json"
JOB_OK=$(python3 -c "
import json,sys
j = json.loads(sys.argv[1])
print('yes' if j.get('status') == 'RUNNING' and j.get('latest_completed_checkpoint_id', 0) > 0
      and not j.get('errors') else 'no')" "$JOB_JSON")
[ "$JOB_OK" = "yes" ] || verify_fail "the job is not running with completed checkpoints: $JOB_JSON"
echo "campaign: job RUNNING with completed checkpoints"

# 3. Output is reaching the sink topic, committed.
OUT_N=$(on_host "$OPS_PUB" "python3 -c \"import json;print(json.load(open('/qual/verdict.json'))['output_records'])\"" 2>/dev/null || echo 0)
[ "${OUT_N:-0}" -gt 0 ] || verify_fail "the verifier has seen no committed output records"
echo "campaign: verifier observing committed output ($OUT_N records)"

# 4. The verifier is JUDGING, not merely consuming - windows must retire.
for _ in $(seq 1 20); do
    JUDGED=$(on_host "$OPS_PUB" "python3 -c \"import json;print(json.load(open('/qual/verdict.json'))['evaluated_windows'])\"" 2>/dev/null || echo 0)
    [ "${JUDGED:-0}" -gt 0 ] && break
    sleep 30
done
[ "${JUDGED:-0}" -gt 0 ] || verify_fail "the verifier has judged no windows; its verdict would be vacuous"
echo "campaign: verifier judging windows ($JUDGED evaluated)"

# --- chaos --------------------------------------------------------------
DURATION_S=$(( DURATION_H * 3600 ))
on_host "$OPS_PUB" "cd /qual && nohup python3 chaos.py --inventory /qual/inventory.json \
    --log /qual/chaos.jsonl --coordinator-url http://${COORD_PRIV}:8095 \
    --job-id $JOB_ID --run-id $RUN_ID --profile $PROFILE --seed $SEED \
    --duration-s $DURATION_S > /qual/chaos.log 2>&1 &"
echo "campaign: chaos started (${DURATION_H}h, profile=$PROFILE)"

# 5. Chaos must actually land a fault. A controller that applies none is
#    the failure this harness has already had once, and it looks exactly
#    like a quiet campaign.
echo "campaign: waiting for the first fault to land"
FAULTS=0
for _ in $(seq 1 40); do
    FAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/chaos.jsonl 2>/dev/null || echo 0" | tr -d ' ')
    [ "${FAULTS:-0}" -gt 0 ] && break
    sleep 30
done
if [ "${FAULTS:-0}" -eq 0 ]; then
    on_host "$OPS_PUB" "tail -20 /qual/chaos.log" >&2 || true
    verify_fail "the chaos controller applied no fault within 20 minutes"
fi
echo "campaign: chaos landing faults ($FAULTS recorded)"

# 6. And the job must SURVIVE that fault - recover and keep checkpointing.
sleep 90
POST=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "coordinator unreachable after the first fault"
POST_OK=$(python3 -c "
import json,sys
j = json.loads(sys.argv[1])
print('yes' if j.get('status') == 'RUNNING' else 'no')" "$POST")
[ "$POST_OK" = "yes" ] || verify_fail "the job did not recover from the first fault: $POST"
echo "campaign: job recovered from the first fault - VERIFICATION PASSED, entering soak"
echo "campaign: verification summary" > "$OUT_DIR/verification.txt"
{ echo "input_events_observed=$P2"; echo "output_records=$OUT_N";
  echo "windows_judged=$JUDGED"; echo "faults_landed=$FAULTS";
  echo "recovered_after_first_fault=yes"; } >> "$OUT_DIR/verification.txt"

# --- watch --------------------------------------------------------------
# Poll evidence back to the laptop every 10 minutes, so a campaign that
# dies at hour 90 still has 89 hours of evidence locally.
END=$(( $(date +%s) + DURATION_S ))
while [ "$(date +%s)" -lt "$END" ]; do
    sleep 600
    for f in verdict.json chaos.jsonl progress.json generator.log verifier.log chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done
    curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" \
        > "$OUT_DIR/job-status.json" 2>/dev/null || true
    python3 - "$OUT_DIR/verdict.json" <<'PY' || true
import json, sys
try:
    v = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
print("campaign: windows=%s missing=%s dup=%s conflicting=%s incorrect=%s foreign=%s"
      % (v.get("correct_windows"), v.get("missing"), v.get("duplicate"),
         v.get("conflicting"), v.get("incorrect"), v.get("foreign")), flush=True)
PY
done

echo "campaign: duration reached; stopping generator and collecting evidence"
on_host "$OPS_PUB" "pkill -INT -f generator.py || true"
sleep 120   # let the pipeline drain the tail and the verifier judge it
on_host "$OPS_PUB" "pkill -INT -f verifier.py || true"
sleep 20
for f in verdict.json chaos.jsonl progress.json progress.json.spec generator.log verifier.log chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
done

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --duration-h "$DURATION_H" --profile "$PROFILE" > "$OUT_DIR/QUAL-01-summary.md"
cat "$OUT_DIR/QUAL-01-summary.md"

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh $RUN_ID --yes && qualification/infra/teardown.sh --check"
