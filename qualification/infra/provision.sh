#!/usr/bin/env bash
# Provision the qualification rig on Hetzner Cloud, labelled per run so
# scripts/qualification/destroy.sh can remove everything by label.
#
#   RUN_ID=<qualification run id> ./provision.sh          create + inventory
#   RUN_ID=... ./provision.sh --dry                        print, touch nothing
#
# Topology (shared-vCPU deliberately: these campaigns measure correctness
# under faults, not comparative performance, and CPX is 3-5x cheaper than
# the CCX types the benchmark rig needs for stable throughput numbers):
#
#   qual-<id>-ops           CPX31  generator + verifier + chaos + observability
#   qual-<id>-coordinator   CPX21  clink coordinator
#   qual-<id>-worker{1..3}  CPX31  clink workers
#   qual-<id>-broker{1..3}  CPX21  Redpanda cluster (Kafka API)
#
# ~EUR 0.19/hour all in; the 7-day QUAL-01 campaign is ~EUR 32.
set -euo pipefail

# Same guard as the benchmark rig, for the same reason: the active hcloud
# context decides who gets billed, and this script must fail in the wrong
# project rather than be cleaned up out of it afterwards.
EXPECTED_CONTEXT="${EXPECTED_CONTEXT-clink-bench}"
if [ -n "$EXPECTED_CONTEXT" ]; then
    active="$(hcloud context active 2>/dev/null || true)"
    if [ "$active" != "$EXPECTED_CONTEXT" ]; then
        echo "REFUSING TO PROVISION: hcloud active context is '${active:-<none>}', expected" >&2
        echo "  '${EXPECTED_CONTEXT}'. Switch with: hcloud context use ${EXPECTED_CONTEXT}" >&2
        exit 1
    fi
fi

RUN_ID="${RUN_ID:?set RUN_ID to the qualification run id (e.g. qual01-20260815)}"
LABELS="qual=1,qual-run=${RUN_ID}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
NET_NAME="qual-${RUN_ID}-net"
NET_RANGE="10.20.0.0/16"
SUBNET="10.20.1.0/24"
SSH_KEY_NAME="qual-key"
SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"

# The cpx*2 generation: the older cpx11/21/31/41 still list prices in
# fsn1 but the API refuses to create them there, so anything defaulting
# to those fails partway through, after the network already exists.
OPS_TYPE="${OPS_TYPE:-cpx32}"
COORD_TYPE="${COORD_TYPE:-cpx22}"
WORKER_TYPE="${WORKER_TYPE:-cpx32}"
BROKER_TYPE="${BROKER_TYPE:-cpx22}"
WORKERS="${WORKERS:-3}"
BROKERS="${BROKERS:-3}"

DRY=0
[ "${1:-}" = "--dry" ] && DRY=1
command -v hcloud >/dev/null 2>&1 || { echo "hcloud CLI not found"; exit 2; }

echo "context : $(hcloud context active)"
echo "run id  : ${RUN_ID}"
echo "plan    : 1x ${OPS_TYPE} (ops) + 1x ${COORD_TYPE} (coordinator) + ${WORKERS}x ${WORKER_TYPE} (workers) + ${BROKERS}x ${BROKER_TYPE} (brokers)"

hcloud server-type list -o json | python3 -c "
import json, sys
types = json.load(sys.stdin)
want = {}
for name, count in (('${OPS_TYPE}',1),('${COORD_TYPE}',1),('${WORKER_TYPE}',${WORKERS}),('${BROKER_TYPE}',${BROKERS})):
    want[name] = want.get(name, 0) + count
total = 0.0
for t in types:
    if t.get('name') not in want:
        continue
    for pr in t.get('prices') or []:
        if pr.get('location') != '${LOCATION}':
            continue
        total += float(pr.get('price_hourly', {}).get('gross', 0) or 0) * want[t['name']]
        break
print('cost    : EUR %.3f/hour (~EUR %.2f/day, ~EUR %.2f for 7 days)' % (total, total*24, total*24*7))
"

if [ "$DRY" = "1" ]; then
    echo "--dry: nothing created."
    exit 0
fi

if [ ! -f "$SSH_KEY_FILE" ]; then
    echo "==> generating ${SSH_KEY_FILE}"
    ssh-keygen -t ed25519 -N "" -f "$SSH_KEY_FILE" -C "clink-qual" >/dev/null
fi
if ! hcloud ssh-key describe "$SSH_KEY_NAME" >/dev/null 2>&1; then
    echo "==> uploading ssh key"
    hcloud ssh-key create --name "$SSH_KEY_NAME" --public-key-from-file "${SSH_KEY_FILE}.pub" \
        --label "qual=1" >/dev/null
fi

if ! hcloud network describe "$NET_NAME" >/dev/null 2>&1; then
    echo "==> creating network ${NET_NAME} (${NET_RANGE})"
    hcloud network create --name "$NET_NAME" --ip-range "$NET_RANGE" --label "$LABELS" >/dev/null
    hcloud network add-subnet "$NET_NAME" --network-zone eu-central --type cloud \
        --ip-range "$SUBNET" >/dev/null
fi

# Two cloud-inits. The ops host EXPORTS /qual/state over NFS; the
# coordinator and workers MOUNT it.
#
# Shared state is not a convenience here, it is a correctness
# requirement. Checkpoint state is laid out as
# <checkpoint_dir>/v1/<subtask_idx>/, resolved on each node's own
# filesystem, so when a killed worker's subtask is redeployed onto a
# different host it finds no state and the restore refuses. A campaign
# whose whole point is killing workers cannot run on per-host local
# disks. (An object-store state backend is the other answer, and is
# QUAL-03's subject rather than a dependency of this one.)
OPS_CLOUD_INIT=$(mktemp)
cat > "$OPS_CLOUD_INIT" <<'YAML'
#cloud-config
package_update: true
packages: [docker.io, docker-compose-v2, python3-pip, iproute2, iptables, nfs-kernel-server]
runcmd:
  - systemctl enable --now docker
  - usermod -aG docker root
  - sysctl -w vm.max_map_count=262144
  - mkdir -p /qual/state
  - chmod 777 /qual/state
  - echo "/qual/state 10.20.1.0/24(rw,sync,no_subtree_check,no_root_squash)" >> /etc/exports
  - exportfs -ra
  - systemctl enable --now nfs-kernel-server
YAML

CLOUD_INIT=$(mktemp)
cat > "$CLOUD_INIT" <<'YAML'
#cloud-config
package_update: true
packages: [docker.io, docker-compose-v2, python3-pip, iproute2, iptables, nfs-common]
runcmd:
  - systemctl enable --now docker
  - usermod -aG docker root
  - sysctl -w vm.max_map_count=262144
  - mkdir -p /qual/state
YAML

create() {  # name type [cloud-init]
    local name=$1 type=$2 init=${3:-$CLOUD_INIT}
    if hcloud server describe "$name" >/dev/null 2>&1; then
        echo "==> ${name} exists, skipping"
        return 0
    fi
    echo "==> creating ${name} (${type})"
    hcloud server create --name "$name" --type "$type" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --network "$NET_NAME" \
        --user-data-from-file "$init" --label "$LABELS" >/dev/null
}

create "qual-${RUN_ID}-ops" "$OPS_TYPE" "$OPS_CLOUD_INIT"
create "qual-${RUN_ID}-coordinator" "$COORD_TYPE"
for i in $(seq 1 "$WORKERS"); do create "qual-${RUN_ID}-worker${i}" "$WORKER_TYPE"; done
for i in $(seq 1 "$BROKERS"); do create "qual-${RUN_ID}-broker${i}" "$BROKER_TYPE"; done
rm -f "$CLOUD_INIT" "$OPS_CLOUD_INIT"

# Firewall. The coordinator's HTTP API accepts job submissions, and a
# submitted job is code, so an unauthenticated control plane on a public
# IP is a remote-execution surface - short-lived test rig or not. SSH and
# every clink port are restricted to the operator's address; the rig
# talks to itself over the private network.
OPERATOR_IP="${OPERATOR_IP:-$(curl -fsS https://ifconfig.me 2>/dev/null || curl -fsS https://api.ipify.org)}"
if [ -z "$OPERATOR_IP" ]; then
    echo "provision: could not determine the operator's public IP; refusing to leave the" >&2
    echo "  control plane open. Set OPERATOR_IP=<addr> and re-run." >&2
    exit 2
fi
FW="qual-${RUN_ID}-fw"
if ! hcloud firewall describe "$FW" >/dev/null 2>&1; then
    echo "==> creating firewall ${FW} (operator ${OPERATOR_IP})"
    hcloud firewall create --name "$FW" --label "qual=1" --label "qual-run=${RUN_ID}" >/dev/null
fi
RULES=$(mktemp)
cat > "$RULES" <<JSON
[
  {"direction":"in","protocol":"tcp","port":"22","source_ips":["${OPERATOR_IP}/32"],"description":"ssh, operator only"},
  {"direction":"in","protocol":"tcp","port":"any","source_ips":["${NET_RANGE}"],"description":"rig-internal"},
  {"direction":"in","protocol":"udp","port":"any","source_ips":["${NET_RANGE}"],"description":"rig-internal"},
  {"direction":"in","protocol":"tcp","port":"8095","source_ips":["${OPERATOR_IP}/32"],"description":"coordinator HTTP, operator only"},
  {"direction":"in","protocol":"tcp","port":"6123","source_ips":["${OPERATOR_IP}/32"],"description":"coordinator control plane, operator only"},
  {"direction":"in","protocol":"tcp","port":"9092","source_ips":["${OPERATOR_IP}/32"],"description":"kafka, operator only"}
]
JSON
hcloud firewall replace-rules "$FW" --rules-file "$RULES" >/dev/null
rm -f "$RULES"
for s in $(hcloud server list -l "qual-run=${RUN_ID}" -o noheader -o columns=name); do
    hcloud firewall apply-to-resource "$FW" --type server --server "$s" >/dev/null 2>&1 || true
done
echo "==> firewall applied to every host"

echo
echo "Inventory:"
hcloud server list -l "qual-run=${RUN_ID}" -o columns=name,status,ipv4,private_net
echo
echo "REMINDER: these bill until destroyed."
echo "  scripts/qualification/destroy.sh ${RUN_ID} --yes   # then:"
echo "  qualification/infra/teardown.sh --check"
