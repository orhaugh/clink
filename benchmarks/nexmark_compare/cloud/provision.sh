#!/usr/bin/env bash
# Provision the Hetzner cluster for a cross-engine nexmark run, and nothing else.
#
# WHY A REAL CLUSTER. The single-box harness runs both engines, the broker and
# four containers on one laptop, and its run-to-run spread on identical work has
# been wide enough to swamp the effects being measured (clink's q0 has been
# recorded at both 1.06M and 571k rec/s for the same configuration). Dedicated
# vCPUs, an isolated broker and a real network between nodes remove that.
#
# DEDICATED vCPU (CCX), not shared (CX/CPX), deliberately. Shared vCPU means
# noisy neighbours, and that variance is precisely what this rig exists to avoid.
#
# EVERY resource carries purpose=clink-bench so ./teardown.sh can remove all of
# it by label. Servers bill until destroyed - run ./teardown.sh when finished and
# ./teardown.sh --check afterwards.
#
#   ./provision.sh          create the cluster and print an inventory
#   ./provision.sh --dry    print what it WOULD create, touch nothing
set -euo pipefail

LABEL="purpose=clink-bench"
LOCATION="${LOCATION:-fsn1}"       # cheapest EU region
IMAGE="${IMAGE:-ubuntu-24.04}"
NET_NAME="clink-bench-net"
NET_RANGE="10.10.0.0/16"
SUBNET="10.10.1.0/24"
SSH_KEY_NAME="clink-bench-key"
SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-bench-ed25519}"

# 1 control plane + 3 workers + 1 broker. The broker is its OWN node so it stops
# competing with the engines for cores - on the laptop rig its CPU was charged to
# neither engine while it sat on the same cores as both.
CONTROL_TYPE="${CONTROL_TYPE:-ccx13}"   # 2 vCPU
WORKER_TYPE="${WORKER_TYPE:-ccx23}"     # 4 vCPU
BROKER_TYPE="${BROKER_TYPE:-ccx23}"     # 4 vCPU
WORKERS="${WORKERS:-3}"

# TOPOLOGY=split is the quota-constrained rig: ONE engine node and ONE broker
# node, 8 dedicated cores total. Hetzner caps dedicated (CCX) cores per project
# and the default here stopped the full cluster at 6 - so until that limit is
# raised this is what fits, and it buys the thing that matters most: the broker
# stops competing with the engines for cores. Both engines run on the SAME engine
# node, one after the other, so the hardware under each is identical.
#
# TOPOLOGY=full is the intended rig (control + 3 workers + broker, 18 cores) and
# needs the quota raised.
TOPOLOGY="${TOPOLOGY:-full}"
if [ "$TOPOLOGY" = "split" ]; then
    WORKERS=1
    CONTROL_TYPE=""          # no separate control node; the engine node hosts it
fi

DRY=0
[ "${1:-}" = "--dry" ] && DRY=1

command -v hcloud >/dev/null 2>&1 || { echo "hcloud CLI not found"; exit 2; }

echo "context : $(hcloud context active)"
echo "location: ${LOCATION}"
if [ "$TOPOLOGY" = "split" ]; then
    echo "plan    : SPLIT - 1x ${WORKER_TYPE} (engine, hosts its own control plane) + 1x ${BROKER_TYPE} (broker)"
else
    echo "plan    : 1x ${CONTROL_TYPE} (control) + ${WORKERS}x ${WORKER_TYPE} (workers) + 1x ${BROKER_TYPE} (broker)"
fi

# Cost, read from the API rather than assumed, so the figure printed is the one
# that will actually be billed.
hcloud server-type list -o json | python3 -c "
import json,sys
types=json.load(sys.stdin)
# Counts ACCUMULATED, not a dict literal: worker and broker types are often the
# same (both ccx23 by default), and a literal silently drops the earlier entry -
# which under-reported this cluster by 3x the first time it ran.
want={}
for name,count in (('${CONTROL_TYPE}',1),('${WORKER_TYPE}',${WORKERS}),('${BROKER_TYPE}',1)):
    if not name: continue
    want[name]=want.get(name,0)+count
total=0.0
for t in types:
    n=t.get('name')
    if n not in want: continue
    for pr in t.get('prices') or []:
        if pr.get('location')!='${LOCATION}': continue
        h=float(pr.get('price_hourly',{}).get('gross',0) or 0)
        total+=h*want[n]
        break
print('cost    : EUR %.3f/hour  (~EUR %.2f for 4h, ~EUR %.2f for 8h)' % (total,total*4,total*8))
"
echo

if [ "$DRY" = "1" ]; then
    echo "--dry: nothing created."
    exit 0
fi

# Dedicated key, not the operator's default identity: this key exists only to
# reach throwaway benchmark nodes and is deleted with them.
if [ ! -f "$SSH_KEY_FILE" ]; then
    echo "==> generating ${SSH_KEY_FILE}"
    ssh-keygen -t ed25519 -N "" -f "$SSH_KEY_FILE" -C "clink-bench" >/dev/null
fi
if ! hcloud ssh-key describe "$SSH_KEY_NAME" >/dev/null 2>&1; then
    echo "==> uploading ssh key"
    hcloud ssh-key create --name "$SSH_KEY_NAME" --public-key-from-file "${SSH_KEY_FILE}.pub" \
        --label "$LABEL" >/dev/null
fi

# Private network: engine traffic stays off the public interface, so the
# measurement is not at the mercy of public routing.
if ! hcloud network describe "$NET_NAME" >/dev/null 2>&1; then
    echo "==> creating network ${NET_NAME} (${NET_RANGE})"
    hcloud network create --name "$NET_NAME" --ip-range "$NET_RANGE" --label "$LABEL" >/dev/null
    hcloud network add-subnet "$NET_NAME" --network-zone eu-central --type cloud \
        --ip-range "$SUBNET" >/dev/null
fi

# Docker via cloud-init, so a node is usable the moment it boots.
CLOUD_INIT=$(mktemp)
cat > "$CLOUD_INIT" <<'YAML'
#cloud-config
package_update: true
packages: [docker.io, docker-compose-v2]
runcmd:
  - systemctl enable --now docker
  - usermod -aG docker root
  - sysctl -w vm.max_map_count=262144
YAML

create() {  # name type
    local name=$1 type=$2
    if hcloud server describe "$name" >/dev/null 2>&1; then
        echo "==> ${name} exists, skipping"
        return 0
    fi
    echo "==> creating ${name} (${type})"
    hcloud server create --name "$name" --type "$type" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --network "$NET_NAME" \
        --user-data-from-file "$CLOUD_INIT" --label "$LABEL" >/dev/null
}

[ -n "$CONTROL_TYPE" ] && create clink-bench-control "$CONTROL_TYPE"
for i in $(seq 1 "$WORKERS"); do create "clink-bench-worker${i}" "$WORKER_TYPE"; done
create clink-bench-broker "$BROKER_TYPE"
rm -f "$CLOUD_INIT"

echo
echo "Inventory:"
hcloud server list -l "$LABEL" -o columns=name,status,ipv4,private_net

echo
echo "REMINDER: these bill until destroyed. ./teardown.sh when finished,"
echo "then ./teardown.sh --check to confirm nothing survived."
