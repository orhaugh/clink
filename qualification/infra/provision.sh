#!/usr/bin/env bash
# Provision the qualification rig on Hetzner Cloud, labelled per run so
# scripts/qualification/destroy.sh can remove everything by label.
#
#   RUN_ID=<qualification run id> ./provision.sh          create + inventory
#   RUN_ID=... ./provision.sh --dry                        print, touch nothing
#   RUN_ID=... STATE_VOLUME_GB=300 ./provision.sh          + a volume for /qual/state
#
# STATE_VOLUME_GB attaches a Hetzner volume to the ops host and mounts it
# at /qual/state before the NFS export, for campaigns whose shared state
# outgrows the ops host's 160 GB root disk. destroy.sh and teardown.sh
# --check already sweep volumes by label, so the orphan guard covers it.
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
# Measured on the 2026-08-17 rig (this exact topology): EUR 0.433/hour,
# so a 2-hour shakedown is ~EUR 0.87 and the 7-day QUAL-01 campaign
# ~EUR 73. Earlier comments quoting EUR 0.19/hour predate the cpx*2
# generation and the 8-host topology; do not budget from them.
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

# Shared-state volume for the ops host, in GB. 0 (the default) keeps the
# original behaviour: /qual/state lives on the ops host's own 160 GB root
# disk, which is ample for the delivery-semantics campaigns (QUAL-01 to
# QUAL-03 peaked at 6.7 GiB).
#
# A large-state campaign cannot use the root disk. Checkpoint retention
# keeps one COMPLETED checkpoint plus the restore point, so the shared
# directory holds roughly twice the live state size: a 100 GB campaign
# needs ~200 GB against a 160 GB disk, and the campaign would die of
# ENOSPC mid-soak with the failure looking like a checkpointing defect.
# Set STATE_VOLUME_GB to at least 2.5x the target state size.
STATE_VOLUME_GB="${STATE_VOLUME_GB:-0}"
STATE_VOLUME_NAME="qual-${RUN_ID}-state"

DRY=0
[ "${1:-}" = "--dry" ] && DRY=1
command -v hcloud >/dev/null 2>&1 || { echo "hcloud CLI not found"; exit 2; }

echo "context : $(hcloud context active)"
echo "run id  : ${RUN_ID}"
echo "plan    : 1x ${OPS_TYPE} (ops) + 1x ${COORD_TYPE} (coordinator) + ${WORKERS}x ${WORKER_TYPE} (workers) + ${BROKERS}x ${BROKER_TYPE} (brokers)"
if [ "${STATE_VOLUME_GB}" -gt 0 ]; then
    echo "volume  : ${STATE_VOLUME_GB} GB attached to the ops host as /qual/state (NFS-exported)"
fi

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
if [ "${STATE_VOLUME_GB}" -gt 0 ]; then
    # Deliberately no euro figure: the API exposes no volume pricing to
    # read it from, and a hardcoded rate in a cost line is how a stale
    # number becomes a budget. Volumes bill per GB-month on top of the
    # hourly figure above, and destroy.sh removes them with the rig.
    echo "          plus a ${STATE_VOLUME_GB} GB volume, billed per GB-month until destroyed"
fi

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
{
    echo '#cloud-config'
    echo 'package_update: true'
    echo 'packages: [docker.io, docker-compose-v2, python3-pip, iproute2, iptables, nfs-kernel-server]'
    if [ "${STATE_VOLUME_GB}" -gt 0 ]; then
        cat <<'YAML'
write_files:
  - path: /usr/local/bin/qual-mount-state.sh
    permissions: "0755"
    content: |
      #!/bin/bash
      # Put the attached volume under /qual/state BEFORE the NFS export
      # goes up. Written as a file rather than inline runcmd so the shell
      # quoting is reviewable and a colon in a message cannot break the
      # cloud-config parse.
      set -euo pipefail
      dev=""
      # The attach and the first boot race, so poll rather than assume.
      for _ in $(seq 1 90); do
          dev=$(ls /dev/disk/by-id/scsi-0HC_Volume_* 2>/dev/null | head -1 || true)
          [ -n "$dev" ] && break
          sleep 2
      done
      if [ -z "$dev" ]; then
          # Fail loudly. Exporting the root disk instead would look like a
          # working rig right up to the point a large-state campaign
          # filled 160 GB mid-soak, and the ENOSPC would read as a
          # checkpointing defect.
          echo "qual-mount-state found no Hetzner volume device" >&2
          exit 1
      fi
      # Format only a blank device - a re-run must never wipe state.
      if ! blkid "$dev" >/dev/null 2>&1; then
          mkfs.ext4 -F -L qualstate "$dev"
      fi
      mkdir -p /qual/state
      grep -q " /qual/state " /etc/fstab || \
          echo "$dev /qual/state ext4 defaults,nofail 0 2" >> /etc/fstab
      mountpoint -q /qual/state || mount /qual/state
YAML
    fi
    echo 'runcmd:'
    echo '  - systemctl enable --now docker'
    echo '  - usermod -aG docker root'
    echo '  - sysctl -w vm.max_map_count=262144'
    if [ "${STATE_VOLUME_GB}" -gt 0 ]; then
        echo '  - /usr/local/bin/qual-mount-state.sh'
    fi
    cat <<'YAML'
  - mkdir -p /qual/state
  - chmod 777 /qual/state
  - echo "/qual/state 10.20.1.0/24(rw,sync,no_subtree_check,no_root_squash)" >> /etc/exports
  - exportfs -ra
  - systemctl enable --now nfs-kernel-server
YAML
} > "$OPS_CLOUD_INIT"

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

create() {  # name type [cloud-init] [extra hcloud args...]
    local name=$1 type=$2 init=${3:-$CLOUD_INIT}
    if [ "$#" -ge 3 ]; then shift 3; else shift "$#"; fi
    if hcloud server describe "$name" >/dev/null 2>&1; then
        echo "==> ${name} exists, skipping"
        return 0
    fi
    echo "==> creating ${name} (${type})"
    hcloud server create --name "$name" --type "$type" --image "$IMAGE" \
        --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --network "$NET_NAME" \
        --user-data-from-file "$init" --label "$LABELS" "$@" >/dev/null
}

# The shared-state volume, created BEFORE the ops host so it can be
# attached at server-create time and be present when cloud-init looks for
# it. Hetzner's minimum volume is 10 GB.
OPS_EXTRA=()
if [ "${STATE_VOLUME_GB}" -gt 0 ]; then
    if [ "${STATE_VOLUME_GB}" -lt 10 ]; then
        echo "provision: STATE_VOLUME_GB must be at least 10 (Hetzner's minimum)" >&2
        exit 2
    fi
    if ! hcloud volume describe "$STATE_VOLUME_NAME" >/dev/null 2>&1; then
        echo "==> creating ${STATE_VOLUME_GB} GB volume ${STATE_VOLUME_NAME}"
        # Not --format: the device is formatted by cloud-init, which skips
        # a device that already carries a filesystem, so re-running this
        # script can never wipe a campaign's state.
        hcloud volume create --name "$STATE_VOLUME_NAME" --size "$STATE_VOLUME_GB" \
            --location "$LOCATION" --label "$LABELS" >/dev/null
    fi
    OPS_EXTRA=(--volume "$STATE_VOLUME_NAME")
fi

create "qual-${RUN_ID}-ops" "$OPS_TYPE" "$OPS_CLOUD_INIT" ${OPS_EXTRA[@]+"${OPS_EXTRA[@]}"}

# Attach fallback for a rig whose ops host already existed when the volume
# was added (create() skips an existing server, so the --volume above
# would never be applied).
if [ "${STATE_VOLUME_GB}" -gt 0 ]; then
    attached_to=$(hcloud volume describe "$STATE_VOLUME_NAME" -o format='{{.Server.ID}}' 2>/dev/null || true)
    if [ -z "$attached_to" ] || [ "$attached_to" = "<no value>" ] || [ "$attached_to" = "0" ]; then
        echo "==> attaching ${STATE_VOLUME_NAME} to the ops host"
        hcloud volume attach --server "qual-${RUN_ID}-ops" "$STATE_VOLUME_NAME" >/dev/null
        echo "    NOTE: the volume was attached after first boot, so cloud-init did not"
        echo "    mount it. On the ops host run: /usr/local/bin/qual-mount-state.sh"
    fi
fi
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
# The machine-readable inventory every other tool keys on (pull-image.sh
# refuses to run without it, chaos.py and campaign.sh read it). Written
# HERE so the rig never exists without its inventory - the prior run had
# to reconstruct it by hand before the image pull could proceed.
"$(dirname "$0")/inventory.sh"
echo
echo "REMINDER: these bill until destroyed."
echo "  scripts/qualification/destroy.sh ${RUN_ID} --yes   # then:"
echo "  qualification/infra/teardown.sh --check"
