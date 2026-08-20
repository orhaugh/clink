#!/usr/bin/env bash
# Build a LOCAL qualification rig: docker-in-docker containers standing in
# for the cloud hosts, so the REAL campaign.sh runs here unmodified via
# its INVENTORY= seam before a cent is spent on provisioning.
#
#   qualification/test/local-rig/local-rig.sh up      build + start hosts,
#                                                     seed images, write
#                                                     inventory.json
#   qualification/test/local-rig/local-rig.sh down    remove everything
#
# Fidelity: same Ubuntu 24.04 + docker.io + pip bootstrap as the rig's
# cloud-init; per-host dockerd so container names, docker pause and kills
# behave per-host; a shared volume at /qual/state models the ops host's
# NFS export (visible to every host, like the mounted export). Deltas the
# rig still owns: real NFS latency/semantics, x86_64, cross-host packet
# paths, provider provisioning.
#
# macOS cannot reach container bridge IPs from the host, so the campaign
# DRIVER is itself a container on the rig network (lrig-driver, built on
# the runtime image: it carries the linux clink_submit_sql). After `up`,
# run the campaign inside it:
#   docker exec lrig-driver bash -c 'cd /repo && \
#     INVENTORY=qualification/test/local-rig/inventory.json \
#     SSH_KEY_FILE=/root/.ssh/id_ed25519 \
#     SUBMIT_BIN=/usr/local/bin/clink_submit_sql \
#     CLINK_IMAGE=clink-runtime:local-faultinj \
#     RUN_ID=qual02-local-a DURATION_S=600 MIN_GAP_S=20 PROFILE=aggressive \
#     qualification/qual02/campaign.sh'
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
NET=clink-localrig
VOL=clink-localrig-state
PREFIX=lrig
# ops + coordinator + 2 workers + 1 broker. The campaign derives worker
# and broker lists from the inventory, so the smaller-than-cloud shape is
# legitimate; anything that hardcodes the cloud counts should fail loudly
# here rather than on a paid rig.
HOSTS="ops:ops coordinator:coordinator worker1:worker worker2:worker broker1:broker"
RUNTIME_IMAGE="${CLINK_IMAGE:-clink-runtime:local-faultinj}"

down() {
    docker rm -f "$PREFIX-driver" >/dev/null 2>&1 || true
    for spec in $HOSTS; do
        docker rm -f "$PREFIX-${spec%%:*}" >/dev/null 2>&1 || true
        docker volume rm "$VOL-dockerd-${spec%%:*}" >/dev/null 2>&1 || true
    done
    docker network rm "$NET" >/dev/null 2>&1 || true
    docker volume rm "$VOL" >/dev/null 2>&1 || true
    echo "local rig removed"
}

if [ "${1:-}" = "down" ]; then
    down
    exit 0
fi
[ "${1:-}" = "up" ] || { echo "usage: local-rig.sh up|down" >&2; exit 2; }

command -v docker >/dev/null || { echo "docker required" >&2; exit 1; }
docker image inspect "$RUNTIME_IMAGE" >/dev/null 2>&1 || {
    echo "runtime image '$RUNTIME_IMAGE' not built; build it first:" >&2
    echo "  docker build --build-arg CLINK_ENABLE_FAULT_INJECTION=ON \\" >&2
    echo "    -f docker/Dockerfile.runtime -t $RUNTIME_IMAGE ." >&2
    exit 1
}

# One key for the whole local rig; campaign + chaos use it over ssh.
if [ ! -f "$HERE/id_ed25519" ]; then
    ssh-keygen -t ed25519 -N "" -f "$HERE/id_ed25519" -C clink-localrig >/dev/null
fi
cp "$HERE/id_ed25519.pub" "$HERE/hostimg/authorized_keys"

echo "==> building the rig-host image"
docker build -q -t clink-localrig-host "$HERE/hostimg" >/dev/null

docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
docker volume inspect "$VOL" >/dev/null 2>&1 || docker volume create "$VOL" >/dev/null

echo "==> starting hosts"
for spec in $HOSTS; do
    name="$PREFIX-${spec%%:*}"
    docker rm -f "$name" >/dev/null 2>&1 || true
    # Privileged: each host runs its own dockerd, and the chaos controller
    # uses tc/netem and iptables inside the host exactly as on the rig.
    # The inner dockerd gets its OWN volume at /var/lib/docker: an inner
    # daemon writing into the host container's overlay rootfs is
    # overlay-on-overlay, and big layer extraction fails there ("apply
    # layer error" the moment compose created the postgres container).
    docker volume create "$VOL-dockerd-${spec%%:*}" >/dev/null
    docker run -d --privileged --name "$name" --hostname "${spec%%:*}" \
        --network "$NET" -v "$VOL":/qual/state \
        -v "$VOL-dockerd-${spec%%:*}":/var/lib/docker \
        clink-localrig-host >/dev/null
done

echo "==> building + starting the driver"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
docker build -q --build-arg RUNTIME_IMAGE="$RUNTIME_IMAGE" \
    -t clink-localrig-driver "$HERE/driverimg" >/dev/null
docker rm -f "$PREFIX-driver" >/dev/null 2>&1 || true
docker run -d --name "$PREFIX-driver" --network "$NET" \
    -v "$REPO_ROOT":/repo -v "$HERE":/keys \
    clink-localrig-driver >/dev/null

ssh_ok() {
    docker exec "$PREFIX-driver" ssh -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null -o ConnectTimeout=3 -o BatchMode=yes \
        "root@$1" true 2>/dev/null
}

echo "==> waiting for sshd + per-host dockerd"
for spec in $HOSTS; do
    name="$PREFIX-${spec%%:*}"
    ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$name")
    for _ in $(seq 1 60); do
        if ssh_ok "$ip" && \
           docker exec "$PREFIX-driver" ssh -o StrictHostKeyChecking=no \
               -o UserKnownHostsFile=/dev/null -o BatchMode=yes \
               "root@$ip" "docker info >/dev/null 2>&1"; then
            break
        fi
        sleep 2
    done
    ssh_ok "$ip" || { echo "host $name never became reachable" >&2; exit 1; }
done

echo "==> seeding images into the per-host daemons"
seed() {  # host-container, image
    docker save "$2" | docker exec -i "$1" docker load >/dev/null
}
docker image inspect postgres:16 >/dev/null 2>&1 || docker pull -q postgres:16 >/dev/null
docker image inspect docker.redpanda.com/redpandadata/redpanda:v24.2.7 >/dev/null 2>&1 || \
    docker pull -q docker.redpanda.com/redpandadata/redpanda:v24.2.7 >/dev/null
seed "$PREFIX-coordinator" "$RUNTIME_IMAGE" & p1=$!
seed "$PREFIX-worker1" "$RUNTIME_IMAGE" & p2=$!
seed "$PREFIX-worker2" "$RUNTIME_IMAGE" & p3=$!
seed "$PREFIX-broker1" docker.redpanda.com/redpandadata/redpanda:v24.2.7 & p4=$!
# wait per pid: a bare `wait` returns 0 whatever the children did, and a
# swallowed seed failure surfaces minutes later as a mid-campaign pull.
for pid in $p1 $p2 $p3 $p4; do
    wait "$pid" || { echo "image seeding failed" >&2; exit 1; }
done
seed "$PREFIX-ops" postgres:16
seed "$PREFIX-ops" docker.redpanda.com/redpandadata/redpanda:v24.2.7

echo "==> writing inventory.json"
{
    echo '{'
    echo "  \"run_id\": \"local\","
    echo '  "hosts": ['
    first=1
    for spec in $HOSTS; do
        short="${spec%%:*}"; role="${spec##*:}"
        ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$PREFIX-$short")
        [ "$first" = 1 ] || echo ','
        first=0
        printf '    {"name": "%s", "role": "%s", "public_ip": "%s", "private_ip": "%s"}' \
            "$PREFIX-$short" "$role" "$ip" "$ip"
    done
    echo ''
    echo '  ]'
    echo '}'
} > "$HERE/inventory.json"
cat "$HERE/inventory.json"
echo "local rig up. Campaign invocation is in this script's header."
