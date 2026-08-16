#!/usr/bin/env bash
# Build the qualification runtime image ON the rig, at the exact commit
# under test, and load it onto every clink host.
#
# PREFER qualification/infra/pull-image.sh. GitHub Actions builds the same
# image for nothing, on a runner warm from the registry build cache, and every
# rig host then pulls the same digest:
#
#   gh workflow run runtime-image.yml -f fault_injection=true --ref <branch>
#   RUN_ID=<id> IMAGE=ghcr.io/orhaugh/clink-runtime:sha-<short>-faultinj ./pull-image.sh
#
# This script compiles the engine in Release with LTO on paid rig hardware -
# about forty minutes of an eight-host rig - and exists for the cases the
# registry cannot serve: an uncommitted tree, or a commit that has not been
# pushed anywhere a runner can reach.
#
# Why not the published image: the campaigns qualify a specific revision,
# and the published :main tag is whatever was last released - for this
# programme that predates the fixes the campaigns depend on (a coordinator
# that ignores the checkpoint configuration cannot run an exactly-once
# campaign at all). Why not build locally and push: the laptop is arm64
# and the rig is amd64, and pushing is not this script's business.
#
# So the source is shipped as a git archive of the commit under test, the
# toolchain base is built from the public Debian image plus the public
# prebuilt dependency archive, and the runtime image is built on top. No
# registry credentials are needed anywhere.
#
#   RUN_ID=<id> ./build-image.sh [--fault-injection]
#
# --fault-injection compiles in the named fault points (2PC crash
# windows). A shipped image has no fault surface, and arming
# CLINK_FAULT_INJECT against one that lacks it is a SILENT no-op, so the
# qualification image is built with it deliberately and the campaign
# asserts it from the capability manifest.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
RUN_ID="${RUN_ID:?set RUN_ID}"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
IMAGE_TAG="${IMAGE_TAG:-clink-runtime:qual}"
FAULT_INJECTION=OFF
[ "${1:-}" = "--fault-injection" ] && FAULT_INJECTION=ON

SSH_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() { ssh "${SSH_OPTS[@]}" "root@$1" "$2"; }

[ -f "$OUT_DIR/inventory.json" ] || { echo "build-image: no inventory at $OUT_DIR/inventory.json" >&2; exit 2; }
read_inv() { python3 -c "
import json
inv = json.load(open('$OUT_DIR/inventory.json'))
print(*[h['$2'] for h in inv['hosts'] if h['role'] == '$1'])
"; }

OPS_PUB=$(read_inv ops public_ip)
OPS_PRIV=$(read_inv ops private_ip)
CLINK_HOSTS="$(read_inv coordinator private_ip) $(read_inv worker private_ip)"

SHA=$(git -C "$REPO_ROOT" rev-parse HEAD)
DIRTY=$(git -C "$REPO_ROOT" status --porcelain | wc -l | tr -d ' ')
echo "build-image: commit $SHA (working tree changes: $DIRTY)"
if [ "$DIRTY" != "0" ]; then
    echo "build-image: REFUSING - the working tree is dirty. A qualification image" >&2
    echo "  must correspond to a commit, or its results cannot be tied to one." >&2
    exit 2
fi

echo "build-image: shipping source to the ops host"
on_host "$OPS_PUB" "rm -rf /qual/src && mkdir -p /qual/src"
git -C "$REPO_ROOT" archive --format=tar HEAD \
    | ssh "${SSH_OPTS[@]}" "root@$OPS_PUB" "tar x -C /qual/src"

echo "build-image: building the toolchain base (public Debian + public prebuilt deps)"
on_host "$OPS_PUB" "cd /qual/src && docker build -t clink-build:qual -f docker/Dockerfile . 2>&1 | tail -5"

echo "build-image: building the runtime image (fault injection: $FAULT_INJECTION)"
BUILD_PARALLEL=$(on_host "$OPS_PUB" "nproc" | tr -d '\r')
echo "build-image: building with parallelism ${BUILD_PARALLEL}"
on_host "$OPS_PUB" "cd /qual/src && docker build \
    --build-arg BASE_IMAGE=clink-build:qual \
    --build-arg BUILD_PARALLEL=${BUILD_PARALLEL} \
    --build-arg CLINK_ENABLE_FAULT_INJECTION=$FAULT_INJECTION \
    --build-arg CLINK_GIT_SHA=$SHA \
    -t $IMAGE_TAG -f docker/Dockerfile.runtime . 2>&1 | tail -5"

# The image's own account of itself, kept as evidence: this is what the
# campaign's claims are scoped to.
on_host "$OPS_PUB" "docker run --rm --entrypoint clink $IMAGE_TAG --capabilities-json" \
    > "$OUT_DIR/image-capabilities.json"
python3 - "$OUT_DIR/image-capabilities.json" "$SHA" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
build = doc.get("build") or {}
print(f"build-image: image git_sha={build.get('git_sha')} clean={build.get('git_clean')} "
      f"sql={build.get('sql')} fault_injection={build.get('fault_injection')}")
if build.get("git_sha") and not sys.argv[2].startswith(str(build.get("git_sha"))):
    print("build-image: WARNING - the image reports a different commit than was shipped",
          file=sys.stderr)
PY

echo "build-image: distributing to $CLINK_HOSTS"
for h in $CLINK_HOSTS; do
    # Over the private network, ops -> host, so the image crosses no
    # public link and no registry.
    on_host "$OPS_PUB" "docker save $IMAGE_TAG | ssh -o StrictHostKeyChecking=no \
        -i /root/.ssh/id_ed25519 root@$h 'docker load'" >/dev/null
    echo "build-image: loaded on $h"
done

echo "build-image: done. Use CLINK_IMAGE=$IMAGE_TAG"
