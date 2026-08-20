#!/usr/bin/env bash
# Pull a published runtime image onto every clink host in the rig.
#
# The alternative this replaces was building the image ON the rig: ship a git
# archive to the ops host, compile the engine in Release with LTO there, then
# `docker save | ssh docker load` it to each of the other hosts. That works,
# and it cost roughly forty minutes of an eight-host paid rig per build, twice
# in one night, for an artifact GitHub Actions will build for nothing on a
# runner that is already warm from the registry build cache.
#
# So the rig now only has to exist for the campaign. Build the image first
# with the runtime-image workflow, then point this at the tag it published:
#
#   gh workflow run runtime-image.yml -f fault_injection=true --ref <branch>
#   gh run watch <id>
#   RUN_ID=<id> IMAGE=ghcr.io/orhaugh/clink-runtime:sha-<short>-faultinj ./pull-image.sh
#
# Every host pulls the same reference, so they are running the same bytes by
# construction rather than because a tarball copied correctly. The package is
# public, so no registry credentials touch the rig.
#
# build-image.sh remains for the case this cannot serve: an uncommitted tree,
# or a commit that has not been pushed anywhere a runner can reach.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
RUN_ID="${RUN_ID:?set RUN_ID}"
IMAGE="${IMAGE:?set IMAGE to the published tag, e.g. ghcr.io/orhaugh/clink-runtime:sha-abc123-faultinj}"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() { ssh -n "${SSH_OPTS[@]}" "root@$1" "$2"; }

[ -f "$OUT_DIR/inventory.json" ] || { echo "pull-image: no inventory at $OUT_DIR/inventory.json" >&2; exit 2; }
read_inv() { python3 -c "
import json
inv = json.load(open('$OUT_DIR/inventory.json'))
print(*[h['$2'] for h in inv['hosts'] if h['role'] == '$1'])
"; }

OPS_PUB=$(read_inv ops public_ip)
COORD_PUB=$(read_inv coordinator public_ip)
WORKER_PUBS=$(read_inv worker public_ip)

echo "pull-image: $IMAGE"
for h in $OPS_PUB $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "docker pull -q $IMAGE" >/dev/null \
        || { echo "pull-image: $h could not pull $IMAGE" >&2; exit 2; }
    echo "pull-image: pulled on $h"
done

# Every host must be on the SAME digest. A tag is mutable, and a rig where one
# host resolved it a minute later than the others is a cluster running two
# builds - which the campaign would have no way to notice.
DIGEST=""
for h in $OPS_PUB $COORD_PUB $WORKER_PUBS; do
    d=$(on_host "$h" "docker image inspect $IMAGE --format '{{index .RepoDigests 0}}'" | tr -d '\r')
    if [ -z "$DIGEST" ]; then
        DIGEST="$d"
    elif [ "$d" != "$DIGEST" ]; then
        echo "pull-image: $h resolved $IMAGE to $d, not $DIGEST." >&2
        echo "  The rig would be running more than one build of clink." >&2
        exit 2
    fi
done
echo "pull-image: every host on $DIGEST"

# The image's own account of itself, kept as evidence, exactly as the
# build-on-rig path did. This is what the campaign's claims are scoped to.
on_host "$COORD_PUB" "docker run --rm --entrypoint clink $IMAGE --capabilities-json" \
    > "$OUT_DIR/image-capabilities.json"
python3 - "$OUT_DIR/image-capabilities.json" "$IMAGE" "$DIGEST" "$OUT_DIR/image-provenance.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
build = doc.get("build") or {}
print(f"pull-image: image git_sha={build.get('git_sha')} clean={build.get('git_clean')} "
      f"sql={build.get('sql')} fault_injection={build.get('fault_injection')}")
json.dump({"image": sys.argv[2], "digest": sys.argv[3], "build": build},
          open(sys.argv[4], "w"), indent=2)
PY
echo "pull-image: done. Use CLINK_IMAGE=$IMAGE"
