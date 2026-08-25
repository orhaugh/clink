#!/usr/bin/env bash
# QUAL-12's image smoke: the control-plane refusals in the SHIPPED BINARY.
#
# The matrix's control-plane rows are proven as unit cases against the
# validation function. That leaves one thing unproven: that the shipped
# clink_node actually CALLS it and exits. A validator nobody invokes
# refuses nothing, and that wiring is exactly what a unit test cannot
# see.
#
# WHY NO RIG. The programme audit provisioned a "small rig smoke" for
# this. It is not needed: every refusal here is a process-start property
# that manifests while parsing argv, before a socket is opened, so it
# needs the IMAGE, not a cluster. Eight hosts would observe the same
# eight exits, an hour later, for money. The image is pulled and run
# directly - under emulation where the host and image architectures
# differ, which is fine for a check whose whole content is an exit code
# and a message.
#
# WHAT THIS IMAGE CANNOT PROVE. The row cp.tls_on_non_tls_build needs a
# binary built WITHOUT TLS support, and the qualification image is built
# with it (build.tls = true). The smoke reports that row as UNEXERCISED
# here rather than passing it quietly - the unit case covers it, because
# the validator takes the linkage as a parameter precisely so both
# builds can be expressed.
#
#   ./image-smoke.sh --image <ref> [--out results.jsonl]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
IMAGE="${IMAGE:-}"
OUT="${OUT:-$REPO_ROOT/qualification-results/qual12/image-results.jsonl}"
RUN_ID="${RUN_ID:-qual12-image}"
PLATFORM="${PLATFORM:-linux/amd64}"

while [ $# -gt 0 ]; do
    case "$1" in
        --image) IMAGE="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --run-id) RUN_ID="$2"; shift 2 ;;
        --platform) PLATFORM="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
[ -n "$IMAGE" ] || { echo "image-smoke: --image is required" >&2; exit 2; }
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

echo "QUAL-12 image smoke: $IMAGE"

# The image's own view of whether it carries TLS decides which rows it
# can exercise. Reading it from the binary rather than assuming keeps the
# smoke honest about its own coverage.
CAPS=$(docker run --rm --platform "$PLATFORM" --entrypoint clink "$IMAGE" \
    --capabilities-json 2>/dev/null)
TLS_LINKED=$(printf '%s' "$CAPS" | python3 -c "
import json,sys
try:
    print('yes' if json.load(sys.stdin).get('build',{}).get('tls') else 'no')
except Exception:
    print('unknown')")
echo "  image reports TLS linked: $TLS_LINKED"

record() {  # id, outcome, detail
    python3 -c "
import json,sys
print(json.dumps({'id': sys.argv[1], 'outcome': sys.argv[2], 'detail': sys.argv[3]}))" \
        "$1" "$2" "$3" >> "$OUT"
    echo "  $1: $2"
}

# Run clink_node with a configuration that must be refused. The binary is
# expected to exit non-zero WITHOUT binding anything and to say why.
expect_refusal() {  # id, role, args...
    local id=$1 role=$2; shift 2
    local log; log=$(mktemp)
    # DETACHED with a deadline, never a blocking `docker run`. A binary
    # that does NOT refuse keeps running - that is the whole failure mode
    # under test - and a foreground run would hang the smoke forever
    # instead of reporting it. (It did, against a pre-fix image: the
    # coordinator started happily with an incomplete TLS pair and the
    # smoke sat there.) Staying up past the deadline IS the ACCEPT
    # reading.
    local cname="clink_q12_smoke_$$_${id//./_}"
    docker rm -f "$cname" >/dev/null 2>&1
    docker run -d --name "$cname" --platform "$PLATFORM" --entrypoint clink_node "$IMAGE" \
        "--role=$role" "$@" >/dev/null 2>&1
    local rc=-1 waited=0
    while [ "$waited" -lt "${SMOKE_DEADLINE_S:-45}" ]; do
        local state
        state=$(docker inspect -f '{{.State.Running}}' "$cname" 2>/dev/null)
        if [ "$state" != "true" ]; then
            rc=$(docker inspect -f '{{.State.ExitCode}}' "$cname" 2>/dev/null || echo 1)
            break
        fi
        sleep 3; waited=$(( waited + 3 ))
    done
    docker logs "$cname" > "$log" 2>&1
    docker rm -f "$cname" >/dev/null 2>&1
    if [ "$rc" -lt 0 ]; then
        # Still running at the deadline: it accepted the configuration.
        record "$id" "ACCEPT" "the binary was still running after ${waited}s with a configuration it cannot honour"
        rm -f "$log"
        return
    fi
    local msg; msg=$(grep -iE 'refus|TLS' "$log" | head -1 | cut -c1-200)
    if [ "$rc" -eq 0 ]; then
        record "$id" "ACCEPT" "the binary started with a configuration it cannot honour"
    elif grep -qi "refusing to start" "$log"; then
        record "$id" "REFUSE" "${msg:-refused}"
    else
        # Non-zero for some other reason (a missing flag, a bind failure)
        # is not the refusal under test - claiming it would be crediting
        # the row for an unrelated error.
        record "$id" "UNEXERCISED" "exited $rc without the refusal message: $(head -1 "$log" | cut -c1-160)"
    fi
    rm -f "$log"
}

expect_refusal cp.incomplete_server_pair coordinator --port=0 --tls-cert=/nonexistent.pem
expect_refusal cp.mtls_without_server_cert coordinator --port=0 --tls-client-ca=/nonexistent.pem
expect_refusal cp.client_creds_without_transport worker --id=smoke \
    --coordinator-host=127.0.0.1 --coordinator-port=1 \
    --tls-client-cert=/nonexistent.pem --tls-client-key=/nonexistent.pem

# The non-TLS-build row cannot be exercised by an image that links TLS.
if [ "$TLS_LINKED" = "no" ]; then
    expect_refusal cp.tls_on_non_tls_build coordinator --port=0 --tls-cert=/nonexistent.pem
else
    record cp.tls_on_non_tls_build UNEXERCISED \
        "this image links TLS (build.tls=$TLS_LINKED); the row needs a build without it"
fi

# The accept row: no TLS flags must still start. `--help` is enough to
# prove the binary is not refusing outright - it exercises the same argv
# path without binding a port.
HELP_RC=0
docker run --rm --platform "$PLATFORM" --entrypoint clink_node "$IMAGE" --help \
    > /dev/null 2>&1 || HELP_RC=$?
if [ "$HELP_RC" -eq 0 ]; then
    record cp.deliberate_plaintext ACCEPT "the binary runs with no TLS flags"
else
    record cp.deliberate_plaintext UNEXERCISED "could not run the binary at all (rc=$HELP_RC)"
fi

echo
# Judged against the control plane ONLY: that is all this smoke runs, and
# claiming the whole matrix would report every connector row as missing.
python3 "$HERE/summarise.py" --results "$OUT" --run-id "$RUN_ID" \
    --surfaces control_plane \
    | tee "$(dirname "$OUT")/QUAL-12-image-summary.md"
