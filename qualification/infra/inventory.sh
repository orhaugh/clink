#!/usr/bin/env bash
# Capture the rig's inventory from Hetzner's own state into
# qualification-results/<RUN_ID>/inventory.json - the file every other
# qualification tool keys on (pull-image.sh, chaos.py, campaign.sh).
#
# Reusable on purpose: provisioning calls it so the inventory exists the
# moment the rig does, and any tool can re-run it to refresh a stale copy.
# It reads cloud state only; it never starts anything, so running it can
# never cost a paid cycle.
#
#   RUN_ID=<qualification run id> ./inventory.sh [RIG_RUN_ID] [OUT_DIR]
#
# RIG_RUN_ID defaults to RUN_ID (they differ only when a campaign reuses
# an existing rig under a new evidence directory). OUT_DIR defaults to
# qualification-results/<RUN_ID> relative to the repository root.
set -euo pipefail

RUN_ID="${RUN_ID:?set RUN_ID to the qualification run id}"
RIG_RUN_ID="${1:-$RUN_ID}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${2:-$HERE/../../qualification-results/$RUN_ID}"
mkdir -p "$OUT_DIR"

command -v hcloud >/dev/null 2>&1 || { echo "inventory: hcloud CLI not found" >&2; exit 2; }

hcloud server list -l "qual-run=${RIG_RUN_ID}" -o json | python3 -c "
import json, sys
servers = json.load(sys.stdin)
if not servers:
    print('inventory: no servers labelled qual-run=${RIG_RUN_ID}', file=sys.stderr)
    sys.exit(2)
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
print('inventory ->', '${OUT_DIR}/inventory.json', '(%d hosts)' % len(hosts))
"
