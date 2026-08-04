#!/usr/bin/env bash
# Every declared fault point must have a call site.
#
# Fault rules are free strings and nothing validates them against a catalogue -
# deliberately, so a rule can name a point a given build does not have. The cost
# is that a point declared but never PLACED makes
#   CLINK_FAULT_INJECT="some.point=exit:70@1"
# run green having injected nothing, and the run reads as coverage. A name that
# promises a failure it cannot cause is worse than no name at all.
#
# Six points were in that state before this check existed. It is cheap enough to
# run on every commit and it is the only thing that would catch a seventh.
set -euo pipefail

cd "$(dirname "$0")/.."
HEADER="include/clink/fault/fault_injection.hpp"
missing=()

while IFS= read -r name; do
    # A call site is any reference outside the declaring header.
    if ! grep -rq --include='*.hpp' --include='*.cpp' "points::${name}\b" include src impls tools 2>/dev/null; then
        missing+=("$name")
    fi
done < <(grep -oE 'inline constexpr char (k[A-Za-z0-9_]+)\[\]' "$HEADER" | awk '{print $4}' | sed 's/\[\]//')

if [ ${#missing[@]} -ne 0 ]; then
    echo "check-fault-points: these fault points are declared but never placed:" >&2
    for m in "${missing[@]}"; do
        echo "  - $m" >&2
    done
    echo "" >&2
    echo "A declared-but-unplaced point makes CLINK_FAULT_INJECT for it succeed while" >&2
    echo "injecting nothing, which reads as coverage. Either add the CLINK_FAULT_POINT" >&2
    echo "call site, or remove the declaration." >&2
    exit 1
fi

echo "check-fault-points: all declared fault points have call sites"
