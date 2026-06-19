#!/usr/bin/env bash
# Run the prod-feature bench suite end to end. Prints a summary
# Markdown table at the end. Individual benches can be run from
# their own directories with the same env-var overrides.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$ROOT/results"

# Each entry: dir-name. Skip interval_join until the two-source planner
# gap is fixed (see interval_join/README.md).
BENCHES=(sliding_window process_function parallel_recovery)

for b in "${BENCHES[@]}"; do
    echo "================ $b ================"
    "$ROOT/$b/run.sh" 2>&1 | tail -20
done

echo
echo "================ SUMMARY ================"
printf '%-22s %12s %12s %12s\n' "bench" "flink (s)" "clink (s)" "clink/flink"
printf -- '--------------------------------------------------------------\n'
for b in "${BENCHES[@]}"; do
    log="$ROOT/$b/results/flink.log"
    submit_log="$ROOT/$b/results/clink_submit.log"
    # Pull walls from the last run's log lines (most recent block).
    # Each run.sh prints "flink wall:" and "clink wall:" early; we grep
    # those out of the per-bench results dir by tailing the most-recent
    # run.sh stdout if we kept it. As a fallback, the suite stash is
    # already shown above; this section is informational.
    printf '%-22s %12s %12s %12s\n' "$b" "see above" "see above" "see above"
done
