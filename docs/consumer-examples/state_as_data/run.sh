#!/usr/bin/env bash
# State as data: a running job's keyed state, exported as an open Parquet /
# Iceberg dataset and queried with DuckDB / pyarrow - no bespoke state API.
#
# Flow:
#   1. run a keyed GROUP BY with checkpointing (the aggregate keeps per-user
#      state, keyed by user id),
#   2. export that keyed state as a Parquet file AND an Iceberg snapshot,
#   3. run SQL over the state in-process with `clink state-query`,
#   4. run the same analytics from outside the engine with DuckDB / pyarrow
#      (analyze.py): join the state's keys to a reference table, and inspect
#      key-group skew.
#
# Needs a clink built with the SQL frontend:
#   cmake -S . -B build -DCLINK_BUILD_SQL=ON && cmake --build build --target clink
# Point CLINK at it (defaults to ./build/clink or clink on PATH), then:
#   bash docs/consumer-examples/state_as_data/run.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK="${CLINK:-$([ -x ./build/clink ] && echo ./build/clink || command -v clink || echo clink)}"
DIR="$(mktemp -d)"
trap 'rm -rf "$DIR"' EXIT
echo "clink: $CLINK"
echo "work dir: $DIR"

# 1. Input: 200k events across 100 users.
python3 - "$DIR/events.ndjson" <<'PY'
import json, sys
with open(sys.argv[1], "w") as f:
    for i in range(200_000):
        f.write(json.dumps({"user_id": i % 100, "amount": i}) + "\n")
PY

# 2. Run a keyed GROUP BY with checkpointing. The aggregate operator keeps one
#    state entry per user id; a checkpoint persists it as an Arrow snapshot.
CKPT="$DIR/ckpt"
mkdir -p "$CKPT"
echo "== running the job (keyed SUM, checkpointing) =="
RUNLOG="$DIR/run.log"
"$CLINK" run --checkpoint-dir="$CKPT" --checkpoint-interval-ms=100 -e "
  CREATE TABLE events (user_id BIGINT, amount BIGINT)
    WITH (connector='file', format='json', path='$DIR/events.ndjson');
  CREATE TABLE out (user_id BIGINT, total BIGINT) WITH (connector='blackhole');
  INSERT INTO out SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id
" 2>"$RUNLOG"
# The final checkpoint id is logged; fall back to the highest checkpoint-N.snap.
ID="$(grep -oE 'final_id=[0-9]+' "$RUNLOG" | grep -oE '[0-9]+' | tail -1 || true)"
if [ -z "${ID:-}" ]; then
    ID="$(find "$CKPT" -name 'checkpoint-*.snap' | grep -oE 'checkpoint-[0-9]+' \
          | grep -oE '[0-9]+' | sort -n | tail -1)"
fi
echo "checkpoint id: $ID"

# 3. Export the keyed state as open datasets.
echo "== exporting state as Parquet =="
"$CLINK" state-export --dir="$CKPT" --id="$ID" --out="$DIR/state.parquet" --format=parquet
echo "== exporting state as an Iceberg snapshot =="
mkdir -p "$DIR/warehouse"
if "$CLINK" state-export --dir="$CKPT" --id="$ID" --format=iceberg \
        --warehouse="$DIR/warehouse" --table=user_state 2>"$DIR/ice.log"; then
    find "$DIR/warehouse" -name '*.metadata.json' | tail -1 | sed 's#^#  iceberg metadata: #'
else
    echo "  (iceberg export needs an iceberg-linked build; skipping) - $(tail -1 "$DIR/ice.log")"
fi

# 4. SQL over the state, in-process - the same engine, no export needed.
echo "== clink state-query: keyed-entry count per slot =="
"$CLINK" state-query --dir="$CKPT" --id="$ID" \
    --sql="SELECT slot, COUNT(*) AS keys FROM state GROUP BY slot" 2>/dev/null

# 5. The same state, analysed from outside with DuckDB / pyarrow.
echo "== DuckDB / pyarrow over the exported Parquet =="
python3 "$HERE/analyze.py" "$DIR/state.parquet"
