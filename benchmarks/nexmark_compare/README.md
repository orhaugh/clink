# nexmark_compare - clink vs Flink on Nexmark (apples-to-apples)

A cross-engine Nexmark throughput comparison. Both engines run the SAME query over
ONE pre-generated Nexmark dataset read from identical Kafka topics, under a written
matched premise, measured the same way. Sibling of `benchmarks/flink_compare`
(which compares a single keyed-window pipeline); this reuses its skeleton and its
honesty discipline for the Nexmark workload.

Read `pipeline.md` FIRST. It pins every dimension a quoted number depends on
(topology, shared input, format, durability, watermark, parallelism, measurement,
correctness gate, and the only honest headline). A number printed without that
premise holding is not apples-to-apples.

## Why this exists

clink's own Nexmark bench (`benchmarks/clink_nexmark_bench`) is clink-only:
in-process, single TaskManager, logical-stream rate, with a steady-state mode for
clink-vs-clink tracking. It is NOT comparable to another engine. The only existing
cross-engine number in this repo (`flink_compare`) is a different, single-pipeline
workload. This harness produces the first defensible clink-vs-Flink Nexmark number.

## Headline scope

SQL-vs-SQL on {q0 stateless, q5 windowed-agg, q8 windowed-join}; q6 reported
separately as clink-SQL-vs-Flink-DataStream (excluded from the geomean); q13/q14/q10
out of v1. See `pipeline.md` for the rationale.

## Run it

One command (Docker + the `../flink_compare/.venv` Python venv + Maven):

```bash
./run.sh                  # q0 + q12, 500k events, then tears down
EVENTS=1000000 ./run.sh   # more events
QUERIES="q0" ./run.sh     # a subset
KEEP_UP=1 ./run.sh        # leave Kafka + Flink up afterwards
```

It builds both engines, brings up Kafka + Flink, generates ONE canonical dataset,
loads it to a single-partition `nx-bid` both engines read, runs each query on both
at parallelism 1, measures steady-state by broker append-time, gates on identical
output-row counts (a mismatch HALTS that query, no ratio), and prints the
scoreboard under the matched-premise banner.

## Build increments (all done for the v1 deliverable)

- [x] **INC 0** - premise doc (`pipeline.md`) + scaffold.
- [x] **INC 2** - shared-input producer (`nexmark_dump`): one canonical dataset,
  deterministic (byte-identical on replay), 1:3:46 ratio.
- [x] **INC 3** - clink Nexmark Kafka SQL job (also fixed the SQL Kafka
  source/sink runtime wiring).
- [x] **INC 4** - Flink Nexmark SQL job on the pinned Flink 2.2.0 image
  (Table-API jar, Kafka SQL connector shaded in).
- [x] **INC 8** - one-command `run.sh` + `scoreboard.py` (steady-state table +
  geomean + banner + correctness gate).
- [ ] q8 (windowed join), q6 (SQL-vs-DataStream) - more queries.
- [ ] CPU normalisation (Cores*Time); clink SQL parallelism flag + the
  start-of-stream partition-watermark refinement for parallelism>1 runs.

## Results (parallelism 1, 1-partition, hot-path)

Apples-to-apples, both engines reading the same pre-generated 1-partition Kafka
topic (JSON), hot-path durability (checkpoints off, in-memory state), steady-state
measured identically from each output record's broker append timestamp
(`message.timestamp.type=LogAppendTime`) over the middle 80%, both gate-verified
(identical output-row counts -> same relation):

A representative `./run.sh` (500k events, tps=1000), both gate-passing:

| query | class | clink steady | Flink 2.2.0 steady | ratio | gate |
|---|---|---|---|---|---|
| q0 | stateless pass-through | 593,546 ev/s | 197,955 ev/s | 3.00x | 460,000 ✓ |
| q12 | windowed aggregate (10s tumbling per-bidder COUNT) | 388,981 panes/s | 122,260 panes/s | 3.18x | 184,767 ✓ |
| | | | | **geomean 3.09x** | |

clink leads on both (run-to-run the ratios sit around 2.7-3.2x; absolute rates
vary with machine warmth). On q0 it wins despite paying the heavier `JsonValue`
row-materialisation cost the JSON-input premise disclosed (which favours Flink on
stateless queries). q12's metric is steady output-pane rate (the same metric on
both engines, so the ratio is fair).

Caveats (so these are not yet the full `pipeline.md` headline): parallelism is 1
on BOTH (matched, the cleanest per-core comparison; **1-partition input topics**
are the correct config at parallelism 1 - see the multi-partition note below).
No CPU normalisation yet (events/sec, not events/sec/core). Two of the three
headline classes so far (stateless, windowed-agg); windowed-join (q8) next.

## A real clink bug the gate caught: multi-partition watermarks

The correctness gate first failed q12 (per-bidder COUNT per 10s tumbling window):
clink emitted ~276k panes vs the data-true 184,767. That gate-fail is the harness
earning its keep - it blocked a meaningless "clink 6.7x" that compared different
relations. The root cause, established by a controlled partition-count matrix
(same ordered data, parallelism 1, partition counts verified):

| input | clink q12 panes | correct? |
|---|---|---|
| 1 partition (ordered) | 184,767 | yes |
| 4 partitions, single-producer (contiguous ranges) | 184,767 | yes |
| 4 partitions, **keyed by auction** (each partition spans the full time range) | 274,608 | **no** |

So the bug is clink's **single global watermark over a multi-partition Kafka
source**. clink's Kafka source emits no per-partition watermarks; one downstream
`assign_timestamps` computes one global max-seen watermark over the merged,
interleaved stream. When data is key-distributed across partitions (each
partition spans the whole time range), one subtask reading all partitions sees a
badly out-of-order stream, the global watermark races to the fastest partition's
time, windows finalise before slower partitions deliver their in-window records,
and late folds re-create + re-fire windows (over-emit) while stranded bids miss
the canonical pane (under-count). This is exactly Flink's per-split watermark
case: Flink tracks a watermark per Kafka partition and emits the min across them,
so no in-window record is falsely late.

(An earlier intermediate conclusion in this investigation - "1-partition also
fails, so it's the keyed path not the partitions" - was wrong: that "1-partition"
topic had silently been recreated at the default 4 partitions by a Kafka
auto-create race. Verified partition counts settle it: 1-partition is correct.)

`WindowRowOp` itself is correct (an ordered bounded file source is byte-exact:
190,729 panes, sum 460,000). The defect is purely the multi-partition watermark.

### The fix (commit `34819d4`)

Per-partition watermarks, the Flink per-split + min-across-splits model:
`Record` carries an engine-only `source_partition` (set by the Kafka source,
preserved through the `json_string_to_row` map), and a new
`PartitionAwareBoundedOutOfOrdernessStrategy` tracks max event-time PER partition
and emits `watermark = (min over partitions) - bound`, so the watermark advances
only as fast as the slowest partition. Records with no partition (file /
generator) fold into one global bucket, so those sources are unchanged
(core 1379/1379 + SQL 546/546 green).

Result: the non-deterministic race is **eliminated** (4-partition keyed q12 is
now deterministic), and single-partition is **exact** (184,767). So the
parallelism-1 benchmark (single-partition topics) is fully correct - the q0/q12
ratios above stand.

**Known residual (~1.4%, multi-partition only):** at start-of-stream, before
every partition has delivered its first record, the min is taken over the
partitions seen so far, so the earliest window can fire slightly early
(4-partition keyed q12: 187,432 vs 184,767). Flink blocks the watermark until
every split has data (an empty split sits at -inf); replicating that needs the
source to convey its live partition assignment to the assigner so unseen
partitions block. That is the remaining step for **parallelism>1 gate-exact**
runs; it does not affect the parallelism-1 ratios.

Remaining: the start-of-stream partition-blocking refinement (gates par>1
gate-exactness); q8 (windowed join), q6 (SQL-vs-DataStream); CPU normalisation;
the clink SQL parallelism flag; and a reproducible run.sh + scoreboard.

## Producer (INC 2)

```bash
cmake -S . -B build -DCLINK_BUILD_BENCH=ON -DCLINK_BUILD_SQL=ON
cmake --build build --target nexmark_dump -j

./build/benchmarks/nexmark_dump --events 1000000 --out-dir /tmp/nx
# writes /tmp/nx/{person,auction,bid}.ndjson, one JSON object per line,
# the same shape clink's nexmark_source emits and Flink's JSON format decodes.
```

Determinism + ratio are checked by running it twice and diffing, and counting
per-type lines (≈ 1:3:46). The Kafka load (INC 3) ships the NDJSON to the three
topics via the driver venv (`confluent_kafka`).

## Toolchain

Docker (Kafka + Flink containers), a Python driver venv (`confluent_kafka`), Maven
for the Flink job. The Flink image is pinned in `docker-compose.yml`. Reused from
`flink_compare`.
