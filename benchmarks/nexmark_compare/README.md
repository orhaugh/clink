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

## Build increments (each independently verifiable)

- [x] **INC 0** - premise doc (`pipeline.md`) + scaffold. The contract, written
  first.
- [x] **INC 2 (core)** - shared-input producer: `nexmark_dump` runs clink's
  `NexmarkGenerator` ONCE (all types) and writes `nx-person/auction/bid` NDJSON,
  the same JSON both engines decode. Verified Docker-free for determinism
  (byte-identical on replay) and the 1:3:46 ratio. Kafka load is the next step.
- [ ] **INC 3** - clink Nexmark Kafka SQL job: re-point the existing query SQL onto
  `connector='kafka'` over the shared topics; clustered submit; count + steady-state
  via the consumer. q0 first.
- [ ] **INC 4** - Flink Nexmark SQL job (q0 canary): extend the pom with
  flink-table + flink-sql-connector-kafka; run the upstream nexmark-flink q0 SQL on
  the pinned Flink image reading the same topics. First real ratio. RISK: upstream
  nexmark-flink SQL targeted Flink 1.x; 2.2.0 compatibility is unverified - q0 is
  the canary.
- [ ] **INC 5/6** - q5 (windowed agg), q8 (windowed join), each gated on per-query
  output-row agreement.
- [ ] **INC 7** - q6 as SQL-vs-DataStream, its own row.
- [ ] **INC 8** - scoreboard: per-query Time/Cores/Cores*Time/ratio + SQL-only
  geomean + banner; optional durable-mode matrix.

## Results (parallelism 1, 1-partition, hot-path)

Apples-to-apples, both engines reading the same pre-generated 1-partition Kafka
topic (JSON), hot-path durability (checkpoints off, in-memory state), steady-state
measured identically from each output record's broker append timestamp
(`message.timestamp.type=LogAppendTime`) over the middle 80%, both gate-verified
(identical output-row counts -> same relation):

| query | class | clink steady | Flink 2.2.0 steady | ratio | gate |
|---|---|---|---|---|---|
| q0 | stateless pass-through | ~497,000 ev/s | ~170,000 ev/s | 2.9x | 460,000 ✓ |
| q12 | windowed aggregate (10s tumbling per-bidder COUNT) | ~262,000 panes/s | ~96,000 panes/s | 2.7x | 184,767 ✓ |

clink leads on both, and on q0 it wins despite paying the heavier `JsonValue`
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

What this means here:
- **Parallelism-1 benchmark: use 1-partition input topics** (the correct config
  at parallelism 1 - one subtask reads one ordered partition). q12 is then exact
  on both engines, which is how the q12 ratio above was obtained.
- **The engine fix = per-partition (per-split) watermarks in the Kafka source,
  emitting the min across the subtask's partitions, with idle-partition
  exclusion** (Flink's behaviour). It is real, now precisely root-caused with a
  clean reproduction (4-partition keyed) and control (1-partition), but it is a
  multi-day ABI-touching change (event time is a parsed JSON column, so the
  partition must be threaded from the source through the `json_string_to_row`
  bridge into a partition-aware watermark strategy). It is the prerequisite for
  **parallelism>1** scaled runs (which need multiple partitions). The
  late-record-drop guard (`allowed_lateness=0`) is NOT the fix - it would worsen
  the under-count, since the records are not genuinely late, the global watermark
  is just wrong.

Remaining: the per-partition-watermark engine fix (gates parallelism>1); q8
(windowed join), q6 (SQL-vs-DataStream); CPU normalisation; the clink SQL
parallelism flag; and a reproducible run.sh + scoreboard.

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
