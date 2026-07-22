# nexmark_compare - clink vs Flink on Nexmark (apples-to-apples)

A cross-engine Nexmark throughput comparison. Both engines run the same query over
one pre-generated Nexmark dataset read from identical Kafka topics, under a written
matched premise, measured the same way. Sibling of `benchmarks/flink_compare`
(which compares a single keyed-window pipeline); this reuses its skeleton and its
honesty discipline for the Nexmark workload.

Read `pipeline.md` FIRST. It pins every dimension a quoted number depends on
(topology, shared input, format, durability, watermark, parallelism, measurement,
correctness gate, and the only honest headline). A number printed without that
premise holding is not apples-to-apples.

## Why this exists

clink's own Nexmark bench (`benchmarks/clink_nexmark_bench`) is clink-only:
in-process, single Worker, logical-stream rate, with a steady-state mode for
clink-vs-clink tracking. It is not comparable to another engine. The only existing
cross-engine number in this repo (`flink_compare`) is a different, single-pipeline
workload. This harness produces the first defensible clink-vs-Flink Nexmark number.

## Headline scope

Implemented: q0 (stateless) and q12 (windowed-agg) as gated SQL-vs-SQL throughput
ratios (geomean); q8 (windowed stream-stream join) as a gated correctness result
(row count, throughput indicative); q6 as a SQL-only capability (clink runs it in
SQL; the Flink reference implementation ships q6 only as a DataStream program, so
it is documented, not gated). q13/q14/q10
out of v1. See `pipeline.md` for the rationale.

## Run it

One command (Docker + the `../flink_compare/.venv` Python venv + Maven):

```bash
./run.sh                  # q0 + q12, par 1, 500k events, then tears down
EVENTS=1000000 ./run.sh   # more events
QUERIES="q0" ./run.sh     # a subset
PARALLELISM=4 ./run.sh    # par 4: 4-partition topics, both engines at -p 4
KEEP_UP=1 ./run.sh        # leave Kafka + Flink up afterwards
```

It builds both engines, brings up Kafka + Flink, generates one canonical dataset,
loads it to PAR-partition topics both engines read, runs each query on both at
parallelism PAR, measures steady-state by broker append-time, gates on identical
output-row counts (a mismatch halts that query, no ratio), and prints the
scoreboard under the matched-premise banner. The results table below is the
default par=1; see the Parallelism>1 section for the par=4 run.

`./latency.sh` is the second axis: per-record end-to-end latency under a paced
sustained load (see the Latency section below).

## What the harness covers

- **Premise doc** (`pipeline.md`): pins every dimension a quoted number
  depends on.
- **Shared-input producer** (`nexmark_dump`): one canonical dataset,
  deterministic (byte-identical on replay), 1:3:46 person:auction:bid ratio.
- **Both engines as SQL jobs**: clink over Kafka SQL, and the Flink Nexmark
  SQL job on the pinned Flink 2.2.0 image (Table-API jar, Kafka SQL connector
  shaded in).
- **One-command run**: `run.sh` + `scoreboard.py` (steady-state table +
  geomean + banner + correctness gate).
- **q8** (windowed stream-stream join): gate PASS (1,056 = 1,056), the
  two-source-windowed-join-over-Kafka correctness milestone.
- **q6** (SQL-only capability): clink runs it in SQL over Kafka (5,223
  sellers); the Flink reference suite has no SQL form for it. Documented, not
  gated.
- **CPU normalisation** (Cores*Time): measured CPU per query (clink host
  procs via `ps`, Flink containers via cgroup v2 `cpu.stat`); the scoreboard
  adds an events-per-CPU-second efficiency column (parallelism-independent),
  with the baseline-overhead caveat below.
- **Parallelism > 1**: `clink_submit_sql --parallelism N` / `PARALLELISM=N
  ./run.sh`; verified gate-exact at par=4 over 4-partition topics on both
  engines (q0 460,000, q12 184,767). Single-box, so it shows scale-out
  correctness, not true distributed throughput scaling. Building this
  surfaced and fixed a real clink bug: the distributed two-input (Row,Row)
  co-operator mis-partitioned its input bridges at par>1 (assumed exactly 2);
  `SubtaskEdge.input_index` now groups each side's N bridges correctly.
- **Latency axis** (`latency.sh`): per-record end-to-end latency under
  sustained paced load, gated on count + positional content + pacer rate. See
  the Latency section below and the "Latency axis" premise in `pipeline.md`.

Not covered: a true multi-machine distributed run (out of scope on one host).

## Results (parallelism 1, 1-partition, hot-path)

Apples-to-apples, both engines reading the same pre-generated 1-partition Kafka
topic (JSON), hot-path durability (checkpoints off, in-memory state), steady-state
measured identically from each output record's broker append timestamp
(`message.timestamp.type=LogAppendTime`) over the middle 80%, both gate-verified
(identical output-row counts -> same relation):

A representative `./run.sh` (500k events, tps=1000), all gate-passing, with clink
built **fully optimised** (`Release` `-O3 -DNDEBUG` + LTO, via
`BUILD_DIR=.../build-release`; see the `BUILD_DIR` note in `run.sh`) against
Flink's production JVM. Two metrics: **rate** = steady wall-clock throughput,
**eff** = input events per measured CPU-second (Cores*Time normalised,
parallelism-independent):

par=1:

| query | class | rate (clink/flink) | eff (clink/flink ev per CPU-s) | gate (rows) |
|---|---|---|---|---|
| q0 | stateless pass-through | 452k / 211k = **2.14x** | 192k / 13k = 14.8x | 460,000 ✓ |
| q12 | windowed aggregate (10s tumbling per-bidder COUNT) | 278k / 87k = **3.18x** | 195k / 13k = 15.0x | 184,767 ✓ |
| | | **rate geomean 2.61x** | **eff geomean 14.9x** | |

par=4 (clink scales with parallelism; Flink stays flat on the single box):

| query | rate (clink/flink) | eff (clink/flink ev per CPU-s) | gate (rows) |
|---|---|---|---|
| q0 | 748k / 113k = **6.61x** | 171k / 12k = 14.0x | 460,000 ✓ |
| q12 | 431k / 114k = **3.78x** | 142k / 12k = 12.3x | 184,767 ✓ |
| | **rate geomean 5.00x** | **eff geomean 13.1x** | |

clink is the clear winner at every point, gate-exact, and **scales with
parallelism** (q0 452k->748k from par=1 to par=4) while Flink stays flat
(~110-210k/s). Two honest reads of the numbers:

- The CPU-efficiency **eff (~13-15x)** is a *total-CPU-footprint* number: real
  (native clink vs a JVM Flink cluster) but it INCLUDES each engine's
  baseline/runtime overhead (Flink's JVM GC/JIT/heartbeat/idle-thread CPU is much
  heavier than clink's), so read it as "total CPU to run the workload," not "kernel
  is 14x faster." This is the cleanest *optimisation-sensitive* signal.
- The wall-clock **rate (~2.6x par=1, ~5x par=4)** is run-to-run noisy and, for q0,
  partly bound by the single shared Kafka broker (a pass-through writes every input
  row back). So `-O3`+LTO moved eff more than the headline rate - the rate ceiling
  is partly the broker, not the engine. The ratio is fair (same metric, both
  engines, gate-identical output).

(The earlier figures here were from a `-O2 RelWithDebInfo` clink and were therefore
conservative; these replace them with the fully-optimised build.)

**q8** (windowed stream-stream join) remains a gate-PASS correctness milestone,
indicative-only on throughput and excluded from the geomeans (its rate is
output-burst-bound, its eff baseline-bound) - see below.

**q8 is a correctness milestone, not a throughput data point.** It proves clink's
two-source windowed join over Kafka (person-window aggregate JOIN auction-window
aggregate on `id=seller` + same window) computes the same relation as Flink -
both emit exactly 1,056 rows (the data-derived count of distinct persons created
in a window who also sold an auction in that window, over the 49 watermark-closed
windows). Its throughput rate is **indicative only and excluded from the
geomean**: q8 emits few rows relative to input (1,056 vs ~40k person+auction
events), so the output-row-append rate measures emission-burst dynamics, not
processing throughput (it swings run-to-run, e.g. 3-10x, while the 1,056=1,056
gate is exact). The geomean is over the two throughput-comparable queries
(q0, q12) where output is input-scale.

**q6 is a SQL-only capability, not a measured comparison.** q6 (average selling
price per seller over their last 10 closed auctions) is the query Flink itself
does not express in SQL - its `OVER` operator does not consume retractions, so the
canonical Nexmark q6 ships only via Flink's DataStream API. clink runs it in SQL
(`queries/clink/q6.tmpl.sql`): winning bid per auction (bid INNER JOIN auction +
interval residual + ROW_NUMBER top-1) feeding a last-10-per-seller `AVG ... OVER
(... ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)`, which lowers to clink's
`last_n_agg` operator (consumes the winning-bid changelog, re-emits the per-seller
avg as the last-10 set slides). Verified over Kafka: 30,845 changelog records
netting to a final upsert state of 5,223 distinct sellers. It is not in the gated
`run.sh` suite - there is no Flink SQL counterpart to gate against, and its
changelog output (vs a clean append/pane count) does not fit the row-count gate; a
faithful gate-matched Flink DataStream job was judged disproportionate effort for
a fragile final-state gate. q6 stands as the demonstration that clink covers in
SQL a query class the Flink reference suite expresses only as a DataStream
program.

Caveats (so these are not yet the full `pipeline.md` headline): parallelism is 1
on both (matched, the cleanest per-core comparison; **1-partition input topics**
are the correct config at parallelism 1 - see the multi-partition note below).
CPU is now measured (the eff column), but the eff ratio is a total-CPU-footprint
number incl. each engine's baseline overhead, not a compute-kernel ratio (see the
results note). The throughput geomean covers the two input-scale queries (q0
stateless, q12 windowed-agg); q8
(windowed-join) is a correctness gate; q6 is a SQL-only capability.

## Latency (the second measured axis)

Throughput asks how fast an engine drains a backlog; the latency axis asks how
long one record takes under a load both engines handle comfortably, and what the
tail looks like. Premise: `pipeline.md`, "Latency axis". One command:

```bash
./latency.sh                                     # 3.68M bids paced at 50k ev/s
RATE=100000 EVENTS=8000000 ./latency.sh          # heavier point
BUILD_DIR=$PWD/../../build-release ./latency.sh  # explicit clink build
```

Mechanics: the engine's q0 job (same relation as the gated throughput q0) is
deployed against an EMPTY 1-partition LogAppendTime topic, then the same
deterministic `bid.ndjson` is replayed at a paced wall-clock rate; per-record
latency = output broker-append minus input broker-append (one broker clock, ms
resolution). The positional in/out join is verified by a per-record content
check, producer linger is pinned to 0 on both sinks (the clink Kafka sink
gained a `linger_ms` option for this), and the run gates on exact count, zero
content mismatches, and the pacer hitting its target within 5%. Warm-up and
drain are trimmed by TIME (`--warmup-s`, default 10 s of wall clock, with the
20% head fraction as a floor - a fixed fraction leaves warm-up records inside
the steady window at high paced rates) plus a 5% tail fraction, with a
per-5s-bucket p50/p99 series in the result JSON so the trim is inspectable
rather than trusted.

### Results (par=1, 50k ev/s, 3.68M bids, hot path, Release+LTO clink vs Flink 2.2.0)

All gates passed on both engines (3,680,000 = 3,680,000, zero mismatches,
pace within 0.01%), same run, same conditions:

| engine | p50 | p90 | p99 | p99.9 | max | mean |
|---|---|---|---|---|---|---|
| clink | 2 ms | 7 ms | **8 ms** | **18 ms** | **32 ms** | 2.82 ms |
| flink | 1 ms | 2 ms | 51 ms | 198 ms | 220 ms | 2.82 ms |

The medians sit at the millisecond-resolution floor on both engines and the
means are identical; the distributions part company in the tail: clink's
p99.9 is ~11x lower, and its per-5s bucket series is ruler-flat (p99 = 8 ms
in every steady bucket) where Flink's shows a JIT warm-up decay over the
first ~25 s (absorbed by the head trim) and GC-class spikes inside the steady
window. Flink's tail also swings run to run - p99.9 was 77 ms in one
full-length run and 198 ms in another under identical conditions - which is
itself the finding: the tail is scheduled by the garbage collector, not by
load. clink's post-fix tail reproduced flat across runs. For a latency SLO
(the p99.9 you can promise), the gap is structural: no JVM, no warm-up phase,
no pause-class spikes.

### Revalidation caveat (2026-07-22): the tail is rig-sensitive

A three-rate sweep (50k/100k/150k ev/s, 7.36M bids each, RelWithDebInfo
clink) on a heavily-used single box could NOT reproduce the attribution
above: sporadic 100-900 ms bucket spikes landed on BOTH engines, in
different runs, non-reproducibly (Flink's 50k p99 was 16 ms in one sweep and
392 ms in the next; clink caught 932/1101 ms buckets of its own). On a
shared box the Docker-VM broker's stalls pollute the append timestamps that
ARE the measurement, so p99+ comparisons are not attributable to engines
there. What DID hold across every run, rate, and sweep: the quiet-bucket
steady p99 (clink ~8-10 ms, Flink ~4-8 ms - comparable) and a stable
clink p50/p90 floor of ~3 / 7-8 ms. An earlier reading of a
"load-correlated clink p90" (rising to ~20 ms) pointing at source batch
sizing was REFUTED by an interleaved A/B at 100k ev/s: shrinking the
source batches (`max_batch_size='32', batch_max_wait_ms='1'`, both now
SQL-reachable WITH options) made every percentile worse both times
(p99 33/43 ms default vs 109/732 ms tuned - more batches means more
per-batch overhead downstream and less headroom), and repeated default
runs pinned p90 at 7-8 ms at 100k. The defaults (256 records / 5 ms
fill bound) are latency-sound; the elevated-p90 reading was the same
shared-box contamination. Quote the table above only after reproducing
it on a quiet or dedicated rig; do not quote p99.9 ratios from
shared-box runs in either direction.

### The bench earned its keep on day one: the batch-fill defect it caught

The FIRST gated run told a different story: clink p50/p99/p99.9/max was
4/30/67/84 ms - behind Flink at the median. The per-bucket series and the
25k ev/s smoke run localised the cause precisely: clink's Kafka source filled
a fixed 256-record batch before emitting downstream (the fill loop broke only
on a 100 ms quiet timeout), so every record paid ~`256/rate` seconds of batch
formation (~5 ms at 50k ev/s, ~10 ms at 25k ev/s - the measured p50s), and
early records rode hostage in the clump, coupling the tail to arrival jitter.

The fix is `batch_max_wait` (`KafkaSource::Options`, default 5 ms, WITH-option
`batch_max_wait_ms`): once a batch has begun, the fill loop stops when the
bound elapses and emits a partial batch. Idle stays cheap (the first record
still blocks up to `poll_timeout`), and a saturated consumer queue fills 256
records well inside the bound, so the throughput ceiling is untouched: the
post-fix gated q0 run measured 606k ev/s steady (documented pre-fix figure
452k; rate is run-to-run noisy and partly broker-bound, so read this as "no
regression", not "the fix sped it up"). Post-fix, the whole clink latency
distribution collapsed: 4/30/67/84 -> 2/8/18/32 ms. That before/after is
exactly what this harness exists to measure.

Caveats: ms timestamp resolution (sub-ms differences do not resolve; p50
1 vs 2 is not a meaningful gap), single box, par=1 v1 (the positional join
needs correlation-id injection before par>1 latency is measurable), and short
runs overstate Flink's tail (JIT dominates a thin steady window) - quote only
full-length gated runs.

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

**Residual (~1.4%) is confined to par=1 reading many partitions:** when one
subtask reads 4 interleaved partitions, at start-of-stream (before every
partition has delivered its first record) the min is over the seen subset, so the
earliest window can fire slightly early (par=1 over 4-partition keyed q12:
187,432 vs 184,767). This does not occur at par>1 (next section): there each
subtask reads one ordered partition, so there is no within-subtask interleave -
the cross-partition min happens at the keyed shuffle, which the runtime does
exactly. So the residual is a par=1-multi-partition edge case, not a correctness
limit on scaled runs.

## Parallelism > 1

`clink_submit_sql --parallelism N` (and `PARALLELISM=N ./run.sh`) fans every op
out to N subtasks: keyed ops hash-partition by key, and a Kafka source's N
subtasks share a consumer group so Kafka assigns them disjoint partitions (N
partitions / N subtasks -> one ordered partition each). The runtime min-merges
each source subtask's watermark across the keyed shuffle into the window op.

Verified at PARALLELISM=4 over 4-partition topics, both engines **gate-exact**:

| query | par | clink rate | flink rate | rate | gate |
|---|---|---|---|---|---|
| q0 | 4 | 776,369/s | 196,370/s | 3.95x | 460,000 ✓ |
| q12 | 4 | 363,176/s | 119,880/s | 3.03x | 184,767 ✓ |

So clink's windowed SQL is correct when distributed (the watermark-at-scale path
works). Honest reading of the THROUGHPUT: this is a SINGLE box, so par>1 measures
scale-out coordination overhead, not true distributed scaling. clink's stateless
q0 does scale (776k at par 4 vs ~575k at par 1 - parallel JSON decode); the
windowed q12 stays flat (~360-400k, shuffle / single-broker bound). Flink stays
flat-to-lower (heavier per-subtask JVM cost under single-box contention). The eff
(CPU) ratio narrows at par 4 (clink spends more total CPU across 4 subtasks +
shuffle for the same input - parallelism trades CPU for wall-time). A real
multi-machine cluster is what would show linear scale-out; that is out of scope
for this single-host harness.

### q8 / q6 at par>1: a real engine bug, found AND fixed (distributed equi-join)

Pushing q8 (two-source windowed join) and q6 (interval join) to par>1 surfaced -
and led to fixing - a genuine clink bug in the distributed two-input join:

1. **Slots** (fixed): clink needs one slot per subtask (no Flink-style slot
   sharing), so q8's ~9 ops at par=4 = 36 subtasks exceeded the old 32-slot pool
   and the job did not deploy. `run.sh` now scales slots-per-worker with `PAR`.

2. **Distributed co-operator input mis-partitioning** (fixed): the stream-stream
   equi/interval join is a two-input co-operator whose In1 and In2 share a channel
   type (Row, Row). The worker runner split its input bridges into the two sides by
   ordinal, hard-assuming exactly 2 (the par=1 case: one left + one right bridge).
   At par>1 each side contributes one bridge per upstream subtask (par=4 -> 4 left
   + 4 right = 8 bridges), so the split threw / mis-assigned and the join saw no
   co-located pairs -> 0 output. Fix: `SubtaskEdge` now carries an `input_index`
   (which logical input the edge feeds), the planner stamps it per input-ref
   ordinal, and the same-type co-op runner groups bridges by it - so each side's
   N bridges are collected correctly at any parallelism. (The keys were always
   aligned - `id=5` and `seller=5` hash identically - and `EquiJoinRowOp` emits on
   arrival; only the side-grouping was wrong.)

Verified at PARALLELISM=4 over 4-partition topics, both joins now correct:

- **q8** (windowed join): gate-exact, clink **1,056 = 1,056** Flink. (Rate/eff
  indicative-only as at par=1: tiny output, emission-bound.)
- **q6** (clink SQL only): **5,223 distinct sellers**, identical to par=1 - the
  interval join + last-N pipeline is correct when distributed.

So all four queries are now correct at par>1: q0 (stateless), q12 (windowed agg,
gate-exact), q8 (windowed join, gate-exact), q6 (interval join, matches par=1).
SQL suite 546/546 green (the equi-join at par=1 is unchanged).

## Distributed (containerized) verification + the throughput-measurement caveat

`verify_distributed.sh` runs clink as a real multi-container cluster (1 coordinator + 4 worker,
separate network namespaces, shuffle over container-to-container TCP, in-network
Kafka) and gate-checks q0/q12/q8 at par=4 - all gate-exact (460000 / 184767 /
1056), confirming correctness when distributed across containers, not just over
loopback. The runtime image (`docker/Dockerfile.runtime`) is Release + LTO +
stripped.

`throughput_containers.sh` runs both engines containerized for a throughput
comparison. It is built and gate-passes, but a throughput run at this scale
exposed a measurement-validity problem, and the honest conclusion is that the
rate numbers are not a clean sustained-throughput ratio:

- clink **burst-drains** the pre-loaded Kafka topic - the engine finishes q0 in
  ~0.1s and flushes output in a burst (460000 records, broker-append span 0.11s),
  so the append-time "rate" measures the sink flush, not sustained processing
  (it reads as an absurd ~50x and must not be quoted).
- Flink is **JVM-warmup-dominated** on a 500k-event job (wall ~28s, much of it
  warmup), understating its rate.
- **events/wall is consumer-capped**: the Python driver reads ~50-60k rec/s, which
  bounds the FAST engine, so clink's wall-rate (~3x) understates it.

The least-confounded signal is **CPU consumed**: for q0 clink used ~3.8 CPU-s vs
Flink's ~50.7 (a ~13x total-CPU-footprint gap), q12 ~6x - but even this is
inflated by Flink's JVM warmup/baseline over its longer run at this scale. So the
robust takeaways are (a) correctness holds containerized at par>1, and (b) clink
is materially more CPU-efficient, consistent with the par=1 host run (~3x wall,
~13x CPU-footprint). A precise SUSTAINED-throughput ratio needs engine-side
metrics sampling (each engine's records-out/sec mid-run) plus a much larger or
rate-limited input so warmup amortizes and no downstream consumer is the
bottleneck - which is exactly what `throughput_sampled.sh` does (next section).

## Engine-side metrics sampling (`throughput_sampled.sh`)

The methodology fix for the caveat above. Instead of timing broker append
(burst-fooled) or a downstream consumer (consumer-capped), it polls **each
engine's own records-processed counter** over time and reports the **drain rate**
(input events / time from first record to fully drained):

- clink: `GET /api/v1/jobs/<id>/operators`, max `records_in` across operators.
  The counter is fine-grained (~80ms), so the drain is measured first-record to
  last-record (excludes the deploy gap). clink's per-op counters are cumulative
  across job submissions on a persistent worker, so the sampler anchors a baseline at
  the first positive reading and measures the delta.
- Flink: `GET /jobs/<jid>`, max `read/write-records` across vertices. Flink's
  aggregated metrics lag the metric-fetcher interval (~10s) and arrive as a step,
  so wall-time is useless; instead the sampler uses Flink's own job clock
  (`duration`, ms since RUNNING) - `target / duration_at_completion` is immune to
  the fetch lag. Each Flink job is fresh, so its counts are absolute (no baseline).

This removed the bogus ~52x burst artifact. But the run surfaced a harder truth:

- **clink is stable and fast.** Across 5M/10M, fresh and warm hosts, its drain rate
  sits ~0.8-1.1M rec/s with a peak slope ~1.5-2.0M rec/s. Output is gate-identical
  (4.6M / 9.2M for q0) whenever both engines finish.
- **Flink is highly variable on this rig.** Same query, same input: ~215k rec/s
  (5M, warmup-deflated), ~442k rec/s (10M warm, warmup amortized - its most
  representative point), down to ~38k rec/s (10M cold JVM, did not even drain the
  full input inside the cap). That is a ~10x spread driven by JVM warmth and
  cold-start, plus the single shared Kafka broker becoming a write bottleneck for
  q0 (a passthrough that writes every input row back to Kafka - clink hits that
  ceiling sooner, which compresses the apparent ratio at volume).

**Honest conclusion: a precise single cross-engine ratio is not establishable on
this one-laptop rig.** clink is faster in every run (the warmup-fair 10M point is
~1.8x; smaller scales read higher only because Flink is warmup-deflated there) and
far more stable, with byte-identical output - but Flink's 10x variance and the
shared-broker sink dominate any headline number. A defensible figure needs:
separate hosts (not both clusters on one box), a warmed Flink (discard the first N
seconds), an isolated or rate-limited sink (remove the shared-Kafka ceiling), and
multiple trials. The sampler is the right instrument; the rig is the limit.

## Isolating the sink (`SINK=blackhole`)

`SINK=blackhole ./throughput_sampled.sh` swaps the Kafka output for a discard sink
(clink `connector='blackhole'`, Flink's built-in `blackhole`; the `q*_bh.tmpl.sql`
variants) so the engine's read+process rate is measured with NO output connector -
removing the shared single Kafka broker as a write ceiling. Output is discarded so
there is no row-count gate; the completeness check is that each engine's own
counter drained the full input (correctness is established separately by the
Kafka-sink gate). A larger input (10M) keeps the now-faster drain samplable.

A clean same-session A/B, q0 at 10M / par=4, both measured by each engine's own
counter (only the sink differs):

| sink | clink | Flink | ratio |
|---|---|---|---|
| kafka | 682k/s | 68k/s | 10.07x |
| blackhole | 881k/s | 426k/s | 2.07x |

The result is the opposite of the naive expectation, and it is the honest one. The
Kafka sink throttles both engines, but Flink far more so (426k -> 68k, ~6x)
versus clink **modestly** (881k -> 682k, ~1.3x) - because the shared single broker
contends on the output path and hits Flink's sink far harder. So removing the sink
SHRINKS the ratio (10x -> ~2x). Across runs, Flink's Kafka-sink rate swings wildly
(68k-442k) while its blackhole rate is stable (~426k, matching its good Kafka runs);
clink is steady throughout (682-881k).

**The sink-isolated, pure-engine ratio is ~2x on q0 and ~2.5x on q12 (clink 457k /
Flink 179k).** That is the most apples-to-apples engine-vs-engine number on this
rig: clink is a clear ~2-2.5x faster, and the larger 5-10x Kafka-sink ratios seen
elsewhere were substantially inflated by the shared broker throttling Flink's output
path, not by a 5-10x compute-kernel gap. (Containerized clink is itself ~8x slower
than the same binary run native - the Docker-Desktop VM tax on Mac - so even the
~2x is measured under a heavy shared overhead, not clink's native ceiling.)

Remaining: a true multi-machine (multi-host) run with the sampler; separate-host +
warmed-Flink trials to pin the pure-engine ratio tighter; the niche
par=1-reading-many-partitions residual. (q12 is Kafka-sink-comparable only at par=1:
at par>1 its output diverges because the multi-partition watermark refinement is not
yet on the SQL Kafka-source path - a documented gap, see the q12 template. The
blackhole path sidesteps the output gate, so it compares at any par.)

## Producer (`nexmark_dump`)

```bash
cmake -S . -B build -DCLINK_BUILD_BENCH=ON -DCLINK_BUILD_SQL=ON
cmake --build build --target nexmark_dump -j

./build/benchmarks/nexmark_dump --events 1000000 --out-dir /tmp/nx
# writes /tmp/nx/{person,auction,bid}.ndjson, one JSON object per line,
# the same shape clink's nexmark_source emits and Flink's JSON format decodes.
```

Determinism + ratio are checked by running it twice and diffing, and counting
per-type lines (≈ 1:3:46). The Kafka load step ships the NDJSON to the three
topics via the driver venv (`confluent_kafka`).

## Toolchain

Docker (Kafka + Flink containers), a Python driver venv (`confluent_kafka`), Maven
for the Flink job. The Flink image is pinned in `docker-compose.yml`. Reused from
`flink_compare`.
