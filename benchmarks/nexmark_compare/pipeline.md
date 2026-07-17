# Nexmark cross-engine comparison: the premise

This document is written FIRST, before the harness, and pins every dimension a
quoted clink-vs-Flink Nexmark number depends on. A number printed without this
premise holding is not an apples-to-apples comparison and must not be quoted.

The design and these clauses were grounded in the existing `benchmarks/flink_compare`
harness and an adversarial fairness review. The sibling `flink_compare` (a single
keyed-window-aggregate pipeline) is the template; this reuses its skeleton and its
honesty discipline for the Nexmark workload.

## What is being compared

clink SQL vs Flink SQL, both running the SAME Nexmark query over ONE pre-generated
Nexmark dataset read from identical Kafka topics, under a matched premise, measured
the same way. The headline is a small, defensible SQL-vs-SQL subset, not all 23
queries.

## Headline query set

| query | class | both in SQL? |
|---|---|---|
| q0 | stateless pass-through (source decode + wire + sink floor) | yes |
| q5 | windowed group-by (hot items) | yes |
| q8 | windowed stream-stream join (new users) | yes |

These three span the work-classes that dominate Nexmark cost (stateless,
windowed-aggregate, windowed-join) and stay auditable query-by-query.

Reported SEPARATELY, never folded into the SQL-vs-SQL geomean:
- **q6** (avg price per seller over their last 10 closed auctions): Flink has no
  SQL form (its `OVER` does not consume retractions, so Flink ships q6 only via the
  DataStream API); clink expresses it in SQL via `last_n_agg`. Run as
  clink-SQL-vs-Flink-DataStream, its own row, excluded from the geomean. This is
  the same exclusion the Feldera Nexmark writeup made.

Out of scope for v1 (no like-for-like SQL): q13/q14 (clink lookup-join + scalar
UDFs registered programmatically; need hand-written matched Flink UDFs first), q10
(IT-only, writes partition files, not a throughput query). The headline set can
grow query-by-query once the loop is proven, each gated on running unmodified on
the pinned Flink image.

## The matched premise (non-negotiable)

1. **Topology**: both clustered. clink runs `clink_node` coordinator + N workers as host
   processes over a real network; Flink runs a real coordinator/worker cluster. The in-process
   single-Worker clink number (`clink_nexmark_bench` steady-state mode) is
   clink-vs-clink regression tracking ONLY and must never be cross-quoted against a
   clustered Flink number.

2. **Shared input, generated once**: one canonical Nexmark dataset is generated
   ONCE by clink's deterministic `NexmarkGenerator` (fixed seed
   `0x9E3779B97F4A7C15`, the standard 1:3:46 Person:Auction:Bid ratio) and written
   to three Kafka topics `nx-person` / `nx-auction` / `nx-bid` BEFORE either engine
   starts. Each engine reads only its type's topic, so NEITHER re-derives the
   stream. This eliminates clink's per-table generator re-derivation (the
   `clink_nexmark_bench` instantiates the generator once per base table, physically
   generating ~3x for a 3-table query). Generation is off the timed path for both.

3. **Per-record work / format**: v1 uses JSON on the shared topics. clink reuses
   its existing `connector='kafka', format='json'` path
   (`kafka_source_string -> json_string_to_row`) verbatim; Flink uses
   `flink-sql-connector-kafka` with the JSON format. JSON is chosen over a binary
   layout deliberately: JSON decode parity is well-defined on both engines, whereas
   a hand-rolled binary layout is a parity-bug minefield (a subtle layout mismatch
   would either fail the correctness gate or, worse, pass it while doing different
   work). DISCLOSED ASYMMETRY: clink materialises a `JsonValue` row map per record
   where Flink builds a typed table row; this is a real per-record-work difference
   that slightly favours Flink on stateless queries. Therefore the stateless-query
   (q0) ratio is framed as a floor that INCLUDES clink's heavier row
   materialisation, not as a pure-engine number.

4. **Durability (HEADLINE = hot path)**: checkpointing OFF on both, in-memory state
   (Flink `state.backend.type=hashmap`, clink `--state-backend=memory`),
   at-least-once Kafka sink (Flink `DeliveryGuarantee.AT_LEAST_ONCE`, clink
   `acks='1'`). This compares the compute hot path and is the symmetric choice
   (Flink's checkpoint machinery is more mature, so turning checkpoints on would
   compare maturity, not design). A DURABLE config (RocksDB + matched checkpoint
   interval on both) may be run as a SEPARATE, clearly-labelled second matrix,
   never interleaved with the hot-path numbers. Backends are never crossed.

5. **Watermark**: identical out-of-orderness lag on both, set per query
   (clink DDL `watermark_lag_ms='X'`, Flink `WATERMARK FOR datetime AS datetime -
   INTERVAL 'X' ...`). The existing `clink_nexmark_bench` uses 4000ms; pin both
   engines to the same value and state it. (The existing `flink_compare` jobs use
   0; do not inherit that silently.)

6. **Parallelism**: matched per query. partition count == parallelism == subtask
   count == slots on both. Default 4; raised for the heavy join/double-window
   queries (q5/q8). Chaining policy identical (Flink chaining disabled for parity,
   as `flink_compare` does, OR enabled with the clink fusion equivalent; stated).
   Object reuse off on both. Stated in the scoreboard banner per query.

7. **Sink**: both write the same serialised rows to the same Kafka output topic
   `nx-out-<query>` (needed for row counting and steady-state tagging). No
   blackhole-vs-Kafka asymmetry.

8. **Denominator**: input Nexmark events generated once (the canonical logical
   stream), identical on both engines. Never per-table-multiplied.

## Measurement

- **Headline = warm-up-subtracted steady-state**, measured on the OUTPUT side so it
  is engine-agnostic. Each output record carries an ingest sequence derived from
  the input (the source `datetime`/event identity, carried through the projection).
  The consumer measures throughput over the steady interval `[warmup_n, N - tail_n]`
  of the output topic, discarding the warm-up prefix and the end-of-stream burst on
  BOTH engines identically. The warm-up prefix is large enough that Flink's JIT has
  compiled before the measured window opens. This anchors the clock to the first
  STEADY output record, so it excludes deploy, cluster bring-up and JVM warm-up.
- **CPU normalisation**: report Time(s), measured Cores (`/proc/<pid>/stat` summed
  over the clink coordinator+worker processes; cgroup `cpuacct` for the Flink coordinator+worker containers),
  and Cores*Time, as canonical Nexmark and Feldera do, NOT events/wall/nominal-slots.
  The sampling boundary (which processes/containers) is pinned in the banner.
- **Cold whole-job wall** (engine-start to last output record, the `flink_compare`
  anchor) is reported ALONGSIDE the steady number, clearly labelled, never as the
  headline.
- **Windowed queries (q5/q8)**: a bounded source fires all panes in one
  end-of-stream burst, so a between-positions steady interval measures the streaming
  body, not the window compute. Lower `--tps` so windows fire mid-stream, and report
  body-rate and window-fire-tail as SEPARATE numbers. Never quote one rate that
  conflates them.

## Correctness gate (anti-cheat)

- Per-query EXPECTED output-row count is computed from the pinned deterministic
  dataset, emitted by BOTH engines, and asserted EQUAL. A mismatch HALTS as a
  correctness failure and is never quoted as a perf delta. (This is the gate that
  caught a broken zero-pane no-op in prior cross-engine work.)
- Dataset determinism is proven first: the producer writes byte-identical topic
  contents on replay at a fixed seed, so EXPECTED is reproducible.
- Per-query verification on the pinned Flink image before a query enters the
  headline: any query that does not run unmodified is dropped or
  hand-reconciled-and-disclosed, never silently forced.
- The clink SQL must compute the SAME relation as the Flink SQL. Known clink
  idioms to reconcile per query: q12 substitutes an event-time analogue for
  proctime; q8's per-window grouping carries an unused `COUNT(*)`; some joins are a
  single equi-key plus a column-vs-column residual. If both compute the same
  relation the row counts match; if not, the gate halts.

## The only honest headline

> On a representative {q0 stateless, q5 windowed-agg, q8 windowed-join} SQL subset,
> reading one pre-generated Nexmark dataset from identical Kafka topics with matched
> decode work, hot-path durability (checkpoints off, in-memory state) and matched
> parallelism on both engines, clink's steady-state events/sec/core is X (geomean)
> relative to Flink <version> on <hardware>, with q6 reported separately as
> SQL-vs-DataStream and excluded from the geomean.

Any number printed without that full clause is the documented landmine and is
blocked. The outcome is unknown going in; the goal is a defensible number in either
direction, not a predetermined result.

## Latency axis (second measured axis)

Throughput above asks "how fast can the engine drain a backlog". The latency axis
asks a different question: "under a sustained load both engines can comfortably
handle, how long does one record take to get through, and what does the tail look
like". This section pins the latency premise the same way the sections above pin
throughput. A latency number printed without this premise holding is not quotable.

1. **Workload**: q0 only in v1 (the stateless pass-through), the SAME relation as
   the throughput q0. It measures the record path: source decode, engine wire,
   sink encode, Kafka produce. It deliberately has no windows, so nothing in the
   number is watermark- or trigger-timing; it is pure pipeline latency.

2. **Input discipline (differs from throughput by design)**: the same canonical
   deterministic `bid.ndjson` is REPLAYED at a paced wall-clock rate onto an EMPTY
   1-partition input topic created with `message.timestamp.type=LogAppendTime`,
   while the engine job is already deployed and polling. Each engine gets its own
   fresh input topic and its own replay of the same file, in the same order, at
   the same target rate, sequentially. The topic being empty at job start means
   deploy time creates no backlog; the engine meets each record at arrival.

3. **Load level**: the paced rate must sit well below BOTH engines' gated q0
   Kafka-sink throughput at the same parallelism (par=1: clink ~452k/s, Flink
   ~211k/s), so the number is latency-under-load, not saturation queueing. Default
   50,000 events/s (~24% of the slower engine's ceiling). The harness computes the
   achieved input rate from broker append times and HALTS if it missed the target
   by more than 5% (a lagging pacer invalidates the run).

4. **Definition**: latency of the record at position N = broker append time of
   output record N minus broker append time of input record N. Both topics are
   LogAppendTime on the SAME single broker, so both timestamps come from one clock
   and no host/producer clock skew can enter. Resolution is Kafka's millisecond
   timestamp granularity; sub-ms differences do not resolve, which is disclosed
   and acceptable because the axis targets tail effects (10ms-class and up).

5. **Positional join validity**: v1 is par=1, 1-partition in and out, and q0 is an
   order-preserving pass-through on both engines, so output position N corresponds
   to input position N. This is not assumed; it is verified: the count gate
   (output count == input count) plus a per-position content check (auction,
   price, datetime compared on EVERY record) must both pass, else the run HALTS
   with no number. par>1 latency needs correlation-id injection and is out of v1.

6. **Sink batching pinned**: the engine's output producer linger is 0ms on BOTH
   (clink `linger_ms='0'` - librdkafka's default is 5ms; Flink
   `properties.linger.ms='0'` - the Java default is already 0, set explicitly).
   Without this, clink pays up to 5ms of pure producer batching per record that
   Flink does not, and the number measures configuration, not engines. All other
   producer/consumer settings stay at each engine's defaults (librdkafka vs Java
   client), which is the engines-as-shipped premise; the defaults that matter
   (fetch.min.bytes=1 semantics on both) do not add latency under continuous
   flow. The pacer's own producer linger (2ms) shapes arrival identically for
   both engines and cannot touch the measurement: input timestamps are broker
   append times, stamped on arrival.

7. **Steady window**: drop the first 20% and the last 5% of positions on both
   engines identically. The head trim absorbs consumer-group assignment residue
   and Flink's JIT warm-up; the tail trim keeps end-of-input drain effects out.
   The result JSON records a per-5-second-bucket p99 series so trim adequacy is
   inspectable after the fact rather than trusted.

8. **Reported**: p50 / p90 / p99 / p99.9 / max / mean milliseconds over the steady
   window, the achieved input rate, and the gates (count, order/content, pacer
   rate). Cross-engine comparison quotes the percentiles side by side; there is
   no single-ratio headline because a latency distribution does not reduce to one
   number honestly.

### The only honest latency headline

> On Nexmark q0 (stateless pass-through, the same gated relation as the throughput
> run), with each engine reading a paced replay of the same deterministic dataset
> at R events/s (well below both engines' measured capacity) from an empty
> 1-partition LogAppendTime topic and writing to a 1-partition LogAppendTime topic
> with producer linger pinned to 0 on both, per-record broker-append-to-broker-append
> latency over the steady window was: clink p50/p99/p99.9 = A/B/C ms, Flink
> p50/p99/p99.9 = X/Y/Z ms, on <hardware>, ms resolution, count- and
> content-gated.

Out of v1, deliberately: windowed-emission latency (needs its own defensible
trigger-time definition before any number is quoted), par>1 (positional join
breaks; needs id injection), and a durable-config (checkpoints-on) latency matrix.

## Status

See `README.md` for what the harness covers and the gated results for both
axes. The premise above is the contract every quoted number is held to.
