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

1. **Topology**: both clustered. clink runs `clink_node` JM + N TMs as host
   processes over a real network; Flink runs a real JM/TM cluster. The in-process
   single-TaskManager clink number (`clink_nexmark_bench` steady-state mode) is
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
  over the clink JM+TM processes; cgroup `cpuacct` for the Flink JM+TM containers),
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

## Status

This harness is being built increment by increment (see `README.md`). No
cross-engine ratio exists yet; the premise above is the contract every future
number is held to.
