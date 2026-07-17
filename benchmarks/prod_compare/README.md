# clink vs Flink — production-feature bench suite

A head-to-head suite that exercises the parts of the streaming engine a
production deployment actually leans on, so a port to clink can be
weighed against real numbers, not the toy aggregate from
`inproc_compare/`. Every bench has:

  - matching workload + record shape on both engines
  - the same checkpoint configuration (5 s, RocksDB, file checkpoint
    storage)
  - identical record counts driven through env-vars

The suite is organised by the dominant feature each bench is meant to
exercise; running all of them gives a four-dimensional view rather than
one summary number.

## Suite members

| dir                  | feature exercised                                          | status     |
|----------------------|------------------------------------------------------------|------------|
| `sliding_window/`    | event-time sliding windows (size=1s, slide=250ms; 4x fan-out) | runs       |
| `process_function/`  | KeyedProcessFunction; ValueState + ListState; emit-per-record | runs       |
| `interval_join/`     | two-stream connect_process + ValueState lookup             | scaffolded |
| `parallel_recovery/` | par=4 hash partitioning over the tumbling-window workload  | runs       |

Status: 3 of 4 benches run end to end on 2026-06-01. `interval_join`
builds but the two-source connect_process path hits a coordinator-side planner
gap; see `interval_join/README.md` for the residual issue and fix
candidates. `parallel_recovery/` runs at par=N for any N>=1 but does
not yet exercise the induced-crash / restore axis - v2 work.

Each member has its own `README.md` describing the workload + intent,
a `clink-job/` directory (CLINK_REGISTER_JOB .so), a `flink-job/`
directory (Maven JAR), and a `run.sh` runner that builds both and
prints wall times.

## Apples-to-apples ground rules

  - **State backend semantics**: per-record writes hit RocksDB on
    both sides. The strict mode (`CLINK_WB_STATE_CACHE=0`, the
    **default** for every run.sh) writes every state update to
    RocksDB MemTable, matching Flink-with-disableWAL semantics.
  - **Record data layout**: clink jobs use `shared_ptr<EventBody>`-
    style consolidation only when the Flink job can plausibly enable
    the equivalent (object reuse, immutable string interning, etc.).
  - **Parallelism**: every bench reports walls at par=1 and par=4 so
    the scaling factor is visible.

## Durability modes

Both modes give the same durability contract as Flink with
`setDisableWAL(true)`: state is durable through the last successfully
completed checkpoint barrier; updates between the last barrier and a
crash are lost. They differ in how the state reaches RocksDB:

- **Strict** (`CLINK_WB_STATE_CACHE=0`, default): every record's
  state update writes through to `keyed_->put` immediately, the same
  shape Flink takes by writing every update to its RocksDB MemTable.
- **Write-back cache** (`CLINK_WB_STATE_CACHE=1`): the operator's
  `mem_` is the authoritative working set between barriers; the
  `on_barrier` override flushes every `mem_` entry through
  `keyed_->put` before forwarding the barrier downstream, so the
  engine's checkpoint captures the up-to-date state. Recovery's
  `keyed_->scan` rehydration in `open()` restores the last-barrier
  state - same Flink-equivalent contract, lower per-record cost.

Every `run.sh` defaults to strict (`=0`) and prints
`clink durability mode: ...` in the scoreboard so the mode is
visible alongside the numbers. WB-cache numbers are legitimate
apples-to-apples comparisons against Flink, just with a different
durability/throughput trade-off than the strict shape.

Operator inventory:

  - `SlidingWindowOperator`: per-record `keyed_->get` + `keyed_->put`
    on every record (no `mem_` cache, no WB skip). Strict by
    construction; the WB env var is a no-op here.
  - `TumblingWindowOperator`: `mem_.try_emplace` working set;
    per-record `keyed_->put` when `CLINK_WB_STATE_CACHE!=1`, batched
    `on_barrier` flush when `=1`. Both shapes are durable through
    the last barrier.
  - `KeyedProcessFunctionAdapter`: user-managed `KeyedState`,
    per-record `get` + `put` on every state slot the function touches.
    No engine-level WB cache today - same shape as Flink. A future
    iteration could add the same flush hook here too.

## Common scaffolding

`common/` holds the shared bench scaffolding: the `EventBody`-style
record type, the synthetic source helper, the counting sink helper.
Each bench can either reuse these directly or specialise where the
workload calls for it (e.g. the join bench needs a second event type).

## Running the suite

```bash
# Run all four benches in sequence; produces a Markdown report at
# benchmarks/prod_compare/results/SUITE.md
benchmarks/prod_compare/run_all.sh

# Run one bench by name:
benchmarks/prod_compare/sliding_window/run.sh

# Run with parallelism override:
PARALLELISM=4 benchmarks/prod_compare/sliding_window/run.sh
```

## Reading the numbers

Each `run.sh` prints:

  - per-engine wall clock (median of N runs)
  - throughput in records/sec
  - p99 record latency where applicable (process-function bench
    emits per-record latency to enable this)
  - checkpoint-storage size after the run
