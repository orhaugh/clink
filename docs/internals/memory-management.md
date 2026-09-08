---
title: Memory budgets
description: "Shared accounting for retained state, queues and checkpoint memory, with explicit coverage limits."
---

# Memory budgets

A `MemoryBudget` is a shared, process-local accounting domain for an execution.
Covered allocations and retained-data estimates draw from the same byte limit.
If a charge would cross it, the engine refuses the charge with
`MEMORY_LIMIT_EXCEEDED`, naming the domain, category, requested bytes, current
usage and limit. The limit is opt-in; existing executions remain unbudgeted.

Memory management limits **accounted memory**,
not worker RSS. The coverage table below is part of the contract: enabling a
budget does not make every engine operator or third-party allocation bounded.

## Configure

For a direct local execution:

```cpp
clink::JobConfig config;
config.memory_limit_bytes = 256ull * 1024 * 1024;
clink::LocalExecutor executor(std::move(dag), config);
executor.run();
// Inspect executor.operator_errors() after termination.
```

To share a budget between local executions, retain and pass the same domain:

```cpp
auto budget = std::make_shared<clink::MemoryBudget>(256ull * 1024 * 1024, "orders");
clink::JobConfig config;
config.memory_budget = budget;
config.operator_memory_limits[clink::operator_id_from_uid("order-totals")] =
    64ull * 1024 * 1024;
```

An explicit operator limit creates a child domain. Operator-owned charges count
against both the child and execution limits. Shared backend storage and local
edge queues charge the execution domain. A chained execution can share a runtime
context between operators; the child limit follows that runner context, not an
independently isolated quota for every logical operator in the chain.

`memory_budget` and a non-zero `memory_limit_bytes` are mutually exclusive.
A domain with limit zero accounts usage without imposing its own ceiling.

The environment default is useful for SQL and the embedded runtime:

```bash
CLINK_EXECUTION_MEMORY_LIMIT_BYTES=268435456 clink run pipeline.sql
```

The value is an unsigned decimal count of bytes. Negative values, units,
whitespace and overflow are rejected. Zero disables the environment default.
An explicit shared domain or non-zero configuration limit takes precedence.
For a cluster, set the variable on each Worker. **Each local subtask execution
gets its own limit**; this is neither a cluster-wide job limit nor a Worker-wide
limit. Multiply by the number of concurrent executions when sizing a Worker.

## Coverage

| Memory owner | Accounting and enforcement |
| --- | --- |
| DAG local edge queues, including side outputs | Estimated retained bytes are reserved before enqueue and released on dequeue or queue destruction. Row vector capacity and string payloads are counted. SQL Rows include nested JSON and collection capacity. Arrow batches count retained buffers without materialising rows. Shared buffers are deduplicated within one batch but charged separately for separate queued references; slices count their parent buffers. |
| Blocking exchanges | Retained IPC payload capacity and list-node allocations charge the operator domain. With a configured spill directory, payload refusal migrates the resident prefix to disk and spills subsequent batches, even below the exchange threshold. Control elements and per-batch ordering metadata remain budgeted in memory. Replay releases each entry; destruction releases remaining charges and removes spill files. |
| SQL windowless `GROUP BY` | Incremental retained-state estimates after each touched group changes, including aggregate vectors, group values, prior changelog output and cold aggregate payloads such as distinct sets, `ARRAY_AGG` and UDAF values. Restore rebuilds charges; TTL expiry releases them. With a configured SQL spill directory, pressure switches this operator to disk-backed working groups. |
| SQL tumbling, hopping, cumulative and session windows | Aggregate bucket estimates include every retained pane/session. Row and columnar ingest update the account; firing releases expired values. Whole keyed partitions can spill under pressure. Empty group containers retained for checkpoint replacement continue to count. |
| SQL equi and interval joins | Both input maps count entry-vector capacity and nested row storage. Keyed buffers can spill; matching flags survive reload and checkpoint recovery. Interval expiry removes working and backend keys. |
| SQL OVER and last-N aggregates | Running accumulators, pending/tie-ordered rows, bounded frame history and previous changelog output count. Whole partitions can spill. |
| SQL partitioned ranking | ROW_NUMBER, RANK and DENSE_RANK candidate vectors, encoded rows and derived sort values count and can spill. Sort values are rebuilt on reload. |
| SQL null-aware semi/anti joins | Exact-key probe maps and presence counts can spill. Cross-key null probes and wildcard indexes count but stay in RAM. Both exact and null-bearing state are checkpointed. Plain semi/anti joins retain their existing backend-driven path. |
| SQL global ORDER BY LIMIT | The retained top-N heap and nested rows count, and flush releases the charge. This single global partition cannot spill. |
| SQL TTL indexes | Deadline, dirty-key and pre-watermark key estimates charge the operator budget for GROUP BY, equi joins, semi/anti joins, DISTINCT and set operators. Restore rebuilds charges and expiry releases them. These indexes remain in memory and cannot spill. |
| In-memory and file-backed backend working state | Estimated key/value storage and map overhead, checked before puts and during restore. Erase and clear release charges. Staged barrier copies have separate checkpoint charges. Binding a new domain requires an empty backend. |
| Canonical snapshot writer | Arrow builder and IPC output allocations use a budgeted pool. The final byte-vector copy is reserved while the writer holds it. Returned snapshot byte vectors are caller-owned and are not continuously tracked. |
| Async snapshot worker | Captured byte-vector capacity is charged while queued and while persistence runs. A refused enqueue throws; it cannot produce a successful acknowledgement. Backend-specific resources behind opaque capture handles are outside this charge. |
| Custom operator allocations | `RuntimeContext::memory_budget()` supplies the domain. `MemoryReservation`, `BudgetAllocator<T>` and `BudgetArrowMemoryPool` provide explicit integration. An Arrow pool must outlive all buffers allocated through it. |

Queue charges describe queued ownership only: dequeuing releases the charge even
if the consumer still holds the batch. Custom row types default to their inline
size; they can supply `retained_bytes()` returning total inline plus dynamic
retained bytes. That method must return at least `sizeof(T)`.

State estimates are not exact allocator or RSS measurements. They include
conservative container allowances and string capacity, including inline string
storage. Updates are checked after constructing a bucket, so one mutation can
allocate before its estimate is refused. Large input records and temporary SQL
expressions can therefore exceed the configured limit in actual memory.

Still outside coverage are other SQL and typed operator working maps, timers,
async request buffers, network transport queues, connector-owned buffers,
RocksDB/ForSt native caches, remote-backend dirty or pinned data, allocator
fragmentation and most temporary computation. In particular, choosing a
synchronous RocksDB backend does not move SQL working maps into it. A budget
must not be used as a substitute for a process/container memory limit.

## Pressure and failure

A budget reservation never waits for another owner to free bytes. Such a wait
could stop the record, watermark or checkpoint that would release the memory.
Existing channel-capacity backpressure continues to operate normally.

Blocking exchanges automatically spill on retained-payload pressure when
`BlockingExchangeOptions::spill_dir` names an existing writable directory. They
first move any resident data prefix to disk in arrival order, then keep all
later data on disk. The existing `spill_threshold_bytes` also triggers overflow;
threshold-triggered overflow keeps the resident prefix as before. No spill
directory means payload refusal fails the execution. Ordering metadata cannot
spill: exhaustion there still fails cleanly, including for control-only input.
An I/O failure also fails the execution; spilling is not a durability guarantee.
Blocking exchanges remain for bounded jobs without periodic checkpoints.

For an existing `Dag dag` and an `int64_t` input stage `input`, enable spill
when adding the bounded stage boundary, then configure the executor budget as
above:

```cpp
clink::BlockingExchangeOptions spill;
spill.spill_dir = "/var/tmp/clink-exchange"; // Create this directory before running.
spill.spill_threshold_bytes = 64ull * 1024 * 1024;
auto boundary = dag.add_blocking_exchange(input, clink::int64_arrow_batcher(), spill);
```

This policy covers retained exchange storage. Arrow batch construction, IPC
serialisation scratch space and decoded replay batches remain outside this
account, so a large batch can exceed the byte limit in actual RAM. Spilling one
exchange does not reclaim other operators' state. Neither spill policy adjusts
TTL or evicts correctness-bearing state. Existing window/TTL expiry and backend
eviction retain their original semantics.
Refusal reaches the existing operator-error path;
that path closes every local edge so unrelated blocked branches can terminate.
Checkpoint allocation failures use the existing failed-checkpoint path. Nothing
changes the durability condition for a successful acknowledgement.

### SQL working-map spill

Set `CLINK_SQL_SPILL_DIR` to an existing writable directory alongside the memory
limit to enable local spill for the covered synchronous SQL working maps:
GROUP BY, fixed/session windows, equi/interval joins, OVER, last-N, partitioned
ranking and null-aware semi/anti exact-key maps. A direct GROUP BY factory can
instead pass `spill_dir`, overriding the environment directory for that operator.
A memory budget is required. Async/backend-driven execution keeps its existing
storage path.

```bash
mkdir -p /var/tmp/clink-sql-spill
CLINK_EXECUTION_MEMORY_LIMIT_BYTES=268435456 \
CLINK_SQL_SPILL_DIR=/var/tmp/clink-sql-spill \
clink run pipeline.sql --state-backend=rocksdb:///var/tmp/clink-state
```

The first refused group charge writes the complete resident working map to a
private spill directory and releases its resident groups. Subsequent
mutations load one partition and write it back immediately, including on the
columnar path. This favours bounded retained memory over throughput: after the switch,
each touched group incurs synchronous file I/O and codec work. Each partition must
still fit within the available budget; a join needs the active key from both
inputs at once. A single growing `ARRAY_AGG`, a large
UDAF accumulator or contention from other owners can therefore still fail.
TTL metadata also remains in RAM and can exhaust the budget independently.

Files use the operator state codecs, preserving aggregate accumulators,
window boundaries, join matching flags, frame ordering and prior changelog rows.
Hashed filenames allow direct lookup without
an in-memory index; full stored keys and content checksums are verified on
reads. This uses one file per group under 256 hash-prefix directories, so
filesystem metadata, inode capacity and disk space matter at high cardinality.
Writes use atomic replacement. An I/O error or invalid spill content fails the
operator rather than treating the group as absent. A private directory is
removed when the operator is destroyed; an abruptly killed process can leave
scratch directories for operational cleanup.

Spill files are **not checkpoints**. Before a checkpoint, all spilled groups
are streamed through the normal operator state slots. Recovery reads those slots
and can spill again into a fresh directory. Equi/interval joins persist both
sides; null-aware joins also persist their cross-key null metadata. TTL expiry
erases both disk working state and backend entries. GROUP BY queryable
lookups/scans include spilled groups. Acknowledgement rules do not change.

Use a disk-backed state backend when checkpoints must hold more state than the
memory budget: memory/file backends retain their working state in RAM, so a
checkpoint copying a large spill store into them can still be refused. Native
RocksDB caches remain outside the shared account. Codec buffers, decoded lookup
results, batch grouping/output buffers and filesystem page cache are also
outside this coverage. This is not an RSS cap.

Watermark and cross-key mutation scans rebuild into a separate working store,
processing one partition at a time so file replacement cannot invalidate an
active directory traversal. Checkpoint scans are read-only. Scratch encoding,
decoding and output batches are temporary allocations outside these estimates.
TTL indexes, null-aware wildcard indexes and the global top-N heap remain in
RAM and fail on exhaustion; spilling never drops correctness-bearing state.
Other SQL and typed operator maps still need explicit budget integration.
Last-N and partitioned ranking retain their existing synchronous-backend
checkpoint requirement; this change does not add deferring-backend recovery
for those operators.

For Arrow growth, the pool reserves the whole new allocation while retaining the
old charge, because a reallocation can temporarily hold both buffers. Refused
growth leaves the old allocation and its accounting intact.

## Observe and test

`MemoryBudget::usage()` returns current and peak bytes, refusal count and current
charges by category: state, queue, Arrow and checkpoint. The executor samples
`clink_execution_memory_used_bytes`, `peak_bytes`, `limit_bytes`, `refused`,
`state_bytes`, `queue_bytes`, `arrow_bytes` and `checkpoint_bytes`, each with the
`clink_execution_memory_` prefix and a `root_op_id` label identifying the local
execution's first runner. It also publishes a final sample at termination.
Shared domains appear in each participating execution's series; do not sum those
series as independent allocations. Zero limit means unlimited, not zero capacity.

`tests/test_memory_budget.cpp` covers concurrent and hierarchical reservations,
allocator ownership across threads, Arrow growth failure, queue rejection,
backend state, checkpoint staging, async persistence and failure cancellation.
`tests/test_blocking_exchange.cpp` covers pressure-triggered spilling, ordered
replay, charge release, refusal without spill and bounded spill metadata.
`tests/test_sql_memory_budget.cpp` exercises the real registered aggregate and
window operators, including growing values, window expiry and restored state.
It also checks spilled aggregate/retraction parity, columnar folds, checkpoint
restore across window/OVER/ranking/join families, interval expiry and timestamp
precision, TTL cleanup, queryable scans, oversized partitions and corrupt files.

The implementation lives in `include/clink/runtime/memory_budget.hpp`,
`keyed_memory_account.hpp`, `memory_size.hpp`, `arrow_memory_pool.hpp`,
`src/runtime/memory_size.cpp`, the LocalExecutor and the covered owners above.
SQL working-map accounting lives in `include/clink/sql/working_set.hpp`.
SQL scratch storage lives in `include/clink/sql/spill_store.hpp` and
`src/sql/spill_store.cpp`; TTL accounting lives in `include/clink/sql/state_ttl.hpp`.
