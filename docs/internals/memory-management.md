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

This is the first increment of memory management. It limits **accounted memory**,
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
| SQL windowless `GROUP BY` | Incremental retained-state estimates after each touched group changes, including aggregate vectors, group values, prior changelog output and cold aggregate payloads such as distinct sets, `ARRAY_AGG` and UDAF values. Restore rebuilds charges; TTL expiry releases them. |
| SQL tumbling, hopping and cumulative window aggregates | The same bucket estimates, including each retained pane. Row and columnar ingest update the account; window firing releases pane charges. Empty group containers that the operator retains continue to count. |
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

There is no automatic spill, TTL adjustment or eviction of correctness-bearing
state in this increment. Existing window/TTL expiry and backend eviction retain
their original semantics. Refusal reaches the existing operator-error path;
that path closes every local edge so unrelated blocked branches can terminate.
Checkpoint allocation failures use the existing failed-checkpoint path. Nothing
changes the durability condition for a successful acknowledgement.

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
`tests/test_sql_memory_budget.cpp` exercises the real registered aggregate and
window operators, including growing values, window expiry and restored state.

The implementation lives in `include/clink/runtime/memory_budget.hpp`,
`keyed_memory_account.hpp`, `memory_size.hpp`, `arrow_memory_pool.hpp`,
`src/runtime/memory_size.cpp`, the LocalExecutor and the covered owners above.
