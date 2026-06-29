# Task lifecycle and the local runtime

> The `LocalExecutor` runs a DAG in-process by giving every operator its own `std::jthread`, driving each through a uniform open/process/snapshot/close lifecycle while bounded channels carry data between them and apply backpressure.

## Overview

A clink job is a DAG of operators. The simplest way to run that DAG is the in-process `LocalExecutor`: it spawns one operating-system thread per DAG node (source, operator, sink, fork, split, union and so on) and lets them communicate through the bounded MPMC channels the DAG owns. There is no central scheduler loop; each thread runs its own operator's lifecycle to completion, and backpressure emerges naturally from the channels filling up. This is the runtime that single-process jobs use directly, and it is also the per-subtask engine that the distributed runtime instantiates inside each TaskManager.

This page covers how one operator runs as a task: the lifecycle hooks the runner calls, the per-thread loop that pops input and emits output, how a checkpoint barrier triggers a state snapshot, how operator-thread exceptions are captured rather than crashing the process, and how cancellation and backpressure interact with that loop.

## Where it lives

| File | What it holds |
|------|---------------|
| `include/clink/runtime/local_executor.hpp` | `LocalExecutor` interface: `start`, `await_termination`, `cancel`, `run`, `run_to_completion`, `take_savepoint`, `operator_errors`. |
| `src/runtime/local_executor.cpp` | Thread spawning, exception capture, the metrics-poll and external-cancel watcher loops. |
| `include/clink/runtime/dag.hpp` | The DAG, the type-erased `detail::OperatorRunner`, and the per-node `run` closures that contain the actual operator-driving loops (`add_source`, `add_operator`, `add_sharded_keyed`, `fork`, `add_split`, `union_streams`). |
| `include/clink/operators/operator_base.hpp` | The operator interfaces (`Operator`, `Source`, `Sink`, `CoOperator`, `ChainedOperator`) and the `Emitter` handle. |
| `include/clink/runtime/bounded_channel.hpp` | `BoundedChannel<T>`, the unit of backpressure. |
| `include/clink/runtime/job_config.hpp` | `JobConfig`, the optional knobs the executor consults at startup. |
| `include/clink/runtime/runtime_context.hpp` | `RuntimeContext`, the per-operator handle to state, timers, metrics, side outputs and logging. |

## How it works

### One thread per operator

`LocalExecutor::start()` walks `dag_.runners()` (a vector of `detail::OperatorRunner`) and, for each one, constructs a `RuntimeContext` and spawns a `std::jthread`. The runner is a type-erased pair of closures: `run(RuntimeContext&, const std::function<bool()>& should_stop)` drives the operator, and `cancel()` closes its channels. The executor does not know what kind of node it is driving; all the operator-specific logic lives inside the `run` closure that `Dag::add_source` / `add_operator` / etc. built when the topology was assembled.

The thread body wraps the runner call so a single operator failing does not take down the process:

```
LocalExecutor::start()
  for each runner i:
    contexts_[i] = RuntimeContext(runner.id, runner.name, state_backend, metrics)
    wire side-output channels, checkpoint-ack, drain signal, logger, DLQ onto it
    threads_.emplace_back([...](std::stop_token) {
        set_current_thread_name(runner.name)         // best-effort
        if (pin_operator_threads) pin to core (i % cores)
        try { runner.run(*ctx, stop_predicate); }
        catch (const std::exception& e) {
            record (runner.name, e.what()) into operator_errors_
            cancel_.store(true); runner.cancel();      // wind the rest down
        }
    })
```

`start()` is idempotent via a `compare_exchange_strong` on `running_`. Before any thread is spawned it performs restore-on-start: if a state backend and a `restore_from` snapshot are configured, it calls `state_backend->restore(...)` and, when `expected_state_versions` is set, runs `migrate_restored_state(...)` so no operator ever reads stale-schema bytes. On a fresh start with expected versions but no restore, it stamps the versions onto the backend so future snapshots record them.

`await_termination()` joins every operator thread, clears the thread and context vectors, then flips `running_` to false so the two auxiliary threads (described below) wake from their sleep and join too. `run()` is just `start()` then `await_termination()`. The destructor calls `cancel()` and relies on the `jthread` destructors to join.

### The operator lifecycle

Every node type runs the same shape of lifecycle, expressed through the virtual hooks on `Operator<In, Out>` (and the analogous `Source`, `Sink`, `CoOperator`). For a single-input single-output operator the runner in `Dag::add_operator` drives it as follows.

```
                 attach_runtime(ctx)
                        |
                 restore_timers(backend, id)        (same-parallelism restore)
                        |
                     open()                          (bind keyed state, etc.)
                        |
        +--------- main loop while !should_stop() ---------+
        |   fire due timers (processing-time, between pops)|
        |   pop_for(timeout sized to next timer deadline)  |
        |     data      -> process()                       |
        |                  (or process_columnar /          |
        |                   process_async fast paths)      |
        |     watermark  -> on_watermark() (fires ev-timers)|
        |     barrier    -> snapshot state, then process() |
        |     drain      -> rescale wind-down              |
        |   channel closed & empty -> break                |
        +--------------------------------------------------+
                        |
                 flush(emitter)                      (residual windows/joins)
                        |
                     close()
                        |
                 attach_runtime(nullptr)
                        |
                 out_channel->close()  + close side channels
```

`open()` runs once, after `attach_runtime` so the operator can reach state, metrics and timers, and after `restore_timers` so a restored timer set is visible. The default lifecycle hooks (`open`, `close`, `flush`, `on_processing_time_timer`, `on_event_time_timer`) are no-ops; operators override what they need. `flush(emitter)` is the explicit end-of-input hook for operators that buffer state (windows, sorts, joins) to emit their residual output before `close()`. It runs only on a clean shutdown (`should_stop()` false), so a cancelled job does not spuriously flush.

The runner's pop is timed: `in_channel->pop_for(timeout)` where the timeout is sized to the operator's `next_timer_deadline_ms()` (or a 30s heartbeat when no timer is pending). This lets processing-time timers fire close to their deadline without busy-waiting, and keeps a fully idle operator responsive to cancellation. Processing-time timers are fired at the top of each loop iteration via `fire_due_timers`; event-time timers fire from inside `on_watermark` as watermarks advance.

### Sources and sinks

Sources have no input channel. The `add_source` runner restores the source's offset (if the source overrides `restore_offset`), calls `open()`, then loops `produce(emitter)` until it returns false or `should_stop()` becomes true. Between `produce()` calls it drains any checkpoint barriers that were injected into the source's pending queue (`inject_pending_barrier` / `take_pending_barrier`), calling `snapshot_offset` before emitting each barrier so the offset and the barrier reach durability together with respect to the record stream. On clean exhaustion of a bounded source it emits `Watermark::max()` to fire every downstream event-time window and timer, then handles the end-of-stream tail commit (a JM-coordinated final checkpoint on the cluster path, or a local terminal barrier in-process), before `flush`, `close` and closing the output channel.

Sinks have no output channel. They consume `on_data` / `on_watermark` / `on_barrier`, with `flush()` before `close()`, and the two-phase-commit hooks `on_commit` / `on_abort` for sinks that participate in exactly-once. Connector-specific behaviour is documented separately; see [../connectors/README.md](../connectors/README.md).

### Snapshot on barrier

When the popped element is a `CheckpointBarrier` and a state backend is configured, the runner takes a snapshot before forwarding the barrier. The exact path depends on the backend:

- Synchronous backends (in-memory, RAM-only changelog, RocksDB) call `op->snapshot_timers(...)` then `backend->snapshot(ckpt_id)` on the operator thread, then `process(barrier)` to forward it, then ack the checkpoint via `RuntimeContext::checkpoint_ack`.
- Backends that support an async persist split (`supports_async_persist()`, true for file-backed and disk-backed changelog) capture a detached point-in-time blob on the operator thread (`backend->capture(ckpt_id)`), forward the barrier immediately, and hand the durable write and the ack to a per-subtask `SnapshotWorker` thread spawned in `open()`. The ack fires only after `persist()` returns, preserving the ack-after-durable invariant while keeping the durable-write latency off the critical path.

Only the chain's checkpoint owner (the most-downstream operator sharing a backend, tracked by `chain_checkpoint_owner_`) snapshots and acks; non-owners stage their timer slice and forward the barrier, keeping a shared backend single-writer for the snapshot. Checkpointing and barrier alignment are covered in depth in [./checkpointing.md](./checkpointing.md).

### Backpressure through bounded channels

The DAG owns one `BoundedChannel<StreamElement<T>>` per edge, default capacity 1024 elements (the `Dag` constructor default; `set_default_channel_capacity` overrides it for subsequently-built edges). `BoundedChannel::push` blocks when the queue is at capacity and `pop` blocks when it is empty; a slow consumer therefore fills its inbox, which blocks its producer's `emit`, which fills the producer's inbox, and so on up to the source. No explicit credit protocol is needed within a process: the channel is the unit of backpressure.

`Emitter` is the operator-side write handle and comes in three forms: a single downstream channel (the 1:1 case), a `SubtaskEmitter` that partitions or broadcasts across N channels (parallel stages), or a forwarding callable used by `ChainedOperator` to call the next operator's `process()` directly with no channel in between. Operators see the same `emit` / `emit_data` / `emit_watermark` / `emit_barrier` API in all three forms; `emit` returns false only when the downstream has been closed.

A push or pop that stays blocked for longer than three seconds logs a greppable `BOUNDED_CHANNEL_STUCK` line to stderr (with the channel name, depth, capacity and waiter counts) for backpressure-deadlock diagnosis. The line is informational; it does not cancel or fail the job.

### Cancellation

Cancellation is one-way and idempotent. There are two ways to trigger it:

1. `LocalExecutor::cancel()` sets the `cancel_` atomic and calls every runner's `cancel()` closure, which closes that runner's input and output channels.
2. `JobConfig::external_cancel_token` (a `shared_ptr<atomic<bool>>` the TaskManager flips on a CancelJob): the executor spawns `external_cancel_watch_thread_`, which polls the token every 100ms and calls `cancel()` when it flips.

Each runner's `should_stop` predicate ORs the internal `cancel_` flag with the external token. The crucial mechanism is channel close: when `cancel()` closes a runner's channels, any `pop_for` / `pop` blocked on those channels wakes immediately (close notifies all waiters), so a runner sitting on an idle pop does not need a short timeout just to notice cancellation. The 30s pop timeout exists only as a slow heartbeat for paths that could change `should_stop()` without closing a channel. An operator-thread exception triggers the same path: the catch block sets `cancel_` and calls the failed runner's `cancel()`, so downstream threads drain and exit cleanly rather than hanging.

### Exception capture

The whole point of running each operator in its own thread with a `try`/`catch` is that a single operator failure is recorded, not fatal. The thread body catches `std::exception`, records `(operator_name, message)` into `operator_errors_` under `error_mu_`, optionally appends a best-effort `std::stacktrace` (capture site, behind `CLINK_HAS_STACKTRACE`), then flips `cancel_` and invokes the runner's `cancel()` to wind the rest of the job down. After `run()` / `await_termination()` / `run_to_completion()` returns, the caller inspects `operator_errors()` to find any failures. In the cluster, a non-empty result is what surfaces a subtask failure to the recovery machinery. Note the catch handles `std::exception`; a non-`std::exception` throw is not caught here.

### Bounded jobs and savepoints

`is_bounded_job()` resolves `JobConfig::execution_mode` against the sources: `Batch` is always bounded, `Streaming` never, and `Auto` is bounded iff `Dag::all_sources_bounded()`. `run_to_completion()` is `run()` with an up-front guard: a job forced to `Batch` mode but carrying an unbounded source throws `std::logic_error` rather than blocking forever. After a bounded job terminates, `take_savepoint(id)` snapshots the configured state backend and returns the `Snapshot`; it throws if there is no backend or if the job is still running, so the captured state is always final. This is the batch-to-stream bootstrap: run a bounded backfill to completion, take a savepoint, then start a streaming job that restores from it (the operators must carry the same `uid` so the `OperatorId`, and thus the keyed state, lines up).

### Auxiliary threads

Beyond the operator threads, `start()` spawns up to two helpers:

- `metrics_thread_` runs `metrics_poll_loop_()`, sampling each runner's `input_depth` / `input_capacity` and tracking a high-water mark every 100ms into the `clink_op_input_depth{op_id="N"}` family of gauges. Skipped when no `MetricsRegistry` is configured.
- `external_cancel_watch_thread_` runs only when `external_cancel_token` is set (described under Cancellation).

Both exit when `await_termination()` flips `running_` to false.

## Key types and APIs

| Type / function | Responsibility |
|-----------------|----------------|
| `LocalExecutor::start()` | Restore-on-start, spawn one `jthread` per runner plus auxiliary threads. Idempotent. |
| `LocalExecutor::await_termination()` | Join all operator threads, then the auxiliary threads. |
| `LocalExecutor::cancel()` | Set `cancel_` and close every runner's channels. One-way, idempotent. |
| `LocalExecutor::run()` / `run_to_completion()` | Start then await; the latter guards against a non-terminating Batch job. |
| `LocalExecutor::take_savepoint(id)` | Snapshot the final state of a stopped bounded job. |
| `LocalExecutor::operator_errors()` | Captured `(operator_name, message)` pairs from failed operator threads. |
| `detail::OperatorRunner` | Type-erased per-node closures (`run`, `cancel`) plus identity and channel-introspection hooks. |
| `Operator<In, Out>` hooks | `open` / `process` / `on_watermark` / `on_barrier` / `flush` / `close` plus timer and snapshot hooks. |
| `Source<Out>` / `Sink<In>` | `produce` loop with offset snapshot/restore; sink `on_data` / `on_commit` / `on_abort`. |
| `Emitter<Out>` | Operator-side output handle; single-channel, multi-output (`SubtaskEmitter`), or forwarding (chaining). |
| `BoundedChannel<T>` | Bounded MPMC queue; blocking `push` / `pop` / `pop_for`, one-way `close`. The unit of backpressure. |
| `RuntimeContext` | Per-operator handle for keyed state, timers, metrics, side outputs, DLQ, logging. |

## Configuration and knobs

| Knob | Where | Default | Effect |
|------|-------|---------|--------|
| `JobConfig::execution_mode` | `job_config.hpp` | `Auto` | Selects streaming vs bounded run-to-completion. |
| `JobConfig::state_backend` / `restore_from` | `job_config.hpp` | unset | Restore-on-start before any operator runs. |
| `JobConfig::expected_state_versions` | `job_config.hpp` | unset | Migrate restored state up to expected versions; stamp fresh snapshots. |
| `JobConfig::metrics` | `job_config.hpp` | `nullptr` | Enables the metrics-poll thread and per-operator gauges. |
| `JobConfig::external_cancel_token` | `job_config.hpp` | `nullptr` | Out-of-band cancel signal; enables the watcher thread. |
| `JobConfig::pin_operator_threads` | `job_config.hpp` | `false` | Round-robin pin operator thread `i` to core `i % cores`; best-effort, no-op where hard affinity is unavailable (macOS). |
| `JobConfig::unaligned_checkpoints` | `job_config.hpp` | `false` | Barrier alignment policy at multi-input operators (see [./checkpointing.md](./checkpointing.md)). |
| `JobConfig::dead_letter_queue` | `job_config.hpp` | `nullptr` | When null, the executor installs a `LoggingDeadLetterQueue` so poison records are logged with zero config. |
| `Dag` channel capacity | `dag.hpp` | `1024` elements | Per-edge `BoundedChannel` capacity; set via the `Dag` constructor or `set_default_channel_capacity`. |
| `CLINK_EOS_FINAL_CKPT_TIMEOUT_MS` | `dag.hpp` | `30000` | Source EOS wait for its JM-coordinated final checkpoint to commit. |
| `CLINK_DISABLE_COLUMNAR` | `dag.hpp` | unset | Diagnostic: force the row `process()` path even when a columnar fast path exists. No correctness effect. |

## Guarantees and caveats

- One operator per OS thread. This is, by design, the simplest possible runtime (one `std::jthread` per DAG node); there is no cooperative scheduler, work-stealing pool or shard-per-core placement here. Thread pinning is opt-in and best-effort.
- An operator-thread exception is captured into `operator_errors()` and winds the job down via cancellation rather than crashing the process. Only `std::exception` is caught at this site; the stack trace is best-effort and only present when built with `<stacktrace>` support (`CLINK_HAS_STACKTRACE`). The recorded trace is from the capture (runner) site, not the throw site.
- Backpressure is automatic but in-process only: it propagates through blocking channel pushes. There is no credit-based flow control inside the executor; cross-TaskManager backpressure is handled by the network stack ([./network-stack.md](./network-stack.md)).
- The `flush()` end-of-input hook runs only on a clean shutdown, never on cancel, so buffered windows/joins emit residual output exactly when the stream ends naturally.
- Async-state operators force-align at a barrier: they drain in-flight async work to quiescence before capture regardless of the barrier's mode, because drain-to-quiescence is incompatible with unaligned in-flight capture. For a single input this is lossless. See [./async-state-execution.md](./async-state-execution.md).
- Timer restore on the operator path is same-parallelism only: timers ride operator-state and `restore_timers` narrows by key-group range on a rescale; the rescale timer-routing story is detailed in [./fault-tolerance-and-rescale.md](./fault-tolerance-and-rescale.md).
- `take_savepoint` requires the job to be stopped and a backend to be configured, and throws otherwise; it does not interrupt a running job.

## Related

- [./architecture.md](./architecture.md) - where the local runtime sits in the component stack.
- [./operator-model.md](./operator-model.md) - the operator interfaces and DAG construction the runner drives.
- [./jobs-and-scheduling.md](./jobs-and-scheduling.md) - parallelism, batch planning and how runners are assembled.
- [./distributed-runtime.md](./distributed-runtime.md) - how each TaskManager hosts a `LocalExecutor` per subtask.
- [./network-stack.md](./network-stack.md) - cross-process channels and backpressure between subtasks.
- [./checkpointing.md](./checkpointing.md) - barriers, alignment and the snapshot-on-barrier path.
- [./state-and-backends.md](./state-and-backends.md) - the keyed state and backends the snapshot path persists.
- [./async-state-execution.md](./async-state-execution.md) - the `process_async` fast path and force-alignment.
- [./columnar-execution.md](./columnar-execution.md) - the `process_columnar` fast path.
- [./fault-tolerance-and-rescale.md](./fault-tolerance-and-rescale.md) - failure recovery, rescale and schema evolution.
- [../connectors/README.md](../connectors/README.md) - source and sink connector behaviour.
