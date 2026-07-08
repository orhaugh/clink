# Internals

How the clink engine works inside: the mechanisms behind the operator model, the
distributed runtime, time and state, checkpointing and recovery, and the columnar,
async, and SQL execution paths. These pages are for engineers reading or modifying
the engine; each one names the source files it describes so you can follow into the
code.

For source and sink connectors (Kafka, S3, Postgres, and the rest), see the
[connector reference](../connectors/README.md). For runnable API examples, see
[consumer-examples](../consumer-examples/).

Start with [Architecture and component stack](architecture.md) for the big picture,
then follow the links into each subsystem.

## Engine core and runtime

| Page | What it covers |
| --- | --- |
| [Architecture and component stack](architecture.md) | How clink layers the engine core, the connector and backend impls, the cluster control plane, the SQL frontend, and the client/daemon binaries, and how a job flows from submission to running operators. |
| [The operator model and DAG](operator-model.md) | The typed operator DAG, the in-band stream element model, and how RuntimeContext hands an operator its state, timers and identity. |
| [Task lifecycle and the local runtime](task-lifecycle.md) | How LocalExecutor runs each operator as a task on its own jthread, driving the open/process/snapshot/close lifecycle while bounded channels carry data and apply backpressure. |
| [Jobs, parallelism and scheduling](jobs-and-scheduling.md) | How a logical JobGraphSpec is planned into key-group-routed parallel subtasks and placed onto TaskManager slots by the JobManager. |

## Distribution and data movement

| Page | What it covers |
| --- | --- |
| [Distributed runtime and the cluster control plane](distributed-runtime.md) | How the JobManager and TaskManagers coordinate over a binary length-prefixed TCP protocol to deploy, run, checkpoint and recover jobs, and how jobs are shipped as dlopen'd plugin libraries. |
| [Network stack and data exchange](network-stack.md) | How records, watermarks and checkpoint barriers move between operators in one process and across TaskManagers, over bounded channels and a length-prefixed Arrow-IPC TCP wire. |

## Time and state

| Page | What it covers |
| --- | --- |
| [Time, watermarks, windows and CEP](time-and-windowing.md) | How clink tracks event-time progress with watermarks, fires keyed tumbling/sliding/session/evicting windows via pluggable triggers and allowed-lateness, and matches patterns with the NFA-based CEP operator. |
| [Keyed state and state backends](state-and-backends.md) | How operators store per-key and per-operator state over Codec<T>, the pluggable backends (in-memory, sharded, file, changelog, RocksDB, remote-read), the Arrow IPC snapshot format vs RocksDB native SST checkpoints, queryable state, and the two-tier disaggregated RemoteReadBackend. |

## Reliability

| Page | What it covers |
| --- | --- |
| [Checkpointing and barriers](checkpointing.md) | How clink takes a globally consistent snapshot by flowing in-band barriers through the dataflow, snapshotting state on barrier receipt off-thread, and committing two-phase-commit sinks once every subtask has acked. |
| [The sink committer framework](sink-committer-framework.md) | How CommittingSink gives a connector exactly-once (or honest effectively-once) delivery without hand-rolling 2PC: the verbs a connector supplies, per-checkpoint committable persistence + recover-at-open, the three delivery shapes (staged artifact / external XA / idempotent upsert), and precise guarantee labelling. |
| [Fault tolerance, rescale and schema evolution](fault-tolerance-and-rescale.md) | How clink recovers a job after a lost TaskManager, rescales operator parallelism via key groups, and migrates keyed state across schema changes, all anchored on completed checkpoints. |

## Execution paths

| Page | What it covers |
| --- | --- |
| [Arrow-native columnar execution](columnar-execution.md) | How clink carries data as Arrow RecordBatch sidecars on Batch<T>, the ArrowBatcher<T> wire seam, the opt-in process_columnar() operator fast path, and the precise conditions under which columnar processing actually fires. |
| [Async execution and disaggregated state](async-state-execution.md) | A coroutine substrate that lets keyed operators issue non-blocking state reads and enrichment lookups, backed by a remote state tier with a bounded in-memory hot cache. |

## SQL

| Page | What it covers |
| --- | --- |
| [SQL frontend internals](sql-frontend.md) | How a SQL statement becomes an operator DAG: preparse shim, libpg_query parse, AST, binder, rule-based optimiser, and physical planner producing a JobGraphSpec. |
| [Embedded execution](embedded.md) | How `clink run <file>.sql` runs the whole engine in one process with no daemons: the EmbeddedEngine's in-process JM + TM pair, the shared SQL script runner, bare-SELECT-to-print, and Ctrl-C drain semantics. |

## Observability

| Page | What it covers |
| --- | --- |
| [Data lineage](data-lineage.md) | How clink derives the external datasets a job reads and writes, exposes them over HTTP and the event bus, and ships them to an external lineage system via a pluggable listener with a built-in OpenLineage exporter. |

