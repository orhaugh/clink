# Fault tolerance, rescale and schema evolution

> How clink recovers a job after a TaskManager is lost, changes a running operator's parallelism, and migrates keyed state across a schema change, all anchored on completed checkpoints.

## Overview

A clink job survives infrastructure failure and reconfiguration by redeploying its operators from a previously completed checkpoint rather than from scratch. Three related mechanisms share that foundation. Restart-from-checkpoint failover detects a lost TaskManager (TM), drains the survivors, and redeploys the affected subtasks. Rescale repartitions keyed state across a new subtask count using key groups, scaling up or down by integer factors. State schema evolution lets a job whose state layout has changed restore from an older savepoint by running each stored value through a registered migration before any operator reads it. All three depend on the checkpoint machinery (see [./checkpointing.md](./checkpointing.md)) and the keyed-state backends (see [./state-and-backends.md](./state-and-backends.md)).

## Where it lives

| Area | Files |
| --- | --- |
| Failover and the watchdog | `src/cluster/job_manager.cpp` (`watchdog_loop_`, `mark_tm_lost_locked_`, `restart_job_locked_`), `include/clink/cluster/job_manager.hpp` (`JobManager::Config`) |
| Restart policy resolution | `include/clink/cluster/protocol.hpp` (`CheckpointConfig`, `effective_max_restarts`, `kRestartAuto`, `kDefaultSelfHealRestarts`) |
| Key groups (rescale partitioning) | `include/clink/runtime/key_groups.hpp` |
| Operator-level rescale state machine | `include/clink/cluster/rescale_coordinator.hpp`, `src/cluster/rescale_coordinator.cpp` |
| Cutover deployment planning | `include/clink/cluster/rescale_dispatch.hpp`, `src/cluster/rescale_dispatch.cpp` |
| Whole-job rescale | `src/cluster/job_manager.cpp` (`rescale_job`, `restart_job_locked_` rescale branch) |
| Schema version trait + maps + migration registry | `include/clink/state/schema_version.hpp`, `src/state/schema_version.cpp` |
| Restore-time migration + compatibility check | `include/clink/state/state_migration_on_restore.hpp`, `src/state/state_migration_on_restore.cpp`, `src/runtime/local_executor.cpp` |
| Pre-deploy compatibility gate | `include/clink/cluster/restore_compat_gate.hpp`, `src/cluster/restore_compat_gate.cpp`, `tools/clink_check_savepoint.cpp` |
| Savepoints (online + offline) | `src/cluster/job_manager.cpp` (`take_savepoint`), `include/clink/state_processor/savepoint.hpp` |

## How it works

### Restart-from-checkpoint failover

Every registered TM heartbeats the JobManager. A watchdog thread (`JobManager::watchdog_loop_`) wakes every `watchdog_interval` (default 100ms) and marks any TM whose last message is older than `heartbeat_timeout` (default 2000ms) as lost via `mark_tm_lost_locked_`. Healthy TMs are expected to heartbeat at roughly `heartbeat_timeout / 3` so a single dropped message does not trigger a false positive.

When a TM is declared lost, `mark_tm_lost_locked_` walks every job that had a subtask on that TM and decides, per job, whether to restart. The decision is gated on `effective_max_restarts(job.checkpoint)`:

- A non-empty `checkpoint_dir` and `job.restart_attempts < effective_max_restarts(...)` means the job can recover. It is moved into the `awaiting_restart` state.
- Otherwise the JM fails fast: it synthesises a `TM lost (heartbeat timeout)` error per pending subtask, counts them toward completion, and surfaces the failure to the client.

On the recovery path the JM must first drain the surviving subtasks before it can safely redeploy. The lost-TM subtasks go into `restart_pending`; the still-in-flight subtasks on surviving TMs go into `restart_drain_expected`. The watchdog then broadcasts `CancelJob` to every surviving TM hosting that job. Each survivor's role handler is expected to observe `was_cancelled()`, exit, and report a normal `SubtaskFinished`, which decrements the expected-drain set. Once the drain set empties (or it was already empty because the survivors had finished), `restart_job_locked_` runs and rebuilds the deployment.

```mermaid
flowchart TD
  HB["TM heartbeat stops"] --> WD["watchdog_loop_:<br/>now - last_seen &gt; heartbeat_timeout"]
  WD --> ML{"mark_tm_lost_locked_:<br/>restart budget left?"}
  ML -->|no| FF["fail fast (synthesise errors)"]
  ML -->|yes| AR["awaiting_restart:<br/>restart_pending = lost-TM subtasks,<br/>drain_expected = surviving in-flight subtasks,<br/>restart_deadline = now + restart_drain_timeout"]
  AR --> CJ["CancelJob: survivors drain (SubtaskFinished)"]
  CJ --> DE{"drain set empty?"}
  DE -->|yes| RJ["restart_job_locked_:<br/>place tasks on survivor slots,<br/>Deploy with restore_from_dir +<br/>latest_completed_checkpoint_id"]
```

`restart_job_locked_` rebuilds a `DeploymentTask` template per subtask (preserving `role`, `subtask_idx`, `extra_config`, and peer topology while clearing resolved host/port), places the tasks round-robin across survivor TMs with free slots, resets the job's transient coordination state, and emits one `DeployMsg` per TM. The restore handle on every Deploy is the JM's own coordination directory and last acknowledged id:

```cpp
deploy_msg.restore_from_dir = job.checkpoint.checkpoint_dir;
deploy_msg.restore_from_checkpoint_id = job.latest_completed_checkpoint_id;
```

If no survivor has a free slot, the restart is aborted with a `no slot available` error per subtask and the job is failed.

The drain is bounded. When a job enters `awaiting_restart` the JM sets `restart_deadline = now + restart_drain_timeout` (default 30000ms). A watchdog scan that runs every tick, independent of any new TM loss, fails any job whose drain has outrun its deadline. This catches a survivor that is hung but still heartbeating, so it neither acks the cancel nor dies. Failing is the safe escalation here, because force-restarting could double-run a slow-but-alive subtask against shared state.

Two edge cases are handled explicitly. A second TM lost while the job is already draining folds its subtasks into the in-progress restart (they move from `restart_drain_expected` to `restart_pending`) without consuming an extra restart attempt, since it is the same restart now covering both losses. A subtask error or a bounded-source end-of-stream final-checkpoint timeout follows the same `awaiting_restart` machinery but redeploys the full topology from `tasks_by_tm` rather than just the in-flight set, because the failing subtask may have peers that already finished.

#### The restart-policy default

`CheckpointConfig::max_restarts_on_tm_loss` defaults to the sentinel `kRestartAuto`. `effective_max_restarts` resolves it at every restart decision:

- `kRestartAuto` with a `checkpoint_dir` set resolves to `kDefaultSelfHealRestarts` (10): the job self-heals up to ten times before failing loudly rather than looping forever.
- `kRestartAuto` with no `checkpoint_dir` resolves to 0: fail fast, because there is no checkpoint to restore from.
- An explicit `0` forces fail-fast even with checkpointing.
- An explicit `N` caps the attempts at `N`.

Storing the sentinel rather than the resolved value means the user's intent (auto versus explicit) round-trips through HA recovery.

### Rescale: changing parallelism on a running job

Rescale relies on key groups (`include/clink/runtime/key_groups.hpp`). Every keyed record is bucketed into one of `kNumKeyGroups = 128` groups by a stable FNV-1a hash of its serialised key:

```
key_group = fnv1a_64(key_bytes) mod 128
subtask   = key_group * parallelism / 128
```

`subtask_for_key_group` gives every subtask a contiguous slice of groups, and `key_group_range_for_subtask` is its inverse. Because groups, not raw keys, are the unit of ownership, changing parallelism redistributes whole slices rather than rehashing individual keys, and the snapshot loader can read or filter exactly the state that belongs to a new subtask's range.

clink has two rescale paths that share this math.

**Whole-job rescale** (`JobManager::rescale_job`) changes the parallelism of one or more roles at once. It requires a `checkpoint_dir` and at least one completed checkpoint, validates that each new parallelism is an integer multiple (scale-up) or divisor (scale-down) of the current value, and checks free slot capacity if the change net-grows usage. It then stages `rescale_overrides`, marks the job `awaiting_restart`, sets the drain deadline, and broadcasts `CancelJob`. The same drain-then-`restart_job_locked_` machinery used by failover fires, but now the rescale branch resizes the per-role template set, rewrites peer fan-out for the new subtask count, and tags each new subtask with its restore mapping and key-group range. The per-subtask restore mapping is:

- **Scale-up** (`new_p > old_p`): `k = new_p / old_p` new subtasks per parent. `restore_from_subtask_idx = new_idx / k`, `restore_from_parent_count = 1`. Each new subtask reads its parent's snapshot and filters it down to its own key-group slice on read.
- **Scale-down** (`new_p < old_p`): `k_down = old_p / new_p` parents per new subtask. `restore_from_subtask_idx = new_idx * k_down`, `restore_from_parent_count = k_down`. The snapshot loader concatenates the `k_down` contiguous parent slices into one merged state.

**Operator-level adaptive rescale** is driven by the per-operator `RescaleCoordinator` (`include/clink/cluster/rescale_coordinator.hpp`). Its purpose is to run a graceful cutover under a coordinator state machine rather than a full job restart. An operator is registered with its current parallelism plus `[min_parallelism, max_parallelism]` bounds taken from the `OperatorSpec`; `min == 0 && max == 0` means the operator is not scalable and rescale requests are rejected. The state machine is:

```mermaid
stateDiagram-v2
  [*] --> Idle
  Idle --> Preparing: request_rescale (validate bounds, mark target)
  Preparing --> Draining: mark_checkpoint_ready (a checkpoint landed)
  Draining --> CuttingOver: all old subtasks acked
  CuttingOver --> Complete: all new subtasks ready (current = target)
  Preparing --> Aborted: abort (revert parallelism)
  Draining --> Aborted: abort
  CuttingOver --> Aborted: abort
```

`JobManager::request_operator_rescale` validates the request and refuses if the job has no coordinator, or if periodic checkpointing is not configured (`checkpoint_dir` empty or `interval_ms <= 0`), because the `Preparing -> Draining` transition is driven by a checkpoint landing and would otherwise wait forever. The actual cutover deployment math lives in `plan_operator_cutover` (`rescale_dispatch.cpp`), which computes the same key-group ranges and `restore_from_*` mapping as the whole-job path and places the new subtasks greedily onto TMs with free slots. `JobManager::dispatch_begin_rescale_locked_` sends `BeginRescale` to every TM hosting the operator; each TM fires its drain callbacks so the running subtask emits a `DrainMarker` and shuts down. The coordinator itself is purely the state record: each transition is mutex-protected and the JM RPC handlers update it via `mark_checkpoint_ready`, `mark_old_drained`, and `mark_new_ready`.

#### Sources as operator state

Key-group filtering only narrows keyed state. State with no key, source offsets and broadcast slots, is operator-list state and carries the reserved leading byte `kOperatorStateKeyPrefix = 0xFF` (deliberately `>= kNumKeyGroups` so it can never collide with a real key group). The rescale restore filter narrows a row only when its first byte is a valid key group, so any key carrying the operator-state prefix is exempt and every new subtask restores it whole. That gives broadcast and union semantics for free, but it also means source offset state is not repartitioned across a parallelism change the way keyed state is. Source replay correctness across a rescale depends on the source implementation, the same caveat that applies to plain restart (see the connector notes in [../connectors/README.md](../connectors/README.md)).

### State schema evolution

When the byte layout of a state value changes, an old savepoint's bytes are no longer directly readable by the new operator. clink resolves this with versioned migration applied at restore time, before any operator reads its state.

**Versioning.** `SchemaVersionTrait<T>` is a compile-time trait, defaulting to version 1, that a user specialises to bump the version on a breaking shape change. At snapshot time the engine stamps each `(operator, state_type, slot)` with its version into a `StateVersionMap`. The map is packed into the Arrow IPC schema metadata of the snapshot under the key `clink.state_versions` (see `src/state/in_memory_state_backend.cpp`). A v1-format snapshot has no such metadata, and restore tolerates its absence by treating an unstamped entry as version 1.

**Migration registry.** `StateMigrationRegistry` stores single-step migration functions keyed by `(state_type, from_version)`. A migration function is pure: it maps input bytes to output bytes and never touches the live state map. `migrate()` plans a chain of single steps via BFS over the registered edges, so a v1-to-v3 migration composes a v1-to-v2 and a v2-to-v3 step automatically. The registry also supports Arrow-aware auto-migration: if both versions of a `state_type` have a registered Arrow schema and the change is additive (new fields nullable, existing fields preserved or widened to a non-narrowing integer), the registry synthesises the migration without user code.

**Restore-time migration.** `migrate_restored_state` (`src/state/state_migration_on_restore.cpp`) is called from `LocalExecutor::start` after the backend has restored but before any operator runs. For each expected `(op, state_type)` whose stored version differs from the expected version, it scans the operator's state, runs each value through the registry chain, and writes the migrated values back. It is slot-aware: an entry naming a slot filters the backend scan by that slot's key prefix so a sibling slot under the same operator stays byte-identical. The migrated versions are then re-stamped onto the backend (merged, not replaced, so sibling slots the current generation left unchanged keep their stamps) so the next snapshot records the new versions and a re-restore is a no-op. On a fresh start with no restore, the expected versions are simply stamped so future snapshots record them.

**Two layers of safety.** The same per-`(op, state_type)` comparison feeds both a pre-deploy gate and the restore-time migrator, so the two cannot drift:

```
              StateVersionMap (stored)   StateVersionMap (expected)
                        \                     /
                         v                   v
                   check_restore_compatibility(stored, expected, registry)
                        |                          |
            (pre-deploy gate, fail fast)     (restore-time migrator)
                        |                          |
   check_restore_compatibility_via_plugins   migrate_restored_state
   at SubmitJob / HA-recovery in JM          in LocalExecutor::start
```

`check_restore_compatibility` returns a list of `(op, state_type, from, to)` pairs the registry cannot bridge; an empty list means the restore is compatible. The JM calls `check_restore_compatibility_via_plugins` at job submission: it reads subtask 0's stored version map and asks each job `.so` (where the job's `StateMigrationRegistry` lives) whether it can migrate to its expected versions, rejecting the deploy on a definite incompatibility. This gate is best-effort by design: if it cannot read the savepoint, load a `.so`, or find the exported check, it returns empty and lets the deploy proceed, since `migrate_restored_state` still guarantees correctness (or throws) at restore. The offline `clink check-savepoint` CLI exposes the same inspection for operators without a running JM.

### Savepoints

A savepoint is a checkpoint taken on demand and intended to be restored from later. `JobManager::take_savepoint` assigns a fresh checkpoint id, registers a pending ack for every subtask, broadcasts `TriggerCheckpoint`, and blocks until `latest_completed_checkpoint_id` advances past it (default timeout 30000ms). It requires a `checkpoint_dir`. The resulting `.snap` blob is the same Arrow-IPC snapshot format the runtime produces during periodic checkpointing.

Because the format is shared, clink also exposes an offline State Processor API (`include/clink/state_processor/savepoint.hpp`). `Savepoint::load_from_file` reads a `.snap` blob into an in-memory backend; typed `keyed_state<K, V>(op, slot, kc, vc)` views read and mutate the stored entries; `write_to_file` persists a new savepoint a later job can restore from. This is the path for bulk state migration across a schema change, offline inspection or audit, and seeding a fresh job with pre-populated state. The v1 scope is deliberately tight: keyed state only (broadcast and operator-list state are not surfaced, and timers live in the timer service, not the backend), the whole savepoint is materialised in RAM while open, and there is no schema check on the codec pair, so the codecs must match the originating job's.

## Key types and APIs

| Type / function | Responsibility |
| --- | --- |
| `JobManager::watchdog_loop_` | Periodic liveness scan; declares TMs lost, drives restart drains and the drain-timeout escalation |
| `JobManager::mark_tm_lost_locked_` | Per-job restart-or-fail decision on TM loss; builds the drain and redeploy sets |
| `JobManager::restart_job_locked_` | Rebuilds the deployment from templates, places tasks on survivor slots, emits Deploy frames pointing at the last completed checkpoint |
| `effective_max_restarts(CheckpointConfig)` | Resolves `kRestartAuto` to self-heal (10) with checkpointing or fail-fast (0) without |
| `key_group_for_key` / `subtask_for_key_group` / `key_group_range_for_subtask` | The 128-group partitioning primitive and its inverse used by rescale restore |
| `JobManager::rescale_job` | Whole-job parallelism change via the drain-then-restart machinery |
| `RescaleCoordinator` | Per-operator rescale state machine (`Idle`/`Preparing`/`Draining`/`CuttingOver`/`Complete`/`Aborted`) |
| `plan_operator_cutover` | Computes new-subtask key-group ranges, restore mapping, and TM placement for an operator cutover |
| `SchemaVersionTrait<T>` / `StateVersionMap` | Compile-time version trait and the `(op, state_type, slot) -> version` map stamped into snapshots |
| `StateMigrationRegistry` | Registers single-step migrations, plans multi-step chains, and synthesises additive Arrow migrations |
| `check_restore_compatibility` / `migrate_restored_state` | The shared compatibility decision and the in-place restore-time transform |
| `check_restore_compatibility_via_plugins` | JM-side best-effort pre-deploy gate |
| `JobManager::take_savepoint` | On-demand checkpoint a later run can restore from |
| `clink::state_processor::Savepoint` | Offline read/transform/write API over a savepoint's keyed state |

## Configuration and knobs

| Knob | Where | Default | Effect |
| --- | --- | --- | --- |
| `watchdog_interval` | `JobManager::Config` | 100ms | How often the watchdog re-evaluates TM liveness |
| `heartbeat_timeout` | `JobManager::Config` | 2000ms | A TM is declared lost after this much silence |
| `restart_drain_timeout` | `JobManager::Config` | 30000ms | Upper bound on the `awaiting_restart` drain; on expiry the watchdog fails the job |
| `max_restarts` | `JobManager::Config` | 0 | Per-failing-task retry budget (distinct from TM-level restarts) |
| `max_restarts_on_tm_loss` | `CheckpointConfig` | `kRestartAuto` | Resolves to 10 (self-heal) with a checkpoint dir, 0 (fail-fast) without; explicit `N` caps attempts |
| `checkpoint_dir` | `CheckpointConfig` | empty | Required for any restart-from-checkpoint, rescale, or savepoint |
| `interval_ms` | `CheckpointConfig` | 0 | Periodic-checkpoint cadence; operator rescale requires it `> 0` |
| `restore_from_dir` / `restore_from_checkpoint_id` | `CheckpointConfig` | empty / 0 | Resume a fresh job from a prior completed checkpoint or savepoint |
| `min_parallelism` / `max_parallelism` | `OperatorSpec` | 0 / 0 | `0/0` means not scalable; otherwise bound a rescale request |
| `kNumKeyGroups` | `key_groups.hpp` | 128 | Partition count; tunable in the range 16 to 1024 without protocol changes |
| `kDefaultSelfHealRestarts` | `protocol.hpp` | 10 | Self-heal attempt cap when `max_restarts_on_tm_loss` is auto |

## Guarantees and caveats

- **Recovery is checkpoint-anchored.** Restart, rescale, and savepoint-restore all require a `checkpoint_dir`. Without one a TM loss fails the job. Keyed state is preserved across a restart; source replay correctness depends on the source implementation (see [../connectors/README.md](../connectors/README.md)).
- **Self-heal is bounded and the default.** With checkpointing, a job self-heals up to `kDefaultSelfHealRestarts` (10) times by default, then fails loudly. An explicit `0` keeps the strict fail-fast behaviour. The restart drain is itself bounded by `restart_drain_timeout`; a hung-but-heartbeating survivor causes the job to fail rather than risk a double-run.
- **Rescale is integer-factor only.** Both the whole-job and operator paths require the new parallelism to be an integer multiple (scale-up) or divisor (scale-down) of the current value. Non-integer factors would leave key groups straddling parents and are not supported.
- **Operator rescale needs periodic checkpointing.** `request_operator_rescale` rejects the request unless `checkpoint_dir` is set and `interval_ms > 0`, because the cutover is gated on a checkpoint landing.
- **Operator-list state is not repartitioned.** Source offsets and broadcast slots carry the `0xFF` operator-state prefix and are restored whole by every subtask on rescale, not split by key group.
- **Migration must stay name-stable.** Schema migration looks state up by `(op, state_type)` and filters by slot name, so both the `state_type` tag and the slot name must stay stable from the generation that wrote a savepoint to the one that restores it. Renaming a tag or slot across a restore is unsupported and needs an explicit transform step first. An absent stamp is assumed v1, which is safe only when the tag is genuinely new.
- **TTL slots cannot be migrated in v1.** A TTL-enabled slot stores values as `[8B expire-at][user bytes]`; the migrator hands the full stored value to the migration function, which expects raw user bytes, so do not bump the schema version of a TTL slot.
- **The pre-deploy gate is best-effort.** It only blocks on a definite incompatibility verdict; an unreadable savepoint or missing `.so` lets the deploy proceed, relying on `migrate_restored_state` to enforce correctness (or throw) at restore.
- **The State Processor API is v1-scoped.** Keyed state only; broadcast/operator-list state and timers are not surfaced; the savepoint is held in RAM while open; there is no codec schema check.

## Related

- [./checkpointing.md](./checkpointing.md) - barriers, alignment, and the completed-checkpoint markers every recovery path restores from
- [./state-and-backends.md](./state-and-backends.md) - keyed state, slots, TTL, and the backend snapshot/restore contract
- [./distributed-runtime.md](./distributed-runtime.md) - the JM/TM control plane, heartbeats, and deploy messaging
- [./jobs-and-scheduling.md](./jobs-and-scheduling.md) - slots, placement, and the job graph the restart path rebuilds from
- [./task-lifecycle.md](./task-lifecycle.md) - `LocalExecutor::start`, where restore-time migration runs before operators open
- [../connectors/README.md](../connectors/README.md) - per-connector source replay and exactly-once caveats across a restart or rescale
