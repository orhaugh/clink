# The exactly-once specification

> The protocol behind clink's exactly-once guarantee, written down as a TLA+ model that TLC checks on every push: what it states, what it proves within its bounds, the defects it has been shown to see, the defects it found, and what it leaves out.

## Overview

clink's exactly-once guarantee is the agreement of four mechanisms
documented on their own pages: barrier checkpoints that ack only after the
snapshot is durable ([checkpointing](checkpointing.md)); two-phase-commit
sinks that prepare at the barrier and commit on the coordinator's
`CommitCheckpoint` ([sink committer framework](sink-committer-framework.md));
the coordinator's completion protocol, with its `COMPLETED` and `CONFIRMED`
markers and its withheld broadcasts; and recovery, with restore-point
selection, in-doubt resolution, commit receipts, unresolved markers and the
pre-fence describe ([distributed runtime](distributed-runtime.md),
[fault tolerance](fault-tolerance-and-rescale.md)). The evidence for that
agreement was, until this page, entirely sampled: unit tests pin single
mechanisms, the multi-process gates pin named interleavings, and the
[qualification campaigns](../qualification/README.md) run the whole
protocol under injected faults for hours and judge the output against an
independent oracle. Each buys one fact about one schedule.

`formal/ExactlyOnce.tla` states the protocol whole, at the level of
checkpoint ids, subtasks, transactions, markers and receipts, and the TLC
model checker enumerates every interleaving of its steps and faults within
bounded configurations. Design record
[012](../design/012-machine-checked-exactly-once.md) is the decision; this
page is the reference. The claim it supports is stated carefully at the
end: TLC proves the model, and the model has been shown to see the defects
the rigs saw. Neither proves the code.

## Where it lives

| Path | What it is |
|------|------------|
| `formal/ExactlyOnce.tla` | The specification: state, actions, faults, invariants, liveness |
| `formal/models/` | The configurations CI checks, one `.tla` and `.cfg` pair each |
| `formal/mutants/` | One configuration per defect, and `expected.txt` saying what TLC must find for each |
| `formal/tools.env` | The pinned TLA+ tools and their SHA-256 checksums |
| `formal/trace/TraceExactlyOnce.tla`, `.cfg` | The trace module: follows a recorded protocol trace through the specification |
| `formal/trace/events.txt` | The trace vocabulary, a contract between the engine and the module |
| `formal/traces/` | Traces recorded from real runs, validated on every push |
| `include/clink/cluster/protocol_trace.hpp` | The emitter: `CLINK_PROTOCOL_TRACE_DIR` turns it on |
| `scripts/formal-check.sh` | Fetches and verifies the tools, runs TLC, judges models, mutants and traces (`--trace`) |
| `scripts/protocol-trace-merge.py`, `scripts/check-protocol-trace-events.py` | Merges per-process trace files; holds code, vocabulary and module in agreement |
| `.github/workflows/ci.yml`, jobs `formal` and `trace-validation` | Models, mutants and the recorded traces on every push; the traces each test run leaves, after the build (advisory until it fits its budget and every trace validates) |
| `formal/README.md` | The working guide: running, adding a model, adding a mutant, recording a trace |

## How it works

### The abstraction

There are no records, watermarks or channels in the model. Each checkpoint
interval is one logical **position** in the input. Checkpoint `c` cuts the
input at position `cutOf[c]`; a sink's transaction sealed for `c` covers
exactly that position; a restore rewinds the source to the restore point's
cut; and a sink's visible output is the multiset of positions its committed
transactions carry, less what replay suppression swallowed at emission.
Exactly-once is then a small set of statements about that multiset.

Two connector families share the module through one constant. The Kafka
family cannot re-execute a commit after the owning process dies (a broker
transaction is fenced or expires), so the job runs the commit-confirmed
restore protocol, commit receipts, in-doubt resolution and replay
suppression. The staged-artifact and XA family (file, Parquet, S3,
Postgres) re-commits its persisted handles idempotently at open and
restores from the newest completed checkpoint.

### The steps

The atomic steps are chosen so that every named fault point in
`include/clink/fault/fault_injection.hpp` is a distinct state between two
actions, and a process may die between any two of them. Prepare, ack, marker
write, broadcast, commit, receipt and confirmation are separate actions;
so are each stage of in-doubt resolution and of a sink's open. Every action
names, in its comment, the engine site it abstracts.

| Fault point | State in the model |
|---|---|
| `sink.before_prepare` | the barrier delivered, before `SinkPrepare` |
| `sink.after_prepare` | after `SinkPrepare`, before `SinkAck` |
| `coordinator.before_completed_marker` | every ack in, before `WriteCompleted` |
| `coordinator.after_completed_marker`, `coordinator.before_commit_broadcast` | the marker durable, before `Broadcast` |
| `sink.before_commit` | the commit accepted, before `SinkCommit` |
| `sink.between_commit_and_receipt` | after `SinkCommit`, before `SinkReceipt` |
| `sink.after_external_commit` | after `SinkReceipt`, before `SinkConfirm` |
| `checkpoint.before_write` and its siblings | `SinkPrepareFails`: the capture fails and the ack says so |

Faults are actions with a budget, so the checker may inject them or not:
worker death (the sinks on it lose their process state; their transactions
stay as the broker had them), coordinator death (memory lost, sessions
ended, reseeded from the durable markers on takeover), a superseded
coordinator that keeps triggering under a stale epoch, broker transaction
expiry, an unreachable broker, a snapshot capture that fails, and a
resolution walk cancelled by the watchdog. The broker is modelled as the
transaction coordinator the protocol depends on: a prepared transaction
that expires or is fenced aborts; a commit's outcome stays describable
until a successor transaction begins on the same identity or the identity
is fenced.

### The invariants

| Invariant | Statement |
|---|---|
| `NoDuplicate` | No sink publishes any position more than once |
| `NoLoss` | Every position at or below the cut of the newest confirmed checkpoint (completed, for the recoverable family) is published exactly once at every sink, or is held prepared with a handle the recoverable family's open will commit |
| `FrontierCovered` | Every position the source can no longer re-emit is published or held |
| `RestoreSound` | No restore ever read participant snapshots of mixed vintage |
| `ConfirmedMeansCommitted` | A `CONFIRMED` marker never outruns the commits it vouches for |
| `Fenced` | No worker acted on a frame from a superseded coordinator |
| `EventuallySettled` | With bounded faults and fair progress, the run quiesces with every vouched-for position published once (a temporal property) |

### The configurations

| Model | Family | Bounds | Result |
|---|---|---|---|
| `MC_KafkaSmall` | Kafka | 2 sinks on 2 workers, 3 checkpoints, 1 in flight, one of each fault | 23.3M distinct states, depth 78, all invariants hold, no deadlock |
| `MC_KafkaTwoInFlight` | Kafka | 2 checkpoints in flight, worker death and snapshot failure only | 42,059 distinct states, depth 67, all invariants hold |
| `MC_RecoverableSmall` | recoverable | 2 sinks, 3 checkpoints, 2 in flight, worker and coordinator death, snapshot failure | 12.6M distinct states, depth 55, all invariants hold |
| `MC_KafkaLiveness` | Kafka | 2 checkpoints, one of each fault | invariants and `EventuallySettled` hold, 4.5M distinct states |

Within its bounds each run is exhaustive: TLC visits every reachable state.
The bounds are small so that the push gate finishes in minutes; a larger
run is a deliberate act, not the gate.

## Trace validation

The model proves the model. Trace validation is the check that the code
behaves as the model says, on real runs: the engine records the protocol
steps it takes, and TLC follows that record through the specification.

### The protocol trace

Set `CLINK_PROTOCOL_TRACE_DIR` to a directory and every process
(coordinator, worker, or both in an in-process cluster) appends one JSON
object per protocol event to its own file there,
`<role>-<pid>-<start>.ndjson`. A job plugin loaded into a worker carries its
own copy of the runtime, so a sink inside one writes a second file for the
same process (`process-<pid>-...`); the validator merges them. Off, each
site costs one relaxed atomic load, the way fault points do. A line looks
like this:

```json
{"seq":12,"ts":1725555555123456,"proc":"coordinator:4242","event":"Trigger","job":1,"ckpt":3,"epoch":1}
```

`seq` is per process and monotonic, `ts` is microseconds since the Unix
epoch on that process's clock, and the rest is the event's own. The
vocabulary is `formal/trace/events.txt`; each event is one step of the
specification, or a stutter the trace module recognises.

| Event | Emitted by | Fields | Specification step |
|---|---|---|---|
| `Trigger` | coordinator, trigger loop | `job`, `ckpt`, `epoch` | `Trigger` (or `ZombieTrigger` from a superseded coordinator) |
| `DeliverBarrier` | worker, on `TriggerCheckpoint` | `job`, `ckpt`, `epoch`, `worker`, `fenced` | `DeliverBarrier` |
| `SinkPrepare` | the two-phase sink, transaction sealed | `sub`, `ckpt`, `family`, `staged` | `SinkPrepare` or `SinkPrepareFails`; the ack decides which |
| `SubtaskAck` | coordinator, on `SubtaskCheckpointed` | `job`, `sub`, `ckpt`, `ok` | `SinkAck` (a non-sink subtask's ack is a stutter) |
| `CoordComplete` | coordinator, last ack in | `job`, `ckpt`, `outcome` = `completed`, `failed`, `discarded` | `CoordComplete` |
| `WriteCompleted` | coordinator, marker fsync done | `job`, `ckpt` | `WriteCompleted` |
| `Broadcast` | coordinator | `job`, `ckpt`, `withheld` | `Broadcast` |
| `DeliverCommit`, `DeliverAbort` | the sink, on dispatch | `sub`, `ckpt`, `accepted` | `DeliverCommit`, `DeliverAbort` |
| `SinkCommit` | the sink, external commit executed | `sub`, `ckpt` | `SinkCommit` |
| `SinkReceipt` | the Kafka sink, receipt durable | `sub`, `ckpt` | `SinkReceipt` |
| `SinkConfirm` | worker, `CommitConfirmed` sent | `job`, `sub`, `ckpt` | `SinkConfirm` (Kafka family; a stutter otherwise) |
| `WriteConfirmed` | coordinator, `CONFIRMED` marker written | `job`, `ckpt` | `WriteConfirmed` |
| `WorkerDies` | coordinator, loss detected | `job`, `worker` | `WorkerDies` |
| `SubtaskDrained` | coordinator, survivor drained | `job`, `sub` | `SinkDrains` (non-sinks stutter) |
| `CoordRecovers` | the new leader, per recovered job | `job`, `epoch`, `completed`, `confirmed` | `CoordRecovers` (its `CoordDies` is a hidden step) |
| `RestartProceeds` | coordinator, restart held for resolution | `job`, `resolving`, `completed`, `confirmed` | `RestartProceeds` into `resolving` |
| `Redeploy` | coordinator, deploying: once per restart, after the deploy frames are built, whatever their number | `job`, `restore`, `next` | `RestartProceeds` (from the drain) or `Redeploy` (after resolution) |
| `WalkSkips`, `WalkReadsReceipt`, `WalkProbes`, `WalkRetries`, `WalkExhausted`, `WalkCancelled`, `WalkDecides`, `WalkFinishes` | the in-doubt walk | `job`, `ckpt`, and `sub`, `verdict`, `confirmed` where they apply | the walk's steps, one each |
| `SinkOpens` | the sink, `open()` done | `sub`, `family` | `SinkOpens` after a redeploy; the first open is the initial state |
| `Placement` | worker, task deployed | `job`, `sub`, `worker`, `source` | none: tells the module which worker hosts what |

Everything the engine cannot observe is not in the vocabulary: a
coordinator dying, a superseded coordinator stopping, the broker expiring
or losing a transaction. The trace module lets TLC take those as hidden
steps between events, within the fault budgets the trace itself implies
(one coordinator death per `CoordRecovers`, one expiry per refused probe,
and so on).

### Validating a trace

```bash
scripts/formal-check.sh --trace /path/to/run          # a run's per-process files
scripts/formal-check.sh --trace formal/traces         # every recorded run
```

The script merges a run's files in timestamp order (causally related
events from different processes are a network round trip apart, so on one
machine the clock orders them; a cluster spread over hosts needs the
reordering window the design record states), keeps one job, and runs TLC on
`TraceExactlyOnce.tla` with the trace module constraining the
specification's next-state relation to the recorded events in order. The
constants are read off the trace: the sinks are the subtasks that prepared
a transaction, the workers and the source's worker come from `Placement`,
the checkpoint bound from the highest id seen, the family from the sinks.
The run is accepted when some path through the specification consumes every
event (hidden steps make the state graph a small tree, and a branch that took
a hidden step the run did not need dies out without being a verdict); TLC's
postcondition reads the furthest event any path reached, and when that is
short of the end the script names the first event no allowed step produced.

Two kinds of trace are validated on every push. `formal/traces/` holds
runs recorded from the tests and committed, so a protocol change that makes
a recorded behaviour impossible fails the `formal` job even where the test
that produced it is Docker-gated. And the build job keeps every trace its
own test run leaves (the in-process protocol trace test, every
multi-process harness test) and the `trace-validation` job model-checks
them all, so the engine's behaviour under the faults the integration suite
injects is checked against the model on each commit, not only when a
fixture is refreshed. That job is advisory for now: it has yet to fit its
time budget or accept every trace it reaches, so it reports as a warning
rather than gating the run until both hold.

### What a divergence means

A divergence at event `k` says: from every state the first `k - 1` events
could have reached, the specification has no step that produces event `k`. Either the
engine took a step the protocol does not allow, which is a defect in the
engine, or the model abstracts something the engine legitimately does,
which is a gap in the model. Both are findings; the second is fixed in
`ExactlyOnce.tla` and the mutants keep it honest.

Known gaps, stated so a divergence there is read correctly: the model's
`Host` is fixed, so a sink that moves to another worker on redeploy is
outside it (the source may move; only sinks are pinned); the model has one
source, so a second source subtask's barrier is a stutter; a checkpoint
failed by a subtask that is not a two-phase sink has no sink failure for
the model to see; a commit broadcast withheld because the job was cancelled
is not modelled; and a `Placement` must precede a sink's first event, so a
trace started mid-run is not validated. The in-process happy path and the
multi-process fault tests stay inside those bounds.

Writing the module against real traces found three places where the model
was narrower than the engine, each fixed in the specification: a recovered
coordinator's id floor counts participant snapshots on disk as well as
markers (`SnapshotIds`); the source's worker can die with no sink beside it
and still restart the job (`WorkerDies` on `SrcWorker`); and the first
checkpoint after a redeploy is triggered as soon as the job is deployed,
before a sink's `open()` has returned, the barrier waiting in the sink's
input queue (`Trigger` and `DeliverBarrier` admit a sink that is `opening`).
None is an engine defect; each is a behaviour the engine has always had
that the model had not admitted, and the recorded traces under
`formal/traces/` now pin all three.

## The mutants

A model that proves its own invariants shows nothing until it is shown to
reject a wrong protocol. Every defect the qualification campaigns found and
fixed, and every defect this model found, is a value of the specification's
`Bug` constant that switches one rule back to its pre-fix form, at the rule
itself, so the specification also reads as the record of why each rule
exists. `scripts/formal-check.sh --mutants` runs TLC on each and judges the
outcome against `formal/mutants/expected.txt`. This is the calibration rule the
integration gates already live by: a gate that has not been shown to fail
against the bug it guards is decorative.

| Mutant | Rule it disables | Found by | Refuted |
|---|---|---|---|
| `broadcast_during_drain` | The commit broadcast is withheld while the job drains for a restart | qual01-20260818a | yes, `NoDuplicate` (since the specification admits a checkpoint triggered before a sink reopens; recorded as guarded before that) |
| `close_aborts_prepared` | A cancelled sink preserves its barrier-sealed prepared transaction | qual01-20260818a | no: guarded by the walk's refusal, the replay and receipts |
| `no_receipts` | The sink writes a durable commit receipt the instant the broker acknowledges | qual01-20260818b | yes, `NoDuplicate` |
| `stop_at_first_refusal` | The walk probes every handle of a checkpoint even after a refusal | qual01-20260819f | no: guarded by the marker rule the refusal-wall fix added |
| `no_materialised_receipts` | A commit the walk proves over the wire gets its receipt materialised | qual01-20260819f | yes, `NoDuplicate` |
| `blind_fence` | A reopening sink describes its unresolved orphan before it fences | qual01 rig night | yes, `NoDuplicate` |
| `receipt_after_begin` | The receipt is written before the successor transaction begins | qual01 rig night | yes, `NoDuplicate` |
| `restore_from_completed` | Jobs with a non-recoverable-commit sink restore from the newest confirmed checkpoint | before the commit-confirmed protocol | yes, `FrontierCovered` |
| `no_rewind_on_failed_checkpoint` | A FAILED checkpoint rewinds the job so its aborted interval is re-emitted | correctness sweep item 4 | yes, `NoLoss` |
| `broadcast_before_marker` | The `COMPLETED` marker is durable before any commit is broadcast | hardening round | yes, `NoDuplicate` |
| `id_reuse` | A recovered job numbers new checkpoints above every durable id | qual01-20260817c, 20260819g | yes, `ConfirmedMeansCommitted` |
| `no_fencing` | Workers drop control frames from an epoch below their bound | design | yes, by deadlock (a stale barrier seals a transaction nothing will commit) |
| `refusal_wall` | An early stop of the walk marks every unreceipted handle above it | this model | yes, `NoDuplicate` |
| `complete_above_failed` | A checkpoint above a FAILED one is discarded during the rewind | this model | yes, `NoLoss` |
| `restore_from_memory` | The in-memory restore point advances with the durable marker, not before | this model | yes, `FrontierCovered` |

Thirteen of the fifteen are refuted. The two that are not are recorded in
`formal/mutants/expected.txt` rather than deleted, and the check holds that
record in both directions: each of them disables a rule that a later rule
now guards as well. The preserved prepared transaction predates commit
receipts and in-doubt resolution, which repair the partial commit its
absence would produce; probe-all predates the marker rule the refusal-wall
finding added, which marks the unprobed handles for the sink's pre-fence
describe. Each remains in the engine as defence in depth, and the day TLC
refutes one of them the check fails, because the other guard has gone. The
withheld broadcast was recorded the same way until trace validation widened
the specification: once a checkpoint may be triggered before a sink has
reopened, a barrier can wait in a sink's queue across a drain, complete
during the resolution that follows, and, broadcast then, publish an
interval the restore below re-emits. That is the shape of
qual01-20260818a, and the model now refutes it on its own. The `no_fencing` mutant is judged by the correctness
invariants alone, its configuration dropping the `Fenced` ghost that would
merely restate it; TLC refutes it by deadlock, which is how the model
renders the engine's bounded wait and restart when a stale barrier seals a
transaction nothing will ever commit.

## What the model found

The model was written from the engine as shipped, and the first
configurations TLC ran against it produced counterexamples. Each was
checked against the code and confirmed as an interleaving the engine
reaches; each was fixed in the engine in the same change that introduced
the model, pinned by a deterministic test, and kept as a mutant. None needs
a fault the campaigns do not already inject; the two-hour QUAL-01 run did
not happen to hit them.

1. **The refusal wall.** In-doubt resolution stopped at the first
   checkpoint it refused and returned. A commit that had executed without
   its receipt (a kill in the ack window) in a completed checkpoint above
   the refused one was never proven, so the redeploy fenced it blind and
   the replay published its interval twice. Worse, the refused checkpoint
   stood as a wall every later walk stopped at, until a higher `CONFIRMED`
   marker landed by the normal path. The entrance is a completed checkpoint
   whose broadcast was withheld and whose transactions an outage left
   unresolved (the sinks' pre-fence describe then aborts them, correctly),
   followed one checkpoint later by an ack-window kill. The walk now leaves
   an `.unresolved` marker for every unreceipted handle above any early stop,
   and the owning sink's pre-fence describe settles each one. Pinned by
   `ResolutionFixture.ARefusalMarksTheUnreceiptedHandlesAboveIt`.
2. **The rewind floor.** The trigger loop does not wait for one checkpoint's
   acks before issuing the next, so a checkpoint above a FAILED one is
   routinely still collecting acks when the failure begins its rewind. One
   that finished collecting them during the drain completed and got its
   marker; in-doubt resolution then committed its transactions and confirmed
   it, and the job restored from it, past the aborted interval below it that
   only a rewind below the failed id re-emits. A checkpoint above the failed
   id is now discarded like the failed one, with no marker and an abort for
   its staged transactions, until the restart redeploys. Pinned by
   `CheckpointCompletion.ACheckpointAboveAFailedOneIsDiscardedDuringTheRewind`.
3. **The restore point ahead of its marker.** `latest_completed_checkpoint_id`
   advanced in memory under the lock at completion, with the `COMPLETED`
   marker written after the lock was released. A restart deciding its restore
   point in that window redeployed from a checkpoint the next coordinator
   could not see; when the coordinator then died before the fsync landed,
   its successor restored lower, and the recoverable sinks, which had already
   re-committed the unmarked checkpoint's handles at open, published its
   interval twice. Memory now advances where the marker becomes durable.

Two further counterexamples in the first runs were defects in the model,
not the engine (a producer identity kept in the sink's process record and
so lost with the process; control frames delivered out of order), and were
corrected as such. The distinction was drawn by reading the code at each
step of the trace, which is what the action comments are for.

## Guarantees and caveats

In the honesty categories the qualification pages use:

- **Demonstrated:** within the bounds of each configuration above, every
  reachable interleaving of the modelled protocol steps and faults satisfies
  the invariants, the run never deadlocks, and (in the liveness
  configuration) every run with bounded faults settles with every
  vouched-for position published exactly once. Thirteen of the fifteen
  mutants produce a counterexample; the two that do not are recorded as
  guarded by a later rule, and the check fails the day that stops being
  true.
- **Tested but bounded:** the bounds. Two sinks, three checkpoints, one or
  two in flight, one fault of each kind. A defect that needs three sinks,
  four checkpoints or two coordinator deaths in one run is outside what the
  push gate has enumerated.
- **Demonstrated, per run:** the recorded traces under `formal/traces/`
  and the traces every CI test run leaves are behaviours of the model
  (design record 012, increments 3 and 4). That is evidence about the runs
  recorded, at the model's abstraction, under the reordering assumption
  above; it is not a proof about runs not recorded.
- **Architecturally supported but not qualified:** a validated trace from
  a qualification rig alongside its campaign page (increment 5's tail). The
  rigs run the same binaries and the same switch turns tracing on; none has
  yet been run with it.
- **Unknown, by construction:** everything below the abstraction. Records
  and their values; watermarks and the exactness of replay suppression's
  horizon cut; source partition ownership (the QUAL-01 run C defect lived
  there and this model cannot see it); unaligned-checkpoint in-flight
  capture; rescale; the HA lock primitive and the metadata compare-and-set;
  network frame encoding; time. Each has its own evidence elsewhere.

What the model proves is the model. What the campaigns prove is one run of
the code. The two are different evidence for the same guarantee, and this
page is careful to keep them apart.

## Related

- [Design record 012](../design/012-machine-checked-exactly-once.md): why
  the specification exists and the increments still to land.
- [Checkpointing and barriers](checkpointing.md): the completion protocol
  the model's coordinator actions abstract.
- [Sink committer framework](sink-committer-framework.md): the two sink
  families.
- [Kafka connector](../connectors/kafka.md): receipts, replay suppression,
  the pre-fence describe.
- [Qualification](../qualification/README.md): the campaigns whose findings
  the mutants encode.
