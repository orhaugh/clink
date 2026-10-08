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
| `.github/workflows/ci.yml`, jobs `formal`, `formal-recoverable-small`, `formal-kafka-small`, `formal-mutants`, `formal-mutants-accepted`, `formal-mutant-id-reuse` and `trace-validation` | The models and the recorded traces, the mutants, and the traces each test run leaves, after the build (rescaled runs are skipped and counted, not judged). Models and mutants are separate jobs: the models run one at a time with every TLC worker, while the mutants are independent and want concurrency. The largest searches run alone on their own runners with every worker: `MC_RecoverableSmall`, `MC_KafkaSmall` and `M_id_reuse`; the accepted mutants, which enumerate their whole state space, have a job of their own |
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
write, broadcast, commit, receipt and confirmation are separate actions; so
are the Kafka sink's own finish of a commit (`SinkFinish`: the staged handle
erased, the open transaction resolved, the next prepare free to run) and the
worker's `CommitConfirmed` after it (`SinkConfirm`), since the worker sends
the confirmation only once the commit callback has returned and the sink's
task thread can seal the next checkpoint in between; so are the `CONFIRMED`
marker's put and the advance of the confirmed restore
point behind it (`WriteConfirmed`, `AdvanceConfirmed`), which the engine runs
in two holds of its lock with the put between them, and each stage of
in-doubt resolution and of a sink's open. Every action
names, in its comment, the engine site it abstracts.

| Fault point | State in the model |
|---|---|
| `sink.before_prepare` | the barrier delivered, before `SinkPrepare` |
| `worker.after_trigger_delivered` | the barrier delivered at the source, before `SinkPrepare`: the model has taken the source's capture, which in the engine has not landed yet |
| `sink.after_prepare` | after `SinkPrepare`, before `SinkAck` |
| `coordinator.before_completed_marker` | every ack in, before `WriteCompleted` |
| `coordinator.after_completed_marker`, `coordinator.before_commit_broadcast` | the marker durable, before `Broadcast` |
| `sink.before_commit` | the commit accepted, before `SinkCommit` |
| `sink.between_commit_and_receipt` | after `SinkCommit`, before `SinkReceipt` |
| `sink.after_external_commit` | after `SinkReceipt`, before `SinkFinish` |
| none (the commit callback returned, `CommitConfirmed` not yet sent) | after `SinkFinish`, before `SinkConfirm`: the Kafka sink may already have prepared the next checkpoint, and its next commit waits for the confirmation |
| `coordinator.before_confirmed_marker` | every tracked sink confirmed and the checkpoint out of tracking (`confirming[c] = "due"`), before `WriteConfirmed`: neither the marker nor the in-memory confirmed restore point has moved |
| `coordinator.before_in_doubt_walk` | a restart held for resolution, its walk's start fixed by `RestartProceeds`, before the walk's first step: a confirmation's advance landing here moves the in-memory confirmed restore point, not where the walk starts |
| `coordinator.takeover_after_marker_read` | a takeover's one read of the markers taken, before `CoordRecovers`: a stale `COMPLETED` put landing here is on disk, its superseded coordinator's line may come before the takeover's, and the takeover still reports and restores from the point it read |
| `coordinator.takeover_before_walk` | a takeover's `CoordRecovers` (and `RestartProceeds` when it resolves) taken, before its walk's first step: a stale `COMPLETED` put landing here is on disk, and the walk still ends at the takeover's `completed` |
| `checkpoint.before_write` and its siblings | `SinkPrepareFails`: the capture fails and the ack says so |

Faults are actions with a budget, so the checker may inject them or not:
worker death (the sinks on it lose their process state; their transactions
stay as the broker had them), coordinator death (memory lost, sessions
ended, reseeded from the durable markers on takeover), a superseded
coordinator that keeps triggering under a stale epoch, broker transaction
expiry, an unreachable broker, a snapshot capture that fails, and a
resolution walk cancelled by the watchdog. The job's restart budget is
bounded as well (`MaxJobRestarts`): a failed checkpoint rewinds the job while
budget is left, and once it is spent the job fails, with nothing completed
above the failed checkpoint. The broker is modelled as the
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
| `ConfirmedMemoryOnDisk` | For the Kafka family, the in-memory confirmed restore point is 0 or a `CONFIRMED` marker on disk. A commit's retention floor comes from memory and a takeover restores from the disk, so memory ahead of the disk would let workers purge the snapshots the takeover needs; the model does not represent the purge, so the order is held here |
| `Fenced` | No worker acted on a frame from a superseded coordinator |
| `EventuallySettled` | With bounded faults and fair progress, the run quiesces with every vouched-for position published once (a temporal property) |

### The configurations

| Model | Family | Bounds | Result |
|---|---|---|---|
| `MC_KafkaSmall` | Kafka | 2 sinks on 2 workers, 3 checkpoints, 1 in flight, one of each fault | 78.8M distinct states, depth 86, all invariants hold, no deadlock |
| `MC_KafkaTwoInFlight` | Kafka | 2 checkpoints in flight, worker death and snapshot failure only | 101,698 distinct states, depth 75, all invariants hold |
| `MC_RecoverableSmall` | recoverable | 2 sinks, 3 checkpoints, 2 in flight, worker and coordinator death, snapshot failure | 170.2M distinct states, depth 59, all invariants hold |
| `MC_RecoverableNoBudget` | recoverable | as `MC_RecoverableSmall`, with no restart budget: a failed checkpoint fails the job, and no error restarts | 8.9M distinct states, depth 53, all invariants hold |
| `MC_KafkaLiveness` | Kafka | 2 checkpoints, one of each fault | invariants and `EventuallySettled` hold, 7.4M distinct states |

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
| `Trigger` | coordinator: the trigger loop, a savepoint (`savepoint`), a hot rescale's cut checkpoint (`cutover`, in a run outside the specification's scope), or a source's end-of-input final id (`final`) | `job`, `ckpt`, `epoch` | `Trigger` (or `ZombieTrigger` from a superseded coordinator); diverges when another coordinator that can still reach a worker has triggered the same id |
| `DeliverBarrier` | worker, on `TriggerCheckpoint` | `job`, `ckpt`, `epoch`, `worker`, `fenced` | `DeliverBarrier` |
| `SinkPrepare` | the two-phase sink, transaction sealed | `sub`, `ckpt`, `family`, `staged` | `SinkPrepare` or `SinkPrepareFails`; the ack decides which |
| `SubtaskAck` | worker, as `SubtaskCheckpointed` is sent (the subtask's own step, so it precedes the subtask's next prepare in the merged trace) | `job`, `sub`, `ckpt`, `ok` | `SinkAck` (a non-sink subtask's ack is a stutter) |
| `CoordComplete` | coordinator, last ack in | `job`, `ckpt`, `outcome` = `completed`, `failed`, `discarded`, `epoch` | `CoordComplete`. From a superseded coordinator's epoch, a `completed` decision on acks sent before the supersession is a stutter: the supersession turned that checkpoint's put stale (`DecidedLate`). A late `failed` or `discarded` decision diverges, as does one on an ack sent after the supersession by a worker not yet re-registered |
| `WriteCompleted` | coordinator, in the hold after the marker fsync that advances the completed restore point, before the `after_completed_marker` fault point; not emitted when the job redeployed while the put was out, and the advance is skipped while the restart is held for in-doubt resolution | `job`, `ckpt`, `epoch` | `WriteCompleted` for the leader. From a superseded coordinator's epoch, the stale put landing (`StaleCompletedLands`), or a stutter when the takeover already read the marker; before the takeover's line, a stutter with the put left stale |
| `Broadcast` | coordinator | `job`, `ckpt`, `withheld`, `epoch` | `Broadcast` for the leader; a stutter from a superseded coordinator |
| `DeliverCommit`, `DeliverAbort` | the sink, on dispatch | `sub`, `ckpt`, `accepted` | `DeliverCommit`, `DeliverAbort` |
| `SinkCommit` | the sink, external commit executed | `sub`, `ckpt` | `SinkCommit` |
| `SinkReceipt` | the Kafka sink, receipt durable | `sub`, `ckpt` | `SinkReceipt` |
| `SinkConfirm` | worker, `CommitConfirmed` sent, after the subtask's commit callbacks have returned | `job`, `sub`, `ckpt` | `SinkConfirm` (Kafka family; a stutter otherwise), with the sink's `SinkFinish` before it as a hidden step |
| `WriteConfirmed` | coordinator, in the hold that advances the confirmed restore point, after the `CONFIRMED` marker's put | `job`, `ckpt`, `epoch` | `WriteConfirmed` and `AdvanceConfirmed`, back to back, for the leader. From a superseded coordinator's epoch (it does not know it was superseded, and emits the line as the leader would), the stale put landing, or a stutter when the takeover already read the marker |
| `WorkerDies` | coordinator, loss detected, or a superseded session retired after its worker re-registered | `job`, `worker`, and `spared` when a retirement leaves some of the worker's subtasks running | `WorkerDies` (any worker of the job; the model keeps only the source and the sinks, so the dead set may be empty). Validation takes the sinks the loss found on that worker from the run's own `Placement` events, not from the model's fixed hosts: a redeploy re-places a subtask, and a sink that has moved off the worker survives its death. A retirement kills only what the superseded session held; `spared` lists the worker's subtasks placed on a later session, which the engine drains as survivors, and validation does not kill them. It also lists the subtasks of an earlier session when the lost session was still retiring it: the predecessor's queued frames drain them, or the retirement's own `WorkerDies` kills them |
| `RestartOnError` | coordinator, a whole-job restart begun for a subtask error or an unattributed transport failure | `job`, `cause` | `RestartOnError` (the survivors drain; a loss declared during the drain folds in as its own `WorkerDies`) |
| `SubtaskDrained` | coordinator, survivor drained | `job`, `sub` | `SinkDrains` (non-sinks stutter). A sink the last redeploy placed but that has not finished opening may be among the survivors or not, according to whether its deploy had landed when the loss was declared; the model takes both, since forcing it in removed the shape a mutant's counterexample needs |
| `CoordRecovers` | the new leader, per recovered job | `job`, `epoch`, `completed`, `confirmed`: the newest `COMPLETED` marker in the job's own checkpoint directory, and the confirmed restore point the takeover adopts from it, read once (the newest `CONFIRMED` marker the restore read; 0 when it belongs to an earlier run than the current run's base, or when the job has confirmed nothing of its own and restores from the savepoint it was submitted with) | `CoordRecovers` (its `CoordDies` is a hidden step); the reseeded completed restore point must equal `completed`, and for the Kafka family the reseeded confirmed restore point must equal `confirmed` |
| `RestartProceeds` | coordinator, restart held for resolution | `job`, `resolving`, `completed`, `confirmed` | `RestartProceeds` into `resolving` |
| `Redeploy` | coordinator, deploying: once per restart, after the deploy frames are built, whatever their number | `job`, `restore`, `next` | `RestartProceeds` (from the drain) or `Redeploy` (after resolution) |
| `Renumber` | coordinator, a claim on the job's next checkpoint id refused because the record already holds it | `job`, `next`, `epoch` | `Renumber` (the leader, past a superseded coordinator's claims) or `ZombieRenumber` (the superseded coordinator, under its own epoch, past the leader's) |
| `WalkSkips`, `WalkReadsReceipt`, `WalkProbes`, `WalkRetries`, `WalkExhausted`, `WalkCancelled`, `WalkDecides`, `WalkFinishes` | the in-doubt walk | `job`, `ckpt`, and `sub`, `verdict`, `confirmed` where they apply | the walk's steps, one each |
| `SinkOpens` | the sink, `open()` done | `sub`, `family` | `SinkOpens` after a redeploy; the first open is the initial state |
| `Placement` | worker, task deployed | `job`, `sub`, `worker`, `source` | none: tells the module which worker hosts what |
| `Rescale` | coordinator, a replan staged or a hot cutover armed | `job`, `op`, `from`, `to`, `mode` | none: a scope marker. The model keys a sink by its subtask index and fixes the set and its hosts for the run; a rescale changes both, so the validator skips the run and counts it, until the model covers rescale |

Everything the engine cannot observe is not in the vocabulary: a
coordinator dying, a superseded coordinator stopping, the broker expiring
or losing a transaction. The trace module lets TLC take those as hidden
steps between events, within the fault budgets the trace itself implies
(one coordinator death per `CoordRecovers`, one expiry per refused probe,
and so on). One step the engine could record has no line either: the Kafka
sink's `SinkFinish`, between its `SinkReceipt` and the worker's
`SinkConfirm`. The module takes it as a hidden step only where the next
event needs it, that sink's `SinkConfirm` or its next `SinkPrepare`.

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
the checkpoint bound from the highest id seen (a redeploy's next id
included), the family from the sinks.
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
fixture is refreshed. A run that rescaled an operator carries a `Rescale`
scope marker and is skipped and counted rather than judged: the model keys
a sink by its subtask index and fixes the set and its hosts for the run. A
savepoint is a checkpoint the trigger loop did not start, and records its own
`Trigger` (marked `savepoint`); before it did, a run that took one diverged at
that checkpoint's completion, an id nothing had triggered.
`formal/traces/savepoint-between-checkpoints`, recorded from
`Cluster.ASavepointSurvivesTheCheckpointsTakenAfterIt`, pins it.
The merge script also writes the model's constants (the sink set, the
hosts, the fault budgets, the checkpoint range) as a generated module of
literals, `TraceConstants.tla`, which the trace module extends from TLC's
library path, and the module reads the current event into a state variable
once per step. Deriving both from the trace inside TLC re-evaluated the
whole trace on every reference and made validation quadratic in its length
(180 events in 6 seconds, 900 in 120, 1,500 in 300); as literals the check
is linear and a 1,500-event trace takes seconds. `TRACE_JOBS` runs that
many traces at a time, each with one TLC worker, and prints the reports in
trace order; `CHECK_JOBS` does the same for the models and the mutants,
which is how the `formal` job fits its budget, since TLC scales sublinearly
across workers and these runs are independent.

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

A fourth came from a run of
`KafkaWindowRecoveryTest.WorkerAndHaCoordinatorFailoverKeepSourceWindowAndSinkOnOneCut`,
whose trace recorded a Kafka sink's `SinkPrepare` for checkpoint N+1 before
its worker's `SinkConfirm` for N. The model had one step for both ends of a
commit: the handle erased, the open transaction cleared and the
confirmation counted at the coordinator, so the next prepare could only
follow the confirmation. The engine splits them across two threads. The
commit dispatch runs `on_commit(N)`, whose tail erases the handle and
resolves the open transaction, waking the task thread that waits in
`on_barrier(N+1)`; the worker sends `CommitConfirmed` only after the
callback returns. The specification now takes the two as `SinkFinish` and
`SinkConfirm`, with the checkpoint awaiting confirmation kept in the sink's
`confirmDue`. `CanPrepare` admits a Kafka sink that has finished,
`DeliverCommit` still holds the next commit frame until the confirmation
(the dispatch is one FIFO thread per worker and takes the next
`CommitCheckpoint` only after it), and an abort, which the worker handles on
its reader thread, is accepted in between. TLC explores a worker death and
a coordinator takeover in the new window, after the next prepare and before
`CommitConfirmed`, and finds no violation: the receipt on disk proves the
commit to the in-doubt walk, and the next checkpoint's snapshot no longer
holds the erased handle. `formal/traces/kafka-prepare-before-confirmation`,
synthetic, pins the order.

Trace validation has since caught an engine defect. A takeover numbered its
new checkpoints above the markers and the snapshot files it could see, and a
CI run of `HaFailoverTest.ARecoveredJobParkedForCapacityRunsWhenAWorkerReturns`
recovered from checkpoint 1 and numbered its next checkpoint 2, although the
dead leader's barrier for 2 had reached the worker. The trace diverged at the
`Redeploy`, whose `next` the specification places above every capture that
began. The gap is real in the engine: a worker that outlives the leader can
still write that capture after the new leader has read the directory, at the
path the new run's capture of the same id writes. The coordinator now claims
every id in `_jobs/<job>/TRIGGERED` before it allocates it, and a takeover
numbers above the record as well ([checkpointing](checkpointing.md)). The
trace from that run still diverges, as it should, and
`HaFailoverTest.ATakeoverNumbersAboveABarrierTheDeadLeaderDelivered` holds the
same window open with a worker fault point, `worker.after_trigger_delivered`.

The specification states the rule as a range rather than a value. A fresh
leader's next id lies anywhere from one above every marker and every capture
that began to one past the most any dead or superseded leader can have
claimed: each job's claimer puts the next id on record as soon as the
previous one is allocated, so the record runs at most one id ahead of its
coordinator's counter, and where a crash falls between a claim, its
allocation and its send decides what the next leader finds. Every id in the
range is safe in the model, since none above the lower bound was delivered
and the model binds every worker to the new epoch at the takeover, so a
superseded coordinator's barriers are fenced. The model keeps no record
variable, which would add an interleaving point to every trigger and admit
nothing the range does not; the trace module pins the engine's choice with
the event's `next`, and the lower bound is what refuses a reuse. The model
cannot show the harm itself, because it takes a capture as durable once its
barrier is delivered, so the late writer is below its abstraction; the
lower bound counts captures that began for that reason. No mutant comes
with the record: the lower bound was already the specification's rule
(`id_reuse` refutes weakening it to the restore point), and the defect was
the engine falling short of it, which is what trace validation is for.
Refuting a floor that misses a capture still in flight would take the
source's capture as a durable step of its own that can land after the
coordinator has died, and the record as a variable, in every checkpoint of
every model.

The engine does not bind every worker at the takeover. A worker binds the
new epoch when it re-registers with the new leader, which recovers its jobs
once registrations settle, so a worker still on a superseded coordinator's
connection can accept that coordinator's barriers after the redeploy. The
record keeps the two coordinators' ids apart: a claim lands only above what
the record holds, so an id is claimed once, and a coordinator whose claim is
refused numbers above the record instead and allocates nothing that round.
The specification admits that as two steps, `Renumber` for the leader,
bounded by what the superseded coordinator's counter can have claimed, and
`ZombieRenumber` for the superseded coordinator once a leader of a later
epoch has redeployed and claimed past it, whether or not that leader is still
alive; the engine records both as a `Renumber` event under the
coordinator's own epoch, and the trace module diverges on a renumber past
every id the other coordinator can have claimed, on one with no superseded
coordinator to have claimed past it, and on an unfenced one. No mutant comes
with the renumbering either, since the capture it keeps apart is one the
model, binding every worker at once, cannot reach.

What the claims and the renumber exist for, that a leader and a superseded
coordinator never trigger the same id, the specification does not hold
itself: its superseded coordinator triggers any id, which is harmless there,
since the model fences its barriers at the takeover. The trace module checks
it instead, where it costs the model searches nothing. It keeps a ghost of
the ids triggered by each coordinator that can still reach a worker, under
that coordinator's epoch, and a `Trigger` whose id another of them has
triggered diverges, whichever triggers it second. A leader that dies takes
its ids out of the ghost, since its barriers die with it: a takeover may
still number from an id the dead leader triggered and never delivered, which
the range above admits and which `formal/traces/kafka-coordinator-failover`,
recorded before the record existed, does. A trace whose leader triggers an
id the superseded coordinator triggered, before the takeover or after it, or
whose superseded coordinator triggers an id the leader did, diverges at the
second `Trigger`.

`Renumber` and `ZombieRenumber` stay in the specification, although no rule
depends on what they add to the model searches (a gap in a coordinator's ids
while its checkpoints are in flight) and they enlarge them, `M_id_reuse` the
most, which is why it runs in a CI job of its own. They are the steps the
engine records as `Renumber`, and trace validation's claim is that every
recorded event is a step of the specification the model checks cover: moved
into the trace module alone, each recorded renumber would be validated
against a step no model check had explored.

A claim whose write landed and whose answer was lost no longer renumbers:
the record names the claimant whose claim put each id there, and a retry
that finds its own claim at exactly the id it asked for takes the id as
claimed. Two renumbers remain outside the model, and a trace diverges at
either. A fresh job whose record could not be read at deploy claims from 1,
and renumbers above whatever an earlier run of the job left on record, with
no superseded coordinator behind it. And the end-of-input request for a
final id renumbers while a restart drains as well, where the model renumbers
only a running job. No recorded trace has either shape.

A divergence can also mean the trace is missing a line. The coordinator
records `WriteCompleted` after the COMPLETED marker's durable write, so the
line never claims a marker that is not on disk, and a kill landing between
the two leaves the marker without its line. The next coordinator's
`CoordRecovers` then reports a completed checkpoint the trace never saw
written, and its redeploy from that checkpoint looks impossible. The trace
module takes that marker as a hidden step, `LostWriteCompleted`, admitted
only when the next event is a takeover whose own read of the disk found
exactly the checkpoint whose marker was due, so a recovery reporting any
other checkpoint still diverges. `formal/traces/coordinator-killed-after-marker`
is the run that showed it (a coordinator and a worker killed together, in
`FaultRecoveryTest.CoordinatorAndWorkerDyingTogetherStillCommitsExactlyOnce`).

A `COMPLETED` put can also land with no line at all. The put runs outside the
coordinator's lock, and a restart can redeploy while it is out: the marker
still lands, but the hold after it sees that the job's run has moved on and
neither advances the completed restore point nor emits `WriteCompleted`. A
superseded coordinator's put is stale too, though its handler does not know
it: it advances its own memory, emits `WriteCompleted` and broadcasts, and
the lines carry its epoch, so the trace module reads `WriteCompleted` as the
stale put landing and `Broadcast` as a stutter
(`formal/traces/zombie-completion-after-takeover` and
`zombie-completion-before-takeover-line`). It can also decide a checkpoint
after the takeover: every ack was sent before the supersession, but the last
one waited on its connection's dispatch thread behind a slow store write on
the same connection, the stall that cost the lease. The supersession turns
every checkpoint decidable with all acks ok at that moment stale as well
(`DecidedLate`), and those puts land in id order after the put the coordinator
still had out (`lateBatch`), since each decision waits behind the landing below
it; landing them in any order let TLC find a `NoLoss` violation the engine
cannot produce. The trace module reads the late `CoordComplete` as a stutter
and its `WriteCompleted` as the stale put landing
(`formal/traces/zombie-decision-after-takeover` and
`zombie-decision-behind-stalled-marker`, synthetic, are accepted and diverge
at the late `CoordComplete` without that branch). The specification models
that put: a `Redeploy` or a supersession turns a `COMPLETED` put still out
stale (`staleCompleted`), and `WriteStaleCompleted` lands it with no advance
and no broadcast. Whatever reads the disk afterwards sees it, so a takeover
can restore from a superseded run's checkpoint, and the in-doubt walk can probe
the handles of one, and TLC checks both against the invariants. The trace
module takes the landing as a hidden step only when an event witnesses it: a
takeover whose `completed` names that checkpoint (`LostWriteCompleted`), or a
walk event other than `WalkSkips` at that checkpoint, since the walk reads the
marker before it emits any of them (`LostStaleCompletedAtWalk`).
`CoordRecovers` pins the reseeded completed restore point to the event's
`completed`, so a marker the model lacks diverges at the takeover.
`formal/traces/completed-marker-landing-after-redeploy`, a synthetic run in
which a sink's worker dies with checkpoint 2's put out, the job redeploys from
1, the put lands and a takeover then restores from 2, is accepted, and
diverges at the takeover's redeploy without the stale put.
`formal/traces/completed-marker-walked-after-redeploy` is the Kafka family's
shape, also synthetic: the new run completes 3 and restarts unconfirmed, and
its walk probes checkpoint 2's handle, which the reopened sink fenced. It is
accepted, and diverges at that probe without the stale put or without the
walk's witness.

The `CONFIRMED` marker can be on disk without its line too, in more ways. The
coordinator puts it outside its lock and then takes the lock again to advance
`latest_confirmed_checkpoint_id` and emit `WriteConfirmed`; a kill between the
two loses the line, and a restart that redeployed while the put was out
leaves the marker on disk with neither the memory nor the trace moved,
because the confirmation belongs to the run before the restart. The
specification takes the three steps the engine takes (the hold that drains
the confirmation set, inside `SinkConfirm`; the put, `WriteConfirmed`; the
advance, `AdvanceConfirmed`), so a `Redeploy`, a `RestartProceeds` that holds
the job for resolution, or a takeover can fall between them, and a takeover
seeds its confirmed restore point from whatever the put left on disk. A
redeploy turns a confirmation whose put is still out stale: the put lands,
and nothing advances after it. A superseded coordinator's puts turn stale
the same way and may land after the next leader has read the disk. The
trace module matches a `WriteConfirmed` line against the put and the advance
back to back, and takes a marker with no line as a hidden step,
`LostWriteConfirmed`, admitted on the same terms as `LostWriteCompleted`: the
next event is a takeover whose `confirmed` names exactly that checkpoint.
`CoordRecovers` pins the reseeded confirmed restore point to the event's
`confirmed` for the Kafka family, so a marker the model lacks, or one it has
and the engine did not find, diverges at the takeover rather than at the
next redeploy. `formal/traces/kafka-coordinator-failover` with the first
coordinator's `WriteConfirmed` line removed is accepted through
`LostWriteConfirmed` and diverges without it, and the same trace with its
takeover reporting a different `confirmed` diverges at the takeover.

The in-doubt walk writes its own `CONFIRMED` marker before its `WalkDecides`
line, so a kill between the two leaves a marker no line reports and that
`LostWriteConfirmed` cannot take, since no confirmation set was draining for
it. The hidden step `LostWalkDecides` takes the walk's success there, on the
same terms: the next event is a takeover whose `confirmed` is the checkpoint
being walked, and every handle of that checkpoint was proven committed.
`formal/traces/walk-confirmed-then-killed`, `kafka-kill-after-broker-commit`
cut after the walk's last proof with a takeover appended, is accepted through
it and diverges without it. A superseded coordinator's confirmation handler
runs on, puts the marker, advances its own memory and emits `WriteConfirmed`;
the line carries the coordinator's epoch, and from a non-leader epoch the
trace module takes it as the stale put landing, or as a stutter when the put
landed before the takeover read the disk.
`formal/traces/zombie-confirmation-after-takeover`, a synthetic cut of
`kafka-coordinator-failover` in which the first coordinator's line comes after
the takeover, is accepted, and diverges when the line is read as the
leader's. The takeover reads the disk before it emits `CoordRecovers`, so the
superseded coordinator's put can also land after that read with its line
written before the takeover's: the trace module takes that line as the
supersession already made and the stale confirmation's stutter, leaving the
confirmation stale for a later takeover that reads the marker.
`formal/traces/zombie-confirmation-before-takeover-line`, the same trace with
the line moved before the takeover's, is accepted and diverges without that
branch.

A takeover need not advance the epoch. The engine stamps epoch 0 when there
is no leader election. An HA leader's epoch is always above the one it
displaced, so it is at least 1. An embedded `clink run` resuming from its own
checkpoints after a kill records exactly that: a `CoordRecovers` at epoch 0,
with the previous coordinator gone only because its process died. The trace
module reads such an event as unfenced (`Unfenced`). It pins no model epoch,
because the model still advances its own at the recovery, and it admits no
zombie instead: an epoch-0 `Trigger` or `CoordRecovers` is taken only on a
path where no coordinator has been superseded. That is tighter than the
previous reading, which let an epoch-0 `Trigger` match a zombie's.
`formal/traces/embedded-resume-after-kill` is the run
(`EmbeddedResumeKafka`, two `clink run` processes, the first SIGKILLed), and
the same trace with its `Redeploy` rewritten to restore nothing, which is the
behaviour before resume, diverges there. An embedded run that starts from
empty state in a directory an earlier run used numbers its checkpoints above
the earlier run's without a recovery event, and the model's first checkpoint
is 1, so such a run's trace is not yet validatable. No recorded trace has
that shape.

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
| `sail_on_without_budget` | A FAILED checkpoint with no restart budget left fails the job instead of carrying on | framework review | yes, `NoLoss` |
| `advance_before_confirmed_put` | The in-memory confirmed restore point advances only once its `CONFIRMED` marker is durable | review of the per-connection dispatch change | yes, `ConfirmedMemoryOnDisk` |

Fifteen of the seventeen are refuted. The two that are not are recorded in
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
  vouched-for position published exactly once. Fifteen of the seventeen
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
  capture; rescale, including which new subtask finalises an old subtask's
  prepared transactions (the model keys a sink by a fixed index; the
  one-successor rule is held by `RescaleParentMapping.EveryOldSubtaskHasExactlyOneSuccessor`
  over every factor and by restore tests on the `file://` and `rocksdb://`
  backends); the HA lock primitive and the metadata compare-and-set; a
  worker still bound to a superseded coordinator's epoch after a takeover
  (the model binds every worker at the takeover, so it fences that
  coordinator's frames at once; in the engine the checkpoint-id record keeps
  the two coordinators' checkpoint ids apart, which trace validation checks
  of every recorded run, and nothing keeps the superseded coordinator's
  other frames from such a worker); network frame
  encoding; time. Each has its own evidence elsewhere. A snapshot that
  fails in an operator rather than a sink is outside trace validation too: the
  trace module skips acks from subtasks it does not model, so such a run
  diverges at the failed checkpoint's decision.

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
