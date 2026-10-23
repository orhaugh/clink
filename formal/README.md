# formal: the exactly-once protocol, model-checked

`ExactlyOnce.tla` is clink's exactly-once protocol written down as a TLA+
model and checked by TLC on every push. It states, in one place, what the
coordinator's completion and confirmation rules, the two-phase-commit sinks,
in-doubt resolution and recovery promise between them, and it enumerates
every interleaving of those steps and the faults the qualification campaigns
inject, within bounded configurations. Design record 012
(`docs/design/012-machine-checked-exactly-once.md`) says why it exists; the
published page (`docs/internals/exactly-once-specification.md`) says what it
proves and what it does not. This file is the working guide.

## Running it

```bash
scripts/formal-check.sh                    # every model under formal/models/
scripts/formal-check.sh MC_KafkaSmall      # one model
scripts/formal-check.sh --mutants          # every mutant under formal/mutants/
scripts/formal-check.sh --trace traces/    # every recorded protocol trace
scripts/formal-check.sh --trace /tmp/run   # a run's per-process trace files
```

The script needs a Java 11+ runtime and nothing else. It fetches the TLA+
tools pinned in `tools.env` (SHA-256 verified, cached under
`CLINK_FORMAL_TOOLS_DIR`, default `~/.clink-deps/formal-tools`) and runs TLC
with deadlock checking on. Knobs: `CHECK_JOBS` (how many models or mutants run
at a time, default 1), `TRACE_JOBS` (the same for traces), `TLC_WORKERS`
(default `auto`, or one worker per run when several run side by side),
`TLC_HEAP` (default `2g`), `TLC_EXTRA` for further TLC flags. TLC scales
sublinearly across workers, so N independent checks finish sooner than one
N-way check on the same cores, but only where no single run dominates. In
`.github/workflows/ci.yml` the `formal` job therefore runs the models one at
a time with every worker, and the separate `formal-mutants` job runs the
refuted mutants four at a time with one worker each. The accepted mutants
(`formal/mutants/expected.txt`) enumerate their whole state space, over an hour
each, and run two at a time in the `formal-mutants-accepted` job. One search in
each set dominates it and runs alone on its own runner with every worker:
`MC_RecoverableSmall` (about an hour) in the `formal-recoverable-small` job,
`MC_KafkaSmall` in the `formal-kafka-small` job, and `M_id_reuse`, which allows
two coordinator deaths, in the `formal-mutant-id-reuse` job.

A model is green when TLC reports no invariant violation, no deadlock and
no temporal-property violation. A mutant is judged against
`mutants/expected.txt`: one marked `refuted` is green when TLC **does**
report a violation (the script fails a mutant TLC accepts, because that
means the model can no longer see the defect it re-introduces); one marked
`accepted` records a rule a later rule now guards as well, and is green only
while TLC still accepts it.

## Layout

| Path | What it is |
|---|---|
| `ExactlyOnce.tla` | The specification: state, actions, faults, invariants, liveness |
| `models/MC_*.tla`, `models/MC_*.cfg` | The configurations CI checks (constants, invariants, properties) |
| `mutants/M_*.tla`, `mutants/M_*.cfg`, `mutants/expected.txt` | One configuration per `Bug` value, and what TLC must say about each |
| `trace/TraceExactlyOnce.tla`, `trace/TraceExactlyOnce.cfg` | The trace module: the specification constrained to a recorded run |
| `trace/events.txt` | The protocol trace vocabulary the engine emits and the module consumes |
| `TraceConstants.tla` (generated, untracked) | The model's constants for one trace as literals, written by `protocol-trace-merge.py --constants` beside the merged trace and placed on TLC's library path; the trace module extends it |
| `traces/<run>/*.ndjson` | Recorded runs, validated on every push |
| `tools.env` | The pinned TLA+ tools and their checksums |
| `../scripts/formal-check.sh` | Fetch, verify, run, judge |
| `../scripts/protocol-trace-merge.py` | Merge a run's per-process files into one ordered trace |
| `../scripts/check-protocol-trace-events.py` | Code, vocabulary and module agree |

## The model in brief

There are no records, watermarks or channels. Each checkpoint interval is
one logical **position** in the input. Checkpoint `c` cuts the input at
`cutOf[c]`; a sink's transaction sealed for `c` covers exactly that position;
a restore rewinds the source to the restore point's cut; a sink's visible
output is the multiset of positions its committed transactions carry, less
what replay suppression swallowed at emission. The invariants are then:

| Invariant | Statement |
|---|---|
| `NoDuplicate` | No sink publishes any position more than once |
| `NoLoss` | Every position at or below the cut of the newest confirmed checkpoint (completed, for the recoverable family) is published exactly once at every sink, or held prepared with a handle the recoverable family's open will commit |
| `FrontierCovered` | Every position the source can no longer re-emit is published or held |
| `RestoreSound` | No restore ever read participant snapshots of mixed vintage |
| `ConfirmedMeansCommitted` | A CONFIRMED marker never outruns the commits it vouches for |
| `ConfirmedMemoryOnDisk` | For the Kafka family, the in-memory confirmed restore point is 0 or a CONFIRMED marker on disk: a commit's retention floor comes from memory and a takeover's restore point from the disk, so memory ahead of the disk would let workers purge the snapshots the takeover restores |
| `Fenced` | No worker acted on a frame from a superseded coordinator |
| `EventuallySettled` (liveness) | With bounded faults and fair progress the run quiesces with every vouched-for position published once |

A sink of the recoverable family may find an interval **empty**: nothing of
that position reached it, so it seals no transaction for the checkpoint,
persists no handle, acks the capture, and later declines the commit and the
abort for it (`SinkPrepareEmpty`, `SinkPrepareEmptyFails`,
`LostSinkPrepareEmpty`). The ghost `emptyAt` records each position a sink
found empty; it is the input's property, not the attempt's, so a position
once sealed full is never found empty and one found empty is never sealed
full, and a replay after a restore meets the same answer. `NoLoss`,
`FrontierCovered` and `Settled` ask nothing of a sink at a position it found
empty. `MaxEmptyIntervals` bounds how many (sink, position) pairs a search
may find empty, zero in every configuration but `MC_RecoverableSparse`, so
the other searches are unchanged; the trace validator counts them off the
run. The Kafka sink seals a transaction every barrier, records or none, and
never takes the step. A fraud-detection job recorded against the
specification found the gap: nine alerts in ninety days leave almost every
interval empty for a Postgres sink, and the first commit such a sink
declined was the first step the module could not explain.

Two connector families share the module through the `Recoverable`
constant: the Kafka family (a broker transaction dies with its producer
unless resolved with the saved identity, so the job runs the
commit-confirmed restore protocol, receipts, in-doubt resolution and replay
suppression) and the staged-artifact / XA family (file, Parquet, S3,
Postgres: a persisted handle is re-committed idempotently at open, every
other prepared transaction found there is rolled back, and restores select
the newest completed checkpoint the job's newest deploy did not restore
below).

Faults are actions with a budget, so the checker may inject them or not:
worker death, coordinator death, a superseded coordinator that keeps
triggering, broker transaction expiry, an unreachable broker, a snapshot
capture that fails, and a cancelled resolution walk. Everything else is
weakly fair. The job's restart budget is a bound of its own
(`MaxJobRestarts`): a failed checkpoint rewinds the job while budget is
left, and fails the job when none is.

A worker loss is the coordinator's declaration, and the process need not be
dead (`WorkerLost`). The coordinator declares a worker lost for heartbeat
silence, for an outbound stall or for an outbound queue overflow; it posts
nothing more to the session and drops whatever the session sends. Frames
already handed to the kernel still reach the process, which executes them
before it reads the end of the session. `WorkerDies` is the process gone;
`WorkerLostRunning` is the process still running, and `MaxLostRunning` bounds
how many losses may be of that kind. Each sink the running process hosted
becomes an old incarnation (`lost[s]`, beside the live `sink[s]` the restart
deploys). It executes only the frames on its wire at the declaration
(`lostCommits`, `lostAborts`) and the barriers already queued to it, and seals
or commits accordingly (`LostSinkPrepare`, `LostSinkCommit`, `LostSinkReceipt`,
`LostDeliverAbort`). If the process hosts the source, its
`TriggerCheckpoint` frames stay deliverable there (`LostDeliverBarrier`): the
barrier reaches the run's sinks over the data plane until the redeploy rewires
it, and the checkpoint can never complete, since the coordinator drops the
source's ack. Nothing the process sends counts, so acks and `CommitConfirmed`
have no step. A hidden step stops each old incarnation (`LostSinkStops`,
`LostSourceStops`), and one that can do nothing more is taken as stopped
(`Settle`), which keeps the states it adds to those that differ. Three things
are left out and stated as such: a sink still opening at the declaration is
taken down at once, although its open may finish on the lost process; a
coordinator's death or supersession takes the old incarnations down with every
other sink, as it already took every frame on the wire; and the lost source's
barriers are not followed past the redeploy, where they reach only its own
process, whose checkpoint cannot complete.

The `CONFIRMED` marker takes three steps where `COMPLETED` takes one, because
the coordinator puts it outside its lock: the hold that drains the
confirmation set (inside `SinkConfirm`) leaves the marker due, `WriteConfirmed`
makes it durable, and `AdvanceConfirmed` moves `memConfirmed` to it. A
`Redeploy`, a `RestartProceeds` that holds the job for resolution, or a
takeover can fall between them. A redeploy or a supersession turns a
confirmation whose put is still out stale: the put lands and nothing advances
after it, and a takeover seeds `memConfirmed` from whatever is on disk.
The `COMPLETED` put is outside the lock too, and a `Redeploy` or a
supersession turns one still out stale (`staleCompleted`):
`WriteStaleCompleted` lands it with no advance and no broadcast, and a later
walk reads it like any other marker. A recoverable job's takeover passes it
over once the redeploy's `RESTORED` record (`restoreRec`, put in
`RedeployEffects`) excludes it.

### Fault points are states between steps

The atomic steps are chosen so that every named fault point in
`include/clink/fault/fault_injection.hpp` is a distinct state between two
actions, and a process may die between any two. That is what makes the
enumeration cover the windows the campaigns aim at, and every window between
them.

| Fault point | State in the model |
|---|---|
| `sink.before_prepare` | barrier in `barriers[s]`, before `SinkPrepare(s)` |
| `worker.after_trigger_delivered` | after `DeliverBarrier`, before `SinkPrepare(s)`: the model has taken the source's capture, which in the engine has not landed yet |
| `sink.after_prepare` | after `SinkPrepare(s)`, before `SinkAck(s)` |
| `coordinator.before_completed_marker` | `completeDue = c`, before `WriteCompleted` |
| `coordinator.after_completed_marker`, `coordinator.before_commit_broadcast` | `toBroadcast = c`, before `Broadcast` |
| `sink.before_commit` | `stage = "committing"`, before `SinkCommit(s)` |
| `sink.between_commit_and_receipt` | `stage = "committed"`, before `SinkReceipt(s)` |
| `sink.after_external_commit` | `stage = "receipted"`, before `SinkFinish(s)` |
| (none: the commit callback has returned, `CommitConfirmed` not yet sent) | `stage = "finished"`, before `SinkConfirm(s)`; the sink may already have prepared the next checkpoint |
| `coordinator.before_confirmed_marker` | `confirming[c] = "due"` (the confirmation set drained and `c` out of `broadcastIds`), before `WriteConfirmed`; neither `confirmedDisk` nor `memConfirmed` has moved |
| `coordinator.before_in_doubt_walk` | `phase = "resolving"` with `walkC` fixed by `RestartProceeds`, before the walk's first step; an `AdvanceConfirmed` landing here moves `memConfirmed`, not `walkC` |
| `sink.orphan_described` | `sink[s].opening`, before `SinkOpens(s)`: the reopening Kafka sink's describe has seen its orphan `Ongoing` and the abort is not yet sent. A death there is an open that never happened: the marker stays, and the next open settles again |
| `checkpoint.before_write` and its siblings | `SinkPrepareFails(s)` (the capture fails, the ack says so) |

Every action's comment names the engine site it abstracts, so a reader can
go from a step in a counterexample to the code.

## Configurations

| Model | Family | Bounds | What it adds |
|---|---|---|---|
| `MC_KafkaSmall` | Kafka | 2 sinks on 2 workers, 3 checkpoints, 1 in flight, one of each fault; no lost worker left running | The push gate for the Kafka family |
| `MC_KafkaTwoInFlight` | Kafka | as above with 2 checkpoints in flight, no coordinator death or broker fault; a lost worker may run on | The barrier for the next interval overtaking an outstanding commit; a failed checkpoint below a completing one |
| `MC_RecoverableSmall` | recoverable | 2 sinks, 3 checkpoints, 2 in flight; no lost worker left running | Re-commit at open, restore from the newest completed checkpoint |
| `MC_RecoverableNoBudget` | recoverable | as `MC_RecoverableSmall` with no restart budget and no error restarts; a lost worker may run on | A failed checkpoint that cannot rewind fails the job rather than completing above itself; an old incarnation's commit racing the re-commit at open |
| `MC_KafkaLiveness` | Kafka | 2 checkpoints, one of each fault; no lost worker left running | Checks `EventuallySettled` as well as the invariants |
| `MC_KafkaLostWorker` | Kafka | as `MC_KafkaLiveness`, safety only, with the lost worker running on | An old incarnation executing the frames on its wire alongside the drain, the walk, the redeploy and the reopening sinks |
| `MC_RecoverableSparse` | recoverable | 2 sinks, 2 checkpoints, 1 in flight, worker and coordinator death, snapshot failure, up to 2 empty (sink, position) pairs | A sink that seals nothing for an interval and declines its commit, across a restore and the re-commit at open |

Bounds are small on purpose: a push gate has minutes, and within its bounds
TLC is exhaustive. Larger bounds are for a deliberate longer run, not for
the gate.

Liveness is checked for the Kafka family only. For the recoverable family
`EventuallySettled` fails at these bounds for a reason the bounds create,
with or without empty intervals: `Settled` vouches on the newest `COMPLETED`
marker, and a marker whose put was still out when the job restored from
below it lands afterwards and vouches for a cut the run then has no
checkpoint id left to re-cover (a bounded `MaxCkpt`, or a takeover's
renumbering past a superseded coordinator's claims). The Kafka family
vouches on `CONFIRMED` markers, which only follow executed commits, and does
not meet it. The property, not the protocol, is what stops there; `NoLoss`
holds through those runs, with the positions held in prepared transactions
of a completed checkpoint.

`MaxLostRunning` is 1 in `MC_KafkaTwoInFlight`, `MC_RecoverableNoBudget` and
`MC_KafkaLostWorker`, and 0 in the other three. A lost worker left running about
doubles a search (2.4 times for `MC_KafkaTwoInFlight`, 2.1 for
`MC_RecoverableNoBudget`), and `MC_RecoverableSmall` and `MC_KafkaSmall` each
fill a CI job of their own already, so they keep the old search, as does
`MC_KafkaLiveness`, whose strongly connected components pass grows with it.
The cost of that choice: in the Kafka family the lost worker is checked with
every fault at two checkpoints (`MC_KafkaLostWorker`) and at three checkpoints
with fewer faults (`MC_KafkaTwoInFlight`), and in the recoverable family with
the restart budget spent (`MC_RecoverableNoBudget`); nothing checks it at three
checkpoints with every fault, and liveness is not checked with it. With
`MaxLostRunning` at 0 the extension adds no state (`MC_KafkaTwoInFlight` gives
the same 101,698 states as before it).

## Mutants: the model must see what the rigs saw

A model that proves its own invariants shows nothing until it is shown to
reject a wrong protocol. Every defect the qualification campaigns found and
fixed, and every defect this model found itself, is a value of the `Bug`
constant that switches one rule back to its pre-fix form. The hooks are
inline in `ExactlyOnce.tla`, at the rule they disable, so a reader sees at
the rule what it is for. `scripts/formal-check.sh --mutants` runs TLC on
each and judges the outcome against `mutants/expected.txt`.

| Mutant | Rule it disables | Found by | Result |
|---|---|---|---|
| `broadcast_during_drain` | The commit broadcast is withheld while the job drains for a restart | qual01-20260818a | refuted: NoDuplicate (accepted as guarded until the specification admitted a checkpoint triggered before a sink reopens; the counterexample is the campaign's shape, and it needs a drain that completes while a redeployed sink is still opening, so the drain set takes that sink as possible rather than certain) |
| `close_aborts_prepared` | A cancelled sink preserves its barrier-sealed prepared transaction | qual01-20260818a | accepted: the walk refuses an aborted transaction and the replay re-emits its interval, receipts suppressing the committed siblings; preserving it saves a replay, not correctness |
| `no_receipts` | The sink writes a durable commit receipt the instant the broker acknowledges | qual01-20260818b | refuted: NoDuplicate |
| `stop_at_first_refusal` | The walk probes every handle of a checkpoint even after a refusal | qual01-20260819f | accepted: the marker rule now marks the unprobed handles too, and the sink describes them before fencing |
| `no_materialised_receipts` | A commit the walk proves over the wire gets its receipt materialised | qual01-20260819f | refuted: NoDuplicate |
| `blind_fence` | A reopening sink describes its unresolved orphan before it fences | qual01 rig night | refuted: NoDuplicate |
| `receipt_after_begin` | The receipt is written before the successor transaction begins | qual01 rig night | refuted: NoDuplicate |
| `restore_from_completed` | Jobs with a non-recoverable-commit sink restore from the newest confirmed checkpoint | pre commit-confirmed protocol | refuted: FrontierCovered |
| `no_rewind_on_failed_checkpoint` | A FAILED checkpoint rewinds the job so its aborted interval is re-emitted | correctness sweep item 4 | refuted: NoLoss |
| `broadcast_before_marker` | The COMPLETED marker is durable before any commit is broadcast | hardening round | refuted: NoDuplicate |
| `id_reuse` | A recovered job numbers new checkpoints above every durable id | qual01-20260817c, 20260819g | refuted: ConfirmedMeansCommitted |
| `no_fencing` | Workers drop control frames from an epoch below their bound | design | refuted: deadlock (a stale barrier seals a transaction nothing will commit; the engine reaches the same state through the sink's bounded wait and a restart) |
| `refusal_wall` | An early stop of the walk marks every unreceipted handle above it | this model | refuted: NoDuplicate |
| `complete_above_failed` | A checkpoint above a FAILED one is discarded during the rewind | this model | refuted: NoLoss |
| `restore_from_memory` | The in-memory restore point advances with the durable marker, not before | this model | refuted: FrontierCovered |
| `sail_on_without_budget` | A FAILED checkpoint with no restart budget left fails the job instead of carrying on | framework review | refuted: NoLoss |
| `advance_before_confirmed_put` | The in-memory confirmed restore point advances only once its CONFIRMED marker is durable | review of the per-connection dispatch change | refuted: ConfirmedMemoryOnDisk |
| `orphan_left_to_init` | A reopening Kafka sink settles an undecided orphan (abort, or the receipt of the commit that beat the abort) before `init_transactions` | this model, once a lost worker could run on | refuted: NoDuplicate |
| `open_skips_restore_zero` | A Kafka sink reopening at restore point 0 settles its orphan and arms suppression, as at any other | this model, against the engine's open | refuted: NoDuplicate |
| `restore_from_superseded` | A takeover passes over a COMPLETED marker that the job's newest deploy restored below (the RESTORED record) | this model, once the recoverable open rolled back what its restore did not hold | refuted: NoLoss (FrontierCovered with a second takeover and NoLoss left out) |

A mutant TLC accepts is recorded in `mutants/expected.txt`, not deleted: it
means a later rule guards the same defect (defence in depth), and the check
then holds that record in both directions: the day TLC refutes an
`accepted` mutant, the other guard has gone and the record is wrong. Two of
the twenty are accepted today, both superseded by receipts, in-doubt
resolution and the marker rule the refusal-wall finding added; the withheld
broadcast left that set when trace validation widened the specification,
and the check would have failed had it stayed recorded as accepted. The
`no_fencing` mutant is judged by the correctness invariants alone (its
configuration drops the `Fenced` ghost, which would merely restate the
mutant); TLC refutes it by deadlock, which is how the model renders the
engine's bounded wait and restart.

## What the model found

Three interleavings the campaigns had not enumerated, each fixed in the
engine in the same change that introduced the model and each kept as a
mutant above:

1. **The refusal wall.** In-doubt resolution stopped at the first checkpoint
   it refused. A commit that had executed without its receipt (an ack-window
   kill) in a completed checkpoint above the refused one was never proven,
   the redeploy fenced it blind and the replay published its interval twice;
   and the refused checkpoint stood as a wall every later walk stopped at
   until a higher CONFIRMED marker landed. Fix: every early stop of the walk
   leaves an `.unresolved` marker for each unreceipted handle above it, and
   the sink's pre-fence describe settles them (`src/cluster/in_doubt_resolution.cpp`,
   `ResolutionFixture.ARefusalMarksTheUnreceiptedHandlesAboveIt`).
2. **The rewind floor.** The trigger loop does not wait for one checkpoint's
   acks before issuing the next, so a checkpoint above a FAILED one is
   routinely still collecting acks when the failure begins its rewind; one
   that finished collecting them during the drain completed, was confirmed
   by the walk, and became the restore point past the aborted interval. Fix:
   a checkpoint above the failed id is discarded like the failed one until
   the restart redeploys (`src/cluster/coordinator.cpp`,
   `CheckpointCompletion.ACheckpointAboveAFailedOneIsDiscardedDuringTheRewind`).
3. **The restore point ahead of its marker.** `latest_completed_checkpoint_id`
   advanced under the lock at completion with the marker written after it;
   a restart deciding in that window redeployed from a checkpoint the next
   coordinator could not see, and the recoverable sinks had already
   re-committed its handles at open. Fix: memory advances with the durable
   write.

A fourth came later, once a lost worker could run on, and was fixed in the
engine after the specification stated the fix:

4. **The orphan left to the init.** Once a worker declared lost could run on
   (`WorkerLost`), TLC found a duplicate, a counterexample 28 states long,
   on the `MC_KafkaLiveness` bounds. A checkpoint completes and its commit frame is
   on the wire to w1 when w1 is declared lost, still running. The walk is
   cancelled before it probes anything and leaves `.unresolved` markers; the
   job restores from the confirmed point below. s1's new incarnation opens:
   its describe sees the orphan `Ongoing` and writes no receipt; the old
   incarnation's commit lands; `init_transactions` finds the transaction
   committed and aborts nothing; suppression arms with no receipt; and the
   replay publishes the interval again. The engine's open is that shape
   (`resolve_unresolved_orphan_` in `impls/kafka/src/register_factories.cpp`
   leaves an undecided orphan "for the init below" to abort). The
   specification now states the fixed open: the reopening sink ends an
   undecided orphan with `EndTxn(abort)` under the orphan's own identity
   before the init, and if that abort is refused because the old incarnation's
   commit got there first, it describes again and writes the receipt. Either
   way the outcome is decided, and known, before the fence and before
   suppression arms. The broker offers no way to learn a transaction's outcome
   once a later `init_transactions` has superseded it, so fencing first and
   asking afterwards is not an option. Abort rather than commit because the
   restore point is already below the orphan, so the replay re-produces its
   interval whatever the open does, and an abort makes the replay the only
   copy without leaning on replay suppression. The mutant keeps the old open.
   The engine's open now settles the orphan this way
   (`resolve_unresolved_orphan_`, over `settle_orphan` in
   `impls/kafka/src/txn_resume.cpp`), with the fault point
   `sink.orphan_described` between its describe and its abort. Pinned by
   `TxnResumeLive.AnOrphanCommittedDuringTheOpenIsReceiptedBeforeTheFence` and
   `TxnResumeLive.AnOrphanAbortedByTheOpenRefusesTheOldCommitAndIsReplayedOnce`
   (two sinks on one transactional id, the old commit and the new open released
   in each order; the first publishes the interval twice against the old open),
   by the `TxnResumeSettle` cases for the wire exchange, and by its `SinkOpens`
   line in trace validation (below).

A fifth came from comparing the engine's open with the specification's:

5. **The restart from nothing taken for a fresh start.** The specification's
   open settles markers and arms suppression above the restore point r for
   every r, 0 included, while the engine's open skipped both at restore point
   0. A restart before any checkpoint of the run is confirmed restores from 0,
   so a commit that executed without its receipt (an ack-window kill), which
   the walk then could not settle (cancelled, a broker outage), was neither
   receipted nor suppressed, and the replay from the start published its
   interval a second time; no worker left running is needed. TLC refutes the
   engine's shape (`open_skips_restore_zero`) in 24 states: checkpoint 1
   completes and is broadcast, s1 commits it, s1's worker dies before the
   receipt, the walk is cancelled, the job redeploys from 0, checkpoint 2 cuts
   the same position, and s1 reopens without settling or arming and commits it
   again. The engine skipped restore point 0 for a real reason: job ids restart
   at 1 in every coordinator process and the receipts directory is per job id,
   so a fresh start in a reused directory can find an earlier run's receipts,
   and arming from them would swallow output the new run owes. The model has
   one run per behaviour, a fresh start being the initial state, whose first
   open is no step, so every receipt and marker in it is the current run's.
   The engine now says which are: every deploy carries the run's
   checkpoint-id floor, kept by the run's restarts and an HA takeover, and the
   open reads receipts and markers above the larger of the restore point and
   the floor. A fresh start's floor sits above every id an earlier run of the
   job left in the directory; a run that restores from checkpoint R continues
   the run that took it, and its floor is R. Pinned by
   `TxnResumeLive.ACommitWithoutItsReceiptIsReceiptedAtARestartFromNothing`
   and `TxnResumeLive.AnOrphanRacedAtARestartFromNothingIsSettledBeforeTheFence`
   (both publish the interval twice against the open that skipped restore point
   0), `TxnResumeLive.AFreshStartAboveAnEarlierRunsReceiptsArmsNothing`,
   `TxnResumeLive.AResubmitFromACheckpointArmsFromTheReceiptsAboveIt`, and
   `CheckpointCompletion.EveryDeployOfARunCarriesTheFloorItsFirstSubmitFixed`.

A sixth came from writing the recoverable open as the Postgres sink runs it:

6. **The marker that landed after the job restored below it.** The
   specification's recoverable open re-committed the handles the restore held
   and left every other prepared transaction as it was; the Postgres sink
   rolls those back (`reconcile_orphans_`). With the rollback in, TLC refuted
   the protocol in 12 states: a coordinator superseded with checkpoint 1
   decided and its marker's put out, a takeover that reads no marker and
   redeploys from nothing, sinks that reopen and roll checkpoint 1 back, and
   the superseded put landing after them; a further takeover restores from
   that marker and resumes the source past an interval no store holds. A
   restart redeploying while its own put is out reaches the same state.
   Keeping the transactions at the sink instead wedges a keyed table: the
   replay's insert of the same row waits on the kept transaction's lock,
   which only a later commit would release. So the coordinator records every
   deploy's restore point and first id (`RESTORED`, put in `RedeployEffects`
   before the sinks open, fenced by epoch in the engine), and a recoverable
   job's takeover passes over a marker between the two
   (`RestorableCompleted`, which `VouchedFor` also uses: a marker no restore
   would choose vouches for nothing). `restore_from_superseded` keeps the old
   choice. Pinned by
   `CheckpointCompletion.ATakeoverPassesOverAMarkerThatLandedAfterTheJobRestoredBelowIt`
   and `traces/superseded-marker-passed-over`.

## Trace validation

The engine records its protocol steps when `CLINK_PROTOCOL_TRACE_DIR` names
a directory: one NDJSON file per process, one event per line
(`include/clink/cluster/protocol_trace.hpp`). `TraceExactlyOnce.tla`
extends the specification with a variable `l`, the index of the next event,
and a next-state relation that takes the specification step each event
names with the parameters the engine observed, lets the unobservable
actions (`CoordDies`, `TxnExpires`, the broker going away) happen as hidden
steps within the budgets the trace implies, and treats events about
subtasks that are not two-phase sinks as stutters. The constants come off
the trace (`TraceSinks`, `TraceHost`, `TraceMaxCkpt`, ...), substituted in
`trace/TraceExactlyOnce.cfg`. Some path consuming the whole trace means the
run is a behaviour of the specification (the postcondition `TraceAccepted`
reads the furthest event index any path reached; deadlock checking is off,
since a hidden-step branch the run did not need dies out harmlessly); a
shorter reach names the first event no allowed step produced, and
`formal-check.sh --trace` prints it.

One step of the specification has no line at all: the Kafka sink's
`SinkFinish`, between its `SinkReceipt` and the worker's `SinkConfirm`, where
`on_commit` erases the staged handle and resolves the open transaction. The
worker sends `CommitConfirmed` only after the callback returns, so the task
thread can seal the next checkpoint between the two, and a recorded run of
`KafkaWindowRecoveryTest.WorkerAndHaCoordinatorFailoverKeepSourceWindowAndSinkOnOneCut`
did. The trace module takes the finish as a hidden step only where the next
event needs it, that sink's `SinkConfirm` or its next `SinkPrepare`
(`traces/kafka-prepare-before-confirmation`, synthetic, is accepted and
diverges at the prepare when the finish and the confirmation are one step).

A marker can be on disk without the line that reports it: a kill between the
`COMPLETED` write and its `WriteCompleted` line, or between the `CONFIRMED`
put and the hold that emits `WriteConfirmed`, and a `CONFIRMED` put whose job
redeployed before that hold, or a `COMPLETED` put still out when its job
redeployed. The hidden steps `LostWriteCompleted` and `LostWriteConfirmed`
take such a marker, each admitted only when the next event is a takeover that
read exactly that checkpoint off the disk; a stale `COMPLETED` put is also
admitted just before a walk event at its checkpoint
(`LostStaleCompletedAtWalk`), since the walk reads the marker before any event
but `WalkSkips`. `CoordRecovers` pins the reseeded completed restore point to
the event's `completed`: `traces/completed-marker-walked-after-redeploy`,
synthetic, is accepted and diverges without the stale put, and in
`traces/completed-marker-landing-after-redeploy` the takeover passes the
landed marker over, as the redeploy's `RESTORED` record says. A
`WriteConfirmed` line stands for `WriteConfirmed` and `AdvanceConfirmed` back
to back, since the engine emits it in the advance's hold, and `CoordRecovers`
pins the Kafka family's reseeded confirmed restore point to the event's
`confirmed`, the confirmed restore point the takeover adopts from the job's own
directory (the `CONFIRMED` marker its restore read, or 0 when that marker
belongs to a run before the current run's base). The in-doubt walk puts its
`CONFIRMED` marker before its `WalkDecides` line, and a kill between the two
leaves the marker without the line; the hidden step `LostWalkDecides` takes
the walk's success there, on the same terms, when every handle of the walked
checkpoint was proven committed (`traces/walk-confirmed-then-killed`, a
synthetic cut of `kafka-kill-after-broker-commit`, is accepted through it and
diverges without it). A superseded coordinator emits `WriteConfirmed` too,
unaware of its supersession, and the line carries its epoch: from a
non-leader epoch it is the stale put landing, or a stutter when the takeover
already read the marker (`traces/zombie-confirmation-after-takeover`,
synthetic, is accepted and diverges when the line is read as the leader's).
Its line can also come before the takeover's, since the takeover reads the
disk before it emits `CoordRecovers`; the line then reads as the leader's
epoch, and is taken as the supersession already made and a stutter, the
confirmation staying stale (`traces/zombie-confirmation-before-takeover-line`,
synthetic, is accepted and diverges without that branch). The same holds
for a superseded coordinator's `COMPLETED` put: its handler goes on to advance
its own memory, emit `WriteCompleted` and broadcast, and `CoordComplete`,
`WriteCompleted` and `Broadcast` carry the coordinator's epoch. From a
non-leader epoch, `WriteCompleted` is the stale put landing
(`StaleCompletedLands`), or a stutter when the takeover already read the
marker; before the takeover's line it is the supersession already made and a
stutter, the put staying stale for whatever reads it later
(`traces/zombie-completion-after-takeover` and
`traces/zombie-completion-before-takeover-line`, synthetic, are accepted, and
diverge at the late `WriteCompleted` and at the takeover respectively without
those branches). A superseded coordinator's `Broadcast` is a stutter. Its
commit frames are covered in the model by the supersession itself, which
takes every sink down and clears every frame, and by `CoordRecovers`, which
binds every worker to the new epoch, so none is accepted there. The engine
fences them at every worker that has re-registered with the leader; a worker
that has not yet done so accepts them, the gap described below for the
superseded coordinator's barriers, and the model does not cover it. A
superseded coordinator can also decide a checkpoint after the takeover's
line. Every ack it decides on was sent before the supersession (`SubtaskAck`
is emitted as the frame is sent, so the model has already taken the
`SinkAck`), but its connection's dispatch thread held the last of them behind
a slow store write on the same connection. The supersession turns every
checkpoint decidable with all acks ok at that moment stale (`DecidedLate`),
and their puts land in id order after the put still out (`lateBatch`), since
each decision waits behind the landing below it. A `completed`
`CoordComplete` from a non-leader epoch for such a checkpoint is a stutter,
and its `WriteCompleted` the stale put landing
(`traces/zombie-decision-after-takeover` and
`traces/zombie-decision-behind-stalled-marker`, synthetic, are accepted and
diverge at that `CoordComplete` without the branch). Letting those puts land
in any order made TLC report a `NoLoss` violation on `MC_RecoverableNoBudget`
that the engine cannot produce: a later marker vouched for a cut whose earlier
interval no marker covered. A late decision that fails or discards its
checkpoint still diverges, since the model keeps no record of the superseded
coordinator's acks past the takeover to check it against, and so does a
decision on an ack sent after the supersession by a worker not yet
re-registered, the gap above. A takeover reads the job's markers once, so its
`CoordRecovers` and its `Redeploy` agree, and a takeover parked for capacity
redeploys the restore point it decided (`traces/takeover-reads-markers-once`
and `traces/parked-takeover-keeps-its-restore-point`, synthetic). A restart
of a taken-over job before its first new checkpoint completes walks the gap
the takeover's walk left open, as `RestartProceeds` requires
(`traces/takeover-gap-walked-again`, synthetic). A
`WorkerDies` kills only the sinks its `spared` list leaves out: a worker that
re-registers keeps whatever was placed on its new session when the old one is
retired, and a new session lost while still retiring the old one leaves the
old session's subtasks to that retirement.

A `SinkPrepare` line whose `staged` is false is a recoverable sink that
received nothing of the interval, and the module takes the empty prepare
(`SinkPrepareEmpty`, or the failing capture's and the old incarnation's
forms); the `DeliverCommit` and `DeliverAbort` it later declines are the
steps that find no handle. The Kafka sink's line always says staged.
`traces/recoverable-sparse-sinks` is a run of card-sentry's payment rail
(five Postgres two-phase sinks behind five fraud detectors over a Kafka
feed, a worker killed mid-flight, a batch re-sent by its producer): 4,281
events, 455 prepares of which 9 staged, and it diverged at the first
declined commit before the empty prepare existed.

A `WorkerDies` line is the coordinator's declaration, and the module takes it
either way: the process gone, or still running (`WorkerLost` with `running`
true, within `TraceMaxLostRunning`, one per `WorkerDies` line). A process
still running goes on writing lines for the frames already on the wire to it.
Its source's `DeliverBarrier` is `LostDeliverBarrier`; its old incarnations'
`SinkPrepare`, `SinkCommit`, `SinkReceipt` and `DeliverAbort` are
`LostSinkPrepare`, `LostSinkCommit`, `LostSinkReceipt` and `LostDeliverAbort`;
a `DeliverCommit` it refused is `LostSinkCommit` with no effect, and one it
accepted is a stutter until its `SinkCommit`, since the specification takes the
dispatch and the commit as one step. Its `SubtaskAck` and `SinkConfirm` lines
are stutters: the coordinator drops a lost session's frames. They are accepted
for a checkpoint an earlier incarnation of the sink sealed and the live one does
not hold, which is looser than the old incarnation's own record, since the
specification stops following an old incarnation that can do nothing more. Its
process stopping has no line: `LostSinkStops` and `LostSourceStops` are hidden
steps. `traces/lost-worker-commits-after-declaration` is the run that showed
it, recorded with a temporary test that held a worker's reader on a `Deploy`
until the coordinator's outbound stall verdict declared the worker lost, then
let it read the `CommitCheckpoint` already queued behind that `Deploy`. Before
this module it diverged at event 44, the lost worker's accepted
`DeliverCommit`; it is accepted now, the recovery re-commit at the next open a
no-op.

A Kafka sink's `SinkOpens` line carries `orphan` and `settled` when its open
found an `.unresolved` marker above the restore point and settled it:
`receipt` (a receipt already answered for it), `committed` (the open proved a
commit and wrote the receipt), `aborted` (the open's `EndTxn(abort)` ended it)
or `not_committed` (found decided without a commit). The module then takes the
open (`SinkOpensWith`, with or without the old incarnation's commit landing
during it) only where the orphan is the newest marker above the restore point
and the outcome is one the specification's open can reach from that state; a
line without the fields, from an engine before the settle or an open with no
marker, is taken as before. `traces/kafka-orphan-settled-at-open`, synthetic,
has two losses of a worker whose process runs on and each value once; it is
accepted, and a field changed to an outcome the state rules out diverges (a
`committed` read as `aborted` diverges at the old incarnation's `SinkCommit`,
and an `aborted` read as `committed` at the walk's probe that finds the
transaction aborted). An outcome the specification allows from the same state
is accepted, as it must be: an `aborted` read as `not_committed` is a broker
expiry the engine cannot observe, taken as a hidden step. The merge script
grants one expiry per `not_committed` line for that reason.
`traces/kafka-orphan-settled-at-restart-from-nothing`, synthetic, is the
settle at restore point 0: a commit without its receipt, the walk cancelled,
the redeploy restoring from nothing, and the open receipting the commit. It is
accepted, and diverges at the open with any other `settled` value or another
`orphan`.

To record a run: set the variable, run the job (the in-process protocol
trace test and every multi-process harness test do this themselves when
`CLINK_PROTOCOL_TRACE_OUT` is set), then validate the directory. To add a
recorded run to the push gate, copy its files to `traces/<name>/` and
validate that directory.

Trace validation has caught one engine defect so far. A takeover numbered
its checkpoints above the markers and the snapshot files it could see, and a
CI run of the parked-recovery HA test reused the id of a checkpoint whose
barrier the dead leader had delivered and whose capture had not landed: the
trace diverged at its `Redeploy`. A worker outliving the leader can finish
such a capture after the takeover has looked, at the path the new run's
capture of the reused id writes. The coordinator now claims every id in
`_jobs/<job>/TRIGGERED` before it allocates it, and a takeover numbers above
the record. `RedeployEffects` states the rule as a range (from one above
every capture that began to one past the most a dead leader can have
claimed) and says why a range rather than a record variable, and why no
mutant comes with it: the lower bound was already the rule, and the engine
fell short of it. A claim lands only above what the record holds, so a
superseded coordinator still triggering and the leader never share an id; a
coordinator whose claim is refused renumbers above the record, which the
specification admits as `Renumber` and `ZombieRenumber`. The model binds
every worker to the new epoch at the takeover, so it fences the superseded
coordinator's barriers there; in the engine a worker binds it only when it
re-registers, and the claims are what keep a late capture of one of those
barriers off the leader's ids. That property is checked in the trace module
rather than the specification, where it costs the model searches nothing:
`triggered` is a ghost of the ids each coordinator that can still reach a
worker has triggered, under its epoch, and a `Trigger` whose id another of
them has triggered diverges, whichever triggers it second. A leader that
dies takes its ids out of the ghost, since its barriers die with it, which is
what lets `traces/kafka-coordinator-failover`, recorded before the record
existed, number from an id its dead leader triggered and never delivered.
`Renumber` and `ZombieRenumber` stay in `Next` so that every renumber a trace
records is a step the model searches have covered, not one the trace module
alone admits.

## Conventions

- Every action names the engine site it abstracts, in its comment. A rule
  that exists because of a campaign or a model finding names the run.
- A protocol change is a specification change. Run the models before
  committing; run the mutants when a rule changes.
- A new defect gets a `Bug` value, a hook at the rule it disables, a
  configuration under `mutants/`, and a row in the table above and on the
  published page.
- Mutant modules are generated by hand from a model configuration with the
  `Bug` constant changed; keep their bounds as small as still refutes them.
- The CommunityModules jar is pinned for the trace validator. It is compiled
  against a newer TLC than the release jar and shadows classes in it, so the
  script puts it on the classpath only for the trace module, which imports
  `Json` and `IOUtils`.
- A new emission site in the engine names an event in `trace/events.txt`
  and a step in `trace/TraceExactlyOnce.tla`; a new action in `Next` is
  either reached by an event or listed as unobserved.
  `scripts/check-protocol-trace-events.py` (CI and the pre-commit hook)
  fails when the three disagree.
