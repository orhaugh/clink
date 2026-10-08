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
`MC_RecoverableSmall` (about 86 minutes) in the `formal-recoverable-small` job,
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

Two connector families share the module through the `Recoverable`
constant: the Kafka family (a broker transaction dies with its producer
unless resolved with the saved identity, so the job runs the
commit-confirmed restore protocol, receipts, in-doubt resolution and replay
suppression) and the staged-artifact / XA family (file, Parquet, S3,
Postgres: a persisted handle is re-committed idempotently at open, restores
select the newest completed checkpoint).

Faults are actions with a budget, so the checker may inject them or not:
worker death, coordinator death, a superseded coordinator that keeps
triggering, broker transaction expiry, an unreachable broker, a snapshot
capture that fails, and a cancelled resolution walk. Everything else is
weakly fair. The job's restart budget is a bound of its own
(`MaxJobRestarts`): a failed checkpoint rewinds the job while budget is
left, and fails the job when none is.

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
takeover or walk reads it like any other marker.

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
| `checkpoint.before_write` and its siblings | `SinkPrepareFails(s)` (the capture fails, the ack says so) |

Every action's comment names the engine site it abstracts, so a reader can
go from a step in a counterexample to the code.

## Configurations

| Model | Family | Bounds | What it adds |
|---|---|---|---|
| `MC_KafkaSmall` | Kafka | 2 sinks on 2 workers, 3 checkpoints, 1 in flight, one of each fault | The push gate for the Kafka family |
| `MC_KafkaTwoInFlight` | Kafka | as above with 2 checkpoints in flight, no coordinator death or broker fault | The barrier for the next interval overtaking an outstanding commit; a failed checkpoint below a completing one |
| `MC_RecoverableSmall` | recoverable | 2 sinks, 3 checkpoints, 2 in flight | Re-commit at open, restore from the newest completed checkpoint |
| `MC_RecoverableNoBudget` | recoverable | as `MC_RecoverableSmall` with no restart budget and no error restarts | A failed checkpoint that cannot rewind fails the job rather than completing above itself |
| `MC_KafkaLiveness` | Kafka | 2 checkpoints, one of each fault | Checks `EventuallySettled` as well as the invariants |

Bounds are small on purpose: a push gate has minutes, and within its bounds
TLC is exhaustive. Larger bounds are for a deliberate longer run, not for
the gate.

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

A mutant TLC accepts is recorded in `mutants/expected.txt`, not deleted: it
means a later rule guards the same defect (defence in depth), and the check
then holds that record in both directions: the day TLC refutes an
`accepted` mutant, the other guard has gone and the record is wrong. Two of
the seventeen are accepted today, both superseded by receipts, in-doubt
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
the event's `completed` (`traces/completed-marker-landing-after-redeploy` and
`traces/completed-marker-walked-after-redeploy`, both synthetic, are accepted
and diverge without the stale put). A
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
