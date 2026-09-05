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
```

The script needs a Java 11+ runtime and nothing else. It fetches the TLA+
tools pinned in `tools.env` (SHA-256 verified, cached under
`CLINK_FORMAL_TOOLS_DIR`, default `~/.clink-deps/formal-tools`) and runs TLC
with deadlock checking on. Knobs: `TLC_WORKERS` (default `auto`), `TLC_HEAP`
(default `2g`), `TLC_EXTRA` for further TLC flags. The `formal` job in
`.github/workflows/ci.yml` runs both forms on a bare runner.

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
| `tools.env` | The pinned TLA+ tools and their checksums |
| `../scripts/formal-check.sh` | Fetch, verify, run, judge |

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
weakly fair.

### Fault points are states between steps

The atomic steps are chosen so that every named fault point in
`include/clink/fault/fault_injection.hpp` is a distinct state between two
actions, and a process may die between any two. That is what makes the
enumeration cover the windows the campaigns aim at, and every window between
them.

| Fault point | State in the model |
|---|---|
| `sink.before_prepare` | barrier in `barriers[s]`, before `SinkPrepare(s)` |
| `sink.after_prepare` | after `SinkPrepare(s)`, before `SinkAck(s)` |
| `coordinator.before_completed_marker` | `completeDue = c`, before `WriteCompleted` |
| `coordinator.after_completed_marker`, `coordinator.before_commit_broadcast` | `toBroadcast = c`, before `Broadcast` |
| `sink.before_commit` | `stage = "committing"`, before `SinkCommit(s)` |
| `sink.between_commit_and_receipt` | `stage = "committed"`, before `SinkReceipt(s)` |
| `sink.after_external_commit` | `stage = "receipted"`, before `SinkConfirm(s)` |
| `checkpoint.before_write` and its siblings | `SinkPrepareFails(s)` (the capture fails, the ack says so) |

Every action's comment names the engine site it abstracts, so a reader can
go from a step in a counterexample to the code.

## Configurations

| Model | Family | Bounds | What it adds |
|---|---|---|---|
| `MC_KafkaSmall` | Kafka | 2 sinks on 2 workers, 3 checkpoints, 1 in flight, one of each fault | The push gate for the Kafka family |
| `MC_KafkaTwoInFlight` | Kafka | as above with 2 checkpoints in flight, no coordinator death or broker fault | The barrier for the next interval overtaking an outstanding commit; a failed checkpoint below a completing one |
| `MC_RecoverableSmall` | recoverable | 2 sinks, 3 checkpoints, 2 in flight | Re-commit at open, restore from the newest completed checkpoint |
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
| `broadcast_during_drain` | The commit broadcast is withheld while the job drains for a restart | qual01-20260818a | accepted: receipts and in-doubt resolution repair a partial commit; the withheld broadcast is defence in depth |
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

A mutant TLC accepts is recorded in `mutants/expected.txt`, not deleted: it
means a later rule guards the same defect (defence in depth), and the check
then holds that record in both directions: the day TLC refutes an
`accepted` mutant, the other guard has gone and the record is wrong. Three of
the fifteen are accepted today, all three superseded by receipts, in-doubt
resolution and the marker rule the refusal-wall finding added. The
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
- The CommunityModules jar is pinned for the trace validator (design record
  012, increment 4). It is compiled against a newer TLC than the release jar
  and shadows classes in it, so the script puts it on the classpath only for
  modules that import a community module.
