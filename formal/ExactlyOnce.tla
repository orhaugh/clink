------------------------------ MODULE ExactlyOnce ------------------------------
(*****************************************************************************)
(* clink's exactly-once protocol, as a checkable model.                       *)
(*                                                                            *)
(* This module states the protocol that four parts of the engine implement    *)
(* between them: barrier checkpoints with ack-after-durable snapshots, the    *)
(* two-phase-commit sinks (prepare at the barrier, commit on the             *)
(* coordinator's CommitCheckpoint), the coordinator's completion protocol     *)
(* (a durable COMPLETED marker before any commit broadcast, CONFIRMED         *)
(* markers for sinks whose commit cannot be re-executed), and recovery (the   *)
(* restore point, in-doubt resolution with receipts and unresolved markers,   *)
(* the pre-fence describe at sink open, and checkpoint ids numbered above     *)
(* every durable record). Design record 012 explains why the model exists     *)
(* and what it leaves out; formal/README.md explains how it is checked.       *)
(*                                                                            *)
(* Abstraction. There are no records, watermarks or channels. Each            *)
(* checkpoint interval is one logical POSITION in the input: checkpoint c     *)
(* cuts the input at position cutOf[c], a sink's transaction sealed for c     *)
(* covers exactly that position, a restore rewinds the source to the restore  *)
(* point's cut, and a sink's visible output is the multiset of positions its  *)
(* committed transactions carry, less what replay suppression swallowed at    *)
(* emission. Exactly-once is then two statements about that multiset, made    *)
(* precise in the INVARIANTS section.                                         *)
(*                                                                            *)
(* Granularity. The atomic steps are chosen so that every named fault point   *)
(* in include/clink/fault/fault_injection.hpp is a distinct state between two *)
(* steps (the table is in the README), and a process may die between any two *)
(* steps. Every action names the engine site it abstracts.                    *)
(*                                                                            *)
(* Two connector families share the module. Recoverable = TRUE is the         *)
(* staged-artifact and XA family (file, Parquet, S3, Postgres): a persisted   *)
(* handle is re-committed idempotently at open, and restores select the       *)
(* newest COMPLETED checkpoint. Recoverable = FALSE is the Kafka family: a    *)
(* broker transaction dies with its producer unless resolved with the saved   *)
(* identity, so the job runs the commit-confirmed restore protocol, commit    *)
(* receipts, in-doubt resolution and replay suppression.                      *)
(*                                                                            *)
(* Mutants. Bug = "none" is the shipped protocol. Every other value switches  *)
(* one rule back to a pre-fix form that a qualification campaign found and    *)
(* fixed; the checker must refute each (formal/README.md lists them with the  *)
(* run that found the defect). The hooks are kept inline so a reader sees,    *)
(* at the rule, what it is for.                                               *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Sinks,             \* sink subtasks; each hosts one transactional sink
    Workers,           \* worker processes
    Host,              \* [Sinks -> Workers]
    SrcWorker,         \* the worker hosting the source subtask
    MaxCkpt,           \* highest checkpoint id the model allocates
    MaxInFlight,       \* checkpoints the coordinator keeps in flight at once
    Recoverable,       \* TRUE: staged-artifact/XA family; FALSE: Kafka family
    MaxWorkerDeaths,   \* fault budgets: each fault is an action bounded by one
    MaxCoordDeaths,
    MaxExpiries,       \* broker transaction expiries (transaction.timeout.ms)
    MaxSnapFails,      \* snapshot captures that fail (ack ok = FALSE)
    MaxBrokerOutages,  \* broker unreachable episodes
    MaxWalkCancels,    \* resolution walks cancelled by the watchdog deadline
    Bug                \* "none" or a mutant name (see Mutants below)

Mutants == {
    "none",
    "broadcast_during_drain",         \* qual01-20260818a: commit broadcast into a draining job
    "close_aborts_prepared",          \* qual01-20260818a: teardown aborted barrier-sealed transactions
    "no_receipts",                    \* qual01-20260818b: no commit receipts; a fenced commit read as not committed
    "stop_at_first_refusal",          \* qual01-20260819f: the walk left later handles unproven
    "no_materialised_receipts",       \* qual01-20260819f: a commit proven over the wire got no receipt
    "blind_fence",                    \* qual01 rig night: sink fenced an orphan before asking the broker
    "receipt_after_begin",            \* qual01 rig night: receipt written after the successor began
    "restore_from_completed",         \* pre commit-confirmed protocol: the restore point ignored executed commits
    "no_rewind_on_failed_checkpoint", \* correctness sweep item 4: a FAILED checkpoint's interval sailed on
    "broadcast_before_marker",        \* hardening: commit broadcast before the COMPLETED marker was durable
    "id_reuse",                       \* qual01-20260817c and 20260819g: a recovered job reused checkpoint ids
    "no_fencing",                     \* a superseded coordinator's frames accepted by workers
    "refusal_wall",                   \* found by this model: the walk left later checkpoints' commits unproven
    "complete_above_failed",          \* found by this model: a checkpoint completed above a FAILED one during the rewind
    "restore_from_memory"             \* found by this model: the restore point ran ahead of the durable marker
}

ASSUME Host \in [Sinks -> Workers]
ASSUME SrcWorker \in Workers
ASSUME Bug \in Mutants
ASSUME MaxCkpt \in Nat /\ MaxCkpt >= 1
ASSUME MaxInFlight \in Nat /\ MaxInFlight >= 1

None == 0
Ckpts == 1..MaxCkpt
Positions == 1..MaxCkpt          \* each checkpoint advances the source one position
Kafka == ~Recoverable
Tracked == Kafka                 \* the commit-confirmed restore protocol runs for this family

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

--------------------------------------------------------------------------------
(* STATE *)

VARIABLES
    \* Leadership. leaderEpoch is the coordination store's truth; the leader
    \* stamps it on every control frame (Coordinator::fenced_frame_). A zombie
    \* is a superseded coordinator that has not noticed (partitioned or paused)
    \* and keeps its trigger loop running under its old epoch.
    leaderEpoch, coordUp, zombie, zombieEpoch, zombieNext,

    \* Coordinator memory: lost with the process, reseeded from disk on takeover.
    phase,          \* "running" | "draining" | "resolving" | "deploying"
    nextCkpt,       \* next id to allocate
    inFlight,       \* ids triggered and not yet completed or failed
    ackedOk,        \* [Ckpts -> SUBSET Sinks]: ok acks per id
    ackedFail,      \* [Ckpts -> SUBSET Sinks]: failed acks per id
    completeDue,    \* id whose acks all arrived ok; marker not yet written
    toBroadcast,    \* id whose marker is durable; commit not yet broadcast
    markerDue,      \* (mutant broadcast_before_marker) broadcast sent, marker pending
    memCompleted,   \* latest_completed_checkpoint_id
    memConfirmed,   \* latest_confirmed_checkpoint_id
    broadcastIds,   \* ids whose confirmation set was seeded and has not drained
    unconfirmed,    \* [Ckpts -> SUBSET Sinks]: pending_confirms
    drainSet,       \* survivors that must report drained before a restart proceeds
    freshLeader,    \* TRUE between a takeover and its first redeploy
    rewindFloor,    \* the FAILED checkpoint a pending rewind is for (None if no rewind)
    walkC, walkVerdict, walkRetries,   \* the in-doubt resolution walk

    \* The durable store (checkpoint directory). Survives everything.
    completedDisk,  \* ids with a COMPLETED marker
    confirmedDisk,  \* ids with a CONFIRMED marker
    srcCut,         \* [Ckpts -> Nat]: the source's snapshotted cut per id (0 = none)
    sinkCut,        \* [Sinks -> [Ckpts -> Nat]]: cut recorded in each sink snapshot
    sinkHandles,    \* [Sinks -> [Ckpts -> SUBSET Ckpts]]: staged handles per snapshot
    receipts,       \* SUBSET (Sinks \X Ckpts): commit receipts sub<K>-<N>
    unresolvedMk,   \* SUBSET (Sinks \X Ckpts): sub<K>-<N>.unresolved markers

    \* The broker (the external transaction coordinator).
    txn,            \* [Sinks -> [Ckpts -> record]]: st, owner, desc, has
    brokerUp,
    sinkGen,        \* [Sinks -> Nat]: producer epoch of each sink's transactional id;
                    \* bumped by init_transactions, which fences every lower one

    \* Sink processes (state lost with the process) and control frames.
    sink,           \* [Sinks -> record]: up, openTxn, ackDue, ackOk, stage, suppress, opening
    pendingHandles, \* [Sinks -> SUBSET Ckpts]: handles currently in operator state
    barriers,       \* [Sinks -> SUBSET Ckpts]: barriers delivered, not yet processed
    boundEpoch,     \* [Workers -> Nat]: epoch bound at RegisterAck
    msgs,           \* control frames in flight

    \* The source and the job.
    srcPos,         \* logical position the source has emitted up to
    frontier,       \* cut of the current restore point; nothing at or below re-emits
    restorePoint,   \* id restored from (0 = fresh)
    cutOf,          \* [Ckpts -> Nat]: the cut each allocated id took (ghost)

    \* Output and ghosts for the invariants.
    published,      \* [Sinks -> [Positions -> Nat]]: times each position is visible
    restoreSound,   \* FALSE once a restore read participant snapshots of mixed vintage
    staleAccepted,  \* TRUE once a worker acted on a frame from a superseded epoch

    \* Fault budgets.
    workerDeaths, coordDeaths, expiries, snapFails, brokerOutages, walkCancels

vars == << leaderEpoch, coordUp, zombie, zombieEpoch, zombieNext,
           phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue, toBroadcast,
           markerDue, memCompleted, memConfirmed, broadcastIds, unconfirmed, drainSet,
           freshLeader, rewindFloor, walkC, walkVerdict, walkRetries,
           completedDisk, confirmedDisk, srcCut, sinkCut, sinkHandles, receipts,
           unresolvedMk, txn, brokerUp, sinkGen, sink, pendingHandles, barriers,
           boundEpoch, msgs, srcPos, frontier, restorePoint, cutOf, published, restoreSound,
           staleAccepted, workerDeaths, coordDeaths, expiries, snapFails,
           brokerOutages, walkCancels >>

leaderVars == << leaderEpoch, coordUp, zombie, zombieEpoch, zombieNext >>
coordVars  == << phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                 toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                 unconfirmed, drainSet, freshLeader, rewindFloor, walkC, walkVerdict,
                 walkRetries >>
walkVars   == << walkC, walkVerdict, walkRetries >>
diskVars   == << completedDisk, confirmedDisk, srcCut, sinkCut, sinkHandles,
                 receipts, unresolvedMk >>
sinkVars   == << sink, pendingHandles, barriers >>
jobVars    == << srcPos, frontier, restorePoint, cutOf >>
ghostVars  == << published, restoreSound, staleAccepted >>
budgetVars == << workerDeaths, coordDeaths, expiries, snapFails, brokerOutages,
                 walkCancels >>

NoTxn == [st |-> "none", owner |-> 0, desc |-> FALSE, has |-> FALSE]

DownSink == [up |-> FALSE, openTxn |-> None, ackDue |-> None, ackOk |-> TRUE,
             stage |-> "idle", suppress |-> 0, opening |-> FALSE]

\* Every id with any durable record: markers, a source snapshot, or a sink
\* snapshot. A recovered job numbers new checkpoints above all of them
\* (latest_snapshot_id_on_disk and the marker readers in coordinator.cpp).
DurableIds == completedDisk \cup confirmedDisk
              \cup {c \in Ckpts : srcCut[c] # 0}
              \cup {c \in Ckpts : \E s \in Sinks : sinkCut[s][c] # 0}

Publish(s, c) == IF txn[s][c].has
                 THEN [published EXCEPT ![s][cutOf[c]] = @ + 1]
                 ELSE published

--------------------------------------------------------------------------------
(* INITIAL STATE: the job is deployed and running, nothing checkpointed. *)

Init ==
    /\ leaderEpoch = 1 /\ coordUp = TRUE /\ zombie = FALSE /\ zombieEpoch = 0
    /\ zombieNext = 0
    /\ phase = "running" /\ nextCkpt = 1 /\ inFlight = {}
    /\ ackedOk = [c \in Ckpts |-> {}] /\ ackedFail = [c \in Ckpts |-> {}]
    /\ completeDue = None /\ toBroadcast = None /\ markerDue = None
    /\ memCompleted = 0 /\ memConfirmed = 0 /\ broadcastIds = {}
    /\ unconfirmed = [c \in Ckpts |-> {}]
    /\ drainSet = {} /\ freshLeader = FALSE /\ rewindFloor = None
    /\ walkC = None /\ walkVerdict = [s \in Sinks |-> "none"] /\ walkRetries = 0
    /\ completedDisk = {} /\ confirmedDisk = {}
    /\ srcCut = [c \in Ckpts |-> 0]
    /\ sinkCut = [s \in Sinks |-> [c \in Ckpts |-> 0]]
    /\ sinkHandles = [s \in Sinks |-> [c \in Ckpts |-> {}]]
    /\ receipts = {} /\ unresolvedMk = {}
    /\ txn = [s \in Sinks |-> [c \in Ckpts |-> NoTxn]]
    /\ brokerUp = TRUE /\ sinkGen = [s \in Sinks |-> 1]
    /\ sink = [s \in Sinks |-> [DownSink EXCEPT !.up = TRUE]]
    /\ pendingHandles = [s \in Sinks |-> {}]
    /\ barriers = [s \in Sinks |-> {}]
    /\ boundEpoch = [w \in Workers |-> 1]
    /\ msgs = {}
    /\ srcPos = 0 /\ frontier = 0 /\ restorePoint = 0
    /\ cutOf = [c \in Ckpts |-> 0]
    /\ published = [s \in Sinks |-> [p \in Positions |-> 0]]
    /\ restoreSound = TRUE /\ staleAccepted = FALSE
    /\ workerDeaths = 0 /\ coordDeaths = 0 /\ expiries = 0 /\ snapFails = 0
    /\ brokerOutages = 0 /\ walkCancels = 0

--------------------------------------------------------------------------------
(* CHECKPOINT: trigger, barrier, prepare, ack. *)

\* Coordinator::checkpoint_trigger_loop_ allocates an id and sends
\* TriggerCheckpoint to the worker hosting the source. The frame carries the
\* sender's epoch.
Trigger ==
    /\ coordUp /\ phase = "running"
    /\ \A s \in Sinks : sink[s].up      \* the trigger loop visits only fully deployed jobs
    /\ nextCkpt <= MaxCkpt
    /\ Cardinality(inFlight) < MaxInFlight
    /\ LET c == nextCkpt IN
       /\ nextCkpt' = c + 1
       /\ inFlight' = inFlight \cup {c}
       /\ msgs' = msgs \cup {[kind |-> "barrier", c |-> c, epoch |-> leaderEpoch]}
    /\ UNCHANGED << leaderVars, phase, ackedOk, ackedFail, completeDue, toBroadcast,
                    markerDue, memCompleted, memConfirmed, broadcastIds, unconfirmed,
                    drainSet, freshLeader, rewindFloor, walkVars, diskVars, txn, brokerUp, sinkGen, sinkVars,
                    boundEpoch, jobVars, ghostVars, budgetVars >>

\* A superseded coordinator's trigger loop is still running. Its frames carry
\* its stale epoch; fencing is what keeps them from reaching a source.
ZombieTrigger ==
    /\ zombie /\ zombieNext <= MaxCkpt
    /\ msgs' = msgs \cup {[kind |-> "barrier", c |-> zombieNext, epoch |-> zombieEpoch]}
    /\ zombieNext' = zombieNext + 1
    /\ UNCHANGED << leaderEpoch, coordUp, zombie, zombieEpoch, coordVars, diskVars, txn,
                    brokerUp, sinkGen, sinkVars, boundEpoch, jobVars, ghostVars, budgetVars >>

\* The barrier reaches the source (drain_pending_barriers in dag.hpp): the
\* source's cut for the id is snapshotted as the barrier passes
\* (snapshot_offset, then stage_operator_rows pins the copy to this id), and
\* the barrier flows on to every sink behind this interval's records. One
\* logical interval per barrier: the source advanced one position. A frame
\* from an epoch below the worker's bound is dropped (Worker::accept_epoch_).
Barriers == {m \in msgs : m.kind = "barrier"}

DeliverBarrier ==
    \E m \in Barriers :
        /\ m.c = Min({n.c : n \in Barriers})   \* one FIFO control connection: in order
        /\ msgs' = msgs \ {m}
        /\ LET c == m.c
               fenced == m.epoch < boundEpoch[SrcWorker] /\ Bug # "no_fencing"
           IN IF fenced
              THEN UNCHANGED << srcPos, srcCut, cutOf, barriers, staleAccepted >>
              ELSE /\ staleAccepted' = (staleAccepted \/ m.epoch < leaderEpoch)
                   /\ srcPos' = srcPos + 1
                   /\ srcCut' = [srcCut EXCEPT ![c] = srcPos + 1]
                   /\ cutOf' = [cutOf EXCEPT ![c] = srcPos + 1]
                   /\ barriers' = [s \in Sinks |-> IF sink[s].up THEN barriers[s] \cup {c}
                                                                 ELSE barriers[s]]
    /\ UNCHANGED << leaderVars, coordVars, completedDisk, confirmedDisk, sinkCut,
                    sinkHandles, receipts, unresolvedMk, txn, brokerUp, sinkGen, sink,
                    pendingHandles, boundEpoch, frontier, restorePoint, published,
                    restoreSound, budgetVars >>

\* Sink::on_barrier: the prepare. The Kafka sink flushes the open transaction,
\* stages its resume handle inside this checkpoint and will not seal another
\* until the previous commit resolved (kafka_2pc_sink_string); the
\* CommittingSink family prepares and persists a handle under
\* _xo_pending_sub<N>_<ckpt> and may hold several. The snapshot is durable
\* before the ack (ack-after-durable), so the sink's cut and its handle set
\* are on disk here. Fault points: sink.before_prepare is the state before this
\* step, sink.after_prepare the state after it and before SinkAck.
\*
\* The transaction carries this interval's records unless replay suppression
\* swallowed them at emission (their position is at or below the receipted
\* horizon the sink armed at open).
CanPrepare(s) ==
    /\ sink[s].up /\ sink[s].ackDue = None /\ sink[s].stage = "idle"
    /\ barriers[s] # {}
    /\ Kafka => sink[s].openTxn = None

SinkPrepare(s) ==
    /\ CanPrepare(s)
    /\ LET c == Min(barriers[s])
           has == cutOf[c] > sink[s].suppress
           handles == pendingHandles[s] \cup {c}
       IN /\ barriers' = [barriers EXCEPT ![s] = @ \ {c}]
          /\ txn' = [txn EXCEPT ![s][c] = [st |-> "prepared", owner |-> sinkGen[s],
                                          desc |-> FALSE, has |-> has]]
          /\ sinkCut' = [sinkCut EXCEPT ![s][c] = cutOf[c]]
          /\ sinkHandles' = [sinkHandles EXCEPT ![s][c] = handles]
          /\ pendingHandles' = [pendingHandles EXCEPT ![s] = handles]
          /\ sink' = [sink EXCEPT ![s].openTxn = IF Kafka THEN c ELSE @,
                                  ![s].ackDue = c, ![s].ackOk = TRUE]
    /\ UNCHANGED << leaderVars, coordVars, completedDisk, confirmedDisk, srcCut, receipts,
                    unresolvedMk, brokerUp, sinkGen, boundEpoch, msgs, jobVars, ghostVars,
                    budgetVars >>

\* The runner's capture for this checkpoint fails (a transient disk error): the
\* ack goes back ok = FALSE and nothing of this id reaches disk for the sink.
\* The transaction is sealed all the same; the coordinator's abort discards it.
SinkPrepareFails(s) ==
    /\ snapFails < MaxSnapFails
    /\ CanPrepare(s)
    /\ LET c == Min(barriers[s])
           has == cutOf[c] > sink[s].suppress
       IN /\ barriers' = [barriers EXCEPT ![s] = @ \ {c}]
          /\ txn' = [txn EXCEPT ![s][c] = [st |-> "prepared", owner |-> sinkGen[s],
                                          desc |-> FALSE, has |-> has]]
          /\ pendingHandles' = [pendingHandles EXCEPT ![s] = @ \cup {c}]
          /\ sink' = [sink EXCEPT ![s].openTxn = IF Kafka THEN c ELSE @,
                                  ![s].ackDue = c, ![s].ackOk = FALSE]
    /\ snapFails' = snapFails + 1
    /\ UNCHANGED << leaderVars, coordVars, diskVars, brokerUp, sinkGen, boundEpoch, msgs, jobVars,
                    ghostVars, workerDeaths, coordDeaths, expiries, brokerOutages,
                    walkCancels >>

\* SubtaskCheckpointed reaches the coordinator (handle_subtask_checkpointed_).
\* An ack for an id the coordinator no longer tracks is ignored; an ack to a
\* dead coordinator is lost with the connection.
SinkAck(s) ==
    /\ sink[s].up /\ sink[s].ackDue # None
    /\ LET c == sink[s].ackDue IN
       /\ sink' = [sink EXCEPT ![s].ackDue = None]
       /\ IF coordUp /\ c \in inFlight
          THEN IF sink[s].ackOk
               THEN ackedOk' = [ackedOk EXCEPT ![c] = @ \cup {s}] /\ UNCHANGED ackedFail
               ELSE ackedFail' = [ackedFail EXCEPT ![c] = @ \cup {s}] /\ UNCHANGED ackedOk
          ELSE UNCHANGED << ackedOk, ackedFail >>
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, completeDue, toBroadcast,
                    markerDue, memCompleted, memConfirmed, broadcastIds, unconfirmed,
                    drainSet, freshLeader, rewindFloor, walkVars, diskVars, txn, brokerUp, sinkGen,
                    pendingHandles, barriers, boundEpoch, msgs, jobVars, ghostVars,
                    budgetVars >>

--------------------------------------------------------------------------------
(* COMPLETION: marker, broadcast, confirmation. *)

AllAnswered(c) == ackedOk[c] \cup ackedFail[c] = Sinks

\* Every subtask has answered for c. All ok: the checkpoint completes and its
\* marker is due. Any failure: the checkpoint FAILED. No marker is written, an
\* AbortCheckpoint discards every sink's staged transaction for it, and the job
\* rewinds to its last completed checkpoint so the discarded interval is
\* re-produced (correctness sweep item 4: before that fix the job sailed on
\* minus one interval).
\* Found by this model: a checkpoint ABOVE a failed one can complete while the
\* job drains for the failure's rewind (its barrier was already at the sinks),
\* and used to get its marker; the walk then confirmed it and the job restored
\* past the aborted interval, losing it. A checkpoint above the rewind floor is
\* discarded like a failed one: no marker, and its staged transactions are
\* aborted, since the rewind re-produces its interval too.
AboveRewind(c) == rewindFloor # None /\ c > rewindFloor /\ Bug # "complete_above_failed"

\* Checkpoints are decided in id order. Each subtask acks its barriers in the
\* order it met them and the coordinator decides a checkpoint the moment its
\* last ack is processed, so the last ack for c arrives after the last ack for
\* every id below c: a lower checkpoint's fate is always known first.
Decidable == {c \in inFlight : AllAnswered(c)}

CoordComplete ==
    /\ coordUp /\ completeDue = None /\ markerDue = None /\ Decidable # {}
    /\ LET c == Min(Decidable) IN
        /\ inFlight' = inFlight \ {c}
        /\ IF ackedFail[c] = {} /\ ~AboveRewind(c)
           THEN /\ completeDue' = c
                /\ UNCHANGED << msgs, phase, drainSet, rewindFloor >>
           ELSE /\ msgs' = msgs \cup {[kind |-> "abort", c |-> c, epoch |-> leaderEpoch, s |-> s]
                                      : s \in {t \in Sinks : sink[t].up}}
                /\ IF ackedFail[c] # {} /\ Bug # "no_rewind_on_failed_checkpoint"
                   THEN /\ rewindFloor' = IF rewindFloor = None \/ c < rewindFloor THEN c
                                          ELSE rewindFloor
                        /\ IF phase = "running"
                           THEN phase' = "draining" /\ drainSet' = {t \in Sinks : sink[t].up}
                           ELSE UNCHANGED << phase, drainSet >>
                   ELSE UNCHANGED << phase, drainSet, rewindFloor >>
                /\ UNCHANGED completeDue
    /\ UNCHANGED << leaderVars, nextCkpt, ackedOk, ackedFail, toBroadcast, markerDue,
                    memCompleted, memConfirmed, broadcastIds, unconfirmed, freshLeader,
                    walkVars, diskVars, txn, brokerUp, sinkGen, sinkVars, boundEpoch, jobVars,
                    ghostVars, budgetVars >>

\* The COMPLETED-<id> marker, fsync-durable, written before any commit is
\* broadcast (the marker write in handle_subtask_checkpointed_). Found by this
\* model: latest_completed used to advance in memory at completion, before the
\* marker was durable, and a restart in that window redeployed from a
\* checkpoint the next leader could not see; the recoverable family's sinks
\* had already re-committed its handles at open, and the lower restore
\* replayed them. Memory now advances with the durable write. Fault points:
\* coordinator.before_completed_marker is the state before this step,
\* coordinator.after_completed_marker the state after it.
Advance(c) == IF c > memCompleted THEN c ELSE memCompleted

WriteCompleted ==
    /\ coordUp
    /\ \/ /\ Bug # "broadcast_before_marker" /\ completeDue # None
          /\ LET c == completeDue IN
             /\ completedDisk' = completedDisk \cup {c}
             /\ memCompleted' = Advance(c)
             /\ completeDue' = None /\ toBroadcast' = c
             /\ UNCHANGED markerDue
       \/ /\ Bug = "broadcast_before_marker" /\ markerDue # None
          /\ LET c == markerDue IN
             /\ completedDisk' = completedDisk \cup {c}
             /\ memCompleted' = Advance(c)
             /\ markerDue' = None
             /\ UNCHANGED << completeDue, toBroadcast >>
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, memConfirmed,
                    broadcastIds, unconfirmed, drainSet, freshLeader, rewindFloor, walkVars,
                    confirmedDisk, srcCut, sinkCut, sinkHandles, receipts, unresolvedMk,
                    txn, brokerUp, sinkGen, sinkVars, boundEpoch, msgs, jobVars, ghostVars,
                    budgetVars >>

\* CommitCheckpoint to every sink of the job, or withheld: a checkpoint that
\* completed while the job is not running normally (draining for a restart)
\* keeps its marker but is not committed from here, because a broadcast into a
\* half-torn-down job commits some sinks and not others (qual01-20260818a:
\* 13,519 duplicates). The held restart's in-doubt resolution finalises it as
\* one decision. The decision is re-taken at send time, after the marker fsync
\* ran outside the lock. For the commit-confirmed family the checkpoint's
\* confirmation set is seeded here. Fault point:
\* coordinator.before_commit_broadcast is the state before this step.
Broadcast ==
    /\ coordUp
    /\ LET c == IF Bug = "broadcast_before_marker" THEN completeDue ELSE toBroadcast IN
       /\ c # None
       /\ IF Bug = "broadcast_before_marker"
          THEN completeDue' = None /\ markerDue' = c /\ UNCHANGED toBroadcast
          ELSE toBroadcast' = None /\ UNCHANGED << completeDue, markerDue >>
       /\ IF phase # "running" /\ Bug # "broadcast_during_drain"
          THEN UNCHANGED << msgs, unconfirmed, broadcastIds >>
          ELSE /\ msgs' = msgs \cup {[kind |-> "commit", c |-> c, epoch |-> leaderEpoch, s |-> s]
                                     : s \in {t \in Sinks : sink[t].up}}
               /\ unconfirmed' = IF Tracked THEN [unconfirmed EXCEPT ![c] = Sinks]
                                            ELSE unconfirmed
               /\ broadcastIds' = IF Tracked THEN broadcastIds \cup {c} ELSE broadcastIds
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, memCompleted,
                    memConfirmed, drainSet, freshLeader, rewindFloor, walkVars, diskVars, txn, brokerUp, sinkGen,
                    sinkVars, boundEpoch, jobVars, ghostVars, budgetVars >>

\* A commit frame reaches a worker (Worker::handle_commit_checkpoint_): fenced
\* if its epoch is below the worker's bound, dropped if the sink is gone,
\* refused if the sink holds no matching prepared transaction (the dispatch is
\* then not confirmed and the persisted handle waits for restore-time
\* recovery). Dispatch is one FIFO consumer per worker, so a frame waits while
\* the sink is inside another commit. Fault point: sink.before_commit is the
\* state after a commit is accepted and before SinkCommit.
CommitsFor(s) == {m \in msgs : m.kind = "commit" /\ m.s = s}

DeliverCommit ==
    \E m \in msgs :
        /\ m.kind = "commit"
        /\ m.c = Min({n.c : n \in CommitsFor(m.s)})   \* FIFO dispatch per worker
        /\ LET s == m.s
               fenced == m.epoch < boundEpoch[Host[s]] /\ Bug # "no_fencing"
               deliverable == ~sink[s].up \/ fenced \/ sink[s].stage = "idle"
           IN /\ deliverable
              /\ msgs' = msgs \ {m}
              /\ IF sink[s].up /\ ~fenced
                    /\ m.c \in pendingHandles[s] /\ txn[s][m.c].st = "prepared"
                 THEN /\ sink' = [sink EXCEPT ![s].stage = "committing", ![s].openTxn = m.c]
                      /\ staleAccepted' = (staleAccepted \/ m.epoch < leaderEpoch)
                 ELSE UNCHANGED << sink, staleAccepted >>
    /\ UNCHANGED << leaderVars, coordVars, diskVars, txn, brokerUp, sinkGen, pendingHandles,
                    barriers, boundEpoch, jobVars, published, restoreSound, budgetVars >>

\* An abort frame (Worker::handle_abort_checkpoint_): the staged transaction is
\* rolled back and the handle erased. Idempotent against a handle that is gone.
DeliverAbort ==
    \E m \in msgs :
        /\ m.kind = "abort"
        /\ msgs' = msgs \ {m}
        /\ LET s == m.s
               fenced == m.epoch < boundEpoch[Host[s]] /\ Bug # "no_fencing"
           IN IF sink[s].up /\ ~fenced /\ m.c \in pendingHandles[s]
                 /\ txn[s][m.c].st = "prepared" /\ sink[s].stage = "idle"
              THEN /\ txn' = [txn EXCEPT ![s][m.c].st = "aborted"]
                   /\ pendingHandles' = [pendingHandles EXCEPT ![s] = @ \ {m.c}]
                   /\ sink' = [sink EXCEPT ![s].openTxn = IF @ = m.c THEN None ELSE @]
              ELSE UNCHANGED << txn, pendingHandles, sink >>
    /\ UNCHANGED << leaderVars, coordVars, diskVars, brokerUp, sinkGen, barriers, boundEpoch,
                    jobVars, ghostVars, budgetVars >>

\* The external commit executes: commit_transaction on the broker, or the
\* atomic rename / COMMIT PREPARED of the recoverable family. The broker bumps
\* the producer epoch per commit and, until a successor transaction begins on
\* this identity, DescribeTransactions still names the outcome (desc). The
\* recoverable family is done here. Fault point: sink.between_commit_and_receipt
\* is the state after this step and before SinkReceipt.
SinkCommit(s) ==
    /\ sink[s].up /\ sink[s].stage = "committing" /\ brokerUp
    /\ LET c == sink[s].openTxn IN
       /\ txn[s][c].st = "prepared"
       /\ published' = Publish(s, c)
       /\ IF Recoverable
          THEN /\ txn' = [txn EXCEPT ![s][c].st = "committed"]
               /\ pendingHandles' = [pendingHandles EXCEPT ![s] = @ \ {c}]
               /\ sink' = [sink EXCEPT ![s].stage = "idle", ![s].openTxn = None]
          ELSE /\ txn' = [txn EXCEPT ![s][c].st = "committed",
                                     ![s][c].desc = (Bug # "receipt_after_begin")]
               /\ sink' = [sink EXCEPT ![s].stage = "committed"]
               /\ UNCHANGED pendingHandles
    /\ UNCHANGED << leaderVars, coordVars, diskVars, brokerUp, sinkGen, barriers, boundEpoch, msgs,
                    jobVars, restoreSound, staleAccepted, budgetVars >>

\* The commit receipt sub<K>-<N>, fsync-durable, written between the broker's
\* commit and the next begin_transaction (write_commit_receipt_ inside the
\* commit_transaction callback). Fault point: sink.after_external_commit is the
\* state after this step and before SinkConfirm.
SinkReceipt(s) ==
    /\ Kafka /\ sink[s].up /\ sink[s].stage = "committed"
    /\ LET c == sink[s].openTxn IN
       /\ receipts' = IF Bug = "no_receipts" THEN receipts ELSE receipts \cup {<<s, c>>}
       /\ sink' = [sink EXCEPT ![s].stage = "receipted"]
    /\ UNCHANGED << leaderVars, coordVars, completedDisk, confirmedDisk, srcCut, sinkCut,
                    sinkHandles, unresolvedMk, txn, brokerUp, sinkGen, pendingHandles, barriers,
                    boundEpoch, msgs, jobVars, ghostVars, budgetVars >>

\* The staged handle is erased, the next transaction begins (after which the
\* broker no longer names the previous commit), and CommitConfirmed goes to the
\* coordinator (dispatch_commit_checkpoint_). A confirmation reaching a dead
\* coordinator, or one that no longer tracks the id, is lost.
SinkConfirm(s) ==
    /\ Kafka /\ sink[s].up /\ sink[s].stage = "receipted"
    /\ LET c == sink[s].openTxn IN
       /\ txn' = [txn EXCEPT ![s][c].desc = FALSE]
       /\ pendingHandles' = [pendingHandles EXCEPT ![s] = @ \ {c}]
       /\ sink' = [sink EXCEPT ![s].stage = "idle", ![s].openTxn = None]
       /\ unconfirmed' = IF coordUp /\ c \in broadcastIds
                         THEN [unconfirmed EXCEPT ![c] = @ \ {s}]
                         ELSE unconfirmed
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    drainSet, freshLeader, rewindFloor, walkVars, diskVars, brokerUp, sinkGen, barriers,
                    boundEpoch, msgs, jobVars, ghostVars, budgetVars >>

\* Every tracked sink confirmed c: CONFIRMED-<id>, durable
\* (handle_commit_confirmed_). Restores of this family select the newest
\* confirmed checkpoint, never merely the newest completed one.
WriteConfirmed ==
    /\ coordUp /\ Tracked
    /\ \E c \in broadcastIds :
        /\ unconfirmed[c] = {}
        /\ broadcastIds' = broadcastIds \ {c}
        /\ confirmedDisk' = confirmedDisk \cup {c}
        /\ memConfirmed' = IF c > memConfirmed THEN c ELSE memConfirmed
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, unconfirmed, drainSet,
                    freshLeader, rewindFloor, walkVars, completedDisk, srcCut, sinkCut, sinkHandles,
                    receipts, unresolvedMk, txn, brokerUp, sinkGen, sinkVars, boundEpoch, msgs,
                    jobVars, ghostVars, budgetVars >>

--------------------------------------------------------------------------------
(* FAULTS: processes die, the broker forgets, the coordinator is superseded. *)

\* A worker dies: every sink it hosts loses its process state and its
\* undelivered frames. The broker keeps their transactions as they were: a
\* prepared transaction stays prepared until resolved, fenced or expired; a
\* committed one stays committed. The live coordinator detects the loss and
\* begins a restart: survivors are cancelled and must drain before anything
\* redeploys (mark_worker_lost_locked_). A loss during an ongoing drain folds
\* into it.
WorkerDies(w) ==
    /\ workerDeaths < MaxWorkerDeaths
    /\ \E s \in Sinks : Host[s] = w /\ sink[s].up
    /\ LET dead == {s \in Sinks : Host[s] = w} IN
       /\ sink' = [s \in Sinks |-> IF s \in dead THEN DownSink ELSE sink[s]]
       /\ pendingHandles' = [s \in Sinks |-> IF s \in dead THEN {} ELSE pendingHandles[s]]
       /\ barriers' = [s \in Sinks |-> IF s \in dead THEN {} ELSE barriers[s]]
       /\ msgs' = {m \in msgs : IF m.kind = "barrier" THEN SrcWorker # w ELSE m.s \notin dead}
       /\ IF coordUp /\ phase = "running"
          THEN phase' = "draining" /\ drainSet' = {s \in Sinks : sink[s].up /\ s \notin dead}
          ELSE IF coordUp /\ phase = "draining"
               THEN drainSet' = drainSet \ dead /\ UNCHANGED phase
               ELSE UNCHANGED << phase, drainSet >>
    /\ workerDeaths' = workerDeaths + 1
    /\ UNCHANGED << leaderVars, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, freshLeader, rewindFloor, walkVars, diskVars, txn, brokerUp, sinkGen, boundEpoch,
                    jobVars, ghostVars, coordDeaths, expiries, snapFails, brokerOutages,
                    walkCancels >>

\* A cancelled survivor drains: the sink closes. close() aborts only the open
\* tail; a barrier-sealed prepared transaction is preserved for the resolver
\* (the cascade fix of 2026-08-18). The mutant restores the old behaviour.
SinkDrains(s) ==
    /\ coordUp /\ phase = "draining" /\ s \in drainSet /\ sink[s].up
    /\ drainSet' = drainSet \ {s}
    /\ sink' = [sink EXCEPT ![s] = DownSink]
    /\ pendingHandles' = [pendingHandles EXCEPT ![s] = {}]
    /\ barriers' = [barriers EXCEPT ![s] = {}]
    /\ msgs' = {m \in msgs : m.kind = "barrier" \/ m.s # s}
    /\ txn' = IF Bug = "close_aborts_prepared"
              THEN [txn EXCEPT ![s] = [c \in Ckpts |-> IF txn[s][c].st = "prepared"
                                                        THEN [txn[s][c] EXCEPT !.st = "aborted"]
                                                        ELSE txn[s][c]]]
              ELSE txn
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, freshLeader, rewindFloor, walkVars, diskVars, brokerUp, sinkGen, boundEpoch,
                    jobVars, ghostVars, budgetVars >>

\* The coordinator dies. Its memory goes with it; the workers' control sessions
\* end and every subtask is cancelled (WorkerSupervisor), so every sink closes
\* with its prepared transactions preserved. Frames on the wire are lost with
\* the connections.
CoordDies ==
    /\ coordDeaths < MaxCoordDeaths /\ coordUp
    /\ coordUp' = FALSE
    /\ sink' = [s \in Sinks |-> DownSink]
    /\ pendingHandles' = [s \in Sinks |-> {}]
    /\ barriers' = [s \in Sinks |-> {}]
    /\ msgs' = {}
    /\ coordDeaths' = coordDeaths + 1
    /\ UNCHANGED << leaderEpoch, zombie, zombieEpoch, zombieNext, coordVars, diskVars, txn,
                    brokerUp, sinkGen, boundEpoch, jobVars, ghostVars, workerDeaths, expiries,
                    snapFails, brokerOutages, walkCancels >>

\* The coordinator is superseded without dying: partitioned from the store or
\* paused past its lease, it keeps its trigger loop. The workers reconnect to
\* the new leader (their subtasks cancelled on the way, prepared transactions
\* preserved) and bind its epoch; the zombie's frames now carry a lower epoch
\* than the workers' bound.
CoordSuperseded ==
    /\ coordDeaths < MaxCoordDeaths /\ coordUp /\ ~zombie
    /\ zombie' = TRUE /\ zombieEpoch' = leaderEpoch /\ zombieNext' = nextCkpt
    /\ coordUp' = FALSE
    /\ sink' = [s \in Sinks |-> DownSink]
    /\ pendingHandles' = [s \in Sinks |-> {}]
    /\ barriers' = [s \in Sinks |-> {}]
    /\ msgs' = {}
    /\ coordDeaths' = coordDeaths + 1
    /\ UNCHANGED << leaderEpoch, coordVars, diskVars, txn, brokerUp, sinkGen, boundEpoch, jobVars,
                    ghostVars, workerDeaths, expiries, snapFails, brokerOutages,
                    walkCancels >>

ZombieStops ==
    /\ zombie
    /\ zombie' = FALSE
    /\ UNCHANGED << leaderEpoch, coordUp, zombieEpoch, zombieNext, coordVars, diskVars, txn,
                    brokerUp, sinkGen, sinkVars, boundEpoch, msgs, jobVars, ghostVars, budgetVars >>

\* A new leader takes over (recover_persisted_jobs). Memory is reseeded from
\* the durable markers; every worker re-registers and binds the new epoch; the
\* job is recovered through the same drain-resolve-redeploy path a worker loss
\* takes, with nothing left to drain.
CoordRecovers ==
    /\ ~coordUp
    /\ coordUp' = TRUE /\ leaderEpoch' = leaderEpoch + 1
    /\ boundEpoch' = [w \in Workers |-> leaderEpoch + 1]
    /\ phase' = "draining" /\ drainSet' = {}
    /\ inFlight' = {} /\ completeDue' = None /\ toBroadcast' = None /\ markerDue' = None
    /\ ackedOk' = [c \in Ckpts |-> {}] /\ ackedFail' = [c \in Ckpts |-> {}]
    /\ memCompleted' = Max(completedDisk)
    /\ memConfirmed' = Max(confirmedDisk)
    /\ broadcastIds' = {} /\ unconfirmed' = [c \in Ckpts |-> {}]
    /\ freshLeader' = TRUE /\ rewindFloor' = None
    /\ walkC' = None /\ walkVerdict' = [s \in Sinks |-> "none"] /\ walkRetries' = 0
    /\ UNCHANGED << zombie, zombieEpoch, zombieNext, nextCkpt, diskVars, txn, brokerUp, sinkGen,
                    sinkVars, msgs, jobVars, ghostVars, budgetVars >>

\* transaction.timeout.ms: a prepared transaction whose producer is gone is
\* aborted by the broker. The record of an aborted transaction answers "not
\* committed" to every later probe.
TxnExpires ==
    /\ expiries < MaxExpiries
    /\ \E s \in Sinks, c \in Ckpts :
        /\ txn[s][c].st = "prepared"
        /\ ~sink[s].up \/ txn[s][c].owner # sinkGen[s]
        /\ txn' = [txn EXCEPT ![s][c].st = "aborted"]
    /\ expiries' = expiries + 1
    /\ UNCHANGED << leaderVars, coordVars, diskVars, brokerUp, sinkGen, sinkVars, boundEpoch, msgs,
                    jobVars, ghostVars, workerDeaths, coordDeaths, snapFails,
                    brokerOutages, walkCancels >>

BrokerGoesDown ==
    /\ Kafka /\ brokerUp /\ brokerOutages < MaxBrokerOutages
    /\ brokerUp' = FALSE /\ brokerOutages' = brokerOutages + 1
    /\ UNCHANGED << leaderVars, coordVars, diskVars, txn, sinkGen, sinkVars, boundEpoch, msgs,
                    jobVars, ghostVars, workerDeaths, coordDeaths, expiries, snapFails,
                    walkCancels >>

BrokerComesBack ==
    /\ ~brokerUp
    /\ brokerUp' = TRUE
    /\ UNCHANGED << leaderVars, coordVars, diskVars, txn, sinkGen, sinkVars, boundEpoch, msgs,
                    jobVars, ghostVars, budgetVars >>

--------------------------------------------------------------------------------
(* RECOVERY: drain, resolve, redeploy, open. *)

\* The in-doubt walk (resolve_in_doubt_commits) visits each id in
\* (confirmed, completed]. An id without a marker never completed and is
\* skipped. Otherwise every staged handle gets a verdict, and a handle is read
\* ONLY from its own subtask's snapshot for that id.
WalkAt == coordUp /\ phase = "resolving" /\ walkC # None /\ walkC \in Ckpts
          /\ walkC <= memCompleted
Walking == WalkAt /\ walkC \in completedDisk

WalkHandles(c) == {s \in Sinks : c \in sinkHandles[s][c]}

WalkOpen(s) == Walking /\ s \in WalkHandles(walkC) /\ walkVerdict[s] = "none"

Unsettled == {s \in Sinks : WalkOpen(s) /\ <<s, walkC>> \notin receipts}

\* Every unreceipted handle staged in a completed checkpoint ABOVE the id the
\* walk stops at. Found by this model, not by a rig: the walk used to stop at
\* the first refused checkpoint, and a commit that executed without its receipt
\* in a later completed checkpoint (an ack-window kill) was never proven; the
\* restore below then replayed its interval as duplicates. The walk now leaves
\* an .unresolved marker for each such handle, so the owning sink's pre-fence
\* describe settles it before anything fences it.
LaterUnreceipted(k) ==
    IF Bug = "refusal_wall" THEN {}
    ELSE {<<s, c>> \in Sinks \X Ckpts : c \in completedDisk /\ c > k /\ c <= memCompleted
                                        /\ c \in sinkHandles[s][c] /\ <<s, c>> \notin receipts}

NextId == /\ walkC' = walkC + 1
          /\ walkVerdict' = [s \in Sinks |-> "none"] /\ walkRetries' = 0

WalkSkips ==
    /\ WalkAt /\ walkC \notin completedDisk
    /\ NextId
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, drainSet, freshLeader, rewindFloor, diskVars, txn, brokerUp, sinkGen, sinkVars,
                    boundEpoch, msgs, jobVars, ghostVars, budgetVars >>

\* A receipt on disk is the sink's own record of the broker's acknowledgement:
\* COMMITTED, no wire call. Nothing that happens to the broker afterwards can
\* retract a commit that executed (qual01-20260818b was the inversion of this).
WalkReadsReceipt(s) ==
    /\ WalkOpen(s) /\ <<s, walkC>> \in receipts
    /\ walkVerdict' = [walkVerdict EXCEPT ![s] = "committed"]
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, drainSet, freshLeader, rewindFloor, walkC, walkRetries, diskVars, txn,
                    brokerUp, sinkGen, sinkVars, boundEpoch, msgs, jobVars, ghostVars, budgetVars >>

\* The wire probe: EndTxn(commit) with the dead producer's identity. A still
\* prepared transaction commits (the probe EXECUTES the commit). A transaction
\* that already committed answers fenced, because the broker bumps the epoch
\* per commit, and DescribeTransactions disambiguates it while the broker still
\* names the outcome. Every commit the walk proves gets its receipt
\* materialised from the handle's horizon (qual01-20260819f). An aborted
\* transaction, or a commit the broker no longer names, is a final refusal.
\* The walk probes every handle even after a refusal, so no later commit is
\* left unproven and unreceipted.
WalkProbes(s) ==
    /\ WalkOpen(s) /\ brokerUp /\ <<s, walkC>> \notin receipts
    /\ Bug = "stop_at_first_refusal" => \A t \in Sinks : walkVerdict[t] # "refused"
    /\ LET c == walkC
           t == txn[s][c]
           committed == \/ t.st = "prepared"
                        \/ (t.st = "committed" /\ t.desc)
       IN IF committed
          THEN /\ txn' = [txn EXCEPT ![s][c].st = "committed", ![s][c].desc = TRUE]
               /\ published' = IF t.st = "prepared" THEN Publish(s, c) ELSE published
               /\ receipts' = IF Bug = "no_materialised_receipts" THEN receipts
                              ELSE receipts \cup {<<s, c>>}
               /\ walkVerdict' = [walkVerdict EXCEPT ![s] = "committed"]
          ELSE /\ walkVerdict' = [walkVerdict EXCEPT ![s] = "refused"]
               /\ UNCHANGED << txn, published, receipts >>
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, drainSet, freshLeader, rewindFloor, walkC, walkRetries, completedDisk,
                    confirmedDisk, srcCut, sinkCut, sinkHandles, unresolvedMk, brokerUp, sinkGen,
                    sinkVars, boundEpoch, msgs, jobVars, restoreSound, staleAccepted,
                    budgetVars >>

\* The broker is unreachable: transport-inconclusive, retried in place with a
\* bounded budget.
WalkRetries ==
    /\ Walking /\ ~brokerUp /\ Unsettled # {} /\ walkRetries < 2
    /\ walkRetries' = walkRetries + 1
    /\ UNCHANGED << leaderVars, phase, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, drainSet, freshLeader, rewindFloor, walkC, walkVerdict, diskVars, txn,
                    brokerUp, sinkGen, sinkVars, boundEpoch, msgs, jobVars, ghostVars, budgetVars >>

\* Retries exhausted, or the watchdog's deadline cancelled the walk: it ends
\* UNRESOLVED. Every unsettled handle is persisted as an .unresolved marker (the
\* walk's mandated final act, written even when cancelled), the restore point
\* stays where it is, and the owning sink settles the orphan before it opens a
\* producer.
EndUnresolved ==
    /\ unresolvedMk' = unresolvedMk \cup {<<s, walkC>> : s \in Unsettled} \cup LaterUnreceipted(walkC)
    /\ phase' = "deploying" /\ walkC' = None
    /\ UNCHANGED << leaderVars, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, drainSet, freshLeader, rewindFloor, walkVerdict, walkRetries,
                    completedDisk, confirmedDisk, srcCut, sinkCut, sinkHandles, receipts,
                    txn, brokerUp, sinkGen, sinkVars, boundEpoch, msgs, jobVars, ghostVars >>

WalkExhausted ==
    /\ Walking /\ ~brokerUp /\ Unsettled # {} /\ walkRetries >= 2
    /\ EndUnresolved
    /\ UNCHANGED budgetVars

WalkCancelled ==
    /\ Walking /\ Unsettled # {} /\ walkCancels < MaxWalkCancels
    /\ walkCancels' = walkCancels + 1
    /\ EndUnresolved
    /\ UNCHANGED << workerDeaths, coordDeaths, expiries, snapFails, brokerOutages >>

\* Every handle of the id has a verdict. All committed: CONFIRMED-<id> is
\* written and the walk moves on. Any refusal: the walk stops and the job
\* restores from the last confirmed id; the committed siblings' intervals will
\* be replayed and are swallowed by their receipts. A checkpoint staging no
\* handle at all cannot be proven and stops the walk too.
WalkDecides ==
    /\ Walking
    /\ \/ \A s \in WalkHandles(walkC) : walkVerdict[s] # "none"
       \/ (Bug = "stop_at_first_refusal" /\ \E t \in Sinks : walkVerdict[t] = "refused")
    /\ IF WalkHandles(walkC) # {} /\ \A s \in WalkHandles(walkC) : walkVerdict[s] = "committed"
       THEN /\ confirmedDisk' = confirmedDisk \cup {walkC}
            /\ memConfirmed' = walkC
            /\ NextId
            /\ UNCHANGED << phase, unresolvedMk >>
       ELSE /\ phase' = "deploying" /\ walkC' = None
            /\ unresolvedMk' = unresolvedMk \cup {<<s, walkC>> : s \in Unsettled}
                                            \cup LaterUnreceipted(walkC)
            /\ UNCHANGED << confirmedDisk, memConfirmed, walkVerdict, walkRetries >>
    /\ UNCHANGED << leaderVars, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, broadcastIds, unconfirmed,
                    drainSet, freshLeader, rewindFloor, completedDisk, srcCut, sinkCut, sinkHandles,
                    receipts, txn, brokerUp, sinkGen, sinkVars, boundEpoch, msgs,
                    jobVars, ghostVars, budgetVars >>

WalkFinishes ==
    /\ coordUp /\ phase = "resolving" /\ walkC # None /\ walkC > memCompleted
    /\ phase' = "deploying" /\ walkC' = None
    /\ UNCHANGED << leaderVars, nextCkpt, inFlight, ackedOk, ackedFail, completeDue,
                    toBroadcast, markerDue, memCompleted, memConfirmed, broadcastIds,
                    unconfirmed, drainSet, freshLeader, rewindFloor, walkVerdict, walkRetries, diskVars,
                    txn, brokerUp, sinkGen, sinkVars, boundEpoch, msgs, jobVars, ghostVars,
                    budgetVars >>

\* The redeploy (restart_job_locked_ / recover_one_persisted_job_). The
\* restore point is the newest confirmed checkpoint for the commit-confirmed
\* family and the newest completed one otherwise; the source rewinds to its
\* cut. A recovered coordinator numbers new checkpoints above every id with a
\* durable record, never merely above the restore point (qual01-20260817c
\* reused 246; 20260819g assembled one id from two vintages). The ghost
\* restoreSound records whether the participant snapshots the restore reads
\* agree on the cut.
MemCompletedView == IF Bug = "restore_from_memory" /\ completeDue # None /\ completeDue > memCompleted
                    THEN completeDue ELSE memCompleted

RestoreId == IF Tracked /\ Bug # "restore_from_completed" THEN memConfirmed ELSE MemCompletedView

RedeployEffects ==
    /\ LET r == RestoreId
           cut == IF r = None THEN 0 ELSE srcCut[r]
       IN /\ restorePoint' = r
          /\ frontier' = cut /\ srcPos' = cut
          /\ restoreSound' = (restoreSound /\
                (r = None \/ \A s \in Sinks : sinkCut[s][r] = 0 \/ sinkCut[s][r] = srcCut[r]))
          /\ nextCkpt' = IF ~freshLeader THEN nextCkpt
                         ELSE IF Bug = "id_reuse" THEN r + 1
                         ELSE Max(DurableIds \cup {r}) + 1
    /\ phase' = "running" /\ freshLeader' = FALSE /\ rewindFloor' = None
    /\ inFlight' = {} /\ completeDue' = None /\ toBroadcast' = None /\ markerDue' = None
    /\ ackedOk' = [c \in Ckpts |-> {}] /\ ackedFail' = [c \in Ckpts |-> {}]
    /\ broadcastIds' = {} /\ unconfirmed' = [c \in Ckpts |-> {}]
    /\ sink' = [s \in Sinks |-> [DownSink EXCEPT !.opening = TRUE]]
    /\ pendingHandles' = [s \in Sinks |-> {}]
    /\ barriers' = [s \in Sinks |-> {}]
    /\ msgs' = {}

\* The redeploy after a held resolution: the resolution thread re-acquires the
\* lock and runs restart_job_locked_.
Redeploy ==
    /\ coordUp /\ phase = "deploying"
    /\ RedeployEffects
    /\ UNCHANGED << leaderVars, memCompleted, memConfirmed, drainSet, walkVars, diskVars,
                    txn, brokerUp, sinkGen, boundEpoch, cutOf, published, staleAccepted,
                    budgetVars >>

\* The drain is covered. A tracked job with a completed-but-unconfirmed gap
\* holds its redeploy for in-doubt resolution
\* (stage_in_doubt_resolution_locked_). Anything else redeploys in the same
\* lock hold (restart_job_locked_), so no ack can land in between.
RestartProceeds ==
    /\ coordUp /\ phase = "draining" /\ drainSet = {}
    /\ IF Tracked /\ MemCompletedView > memConfirmed
       THEN /\ phase' = "resolving"
            /\ walkC' = memConfirmed + 1
            /\ walkVerdict' = [s \in Sinks |-> "none"] /\ walkRetries' = 0
            /\ UNCHANGED << nextCkpt, inFlight, ackedOk, ackedFail, completeDue, toBroadcast,
                            markerDue, broadcastIds, unconfirmed, freshLeader, rewindFloor, sinkVars, msgs,
                            srcPos, frontier, restorePoint, restoreSound >>
       ELSE /\ RedeployEffects
            /\ UNCHANGED walkVars
    /\ UNCHANGED << leaderVars, memCompleted, memConfirmed, drainSet, diskVars, txn, brokerUp,
                    sinkGen, boundEpoch, cutOf, published, staleAccepted, budgetVars >>

\* The redeployed sink opens. Kafka family (open() in the 2PC sink): first, an
\* .unresolved marker for one of its own handles is settled with a read-only
\* DescribeTransactions on the never-fenced identity: a commit the broker still
\* names gets its receipt written here (suppression then arms from it); an
\* undecided or aborted transaction is left for the init's abort. While no
\* broker can answer, the sink REFUSES to open (a blind fence would erase the
\* distinction for good). Second, init_transactions fences every older
\* identity: undecided transactions abort, and the broker stops naming their
\* outcomes. Third, replay suppression arms from the receipts newer than the
\* restore point: re-emissions at or below the receipted horizon are swallowed.
\* Recoverable family (CommittingSink::open): recover_all_ re-commits every
\* handle the restored snapshot holds; commit is idempotent.
Older(s) == {c \in Ckpts : txn[s][c].owner # 0 /\ txn[s][c].owner <= sinkGen[s]}

SinkOpens(s) ==
    /\ sink[s].opening
    /\ LET r == restorePoint
           \* Only the newest marker above the restore point is consulted: the
           \* broker's DescribeTransactions names one transaction per identity,
           \* the last, and the receipt-before-begin ordering means only a
           \* sink's last transaction can be committed without a receipt.
           above == {c \in Ckpts : <<s, c>> \in unresolvedMk /\ c > r}
           myMarkers == IF above = {} THEN {} ELSE {Max(above)}
           describeOk == Bug = "blind_fence" \/ myMarkers = {} \/ brokerUp
           describedCommits == IF Bug = "blind_fence" THEN {}
                               ELSE {c \in myMarkers : txn[s][c].st = "committed" /\ txn[s][c].desc}
           receipts1 == IF Kafka THEN receipts \cup {<<s, c>> : c \in describedCommits}
                        ELSE receipts
           recoverSet == IF Recoverable /\ r # None
                         THEN {c \in sinkHandles[s][r] : txn[s][c].st = "prepared"}
                         ELSE {}
           after == [c \in Ckpts |->
                        IF Kafka /\ c \in Older(s) /\ txn[s][c].st = "prepared"
                        THEN [txn[s][c] EXCEPT !.st = "aborted", !.desc = FALSE]
                        ELSE IF Kafka /\ c \in Older(s)
                        THEN [txn[s][c] EXCEPT !.desc = FALSE]
                        ELSE IF c \in recoverSet
                        THEN [txn[s][c] EXCEPT !.st = "committed"]
                        ELSE txn[s][c]]
           recovered == {p \in Positions : \E c \in recoverSet : cutOf[c] = p /\ txn[s][c].has}
           horizon == Max({cutOf[c] : c \in {d \in Ckpts : <<s, d>> \in receipts1 /\ d > r}})
       IN /\ describeOk
          /\ receipts' = receipts1
          /\ unresolvedMk' = unresolvedMk \ {<<s, c>> : c \in myMarkers}
          /\ txn' = [txn EXCEPT ![s] = after]
          /\ published' = [published EXCEPT ![s] =
                             [p \in Positions |-> IF p \in recovered THEN @[p] + 1 ELSE @[p]]]
          /\ sinkGen' = [sinkGen EXCEPT ![s] = @ + 1]
          /\ sink' = [sink EXCEPT ![s] = [up |-> TRUE,
                                          openTxn |-> None, ackDue |-> None, ackOk |-> TRUE,
                                          stage |-> "idle", opening |-> FALSE,
                                          suppress |-> IF Kafka /\ horizon > frontier
                                                       THEN horizon ELSE 0]]
    /\ UNCHANGED << leaderVars, coordVars, completedDisk, confirmedDisk, srcCut, sinkCut,
                    sinkHandles, brokerUp, pendingHandles, barriers, boundEpoch, msgs,
                    jobVars, restoreSound, staleAccepted, budgetVars >>

--------------------------------------------------------------------------------
(* THE RUN ENDS when every allocated id is settled and nothing is in flight. *)

Quiescent ==
    /\ coordUp /\ phase = "running" /\ ~zombie /\ brokerUp
    /\ nextCkpt > MaxCkpt /\ inFlight = {}
    /\ completeDue = None /\ toBroadcast = None /\ markerDue = None
    /\ msgs = {} /\ broadcastIds = {}
    /\ \A s \in Sinks : sink[s].up /\ sink[s].ackDue = None /\ sink[s].stage = "idle"
                        /\ barriers[s] = {} /\ ~sink[s].opening
                        /\ (Kafka => sink[s].openTxn = None)

Done == Quiescent /\ UNCHANGED vars

Next ==
    \/ Trigger \/ ZombieTrigger \/ DeliverBarrier
    \/ \E s \in Sinks : SinkPrepare(s) \/ SinkPrepareFails(s) \/ SinkAck(s)
    \/ CoordComplete \/ WriteCompleted \/ Broadcast \/ DeliverCommit \/ DeliverAbort
    \/ \E s \in Sinks : SinkCommit(s) \/ SinkReceipt(s) \/ SinkConfirm(s)
    \/ WriteConfirmed
    \/ \E w \in Workers : WorkerDies(w)
    \/ \E s \in Sinks : SinkDrains(s)
    \/ CoordDies \/ CoordSuperseded \/ ZombieStops \/ CoordRecovers
    \/ TxnExpires \/ BrokerGoesDown \/ BrokerComesBack
    \/ RestartProceeds \/ WalkSkips
    \/ \E s \in Sinks : WalkReadsReceipt(s) \/ WalkProbes(s)
    \/ WalkRetries \/ WalkExhausted \/ WalkCancelled \/ WalkDecides \/ WalkFinishes
    \/ Redeploy
    \/ \E s \in Sinks : SinkOpens(s)
    \/ Done

\* Faults are unfair: the model may inject them or not. Everything else is
\* weakly fair, which is what the liveness property assumes.
Fairness ==
    /\ WF_vars(Trigger) /\ WF_vars(DeliverBarrier)
    /\ \A s \in Sinks : WF_vars(SinkPrepare(s)) /\ WF_vars(SinkAck(s))
                        /\ WF_vars(SinkCommit(s)) /\ WF_vars(SinkReceipt(s))
                        /\ WF_vars(SinkConfirm(s)) /\ WF_vars(SinkDrains(s))
                        /\ WF_vars(WalkReadsReceipt(s)) /\ WF_vars(WalkProbes(s))
                        /\ WF_vars(SinkOpens(s))
    /\ WF_vars(CoordComplete) /\ WF_vars(WriteCompleted) /\ WF_vars(Broadcast)
    /\ WF_vars(DeliverCommit) /\ WF_vars(DeliverAbort) /\ WF_vars(WriteConfirmed)
    /\ WF_vars(CoordRecovers) /\ WF_vars(ZombieStops) /\ WF_vars(BrokerComesBack)
    /\ WF_vars(RestartProceeds) /\ WF_vars(WalkSkips) /\ WF_vars(WalkRetries)
    /\ WF_vars(WalkExhausted) /\ WF_vars(WalkDecides) /\ WF_vars(WalkFinishes)
    /\ WF_vars(Redeploy)

Spec == Init /\ [][Next]_vars /\ Fairness

--------------------------------------------------------------------------------
(* INVARIANTS *)

TypeOK ==
    /\ phase \in {"running", "draining", "resolving", "deploying"}
    /\ nextCkpt \in 1..(MaxCkpt + 1)
    /\ inFlight \subseteq Ckpts
    /\ completedDisk \subseteq Ckpts /\ confirmedDisk \subseteq Ckpts
    /\ receipts \subseteq Sinks \X Ckpts /\ unresolvedMk \subseteq Sinks \X Ckpts
    /\ \A s \in Sinks, c \in Ckpts :
         txn[s][c].st \in {"none", "prepared", "committed", "aborted"}
    /\ \A s \in Sinks : sink[s].stage \in {"idle", "committing", "committed", "receipted"}

\* No sink publishes any position more than once.
NoDuplicate ==
    \A s \in Sinks, p \in Positions : published[s][p] <= 1

\* The cut of the newest confirmed checkpoint (completed, for the recoverable
\* family) is a frontier the protocol vouches for: every position at or below
\* it is published exactly once at every sink, or is held prepared for a
\* completed checkpoint whose commit the recoverable family will re-execute.
VouchedFor == IF Tracked THEN Max(confirmedDisk) ELSE Max(completedDisk)

VouchedCut == IF VouchedFor = None THEN 0 ELSE cutOf[VouchedFor]

\* A completed checkpoint's handle is committed by the commit dispatch or, for
\* the recoverable family, re-committed by recover_all_ at the next open.
\* Completion counts from the coordinator's memory: the marker fsync trails it.
Held(s, p) == Recoverable /\ \E c \in Ckpts :
                 /\ c \in completedDisk \/ c <= memCompleted
                 /\ txn[s][c].st = "prepared" /\ cutOf[c] = p /\ txn[s][c].has

NoLoss ==
    \A s \in Sinks, p \in 1..VouchedCut :
        published[s][p] = 1 \/ (published[s][p] = 0 /\ Held(s, p))

\* Positions the source can no longer re-emit are published or held.
FrontierCovered ==
    \A s \in Sinks, p \in 1..frontier :
        published[s][p] = 1 \/ (published[s][p] = 0 /\ Held(s, p))

\* A restore never read participant snapshots of mixed vintage.
RestoreSound == restoreSound

\* No worker acted on a frame from a superseded coordinator.
Fenced == ~staleAccepted

\* A CONFIRMED marker never outruns the commits it vouches for.
ConfirmedMeansCommitted ==
    Tracked => \A c \in confirmedDisk, s \in Sinks :
                 c \in sinkHandles[s][c] => txn[s][c].st = "committed"

Safety == NoDuplicate /\ NoLoss /\ FrontierCovered /\ RestoreSound /\ ConfirmedMeansCommitted

--------------------------------------------------------------------------------
(* LIVENESS: with bounded faults and fair progress, the run settles with every
   vouched-for position published exactly once at every sink. *)

Settled ==
    /\ Quiescent
    /\ \A s \in Sinks, p \in 1..VouchedCut : published[s][p] = 1

EventuallySettled == <>[]Settled

================================================================================
