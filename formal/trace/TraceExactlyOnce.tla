------------------------- MODULE TraceExactlyOnce -------------------------
(* Trace validation for the exactly-once protocol (design record 012).

   A protocol trace is the engine's own account of a run: one JSON object per
   line, emitted at the protocol's decision points (include/clink/cluster/
   protocol_trace.hpp), merged and ordered by scripts/protocol-trace-merge.py.
   This module constrains the specification's next-state relation to follow
   the recorded events in order. Each event names the action it corresponds
   to and pins the parameters the engine observed (which checkpoint, which
   sink, which outcome); everything the engine cannot observe (the broker
   forgetting a transaction, a coordinator dying) may happen as a hidden step
   between events, within the fault budgets the trace itself implies.

   The trace is accepted when some path consumes every event. Hidden steps
   make the state graph a small tree rather than a line, and a branch that
   takes a hidden step the run did not need dies out early, so a dead end is
   not a verdict: deadlock checking is off, and TraceAccepted (a TLC
   postcondition) reads the highest event index any path reached. Reaching
   past the last event accepts the run; anything less names the first event
   no allowed step produced.

   Events for subtasks that are not two-phase sinks (they never prepare a
   transaction) are stutters: the model abstracts a job to its source and its
   sinks. Placement events are stutters too; they only tell the module which
   worker hosts which sink and which hosts the source.

   The model's constants (the sink set, the hosts, the fault budgets, the
   checkpoint range) come from TraceConstants, a module of literals the
   validator generates beside the merged trace (scripts/protocol-trace-merge.py
   --constants) and puts on TLC's library path. They used to be derived from
   the trace here; TLC re-evaluates a definition on every reference, so every
   step walked the whole trace again and validation was quadratic in trace
   length (180 events in 6 seconds, 900 in 120, 1,500 in 300). The current
   event is read into a variable once per step for the same reason.

   One property is checked here and not in the specification: no two
   coordinators that can both still reach a worker trigger the same id. The
   engine keeps it with the job's checkpoint-id record, on which each id is
   claimed once across every coordinator of the job, and with the renumber
   past a refused claim. The specification does not keep it: ZombieTrigger
   takes any id, which is harmless there, because the model binds every
   worker to the new epoch at the takeover and so fences the superseded
   coordinator's barriers. In the engine a worker that has not re-registered
   with the leader still accepts them, and its capture of a shared id writes
   the path the leader's capture of that id writes. A ghost of the ids
   triggered under each epoch costs the model searches nothing here, and a
   trace whose leader and superseded coordinator both trigger one id diverges
   at the second Trigger. *)
EXTENDS ExactlyOnce, Json, IOUtils, TLC, Sequences, TraceConstants

\* The merged trace, named by the environment (the validator sets it).
\* Referenced exactly once per step (ev' = Trace[l + 1]); see the header.
TraceFile == IOEnv.CLINK_TRACE_FILE
Trace == ndJsonDeserialize(TraceFile)

Has(e, f) == f \in DOMAIN e

\* The engine's epochs need not start at 1; the model's do.
ModelEpoch(e) == e - TraceFirstEpoch + 1

--------------------------------------------------------------------------------
(* Following the trace. *)

VARIABLES l,         \* index of the next event to match
          ev,        \* Trace[l], read once per step
          placed,    \* [Sinks -> Workers]: where each sink is deployed RIGHT NOW
          triggered  \* <<model epoch, id>> per id triggered (see TriggeredElsewhere)

\* Register 1 holds the highest event index any path has reached; the
\* validator runs TLC with one worker, so the register is a single number.
ASSUME TLCSet(1, 1)
Reached(i) == TLCSet(1, IF i > TLCGet(1) THEN i ELSE TLCGet(1))

E == ev
Is(kind) == E.event = kind

\* Epoch 0 is a coordinator without leader election (an HA leader's epoch is
\* always above the one it displaced, so at least 1). It stamps no fence
\* because nothing can supersede it: the previous coordinator of its job is
\* gone only because its process died, which is what an embedded `clink run`
\* resuming from its own checkpoints after a kill records. The model still
\* advances its epoch at the recovery - the fence is abstract there - so an
\* unfenced event pins no epoch, and instead admits no zombie: a run whose
\* coordinator could not be superseded has none.
Unfenced == E.epoch = 0

\* A line stamped by a coordinator the model has already superseded: its epoch
\* is not the leader's. A line with no epoch, or the unfenced epoch 0, is the
\* leader's: a coordinator that cannot be superseded has no zombie.
ZombieLine == Has(E, "epoch") /\ ~Unfenced /\ ModelEpoch(E.epoch) # leaderEpoch

\* A superseded coordinator's line written before the takeover's CoordRecovers:
\* the model's leaderEpoch moves only at that step, so the line does not yet
\* read as a zombie's, but the hidden supersession is already taken and the
\* line's epoch is the superseded coordinator's.
SupersededBeforeTakeover ==
    /\ Has(E, "epoch") /\ ~Unfenced
    /\ ~coordUp /\ zombie /\ zombieEpoch = ModelEpoch(E.epoch)

\* The event names a modelled sink.
ForSink == Has(E, "sub") /\ E.sub \in Sinks
Skip == UNCHANGED vars

\* The model epoch a Trigger event is taken under: the event's own, or the
\* leader's for an unfenced coordinator, which has no zombie beside it.
TriggerEpoch == IF Unfenced THEN leaderEpoch ELSE ModelEpoch(E.epoch)

\* The ids triggered by every coordinator that can still reach a worker: the
\* leader, and a superseded coordinator, whose barriers reach a worker until
\* that worker re-registers with the leader (see the header). A Trigger whose
\* id another of them has triggered diverges, whichever triggers it second.
\* A dead leader's ids are forgotten at its death (Hidden): its barriers died
\* with it, so a takeover may number from an id it triggered and never
\* delivered, which the specification's range admits and engines before the
\* record did (formal/traces/kafka-coordinator-failover).
TriggeredElsewhere == \E p \in triggered : p[2] = E.ckpt /\ p[1] # TriggerEpoch

StepTrigger ==
    /\ Is("Trigger")
    /\ ~TriggeredElsewhere
    /\ IF Unfenced
       THEN Trigger /\ nextCkpt = E.ckpt /\ ~zombie
       ELSE \/ Trigger /\ nextCkpt = E.ckpt /\ leaderEpoch = ModelEpoch(E.epoch)
            \/ ZombieTrigger /\ zombieNext = E.ckpt /\ zombieEpoch = ModelEpoch(E.epoch)

\* One barrier per id in the model; a second source subtask's copy is a stutter.
StepDeliverBarrier ==
    /\ Is("DeliverBarrier")
    /\ IF E.ckpt \in {m.c : m \in Barriers}
       THEN /\ Min({m.c : m \in Barriers}) = E.ckpt
            /\ LET m == CHOOSE n \in Barriers : n.c = E.ckpt
               IN E.fenced <=> (m.epoch < boundEpoch[SrcWorker])
            /\ DeliverBarrier
       ELSE Skip

\* The engine learns whether the capture failed only from the ack, so the
\* prepare step is either action; the ack's `ok` picks the branch.
StepSinkPrepare ==
    /\ Is("SinkPrepare")
    /\ IF ForSink
       THEN /\ CanPrepare(E.sub) /\ Min(barriers[E.sub]) = E.ckpt
            /\ SinkPrepare(E.sub) \/ SinkPrepareFails(E.sub)
       ELSE Skip

StepSubtaskAck ==
    /\ Is("SubtaskAck")
    /\ IF ForSink
       THEN /\ Len(sink[E.sub].ackDue) > 0
            /\ Head(sink[E.sub].ackDue).c = E.ckpt /\ Head(sink[E.sub].ackDue).ok = E.ok
            /\ SinkAck(E.sub)
       ELSE Skip

\* A completion decided before the takeover's line is the leader's, with the
\* supersession hidden after it, or a superseded coordinator's late decision
\* (below) with the supersession taken before it.
\*
\* A superseded coordinator's completion is not the leader's (see ZombieLine).
\* Every ack it decides on was sent before the supersession: the worker emits
\* SubtaskAck as it sends the frame, which is the specification's SinkAck,
\* and the supersession then takes every sink down. Its connection's dispatch
\* thread can still hold that frame behind a slow store write on the same
\* connection and decide the checkpoint only after the takeover's
\* CoordRecovers. A checkpoint all of whose acks were ok at the supersession
\* is one the specification turned stale then (DecidedLate in
\* CoordSuperseded), so the late decision is a stutter and its WriteCompleted
\* the stale put landing (StaleCompletedLands).
\* formal/traces/zombie-decision-after-takeover is that order.
\*
\* A late decision that fails or discards its checkpoint still diverges: its
\* only effect is an abort the workers bound to the leader fence, and the
\* model keeps no record of the superseded coordinator's acks past the
\* takeover to check the outcome against. So does an ack from a worker that
\* has not yet re-registered with the leader, sent after the supersession, the
\* gap the header describes for its barriers.
ZombieDecidesLate ==
    /\ ZombieLine \/ SupersededBeforeTakeover
    /\ E.outcome = "completed" /\ E.ckpt \in staleCompleted
    /\ Skip

StepCoordComplete ==
    /\ Is("CoordComplete")
    /\ \/ /\ ~ZombieLine
          /\ Decidable # {} /\ Min(Decidable) = E.ckpt
          /\ CoordComplete
          /\ (E.outcome = "completed") <=> (completeDue' = E.ckpt)
          /\ (E.outcome = "failed") => (ackedFail[E.ckpt] # {})
          /\ (E.outcome = "discarded") => (ackedFail[E.ckpt] = {} /\ AboveRewind(E.ckpt))
       \/ ZombieDecidesLate

\* The leader's WriteCompleted is the specification's. A superseded
\* coordinator's handler does not know it was superseded: once its put lands
\* it emits the line under its own epoch, and goes on to its broadcast. The
\* specification turned that put stale at the supersession
\* (CompletedOrphaned), so the line is the put landing (StaleCompletedLands),
\* or nothing when it already landed and the takeover read it
\* (LostWriteCompleted, then this line a stutter). Its memory is not the
\* leader's, and the model has none of it to advance. As for WriteConfirmed,
\* the line can be written before the takeover's CoordRecovers, after the
\* takeover's read of the disk (SupersededBeforeTakeover): the supersession is
\* already taken, the takeover did not see the marker, and the put stays
\* stale for a later reader (LostWriteCompleted, LostStaleCompletedAtWalk).
\* formal/traces/zombie-completion-after-takeover and
\* zombie-completion-before-takeover-line are those two orders.
ZombieCompletedBeforeTakeover == SupersededBeforeTakeover /\ E.ckpt \in staleCompleted

StepWriteCompleted ==
    /\ Is("WriteCompleted")
    /\ IF ZombieLine
       THEN \/ E.ckpt \in staleCompleted /\ StaleCompletedLands(E.ckpt)
            \/ E.ckpt \notin staleCompleted /\ E.ckpt \in completedDisk /\ Skip
       ELSE \/ completeDue = E.ckpt /\ WriteCompleted
            \/ ZombieCompletedBeforeTakeover /\ Skip

\* A superseded coordinator's broadcast is a stutter: the specification's
\* supersession cleared every frame and took every sink down, and the
\* takeover binds every worker to the new epoch, so no commit frame the
\* superseded coordinator sends after it is accepted there. In the engine a
\* worker bound to the leader fences it; one that has not yet re-registered
\* accepts it, the gap the header describes for barriers (formal/README.md).
\* Before the takeover's line, it is a stutter only once the supersession is
\* taken; otherwise it is the leader's.
StepBroadcast ==
    /\ Is("Broadcast")
    /\ IF ZombieLine
       THEN Skip
       ELSE \/ /\ toBroadcast = E.ckpt
               /\ E.withheld <=> (phase # "running")
               /\ Broadcast
            \/ SupersededBeforeTakeover /\ Skip

StepDeliverCommit ==
    /\ Is("DeliverCommit")
    /\ IF ForSink
       THEN \E m \in msgs :
               /\ m.kind = "commit" /\ m.s = E.sub /\ m.c = E.ckpt
               /\ DeliverCommit /\ m \notin msgs'
               /\ E.accepted <=> (/\ sink[E.sub].up
                                  /\ ~(m.epoch < boundEpoch[Host[E.sub]])
                                  /\ E.ckpt \in pendingHandles[E.sub]
                                  /\ txn[E.sub][E.ckpt].st = "prepared")
       ELSE Skip

StepDeliverAbort ==
    /\ Is("DeliverAbort")
    /\ IF ForSink
       THEN \E m \in msgs :
               /\ m.kind = "abort" /\ m.s = E.sub /\ m.c = E.ckpt
               /\ DeliverAbort /\ m \notin msgs'
               /\ E.accepted <=> (/\ sink[E.sub].up
                                  /\ ~(m.epoch < boundEpoch[Host[E.sub]])
                                  /\ E.ckpt \in pendingHandles[E.sub]
                                  /\ txn[E.sub][E.ckpt].st = "prepared"
                                  /\ OutsideCommit(E.sub))
       ELSE Skip

StepSinkCommit ==
    /\ Is("SinkCommit")
    /\ IF ForSink THEN sink[E.sub].openTxn = E.ckpt /\ SinkCommit(E.sub) ELSE Skip

StepSinkReceipt ==
    /\ Is("SinkReceipt")
    /\ IF ForSink /\ Kafka THEN sink[E.sub].openTxn = E.ckpt /\ SinkReceipt(E.sub) ELSE Skip

\* The recoverable family's worker also reports CommitConfirmed; the model has
\* no such step for it, so the event is a stutter there. For the Kafka family
\* the line confirms the checkpoint the sink finished, a step with no line of
\* its own (FinishNeeded).
StepSinkConfirm ==
    /\ Is("SinkConfirm")
    /\ IF ForSink /\ Kafka THEN sink[E.sub].confirmDue = E.ckpt /\ SinkConfirm(E.sub) ELSE Skip

\* The coordinator emits WriteConfirmed in the hold that advances its confirmed
\* restore point, after the marker's put, so the leader's line stands for the
\* specification's WriteConfirmed and AdvanceConfirmed back to back. Nothing
\* the engine records reads the marker between the two but a takeover, and a
\* takeover between them leaves no line (LostWriteConfirmed).
\*
\* A superseded coordinator does not know it was superseded: its handler puts
\* the marker, advances its own memory and emits the line, stamped with its
\* own epoch. The specification turned that confirmation stale at the
\* supersession, so the line is its put landing (WriteConfirmed from stale),
\* or nothing at all when the put already landed and the takeover read it
\* (LostWriteConfirmed, then this line a stutter). Its memory is not the
\* leader's, and the model has none of it to advance.
\*
\* The takeover reads the disk before it emits CoordRecovers (plugin loading
\* lies between the two), so a superseded coordinator's put can land after
\* that read with its line written before the takeover's
\* (SupersededBeforeTakeover), with the confirmation stale and its line a
\* stutter: the takeover did not see
\* the marker, and the confirmation stays stale, so a later takeover that
\* reads the marker still takes it (LostWriteConfirmed).
\* formal/traces/zombie-confirmation-before-takeover-line is that order.
ZombieLineBeforeTakeover == SupersededBeforeTakeover /\ confirming[E.ckpt] = "stale"

StepWriteConfirmed ==
    /\ Is("WriteConfirmed")
    /\ IF ZombieLine
       THEN \/ confirming[E.ckpt] = "stale" /\ ConfirmedStep(E.ckpt, TRUE, FALSE)
            \/ confirming[E.ckpt] = "none" /\ E.ckpt \in confirmedDisk /\ Skip
       ELSE \/ coordUp /\ confirming[E.ckpt] = "due" /\ ConfirmedStep(E.ckpt, TRUE, TRUE)
            \/ ZombieLineBeforeTakeover /\ Skip

\* The sinks the loss actually takes: the ones deployed on that worker when it
\* died, not the ones the model's fixed Host assigns to it. A redeploy
\* re-places a subtask, and a sink that has moved off the worker survives its
\* death. Validating against Host instead reported a false divergence: a sink
\* moved worker-1 -> worker-0 on a redeploy, worker-1 then died, and the model
\* killed a sink that was no longer there, so its drain had no step.
\* A worker that re-registers retires its previous session, and the retirement
\* kills only what that session held: subtasks placed on the new session (a
\* deploy that landed while the old session's frames drained) live on, and the
\* event names them in `spared`.
Spared == IF Has(E, "spared") THEN {E.spared[k] : k \in DOMAIN E.spared} ELSE {}

StepWorkerDies ==
    /\ Is("WorkerDies")
    /\ WorkerDiesKilling(E.worker, {s \in Sinks : placed[s] = E.worker /\ s \notin Spared})

\* The coordinator restarts the whole job for a subtask error or a transport
\* failure it could not attribute to a worker loss: the survivors drain, then
\* the redeploy. No sink dies in the model's view; a worker declared lost
\* during that drain folds in as WorkerDies does.
StepRestartOnError == Is("RestartOnError") /\ RestartOnError

StepSubtaskDrained ==
    /\ Is("SubtaskDrained")
    /\ IF ForSink THEN SinkDrains(E.sub) ELSE Skip

\* A takeover: the previous coordinator's death is a hidden step before it.
\* Unfenced, that death was its process's (see Unfenced), never a supersession.
\* The event's completed is the newest COMPLETED marker the takeover read, and
\* the model's reseeded completed point must equal it, so a marker the model
\* does not have on disk (or has and the engine did not read) diverges here
\* rather than at whatever later event reads it.
\* For the commit-confirmed family the event's confirmed is the confirmed
\* restore point the takeover adopts from the job's own directory: the newest
\* CONFIRMED marker its restore read, read once, or 0 when that marker is an
\* earlier run's (below the run base) or when the job has confirmed nothing of
\* its own and restores from the savepoint it was submitted with. The model's
\* reseeded restore point must equal it.
StepCoordRecovers ==
    /\ Is("CoordRecovers")
    /\ CoordRecovers
    /\ IF Unfenced THEN ~zombie ELSE leaderEpoch' = ModelEpoch(E.epoch)
    /\ Has(E, "completed") => memCompleted' = E.completed
    /\ (Tracked /\ Has(E, "confirmed")) => memConfirmed' = E.confirmed

\* The drain is covered and the job holds for in-doubt resolution.
StepRestartProceeds ==
    /\ Is("RestartProceeds")
    /\ E.resolving
    /\ RestartProceeds /\ phase' = "resolving"

\* The redeploy: straight from the drain, or after a held resolution. A fresh
\* leader's next id is a range in the specification (RedeployEffects); the
\* event's next picks the engine's choice out of it. A next below the range
\* (an id whose capture began under the dead leader) diverges here, and so
\* does one above it (an id no leader can have recorded).
StepRedeploy ==
    /\ Is("Redeploy")
    /\ IF phase = "draining" THEN RestartProceeds /\ phase' = "running" ELSE Redeploy
    /\ restorePoint' = E.restore
    /\ nextCkpt' = E.next

\* A claim on the next checkpoint id refused, and the coordinator that made it
\* numbering above the record: the leader, past a superseded coordinator's
\* claims, or the superseded one under its own epoch, past the leader's. The
\* event's next is where it numbers from. An unfenced coordinator has no
\* superseded one (see Unfenced), so its renumber has no step here.
StepRenumber ==
    /\ Is("Renumber")
    /\ ~Unfenced
    /\ \/ Renumber /\ leaderEpoch = ModelEpoch(E.epoch) /\ nextCkpt' = E.next
       \/ ZombieRenumber /\ zombieEpoch = ModelEpoch(E.epoch) /\ zombieNext' = E.next

StepWalkSkips == Is("WalkSkips") /\ walkC = E.ckpt /\ WalkSkips
StepWalkReadsReceipt ==
    /\ Is("WalkReadsReceipt")
    /\ walkC = E.ckpt /\ E.sub \in Sinks /\ WalkReadsReceipt(E.sub)
StepWalkProbes ==
    /\ Is("WalkProbes")
    /\ walkC = E.ckpt /\ E.sub \in Sinks
    /\ WalkProbes(E.sub)
    /\ walkVerdict'[E.sub] = E.verdict
StepWalkRetries == Is("WalkRetries") /\ walkC = E.ckpt /\ WalkRetries
StepWalkExhausted == Is("WalkExhausted") /\ walkC = E.ckpt /\ WalkExhausted
StepWalkCancelled == Is("WalkCancelled") /\ walkC = E.ckpt /\ WalkCancelled
StepWalkDecides ==
    /\ Is("WalkDecides")
    /\ walkC = E.ckpt
    /\ WalkDecides
    /\ E.confirmed <=> (E.ckpt \in confirmedDisk')
StepWalkFinishes == Is("WalkFinishes") /\ WalkFinishes

\* A sink opening on its first deployment is the model's initial state; only
\* an open after a redeploy is a step.
StepSinkOpens ==
    /\ Is("SinkOpens")
    /\ IF ForSink /\ sink[E.sub].opening THEN SinkOpens(E.sub) ELSE Skip

StepPlacement == Is("Placement") /\ Skip

TraceStep ==
    /\ l <= TraceLen
    /\ l' = l + 1
    /\ ev' = IF l + 1 <= TraceLen THEN Trace[l + 1] ELSE ev
    \* Followed here rather than in StepPlacement so every step carries it, and
    \* read unprimed by StepWorkerDies: a death kills where the sinks were.
    /\ placed' = IF Is("Placement") /\ ForSink
                 THEN [placed EXCEPT ![E.sub] = E.worker]
                 ELSE placed
    /\ triggered' = IF Is("Trigger")
                    THEN triggered \cup {<<TriggerEpoch, E.ckpt>>}
                    ELSE triggered
    /\ \/ StepTrigger \/ StepDeliverBarrier \/ StepSinkPrepare \/ StepSubtaskAck
       \/ StepCoordComplete \/ StepWriteCompleted \/ StepBroadcast
       \/ StepDeliverCommit \/ StepDeliverAbort
       \/ StepSinkCommit \/ StepSinkReceipt \/ StepSinkConfirm \/ StepWriteConfirmed
       \/ StepWorkerDies \/ StepRestartOnError \/ StepSubtaskDrained \/ StepCoordRecovers
       \/ StepRestartProceeds \/ StepRedeploy \/ StepRenumber
       \/ StepWalkSkips \/ StepWalkReadsReceipt \/ StepWalkProbes \/ StepWalkRetries
       \/ StepWalkExhausted \/ StepWalkCancelled \/ StepWalkDecides \/ StepWalkFinishes
       \/ StepSinkOpens \/ StepPlacement
    /\ Reached(l + 1)

\* A COMPLETED marker that landed with no line. The coordinator emits
\* WriteCompleted after the durable write, so the line never claims a marker
\* that is not on disk; a kill landing between the two leaves the marker with
\* no line. A put still out when its job redeployed lands with no line at all
\* (WriteStaleCompleted); a superseded coordinator's lands with its own line
\* (StepWriteCompleted), or with none when a kill cuts it off. The only
\* witness admitted is the next event itself: a takeover whose own read of
\* the disk found exactly that checkpoint completed.
LostWriteCompleted ==
    /\ Is("CoordRecovers") /\ Has(E, "completed")
    /\ \/ completeDue # None /\ E.completed = completeDue /\ WriteCompleted
       \/ E.completed \in staleCompleted /\ StaleCompletedLands(E.completed)

\* A stale COMPLETED put that landed with no line, witnessed by the walk: every
\* walk event at an id but WalkSkips comes after the walk read that id's
\* marker, so one at a stale id says the put has landed.
LostStaleCompletedAtWalk ==
    /\ E.event \in {"WalkReadsReceipt", "WalkProbes", "WalkRetries", "WalkExhausted",
                    "WalkCancelled", "WalkDecides"}
    /\ walkC = E.ckpt /\ E.ckpt \in staleCompleted
    /\ StaleCompletedLands(E.ckpt)

\* A CONFIRMED marker that landed with no WriteConfirmed line: a kill between
\* the put and the hold that emits the line, a superseded coordinator's put
\* landing on its own, or a put whose hold found the job redeployed since and
\* left memory, and the trace, alone (see WriteConfirmed in the
\* specification). As for LostWriteCompleted, the only witness admitted is
\* the next event: a takeover whose own read of the disk found exactly that
\* checkpoint confirmed.
LostWriteConfirmed ==
    /\ Is("CoordRecovers") /\ Has(E, "confirmed")
    /\ E.confirmed \in Ckpts
    /\ \/ confirming[E.confirmed] = "due" /\ coordUp
       \/ confirming[E.confirmed] = "stale"
    /\ ConfirmedStep(E.confirmed, TRUE, FALSE)

\* A walk's CONFIRMED marker whose WalkDecides line a kill cut off. The walk
\* puts CONFIRMED-<id> before it emits the line, so a kill between the two
\* leaves the marker on disk with no line. Admitted on the terms of
\* LostWriteCompleted: the next event is a takeover whose own read of the disk
\* found exactly the checkpoint being walked confirmed, and every handle the
\* walk had to prove was proven committed. It is WalkDecides' success branch.
LostWalkDecides ==
    /\ Is("CoordRecovers") /\ Has(E, "confirmed")
    /\ Walking /\ E.confirmed = walkC
    /\ WalkHandles(walkC) # {}
    /\ \A s \in WalkHandles(walkC) : walkVerdict[s] = "committed"
    /\ WalkDecides

\* The Kafka sink's finish (SinkFinish) has no line. The sink emits SinkReceipt
\* inside its commit callback and the worker SinkConfirm as it sends
\* CommitConfirmed, after the callback has returned, and nothing between marks
\* the handle erased and the open transaction resolved. The finish is taken
\* as a hidden step only where the next event needs it: that sink's
\* SinkConfirm, or its next SinkPrepare, which the task thread can reach
\* before the confirmation goes out, since the finish resolved the open
\* transaction its barrier waits on. A death after the receipt needs no
\* finish: the death takes the sink's handles with or without one, and the
\* receipt on disk answers for the commit either way.
FinishNeeded == E.event \in {"SinkPrepare", "SinkConfirm"} /\ ForSink /\ Kafka

\* What the engine cannot observe and so never emits: the model may take
\* these between events, within the budgets the trace implies.
\* A dying leader takes its ids out of `triggered`; a superseded one keeps them,
\* under the epoch it goes on triggering with.
Hidden ==
    /\ \/ CoordDies /\ triggered' = {p \in triggered : p[1] # leaderEpoch}
       \/ /\ \/ CoordSuperseded \/ ZombieStops
             \/ TxnExpires \/ BrokerGoesDown \/ BrokerComesBack
             \/ LostWriteCompleted \/ LostStaleCompletedAtWalk \/ LostWriteConfirmed
             \/ LostWalkDecides
             \/ FinishNeeded /\ SinkFinish(E.sub)
          /\ UNCHANGED triggered
    /\ UNCHANGED <<l, ev, placed>>

\* The trace consumed: the run stutters here rather than deadlocking.
TraceEnd == l > TraceLen /\ UNCHANGED <<vars, l, ev, placed, triggered>>

TraceNext == TraceStep \/ Hidden \/ TraceEnd

TraceInit == Init /\ l = 1 /\ ev = Trace[1] /\ placed = Host /\ triggered = {}

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, l, ev, placed, triggered>>

\* POSTCONDITION: TRUE when some path consumed the whole trace.
TraceAccepted ==
    LET reached == TLCGet(1)
    IN IF reached > TraceLen
       THEN TRUE
       ELSE Print(<<"divergence", reached, Trace[reached]>>, FALSE)

================================================================================
