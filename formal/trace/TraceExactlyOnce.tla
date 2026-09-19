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
   event is read into a variable once per step for the same reason. *)
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

VARIABLES l,   \* index of the next event to match
          ev   \* Trace[l], read once per step

\* Register 1 holds the highest event index any path has reached; the
\* validator runs TLC with one worker, so the register is a single number.
ASSUME TLCSet(1, 1)
Reached(i) == TLCSet(1, IF i > TLCGet(1) THEN i ELSE TLCGet(1))

E == ev
Is(kind) == E.event = kind
\* The event names a modelled sink.
ForSink == Has(E, "sub") /\ E.sub \in Sinks
Skip == UNCHANGED vars

StepTrigger ==
    /\ Is("Trigger")
    /\ \/ Trigger /\ nextCkpt = E.ckpt /\ leaderEpoch = ModelEpoch(E.epoch)
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
       THEN /\ sink[E.sub].ackDue = E.ckpt /\ sink[E.sub].ackOk = E.ok
            /\ SinkAck(E.sub)
       ELSE Skip

StepCoordComplete ==
    /\ Is("CoordComplete")
    /\ Decidable # {} /\ Min(Decidable) = E.ckpt
    /\ CoordComplete
    /\ (E.outcome = "completed") <=> (completeDue' = E.ckpt)
    /\ (E.outcome = "failed") => (ackedFail[E.ckpt] # {})
    /\ (E.outcome = "discarded") => (ackedFail[E.ckpt] = {} /\ AboveRewind(E.ckpt))

StepWriteCompleted == Is("WriteCompleted") /\ completeDue = E.ckpt /\ WriteCompleted

StepBroadcast ==
    /\ Is("Broadcast")
    /\ toBroadcast = E.ckpt
    /\ E.withheld <=> (phase # "running")
    /\ Broadcast

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
                                  /\ sink[E.sub].stage = "idle")
       ELSE Skip

StepSinkCommit ==
    /\ Is("SinkCommit")
    /\ IF ForSink THEN sink[E.sub].openTxn = E.ckpt /\ SinkCommit(E.sub) ELSE Skip

StepSinkReceipt ==
    /\ Is("SinkReceipt")
    /\ IF ForSink /\ Kafka THEN sink[E.sub].openTxn = E.ckpt /\ SinkReceipt(E.sub) ELSE Skip

\* The recoverable family's worker also reports CommitConfirmed; the model has
\* no such step for it, so the event is a stutter there.
StepSinkConfirm ==
    /\ Is("SinkConfirm")
    /\ IF ForSink /\ Kafka THEN sink[E.sub].openTxn = E.ckpt /\ SinkConfirm(E.sub) ELSE Skip

StepWriteConfirmed ==
    /\ Is("WriteConfirmed")
    /\ E.ckpt \in broadcastIds
    /\ WriteConfirmed
    /\ E.ckpt \notin broadcastIds'

StepWorkerDies == Is("WorkerDies") /\ WorkerDies(E.worker)

StepSubtaskDrained ==
    /\ Is("SubtaskDrained")
    /\ IF ForSink THEN SinkDrains(E.sub) ELSE Skip

\* A takeover: the previous coordinator's death is a hidden step before it.
StepCoordRecovers ==
    /\ Is("CoordRecovers")
    /\ CoordRecovers
    /\ leaderEpoch' = ModelEpoch(E.epoch)

\* The drain is covered and the job holds for in-doubt resolution.
StepRestartProceeds ==
    /\ Is("RestartProceeds")
    /\ E.resolving
    /\ RestartProceeds /\ phase' = "resolving"

\* The redeploy: straight from the drain, or after a held resolution.
StepRedeploy ==
    /\ Is("Redeploy")
    /\ IF phase = "draining" THEN RestartProceeds /\ phase' = "running" ELSE Redeploy
    /\ restorePoint' = E.restore
    /\ nextCkpt' = E.next

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
    /\ \/ StepTrigger \/ StepDeliverBarrier \/ StepSinkPrepare \/ StepSubtaskAck
       \/ StepCoordComplete \/ StepWriteCompleted \/ StepBroadcast
       \/ StepDeliverCommit \/ StepDeliverAbort
       \/ StepSinkCommit \/ StepSinkReceipt \/ StepSinkConfirm \/ StepWriteConfirmed
       \/ StepWorkerDies \/ StepSubtaskDrained \/ StepCoordRecovers
       \/ StepRestartProceeds \/ StepRedeploy
       \/ StepWalkSkips \/ StepWalkReadsReceipt \/ StepWalkProbes \/ StepWalkRetries
       \/ StepWalkExhausted \/ StepWalkCancelled \/ StepWalkDecides \/ StepWalkFinishes
       \/ StepSinkOpens \/ StepPlacement
    /\ Reached(l + 1)

\* What the engine cannot observe and so never emits: the model may take
\* these between events, within the budgets the trace implies.
Hidden ==
    /\ \/ CoordDies \/ CoordSuperseded \/ ZombieStops
       \/ TxnExpires \/ BrokerGoesDown \/ BrokerComesBack
    /\ UNCHANGED <<l, ev>>

\* The trace consumed: the run stutters here rather than deadlocking.
TraceEnd == l > TraceLen /\ UNCHANGED <<vars, l, ev>>

TraceNext == TraceStep \/ Hidden \/ TraceEnd

TraceInit == Init /\ l = 1 /\ ev = Trace[1]

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, l, ev>>

\* POSTCONDITION: TRUE when some path consumed the whole trace.
TraceAccepted ==
    LET reached == TLCGet(1)
    IN IF reached > TraceLen
       THEN TRUE
       ELSE Print(<<"divergence", reached, Trace[reached]>>, FALSE)

================================================================================
