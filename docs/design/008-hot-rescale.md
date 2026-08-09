# 008: Rescale one operator at a barrier, without stopping the job

**Status:** accepted 2026-08-09; implementation in progress. The stop-the-world
replan remains the fallback for every abort path.

## Context

`clink rescale-op` changes one operator's parallelism by draining the whole
job, replanning every task from the retained graph, and redeploying from the
last completed checkpoint. That is correct - the exactly-once tests carry over
unchanged - but it prices every scaling decision as a full restart: every
source rewinds to the checkpoint, every operator restores, every sink's
transaction cycle resets. For a long-window or large-state job, an autoscaler
acting on a load signal turns each decision into a latency event.

The machinery for something better already exists but is unreachable. A
`RescaleCoordinator` state machine (`Preparing -> Draining -> CuttingOver ->
Complete`), a `BeginRescale` control message, a `Drain` marker on the data
wire, and a cutover planner (`plan_operator_cutover`) were built when one task
hosted one operator and a task's `role` was the operator id. The chain planner
ended that world: every task is now an operator chain with the shared role
`__clink_subtask` and a job-global subtask index, so nothing on the wire can
address "operator X's subtasks", and the dormant path matches nothing.

Two facts fix the shape of any correct design. State handoff is
checkpoint-based, so the new subtasks can only start from a completed
snapshot; and barrier alignment is the engine's only global ordering
primitive, so a consistent boundary between "the old subtasks' records" and
"the new subtasks' records" has to be a barrier.

## Decision

Rescale the operator's chain in place, using one checkpoint barrier as the
cutover boundary. Six rules define the contract:

1. **One barrier is the boundary.** The coordinator assigns the cutover
   checkpoint id C explicitly and arms every affected task before triggering
   it. Records up to C belong to the old subtasks; records after C belong to
   the new ones. Nothing about the boundary is inferred from timing.
2. **Old subtasks end at C exactly.** An old subtask processes through
   barrier C, snapshots, acks, forwards the barrier, then emits nothing more.
   A source chain arms the stop at barrier injection; a mid-dag chain arms it
   at barrier receipt. A chain ending in a two-phase-commit sink stays alive
   until `CommitCheckpoint(C)` lands, then exits.
3. **Upstream holds the rescaled edge across the boundary.** After forwarding
   barrier C into an output group that feeds the rescaled operator, that
   group blocks further emission - ordinary backpressure - until the
   coordinator delivers the new peer set. On release it re-emits its current
   watermark into the fresh channels so downstream time resumes immediately.
   Other output groups, and the rest of the job, keep flowing.
4. **Downstream swaps its input set at the boundary.** Each old input channel
   ends with barrier C then Close, and closed inputs already satisfy
   alignment. The armed downstream binds listeners for the new upstream
   subtasks before they deploy and admits them into barrier alignment from
   C+1 onward.
5. **The checkpoint clock pauses for the window.** No trigger is issued while
   any operator sits between Preparing and Complete or Aborted, so C is the
   last checkpoint of the old layout and C+1 the first of the new. One
   rescale runs at a time.
6. **Identity is append-only.** New subtasks take fresh job-global indices
   appended past the current allocation, in the same state generation: no
   other task's index, key-group range, or state directory changes, and
   nothing else redeploys. The operator's index map (index within the
   operator to job-global index) becomes explicit, recorded state - carried
   in the deploy directive, the completed-checkpoint marker, and the HA
   snapshot - replacing base-plus-offset arithmetic, which append-only
   allocation breaks by design.

Addressing an operator requires operator identity on the wire: the deploy
directive carries which operator (chain) a task hosts and its index within
that operator, and the coordinator translates subtask acks back to operators
through the same record. The deployable unit is the chain, so rescaling an
operator rescales the chain that hosts it.

Every stage of the choreography can abort - a worker is lost mid-window, a
plan fails, an arm ack never arrives. Every abort lands in the same place:
the proven stop-the-world replan from the latest completed checkpoint, still
at the requested parallelism. Hot when possible, correct always.

## Consequences

- The cost of a rescale drops from a whole-job restart to a stall on the
  rescaled operator's edges, bounded by drain plus deploy plus restore.
  Sources do not rewind, unaffected operators do not restart, sink
  transaction cycles do not reset. Those three properties are asserted by
  tests, not just claimed.
- The data plane gives up static wiring at two seams: an output group's peer
  set must be swappable while its stage runs (held at a barrier), and the
  fan-in stage must admit new input channels mid-run. Both changes are scoped
  to those seams - the emitter and the union input stage - rather than to
  every runner.
- The checkpoint interval bounds how quickly a rescale can begin, since the
  cutover waits for the next barrier; and the trigger pause (rule 5) trades
  checkpoint freshness during the window for a layout that never mixes
  epochs.
- Integer-factor scaling only, one operator at a time, inherited from the
  parent-mapping arithmetic and rule 5. Both are policy choices that can be
  revisited separately; neither weakens the contract.
- The explicit per-operator index map becomes part of the job's durable
  metadata. A later whole-job restart (which bumps the generation and
  replans contiguously, per [007](007-state-generations.md)) translates
  restore addressing through the map recorded with the checkpoint it
  restores from.

See [Fault tolerance and rescale](../internals/fault-tolerance-and-rescale.md)
for the shipped mechanism as it lands.
