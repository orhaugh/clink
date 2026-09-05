# 012: The exactly-once protocol is a machine-checked specification, and the engine's traces are validated against it

Status: accepted; landing in increments. Increments 1 and 2 (the
specification model-checked in CI, and the mutants) ship with this record,
together with the three engine defects the first model check found; the
remaining increments are listed at the end with their state.

## Context

clink's exactly-once guarantee is not one mechanism. It is the agreement of
four, each documented on its own page and implemented in its own place:
barrier-aligned snapshots that ack only after the bytes are durable
([checkpointing](../internals/checkpointing.md)); two-phase-commit sinks that
prepare at the barrier and commit on `CommitCheckpoint`
([sink committer framework](../internals/sink-committer-framework.md)); the
coordinator's completion protocol, which writes `COMPLETED-N` before any
commit is broadcast, withholds the broadcast while a job drains for a
restart, and tracks `CONFIRMED-N` for sinks whose commit cannot be
re-executed; and recovery, which selects a restore point, resolves in-doubt
transactions with receipts, materialised receipts and unresolved markers,
refuses to fence an orphan blind, numbers new checkpoints above every
durable id, and fences a superseded coordinator's frames
([distributed runtime](../internals/distributed-runtime.md),
[fault tolerance](../internals/fault-tolerance-and-rescale.md)).

The evidence that this agreement holds is substantial and entirely sampled.
Unit tests pin single mechanisms. The multi-process integration gates pin
named interleavings: a death at each of seven fault points, a fenced partial
commit, an ack-window kill composed with a coordinator loss. The
qualification campaigns run the whole protocol for hours under injected
faults and judge the output against an independent oracle
([QUAL-01](../qualification/qual-01-kafka-exactly-once.md)). Every one of
these buys one fact about one schedule. The campaigns, in particular, buy
one fact per paid rig cycle.

The QUAL-01 campaign record is the case for doing better. Between its first
shakedown and its green run, the rig found eight protocol defects that the
test suites had passed over, and every one was an interleaving nobody had
enumerated: a checkpoint completing while the job drained, committed on
some sinks and not others; a commit whose broker acknowledgement arrived
but whose confirmation never left the dying worker, answered "fenced" by a
successor and replayed as duplicates; a resolution walk that stopped at the
first refusal and left the commits after it unproven; a recovered
coordinator reusing a checkpoint id that still had durable files, so a later
restore assembled one checkpoint from two vintages; a redeployed sink fencing
an orphaned transaction before anything had asked the broker what became of
it. Each fix is now pinned by a deterministic gate, and each gate encodes
exactly one schedule. The class of defect is "an interleaving of protocol
steps and faults that violates the guarantee", and the tool built for that
class is a model checker, which enumerates the interleavings rather than
sampling them.

There is also no artefact that states the protocol whole. The rules live
across a coordinator of several thousand lines, the sink framework, the
Kafka sink, the resolution walk, three internals pages and a campaign
handover. A reviewer cannot check a proposed change against the design
without reading all of it, and the defects above show that the design's
assumptions cross those boundaries: the stale-handle defect was a
sink-side restore rule violating a walk-side assumption. A written,
checkable model is the artefact a change can be reviewed against.

## Decision

The exactly-once protocol gets a formal specification in TLA+, under
`formal/`, and the specification becomes a gate in three ways: the model
checker proves the stated invariants over bounded configurations on every
push; each protocol defect the rigs found is reproduced as a mutant of the
specification that the checker must refute, so the model is known to be
sharp enough to see what the rigs saw; and the engine emits a protocol
trace whose every recorded behaviour must be one the specification allows.

### The specification

`formal/ExactlyOnce.tla` models the protocol at the level of checkpoint
ids, subtasks, transactions, markers and receipts. There are no records,
bytes, watermarks or channels. What the model keeps is the one quantity the
guarantee is about: each checkpoint interval is a logical position in the
input, a sink's transaction for a checkpoint covers that position, a
restore rewinds the source to the restore point's position, and the
published output of a sink is the multiset of positions its committed
transactions cover, less what replay suppression swallowed. The invariants
are then plain statements:

- **No duplicate.** No sink publishes any position more than once.
- **No loss.** Once the source can no longer re-emit a position (it lies at
  or below the restore frontier), every sink has committed it, or holds it
  prepared with a handle the restore will finalise.
- **Restore soundness.** A restore point's participant snapshots all record
  the same cut, and for a sink whose commit cannot be re-executed the
  restore point is one whose commits provably executed.
- **Fencing.** A superseded coordinator cannot complete, commit or record
  anything.

The atomic steps are chosen so that every named fault point in
`include/clink/fault/fault_injection.hpp` is a distinct state between two
steps: prepare, ack, marker write, broadcast, commit, receipt and
confirmation are separate actions, and a process may die between any two
of them. That is what makes the model checker's enumeration cover the
windows the campaigns aim at, and every window between them that no
campaign has aimed at yet.

Faults are first-class actions with a budget: worker death, coordinator
death and a superseded coordinator that keeps acting, broker transaction
expiry, producer fencing, an unreachable broker during resolution, a
snapshot that fails, and a cancelled resolution walk. The broker is
modelled as the transaction coordinator the protocol actually depends on:
a prepared transaction that expires or is fenced aborts; a commit's
outcome stays describable until a successor transaction begins on the same
identity or the identity is fenced.

Two connector families are modelled. The Kafka family cannot re-execute a
commit after the owning process dies, so it runs the commit-confirmed
restore protocol, receipts and in-doubt resolution; the staged-artifact and
XA families (file, Parquet, S3, Postgres) re-commit their persisted handles
idempotently at open. The same specification covers both through one
constant.

### Model checking in CI

`scripts/formal-check.sh` fetches the TLA+ tools at the version and SHA-256
pinned in `formal/tools.env`, verifies them, and runs TLC over every model
configuration under `formal/models/`. A `formal` job in `ci.yml` runs it on
every push and pull request, with a bounded budget: small configurations
(two sinks on two workers, a handful of checkpoints, one or two deaths of
each kind) that finish in minutes but still enumerate every interleaving of
the protocol steps and faults within those bounds. The job needs a Java
runtime and nothing from the C++ toolchain, so it runs on a bare runner
beside the manifest gates. Larger configurations are for a scheduled run,
not the push gate.

### Mutants: the model must see what the rigs saw

A model that proves its own invariants is not evidence of anything until
it is shown to reject a wrong protocol. Every defect the qualification
campaigns found and fixed becomes a named mutant: a constant in the
specification that switches one rule back to its pre-fix form. The check
script runs TLC against each mutant and fails unless TLC produces a
counterexample violating the invariant that defect violated in production,
or unless the mutant is recorded as one a later rule now guards as well, in
which case the check fails the day TLC refutes it.
The mutant table (in `formal/README.md` and on the published page) names
the campaign run that found each defect, the rule the mutant disables and
the invariant it breaks, and each mutant's counterexample is the defect's
mechanism written out as a sequence of protocol steps.

This is the same rule the integration gates already live by: a gate that
has not been shown to fail against the bug it guards is decorative. It also
sets the model's abstraction level empirically. A defect the model cannot
express is a gap in the model, recorded as such rather than left implicit.

### The protocol trace

The engine gains a protocol trace: a stream of structured events at the
protocol's decision points, off by default, enabled by an environment
variable naming a directory, and costing one relaxed atomic load per site
when disabled, the way fault points do. The event vocabulary is derived
from the fault-point names and the specification's actions, and it is a
versioned contract: listed in the
[protocol compatibility inventory](../internals/protocol-compatibility.md),
frozen in a manifest, and checked by a script so that an event emitted in
code that the specification does not know, or a specification action that
nothing in the engine ever emits, fails the build.

### Trace validation

A trace is validated by model-checking it: a trace module constrains the
specification's next-state relation to the recorded events in order, with
unobserved state left nondeterministic, and TLC either reaches the end of
the trace (the behaviour is one the specification allows) or reports the
first event no allowed step could produce. Cross-process ordering uses
per-process timestamps with a bounded reordering window, since the events
that are causally related are separated by at least a network round trip
and the events that are not commute in the model.

The multi-process integration tests that drive fault-point kills write
protocol traces as a matter of course, and the CI integration step validates
every trace they leave. A set of representative traces is committed under
`formal/traces/` and replayed by the `formal` job on every push, so a
protocol change that invalidates a recorded behaviour is caught even where
the integration test that produced it is Docker-gated. Traces from
qualification rigs are validated by the same script, and a campaign page
can then state that every behaviour the engine exhibited during the run
was one the specification allows.

### What the specification does not model

Records and their values, watermarks and the exactness of replay
suppression's horizon cut, source partition ownership, unaligned-checkpoint
in-flight capture, rescale, the HA lock primitive and the metadata
compare-and-set, network frame encoding, and time. Each is a documented
boundary with its own evidence elsewhere; the specification's job is the
protocol between those parts, and the published page states the boundary
in the same honesty categories the qualification pages use.

## Consequences

Protocol changes get a review artefact. A change to the coordinator's
completion rules, the sink commit choreography or the resolution walk is
also a change to `ExactlyOnce.tla`, and a pull request that changes one
without the other fails either the model check (the specification no longer
proves its invariants) or the trace validation (the engine no longer
behaves as the specification says). The specification is therefore part of
"done" for protocol work, alongside the docs.

Defects of the interleaving class are found by enumeration before a rig
is paid for. The campaigns keep their role, since they test the code and
the model tests the design, and a green model check says nothing about a
bug in a `std::erase_if`. The published claim must say this plainly: TLC
proves the model; trace validation shows the implementation's recorded
behaviours are behaviours of the model; neither proves the code.

Trade-offs accepted. CI gains a Java runtime and a pinned jar, fetched and
checksum-verified, on one bare-runner job. The specification is a second
statement of the protocol that must be kept in agreement with the first,
and the trace gate is what makes that agreement checked rather than hoped
for. The abstraction boundary is deliberate and wide: a defect below it
(the source seeking a stale partition offset after a rebalance, a
watermark-horizon misclassification) is invisible to the model, and the
mutant table is the honest record of what the model has been shown to see.
Trace ordering across hosts is approximate; the reordering window is a
stated assumption, not a proof of causality, and a Lamport clock on the
control frames is the known upgrade if it proves too loose.

## Increments

1. **Specification and model check.** `formal/ExactlyOnce.tla`, the model
   configurations, `formal/tools.env`, `scripts/formal-check.sh`, the
   `formal` CI job, this record, `formal/README.md`. Shipped with this
   record. Writing the model against the engine as shipped produced three
   counterexamples that were engine defects (the refusal wall, the rewind
   floor, the restore point ahead of its marker), each fixed in the same
   change and pinned by a test; the
   [published page](../internals/exactly-once-specification.md) records
   them.
2. **Mutants.** One constant per campaign-found defect and per model
   finding, a check that TLC refutes each, and the mutant table on the
   published page. Shipped with this record.
3. **Protocol trace.** The emitter, the environment switch, the event
   manifest and its check script, unit tests, and the inventory row.
4. **Trace validation.** The trace module, the validator script, trace
   capture in the integration harness, the CI validation step, and the
   committed regression traces.
5. **Publication.** The internals page, the capability catalogue row, the
   README, the changelog; then a validated rig trace alongside a campaign
   page.
