---
title: Three checks on exactly-once, and the bug all three missed
description: "How clink checks its exactly-once guarantee with fault-injection campaigns, a model-checked TLA+ specification and validation of real protocol traces, what each check found, and the duplicate that got past all three."
---

# Three checks on exactly-once, and the bug all three missed

*September 2026*

Until v0.10.0, you could kill `clink run` in the middle of a Kafka
pipeline, start it again with the same command, and get duplicates. The
integration test that shows it reads a Kafka topic, counts events in
ten-second windows and writes each window to a transactional Kafka sink.
Kill the process with SIGKILL, feed it the rest of the topic, run it again,
and read the output topic as a `read_committed` consumer. With resume
switched off, 16 of the 40 windows are committed twice.

That is an awkward bug for an engine whose main claim is exactly-once
delivery, and one that has three separate ways of checking that claim.
Each of the three has found real defects, and none of them found this one.
This post is about what each check covers, what it caught, and why the
gap was where it was.

## What is being claimed

clink is a stream processing engine you run from a SQL file. A job reads
from a source such as Kafka, keeps event-time windows and keyed state, and
writes to sinks. At a regular interval it takes a checkpoint: the
source offsets, the operator state and each sink's open transaction are
captured at the same cut of the stream. A sink's transaction commits only
after the checkpoint it belongs to is durable. After a crash, the job
restores the last checkpoint whose commits are known, resolves any commit
whose outcome it cannot see (a kill between the broker acknowledging a
commit and the engine recording it, for example), and replays from there.

The claim is that every input event is reflected in the committed output
once, whatever gets killed and whenever. The hard part is "whenever". The
failures that matter sit in windows a few milliseconds wide inside the
commit protocol, and a test that kills a process at a random moment almost
never lands in them.

## Check one: kill it where it hurts, for hours

The first check is the qualification campaigns. The Kafka campaign,
[QUAL-01](../qualification/qual-01-kafka-exactly-once.md), ran a windowed
aggregation for two hours on eight cloud hosts. It killed workers and the
coordinator, restarted brokers, took the brokers away entirely, and
partitioned the network. Thirteen of the kills fired at named points
inside the two-phase commit, armed in the engine's own sink and
coordinator code, so they landed at an exact protocol step rather than at
a wall-clock moment. An oracle on a separate host recomputed every
window's expected result from the generator's seed and judged the
committed output against it. The result was 755 of 755 windows correct,
with nothing missing and nothing duplicated.

The runs before that one were not clean, which is the point of running
them. One found that a recovered job reused checkpoint ids. An
incarnation of a job that lived only a few seconds, in the middle of a
restart storm, died holding snapshot files for checkpoints that never
completed. Its successor numbered new checkpoints above the last completion
marker, so it reused those ids, and a later restore assembled one checkpoint
from two incarnations' files. Ten
windows were published twice. The fix was to number new checkpoints above
every id with any durable record on disk, markers and snapshot files alike.

Another night's run reported an extra event per key in some windows, and
the engine was not at fault. The test's feeder used a plain Kafka producer.
When the broker went down mid-batch and came back, the producer retried and
wrote that batch into the input topic twice. The engine counted what it was
given. Every wrong window matched a twice-written event id in the input.
The feeder is now idempotent, and the lesson stuck: check the input before
blaming the engine.

Campaigns are slow and they cost money, so each run buys roughly one fact.
They also only find what the chaos happens to reach in the time available.

## Check two: a model that tries every interleaving

The second check is a TLA+ specification of the protocol,
[model-checked by TLC](../internals/exactly-once-specification.md) on
every push. It describes the coordinator's completion and confirmation
rules, the sinks' two-phase commit, the in-doubt resolution walk and the
restore-point choice, along with the faults: process deaths, a coordinator
that is superseded but keeps running, brokers that lose or expire
transactions. Within its bounds, TLC tries every interleaving and checks
that nothing is lost, nothing is published twice, and a confirmed
checkpoint really was committed.

The model was written from the engine as it shipped, and the first runs
found three real defects, each checked against the code before it was
called one. In-doubt resolution stopped at the first checkpoint it could
not prove and returned, so a commit that had executed without its receipt,
in a completed checkpoint above that one, was fenced blind and replayed.
The rewind after a failed checkpoint could be overtaken by a later
checkpoint that finished collecting acknowledgements during the rewind,
and the job then restored past the interval the rewind existed to re-emit.
And the in-memory restore point advanced before the completion marker was
durable, so a restart in that window could redeploy from a checkpoint the
next coordinator would never see. None of the three needs a fault the
campaigns do not inject. The two-hour run just did not happen to hit them.

Every defect the campaigns or the model have found is now a named mutant:
a configuration of the specification with the fixing rule switched off.
CI runs TLC on each mutant and requires it to produce a counterexample.
Thirteen of the fifteen do. The other two are recorded as guarded, because
a later rule catches the same failure, and the check fails in either
direction if that changes. A check that has never been shown to fail
against the bug it guards is decoration.

The model has its own blind spot. It is a description of the protocol, and
the engine could drift away from it without anyone noticing.

## Check three: hold the engine to the model

The third check closes that gap. The engine emits a protocol trace at each
decision point the specification names: a checkpoint triggered, a barrier
delivered, a transaction prepared, a marker written, a commit confirmed, a
coordinator taking over. A TLA+ module constrains the specification to
follow a recorded trace event by event, and TLC reports the first event no
allowed step can produce. CI records traces from the test runs, including
the multi-process fault tests, and validates them on every commit.

Writing that module against real traces found three places where the
model was narrower than the engine. One was the first checkpoint after a
redeploy, which is triggered as soon as the job is deployed, before a sink
has finished opening, so the barrier waits in the sink's input queue.
None of the three was an engine defect, and each was fixed in the
specification. Widening the model mattered on its own account. With the
early barrier admitted, the model refuted a mutant it had previously
recorded as guarded, the shape of a duplicate one campaign run had found.
The narrower model had been unable to see it.

## The bug in the gap

All three checks start from the same place: a job is running, something
dies, and a coordinator takes over. The campaigns kill processes inside a
running cluster. The model's recovery step is a takeover. The traces
record takeovers.

An embedded `clink run` does not take over anything. Everything runs in one
process, so a kill takes the coordinator, the worker and the in-flight
checkpoint down together. Running the command again starts a new process
with a new coordinator, whose first job is job 1 again. It had no reason to
look at the checkpoints the last process left behind. It started from
empty state, read the topic from the beginning, and committed everything
the first run had already committed. The protocol was never broken. The
new job simply never entered it.

The fix, in [v0.10.0](https://github.com/orhaugh/clink/releases/tag/v0.10.0),
treats a rerun as the takeover it is. The job restores from its own last
checkpoint, chosen and resolved the same way a cluster's recovery chooses
it, and numbers new checkpoints above every id on disk. Because job ids
restart at 1 in every process, three records beside the checkpoints say
what they belong to:

- a fingerprint of the job graph, so a different script pointed at the
  same directory is refused instead of being handed another job's state;
- a clean-finish marker, so a bounded job that reached the end of its input
  runs again on the next invocation rather than resuming at its end and
  publishing nothing;
- the checkpoint id each fresh run started above, so a run killed before
  its own first checkpoint is never handed the state of the run before it.

The third record exists because an independent review of the change found
that case. The first version had only the other two, and a fresh start
killed early would have resumed its predecessor. That is the same shape of
bug, a new run silently reading an old run's checkpoints, one level down.

Making reruns resume also exposed a second, older defect. The plain file
sink truncated its file whenever it opened, so after any restore, a
cluster failover included, the file held only what was written since the
restore point. It now appends on a restore.

The recorded kill-and-rerun run is now part of the trace corpus, and the
model accepts it. Accepting it needed one addition. An embedded coordinator
has no leader election, so its epoch is always 0, where the model assumed
every takeover raises it. The trace module now reads an epoch-0 takeover as
unfenced: it pins no epoch, and it admits no superseded coordinator, since
nothing could have superseded it. Rewriting that trace so the rerun
restores nothing, which is the old behaviour, makes validation fail at the
redeploy.

## What the checks do not cover

The model is bounded: two sinks, three checkpoints, one fault of each kind.
A defect that needs three sinks, four checkpoints or two coordinator deaths
in one run is outside what TLC has enumerated. The traces are evidence
about the runs recorded, not a proof about the rest. The specification page lists the rest of what it leaves out.
The campaigns cover the connectors they ran against. And the plain Parquet
sink still cannot append, so after any restore it keeps only the rows
written since. Its exactly-once variant is the one to use where earlier
output must survive.

clink is pre-1.0 and written by one maintainer with a lot of AI assistance.
That is part of why the checks exist. They do not depend on anyone having
been careful.

## Try it

```bash
pip install pyclink
```

With `--checkpoint-dir` set, kill a running `clink run pipeline.sql` and
run the same command again: it resumes where the last checkpoint left off.
`--fresh` starts over. The
[Kafka to ClickHouse tutorial](../tutorials/kafka-to-clickhouse.md) walks
through a full pipeline, including killing a worker mid-stream and checking
the recovery independently, and the
[qualification pages](../qualification/README.md) and the
[specification](../internals/exactly-once-specification.md) have the
details behind every number here.
