# QUAL-01: Kafka exactly-once under a fault campaign

**Status: completed. The campaign found a genuine exactly-once defect in
clink, which is fixed and pinned by a regression test.**

This is the flagship correctness campaign: a keyed, event-time windowed
aggregation reading from Kafka and writing to Kafka through a transactional
two-phase-commit sink, on a real cluster, while workers are killed and
partitioned underneath it. An independent oracle judges every closed window.

The headline result is not a clean bill of health. The campaign did what it
was built to do: it found a way for clink to silently produce wrong output
after a worker failure, on a path that every existing unit and integration
test passed.

## What was run

| | |
|---|---|
| Run id | `qual01-20260816` |
| clink revision | `440043f` (image built by `git archive` of that commit, fault injection compiled in) |
| Infrastructure | Hetzner Cloud, 8 hosts: 1 coordinator, 3 workers, 3 Redpanda brokers, 1 ops host |
| Pipeline | Kafka source (4 partitions) → event-time tumbling 10s window → `GROUP BY` key → transactional Kafka sink |
| Delivery guarantee | End-to-end exactly-once, as classified by the coordinator |
| Parallelism | 4 |
| Input rate | 2,000 events/sec, 50,000 distinct keys |
| Checkpoint interval | 10s configured (and not honoured - see QUAL-01-D2) |
| Duration | 1 hour |
| Faults actually applied | **One**: a single worker SIGKILL, at 21:45:02, against checkpoint 341 |

That last row is not what was intended and is stated as measured. The chaos
controller was configured for a fault every two to four minutes for the whole
hour. It applied one, then died - see the harness finding below. The remaining
fifty-six minutes were an undisturbed soak. Everything on this page is scoped
to that: **one** worker failure, not a sustained fault campaign.

The generator, the oracle and the verifier all run on the ops host, outside
the clink failure domain, so a clink crash cannot destroy the evidence needed
to judge it. Every input event's fields are a pure function of
(seed, partition, sequence), so the expected output of every window is
recomputable by anyone holding the seed - the oracle is that function, not a
stored copy of history, and never clink.

## Verification before soak

Nothing was allowed to soak until each link in the chain was proven to carry
traffic. The gate requires, in order: the generator's per-partition progress
actually advancing; the job `RUNNING` with completed checkpoints; the verifier
observing committed output; the verifier judging closed windows; the chaos
controller recording faults; **the coordinator's own count of workers lost
being non-zero**; and the job recovering from the first fault.

That sixth check exists because of what happened on the previous attempt. The
chaos controller had addressed every host by its public IP, which the rig's own
firewall blocks, so each fault command timed out and was silently ignored while
the controller wrote the fault into the evidence log and carried on. The
cluster completed 1,771 checkpoints, failed none, and lost no worker. The
campaign was minutes from publishing a fault-tolerance result for a cluster
that nothing had touched. A recorded fault is now not accepted as evidence of
a fault; the engine has to have noticed.

## Correctness result

| Counter | Value |
|---|---|
| Windows judged | 396 |
| Windows fully correct | 395 |
| Windows incorrect | 1 |
| Duplicate results | 0 |
| Conflicting results (same key and window, two different values) | 0 |
| Foreign results (output for input never produced) | 0 |
| Output records committed | 6,738,500 |

Every window in the campaign was exact except one: the window that was in
flight when the worker was killed. In that window, 938 of its 16,500 keys were
wrong - some counted twice, others missing entirely - and every window before
and after it was correct. The defect count stayed frozen at that single
window's 938 keys across six consecutive judging rounds while the campaign
went on judging hundreds more windows correctly.

One worker kill was sufficient to produce it. The campaign did not need a
sustained fault campaign to find this, which is worth stating plainly: the
failure is not a rare interleaving reachable only under exotic pressure, it
is what a single ordinary worker loss did on the first attempt.

## The defect

**QUAL-01-D1: a plain restart could silently replay or skip source records.**

Diagnosis had to establish which of three things was wrong - the engine, the
oracle, or the input - because a verdict file cannot tell them apart. The
window was recounted directly from the bytes on the input topic,
independently of both the deterministic spec and the engine:

| | Result |
|---|---|
| Keys where input, spec and engine all agree | 15,562 |
| Keys where the engine disagrees with both input and spec | 938 |
| Keys where the spec disagrees with both | 0 |
| Duplicate event ids on the input topic | 0 |
| Keys emitted more than once by the engine | 0 |

The oracle and the generator were both exonerated. The engine was wrong.

The chain, once found, is short. A Kafka source subscribes to a consumer
group, so which subtask owns which partition is decided by the group
coordinator and is not stable across a restart. Source offsets are
checkpointed per subtask, one row per partition that subtask owned. But the
union of other subtasks' operator rows on restore was gated on *rescale*, on
the reasoning that at unchanged parallelism each subtask's own directory
already holds its state. That is true of keyed state, whose key groups are
pinned to a subtask index. It is false of operator state whose ownership
something outside clink decides.

So a subtask that came back holding a partition it had not owned before found
no checkpointed offset for it and fell through to the broker's committed group
offset. Partitions that rewound re-delivered records already folded into the
open window; partitions that jumped forward lost theirs. Both directions at
once, which is exactly the shape the oracle measured.

The campaign's own logs recorded the trigger: a worker SIGKILL at 21:45:02,
a whole-job restart, a second worker lost during the restart drain, and
recovery with **two** surviving workers rather than three - a changed layout,
and therefore a changed partition assignment, at unchanged parallelism.

**Fixed.** The operator-state union is now unconditional. The regression test
splits two partitions' offsets across two subtasks the way a running job
splits them, restores at unchanged parallelism, and fails against the previous
code in 6 milliseconds.

**Left open and stated plainly:** partition assignment is still dynamic, so a
rebalance that moves a partition mid-run *without* a restart is not addressed
by this fix. Static per-subtask assignment would close it, and is a larger
change than the evidence in hand justifies.

**QUAL-01-D2: the configured checkpoint interval was ignored.**

While investigating the first defect, the cluster was observed completing 61
checkpoints every 30 seconds against a configured interval of 10 seconds -
about one every 490 milliseconds, twenty times the intended rate. One worker
logged 5,947 refused commit dispatches in fifty minutes purely because so many
checkpoints were being taken.

The trigger loop used the smallest configured interval as its own sleep, so it
never *overslept* a job, and then triggered every eligible job on every pass -
so it never waited for one either. Any interval above the loop's 500ms tick
did nothing at all. The cost is paid in state writes, barrier injections and
transactional sink commits, at whatever multiple the configured interval
exceeds 500ms.

**Fixed**, with a regression test that asserts the cadence: against a
60-second interval the unfixed code takes 7 checkpoints in 4 seconds and the
fixed code takes one. This defect predates the hardening phase.

## The harness finding

**The fault generator died four minutes in, and nothing noticed.**

The controller killed a worker, waited, brought it back with
`docker compose up -d`, and asserted the container was running. That restart
carried none of the environment the campaign had deployed with, so compose
fell back to its defaults: the worker came back on the *published* image
rather than the one under test, with an empty coordinator address, and exited
immediately. The assertion then raised, and the exception killed the
controller.

The campaign continued for another fifty-six minutes, collecting evidence and
reporting healthy every ten minutes, with nothing touching the cluster. A soak
with no faults in it looks exactly like a soak that survived them.

Two things follow, and both are now fixed:

- The near miss is worse than the miss. Had that container merely *started*,
  the campaign would have spent the rest of the hour measuring a cluster
  running two different builds of clink and reporting the result as a single
  revision. Restart configuration now lives on the host where anything
  restarting a container reproduces it, and a container that comes back on a
  different image than it went down with is a hard failure.
- The gate proved faults were landing at the start of the run; nothing proved
  they were still landing. The soak loop now checks that the fault generator
  is alive, records the moment it stops, and states that everything after that
  point is an undisturbed soak rather than a fault campaign. A single failed
  fault no longer kills the controller either - it is recorded and the run
  continues, with a ceiling on consecutive failures.

This is the third harness defect in this family found by this programme, after
faults addressed to a firewalled interface and an oracle that could not judge.
Each one would have produced a confident green page. The pattern is consistent
enough to be worth naming: **every link between "the campaign ran" and "the
property holds" has to be proven, continuously, and by something other than
the component being judged.**

## The re-run

A second run (`qual01-20260816b`) at revision `5a767f5`, with both fixes in the
image and the harness repairs above in place, was used to test the fix in the
field. With the fault generator no longer dying, it delivered the campaign the
first run was supposed to have:

| Fault | Count |
|---|---|
| Worker SIGKILL (and its restart) | 3 |
| Coordinator-to-worker partition (and heal) | 2 |
| Two-phase-commit window faults (`sink.before_prepare`, `sink.before_commit`, `coordinator.after_completed_marker`) | 3 |
| Broker restart | 1 |
| Network latency injection (and clear) | 1 |

Correctness across all of it, over 8.27 million input events:

| Counter | Value |
|---|---|
| Windows fully correct | 223 |
| Incorrect aggregates | **0** |
| Duplicate results | **0** |
| Conflicting results | **0** |
| Foreign results | **0** |

Three worker kills and three two-phase-commit window faults produced no
corruption of any kind, where a single worker kill on the unfixed build
corrupted the window in flight. That is the evidence the fix was after.

**But the run is not a clean pass, and its own harness says so.** It is
recorded as FAIL and void, for a reason worth publishing in full.

The last two-phase-commit fault was injected at the coordinator's
`after_completed_marker` point, which crashed the coordinator by design. It
restarted clean - and came back with *no jobs at all*, because the rig
deployed it without `--ha-dir` and it had therefore never persisted a job
manifest to recover from. The pipeline stopped for good. The oracle carried on
doing exactly its job, reporting every subsequent expected result as missing,
and by the end of the hour that was 2.9 million of them.

Read cold, 2.9 million missing results is catastrophic data loss. It is
nothing of the kind: every counter that means corruption stayed at zero, and
the job had simply ceased to exist. Killing a control plane that was never
configured to survive it measures the deployment, not the engine.

Two fixes followed. The rig's coordinator now runs with `--ha-dir` on local
disk, so a coordinator kill becomes a genuine test of job recovery instead of
a guaranteed ending. And the campaign now checks that the *subject* of the
test is still running, not only that its fault generator is - it says the
moment the job stops being `RUNNING`, so a summary can distinguish "the engine
lost data" from "there was no engine". A third run with that configuration is
under way; its result will be added here.

## What this campaign demonstrates

**Demonstrated** - at revision `440043f`, on the topology and scale above:

- Steady-state exactly-once through a transactional Kafka sink: 395 of 396
  windows exact over 6.7 million committed records, with zero duplicate,
  conflicting or foreign results anywhere in the run.
- The engine detects worker loss, restarts the whole job rather than
  redeploying individual subtasks, selects a restore point, and resumes
  processing without operator intervention, continuing to commit afterwards.
- The oracle can distinguish engine error from its own: the recount that
  settled QUAL-01-D1 is retained and reproducible.

**Tested but bounded** - proven only within these limits, which are narrower
than the campaign intended:

- One hour, 2,000 events/sec, 4 partitions, parallelism 4, 50,000 keys,
  in-memory state. Longer durations, higher rates, larger state and higher
  parallelism are not covered.
- **One fault.** A single worker SIGKILL and its restart. Coordinator loss,
  broker restart, network partition, packet loss, latency injection and the
  two-phase-commit window faults were all in the configured profile and none
  of them ran. This campaign is not evidence about any of them.
- The correctness fix is proven by a deterministic regression test, and
  supported in the field by the re-run above: three worker kills and three
  two-phase-commit window faults produced zero corruption of any kind, where
  one kill on the unfixed build corrupted the window in flight. That re-run is
  **not** a completed clean campaign - it is recorded FAIL and void because
  its coordinator was killed by a fault it was not configured to survive, so
  it covers 223 windows rather than a full hour.

**Architecturally supported but not qualified** - exercised incidentally or
not at all: rescale during faults, savepoints, schema evolution, multi-sink
commit groups.

**Unknown** - multi-day behaviour, memory and file-descriptor trends over
days, behaviour under simultaneous broker and worker failure, and behaviour
under repeated back-to-back restarts. The single fault this run applied says
nothing about what the tenth would do.

## Evidence retained

Machine-readable evidence for this run is retained under
`qualification-results/qual01-20260816/`: the host inventory, the build
provenance record, the deployed build's capability manifest, the generated
pipeline SQL, the submission response, the verification gate summary, the
verifier's verdict with defect samples, the chaos controller's fault log, the
independent recount report, and the campaign summary.

Two gaps in that record, stated rather than quietly left:

- The campaign did not retain the coordinator's final metrics, so the restart
  and worker-loss counters quoted above were read from the live cluster while
  it was still up rather than from a retained file. Later campaigns retain
  them.
- The capability manifest is captured from the `clink` CLI, which does not
  link the connector implementations: it lists six connectors where the
  running coordinator lists thirty-three, including the Kafka connector this
  campaign is about. The manifest is therefore accurate about build flags
  (SQL, TLS, fault injection) and understates connectors. Later campaigns also
  retain the coordinator's own registry, which is the authoritative answer and
  the same one the deployment gate consults.

## Honest summary

A campaign that finds a real defect in the property it was built to test is
worth more than one that does not, and this page is deliberately written so
that the defect is the headline rather than a footnote. Two genuine engine
bugs were found in a build that passes its full unit suite and a green CI
matrix, and one worker kill on real infrastructure was enough to surface the
first of them.

The correct reading is not "clink is not exactly-once". It is that clink's
exactly-once guarantee held for 395 of 396 windows and failed on one specific
restart path; that the path is now fixed and pinned by a test that fails
without the fix; and that the field re-run confirming it is still outstanding.

The correct reading is also not "clink survived a fault campaign", because it
did not have one. It had a single worker kill, and this page says so in the
places where a less careful one would have quietly implied otherwise.
