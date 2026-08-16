# Production qualification report

This is the aggregated result of clink's production-qualification programme:
what has been demonstrated on real infrastructure, under what conditions, and
what has not. It is deliberately written so that the defects found are the
headline, because that is what the programme was for.

Anyone evaluating clink should read the four categories in
[how to read these results](README.md#how-to-read-these-results) first. No
claim here is promoted beyond the scope actually tested, and absence of
failure is never treated as proof.

## The short version

The programme found **four correctness defects in clink** and **four defects
in its own test harness**, all on a codebase that passes its full unit suite
and a green CI matrix. Three of the four engine defects are fixed and pinned
by regression tests. **One is open and published unfixed.**

The most serious was an exactly-once violation: after a single ordinary worker
failure, a windowed aggregation silently produced wrong results for the window
in flight - some keys counted twice, others lost. One worker kill on real
infrastructure was enough to surface it on the first attempt. It is fixed,
pinned by a regression test that fails in 6 milliseconds without the fix, and
supported in the field by a re-run in which three worker kills and three
two-phase-commit window faults produced no corruption at all.

The open one, D4, is a different exactly-once violation and was found by the
third run: when a restart *fails* - a second worker lost while the first
restart was still draining, ending in a drain timeout - the job committed
181,071 duplicate window results. Every value was correct; the same result
simply reached the topic twice.

The harness defects matter as much, because each one would have produced a
confident, green, meaningless result page. They are recorded on the campaign
pages in the same voice as the engine defects.

## Defects found

| # | Defect | Severity | Status |
|---|---|---|---|
| D1 | Source offsets could be silently replayed or skipped on a plain restart, breaking exactly-once | **Correctness** | Fixed, regression test, supported by a field re-run |
| D2 | The configured checkpoint interval was ignored; every job checkpointed at the 500ms loop tick | Resource / operability | Fixed, regression test |
| D3 | A peer's operator row could overwrite a subtask's own, which would have made D1's fix lose data for fixed-key sources | **Correctness** | Fixed before shipping, regression test |
| D4 | A restart that fails - a second worker lost mid-drain, ending in a drain timeout - committed 181,071 duplicate window results | **Correctness** | **OPEN** |
| H1 | Chaos faults addressed to a firewalled interface: every fault silently timed out and was logged as applied | Harness | Fixed |
| H2 | The oracle could not judge: wrong consumer group, an unsatisfiable high-water requirement, and a null offset snapshot that made windows permanently unjudgeable | Harness | Fixed |
| H3 | The fault generator died four minutes into an hour and nothing noticed; the campaign reported healthy throughout | Harness | Fixed |
| H4 | A leftover generator from a finished run survived its drain and recreated the topic the next run had just deleted, aborting it | Harness | Fixed |

Two further live defects were found by the pre-campaign code audit, before any
infrastructure was provisioned: Postgres connections could silently downgrade
to plaintext, and Kafka credentials could be configured and never presented.
Both are fixed. See [the feasibility assessment](feasibility.md).

### D1, in one paragraph

A Kafka source subscribes to a consumer group, so which subtask owns which
partition is the broker's decision and is not stable across a restart. Source
offsets are checkpointed per subtask. The union of other subtasks' operator
rows on restore was gated on *rescale*, reasoning that at unchanged
parallelism each subtask's own directory already holds its state. That is true
of keyed state, whose key groups are pinned to a subtask index, and false of
operator state whose ownership something outside clink decides. A subtask that
came back holding a partition it had not owned before found no checkpointed
offset for it and fell through to the broker's committed group offset -
rewinding some partitions and advancing past others, in the same window.

D3 is the other half of that rule, found by auditing the rest of the codebase
after D1's fix landed rather than by any test: not all operator state is
partition-scoped, and unioning peers' rows unconditionally would have handed
every subtask the furthest position any of them had reached for the sources
that keep their position under a single fixed key.

## Campaign results

| Campaign | Subject | Verdict |
|---|---|---|
| [QUAL-01](qual-01-kafka-exactly-once.md) | Kafka exactly-once under fault | Completed over three runs. Three engine defects found; two fixed, D4 open |
| QUAL-02 | Postgres two-phase-commit sink | Prepared; oracle proven against injected defects |
| Others | See the index | Blocked, rescoped, or not yet run - stated per campaign |

## What is demonstrated

Scoped to the topology, scale and duration on each campaign page, and to the
named revision:

- Steady-state exactly-once through a transactional Kafka sink, with an
  independent oracle recomputing every closed window from a deterministic
  specification rather than from anything clink produced.
- Automatic recovery from worker loss: whole-job restart from a selected
  restore point, without operator intervention, continuing to commit
  afterwards - **when the restart succeeds.** When it does not, see D4.

Note the scope carefully. The first run applied **one** fault before its fault
generator died. The re-run applied three worker kills, two partitions, a broker
restart and three two-phase-commit window faults, and produced zero corruption
of any kind - the evidence D1's fix was after - but is recorded void because a
fault killed a coordinator the rig had not configured to survive. The third run
found D4 and failed. No single run is a clean pass, and the page says which is
which.

## What is not

- No campaign has run for longer than an hour. Multi-day behaviour, memory and
  descriptor trends, and slow leaks are **unknown**.
- Large state, TTL steady state, DAG scaling and rolling upgrade are blocked
  or rescoped, for reasons given in the feasibility assessment. They are
  **architecturally supported but not qualified**.
- No campaign has yet exercised broker loss, disk faults, clock skew, or
  simultaneous multi-component failure.
- Throughput and latency are not qualification claims; see the benchmarks
  pages, which have their own premises.

## Method notes that changed the results

Three principles earned their place the hard way, and are recorded because
they generalise beyond this codebase:

**The oracle must be able to fail.** Every verifier in this programme was
handed deliberately corrupted data before it was trusted. The QUAL-02 oracle
was found broken on its first such test - a single malformed identifier made
its judging query throw, which the live verifier would have swallowed as a
database blip and retried forever, writing a verdict file with no findings
while judging nothing.

**A recorded fault is not a fault.** The campaign gate now reads the engine's
own count of workers lost, and refuses to distinguish "the metric says zero"
from "the metric could not be read".

**Proving it once is not proving it throughout.** The gate proves faults land
at the start of a run; a separate check proves they are still landing, because
a soak with no faults in it looks exactly like a soak that survived them.

Underlying all three: *a qualification result is only worth the weakest
unverified assumption in the path that produced it.*

## Infrastructure and cost

Campaigns run on Hetzner Cloud under a unique `qualification_run_id`, with
every resource labelled with it, and are destroyed by label
(`scripts/qualification/destroy.sh <run-id>`) followed by a verification sweep
that fails if anything survives. Orphaned infrastructure is treated as a
failed test. The rig for the campaigns above is eight small instances; total
spend for the work on this page is a few euros.

No credentials are stored in clink checkpoint state or in this repository.
Campaign databases and brokers are dedicated to the run and destroyed with it.

## Reproducing

Every campaign records its clink git SHA, build flags, compiler, dependency
versions, connector capability manifest, cluster configuration and timestamps
under its run id. The qualification image is built from a git archive of the
commit under test and refuses to build from a dirty tree, and the image states
which commit it is. Raw evidence stays machine-readable under
`qualification-results/<run-id>/`; these pages carry the summaries, and never
a number without retained evidence behind it.
