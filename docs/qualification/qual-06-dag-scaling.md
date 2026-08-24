# QUAL-06: wide job graphs at high parallelism, exactly once under faults

Clink deploys and runs a wide SQL job graph - **147 operators, deployed as
292 network-bridged subtasks** - with exactly-once output held across a
sustained fault battery. The claim is made on a ladder: each width must
first deploy, checkpoint, fill its sink, and then **survive a controlled
worker kill** - checkpoint id and folded output both advancing past their
pre-kill values - before the next width is attempted, so every green rung
is a recovery claim, not a deployment claim. The independent oracle then
judged the largest green width under chaos: **838,824 events produced,
838,824 folded exactly once, and 228,152 of 228,152 keys holding exactly
their seed-derived count.**

| Provenance | |
|---|---|
| Campaign run | `qual06-20260824d`, recovery-gated ladder then a 45-minute battery at the claim rung, aggressive chaos profile |
| Engine | revision `3c7ffd1` (runtime image `sha-3c7ffd1cff9b-faultinj`) |
| Preceded by | a local-rig pass at the same revision (three rungs, 264,000 = 264,000 exact), and three earlier rig runs at `bed138c` whose failures drove the engine fixes below |
| Graph shape | N-branch SQL `UNION ALL`: per-branch Kafka source, JSON decode, watermark assigner, filter and projection, a binary union tree, one keyed aggregate, one upsert sink |

## Why width is the subject

The SQL planner deploys every operator as its own subtasks - there is no
operator chaining on the cluster - so graph width multiplies **network
bridges**, and the bridges are where scale bites: peer resolution at
deploy, barrier alignment across every edge at each checkpoint, and drain
choreography across every subtask at each recovery. A wide `UNION ALL` is
the deliberate worst case: B branches make 6B+3 operators whose union
tree funnels every edge through the checkpoint and restart machinery at
once.

Two shapes of honesty about the width numbers:

- Each branch reads the shared topic under its **own consumer group** -
  replicated scans of one table share a group and would silently split
  the stream across branches, losing most of it by construction. The
  B-fold broker read amplification is the stated premise.
- The claim counts **deployed subtasks** (the coordinator's
  `expected_completion`), not arithmetic. Source chains cap their
  parallelism at the partition count, so the 147-operator graph at
  parallelism 4 deploys 292 tasks, not 588. Published widths are what
  ran, not what was multiplied.

## The ladder

| Rung | Branches | Operators | Deployed subtasks | Deploy | First checkpoint | Recovered a worker kill in | Status |
|---|---|---|---|---|---|---|---|
| 1 | 8 | 51 | 100 | 3s | 3s | 28s | green |
| 2 | 24 | 147 | 292 | 1s | 1s | 28s | green |

The battery then ran on the rung-2 job - a job that had already survived
one controlled kill before chaos began.

**The measured boundary above the claim:** at 48 branches (291 operators,
1,160 deployed tasks on this rig's slot layout), deployment does not
complete: the startup connection storm across the bridge fan-in is read
as dead peers, the whole job restarts into a drain of over a thousand
subtasks, and the deploy window expires. Reproduced on two runs; tracked
as an open engine finding. The boundary is a result: this campaign
publishes the largest width where recovery *works*, with the limit stated
above it.

## What the engine survived at the claim rung

| Fault | Count |
|---|---|
| Worker SIGKILL (with restart) | 2 |
| Network partition from the coordinator (healed) | 2 |
| Coordinator SIGKILL + restart (worker PIDs stable across recovery) | 2 |
| Broker restart | 3 |
| Injected network latency (cleared) | 2 |
| Injected packet loss (cleared) | 1 |
| Kill at a coordinator completion-marker point - fired and recovered | 2 |

## Measured outcome

| Property | Observed |
|---|---|
| Claim width | 147 operators as 292 deployed subtasks (parallelism 4) |
| Recovery from a controlled worker kill at that width | 28 seconds to checkpoint AND output advancing |
| Events produced / folded | 838,824 / 838,824 (exact) |
| Keys judged against the seed | 228,152 of 228,152 correct |
| Keys missing / wrong / invented / NULL | 0 / 0 / 0 / 0 |
| Oracle samples during the battery / findings | 152 / 0 |

Correctness rests on the same discipline as every campaign: exact
accounting (the sum of every key's count equals the events produced),
plus every expected key recomputed from `detspec`, the pure function the
generator produced events from - all 228,152 keys, in a fresh process,
over a settled table, after the pipeline provably caught up.

## What this campaign found

QUAL-06 is the programme's best argument for progressive ladders: three
earlier rig runs at `bed138c` each failed differently at width, and the
retained evidence closed every mechanism. The claim above runs on the
fixed engine; each fix is pinned by mutation-checked regression tests.

- **A job could die without ever reaching a terminal state.** Both the
  fatal-error path and a client cancel terminate a job by cancelling
  every peer and counting the exits up to the expected total - and that
  count had no deadline anywhere. One peer whose cancel never landed
  parked a 292-task job at 291/292: RUNNING for 75 minutes, checkpoint
  frozen, its verdict already recorded, invisible to every watchdog - and
  on an earlier run, a client cancel "ignored" for 40 minutes the same
  way. A terminal-cancel deadline now force-completes the job (FAILED or
  CANCELLED as appropriate), naming the subtasks that never reported; the
  fatal-error broadcast also gained the log line whose absence made the
  wedge silent.
- **A cancel could miss a task still being constructed.** CancelJob flips
  the cancel tokens registered at that moment, and task construction runs
  on task threads: a task finishing construction just after the flip
  registered a token nobody would ever set and ran on as an orphan of a
  cancelled deployment - the very peer that parks the count. The worker
  now latches cancelled job ids and starts such a task pre-cancelled.
- **Retention could purge recovery's only fallback.** With one retained
  checkpoint per subtask (the default), an unreadable newest checkpoint
  leaves the restore no older floor to fall back to - which converted one
  unreadable sidecar into a fatal restore refusal at width. Retention
  depth is now configurable per worker (`--checkpoint-num-retained`);
  qualification rigs keep 3. Why that sidecar was unreadable while its
  checkpoint carried a completion marker is still under investigation as
  an open finding.

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** exactly-once output from a 147-operator, 292-subtask
  SQL graph under the fault battery above, including 28-second recovery
  from a worker kill at that width, at revision `3c7ffd1`.
- **Tested but bounded:** the ladder stopped at the stated startup
  boundary (~1,160 deployed tasks on this rig); keyed-operator
  parallelism is architecturally capped at 128 key groups; the battery
  ran 45 minutes at one workload shape.
- **Architecturally supported but not qualified:** the same widths on
  disaggregated state backends, and typed-API (plugin) graphs at width.
- **Unknown:** mid-run rescale at these widths, and fault classes the
  battery does not schedule (disk corruption, clock steps).

## Caveats

- Correctness is asserted only for the graph shape, widths, fault profile
  and duration above.
- The two earlier rig runs whose failures drove the fixes were judged by
  the same oracle: their failures were availability failures (jobs
  wedging non-terminal), never data failures - 0 invented keys and 0 NULL
  counts across every run of this campaign, passing and failing alike.
- The startup boundary is a property of this rig's size and slot layout
  as much as of the engine; it is stated as measured, not as a universal
  limit.

Raw evidence (per-rung records, chaos schedule, oracle output,
coordinator and worker logs, the end-state seed verification, the image's
digest-verified provenance) is retained per run - for the passing run and
for the three failing ones its fixes came from - and every number above
is taken from it.
