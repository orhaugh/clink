# QUAL-05: bounded state through retention, held under fault injection

Clink bounds the keyed state of an unbounded streaming job through
declared retention (`state_ttl`): over a stream whose key space keeps
turning over, live state reaches a plateau and stays there - within 2% of
the level predicted from the workload's own arithmetic - while every
output stays exactly correct, across a sustained fault battery.

The first four campaigns qualify delivery and size. This one qualifies
the property that makes long-lived jobs operable at all: that state
STOPS GROWING. A job folded 8.3 million events under continuous faults
while retention released state underneath it, and an independent oracle
recomputed every expected key from the generator's seed: **8,326,400
events produced, 8,326,400 folded exactly once, and 694,996 of 694,996
keys holding exactly their seed-derived count.**

| Provenance | |
|---|---|
| Campaign run | `qual05-20260823a`, 90-minute soak after a control arm and warm-up, aggressive chaos profile |
| Engine | revision `bed138c` (runtime image `sha-bed138c3e94a-faultinj`) |
| Rig | the standard 8-host cloud rig - 3 workers, coordinator, 3 brokers, operations host - provisioned for this run and destroyed after it ([what that is](README.md#the-rig)) |
| Preceded by | three isolated local runs at the same revision, each folding its stream exactly (52,890 = 52,890, three times) |
| State backend | file-backed snapshots on a shared mount; retention via `state_ttl='10m'`, event-time domain |

## The control arm: what makes a flat line falsifiable

A flat state curve proves nothing on its own - a workload that never
accumulated anything is also flat. So the campaign runs the identical
workload twice. The control arm runs first with retention removed
(`ALLOW UNBOUNDED STATE`): its state grew **12.9x** during its window,
establishing that this workload grows without retention. The subject arm
then runs with `state_ttl` declared, and the summary refuses a PASS
unless the control grew. The two arms differ in nothing but those clauses.

## How the claim was measured

State size is measured from **outside** the engine: the instrument sums
one checkpoint's worth of snapshot files - each subtask's newest - from
the artefacts the engine writes for its own recovery on the shared mount,
and refuses to report rather than reading zero on an empty directory.

The plateau verdict is decided against bands written before the run: the
fitted trend across the steady-state window may drift at most 25% of the
mean, level stability (p90/p10) at most 1.5, worst transient (max/min) at
most 2.5.

Correctness rests on the same discipline as the other campaigns: exact
accounting (the sum of every key's count must equal the events produced -
one number asserting every event folded exactly once), plus every
expected key recomputed from `detspec`, the pure function the generator
produced events from. Not a sample: all 694,996 keys were judged, in a
fresh process, over a settled table, after the pipeline provably caught
up. The workload's key space advances with event time in disjoint epochs,
so a key's whole life fits inside one epoch and its final count is exact
regardless of when retention later released its state.

## Workload

- 8,326,400 input events at 1,000 events/s, 4 partitions, parallelism 4
- Key space turning over: 5,000 keys per 60-second epoch, never revisited
- Retention `state_ttl='10m'` on a pipeline of `SELECT DISTINCT` into an
  unwindowed `GROUP BY` - the two operator families plus the aggregate
  tracker exercising the engine's retention machinery
- Checkpoint interval 15 seconds; 37 steady-state samples over 90 minutes

## What the engine survived

| Fault | Count |
|---|---|
| Worker SIGKILL (with restart) | 8 |
| Network partition from the coordinator (healed) | 4 |
| Coordinator SIGKILL (recovery verified by stable worker PIDs) | 3 |
| Broker restart | 3 |
| Injected network latency (cleared) | 3 |
| Injected packet loss (cleared) | 2 |
| All brokers unavailable, then restored | 1 |
| Kill at a coordinator completion-marker point - fired and recovered | 4 |

## Measured outcome

| Property | Observed |
|---|---|
| Mean live state over the steady window | 67.4 MiB |
| Predicted plateau from the workload arithmetic | ~68 MiB |
| Drift across 90 minutes | 1.9% of mean (band ±25%) |
| Level stability, p90/p10 | 1.02 (band 1.5) |
| Worst transient, max/min | 1.02 (band 2.5) |
| Control arm growth without retention | 12.9x |
| Events produced / folded | 8,326,400 / 8,326,400 (exact) |
| Keys judged against the seed | 694,996 of 694,996 correct |
| Keys missing / wrong / invented / NULL | 0 / 0 / 0 / 0 |

## What preparing this campaign found

This campaign could not have passed on the engine as it stood a day
earlier. Its local phase - at no cloud cost - found and drove the fixes
for three real defects in the retention path, each now pinned by
mutation-checked regression tests:

- **A declared `state_ttl` did not bound DISTINCT or set operations.**
  Their expiry sweep could release at most 256 entries per watermark and
  restarted from the front each time, so any arrival rate above that
  outran reclamation for ever (measured: 4,880 of 10,000 entries still
  resident). Both operators now use the same deadline index as the
  aggregate and joins.
- **A retention deadline could precede the record that set it.** Deadlines
  were stamped from the watermark alone; under partition skew - any
  catch-up read - a fast input's records carry event times far ahead of
  the aligned watermark, their state expired before the stream's time
  reached them, and the slow input's on-time records re-opened it from
  zero: a silent permanent under-count, reproduced with zero faults.
  Deadlines now stamp from the later of the watermark and the newest
  observed record time.
- **Records carried no event time at all on the columnar path.** The
  watermark assigner computed every row's event time, advanced the
  watermark - and forwarded the batch with its event-time column still
  null, so every downstream consumer of per-record event time was blind
  on the default Kafka-JSON path. The assigner now stamps the extracted
  times into the forwarded batch, still columnar.

The campaign also surfaced, and works around, an open finding: a
cancelled job is resurrected by a coordinator restart, because the HA
store records no terminal transition. It is tracked for an engine fix;
this campaign isolates its arms so a resurrected control cannot reach the
subject's evidence.

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** bounded keyed state through declared event-time
  retention, at this workload shape, TTL and duration, at revision
  `bed138c`, with exact output throughout the fault battery above.
- **Tested but bounded:** the plateau held for 90 minutes at one
  TTL-to-turnover ratio; multi-day steady state is QUAL-10's subject.
  Retention gauges (`clink_state_ttl_*`) are process-local and reset on
  worker restart, so their drain-time totals under-read a chaos run - the
  pass criterion never rests on them.
- **Architecturally supported but not qualified:** retention on the
  disaggregated backends. A TTL'd operator's restore currently scans its
  deadline slot, which on `remote-read://` costs one store read per
  entry; qualifying that combination needs its own campaign once the
  restore path is batched.
- **Unknown:** fault classes this campaign does not schedule (disk
  corruption, clock steps), and retention behaviour under mid-run
  rescale.

## Caveats

- Correctness is asserted only for the workload, retention configuration,
  fault profile and duration above.
- The control arm's sampling window overran its nominal 15 minutes
  (slow remote sampling ticks); its growth measurement is first-to-last
  and unaffected.
- The laptop-side campaign driver died mid-soak and was replaced by a
  resume driver; the campaign itself never noticed, because every
  rig-side process is detached by design and the drain logic is
  idempotent. The evidence records both drivers' outputs.

Raw evidence (chaos schedule, oracle output, the state series and its
bands, coordinator and worker logs, the end-state seed verification, the
image's digest-verified provenance) is retained per run, and every number
above is taken from it.
