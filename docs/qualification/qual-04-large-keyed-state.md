# QUAL-04: large keyed state under sustained fault injection

Clink holds tens of gigabytes of keyed state on a disaggregated backend -
state far larger than the memory of the machines processing it - and keeps
every key's value exactly correct while processes are killed underneath
it.

The first three campaigns qualify what leaves the engine: exactly-once
into Kafka, into PostgreSQL, into object storage. This one qualifies what
the engine *holds*. A streaming aggregation accumulated a fat value per
key until the job was carrying 29 GiB of live state across 287,524 keys,
then ran for an hour under continuous faults. An independent oracle
recomputed every judged key from the generator's seed and compared it
against the engine's answers: **949,856 events produced, 949,856 folded
into keyed state exactly once, and all 1,915 sampled keys matching their
seed-derived expectation.**

| Provenance | |
|---|---|
| Campaign run | `qual04-20260823a`, 1 hour of faults after a 35-minute fill, aggressive chaos profile |
| Engine | revision `6ba73b5` (runtime image `sha-6ba73b56dd12-faultinj`) |
| Preceded by | `qual04-smoke-g`, a shorter rehearsal of the same battery at the same revision: 415,224 events, exact, all sampled keys correct |
| State backend | `remote-read://` - S3-compatible object storage, per-subtask memory cache bounded at 256 MiB |

## How the claim was measured

State size is measured from **outside** the engine. Clink exposes no live
keyed-state gauge for any deferring backend, and a campaign that trusted
a self-report would be asking the engine to mark its own work. Instead the
oracle decodes the backend's own checkpoint manifest - the structure the
engine restores from - and sums the value objects it references,
deduplicated by content hash. The pass criterion is that measurement, and
the summary refuses to pass a run that did not reach its target: a clean
result at 2 GiB says nothing about 29.

Correctness rests on two independent checks. The first is exact
accounting: the total of every key's event count must equal the number of
events the generator produced, which is one number asserting that every
event was folded exactly once across every fault in the run. The second is
per-key truth: a seeded sample of keys is recomputed from `detspec`, the
same pure function the generator produced events from, and compared
against what the engine holds. Both run after the drain, in a fresh
process, over a settled table - and only once the pipeline has finished
reading, because a pipeline still catching up has legitimately missing
keys that are not lost data.

The workload is built to make state large rather than to move records
quickly: each event is padded to a fixed 32 KiB inside the engine, so
state size is set by key count rather than by throughput, and an
unwindowed `GROUP BY` never closes, so state accumulates for the whole
run. All faults land *after* the fill completes, so every one of them hits
the job at full size.

The rig is eight cloud hosts: three clink workers, one coordinator, a
three-node Redpanda cluster, the object store, and an operations host
outside the engine's failure domain running the generator, the oracle and
the chaos controller.

## Workload

- 949,856 input events over the run, 8 partitions, parallelism 8
- 32,768-byte accumulator per key
- 287,524 distinct keys in state at the end
- Checkpoint interval 30 seconds
- 321 oracle samples during the run

## What the engine survived

| Fault | Count |
|---|---|
| Worker SIGKILL (with restart) | 4 |
| Network partition from the coordinator (healed) | 3 |
| Coordinator restart (recovery verified by stable worker PIDs) | 3 |
| Broker restart | 2 |
| Injected network latency (cleared) | 2 |
| Injected packet loss (cleared) | 2 |
| Kill at a coordinator completion-marker point - fired and recovered | 2 |

Three of those worker losses landed while the job was *already* draining
for a restart caused by an earlier one. Overlapping failures are the case
that distinguishes a recovery path that works from one that merely looks
like it does, and all three resolved with no drain timeout and without the
job stopping.

## Measured outcome

| Property | Observed |
|---|---|
| Live keyed state held | 29.0 GiB |
| Distinct keys | 287,524 |
| Events produced | 949,856 |
| Events folded into keyed state | 949,856 (exact) |
| Sampled keys verified against the seed | 1,915 of 1,915 correct |
| Keys missing from state | 0 |
| Keys with a wrong event count | 0 |
| Keys the engine invented | 0 |
| Accumulators not at full width | 0 |
| Restart drains that timed out | 0 |

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** 29 GiB of keyed state on a disaggregated
  S3-compatible backend, held across the fault battery above with every
  event folded exactly once and every sampled key byte-correct, for this
  workload, for this duration, at revision `6ba73b5`.
- **Tested but bounded:** recovery under overlapping worker losses - three
  occurred and all resolved, which is evidence rather than a guarantee
  about every ordering. Also bounded: state size. 29 GiB is what this run
  reached, not a ceiling the engine was pushed to.
- **Architecturally supported but not qualified:** the 100 GB+ tier, and
  the same guarantees over multi-day durations. Neither has been
  campaigned.
- **Unknown:** fault classes this campaign does not schedule (disk
  corruption, object-store data corruption, clock steps), and behaviour
  against managed object-storage services rather than the self-hosted,
  S3-compatible store used here.

## Caveats

- Correctness is asserted only for the workload, fault profile, state size
  and duration above.
- The accumulator is a synthetic fixed-width value. This campaign measures
  state **volume** and its survival, not a representative application's
  value distribution.
- **The store holds more than the live state, and does not reclaim it on
  its own.** At the end of this run the object store held 78.5 GiB across
  947,273 objects against 29.0 GiB of live state, a ratio of 2.7x. The
  backend is content-addressed and effectively append-only within a run:
  each update to a key writes a new value object, and dropping a
  checkpoint deliberately leaves its objects behind, because the manifest
  set is the reference count. Reclaiming them is a deliberate operation -
  `clink state-sweep` - and an operator running a long-lived job on this
  backend should plan for it. Nothing here is at risk of incorrectness:
  restores read only referenced objects, and this run verified every
  sampled key. It is a storage and cost consideration.
- The sink table is maintained by an upsert sink, which is the
  verification channel rather than the subject. Exactly-once delivery has
  its own campaigns:
  [QUAL-01](qual-01-kafka-exactly-once.md),
  [QUAL-02](qual-02-postgres-two-phase-commit.md),
  [QUAL-03](qual-03-s3-staged-commits.md).
- The state figure is live state as the backend's own manifest describes
  it. The store footprint above is reported separately and deliberately.

Raw evidence (chaos schedule, oracle output, coordinator and worker logs,
the end-state seed verification, the measured store write rate, the
image's digest-verified provenance) is retained per run, and every number
above is taken from it.
