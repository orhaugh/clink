# QUAL-03: S3 exactly-once through staged object commits

Clink delivers exactly-once into S3-compatible object storage through
staged multipart commits, and holds that guarantee when processes are
killed inside the commit protocol's own windows and when the object
store itself is unavailable during a recovery.

Object storage offers no transaction manager. There is no primary key to
reject a duplicate and no prepared-transaction registry to reconcile
against, so the guarantee rests entirely on the engine's own bookkeeping:
records are buffered for a checkpoint interval, uploaded as multipart
parts at the barrier, and the object becomes visible atomically at
CompleteMultipartUpload only once the checkpoint is durable. This
campaign ran that pipeline for two hours on a multi-host cluster under
continuous faults. An independent oracle read the bucket directly and
judged every committed line: **7,477,200 events produced, 7,477,200
committed exactly once, with zero duplicates, zero gaps, zero foreign
records, and zero corrupted objects.**

| Provenance | |
|---|---|
| Campaign run | `qual03-20260821a`, 2 hours, aggressive chaos profile |
| Engine | revision `625cc82` (runtime image `sha-625cc82d69af-faultinj`) |
| Preceded by | `qual03-smoke-a`, a 45-minute rehearsal of the same battery at the same revision: 2,917,500 events, all committed exactly once |
| Sink | `s3_2pc_sink`, confirmed deployed from the worker's own open-time log before the soak began |

## How the claim was measured

The oracle is a seeded generator, not a recording: it recomputes what
should exist from the seed and judges the bucket against it, reading only
what a downstream consumer would see. It reads incrementally, fetching
each object once on first sight and keeping an ETag ledger, so a
committed object that later changes is itself a finding. The generator,
the chaos controller and the oracle all run on an operations host outside
the engine's failure domain, so no injected fault can destroy the
evidence needed to judge it.

Five failure modes are independently countable across the visible
objects: a duplicate is a repeated event id, a loss is a gap in a
partition's contiguous sequence, a fabrication is an id outside the
generated range, a zero-byte object is a corrupted commit, and a
committed object whose content changes after it was judged is a
mutation. The final verdict is not the oracle's own running total: after
the drain, a separate process re-reads the settled bucket in full and
recounts from scratch, so the campaign never certifies itself.

The rig is eight cloud hosts: three clink workers, one coordinator, a
three-node Redpanda cluster speaking the Kafka protocol, the object
store, and the operations host. Nothing soaks until a functional gate has
proven input flowing, the job running with completed checkpoints,
objects committed, the staged-commit sink family actually deployed, a
fault landed, and recovery after it. Two-phase-commit faults are armed as
named points inside the sink and coordinator protocol code, so a kill
lands at an exact protocol instant rather than at a wall-clock moment.

## Workload

- 7,477,200 input events over the run
- 4 partitions, 50,000 distinct keys
- Checkpoint interval 8 seconds, so a commit window opens that often per subtask
- 3,688 objects committed and read by the oracle
- 377 verifier samples over the run

## What the engine survived

| Fault | Count |
|---|---|
| Worker SIGKILL (with restart) | 6 |
| Kill at a named 2PC protocol point - fired and recovered exactly-once | 10 |
| Object store unavailable, then restored | 4 |
| Network partition from the coordinator (healed) | 4 |
| Coordinator SIGKILL (recovery verified by stable worker PIDs, 7 times) | 3 |
| Broker restart | 3 |
| Injected packet loss (cleared) | 2 |

Every mandatory protocol point was covered: the kills either side of
prepare, either side of the checkpoint's completion record, before the
external commit, and after it. The last of those is the case that only an
idempotent commit survives, because recovery re-completes an upload the
store has already finalised.

The campaign's decisive composition is the object store going down while
a worker-loss recovery needs it. A staged multipart upload outlives the
process that started it, so a restarted worker must complete a restored
handle against an endpoint that is not answering, retry through the
restart cycle, resolve nothing blind, and converge on the heal with the
pane committed exactly once. That happened four times during the run,
and the verdict was unaffected.

## Measured outcome

| Property | Observed |
|---|---|
| Events produced | 7,477,200 |
| Distinct events committed (fresh full re-read) | 7,477,200 |
| Duplicates | 0 |
| Gaps (missing sequences in a partition's prefix) | 0 |
| Foreign records | 0 |
| Zero-byte or corrupted objects | 0 |
| Committed objects mutated after being judged | 0 |
| Partitions committed past what the generator produced | 0 |

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** exactly-once from a Kafka source into the staged
  object-store sink under the fault battery above, including store
  outages during recovery, for this workload, for two hours, at revision
  `625cc82`, against an S3-compatible store (MinIO, pinned release).
- **Tested but bounded:** recovery liveness (every fault was followed by
  a completed checkpoint) - bounded by this topology's restart pacing and
  the campaign's recovery deadline.
- **Architecturally supported but not qualified:** the same guarantees
  against AWS S3 and other managed S3-compatible services; the same
  guarantees at higher parallelism and multi-day durations. Keyed state at
  size has its own campaign: [QUAL-04](qual-04-large-keyed-state.md).
  Exactly-once into Kafka and into PostgreSQL have their own campaigns:
  [QUAL-01](qual-01-kafka-exactly-once.md),
  [QUAL-02](qual-02-postgres-two-phase-commit.md).
- **Unknown:** fault classes this campaign does not schedule (disk
  corruption, object-store data corruption, clock steps).

## Caveats

- Correctness is asserted only for the workload, fault profile and
  duration above. Nothing here extends to untested connectors, larger
  state or longer runs.
- Multipart uploads left pending by a worker killed before its checkpoint
  became durable are expected, not defects: they produce no visible
  object and no downstream consumer can see them. 110 remained after the
  run, which is why the sink's documentation calls for a lifecycle rule
  (`AbortIncompleteMultipartUpload`) to expire them. The campaign records
  the count as evidence and does not treat it as a fault.
- The store was self-hosted so that its availability could itself be a
  scheduled fault. A managed service cannot be paused on command, and the
  outage composition above is the point of the campaign.
- The oracle recorded three transient sample errors against the store
  during injected outages. Each retried and judged normally; a persistent
  failure would have been recorded as a stuck oracle and failed the run.

Raw evidence (chaos schedule, verifier output, coordinator and worker
logs, the end-state re-read, the image's digest-verified provenance) is
retained per run, and every number above is taken from it.
