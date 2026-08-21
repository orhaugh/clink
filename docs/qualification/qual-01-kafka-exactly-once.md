# QUAL-01: Kafka exactly-once under sustained fault injection

Clink delivers exactly-once semantics from a Kafka source, through
checkpointed stateful processing, into its transactional Kafka sink - read
with `isolation.level=read_committed` - and holds that guarantee under
faults aimed deliberately at the narrowest windows of the commit protocol.

This campaign ran a windowed aggregation pipeline for two hours on a
multi-host cluster while processes were killed inside the two-phase-commit
protocol's own windows, the coordinator was killed outright, brokers were
restarted and taken away entirely, and the network was partitioned and
degraded. An independent oracle judged every closed window byte-exact:
**755 of 755 windows correct, with zero missing results, zero duplicates,
and zero foreign records.**

| Provenance | |
|---|---|
| Campaign run | `qual01-20260820h`, 2 hours, aggressive chaos profile |
| Engine | revision `33f30e7` (runtime image `sha-33f30e732fad-faultinj`) |
| Preceded by | `qual01-smoke-d`, a 45-minute rehearsal of the same battery at the same revision: 6,108,400 events, 303 of 303 windows correct |

## How the claim was measured

The oracle is a seeded generator, not a recording: the verifier recomputes
every window's expected aggregate from the seed and judges the sink's
committed output against it, reading Kafka as any `read_committed`
consumer would. The generator, the chaos controller and the verifier all
live on an operations host outside the engine's failure domain, so no
injected fault can destroy the evidence needed to judge it.

The rig is eight cloud hosts: three clink workers, one coordinator, a
three-node Redpanda cluster speaking the Kafka protocol, and the
operations host. Faults are injected serially, and nothing soaks until a
functional gate has proven input flowing, the job running with completed
checkpoints, committed output observed, windows judged, a fault landed and
recovery after it. Two-phase-commit faults are armed as named points
inside the sink and coordinator protocol code, so a kill lands at an exact
protocol instant rather than at a wall-clock moment.

## Workload

- 15,106,800 input events over the run
- 4 partitions, 100,000 distinct keys
- 10-second tumbling windows, event-time jitter up to 1,500 ms
- 13,675,511 output records observed by the verifier
- Checkpoint interval 300 ms; the final coordinator incarnation alone
  confirmed 224 checkpoints

## What the engine survived

| Fault | Count |
|---|---|
| Kill at a named 2PC protocol point - fired and recovered exactly-once | 13 |
| Worker SIGKILL (outside the protocol windows) | 6 |
| Coordinator SIGKILL (recovery verified by stable worker PIDs, 8 times) | 3 |
| Broker restart | 3 |
| Network partition from the coordinator (healed) | 3 |
| Full broker outage (restored) | 2 |
| Injected network latency (cleared) | 1 |

Every mandatory protocol point was covered, including the hardest:
`sink.between_commit_and_receipt` - the ack window, where the broker has
committed but nothing durable records it yet - and the kills either side
of the checkpoint's completion record. A worker killed inside the ack
window is recovered by proving the commit over the wire before anything
can fence the orphaned transaction; the receipts, resolution walk and
pre-fence describe that make this exact are described in the
[Kafka connector reference](../connectors/kafka.md).

## Measured outcome

| Property | Observed |
|---|---|
| Windows judged | 755 |
| Windows fully correct | 755 |
| Missing results | 0 |
| Duplicates (same value twice) | 0 |
| Conflicting results (same key and window, different values) | 0 |
| Foreign results | 0 |
| Recovery timeouts (no completed checkpoint after a fault) | 0 |

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** exactly-once (zero missing, zero duplicate, zero
  foreign) from Kafka source to transactional Kafka sink under the fault
  battery above, for this workload, for two hours, at revision `33f30e7`.
- **Tested but bounded:** recovery liveness (every fault was followed by a
  completed checkpoint) - bounded by this topology's restart pacing and
  the campaign's recovery deadline.
- **Architecturally supported but not qualified:** the same guarantees at
  larger state, higher parallelism, and multi-day durations. Exactly-once
  into PostgreSQL and into object storage have their own campaigns:
  [QUAL-02](qual-02-postgres-two-phase-commit.md),
  [QUAL-03](qual-03-s3-staged-commits.md).
- **Unknown:** fault classes this campaign does not schedule (disk
  corruption, byzantine brokers, clock steps).

## Caveats

- Correctness is asserted only for the workload, fault profile and
  duration above. Nothing here extends to untested connectors, larger
  state or longer runs.
- The verdict counts closed windows whose full input was produced and
  whose grace period elapsed; windows still open at shutdown are not
  judged either way.
- Consumers must read with `isolation.level=read_committed` for the
  guarantee to hold, and the campaign's verifier does.

Resource footprint: checkpoint volume usage peaked at 6.7 GiB on shared
storage; host memory and CPU peaks were not systematically collected in
this campaign and are deliberately not claimed.

Raw evidence (chaos schedule, verifier output, coordinator and worker
logs, metrics snapshots) is retained per run, and every number above is
taken from it.
