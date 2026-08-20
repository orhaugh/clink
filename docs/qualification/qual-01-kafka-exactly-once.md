# QUAL-01: Kafka exactly-once under sustained fault injection

**Result: PASS.** A windowed aggregation pipeline reading from Kafka and
writing through clink's transactional Kafka sink ran for two hours under an
aggressive, continuously scheduled fault campaign - process kills inside the
two-phase-commit protocol, coordinator loss, broker restarts, network
partitions and full broker outages - and an independent oracle judged every
closed window byte-exact: **zero missing results, zero duplicates, zero
foreign records**.

| | |
|---|---|
| Campaign run id | `qual01-20260820h` |
| Engine revision | `33f30e7` (clink v0.6.0) |
| Runtime image | `sha-33f30e732fad-faultinj` (fault-injection build of the standard runtime image) |
| Duration | 2 hours, aggressive chaos profile |
| Preceded by | `qual01-smoke-d`, a 45-minute full-lifecycle rehearsal of the same battery at the same revision, also PASS (6,108,400 events, 303/303 windows correct) |

## What this campaign asserts

Exactly-once delivery from a Kafka source, through checkpointed windowed
aggregation, into a transactional Kafka sink read with
`isolation.level=read_committed` - held not on a quiet cluster but under
faults injected deliberately into the narrowest windows of the commit
protocol, for the whole run. The claim is bounded by the workload, fault
profile and duration below; the caveats section states exactly what it does
not cover.

## Method

The rig is eight cloud hosts: three clink workers, one coordinator, a
three-node Redpanda cluster speaking the Kafka protocol, and a dedicated
operations host. The generator, the chaos controller and the verifying
oracle all live on the operations host, outside the engine's failure domain,
so no injected fault can destroy the evidence needed to judge it.

The oracle is a seeded generator, not a recording: the verifier recomputes
every window's expected aggregate from the seed and judges the sink's
committed output against it. Faults are injected serially with a minimum
gap, and the campaign refuses to enter its soak until a six-step functional
gate has proven input flowing, the job running with completed checkpoints,
committed output observed, windows judged, a fault landed and recovery
after it. A campaign that injected no faults, or whose job silently died,
cannot report a pass.

Two-phase-commit faults are armed as named points inside the sink and
coordinator protocol code (a fault-injection build of the standard runtime
image), so a kill lands at an exact protocol instant rather than at a
wall-clock moment.

## Workload

- 15,106,800 input events produced over the run
- 4 partitions, 100,000 distinct keys
- 10-second tumbling windows, event-time jitter up to 1,500 ms
- Output observed by the verifier: 13,675,511 records
- Checkpoint interval 300 ms; the final coordinator incarnation alone
  confirmed 224 checkpoints

## Faults injected

| Fault | Count |
|---|---|
| Two-phase-commit point armed (kill at a named protocol instant) | 19 |
| ... of which fired (worker died at the armed point) | 13 |
| ... of which recovered exactly-once | 13 |
| Worker SIGKILL (outside the protocol windows) | 6 |
| Coordinator SIGKILL | 3 |
| Broker restart | 3 |
| Network partition from the coordinator (healed) | 3 |
| Full broker outage (restored) | 2 |
| Injected network latency (cleared) | 1 |

Every mandatory protocol point was covered, including the hardest ones:

- `sink.before_prepare` and `sink.after_prepare` (kill around the barrier
  that seals a transaction)
- `coordinator.before_completed_marker` and `after_completed_marker` (kill
  around the checkpoint's durable completion record)
- `sink.before_commit`, `sink.between_commit_and_receipt` and
  `sink.after_external_commit` (kill around and inside the ack window,
  where the broker has committed but nothing durable records it yet)

Coordinator recovery was additionally verified with stable-PID evidence
eight times: the restarted coordinator proved it was a genuinely new
process resuming from durable state.

## Correctness (independent oracle, read_committed)

| Counter | Value |
|---|---|
| Windows judged | 755 |
| Windows fully correct | 755 |
| Missing results | 0 |
| Duplicates (same value twice) | 0 |
| Conflicting results (same key and window, different values) | 0 |
| Incorrect aggregates | 0 |
| Foreign results | 0 |
| Recovery timeouts (no completed checkpoint after a fault) | 0 |

## What the campaign hardened

QUAL-01 was run to find defects, and it found them; each mechanism below
was driven in by campaign evidence, reproduced locally as a failing test
first, and is now held by permanent regression gates in the integration
suite.

- **Commit receipts and replay suppression.** The sink durably records each
  broker-acknowledged commit between the commit and the next transaction
  begin, and a restore that must replay a committed interval swallows
  exactly the re-emissions the receipt's watermark horizon covers.
- **In-doubt resolution.** Recovery proves or executes orphaned prepared
  transactions over the wire before choosing a restore point, walks every
  handle of a checkpoint before deciding, and materialises the receipt for
  every commit it proves.
- **Unresolved-orphan markers and the pre-fence describe.** A resolution
  walk that a broker outage leaves unresolved persists what it could not
  settle; the restarted sink then asks the broker about its predecessor's
  transaction before opening a producer, and refuses to open at all while
  no broker can answer - because fencing first would erase the only
  evidence of whether the orphan committed.
- **Bounded, cancellable recovery.** The resolution walk is cancellable at
  a deadline without abandoning safety; restarts held on missing capacity
  wait for workers to return rather than failing the job; restart drains
  tolerate sinks legitimately blocked in bounded client calls against an
  unreachable broker.
- **Crash-consistent checkpoint numbering.** New checkpoints number above
  every snapshot file any incarnation left on disk, so rapid restart storms
  can never assemble one checkpoint id from two vintages.
- **Session liveness under broker outages.** Worker commit dispatch runs on
  its own thread, so a commit blocked on an unreachable broker cannot
  freeze the worker's control-plane session.

Regression gates:
`AnOrphanedCommitIsResolvedBeforeFencing`,
`AnUnreceiptedCommitInAMixedVerdictIsNotReplayedAsDuplicates`,
`AFencedPartialCommitFallsBackWithoutDuplicates`,
`ARestartStormStaysExactlyOnce`,
`ABrokerOutageDuringRecoveryStaysExactlyOnce` and
`TheRigNightCompositeStaysExactlyOnce` (the whole campaign night - restart
storm, slow persists, armed double-kill, broker outage during recovery,
heal - as one exact-oracle test), plus unit coverage of the resolution
walk's verdict, marker and cancellation semantics.

## Resource footprint

Checkpoint volume usage peaked at 6.7 GiB on shared storage. Host memory
and CPU peaks were not systematically collected in this campaign and are
deliberately not claimed; a later campaign covers steady-state resource
behaviour.

## Claim categories

Following the programme's honesty labels:

- **Demonstrated:** exactly-once (zero missing, zero duplicate, zero
  foreign) from Kafka source to transactional Kafka sink under the fault
  battery above, for this workload, for two hours, at revision `33f30e7`.
- **Tested but bounded:** recovery liveness (every fault was followed by a
  completed checkpoint; recovery timeouts 0) - bounded by this topology's
  restart pacing and the campaign's recovery deadline.
- **Architecturally supported but not qualified:** the same guarantees for
  other exactly-once sinks (Postgres two-phase commit, staged object-store
  commits), larger state, higher parallelism, and multi-day durations.
- **Unknown:** behaviour under fault classes this campaign does not
  schedule (disk corruption, byzantine brokers, clock steps).

## Caveats

- Correctness is asserted only for the workload, fault profile and duration
  above. Nothing here extends to untested connectors, larger state or
  longer runs.
- The verdict counts closed windows whose full input was produced and whose
  grace period elapsed; windows still open at shutdown are not judged
  either way.
- Consumers must read with `isolation.level=read_committed` for the
  guarantee to hold, and the campaign's verifier does.

Raw evidence (chaos schedule, verifier output, coordinator and worker logs,
metrics snapshots) is retained per run and every number above is taken from
it.
