# QUAL-02: Exactly-once into PostgreSQL

Clink's PostgreSQL sink delivers exactly-once semantics through genuine
two-phase commit: rows are staged in an open transaction, `PREPARE
TRANSACTION` seals them under a deterministic global id at the checkpoint
barrier, and `COMMIT PREPARED` executes only once the checkpoint is
globally durable. A prepared transaction survives the death of the process
that created it, so a crash between prepare and commit loses nothing:
recovery commits what the durable checkpoint covers and rolls back what it
does not.

This campaign put that guarantee under sustained attack on a multi-host
cluster and measured the outcome from the database itself. Over a two-hour
run with faults injected continuously into the narrowest windows of the
commit protocol, **7,461,000 events were produced and 7,461,000 distinct
rows were committed - every produced event exactly once, with zero
duplicates, zero gaps, zero foreign records, and no prepared transaction
left behind.**

| Provenance | |
|---|---|
| Campaign run | `qual02-20260821b`, 2 hours, aggressive chaos profile |
| Engine | clink v0.7.0, revision `2e55943` (clean tree) |
| Preceded by | `qual02-smoke-a`, a 45-minute rehearsal of the same battery at the same revision: 3,037,900 events, all exactly once |

## How the claim was measured

The judge is independent of the engine. A seeded generator produces
events whose exact expected set is recomputable from the seed; a verifier
reads PostgreSQL directly - it never asks clink anything - and continuously
checks four properties per partition: no duplicates (total rows equal
distinct rows), no gaps (a contiguous committed prefix from zero), no
foreign records (nothing the generator never produced), and no orphaned
prepared transactions accumulating server-side. The final verdict is taken
only over a settled table, after the drain, when the generator's completed
output is a true upper bound.

The rig is eight cloud hosts - three clink workers, one coordinator, a
three-node Redpanda cluster at replication 3, and an operations host
carrying the generator, the verifier and the chaos controller outside the
engine's failure domain - with checkpoint state on an NFS export shared
across the cluster, so a killed worker's subtasks restore their state
wherever they are redeployed.

Nothing soaks until a functional gate has proven, in order: input flowing,
the job running with completed checkpoints, exactly one job on the
coordinator, rows committed to the database, the two-phase-commit sink
family genuinely deployed, a first fault confirmed by the engine's own
counters, and recovery from that fault with commits continuing. A campaign
that cannot demonstrate its own machinery never reaches the soak, and
cannot produce a verdict.

## What the engine survived

Faults were injected serially for the full two hours, the decisive ones
scheduled first and every one verified to have actually landed - a kill
inside a protocol window is only counted when the armed process died at
that exact point and the pipeline then recovered.

| Fault | Count |
|---|---|
| Worker SIGKILL (with recovery each time) | 8 |
| Coordinator SIGKILL (7 recoveries verified by stable worker PIDs) | 5 |
| Kill inside a named 2PC protocol window - all six points, each fired and recovered exactly-once | 9 fired |
| PostgreSQL frozen mid-run (healed) | 3 |
| Full Kafka-cluster outage (restored) | 2 |
| Network partition from the coordinator (healed) | 2 |
| Packet loss (cleared) | 3 |
| Broker restart under replication 3 | yes |
| Injected network latency | yes |

The six two-phase-commit protocol points bracket the exact instants where
an exactly-once claim can break: either side of `PREPARE TRANSACTION`,
either side of the checkpoint's durable completion record, and either side
of `COMMIT PREPARED`. A worker killed at any of them left the database
exactly once. The PostgreSQL outages compose the hardest case: the
external transaction manager down precisely while a recovery needs it to
resolve prepared transactions - the sink retries through the restart cycle
and resolves nothing blind.

## Measured outcome

| Property | Observed |
|---|---|
| Events produced | 7,461,000 |
| Distinct rows committed | 7,461,000 (complete) |
| Duplicates, gaps, foreign records | 0 |
| Prepared transactions after the clean stop | none |
| Prepared transactions in flight at peak | 4 |
| Verdict taken over a settled table | yes |
| Mandatory fault coverage | complete, no gaps |

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** exactly-once delivery into PostgreSQL through
  `PREPARE TRANSACTION` / `COMMIT PREPARED` under the fault battery above,
  for this workload, for two hours, at revision `2e55943`; and clean
  resolution of every prepared transaction, including through PostgreSQL
  outages that landed during recovery.
- **Tested but bounded:** recovery liveness - every fault was followed by
  restored commit flow - bounded by this topology's restart pacing.
- **Architecturally supported but not qualified:** the same guarantee at
  larger state, higher parallelism, and multi-day durations; the other
  recoverable-commit sinks (staged object-store commits) share the
  framework but have their own campaign.
- **Unknown:** fault classes this campaign does not schedule (disk
  corruption, byzantine database behaviour, clock steps).

## Caveats

- Correctness is asserted only for the workload, fault profile and
  duration above; nothing here extends to untested connectors or longer
  runs.
- The completeness assertion is valid only after the drain, comparing the
  generator's final produced count with distinct committed rows once the
  table settled.
- The sink table deliberately carries no unique constraint, so the
  database cannot mask a duplicate by rejecting it - the oracle must be
  able to see one land.

Raw evidence - the chaos schedule, the verifier's every sample, cluster
logs, the image's capability manifest and digest-verified provenance - is
retained per run, and every number above is taken from it.
