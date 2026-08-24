# QUAL-09: infrastructure faults - a full disk, a sustained partition, stepped clocks

Clink keeps exactly-once output when the infrastructure itself misbehaves:
the state volume filling to ENOSPC mid-run, a network partition held
longer than every recovery deadline, and host clocks stepped backwards by
tens of seconds - on top of the process-kill and 2PC crash-window battery
every earlier campaign ran. **3,384,700 events produced, 3,384,700 folded
exactly once, and 284,967 keys each holding exactly their seed-derived
count** - with zero missing, zero wrong, zero invented and zero NULL
counts, judged only after the pipeline had provably caught up.

| Provenance | |
|---|---|
| Campaign run | `qual09-20260824c`: 5-minute state fill, then a 45-minute `infra` battery, then quiesce, catch-up and end-state verification |
| Engine | revision `73b6565` (runtime image `sha-73b656549ed1-faultinj`, digest-verified provenance) |
| Rig | 8 dedicated cloud hosts: coordinator, 3 workers, 3 brokers, ops; state on an NFS export deliberately backed by a **4 GiB loopback volume** so ENOSPC is real, reachable and safe |
| Preceded by | two green local-rig gates and two failed cloud runs whose findings were fixed first (below) - the campaign publishes only a green run |
| Workload | the QUAL-05 retention shape: TTL'd `SELECT DISTINCT` into an unwindowed `GROUP BY`, 1,000 events/s over 4 partitions, key space turning over in 60-second epochs, `state_ttl='10m'`, 15-second checkpoints, upsert sink into Postgres |

## Coverage requires engagement, not just injection

An infrastructure fault that did not bite proves nothing, so this
campaign's verdict logic demands **engagement evidence** per fault, and a
fired-but-unengaged fault makes the whole run inconclusive:

- **Disk pressure**: the ops host's state volume was filled to ENOSPC and
  held there for ~110 seconds. Engagement = the engine's failed-checkpoint
  counter moved during the window. It did: checkpoints failed while the
  volume was full, and resumed when it was released.
- **Sustained partition**: a worker was firewalled off for ~7 minutes -
  past every recovery deadline, so the coordinator declared it lost,
  redeployed, and then had to handle the healed worker's return.
  Engagement = the workers-lost counter moved and the worker rejoined.
- **Clock step**: a worker host's clock was stepped backwards (twice,
  with the time-sync service stopped for the hold so the step actually
  held). Engagement = the offset measured from the ops host matched the
  applied step.

Alongside the infra matrix, the battery ran the standing workhorses:
worker SIGKILL (3x), coordinator SIGKILL + restart, and both
coordinator-side 2PC crash windows (`before_completed_marker`,
`after_completed_marker`), each armed, fired and recovered - 173 oracle
samples across the run, zero findings, and a clean quiesce.

## Bounded retention, proven from the disk

The 4 GiB sandbox is not just a fault target - it is the instrument that
makes unbounded growth terminal instead of silent. The campaign audits
retention **by walking the checkpoint directories at drain**, not by
reading an engine gauge: the deepest snapshot pile on disk was **3
snapshots against a configured retention of 3** - the retention sweep
holds storage exactly at the bound through kills, partitions and
ENOSPC-failed checkpoints.

## What this campaign forced out of the engine

Four engine defects were found, fixed and regression-locked by this
campaign's failing runs before the green run above - each invisible to
every prior campaign:

1. **Chain-path snapshots were never retained-bounded.** Subtask
   directories on the operator-chain path missed retention registration
   entirely; a bounded volume filled organically in ~40 minutes with the
   job's own snapshots (piles of 81 against retained=3). Fixed: the chain
   path registers its backends like every other path.
2. **Persistent checkpoint failure had no circuit-breaker.** Under
   permanent ENOSPC the job rewind-restarted ~100 times in 35 minutes,
   each rewind visibly shrinking upsert output. Fixed: consecutive
   failed-checkpoint restarts now terminate the job with the cause named.
3. **Purges rode the completion broadcast and leaked on every missed
   one.** A worker partitioned at completion time permanently orphaned
   those snapshots. Fixed: retention is self-healing - every broadcast
   also sweeps orphans below the retention cutoff, so the next broadcast
   heals whatever any missed one left.
4. **The circuit-breaker judged ticks, not time.** A 109-second transient
   ENOSPC window read as "persistent" and terminally killed a job that a
   rewind would have repaired - 37 seconds before the window released.
   Fixed: the terminal verdict requires the failure count AND a sustained
   wall-clock window (default 10 minutes); the green run above rode
   through the same ~110-second window and recovered.

Every fix carries a mutation-checked regression: the guard is disabled in
a compiled mutant and the test must go red before the fix counts.

## What this qualifies

- **Demonstrated:** exactly-once output and bounded, diagnosable recovery
  under ENOSPC on the state volume, a partition sustained past every
  recovery deadline, backwards clock steps, process kills and 2PC crash
  windows, at revision `73b6565` - with checkpoint storage held exactly
  at its configured retention, measured from the disk.
- **Tested but bounded:** one workload shape, 45-minute battery, ~65 MiB
  of live state on a 4 GiB volume; the partition was one worker for one
  hold; clock steps were backwards steps on one worker host.
- **Architecturally supported but not qualified:** the same matrix on
  disaggregated state backends; concurrent infrastructure faults
  (partition during ENOSPC); clock steps on the coordinator host.
- **Unknown:** disk corruption (as opposed to exhaustion), sustained
  partial packet loss, and kernel-level clock slew (as opposed to steps).

## Caveats

- Correctness is asserted for the workload, fault profile and durations
  above, on this rig.
- The two failed cloud runs that drove the fixes were judged by the same
  oracle: their failures were availability failures (a job filling its
  own volume; a job killed by its own circuit-breaker), never data
  failures - zero invented keys and zero NULL counts across every run of
  this campaign, passing and failing alike.
- The engagement gates are load-bearing: the first cloud run's clock step
  was silently corrected by the host's time-sync service within a second,
  and the gate refused it coverage credit rather than crediting a fault
  that proved nothing.

Raw evidence (chaos schedule with engagement records, oracle samples,
completeness and catch-up records, the retention audit, coordinator and
worker logs, and the image's digest-verified provenance) is retained per
run - for the green run and for the failing runs its fixes came from -
and every number above is taken from it.
