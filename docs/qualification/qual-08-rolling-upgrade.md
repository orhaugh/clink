# QUAL-08: a rolling engine upgrade with exactly-once continuity

Clink upgrades a running stateful job across engine revisions without
losing or duplicating a single event: savepoint on the old engine, swap
every cluster binary, restore on the new one - and an oracle that spans
the boundary, because the generator never stopped producing, counts every
event exactly once. **4,115,900 events produced across the whole run,
4,115,900 folded exactly once, and 344,997 of 344,997 keys holding
exactly their seed-derived count** - with the entire fault battery run on
the upgraded engine, so the restored state is also the state that
survived faults.

| Provenance | |
|---|---|
| Campaign run | `qual08-20260824a`: 15-minute fill on the old revision, the upgrade, then a 45-minute aggressive battery on the new one |
| Upgraded from | revision `bed138c` (runtime image `sha-bed138c3e94a-faultinj`) - the engine QUAL-05 qualified |
| Upgraded to | revision `3c7ffd1` (runtime image `sha-3c7ffd1cff9b-faultinj`) - the engine QUAL-06 qualified |
| Rig | the standard 8-host cloud rig - 3 workers, coordinator, 3 brokers, operations host - provisioned for this run and destroyed after it ([what that is](README.md#the-rig)) |
| Preceded by | a single-image machinery smoke on the local rig (exact across the boundary), which found and fixed three harness defects and one engine finding before any spend |
| Workload | the QUAL-05 retention shape: TTL'd `SELECT DISTINCT` into an unwindowed `GROUP BY`, 1,000 events/s, key space turning over in 60-second epochs, `state_ttl='10m'`, 15-second checkpoints |

## What carries the state across - and what does not

The upgrade never relies on binary compatibility. Clink's plugin ABI
fingerprint gates `dlopen` only - it is what makes STL types across the
plugin boundary safe, and a compiled job upgrades by recompiling against
the new engine. A SQL job ships no binary at all. What carries state
across a revision is the **savepoint contract**, made of three parts:

1. the snapshot format - canonical Arrow IPC, self-describing, with
   state-schema version stamps riding the metadata;
2. operator identity - the ids restore keys on, which for SQL are the
   planner's node ids. Because those are counters in plan order, every
   run of this campaign compiles the same script inside **both** images
   and diffs the ids before anything runs: a renumbering would silently
   orphan every restored slot. This pair: identical, `src_0` … `snk_9`.
3. migrate-at-restore - each state slot's declared schema version is
   checked, and `clink check-savepoint`, executed **inside the new
   image** so the verdict comes from the migration registry that will
   actually perform the restore, refuses any savepoint the new engine
   cannot migrate. A refused pair publishes nothing; the refusal is the
   finding. This pair: accepted.

## The upgrade, measured

| Step | Observed |
|---|---|
| Savepoint on the old engine (`clink savepoint`) | checkpoint 71, **2 seconds**, 68,515,072 bytes of live keyed state |
| Relocation to a portable directory | hard links, immediate - see the finding below |
| `check-savepoint` under the new image | accepted, every participant snapshot verified |
| Old job cancelled, every container swapped, HA store fresh | - |
| Resubmit with `--restore-from-dir` on the new engine | RUNNING in **2 seconds** |
| Restored state carried | first new-engine checkpoint held 68,517,696 bytes - the full savepoint |
| **Downtime** (savepoint complete → first checkpoint on the new engine) | **114 seconds**, dominated by container recreation across four hosts |

The consumer group is identical on both sides of the boundary: source
offsets ride the savepoint, so the restored job resumes from the exact
offsets the savepoint captured, and the events produced while the
cluster was down were read on the new engine - the exact accounting
above is over the *whole* stream, downtime included.

## What the upgraded engine then survived

| Fault | Count |
|---|---|
| Worker SIGKILL (with restart) | 2 |
| Network partition from the coordinator (healed) | 2 |
| Coordinator SIGKILL + restart | 2 |
| Broker restart | 1 |
| Injected network latency (cleared) | 3 |
| Injected packet loss (cleared) | 1 |
| Kill at a coordinator completion-marker point - fired and recovered | 3 |

209 independent oracle samples during the battery, zero findings,
quiesced before judgement.

## What preparing this campaign found

- **A savepoint's files could be garbage-collected one checkpoint
  interval after the handle was printed.** Snapshot retention keeps the
  newest checkpoint for some operators, so the moment the next periodic
  checkpoint completed, the savepoint's snapshots were unlinked - the
  first machinery smoke lost 4 of 10 subtask snapshots between savepoint
  and restore, and the restore correctly refused. The campaign driver
  now relocates the savepoint with hard links in the same breath as
  parsing the handle; a savepoint that pins itself against retention is
  tracked as the engine-side fix.
- The restore path demands both the directory **and** the checkpoint id;
  the directory alone restores nothing, which configuration lint says
  loudly - the driver encodes both.
- A restore that "succeeds" while carrying none of the savepoint's bytes
  is the silent failure mode; the campaign gates on the first
  new-engine checkpoint holding at least a stated fraction of the
  savepoint's size, so an empty restore can never pass as an upgrade.

## Claim boundaries

Following the programme's honesty categories:

- **Demonstrated:** exactly-once continuity across this exact revision
  pair (`bed138c` → `3c7ffd1`), for a SQL job of this shape, at the same
  topology and parallelism on both sides, with the measured savepoint,
  restore and downtime figures above.
- **Tested but bounded:** one upgrade hop, forward only; the state
  crossed unchanged in schema (this pair declares no state-schema
  version bump, so migrate-at-restore validated identity rather than
  performing a transform).
- **Architecturally supported but not qualified:** upgrades whose state
  schemas actually migrate (`SchemaVersionTrait` + the migration
  registry, unit-tested but not yet campaigned); compiled plugin jobs,
  which recompile against the new engine and restore the same way;
  multi-hop upgrade chains; rollback to the older revision.
- **Unknown:** upgrades that change operator topology or parallelism at
  the boundary (that is rescale-at-restore territory), and version pairs
  whose planner renumbers SQL operator ids - the pre-flight diff exists
  precisely because id stability is measured per pair, not proven.

## Caveats

- The claim is for this pair, workload, fault profile and duration; "any
  version upgrades to any version" is not claimed and never will be -
  each qualified pair is stated exactly.
- The downtime figure includes deliberate campaign choreography
  (retained log collection, an HA wipe between sides) and is an upper
  bound for this rig rather than a general promise.

Raw evidence (both sides' container logs, the savepoint and its
relocation, the check-savepoint transcript per participant, the op-id
diff, chaos schedule, oracle output, and the end-state seed verification)
is retained for the run, and every number above is taken from it.
