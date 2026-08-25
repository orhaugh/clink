# QUAL-11: changing a running job's state type

Clink migrates a running stateful job's KEYED STATE to a new shape
without losing a key or an event: savepoint the job, replace its
operator code with a version whose state type gained fields, and restore
through a registered migration. **74,000 keys judged, every one of them
holding exactly the count and sum the deterministic spec predicts - zero
missing, zero wrong, zero invented - across a boundary that a full fault
battery then ran on top of.** And the migration's own output is checked
where it actually lives: **10,000 keys read straight out of the
savepoint, every one carrying byte-for-byte what the registered
migration function is defined to write.**

| Provenance | |
|---|---|
| Campaign run | `qual11-20260825c`: 5-minute fill on v1, the evolution boundary, then a 30-minute aggressive battery on the migrated job |
| Engine | revision `ba680f9` throughout - ONE engine; the JOB is what changes |
| Job | a compiled plugin (`CLINK_REGISTER_JOB`), three variants built from one source at the image's own revision: v1, v2, and a deliberately broken v2 |
| State evolved | `AccountState` v1 `{count,sum}` (16 bytes) -> v2 `{count,sum,vmin,vmax}` (32 bytes), schema version 1 -> 2, with a registered v1->v2 migration |
| Rig | the standard 8-host cloud rig - 3 workers, coordinator, 3 brokers, operations host ([what that is](README.md#the-rig)); the savepoint on an NFS export every worker can read |
| Workload | 1,000 events/s over 4 partitions, 2,000 keys per 60-second epoch, 15-second checkpoints |

## What is being evolved, and what carries it

QUAL-08 qualified an engine swap with an unchanged state schema. This is
the other half: the engine stays put and the STATE TYPE changes. That is
the typed surface - `SchemaVersionTrait` stamps a version into every
snapshot, and `StateMigrationRegistry` holds the functions that bridge
versions - so the workload is a compiled job plugin rather than SQL,
whose state shapes are engine-internal.

All three job variants are built inside the image at its own revision,
because the plugin ABI fingerprint IS the engine's git SHA: a `.so`
built anywhere else is refused at submit by the gate that exists to
catch exactly that.

## Four gates, and why the second one matters most

1. **The pre-deploy check passed the job about to be deployed.**
   `clink check-savepoint --expected=<job.so>` runs the compatibility
   check *inside the `.so`*, because the migration registry is
   `.so`-local; the host dlopens, asks, and reports. A refusal stops the
   campaign before the deploy - it never pushes past a check that said
   no.

2. **The same check REFUSED a job that cannot migrate.** In the same
   run, against the same savepoint, a v2 built identically except that
   its migration is not registered was refused with exit 3. Without this
   the first gate proves nothing: a check that approves everything
   approves the good job too. This is the campaign's negative control,
   and it fired against a savepoint the engine itself had just written -
   not a synthetic version map.

3. **Continuity and exactness across the boundary.** The generator never
   stopped, so the oracle spans the swap: 74,000 keys, each one's final
   count and sum recomputed from the deterministic spec and matched
   exactly. Savepoint took 3 seconds, restore reached RUNNING in 3.

4. **The migration's effect, predicted and then measured in the state.**
   Both savepoints are dumped with `clink state-cat --json` and compared
   as raw bytes: every pre-boundary key must have a 32-byte entry whose
   count and sum did not regress (11,997 carried, 0 lost), and every key
   still *untouched* since the restore must hold precisely the
   empty-range sentinels the migration writes (10,000 of 10,000). That
   is the pure function's output, verified without asking the engine
   what it thinks it did.

The battery then ran entirely on the migrated job - coordinator SIGKILL
and restart, worker kills, broker restart, partitions and heals, network
latency, and the coordinator's checkpoint-marker crash windows armed,
fired and recovered - so the restored, migrated state is also the state
that survived faults.

## What this qualifies

- **Demonstrated:** a keyed-state type change on a running compiled job,
  restored through a registered migration with every key's history
  intact, the migration's exact output verified from the savepoint
  bytes, a pre-deploy check that refuses what it cannot migrate, and a
  full fault battery on the migrated job, at revision `ba680f9`.
- **Tested but bounded:** one migration step (v1->v2, additive fields)
  on one state slot, 30-minute battery, ~74,000 keys; a second,
  unchanged slot rode alongside and restored intact.
- **Architecturally supported but not qualified:** multi-step migration
  chains (the registry composes v1->v2->v3 by BFS), Arrow-aware
  automatic migration for additive nullable fields, and migration on a
  disaggregated state backend.
- **Unknown:** narrowing or type-changing migrations (this one is
  additive), migration combined with a rescale in the same boundary, and
  rollback to an older schema (the check refuses it, which is the
  designed behaviour, but a rollback path is not qualified).

## Caveats

- The claim is for the workload, migration shape and durations above.
- Re-emitted rows are expected and not judged: the sink is
  at-least-once and the restore replays, so the same (key, count) is
  re-emitted by design. Only the converged final state is judged for
  exactness.
- Ten local rig runs preceded this one and cost nothing but time; they
  drove one engine fix (the compiled-job submit path did not report the
  job id it created, which is the handle for savepoint and cancel) and a
  series of harness corrections. The engine's behaviour at the boundary
  was correct in every run where the harness measured it correctly.

Raw evidence (both savepoints' state dumps, the check-gate outputs for
both the good and the deliberately broken job, the chaos schedule, the
oracle's per-key verdict, coordinator and worker logs, and the image's
digest-verified provenance) is retained for this run and the local gate
that preceded it; every number above is taken from it.
