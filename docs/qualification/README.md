# Production qualification

Benchmarks say how fast an engine is; they say nothing about whether its
guarantees hold when processes die at the worst possible instant. These
pages publish the results of clink's production-qualification campaigns:
long-running workloads on real multi-host clusters with faults injected
continuously into the narrowest windows of the engine's protocols, judged
by an independent oracle that recomputes expected results from a seed
rather than trusting anything the engine reports about itself.

Two rules govern what appears here:

1. **Only completed campaigns with retained evidence are published.** Every
   number on these pages is taken from a specific run's retained artifacts
   (chaos schedule, verifier output, cluster logs, metrics snapshots, the
   image's digest-verified provenance).
2. **Every claim carries an honesty category:** *Demonstrated* (held under
   the stated battery), *Tested but bounded* (held, within stated bounds),
   *Architecturally supported but not qualified* (designed for, not yet
   campaigned), or *Unknown*.

## Qualified capabilities

| Capability | Campaign | Qualified |
|---|---|---|
| Exactly-once from Kafka source to transactional Kafka sink, under process, coordinator, broker and network faults | [QUAL-01](qual-01-kafka-exactly-once.md) | 20 August 2026, engine `33f30e7` |
| Exactly-once into PostgreSQL through two-phase commit, including database outages during recovery | [QUAL-02](qual-02-postgres-two-phase-commit.md) | 21 August 2026, engine `2e55943` |
| Exactly-once into S3-compatible object storage through staged multipart commits, including store outages during recovery | [QUAL-03](qual-03-s3-staged-commits.md) | 22 August 2026, engine `625cc82` |
| Tens of GiB of keyed state on a disaggregated backend, every key correct under process, coordinator, broker and network faults | [QUAL-04](qual-04-large-keyed-state.md) | 23 August 2026, engine `6ba73b5` |
| Bounded state through declared retention: an 8.3M-event run under faults holding a flat 67 MiB plateau with every key exact | [QUAL-05](qual-05-ttl-steady-state.md) | 23 August 2026, engine `bed138c` |
| Wide job graphs: 147 operators as 292 network-bridged subtasks, exactly once under faults, with 28-second recovery from a worker kill at that width | [QUAL-06](qual-06-dag-scaling.md) | 24 August 2026, engine `3c7ffd1` |
| A rolling engine upgrade (`bed138c` → `3c7ffd1`) with exactly-once continuity: 2s savepoint, 2s restore, 114s downtime, every event across the boundary counted once | [QUAL-08](qual-08-rolling-upgrade.md) | 24 August 2026 |

## Not yet campaigned

Keyed state at the 100 GB+ tier, multi-day steady-state resource
behaviour, and retention on the disaggregated backends are architecturally
supported and covered by the engine's test suites, but have not yet been
through a campaign of this kind. QUAL-04 qualifies keyed state at 29 GiB
and QUAL-05 qualifies bounded state over 90 minutes; the larger tier and
longer durations are separate campaigns. Pages are added to the
table above only when a campaign concludes with useful results and
retained evidence.
