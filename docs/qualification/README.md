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

## Not yet campaigned

Keyed state at the 100 GB+ tier, and multi-day steady-state resource
behaviour, are architecturally supported and covered by the engine's test
suites, but have not yet been through a campaign of this kind. QUAL-04
qualifies keyed state at 29 GiB; the larger tier is a separate campaign. Pages are added to the
table above only when a campaign concludes with useful results and
retained evidence.
