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
| Content-level agreement with an independent reference engine: 19 of 19 queries, byte-exact multisets and key-paired final states, under pre-declared judgement classes | [QUAL-07](qual-07-semantic-comparison.md) | 24 August 2026, engine `998e0ff` |
| A rolling engine upgrade (`bed138c` → `3c7ffd1`) with exactly-once continuity: 2s savepoint, 2s restore, 114s downtime, every event across the boundary counted once | [QUAL-08](qual-08-rolling-upgrade.md) | 24 August 2026, engine `3c7ffd1` (upgraded from `bed138c`) |
| Infrastructure faults: ENOSPC on the state volume, a 7-minute partition, stepped clocks - 3.38M events exactly once, checkpoint storage held at its configured bound | [QUAL-09](qual-09-infrastructure-faults.md) | 24 August 2026, engine `73b6565` |
| A running job's keyed-state TYPE changed and migrated at restore: 74,000 keys exact across the boundary, 10,000 verified byte-for-byte against the migration's own output | [QUAL-11](qual-11-schema-evolution.md) | 25 August 2026, engine `ba680f9` |
| Refusing to silently weaken a security posture: 15 declared refusals measured as declared, 5 against real SASL and TLS servers, with the control-plane wiring proven in a published image | [QUAL-12](qual-12-security-refusals.md) | 25 August 2026, engine `0ca5936` |

## The rig

Nine of the eleven campaigns ran on the same disposable cloud rig, so
their results are comparable and none of them is a bespoke arrangement
assembled to flatter a particular number. Eight hosts on Hetzner Cloud
(Falkenstein, Ubuntu 24.04), on a private network:

| Role | Hosts | Instance |
|---|---|---|
| clink workers | 3 | cpx32 |
| clink coordinator | 1 | cpx22 |
| Brokers - Redpanda 24.2.7 speaking the Kafka protocol, replication 3 | 3 | cpx22 |
| Operations host - generator, oracle, chaos controller, shared state | 1 | cpx32 |

The operations host sits outside the engine's failure domain deliberately.
The generator that produces the input, the oracle that judges the output
and the chaos controller that injects the faults must not be casualties
of the faults they are measuring, or a lost result becomes
indistinguishable from a lost record.

The rig is provisioned per run by `qualification/infra/provision.sh`,
labelled with the run id, and destroyed when the campaign ends;
`teardown.sh --check` then verifies nothing is left running or billing.
The host inventory for each run is retained with that campaign's
evidence, so the rig a result came from is recoverable rather than
remembered.

Two campaigns needed no rig, and their pages say so. QUAL-07 compares
clink against a reference engine on one machine, where a shared host is
the point rather than a compromise: both engines meet identical input.
QUAL-12's refusals are all process-start properties that manifest while
parsing arguments, before a socket is opened, so they need the binary
and its image rather than a cluster. Eight hosts would have observed the
same eight exits an hour later, for money.

Where a campaign needed something the standard rig does not provide, its
page states it: QUAL-09 put the shared state on an NFS export backed by
a deliberately small loopback volume, because an ENOSPC campaign needs a
disk that can genuinely fill.

## Not yet campaigned

Keyed state at the 100 GB+ tier, multi-day steady-state resource
behaviour, and retention on the disaggregated backends are architecturally
supported and covered by the engine's test suites, but have not yet been
through a campaign of this kind. QUAL-04 qualifies keyed state at 29 GiB
and QUAL-05 qualifies bounded state over 90 minutes; the larger tier and
longer durations are separate campaigns. Pages are added to the
table above only when a campaign concludes with useful results and
retained evidence.
