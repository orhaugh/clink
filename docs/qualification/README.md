# Production qualification

Benchmarks say how fast an engine is; they say nothing about whether its
guarantees hold when processes die at the worst possible instant. This
section publishes the results of clink's production-qualification
campaigns: long-running workloads on real multi-host clusters with faults
injected continuously into the narrowest windows of the engine's protocols,
judged by an independent oracle that recomputes expected results from a
seed rather than trusting anything the engine reports about itself.

Two rules govern what appears here:

1. **Only completed campaigns with retained evidence are published.** Every
   number on these pages is taken from a specific run's retained artifacts
   (chaos schedule, verifier output, cluster logs, metrics snapshots).
2. **Every claim carries an honesty category:** *Demonstrated* (held under
   the stated battery), *Tested but bounded* (held, within stated bounds),
   *Architecturally supported but not qualified* (designed for, not yet
   campaigned), or *Unknown*.

## Campaigns

| Campaign | What it qualifies | Status |
|---|---|---|
| [QUAL-01: Kafka exactly-once](qual-01-kafka-exactly-once.md) | Exactly-once from Kafka source to transactional Kafka sink under sustained process, coordinator, broker and network faults | **PASS** (`qual01-20260820h`, engine `33f30e7`) |
| Postgres two-phase-commit sink | The same guarantee through the recoverable-commit sink family | Not yet campaigned |
| Object-store commits | Staged multipart commits to object storage under faults | Not yet campaigned |
| Large state | Checkpoint, restore and rescale behaviour at 100 GB+ of keyed state | Not yet campaigned |
| Steady-state resources | Memory and descriptor behaviour over multi-day quiet runs | Not yet campaigned |

Campaign pages are added to this table only when they conclude green with
useful results; the raw evidence behind each page is retained per run.
