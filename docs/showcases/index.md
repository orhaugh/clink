---
title: "Showcases: clink in seven real applications"
description: "Seven open-source applications built on clink in real domains, each checked against an independent model of what its outputs must hold, through failures injected on purpose."
---

# Showcases

Seven open-source applications built on clink, each in a real domain and each
complete: data goes in, decisions and tables come out, and a checker that never
touches clink says whether every output is right. They use clink the way its
users would, as an installed release, a runtime image or a build of its
binaries, and run whatever else they need in Docker, on one machine.

Start with the one closest to your problem.

| Showcase | The application | clink runs | Written in |
| --- | --- | --- | --- |
| [market-pulse](market-pulse.md) | Market data: price bars, spreads, patterns and a mistyped print | in one process, from Python and on a cluster | SQL, C++, Python |
| [card-sentry](card-sentry.md) | Card fraud detection, with alerts and decisions delivered exactly once | in one process and on a cluster | C++ |
| [grounded](grounded.md) | Retrieval for an operations assistant | in one process | SQL |
| [transfer-sentry](transfer-sentry.md) | Fraud detection on a transaction graph in Neo4j | on a cluster | SQL |
| [half-time](half-time.md) | A delivery platform on the night of a final | on a cluster with a standby Coordinator | SQL, C++ |
| [coldchain](coldchain.md) | Refrigerated freight monitoring | in one process and on a cluster | C++ |
| [playhead](playhead.md) | Video playback analytics | on Kubernetes, through the operator | SQL |

## Find one by what you want to see

| To see | Look at |
| --- | --- |
| clink in one process, with no cluster | [market-pulse](market-pulse.md), [grounded](grounded.md), [coldchain](coldchain.md), [card-sentry](card-sentry.md) |
| clink from Python | [market-pulse](market-pulse.md) |
| The typed C++ API | [coldchain](coldchain.md), [card-sentry](card-sentry.md), the operator in [market-pulse](market-pulse.md), the riders job in [half-time](half-time.md) |
| Many SQL jobs feeding each other | [half-time](half-time.md), [transfer-sentry](transfer-sentry.md), [playhead](playhead.md) |
| Event-pattern detection | [card-sentry](card-sentry.md) in C++; [market-pulse](market-pulse.md) and [half-time](half-time.md) with `MATCH_RECOGNIZE` |
| Exactly-once delivery through a Worker kill | [card-sentry](card-sentry.md), [transfer-sentry](transfer-sentry.md), [half-time](half-time.md), [playhead](playhead.md) |
| A Coordinator failover | [half-time](half-time.md), [playhead](playhead.md) |
| A job upgraded through a savepoint | [half-time](half-time.md), [playhead](playhead.md) |
| A running job rescaled | [half-time](half-time.md) |
| Kubernetes and the operator | [playhead](playhead.md) |
| A job scaled to zero and woken by new data | [playhead](playhead.md) |
| Change data capture | [half-time](half-time.md), from PostgreSQL and MySQL |
| Neo4j as a sink and a lookup | [transfer-sentry](transfer-sentry.md), [half-time](half-time.md) |
| Cloud storage and table formats | [playhead](playhead.md) with Iceberg, GCS and Azure; [half-time](half-time.md) with Parquet and Delta |
| SQL-native AI | [grounded](grounded.md) |
| Live state served over HTTP | [card-sentry](card-sentry.md) |
| A stopped job's state read as data | [market-pulse](market-pulse.md) |
| Incidents replayed byte for byte | [market-pulse](market-pulse.md), [card-sentry](card-sentry.md), [grounded](grounded.md), [half-time](half-time.md) |
| A run checked against the exactly-once specification | [card-sentry](card-sentry.md), [half-time](half-time.md) |
| Traces, metrics and lineage | [playhead](playhead.md) |
| What a run costs in CPU and memory | [half-time](half-time.md), [playhead](playhead.md) |

## How every showcase is checked

They share one discipline, which is what makes their results worth reading.

1. **A deterministic tape.** A generator writes the input from a seed, so every
   run sees the same events, out of event-time order the way real feeds are.
2. **Planted cases and negative controls.** The tape carries incidents that must
   be found and near-misses that must not be, at known coordinates.
3. **An independent checker.** It never reads the application's code or clink's
   state. It works out from the tape alone what each output must hold, reads
   every store back through a client of its own, and fails the run on a row too
   many or too few.
4. **Failure on purpose.** Most showcases kill a Worker mid-stream, and some fail
   over a Coordinator, upgrade a job or resend a batch a producer had already
   delivered. The checks must pass anyway: a store declared exactly once must
   hold nothing twice, and a store declared at least once is checked with its
   copies counted.

Each showcase's page follows the same order: what the application does, where
clink does the work, what a run proves, what is distinctive, and how to run it.
