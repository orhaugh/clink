---
title: Clink - C++23 Stream Processing Engine
description: Clink is an embedded-first, Arrow-native C++23 stream processing engine with streaming SQL, event time, exactly-once checkpoints, stateful operators, distributed execution, and production connectors.
---

# Clink - C++23 Stream Processing Engine

`clink` is an embedded-first, Arrow-native stream processing engine in modern
C++ (C++23): stateful stream processing with engine-grade semantics - SQL,
event time, keyed state, exactly-once checkpoints - that you run like a tool
rather than operate like a platform.

The whole engine lives in one library, and the same pipeline runs two ways.
In-process, `clink run pipeline.sql` executes it in a single process with no
daemons; `libclink` embeds the engine in any service behind a pure-C ABI, and
`pyclink` returns results as pyarrow tables. At scale, the same SQL file,
unchanged, submits to a distributed Coordinator/Worker cluster with
parallelism, failover, and rescale.

New here? [Your first real Clink pipeline](tutorials/kafka-to-clickhouse.md)
runs Kafka, clink and ClickHouse on your machine, kills the Worker while data
is flowing, and verifies the recovery independently. About ten minutes.

If you are evaluating stream processing in C++, start with the focused guides:

- [C++ stream processing engine](guides/cpp-stream-processing.md)
- [Streaming SQL in C++](guides/cpp-streaming-sql.md)
- [Clink vs Apache Flink](guides/clink-vs-flink.md)
- [Arrow-native stream processing](guides/arrow-stream-processing.md)

This site is the deep reference for the engine, organised in three layers:

## What it can do

The [capability catalogue](capabilities.md) is the complete, shipped feature
surface in one page: execution model, SQL, state, delivery guarantees,
operations, observability, and embedding APIs, each row linking to the page
that documents it in depth.

## What it costs to run

[Benchmarks](benchmarks.md) publishes the measured cost of processing an event across
the whole 17-query nexmark suite on a five-node cluster at parallelism 12: **1.9x to
5.3x less CPU per event than a JVM stream processor** (median 2.45x), every query in
clink's favour, on a correctness-gated comparison where both engines produce identical
output - and on stateless work the whole engine runs in 184 MB against the JVM engine's
gigabytes. It is explicit about where the advantage narrows: on dedup and ranking most
of the memory is the query's own retained state, and the CPU lead there is 1.9x rather
than 5x. The raw per-run output is published alongside it.

[Cost and environmental footprint](efficiency.md) prices those measurements - instance
counts, dollars, modelled energy and CO2e - with every coefficient named and swappable,
and is explicit that no wall power was measured, so no kWh or CO2e figure is a
measurement.

## How it works

The [internals references](internals/README.md) explain every subsystem the
way its code is actually structured: the operator model, task lifecycle,
network stack, state backends and snapshot format, checkpointing, the SQL
frontend, columnar execution, and more. Each page cites the sources it
describes.

## Why it is built this way

The [design decisions](design/index.md) record the reasoning behind the
choices that shape everything else - the Arrow-native data plane, the
embedded-first execution model, state as open data, deterministic replay -
including the trade-offs accepted and the consequences that followed.

## Elsewhere

- [Connector catalogue](connectors/README.md): every source and sink, with
  dependencies, factory names, options, and SQL usage.
- [Tutorial](tutorials/kafka-to-clickhouse.md): Kafka to clink to ClickHouse
  on one machine, with a deliberate Worker kill and independent verification.
- [Runnable examples](consumer-examples/README.md): buildable programs from
  hello-pipeline to the testing framework and state-as-data workflows.
- [Repository](https://github.com/orhaugh/clink) and
  [operations console](https://github.com/orhaugh/clink-fe).
