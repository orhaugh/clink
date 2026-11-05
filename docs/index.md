---
title: Clink - C++23 Stream Processing Engine
description: "Clink is an Arrow-native C++23 stream processing engine that stays correct when processes die: streaming SQL, event time, keyed state, exactly-once delivery and twenty-plus connectors, with the same pipeline on a single machine or across a cluster."
---

# Clink - C++23 Stream Processing Engine

`clink` is an Arrow-native stream processing engine in modern C++ (C++23)
that stays correct when processes die. You write a pipeline in SQL or typed
C++; clink keeps its event-time windows and keyed state, checkpoints them, and
delivers exactly once into Kafka, PostgreSQL, Iceberg and Parquet on S3, GCS or
Azure. Kill a Worker mid-stream and the job resumes from its last checkpoint
without losing or double-counting a record. Pipelines read from and write to
more than twenty systems, and each sink is held to the guarantee its table
declares.

The same pipeline runs unchanged on a single machine, inside a service or
across a cluster. `clink run pipeline.sql` executes it in one process with no
daemons; `libclink` embeds the engine in any service behind a pure-C ABI, and
`pyclink` returns results as pyarrow tables. Submitted to a Coordinator, the
same SQL file runs across its Workers with failover, live rescale and upgrades
through savepoints, and the cluster scales out by adding Workers. On
Kubernetes, a Helm chart or an operator runs it on a secured, highly available
cluster.

New here? [Your first real Clink pipeline](tutorials/kafka-to-clickhouse.md)
runs Kafka, clink and ClickHouse on your machine, kills the Worker while data
is flowing, and verifies the recovery independently. About ten minutes.

If you are evaluating stream processing in C++, start with the focused guides:

- [C++ stream processing engine](guides/cpp-stream-processing.md)
- [Streaming SQL in C++](guides/cpp-streaming-sql.md)
- [Clink vs Apache Flink](guides/clink-vs-flink.md)
- [Arrow-native stream processing](guides/arrow-stream-processing.md)

To see it in complete applications, the [showcases](showcases/index.md) are
seven open-source projects built on clink, each checked against an independent
model of what its outputs must hold.

This site is the deep reference for the engine, organised in three layers:

## What it can do

The [capability catalogue](capabilities.md) is the complete, shipped feature
surface in one page: execution model, SQL, state, delivery guarantees,
operations, observability, and embedding APIs, each row linking to the page
that documents it in depth. [Compatibility](compatibility.md) states which
of those surfaces the 1.x line promises to hold stable, and how each promise
is enforced.

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
