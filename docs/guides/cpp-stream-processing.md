---
title: C++ Stream Processing Engine — Clink
description: Use Clink as an embedded or distributed C++23 stream processing engine with event time, state, checkpoints, SQL, Arrow data, and production connectors.
---

# C++ Stream Processing Engine

Clink is a C++23 stream processing engine designed for applications that need more than callbacks over a message queue: event time, keyed state, checkpoints, replay, rescaling, SQL, and external connectors are part of the same runtime.

Unlike JVM-first stream processors, Clink can run directly inside a native process. The same engine can also execute a pipeline on a Coordinator/Worker cluster when a workload outgrows a single process.

## Why use C++ for stream processing?

A native runtime is useful when the streaming engine must sit inside an existing C++ service, run close to devices or gateways, avoid a managed VM, or exchange columnar data with native analytics code without serialization through a separate runtime.

Clink uses Apache Arrow as a first-class data representation and exposes state and results through Arrow-compatible interfaces. It supports row and columnar execution rather than treating Arrow only as an interchange format.

## Stateful processing

Clink provides keyed and operator state, event-time windows, interval joins, CEP, checkpointing, savepoints, state backends, failure recovery, and rescaling. See the [capability catalogue](../capabilities.md) and [state internals](../internals/state-and-backends.md).

## Embedded and distributed execution

An application can execute a pipeline in-process with `libclink`, through the C++ API, through the C ABI, or with the `clink run` command. The same SQL and operator model can also be submitted to a distributed Clink cluster.

See [embedded execution](../internals/embedded.md) and [distributed runtime](../internals/distributed-runtime.md).

## Streaming SQL

Clink includes a streaming SQL frontend based on the PostgreSQL grammar with event-time and streaming extensions. SQL pipelines can read and write Kafka, databases, object stores and other systems depending on which connector modules are linked into the runtime.

See [Streaming SQL in C++](cpp-streaming-sql.md) and the full [SQL reference](../sql.md).

## Connectors

The connector catalogue includes Kafka, Pulsar, RabbitMQ, NATS, PostgreSQL, MySQL, ClickHouse, Redis, Cassandra/ScyllaDB, S3, GCS, Azure, Iceberg, Kinesis and HTTP-oriented systems. Delivery guarantees differ by connector and are machine-readable through Clink's capability registry.

See the [connector catalogue](../connectors/README.md).

## When Clink is a good fit

Clink is aimed at native services, edge and industrial systems, low-footprint stream processing, Kafka and database ETL, event-time analytics, and applications that benefit from open Arrow state.

It is a young project rather than a replacement-by-default for mature platforms such as Apache Flink. The [benchmarks](../benchmarks.md), [internals](../internals/README.md), and qualification material document what has actually been measured and tested.
