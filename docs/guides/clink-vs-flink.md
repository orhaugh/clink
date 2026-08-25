---
title: Clink vs Apache Flink - Native C++23 Stream Processing
description: Compare Clink and Apache Flink for stream processing: native C++23 embedding and Arrow state versus Flink's mature JVM ecosystem and production history.
---

# Clink vs Apache Flink

Clink is strongly influenced by Apache Flink's stream-processing model: typed operator graphs, event time, watermarks, keyed state, checkpoint barriers and recoverable state are foundational concepts in both systems.

They target different operating points.

## Clink's focus

Clink is designed to be embedded directly in native applications and to scale from one process to a distributed Coordinator/Worker cluster. It is written in C++23 and uses Apache Arrow as a first-class data representation, including open checkpoint and state surfaces.

That makes Clink attractive when the stream processor must be part of an existing native service, run with a small runtime footprint, or exchange Arrow data directly with native analytics and Python tooling.

## Flink's focus

Apache Flink is a mature distributed stream-processing platform with a large ecosystem, broad operational tooling, extensive connector coverage, and many years of production use at large scale.

For organisations that primarily need a battle-tested cluster platform and already accept a JVM-based stack, that production history is an important advantage.

## Execution model

Clink can execute SQL or DataStream-style pipelines in-process without standing up a cluster. The same pipeline model can also run distributed.

Flink is primarily operated as a distributed processing platform, although it also offers local execution modes for development and testing.

## State

Clink deliberately exposes state as open data: Arrow-based snapshots and state-serving interfaces are intended to make state inspectable outside the engine.

Flink's state architecture is considerably more mature and has been exercised on very large production workloads for years.

## SQL

Both projects support streaming SQL. Clink uses the PostgreSQL grammar through `libpg_query` plus streaming extensions; Flink provides a mature Table/SQL ecosystem with much broader field history.

See [Clink SQL](../sql.md) and [Streaming SQL in C++](cpp-streaming-sql.md).

## Delivery guarantees

Clink performs capability-aware delivery-guarantee analysis and has checkpoint-aware transactional and idempotent connector paths. Exactly-once guarantees depend on the complete source/operator/sink chain rather than a generic engine label.

Flink has a much longer track record operating exactly-once state and transactional sinks in production.

## Which should you choose?

Consider Clink when native embedding, Arrow-native state/data, a small deployment surface, or one engine spanning embedded and distributed execution are central requirements.

Consider Flink when ecosystem breadth, organisational familiarity, managed-service availability, and accumulated production mileage matter more than native embedding.

Clink is intentionally not presented as a blanket Flink replacement. See the [capability catalogue](../capabilities.md), [benchmarks](../benchmarks.md), and [production qualification material](../qualification/) for the evidence behind its current claims.
