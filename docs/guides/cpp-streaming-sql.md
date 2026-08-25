---
title: Streaming SQL in C++ - Clink
description: Run streaming SQL from a native C++23 engine with event time, windows, joins, state, exactly-once checkpoints, and Kafka, database, and object-store connectors.
---

# Streaming SQL in C++

Clink brings streaming SQL into a native C++23 runtime. A SQL statement is compiled into a continuously running operator graph rather than executed as a one-shot database query.

The SQL frontend uses the PostgreSQL grammar through `libpg_query` and adds streaming concepts including event-time windows and `MATCH_RECOGNIZE`.

## Example

```sql
CREATE TABLE bids (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  event_time BIGINT
) WITH (
  connector='kafka',
  format='json',
  topic='bids',
  event_time_column='event_time',
  watermark_lag_ms='2000'
);

SELECT auction, SUM(price)
FROM bids
GROUP BY auction;
```

Clink validates state-retention requirements for operations that can retain unbounded state and supports explicit TTL controls where appropriate.

## Event time and state

Streaming queries can use watermarks, tumbling/sliding/session windows, joins, aggregations, CEP and stateful operators. Checkpoints capture operator state and replayable source positions for recovery.

See the full [SQL reference](../sql.md), [time and windowing internals](../internals/time-and-windowing.md), and [checkpointing](../internals/checkpointing.md).

## Connectors from SQL

Depending on the build, SQL tables can bind directly to Kafka, Pulsar, RabbitMQ, NATS, PostgreSQL, MySQL, ClickHouse, Redis, S3/Parquet, Iceberg, Kinesis and HTTP-oriented systems. The runtime exposes a capability manifest describing what the current binary actually contains.

See [Connectors](../connectors/README.md).

## Native embedding

SQL execution does not require a JVM service beside the application. Clink can run the pipeline in-process, expose results through its C ABI and Arrow interfaces, or submit the same pipeline to a distributed Clink cluster.

For the runtime model, see [C++ stream processing engine](cpp-stream-processing.md).
