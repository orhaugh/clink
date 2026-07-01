# RabbitMQ (AMQP 0-9-1)

> Source and sink for RabbitMQ over AMQP 0-9-1, consuming from a queue and publishing to an exchange.

## Overview

This connector integrates a RabbitMQ broker over the AMQP 0-9-1 protocol using the rabbitmq-c client library (librabbitmq). The source issues `basic.consume` against a queue and emits each message body as a `std::string`; the sink issues `basic.publish` of each `std::string` record to an exchange with a routing key. Only the string channel is supported. Message bodies are treated as opaque strings, which on the SQL path carry JSON objects (the planner bridges them to and from Row via `json_string_to_row` / `row_to_json_string`).

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| librabbitmq (rabbitmq-c) | System package via apt (Debian: `librabbitmq-dev`) / brew (macOS: `rabbitmq-c`) | Not pinned by clink |

The connector links no Arrow component directly; records move on the string channel.

## Enabling it

The build is gated by `CLINK_WITH_RABBITMQ` (`AUTO` / `ON` / `OFF`). With `AUTO`, the target is defined only if librabbitmq is discovered (via `pkg-config` `librabbitmq`, then a manual header/library probe). With `ON`, a missing librabbitmq is a hard configure error. With `OFF`, the target is not defined.

Install the client library first:

- Debian: `apt-get install librabbitmq-dev`
- macOS: `brew install rabbitmq-c`

Then configure:

```bash
cmake -S . -B build -DCLINK_WITH_RABBITMQ=ON
cmake --build build -j
```

When enabled, the impl defines `CLINK_HAS_RABBITMQ` and builds the static library `clink::rabbitmq`.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `rabbitmq_source_string` | Source | `std::string` |
| `rabbitmq_sink_string` | Sink | `std::string` |

## Configuration

Connection parameters are shared by the source and sink and parsed by `parse_conn` (`impls/rabbitmq/src/register_factories.cpp`).

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | No | `localhost` | Broker host. |
| `port` | No | `5672` | Broker port. |
| `vhost` | No | `/` | AMQP virtual host. |
| `user` | No | `guest` | SASL PLAIN login user. |
| `password` | No | `guest` | SASL PLAIN login password. |
| `heartbeat_s` | No | `60` | AMQP heartbeat interval in seconds (`0` disables). |

Source options (`rabbitmq_source_string`, parsed in `register_factories.cpp`; struct in `impls/rabbitmq/include/clink/rabbitmq/rabbitmq_source.hpp`):

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `queue` | Yes | none | Queue to consume from. Construction throws if empty. |
| `consumer_tag` | No | `""` | Consumer tag; empty means the broker assigns one. |
| `declare_queue` | No | `true` | If true, `queue.declare` (durable) runs on open; idempotent. Set `false` to skip. |
| `prefetch` | No | `256` | `basic.qos` prefetch count, bounding unacked messages in flight. |
| `poll_timeout_ms` | No | `100` | Consume wait per `produce()` turn, in milliseconds. |
| `batch_size` | No | `256` | Maximum messages emitted per `produce()` turn. |

Sink options (`rabbitmq_sink_string`, parsed in `register_factories.cpp`; struct in `impls/rabbitmq/include/clink/rabbitmq/rabbitmq_sink.hpp`):

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `exchange` | No | `""` | Target exchange. Empty means the default direct exchange. |
| `routing_key` | Yes | none | Routing key. On the default exchange this is the queue name. Construction throws if empty. |
| `persistent` | No | `true` | Publish with delivery mode 2 (survives broker restart). Set `false` for delivery mode 1. |
| `content_type` | No | `application/json` | AMQP `content-type` property set on each published message. |
| `confirm_timeout_ms` | No | `30000` | Time to wait for broker publisher confirms before failing, in milliseconds. |

Authentication uses SASL PLAIN with `user` and `password`. The connection is plain TCP only in this version; there is no TLS option.

## SQL usage

Mapped in `src/sql/physical_plan.cpp` under `connector='rabbitmq'`.

Source:

```sql
CREATE TABLE orders_in (
  id   BIGINT,
  v    STRING
) WITH (
  connector = 'rabbitmq',
  format = 'json',
  host = 'localhost',
  port = '5672',
  queue = 'orders',
  prefetch = '256'
);
```

Sink:

```sql
CREATE TABLE orders_out (
  id   BIGINT,
  v    STRING
) WITH (
  connector = 'rabbitmq',
  format = 'json',
  host = 'localhost',
  routing_key = 'orders',
  persistent = 'true'
);
```

The planner rejects `mode='upsert'` and exactly-once delivery for the sink (RabbitMQ has no producer dedup key), reporting the at-least-once nature in the error message.

## Example

Based on the round-trip in `impls/rabbitmq/tests/test_rabbitmq_live.cpp`, using the typed classes with their `Options`:

```cpp
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/rabbitmq/rabbitmq_source.hpp"

using clink::Batch;
using clink::rabbitmq::RabbitMqSink;
using clink::rabbitmq::RabbitMqSource;

// Source: declares + consumes the queue, emitting each body as a string.
RabbitMqSource::Options so;
so.conn.host = "localhost";
so.queue = "orders";          // required
RabbitMqSource src(std::move(so));
src.open();

// Sink: default exchange ("") routes by routing_key == queue name.
RabbitMqSink::Options ko;
ko.conn.host = "localhost";
ko.routing_key = "orders";    // required
RabbitMqSink sink(std::move(ko));
sink.open();

Batch<std::string> out;
out.emplace(R"({"id":1,"v":"a"})");
sink.on_data(out);
sink.flush();                 // blocks until the broker confirms; throws on failure
```

The same factories can be resolved by name through the runner registry, as `impls/rabbitmq/tests/test_factory_registration.cpp` does with `rabbitmq_source_string` and `rabbitmq_sink_string`.

## Delivery semantics

Both ends are at-least-once.

Source: messages are consumed with manual ack (`no_ack=false`). The ack is deferred to checkpoint COMMIT, not the barrier. `snapshot_offset()` records the highest delivery tag emitted before each barrier against that checkpoint id; `notify_checkpoint_complete()` (driven from the cluster's `CommitCheckpoint` dispatch) advances a safe-to-ack watermark, and the next `produce()` turn issues one `basic.ack(multiple=true)` up to it - so a message is confirmed to the broker only after the checkpoint that captured it is globally durable. `notify_checkpoint_aborted()` drops the pending record without acking, so an aborted checkpoint (or a crash before commit) leaves those messages for redelivery. All AMQP calls stay on the `produce()` thread (the connection is not thread-safe); the commit notification only advances an atomic watermark that `produce()` drains. AMQP has no seekable offset, so recovery relies on broker redelivery rather than offset replay.

Sink: messages are published persistent (delivery mode 2 by default) on a channel in publisher-confirm mode (`confirm.select`). `flush()` / `on_barrier()` block until the broker has confirmed every outstanding publish, and throw on a nack or timeout so the job replays from the last checkpoint rather than dropping data. Replay can duplicate messages. RabbitMQ has no producer dedup key, so exactly-once is rejected by the SQL planner.

## Limitations

- String channel only; no int64 or Arrow-columnar channel.
- Plain TCP only; no TLS in this version.
- Authentication is SASL PLAIN (`user` / `password`); no other mechanisms.
- At-least-once on both ends; the sink does not support exactly-once (`mode='exactly_once'`) or `mode='upsert'` (rejected at SQL bind time).
- The source acks at checkpoint commit (not the barrier), so the earlier drop-on-crash window is closed; duplicates on replay remain the at-least-once trade-off.
- The AMQP connection is not thread-safe; the source serialises `basic.ack` with `basic.consume` on the single connection, and the ack runs on the `produce()` thread.
- The commit-time ack relies on the cluster's per-checkpoint `CommitCheckpoint` dispatch, wired for both the default (non-fused) subtask deployment and the opt-in par-1 chain fusion (`CLINK_PLAN_FUSE_PAR1=1`).

## Testing

Offline tests in `impls/rabbitmq/tests/test_rabbitmq.cpp` cover required-parameter validation and clean failure against a dead endpoint, and `test_factory_registration.cpp` checks that both factories are registered. These need no broker.

The live round-trip in `impls/rabbitmq/tests/test_rabbitmq_live.cpp` is skipped unless `CLINK_RABBITMQ_TEST_ENDPOINT` is set (the broker host, for example `localhost`). Port, user and password come from `CLINK_RABBITMQ_TEST_PORT`, `CLINK_RABBITMQ_TEST_USER` and `CLINK_RABBITMQ_TEST_PASSWORD` (defaults `5672` / `guest` / `guest`). It publishes three messages with publisher confirms and consumes them back from a freshly declared queue.

Run the impl's tests (CTest label `rabbitmq`):

```bash
ctest --test-dir build -L rabbitmq
```

To run the live test, point it at a broker, for example a local container:

```bash
CLINK_RABBITMQ_TEST_ENDPOINT=localhost ctest --test-dir build -L rabbitmq
```

The test reads `CLINK_RABBITMQ_TEST_ENDPOINT` (host) plus the optional `_PORT` / `_USER` / `_PASSWORD` overrides; it does not pin a specific broker image.
