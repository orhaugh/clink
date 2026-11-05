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
| `prefetch` | No | `10000` | `basic.qos` prefetch count: the most unacknowledged messages the broker hands the source. None is acknowledged before its checkpoint completes, so this is the most a subtask reads per checkpoint. 1 to 65535. |
| `poll_timeout_ms` | No | `100` | Consume wait per `produce()` turn, in milliseconds. |
| `batch_size` | No | `256` | Maximum messages emitted per `produce()` turn. |

Sink options (`rabbitmq_sink_string`, parsed in `register_factories.cpp`; struct in `impls/rabbitmq/include/clink/rabbitmq/rabbitmq_sink.hpp`):

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `exchange` | No | `""` | Target exchange. Empty means the default direct exchange. |
| `routing_key` | Yes | none | Routing key. On the default exchange this is the queue name. Construction throws if empty. |
| `persistent` | No | `true` | Publish with delivery mode 2 (survives broker restart). Set `false` for delivery mode 1. |
| `mandatory` | No | `true` | Publish mandatory, so a message no queue is bound to receive is returned and fails the flush. Set `false` to let the exchange drop such messages without an error. |
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
  prefetch = '10000'
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

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

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

Source: messages are consumed with manual ack (`no_ack=false`), and the ack waits for the checkpoint that covers a message to complete. `snapshot_offset()` seals the delivery tags emitted since the last barrier as that checkpoint's, `notify_checkpoint_complete()` (from the cluster's `CommitCheckpoint` dispatch, which only moves tags between lists) makes every interval at or below the completed id ready, and the next `produce()` turn, or `close()`, acknowledges them through `CheckpointAckLedger` (`include/clink/connectors/checkpoint_ack_ledger.hpp`): one `basic.ack(multiple=true)` when the ready tags follow on from the last acknowledged one, tag by tag when an aborted interval lies below them. `notify_checkpoint_aborted()` releases its interval: never acknowledged on that channel, it is redelivered once the channel closes. It used to be dropped from the source's records only, and a later checkpoint's cumulative `basic.ack(multiple=true)` acknowledged it after all. `close()` acknowledges what completed checkpoints cover and nothing else, so a stop's final checkpoint is not redelivered and an uncommitted tail is; it used to acknowledge nothing. A crash before completion, or between a completion and its ack, leaves the messages unacknowledged for redelivery (duplicates). All AMQP calls stay on the `produce()` thread (the connection is not thread-safe). AMQP has no seekable offset, so recovery relies on broker redelivery rather than offset replay.

The broker hands the consumer at most `prefetch` unacknowledged messages, and none is acknowledged before its checkpoint completes, so a subtask reads at most `prefetch` messages per checkpoint: with the old default of 256 and 10 s checkpoints, about 25 messages a second. `RabbitMqSourceAckLive.TheDefaultPrefetchIsWhatOneCheckpointCanRead` measures it against a broker. The default is now 10,000, and a job that completes no checkpoint reads that many and waits.

Sink: messages are published persistent (delivery mode 2 by default) on a channel in publisher-confirm mode (`confirm.select`). `flush()` / `on_barrier()` block until the broker has confirmed every outstanding publish, and throw on a nack or timeout so the job replays from the last checkpoint rather than dropping data. Publishes are mandatory by default (`mandatory`, default `true`): a message that no queue is bound to receive comes back to the sink as a `basic.return`, and the flush fails with the exchange and routing key named, because a confirm alone does not mean the message reached a queue. With `mandatory='false'` the exchange drops such a message and still confirms it, so it is lost without an error; use that only where publishing to nowhere is intended. Before `mandatory` existed the sink always published that way. Replay can duplicate messages. RabbitMQ has no producer dedup key, so exactly-once is rejected by the SQL planner.

The capability record declares the source's position model as `acknowledgement`, and clink's delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused, because a checkpoint acknowledged late is delivered again.

## Limitations

- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String channel only; no int64 or Arrow-columnar channel.
- Plain TCP only; no TLS in this version.
- Authentication is SASL PLAIN (`user` / `password`); no other mechanisms.
- At-least-once on both ends; the sink does not support exactly-once (`delivery_guarantee='exactly_once'`) or `mode='upsert'` (rejected at SQL bind time).
- The source acknowledges at checkpoint completion, so a crash between a completion and its ack delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged (commit-confirmed, a savepoint) does not see that checkpoint's messages again.
- `prefetch` caps what a subtask reads per checkpoint (see Delivery semantics).
- The AMQP connection is not thread-safe; the source serialises `basic.ack` with `basic.consume` on the single connection, and the ack runs on the `produce()` thread.
- The commit-time ack relies on the cluster's per-checkpoint `CommitCheckpoint` dispatch, wired for both the default (non-fused) subtask deployment and the opt-in par-1 chain fusion (`CLINK_PLAN_FUSE_PAR1=1`).

## Testing

Offline tests in `impls/rabbitmq/tests/test_rabbitmq.cpp` cover required-parameter validation and clean failure against a dead endpoint, and `test_factory_registration.cpp` checks that both factories are registered. These need no broker.

The live tests in `impls/rabbitmq/tests/test_rabbitmq_live.cpp` are skipped unless `CLINK_RABBITMQ_TEST_ENDPOINT` is set (the broker host). Port, user and password come from `CLINK_RABBITMQ_TEST_PORT`, `CLINK_RABBITMQ_TEST_USER` and `CLINK_RABBITMQ_TEST_PASSWORD` (defaults `5672` / `guest` / `guest`). `RabbitMqLive` covers the round trip and the sink's refusals. `RabbitMqSourceAckLive` holds the source to its acknowledgement contract, judged by what a fresh consumer of the queue receives once the source has gone: an aborted checkpoint, a crash after a completion and before its ack, intervals and notifications out of order, an aborted interval below a completed one, close after a cancel, the prefetch bound, and the default prefetch measured. The aborted-interval and close cases failed against the source before this contract, and pass.

`scripts/ack-sources-live.sh` runs both suites against the `rabbitmq` service of `docker/integration-services.yml` (`rabbitmq:3-management-alpine`, user and password `clink`), as a one-off container on a free loopback port, and fails on a skipped case or a short suite; the `ack-sources` CI workflow runs it.

```bash
ctest --test-dir build -L rabbitmq          # offline cases; the live ones skip
scripts/ack-sources-live.sh build           # every acknowledging source, live
```
