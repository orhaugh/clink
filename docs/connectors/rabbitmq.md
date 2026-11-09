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

Connection parameters are shared by the source and sink and parsed by `parse_conn` (`impls/rabbitmq/src/register_factories.cpp`). They, the source's options and the sink's are read strictly: a number outside its range, or a boolean other than `true` or `false`, fails the job's build rather than reading as the default. Releases up to and including v0.15.0 read the source's own options leniently: `prefetch='1.5'` read as 1, `batch_size='10x'` as 10, a typo as the default, and `declare_queue='False'` declared the queue.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | No | `localhost` | Broker host. |
| `port` | No | `5672` | Broker port, 1 to 65535. |
| `vhost` | No | `/` | AMQP virtual host. |
| `user` | No | `guest` | SASL PLAIN login user. |
| `password` | No | `guest` | SASL PLAIN login password. |
| `heartbeat_s` | No | `60` | AMQP heartbeat interval in seconds (`0` disables), up to 65535. |
| `connect_timeout_ms` | No | `10000` | The longest the TCP connect, and then the AMQP handshake and login, each wait, 1 to 300,000 ms. |

Source options (`rabbitmq_source_string`, parsed in `register_factories.cpp`; struct in `impls/rabbitmq/include/clink/rabbitmq/rabbitmq_source.hpp`):

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `queue` | Yes | none | Queue to consume from. Construction throws if empty. |
| `consumer_tag` | No | `""` | Consumer tag; empty means the broker assigns one. |
| `declare_queue` | No | `true` | If true, `queue.declare` (durable) runs on open; idempotent. Set `false` to skip. `true` or `false`. |
| `prefetch` | No | `10000` | `basic.qos` prefetch count: the most unacknowledged messages the broker hands the source. None is acknowledged before its checkpoint completes, so this is the most a subtask reads per checkpoint. 1 to 65535. |
| `poll_timeout_ms` | No | `100` | Consume wait per `produce()` turn, in milliseconds. 1 to 600000. |
| `batch_size` | No | `256` | Maximum messages emitted per `produce()` turn. At least 1. |

Sink options (`rabbitmq_sink_string`, parsed in `register_factories.cpp`; struct in `impls/rabbitmq/include/clink/rabbitmq/rabbitmq_sink.hpp`):

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `exchange` | No | `""` | Target exchange. Empty means the default direct exchange. |
| `routing_key` | Yes | none | Routing key. On the default exchange this is the queue name. Construction throws if empty. |
| `persistent` | No | `true` | `true` publishes with delivery mode 2 (survives a broker restart on a durable queue); `false` with delivery mode 1. |
| `mandatory` | No | `true` | `true` publishes mandatory, so a message no queue is bound to receive is returned and fails the flush; `false` lets the exchange drop such messages without an error. |
| `content_type` | No | `application/json` | AMQP `content-type` property set on each published message. |
| `confirm_timeout_ms` | No | `30000` | The longest a flush, or a wait at `max_unconfirmed`, waits for the broker's confirms, 1 to 3,600,000 ms; past it the sink fails with the unconfirmed messages' outcome unknown. |
| `max_unconfirmed` | No | `10000` | The most messages awaiting confirms at once, 1 to 1,000,000; at the bound the sink waits for confirms before it publishes more. |
| `max_record_bytes` | No | `16777216` | A longer record fails the sink before it is published; 1 byte to 512 MiB. |

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

The planner rejects `mode='upsert'` and exactly-once delivery for the sink (RabbitMQ has no producer dedup key), reporting the at-least-once nature in the error message. A sink table's options are checked when the job is planned (`check_rabbitmq_sink_options`): one the sink does not read is refused, naming those it reads, so a misspelt `confirm_timeout` no longer leaves the default in force. The source's options pass, so one table can serve as both.

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

## The sink

The sink (`impls/rabbitmq/src/rabbitmq_sink.cpp`) publishes through a publisher that keeps librabbitmq to itself (`impls/rabbitmq/src/rabbitmq_publisher.cpp`, behind `clink/rabbitmq/rabbitmq_publisher.hpp`), so its failure paths can be tested against a scripted broker.

### Publishing and confirms

Each record is published as it arrives, on a channel in publisher-confirm mode (`confirm.select`), persistent and mandatory unless set otherwise, to `exchange` with `routing_key`. Every checkpoint barrier waits until the broker has confirmed each message published before it, so a completed checkpoint covers only confirmed messages. A confirm means the broker took responsibility for the message: written to disk for a persistent message on a durable queue, accepted by a majority of its replicas on a quorum queue. A transient message, or one on a queue that is not durable, can be lost on a broker restart after its confirm.

The broker confirms out of order: a queue refuses a publish at once (`x-overflow=reject-publish`) but confirms a persistent one only once it is on disk. A flush therefore reads every confirm before it decides, and a failure says what became of each message published since the last complete flush: confirmed, refused (`basic.nack`), returned unroutable, or unconfirmed, whose outcome is unknown.

### Failures

- A nack or reject fails the flush once the rest are settled, naming how many were refused.
- A mandatory message no queue is bound to receive comes back as a `basic.return` ahead of its confirm, and the flush fails with the exchange, the routing key and the broker's reply (`312 NO_ROUTE`), because a confirm alone does not mean the message reached a queue. With `mandatory='false'` the exchange drops such a message and still confirms it, so it is lost without an error; use that only where publishing to nowhere is intended.
- The broker closing the channel (an exchange that does not exist answers `404 NOT_FOUND`) or the connection fails the flush with the broker's reply code and text.
- A lost connection, and confirms that have not all come within `confirm_timeout_ms`, leave the unconfirmed messages unknown, and they are only that: the live suite holds the network past the timeout and finds the broker enqueued them once it carried again.

Any failure fails the sink: every later record, barrier and flush is refused, an empty flush included, until the sink is opened again, which the job's restart does. The restart publishes again what its last checkpoint did not cover. `close()` does not wait for unconfirmed messages, closes the channel and connection with the close handshake (each step waits at most 2 s) when the sink is healthy and by closing the socket when it failed or its task was cancelled, and never throws; a clean end of input flushes before it.

### Bounds and cancellation

The TCP connect, and then the AMQP handshake and login, each wait at most `connect_timeout_ms`, for the source too. At most `max_unconfirmed` messages wait for confirms at once: at the bound the sink waits for confirms, within `confirm_timeout_ms`, before it publishes more. A record longer than `max_record_bytes` fails the sink before it is published.

librabbitmq writes a publish with no deadline, so a broker that stops reading the connection (a memory or disk alarm blocks publishing connections this way) would hold a publish for as long as the alarm lasts. When the task's cancel is requested during a publish or a wait, a watch thread shuts the connection's socket, and the publish or wait ends at once.

## Delivery semantics

Both ends are at-least-once.

Source: messages are consumed with manual ack (`no_ack=false`), and the ack waits for the checkpoint that covers a message to complete. `snapshot_offset()` seals the delivery tags emitted since the last barrier as that checkpoint's, `notify_checkpoint_complete()` (from the cluster's `CommitCheckpoint` dispatch, which only moves tags between lists) makes every interval at or below the completed id ready, and the next `produce()` turn, or `close()`, acknowledges them through `CheckpointAckLedger` (`include/clink/connectors/checkpoint_ack_ledger.hpp`): one `basic.ack(multiple=true)` when the ready tags follow on from the last acknowledged one, tag by tag when an aborted interval lies below them. `notify_checkpoint_aborted()` releases its interval: never acknowledged on that channel, it is redelivered once the channel closes. It used to be dropped from the source's records only, and a later checkpoint's cumulative `basic.ack(multiple=true)` acknowledged it after all. `close()` acknowledges what completed checkpoints cover and nothing else, so a stop's final checkpoint is not redelivered and an uncommitted tail is; it used to acknowledge nothing. A crash before completion, or between a completion and its ack, leaves the messages unacknowledged for redelivery (duplicates). All AMQP calls stay on the `produce()` thread (the connection is not thread-safe). AMQP has no seekable offset, so recovery relies on broker redelivery rather than offset replay.

The broker hands the consumer at most `prefetch` unacknowledged messages, and none is acknowledged before its checkpoint completes, so a subtask reads at most `prefetch` messages per checkpoint: with the old default of 256 and 10 s checkpoints, about 25 messages a second. `RabbitMqSourceAckLive.TheDefaultPrefetchIsWhatOneCheckpointCanRead` measures it against a broker. The default is now 10,000, and a job that completes no checkpoint reads that many and waits.

Sink: see [the sink](#the-sink). It is at-least-once: RabbitMQ has no producer dedup key, so a replay publishes its messages again, and exactly-once is rejected by the SQL planner.

The capability record declares the source's position model as `acknowledgement`, and clink's delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused, because a checkpoint acknowledged late is delivered again.

## Limitations

- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String channel only; no int64 or Arrow-columnar channel.
- Plain TCP only; no TLS in this version.
- A confirm is the broker's responsibility for a message, not a consumer's receipt; a transient message, or one on a queue that is not durable, can be lost on a broker restart after its confirm.
- The sink is at-least-once: a lost connection or a confirm timeout leaves messages whose outcome is unknown, and the restart publishes them again.
- Authentication is SASL PLAIN (`user` / `password`); no other mechanisms.
- At-least-once on both ends; the sink does not support exactly-once (`delivery_guarantee='exactly_once'`) or `mode='upsert'` (rejected at SQL bind time).
- The source acknowledges at checkpoint completion, so a crash between a completion and its ack delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged (commit-confirmed, a savepoint) does not see that checkpoint's messages again.
- `prefetch` caps what a subtask reads per checkpoint (see Delivery semantics).
- The AMQP connection is not thread-safe; the source serialises `basic.ack` with `basic.consume` on the single connection, and the ack runs on the `produce()` thread.
- The commit-time ack relies on the cluster's per-checkpoint `CommitCheckpoint` dispatch, wired for both the default (non-fused) subtask deployment and the opt-in par-1 chain fusion (`CLINK_PLAN_FUSE_PAR1=1`).

## Testing

Offline tests in `impls/rabbitmq/tests/test_rabbitmq.cpp` cover required-parameter validation and clean failure against a dead endpoint, and `test_factory_registration.cpp` checks that both factories are registered, that the source's and the sink's factories read their options strictly, and the record the analyser reads. `RabbitMqSinkFailures` (`test_rabbitmq_sink_failures.cpp`) drives the sink against a scripted broker: confirms in ranges, a nack, a returned message, a closed channel, a lost connection and a confirm timeout, each followed by a record, a barrier and a flush; reopening; a cancel during a wait for confirms, during a publish the broker does not read, and before a publish; `close()`; and the record and unconfirmed bounds. These need no broker.

The live tests in `impls/rabbitmq/tests/test_rabbitmq_live.cpp` are skipped unless `CLINK_RABBITMQ_TEST_ENDPOINT` is set (the broker host). Port, user and password come from `CLINK_RABBITMQ_TEST_PORT`, `CLINK_RABBITMQ_TEST_USER` and `CLINK_RABBITMQ_TEST_PASSWORD` (defaults `5672` / `guest` / `guest`). `RabbitMqLive` covers the round trip and the sink's refusals. `RabbitMqSourceAckLive` holds the source to its acknowledgement contract, judged by what a fresh consumer of the queue receives once the source has gone: an aborted checkpoint, a crash after a completion and before its ack, intervals and notifications out of order, an aborted interval below a completed one, close after a cancel, the prefetch bound, and the default prefetch measured. The aborted-interval and close cases failed against the source before this contract, and pass.

`RabbitMqSinkContractLive` (`test_rabbitmq_sink_contract_live.cpp`) holds the sink to its contract against the broker, read through an AMQP connection of the test's own: messages exactly as written with their properties, a thousand messages within `max_unconfirmed=10`, a full queue refusing three of five publishes (the nacks arriving before the acks), two unroutable messages returned, an exchange that does not exist, an oversized record, and, through a TCP proxy that blackholes the connection, a confirm timeout whose messages the broker enqueues once the network carries again, a cancel during a wait and during a publish the broker no longer reads, and a handshake that stalls. With the watch off, the cancelled publish did not return before a 20 s limit.

The `RabbitMqSinkRecovery` cells (`tests/integration/test_rabbitmq_sink_recovery.cpp`) run a coordinator and two `clink_node` workers polling a feed the test serves into the sink, with worker 0 killed at `rabbitmq.sink_before_confirm` (a barrier's messages published and none of their confirms read) and at `rabbitmq.sink_after_confirm` (every message confirmed and the barrier not acknowledged), and a SQL job at parallelism 2. They read the queue through RabbitMQ's management API: every record is in it exactly as written, and the records in it twice are exactly those published after the checkpoint the job restored and before the kill.

`scripts/brokers-live.sh` runs the source's suites in its sources phase and the sink's suites and cells in its sinks phase, against the `rabbitmq` service of `docker/integration-services.yml` (`rabbitmq:3-management-alpine`, user and password `clink`), as a one-off container on free loopback ports, and fails on a skipped case, a short suite, or a case of `clink_rabbitmq_tests` that neither phase ran; the `brokers` CI workflow runs it.

```bash
ctest --test-dir build -L rabbitmq          # offline cases; the live ones skip
scripts/brokers-live.sh build               # every broker connector, live
```
