# Apache Pulsar connector

> Source and sink for Apache Pulsar topics, carrying string-channel records over the libpulsar C API.

## Overview

The Pulsar connector integrates with Apache Pulsar brokers using the libpulsar C API (`pulsar/c/`), so no C++ ABI is shared with the prebuilt client library. It provides both a source and a sink on the `std::string` channel. The source subscribes to a topic and emits each message body as a string; the sink publishes each string record to a topic. When mapped through the SQL frontend, each message body is treated as a JSON object string (`json_string_to_row` on read, `row_to_json_string` on write). Authentication is via an optional JWT token.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Apache Pulsar C++ client (libpulsar) | Compiled/prebuilt from source at the pinned version: Homebrew `libpulsar` on the macOS host, the Apache prebuilt `apache-pulsar-client` + `-dev` `.deb` from archive.apache.org in the Debian image. Not in Debian apt. | 4.2.0 |

Arrow is not used by this connector; records ride the string channel.

## Enabling it

The connector is gated by the CMake option `CLINK_WITH_PULSAR` (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the build probes for `pulsar/c/client.h` and `libpulsar` in the usual prefixes (`/opt/homebrew`, `/usr/local`, `/usr`); if found the `clink::pulsar` target is defined, otherwise it is skipped. With `ON` a missing library is a hard configure error.

The build environment must provide libpulsar:

- Debian image: the Apache prebuilt `apache-pulsar-client` and `apache-pulsar-client-dev` `.deb` packages (installed by `scripts/install-connector-deps.sh`).
- macOS host: `brew install libpulsar`.

```bash
cmake -S . -B build -DCLINK_WITH_PULSAR=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `pulsar_source_string` | Source | `std::string` |
| `pulsar_sink_string` | Sink | `std::string` |

## Configuration

Connection parameters are shared by both factories; topic and per-direction options differ. Values are read from the `BuildContext` params (see `impls/pulsar/src/register_factories.cpp`); defaults come from the `Options` structs in `pulsar_source.hpp` / `pulsar_sink.hpp` and from `connection_params.hpp`.

Shared connection options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `service_url` | No | `pulsar://localhost:6650` | Pulsar broker service URL. |
| `token` | No | (empty) | Optional JWT for authentication (`pulsar_authentication_token`). Leave blank for no-auth. |
| `operation_timeout_s` | No | `30` | Per-operation timeout in seconds. |

Source (`pulsar_source_string`) options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `topic` | Yes | (none) | Topic to subscribe to. Construction throws if empty. |
| `subscription` | No | `clink` | Subscription name, shared across parallel subtasks (Shared subscription). |
| `receiver_queue_size` | No | `1000` | Prefetch size; bounds the held-unacked message buffer. |
| `receive_timeout_ms` | No | `1000` | Per `produce()` turn receive timeout in milliseconds (cancel latency). |
| `batch_size` | No | `256` | Maximum messages emitted per `produce()` turn. |

Sink (`pulsar_sink_string`) options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `topic` | Yes | (none) | Topic to publish to. Construction throws if empty. |
| `batching` | No | `true` | Producer batching for throughput. Any value other than `false` enables it. |
| `send_timeout_ms` | No | `30000` | Producer send timeout in milliseconds. |

## SQL usage

The connector is mapped in `src/sql/physical_plan.cpp` under `connector='pulsar'` for both source and sink. The source binds to `pulsar_source_string` with a `json_string_to_row` decoder; the sink binds to `pulsar_sink_string` with a `row_to_json_string` encoder. Each row maps to one JSON object string message body.

```sql
CREATE TABLE events (
    id    BIGINT,
    v     STRING
) WITH (
    connector = 'pulsar',
    service_url = 'pulsar://localhost:6650',
    topic = 'persistent://public/default/events',
    subscription = 'clink',
    token = ''
);
```

The sink rejects `mode='upsert'` and rejects `exactly_once`; it is at-least-once only (see Delivery semantics).

## Example

Programmatic round trip using the typed classes, based on `impls/pulsar/tests/test_pulsar_live.cpp`. Subscribe before publishing so the durable subscription cursor covers the messages.

```cpp
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/pulsar/pulsar_source.hpp"

using clink::pulsar::PulsarSink;
using clink::pulsar::PulsarSource;

PulsarSource::Options so;
so.conn.service_url = "pulsar://localhost:6650";
so.topic = "events";
so.subscription = "clink";
PulsarSource src(std::move(so));
src.open();

PulsarSink::Options ko;
ko.conn.service_url = "pulsar://localhost:6650";
ko.topic = "events";
PulsarSink sink(std::move(ko));
sink.open();

clink::Batch<std::string> out;
out.emplace(R"({"id":1,"v":"a"})");
sink.on_data(out);
sink.flush();  // blocks until the broker persists every pending publish; throws on failure
```

Through the registry, look up the factories `pulsar_source_string` and `pulsar_sink_string` on the string channel and pass the options above as `BuildContext` params.

## Delivery semantics

Both ends are at-least-once.

- Source: messages are received and held. The acknowledge is deferred to checkpoint COMMIT, not the barrier: `snapshot_offset` buckets the messages emitted before each barrier against that checkpoint id, `notify_checkpoint_complete` (driven from the cluster's `CommitCheckpoint` dispatch) queues a committed bucket for ack, and the next `produce()` turn issues `pulsar_consumer_acknowledge` for them - so a message is acked only after the checkpoint that captured it is globally durable. `notify_checkpoint_aborted` frees a bucket without acking, so an aborted checkpoint (or a crash before commit) leaves those messages for redelivery. All pulsar C calls stay on the `produce()` thread. The subscription cursor is durable server-side, so recovery needs no local state (`restore_offset` is a no-op). The default Shared subscription lets parallel subtasks split the topic, each acking only its own messages. The receiver-queue size bounds the held-unacked buffer.
- Sink: records are published asynchronously for throughput; `flush()` / `on_barrier()` blocks until the broker has persisted every pending publish, and throws if any send failed so the job replays from the last checkpoint rather than dropping data. Replay can therefore duplicate messages (at-least-once). The SQL planner rejects `exactly_once`: Pulsar producer dedup would need a producer name plus sequence id, which is not wired in v1.

## Limitations

- String channel only. Both factories are `std::string`; there is no int64 channel and no native columnar/Arrow path.
- Source and sink each handle a single topic; no topic patterns, glob or multi-topic subscription.
- Sink is at-least-once only. `exactly_once` and `mode='upsert'` are rejected by the SQL planner; no producer dedup (producer name + sequence id) is wired.
- Source acknowledges at checkpoint commit (not the barrier), so the earlier crash-window drop is closed; duplicates on replay remain the at-least-once trade-off. This relies on the cluster's per-checkpoint `CommitCheckpoint` dispatch, wired for both the default (non-fused) deployment and the opt-in par-1 chain fusion (`CLINK_PLAN_FUSE_PAR1=1`).
- Authentication is limited to an optional JWT token; no other auth mechanisms are exposed.

## Testing

A factory-registration test (`impls/pulsar/tests/test_factory_registration.cpp`) checks that `install()` registers both string-channel factories. It runs without a broker.

A live integration test (`impls/pulsar/tests/test_pulsar_live.cpp`, `PulsarLive.PublishThenConsumeRoundTrip`) drives the sink and source against a real broker. It is skipped unless the environment variable `CLINK_PULSAR_TEST_ENDPOINT` is set to a Pulsar service URL, for example:

```bash
CLINK_PULSAR_TEST_ENDPOINT=pulsar://localhost:6650 ctest --test-dir build -L pulsar
```

The broker auto-creates the topic (`allowAutoTopicCreation`), so no provisioning is needed. The test does not pin a specific broker container image; point `CLINK_PULSAR_TEST_ENDPOINT` at any reachable Pulsar broker (for example a standard `apachepulsar/pulsar` standalone instance).
