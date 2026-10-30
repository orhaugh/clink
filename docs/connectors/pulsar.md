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
| `tls_ca` | No | (empty) | PEM file of the CAs to trust for a `pulsar+ssl://` service URL, in place of the default store. Checked at open. |
| `tls_verify_hostname` | No | `true` | `false` keeps the certificate-chain check but accepts a broker certificate that names another host. |

A `pulsar+ssl://` service URL connects over TLS. The client checks that each broker's certificate chains to a trusted CA (`tls_ca`, or the default store when it is empty) and that it names the host the client dials that broker by: the host in `service_url` for the first connection, and the address the lookup returns for each broker after that. libpulsar leaves the name check off unless asked, so the connector turns it on. A refused certificate surfaces as a `ConnectError` from `create_producer` or `subscribe`, and the client log says `Handshake failed: certificate verify failed`; the error clink raises says so and names `tls_verify_hostname=false`.

Source (`pulsar_source_string`) options:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `topic` | Yes | (none) | Topic to subscribe to. Construction throws if empty. |
| `subscription` | No | `clink` | Subscription name, shared across parallel subtasks (Shared subscription). |
| `subscription_type` | No | `shared` | `shared` lets parallel subtasks split the topic, and a restart is replayed by broker redelivery. `exclusive` makes the source the subscription's only consumer and replays a restart in topic order from the checkpointed position (see "Replay after a restart"). `exclusive` needs parallelism 1 and a non-partitioned topic; the factory refuses it above parallelism 1, and `open()` refuses a partitioned topic. |
| `subscribe_timeout_ms` | No | `60000` | `exclusive` only: how long `open()` keeps retrying a subscription a previous incarnation's consumer still holds, as it can while that incarnation is torn down. |
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

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

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

- Source: messages are received and held. The acknowledge is deferred to checkpoint COMMIT, not the barrier: `snapshot_offset` buckets the messages emitted before each barrier against that checkpoint id, `notify_checkpoint_complete` (driven from the cluster's `CommitCheckpoint` dispatch) queues a committed bucket for ack, and the next `produce()` turn issues `pulsar_consumer_acknowledge` for them - so a message is acked only after the checkpoint that captured it is globally durable. `notify_checkpoint_aborted` frees a bucket without acking, so an aborted checkpoint (or a crash before commit) leaves those messages for redelivery. All pulsar C calls stay on the `produce()` thread. With the default Shared subscription the cursor is durable server-side, so recovery needs no local state (`restore_offset` is a no-op), and parallel subtasks split the topic, each acking only its own messages; an Exclusive subscription also records its position at each barrier and seeks back to it on restore (see "Replay after a restart"). The receiver-queue size bounds the held-unacked buffer.
- Sink: records are published asynchronously for throughput; `flush()` / `on_barrier()` blocks until the broker has persisted every pending publish, and throws if any send failed so the job replays from the last checkpoint rather than dropping data. Replay can therefore duplicate messages (at-least-once). The SQL planner rejects `exactly_once`: Pulsar producer dedup would need a producer name plus sequence id, which is not wired in v1.

### Replay after a restart

A shared subscription (the default) keeps no position of its own. After a restart the broker delivers again whatever was not acknowledged, when it gets to it, which can be after newer messages, so the replay is complete but not in topic order. A job that matches rows in event-time order over such a source needs a watermark lag wide enough to take the redelivered messages back in.

An exclusive subscription replays in order. At each barrier the source records, as operator state, the id of the last message it emitted before the barrier (`pulsar.position`). A restore reads it back before `open()`, and `open()` seeks the subscription to it: the broker resets the cursor there and delivers everything after it again, in topic order, acknowledged or not (the seek excludes the recorded message itself, libpulsar's default). A message at or before the recorded one that still arrives, which the restored checkpoint already covers, is acknowledged and not emitted. The seek needs the broker to still hold the messages after the recorded one: a restore from the newest checkpoint finds them, because they are acknowledged only once a later checkpoint completes, but a restore from an older point, a savepoint for instance, reaches back past messages a later checkpoint acknowledged, and those are kept only as long as the namespace's retention policy keeps acknowledged messages. `PulsarLive.AnExclusiveSubscriptionReplaysInOrderFromItsCheckpointedPosition` restores from a checkpoint older than the newest acknowledged one and holds the replay to the exact messages after it, in order.

The capability record declares the source's position model as `acknowledgement`, and clink's delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused, because a checkpoint acknowledged late is delivered again.

## Limitations

- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String channel only. Both factories are `std::string`; there is no int64 channel and no native columnar/Arrow path.
- Source and sink each handle a single topic; no topic patterns, glob or multi-topic subscription.
- Sink is at-least-once only. `exactly_once` and `mode='upsert'` are rejected by the SQL planner; no producer dedup (producer name + sequence id) is wired.
- Source acknowledges at checkpoint commit (not the barrier), so the earlier crash-window drop is closed; duplicates on replay remain the at-least-once trade-off. This relies on the cluster's per-checkpoint `CommitCheckpoint` dispatch, wired for both the default (non-fused) deployment and the opt-in par-1 chain fusion (`CLINK_PLAN_FUSE_PAR1=1`).
- Authentication is limited to an optional JWT token; no other auth mechanisms are exposed, and there is no TLS client certificate option.
- With `tls_ca` empty, the trusted CAs are those in the default store of the OpenSSL libpulsar carries, not necessarily the system's. The prebuilt Apache package in the build image points it at `/etc/ssl`.

## Testing

A factory-registration test (`impls/pulsar/tests/test_factory_registration.cpp`) checks that `install()` registers both string-channel factories. It runs without a broker.

Broker-certificate tests (`impls/pulsar/tests/test_pulsar_tls.cpp`) run without a broker too, in any build that finds OpenSSL. They open a sink against a local TLS listener (`tests/test_helpers/tls_handshake_server.hpp`), which counts the handshakes the client completes: libpulsar finishes the handshake before it sends anything, so a refused certificate never completes one. They cover a certificate naming another host (refused by default, with the error), one naming the host, the hostname opt-out, the opt-out still checking the chain, and a missing `tls_ca` file.

The factory test also holds the exclusive subscription to parallelism 1, and the source's constructor refuses an unknown `subscription_type`.

Live integration tests (`impls/pulsar/tests/test_pulsar_live.cpp`) drive the sink and source against a real broker: `PulsarLive.PublishThenConsumeRoundTrip`, and `PulsarLive.AnExclusiveSubscriptionReplaysInOrderFromItsCheckpointedPosition` (see "Replay after a restart"). They are skipped unless the environment variable `CLINK_PULSAR_TEST_ENDPOINT` is set to a Pulsar service URL, for example:

```bash
CLINK_PULSAR_TEST_ENDPOINT=pulsar://localhost:6650 ctest --test-dir build -L pulsar
```

The broker auto-creates the topic (`allowAutoTopicCreation`), so no provisioning is needed. The tests do not pin a specific broker container image; point `CLINK_PULSAR_TEST_ENDPOINT` at any reachable Pulsar broker (for example a standard `apachepulsar/pulsar` standalone instance).
