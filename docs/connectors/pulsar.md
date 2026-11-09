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
| `operation_timeout_s` | No | `30` | Per-operation timeout in seconds, 1 to 3600. It also bounds how long the sink's `close()` waits for the broker. |
| `tls_ca` | No | (empty) | PEM file of the CAs to trust for a `pulsar+ssl://` service URL, in place of the default store. Checked at open. |
| `tls_verify_hostname` | No | `true` | `false` keeps the certificate-chain check but accepts a broker certificate that names another host. `true` or `false` only. |

The connection options and the sink's options are read strictly: a number outside its range, or a boolean other than `true` or `false`, fails the job's build rather than reading as the default.

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
| `batching` | No | `true` | `true` batches messages in the producer for throughput; `false` sends each on its own. |
| `send_timeout_ms` | No | `30000` | A message the broker has not answered this long fails in the client, 1 to 3,600,000 ms. `0`, which the client reads as no timeout, is refused. |
| `max_pending` | No | `1000` | The most messages awaiting the broker's answers at once, 1 to 500,000; past it the sink waits for answers before it publishes more. |
| `max_pending_bytes` | No | `67108864` | The most bytes of messages awaiting answers at once, up to 4 GiB. |

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

The sink rejects `mode='upsert'` and rejects `exactly_once`; it is at-least-once only (see Delivery semantics). A sink table's options are checked when the job is planned (`check_pulsar_sink_options`): one the sink does not read is refused, naming those it reads, so a misspelt `send_timeout` no longer leaves the default in force. The source's options pass, so one table can serve as both.

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

## The sink

The sink (`impls/pulsar/src/pulsar_sink.cpp`) publishes through a publisher that keeps the Pulsar C API to itself (`impls/pulsar/src/pulsar_publisher.cpp`, behind `clink/pulsar/pulsar_publisher.hpp`), so its failure paths can be tested against a scripted broker.

### Publishing and answers

Each record is handed to the producer as it arrives (`pulsar_producer_send_async`), batched unless `batching='false'`, and every checkpoint barrier sends what the producer batches and waits until the broker has answered each message published before it, so a completed checkpoint covers only persisted messages. Every message's answer reaches a callback, so the sink knows exactly how many were persisted and how many failed. The callback also frees the message id the C client allocates for each successful send, which the previous sink never freed: it leaked one for every message it published.

### Bursts

The client queues at most a set number of unanswered messages and, by default, refuses the rest at once (`ProducerQueueIsFull`) rather than wait. The previous sink handed every record to the client as it arrived, so a burst of more than 1,000 records faster than the broker answered failed the barrier, and the job's replay met the same queue: 20,000 records published at once failed with `ProducerQueueIsFull` against the pinned broker. The sink now keeps its own window, `max_pending` messages and `max_pending_bytes` of them: past either it sends what is batched and waits for answers before it publishes more, and it gives the client a queue twice the window, so the client's queue is never full.

### Failures

- A send the client fails, for a message larger than the broker takes (`MessageTooBig`), a refused topic, or no answer within `send_timeout_ms`, fails the flush once every message is answered, naming the client's result. A send the client failed on its timeout was on the wire, and the broker can still persist it: the live suite holds the network past the timeout and finds all three messages on the topic once it carries again.
- A wait for answers lasts at most `send_timeout_ms` and two seconds more, since the client fails each message at its own timeout; past it the sink fails with the unanswered messages' outcome unknown.

Each failure says how many of the messages published since the last complete flush were persisted, failed, and left unanswered. Any failure fails the sink: every later record, barrier and flush is refused, an empty flush included, until the sink is opened again, which the job's restart does. The restart publishes again what its last checkpoint did not cover. `close()` does not wait for answers itself and never throws; the client's close fails what is still unanswered and waits at most `operation_timeout_s` for the broker (two seconds against a silent broker with `operation_timeout_s=2`). A clean end of input flushes before it.

Waits for answers sleep at most 100 ms between looks at the task's cancel, so a cancelled task stops waiting at once.

## Delivery semantics

Both ends are at-least-once.

- Source: messages are received and held, and the acknowledge waits for the checkpoint that covers a message to complete. `snapshot_offset` buckets the messages emitted before each barrier against that checkpoint id, `notify_checkpoint_complete` (from the cluster's `CommitCheckpoint` dispatch) queues every bucket at or below the completed id for ack, and the next `produce()` turn, or `close()`, issues `pulsar_consumer_acknowledge` for them. `notify_checkpoint_aborted` frees a bucket without acking, so Pulsar redelivers it once the consumer closes. `close()` acknowledges what completed checkpoints cover and frees the rest unacknowledged, which Pulsar redelivers to the subscription's other consumers; it used to acknowledge nothing, so a stop's final checkpoint was delivered again. A crash before completion, or between a completion and its ack, leaves the messages for redelivery (duplicates). All pulsar C calls stay on the `produce()` thread. With the default Shared subscription the cursor is durable server-side, so recovery needs no local state (`restore_offset` is a no-op), and parallel subtasks split the topic, each acking only its own messages; an Exclusive subscription also records its position at each barrier and seeks back to it on restore (see "Replay after a restart"). The broker's per-consumer limit on unacknowledged messages (`maxUnackedMessagesPerConsumer`) bounds what is held.
- Sink: see [the sink](#the-sink). It is at-least-once: broker-side de-duplication needs a producer name and sequence ids, which the sink does not set, so a replay stores its messages again, and the SQL planner rejects `exactly_once`.

### Replay after a restart

A shared subscription (the default) keeps no position of its own. After a restart the broker delivers again whatever was not acknowledged, when it gets to it, which can be after newer messages, so the replay is complete but not in topic order. A job that matches rows in event-time order over such a source needs a watermark lag wide enough to take the redelivered messages back in.

An exclusive subscription replays in order. At each barrier the source records, as operator state, the id of the last message it emitted before the barrier (`pulsar.position`). A restore reads it back before `open()`, and `open()` seeks the subscription to it: the broker resets the cursor there and delivers everything after it again, in topic order, acknowledged or not (the seek excludes the recorded message itself, libpulsar's default). A message at or before the recorded one that still arrives, which the restored checkpoint already covers, is acknowledged and not emitted. The seek needs the broker to still hold the messages after the recorded one: a restore from the newest checkpoint finds them, because they are acknowledged only once a later checkpoint completes, but a restore from an older point, a savepoint for instance, reaches back past messages a later checkpoint acknowledged, and those are kept only as long as the namespace's retention policy keeps acknowledged messages. `PulsarLive.AnExclusiveSubscriptionReplaysInOrderFromItsCheckpointedPosition` restores from a checkpoint older than the newest acknowledged one and holds the replay to the exact messages after it, in order.

The capability record declares the source's position model as `acknowledgement`, and clink's delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused, because a checkpoint acknowledged late is delivered again.

## Limitations

- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String channel only. Both factories are `std::string`; there is no int64 channel and no native columnar/Arrow path.
- Source and sink each handle a single topic; no topic patterns, glob or multi-topic subscription.
- Sink is at-least-once only. `exactly_once` and `mode='upsert'` are rejected by the SQL planner; no producer dedup (producer name + sequence id) is wired.
- A send the client failed on its timeout may still have been persisted; the restart publishes it again.
- The sink's `close()` waits up to `operation_timeout_s` for a broker that does not answer.
- The source acknowledges at checkpoint completion, so a crash between a completion and its ack delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged (commit-confirmed, a savepoint) does not see that checkpoint's messages again (an Exclusive subscription's seek reaches back only as far as the broker still holds messages).
- Authentication is limited to an optional JWT token; no other auth mechanisms are exposed, and there is no TLS client certificate option.
- With `tls_ca` empty, the trusted CAs are those in the default store of the OpenSSL libpulsar carries, not necessarily the system's. The prebuilt Apache package in the build image points it at `/etc/ssl`.

## Testing

A factory-registration test (`impls/pulsar/tests/test_factory_registration.cpp`) checks that `install()` registers both string-channel factories, that the sink's factory reads its options strictly, and the record, whose authentication methods are now `none` and `token` only (it claimed passwords too). It runs without a broker. `PulsarSinkFailures` (`test_pulsar_sink_failures.cpp`) drives the sink against a scripted broker: a failed send, answers that do not come and a publish the client refuses, each followed by a record, a barrier and a flush; reopening; a cancel during a wait and before a publish; `close()`; the count and byte windows, which push the batch before they wait; and a zero send timeout refused.

Broker-certificate tests (`impls/pulsar/tests/test_pulsar_tls.cpp`) run without a broker too, in any build that finds OpenSSL. They open a sink against a local TLS listener (`tests/test_helpers/tls_handshake_server.hpp`), which counts the handshakes the client completes: libpulsar finishes the handshake before it sends anything, so a refused certificate never completes one. They cover a certificate naming another host (refused by default, with the error), one naming the host, the hostname opt-out, the opt-out still checking the chain, and a missing `tls_ca` file.

The factory test also holds the exclusive subscription to parallelism 1, and the source's constructor refuses an unknown `subscription_type`.

Live integration tests (`impls/pulsar/tests/test_pulsar_live.cpp`) drive the sink and source against a real broker: `PulsarLive.PublishThenConsumeRoundTrip`, and `PulsarLive.AnExclusiveSubscriptionReplaysInOrderFromItsCheckpointedPosition` (see "Replay after a restart"). They are skipped unless the environment variable `CLINK_PULSAR_TEST_ENDPOINT` is set to a Pulsar service URL, for example:

`PulsarSourceAckLive` holds the source to its acknowledgement contract, judged by what another consumer of the same Shared subscription receives once the source has gone: an aborted checkpoint, a crash after a completion and before its ack, notifications out of order, and close after a cancel. The close case failed against the source before this contract, and passes.

`PulsarSinkContractLive` (`test_pulsar_sink_contract_live.cpp`) holds the sink to its contract against the broker, read through a consumer of the test's own: messages persisted exactly as written, a burst of 20,000 messages (which failed with `ProducerQueueIsFull` before the window), a message too big for the broker beside two that are persisted, and, through a TCP proxy that blackholes the connection, sends failed on their timeout that the broker still persists, a cancel during a wait, and `close()` on a silent broker.

The `PulsarSinkRecovery` cells (`tests/integration/test_pulsar_sink_recovery.cpp`) run a coordinator and two `clink_node` workers polling a feed the test serves into the sink, with worker 0 killed at `pulsar.sink_before_ack` (a barrier's messages sent and the sink not yet waiting for their answers) and at `pulsar.sink_after_ack` (every message persisted and the barrier not acknowledged), and a SQL job at parallelism 2. They read the topic through a consumer of their own: every record is on it exactly as written, and the records on it twice are exactly those published after the checkpoint the job restored and before the kill.

`scripts/brokers-live.sh` runs the source's suites in its sources phase and the sink's suites and cells in its sinks phase, against the `pulsar` service of `docker/integration-services.yml` (`apachepulsar/pulsar:3.3.1` standalone), as a one-off container on free loopback ports, and fails on a skipped case, a short suite, or a case of `clink_pulsar_tests` that neither phase ran (the certificate cases aside, which need the openssl CLI). Before the first case it looks up 32 topics, so the broker has loaded every bundle of `public/default`: it reports healthy before it owns them, and a cold bundle once held a subscribe past the client's 30 s timeout. The broker runs with the image's memory settings, a 2 GiB heap touched in full at start and up to 4 GiB of direct memory, unless `CLINK_PULSAR_MEM` sets its `PULSAR_MEM`: on a machine whose Docker has less than that free, a smaller heap keeps the broker from stalling and from starving every other container, and the gate records the setting in its provenance. The `brokers` CI workflow runs it.

```bash
CLINK_PULSAR_TEST_ENDPOINT=pulsar://localhost:6650 ctest --test-dir build -L pulsar
scripts/brokers-live.sh build           # every broker connector, live
BROKERS_LIVE_ONLY=pulsar CLINK_PULSAR_MEM="-Xms512m -Xmx768m -XX:MaxDirectMemorySize=512m" \
  scripts/brokers-live.sh build         # Pulsar alone, with a smaller broker
```

The broker auto-creates the topic (`allowAutoTopicCreation`), so no provisioning is needed. The tests do not pin a specific broker container image; point `CLINK_PULSAR_TEST_ENDPOINT` at any reachable Pulsar broker (for example a standard `apachepulsar/pulsar` standalone instance).
