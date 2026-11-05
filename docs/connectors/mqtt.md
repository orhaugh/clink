# MQTT connector

> A source (`mqtt_source`, via SUBSCRIBE) and a sink (`mqtt_sink`, via PUBLISH) for MQTT brokers, exchanging string payloads. The sink publishes through libmosquitto; the source subscribes through its own MQTT 5 / 3.1.1 client, which acknowledges a message only once a completed checkpoint covers it.

## Overview

The MQTT connector integrates an MQTT broker (for example Mosquitto). The sink PUBLISHes each input record to a fixed topic; the source SUBSCRIBEs to a topic filter and emits each received message. Both operate on the `std::string` channel: the payload is treated as an opaque string and published or emitted verbatim, with no Arrow or Parquet encoding. The source can optionally wrap each message as a JSON envelope (`{"topic":...,"payload":...}`) so that a wildcard subscription can carry the per-message topic.

The two sides use different clients. The sink publishes over the libmosquitto synchronous client API (`mqtt_client.hpp`). The source subscribes through the impl's own client (`mqtt_subscriber.hpp`, `src/mqtt_subscriber.cpp`, with the packet codec in `mqtt_protocol.hpp`), because libmosquitto, like Paho, acknowledges a message as soon as it hands it over and cannot hold the acknowledgement until a checkpoint covers the message. The source's client speaks MQTT 5 by default and MQTT 3.1.1 when asked, over TCP or TLS.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| libmosquitto (the sink) | System package via apt (Debian, `libmosquitto-dev`) / brew (macOS, `mosquitto`) | Not pinned by clink |
| OpenSSL (the source's TLS) | System package | Not pinned by clink |

The sink's TLS is built into libmosquitto (OpenSSL): `tls=true` works whenever the installed libmosquitto was compiled with TLS support, and otherwise fails at connect time with libmosquitto's own error. The source's TLS needs OpenSSL found at build time; without it the source still builds, and `tls=true` fails at connect saying the build has no TLS. The connector does not link Arrow.

## Enabling it

The connector is gated by the CMake option `CLINK_WITH_MQTT`, with values `AUTO` (default), `ON`, or `OFF`.

- `AUTO`: built only if libmosquitto is found; otherwise the impl is silently skipped. The source needs no client library of its own, but builds as part of the impl.
- `ON`: configuration fails if libmosquitto is not found.
- `OFF`: the target is not defined.

CMake discovers libmosquitto three ways, most portable first: a pkg-config imported target (`libmosquitto.pc`, shipped by `libmosquitto-dev`), mosquitto's own CONFIG package, then a manual header/library probe (covering a Homebrew keg or a bare Linux install with no pkg-config tool). `scripts/install-connector-deps.sh` installs `libmosquitto-dev`, so the toolchain image builds the connector (CI's build of commit 6ad5168a on 2026-11-05 built `clink_mqtt_tests`). On a host, install the library first:

```bash
# Debian/Ubuntu
sudo apt-get install -y libmosquitto-dev
# macOS
brew install mosquitto

cmake -S . -B build -DCLINK_WITH_MQTT=ON
cmake --build build --parallel 10
```

The mosquitto 2.1 umbrella header pulls in `<cjson/cJSON.h>`; on Homebrew cJSON is a separate keg, so CMake also discovers that header path best-effort and adds it to the include path. Only the header is needed; libmosquitto links cJSON itself.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `mqtt_source` | Source | `std::string` |
| `mqtt_sink` | Sink | `std::string` |

## Configuration

Both factories share the connection options. `topic` is required on both sides; passing no `topic` causes the operator constructor to throw.

### Shared connection options (source and sink)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | No | `localhost` | Broker hostname. |
| `port` | No | `1883` | Broker port. |
| `keepalive` | No | `60` | Keepalive in seconds (clamped to a minimum of 5). |
| `username` | No | (empty) | Username for broker authentication; empty means no auth. |
| `password` | No | (empty) | Password for broker authentication. |
| `client_id` | No | `clink-mqtt-source` / `clink-mqtt-sink` | Client id base; the subtask index is appended so parallel subtasks hold distinct ids. |
| `tls` | No | `false` | `true` enables TLS encryption (the sink through libmosquitto's OpenSSL, the source through OpenSSL found at build time). Without `tls_ca` or `tls_capath`, the source verifies against the system's default CAs. |
| `tls_ca` | No | (empty) | CA certificate file (PEM) used to verify the server. |
| `tls_capath` | No | (empty) | Directory of CA certificates. |
| `tls_cert` | No | (empty) | Client certificate file for mutual TLS. |
| `tls_key` | No | (empty) | Client private key file for mutual TLS (the source reads the key from `tls_cert` when this is empty). |
| `tls_verify` | No | `true` | `false` skips server-certificate and hostname verification (self-signed dev only). |

### `mqtt_sink` options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `topic` | Yes | (none) | Topic to publish to. |
| `qos` | No | `1` | Publish QoS: 0 fire-and-forget, 1 at-least-once to broker, 2 once to broker. Must be 0, 1 or 2. |
| `retain` | No | `false` | Set the MQTT retain flag on each published message. |
| `clean_session` | No | `true` | `false` keeps a persistent broker session for the sink's client id. |
| `batch_records` | No | `1000` | Flush after this many buffered records (0 is treated as 1). |
| `max_bytes` | No | `0` | Flush after this many buffered payload bytes; 0 disables the byte threshold. |
| `linger_ms` | No | `0` | Flush a partial batch this many milliseconds old; 0 disables linger. |
| `ack_timeout_ms` | No | `30000` | Bound on the per-flush wait for QoS>0 acknowledgements. |

### `mqtt_source` options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `topic` | Yes | (none) | Topic filter to subscribe to. |
| `qos` | No | `1` | Subscription QoS. Must be 0, 1 or 2. |
| `shared_group` | No | (empty) | Non-empty subscribes to `$share/<group>/<topic>` so the broker load-balances across parallel subtasks. Empty means only subtask 0 is active. |
| `include_topic` | No | `false` | `true` emits each message as `{"topic":...,"payload":...}` JSON; otherwise the payload is emitted verbatim. |
| `block_ms` | No | `500` | Longest wait for a message in one `produce()` call, in milliseconds (bounds cancellation latency); a value of 0 or less is reset to 500. |
| `protocol_version` | No | `5` | `5` (MQTT 5) or `3.1.1`. A broker that does not speak MQTT 5 refuses the connection, and the error names this option. |
| `receive_maximum` | No | `10000` | MQTT 5: how many QoS 1 and 2 messages the broker may send unacknowledged, 1 to 65535. The source acknowledges only at checkpoint completion, so this is the most one subtask reads per checkpoint interval. Not sent under MQTT 3.1.1. |
| `session_expiry_s` | No | `4294967295` (never) | MQTT 5: how long the broker keeps the session after its connection ends, at least 1. A session that ends with its connection would deliver nothing again. |

The source always holds a persistent session (clean session or clean start off) under its client id; it has no `clean_session` option, since its recovery is the broker delivering again what the session holds unacknowledged.

Authentication-related options are `username` and `password` (broker auth) and the `tls_*` group (TLS, including `tls_cert`/`tls_key` for mutual TLS and `tls_verify`).

All options are read from the `BuildContext` parameters in `impls/mqtt/src/register_factories.cpp`. The defaults are mirrored in the `MqttSinkOptions` and `MqttSourceOptions` structs and the `ConnectOptions` struct in `impls/mqtt/include/clink/mqtt/`.

## SQL usage

Not exposed through the SQL frontend; use the programmatic API. There is no `connector='mqtt'` mapping in `src/sql/physical_plan.cpp`.

## Example

Construct the sink and source directly with their option structs (the pattern used by the connector's tests in `impls/mqtt/tests/test_mqtt_live.cpp`). The example publishes a few records and reads them back from the same topic.

```cpp
#include "clink/mqtt/mqtt_sink.hpp"
#include "clink/mqtt/mqtt_source.hpp"

using clink::mqtt::MqttSink;
using clink::mqtt::MqttSinkOptions;
using clink::mqtt::MqttSource;
using clink::mqtt::MqttSourceOptions;

// Sink: publish records to a topic at QoS 1 (at-least-once to the broker).
MqttSinkOptions sink_opts;
sink_opts.conn.host = "localhost";
sink_opts.conn.port = 1883;
sink_opts.topic = "clink/demo";
sink_opts.qos = 1;
MqttSink sink(std::move(sink_opts));
sink.open();
clink::Batch<std::string> batch;
batch.emplace(R"({"i":0})");
batch.emplace(R"({"i":1})");
sink.on_data(batch);
sink.flush();   // waits until every QoS>0 message is acked by the broker
sink.close();

// Source: subscribe to the same topic and emit each message verbatim.
MqttSourceOptions src_opts;
src_opts.conn.host = "localhost";
src_opts.conn.port = 1883;
src_opts.topic = "clink/demo";
src_opts.qos = 1;
MqttSource src(std::move(src_opts));
src.open();
// src.produce(emitter) reads what arrives within block_ms and emits it. In a
// checkpointing job each message is acknowledged once a completed checkpoint
// covers it; run on its own like this, nothing is acknowledged, and the
// session delivers everything again on the next connection.
```

Via the registry, the factories are reached as `mqtt_sink` and `mqtt_source` on the `string` channel, with `host`, `port`, `topic`, `qos` and the other keys above supplied as `BuildContext` params (see `impls/mqtt/src/register_factories.cpp` and `impls/mqtt/tests/test_factory_registration.cpp`).

## Delivery semantics

Source. MQTT has no position a source could store, so recovery is the broker's: the source holds a persistent session under a stable per-subtask client id, and when the session reconnects the broker delivers again every QoS 1 or 2 message it holds unacknowledged. The source therefore acknowledges a message only once a completed checkpoint covers it. `snapshot_offset` seals what arrived since the last barrier as that checkpoint's (through `CheckpointAckLedger`, persisting nothing), `notify_checkpoint_complete` makes it ready, and the next `produce()` or `close()` sends the PUBACKs, in the order the messages arrived, as MQTT requires. At QoS 2 the deferred packet is the PUBREC, which is what hands the message over; the PUBREL that follows is answered with PUBCOMP at once. A crash before the acknowledgements go out leaves the messages with the session, and they are delivered again. The record (`mqtt_source`) declares `OffsetModel::Acknowledgement`, and the delivery analyser reports a job over the source as `AT_LEAST_ONCE_SOURCE`: a sink may declare `at_least_once`, and `exactly_once` is refused.

Two rules come from MQTT itself. An acknowledgement goes out only on the connection its message arrived on: after a reconnect the broker sends every unacknowledged message again, under packet identifiers it may since have reused, so what the source still holds from an earlier connection is never acknowledged on a later one, and the copy delivered again is acknowledged instead. And MQTT has no negative acknowledgement, so an aborted checkpoint reconnects: only a new connection gets the broker to deliver that checkpoint's messages again. A message after the aborted ones is not acknowledged on the old connection either, since that would break the order. Both deliver again messages a later checkpoint may already cover, so the guarantee is at least once. `close()` acknowledges what completed checkpoints cover and leaves the rest with the session.

How much one checkpoint interval reads is the session's in-flight window, since the broker sends at most that many messages unacknowledged. Under MQTT 5 the source states it as its Receive Maximum (`receive_maximum`, default 10,000). Under MQTT 3.1.1 the broker's own setting decides: mosquitto's `max_inflight_messages` is 20 unless configured, so at 10-second checkpoints a subtask reads about two messages a second. While acknowledgements wait, the broker queues what it cannot yet send, and a broker that bounds that queue drops what overflows it: mosquitto's `max_queued_messages` defaults to 1,000. Measured against mosquitto 2.1.2 in its default configuration, an MQTT 3.1.1 subscriber holding its acknowledgements received 20 of 3,000 messages, and once it reconnected and acknowledged everything, 1,040 came back; the broker logged that it was dropping the rest. Size `receive_maximum`, and the broker's queue limits, for what arrives in one checkpoint interval.

Under MQTT 5 the source subscribes with Retain Handling 1, so retained messages come only with a subscription that is new: a source that resumes its session does not receive a retained message again. MQTT 3.1.1 has no such option, and a restart receives the retained message again.

Recovery stands on the session. A session the broker no longer holds, because it expired or because a broker that does not persist sessions restarted, takes what it held unacknowledged with it; on such a reconnect the source logs that the session was gone and subscribes again. The session never expires by default (`session_expiry_s`), so a job that is deleted leaves its sessions queueing messages on the broker until they are removed there. With `shared_group` set, a subtask's session also holds its messages when the subtask goes, and no remaining subtask receives them, so the coordinator refuses to rescale the source to fewer subtasks; the broker may keep those messages until the session expires. Without `shared_group` only subtask 0 subscribes, the subtasks a scale-down removes hold no session, and the source can be scaled down.

Sink. At QoS 1 (the default) and QoS 2 the sink is at-least-once end to end. `flush()` (called on the batch thresholds, on linger, and on every checkpoint barrier) waits until every buffered message is acknowledged, PUBACK at QoS 1 and PUBCOMP at QoS 2, so everything buffered is durable in the broker by the barrier. A flush that fails after some messages have reached the broker is replayed from the last checkpoint, re-publishing those messages, so duplicates are possible and must be absorbed downstream. At QoS 0 the sink is at-most-once: the broker acknowledges nothing, so the flush returns once the messages are written to the connection, and a message lost with a connection that drops after that is gone while the checkpoint completes. On a flush error the sink drops and rebuilds its connection on re-open to avoid a desynced in-flight window.

The sink keeps its connection while it has nothing to publish. A broker drops a client it has heard nothing from for one and a half keepalives, and libmosquitto sends the keepalive's ping from its network loop, which the sink used to run only to flush a batch: a sink that published rarely, an alert's, was dropped after ninety quiet seconds at the default keepalive and failed the job at its next flush. Every checkpoint barrier and every flush with nothing to send now services the connection without blocking, so a checkpointing job's sink stays connected however long it idles. A connection that drops while nothing is in flight on it, with no barrier within the keepalive or after a broker restart, loses nothing: the sink reconnects, awaiting the broker's CONNACK, before it publishes the next batch, and only a reconnect that fails fails the job.

## Limitations

- The source is at-least-once at QoS 1 and 2: messages a completed checkpoint covers whose acknowledgements had not gone out are delivered again after a crash, and an aborted checkpoint or a lost connection delivers again everything the session holds unacknowledged. At QoS 0 nothing is acknowledged and nothing delivered again.
- A restore from a checkpoint older than one the source acknowledged (commit-confirmed restore, a savepoint) does not see that checkpoint's messages again.
- One checkpoint interval reads at most the session's in-flight window, and while acknowledgements wait a broker that bounds its queue drops what overflows it (see above).
- With `shared_group` set, the source cannot be rescaled to fewer subtasks.
- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String channel only: payloads are opaque `std::string` values, published and emitted verbatim. There is no Arrow or Parquet encoding and no typed (for example int64) channel.
- The sink publishes to a single fixed `topic`; there is no per-record topic routing.
- `include_topic=true` assumes UTF-8 text payloads; for binary payloads use the default verbatim mode.
- Source parallelism: by default (no `shared_group`) only subtask 0 subscribes and emits, with other subtasks dormant, so it is best run at parallelism 1. Spreading load across subtasks requires `shared_group`, which in turn requires a broker that supports shared subscriptions (`$share/...`).
- No exactly-once end-to-end: the sink is at-least-once at QoS 1 and 2 and at-most-once at QoS 0, and so is the source.
- The source's client has no WebSocket transport and no MQTT 5 enhanced authentication (AUTH).
- Retained-message clearing and other broker housekeeping are the operator's responsibility, not the connector's.

## Testing

A live integration test, `impls/mqtt/tests/test_mqtt_live.cpp`, is skipped unless an environment variable points it at a real broker:

- `CLINK_MQTT_TEST_URL` (for example `mqtt://localhost:1883`) enables the plaintext tests. `MqttLive` covers a sink-to-source round-trip delivering every record verbatim, the `include_topic` JSON envelope, retained-message delivery to a fresh subscriber, and the idle sink at a five-second keepalive: `AnIdleSinkStaysConnectedAcrossItsBarriers` idles it for twelve seconds with a barrier every half second and requires its next batch to go out without a reconnect, and `ASinkDroppedWhileIdleReconnectsForItsNextBatch` idles it as long with no barrier, so the broker drops it, and requires the next batch to reconnect once and arrive. `MqttSourceAckLive` is the source's recovery contract, judged by what a later source on the same session receives: a crash before any completion and one after a completion, an aborted checkpoint, `close()`, QoS 2, the window under MQTT 5 (`receive_maximum`) and under MQTT 3.1.1 (mosquitto's default of 20), and a resumed session that does not receive a retained message again.
- `CLINK_MQTT_TLS_TEST_URL` (a `host:port` pointing at a TLS-enabled broker) enables the TLS round-trip test. `CLINK_MQTT_TLS_CA` may supply the CA certificate (PEM); without it, verification is skipped (self-signed dev cert: encrypt but do not authenticate).

`docker/integration-services.yml` defines an `mqtt` service (`eclipse-mosquitto:2` on port 1883). `scripts/ack-sources-live.sh` runs `MqttLive` and `MqttSourceAckLive` against a one-off container of it, with the other acknowledging sources, and fails on a skipped case or a short suite; the `ack-sources` CI workflow runs that script. The TLS case is left out there, since nothing provides the TLS broker it needs. `ONLY=mqtt scripts/run-all-live.sh` runs the live suites by hand inside the toolchain image.

In-process tests run without a broker. `test_mqtt_protocol.cpp` holds each packet the source's client sends, byte for byte, and the malformed ones it refuses; `test_mqtt_source_ack.cpp` runs the source against an in-process broker (`fake_mqtt_broker.hpp`) that records every acknowledgement in the order it came, covering the same cases as `MqttSourceAckLive` plus a message sent again on the same connection, a refused connection and the keepalive's ping. `test_factory_registration.cpp` checks the registrations, both records and the analyser's verdicts, and `test_mqtt_sink_logic.cpp` / `test_mqtt_source_logic.cpp` exercise the sink and source options. The test executable is `clink_mqtt_tests`, labelled `mqtt`:

```bash
ctest --test-dir build -L mqtt
```
