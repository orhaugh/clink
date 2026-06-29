# MQTT connector

> A source (`mqtt_source`, via SUBSCRIBE) and a sink (`mqtt_sink`, via PUBLISH) for MQTT brokers, exchanging string payloads over libmosquitto.

## Overview

The MQTT connector integrates an MQTT broker (for example Mosquitto) over the libmosquitto synchronous client API. The sink PUBLISHes each input record to a fixed topic; the source SUBSCRIBEs to a topic filter and emits each received message. Both operate on the `std::string` channel: the payload is treated as an opaque string and published or emitted verbatim, with no Arrow or Parquet encoding. The source can optionally wrap each message as a JSON envelope (`{"topic":...,"payload":...}`) so that a wildcard subscription can carry the per-message topic. The client wrapper is shared by both sides and handles connection, authentication, optional TLS, and the CONNACK/SUBACK waits.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| libmosquitto | System package via apt (Debian, `libmosquitto-dev`) / brew (macOS, `mosquitto`) | Not pinned by clink |

TLS is built into libmosquitto (OpenSSL), so there is no separate SSL library to discover. `tls=true` works whenever the installed libmosquitto was compiled with TLS support; if not, the connection fails at connect time with libmosquitto's own error. The connector does not link Arrow.

## Enabling it

The connector is gated by the CMake option `CLINK_WITH_MQTT`, with values `AUTO` (default), `ON`, or `OFF`.

- `AUTO`: built only if libmosquitto is found; otherwise the impl is silently skipped.
- `ON`: configuration fails if libmosquitto is not found.
- `OFF`: the target is not defined.

CMake discovers libmosquitto three ways, most portable first: a pkg-config imported target (`libmosquitto.pc`, shipped by `libmosquitto-dev`), mosquitto's own CONFIG package, then a manual header/library probe (covering a Homebrew keg or a bare Linux install with no pkg-config tool). libmosquitto is not installed by the project's Docker build-env scripts, so to build the connector you install it yourself first:

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
| `tls` | No | `false` | `true` enables TLS encryption (libmosquitto built-in OpenSSL). |
| `tls_ca` | No | (empty) | CA certificate file (PEM) used to verify the server. |
| `tls_capath` | No | (empty) | Directory of CA certificates. |
| `tls_cert` | No | (empty) | Client certificate file for mutual TLS. |
| `tls_key` | No | (empty) | Client private key file for mutual TLS. |
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
| `block_ms` | No | `500` | libmosquitto loop wait in milliseconds (bounds cancellation latency); a value of 0 or less is reset to 500. |

The source forces `clean_session=false` (a persistent broker session is required for its recovery model and is not a user-tunable parameter).

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
// src.produce(emitter) drives one libmosquitto loop iteration per call,
// emitting any messages received during that iteration.
```

Via the registry, the factories are reached as `mqtt_sink` and `mqtt_source` on the `string` channel, with `host`, `port`, `topic`, `qos` and the other keys above supplied as `BuildContext` params (see `impls/mqtt/src/register_factories.cpp` and `impls/mqtt/tests/test_factory_registration.cpp`).

## Delivery semantics

Source. MQTT has no replayable offset or cursor, so the source persists no checkpoint offset (`snapshot_offset`/`restore_offset` are the no-op base defaults). Durability comes from the broker session: the source always runs with a stable per-subtask client id and a persistent session (`clean_session` is forced off by the factory), so the broker retains the subscription and queues QoS 1/2 messages for this client while it is disconnected, redelivering un-acked messages on reconnect. This gives at-least-once across a clean reconnect. libmosquitto auto-acks a QoS 1/2 message when the `on_message` callback returns, so a process crash after the callback but before the next checkpoint loses those in-flight messages, making delivery best-effort across a hard crash. A downstream idempotent or keyed-dedup consumer is the robust path.

Sink. End-to-end the sink is at-least-once regardless of QoS. `flush()` (called on the batch thresholds, on linger, and on every checkpoint barrier) waits until every buffered message is acknowledged for QoS>0, so everything buffered is durable in the broker by the barrier. A flush that fails after some messages have reached the broker is replayed from the last checkpoint, re-publishing those messages, so duplicates are possible and must be absorbed downstream. QoS 0 is fire-and-forget with no broker ack and is lossy on any drop; QoS 1 awaits PUBACK; QoS 2 awaits PUBCOMP. On a flush error the sink drops and rebuilds its connection on re-open to avoid a desynced in-flight window.

## Limitations

- String channel only: payloads are opaque `std::string` values, published and emitted verbatim. There is no Arrow or Parquet encoding and no typed (for example int64) channel.
- The sink publishes to a single fixed `topic`; there is no per-record topic routing.
- `include_topic=true` assumes UTF-8 text payloads; for binary payloads use the default verbatim mode.
- Source parallelism: by default (no `shared_group`) only subtask 0 subscribes and emits, with other subtasks dormant, so it is best run at parallelism 1. Spreading load across subtasks requires `shared_group`, which in turn requires a broker that supports shared subscriptions (`$share/...`).
- No exactly-once end-to-end: the sink is at-least-once at any QoS, and the source is at-least-once across clean reconnects but best-effort across a hard crash (no replayable offset).
- Retained-message clearing and other broker housekeeping are the operator's responsibility, not the connector's.

## Testing

A live integration test, `impls/mqtt/tests/test_mqtt_live.cpp`, is skipped unless an environment variable points it at a real broker:

- `CLINK_MQTT_TEST_URL` (for example `mqtt://localhost:1883`) enables the plaintext round-trip tests: a sink-to-source round-trip delivering every record verbatim, the `include_topic` JSON envelope, and retained-message delivery to a fresh subscriber.
- `CLINK_MQTT_TLS_TEST_URL` (a `host:port` pointing at a TLS-enabled broker) enables the TLS round-trip test. `CLINK_MQTT_TLS_CA` may supply the CA certificate (PEM); without it, verification is skipped (self-signed dev cert: encrypt but do not authenticate).

There is no dedicated MQTT broker Docker image wired into the test harness; point the environment variable at a broker you run yourself (for example a local `eclipse-mosquitto` container).

In-process unit tests run without a broker: `test_factory_registration.cpp` checks that `mqtt_source` and `mqtt_sink` are registered on the `string` channel, and `test_mqtt_sink_logic.cpp` / `test_mqtt_source_logic.cpp` exercise the sink and source logic. The test executable is `clink_mqtt_tests`, labelled `mqtt`:

```bash
ctest --test-dir build -L mqtt
```
