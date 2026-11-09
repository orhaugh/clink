# MQTT connector

> A source (`mqtt_source`, via SUBSCRIBE) and a sink (`mqtt_sink`, via PUBLISH) for MQTT brokers, exchanging string payloads. The sink publishes through libmosquitto and waits at every checkpoint for the broker to acknowledge what it published; the source subscribes through its own MQTT 5 / 3.1.1 client, which acknowledges a message only once a completed checkpoint covers it.

## Overview

The MQTT connector integrates an MQTT broker (for example Mosquitto). The sink PUBLISHes each input record to a fixed topic; the source SUBSCRIBEs to a topic filter and emits each received message. Both operate on the `std::string` channel: the payload is treated as an opaque string and published or emitted verbatim, with no Arrow or Parquet encoding. The source can optionally wrap each message as a JSON envelope (`{"topic":...,"payload":...}`) so that a wildcard subscription can carry the per-message topic.

The two sides use different clients. The sink publishes over the libmosquitto synchronous client API (`mqtt_client.hpp`), speaking MQTT 5 unless `protocol_version` says 3.1.1. The source subscribes through the impl's own client (`mqtt_subscriber.hpp`, `src/mqtt_subscriber.cpp`, with the packet codec in `mqtt_protocol.hpp`), because libmosquitto, like Paho, acknowledges a message as soon as it hands it over and cannot hold the acknowledgement until a checkpoint covers the message. The source's client speaks MQTT 5 by default and MQTT 3.1.1 when asked, over TCP or TLS.

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
| `port` | No | `1883` | Broker port, 1 to 65535. |
| `keepalive` | No | `60` | Keepalive in seconds, 5 to 65535 (libmosquitto refuses less than 5). |
| `username` | No | (empty) | Username for broker authentication; empty means no auth. |
| `password` | No | (empty) | Password for broker authentication. |
| `client_id` | No | source: `clink-mqtt-source`; sink: one of its own | Client id base; the subtask index is appended so parallel subtasks hold distinct ids. Unset, each sink connects under a random id of its own (see [the client id](#the-client-id)). |
| `tls` | No | `false` | `true` enables TLS encryption (the sink through libmosquitto's OpenSSL, the source through OpenSSL found at build time). Without `tls_ca` or `tls_capath`, the source verifies against the system's default CAs. |
| `tls_ca` | No | (empty) | CA certificate file (PEM) used to verify the server. |
| `tls_capath` | No | (empty) | Directory of CA certificates. |
| `tls_cert` | No | (empty) | Client certificate file for mutual TLS. |
| `tls_key` | No | (empty) | Client private key file for mutual TLS (the source reads the key from `tls_cert` when this is empty). |
| `tls_verify` | No | `true` | `false` skips server-certificate and hostname verification (self-signed dev only). |

`tls` and `tls_verify` take only `true` or `false`, and `tls_ca`, `tls_capath`, `tls_cert` and `tls_key` are refused unless `tls` is `true`, since the connection would not use them. A value outside its range is refused when the job is built, naming the option; `tls='TRUE'` once read as `false` and connected in the clear.

### `mqtt_sink` options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `topic` | Yes | (none) | Topic to publish to. |
| `qos` | No | `1` | Publish QoS: 0, 1 or 2 (see [the sink](#the-sink)). |
| `retain` | No | `false` | `true` sets the MQTT retain flag on each published message. |
| `protocol_version` | No | `5` | `5` (MQTT 5) or `3.1.1`. Under MQTT 3.1.1 the sink cannot tell a message the broker refused from one it took (see [refusals](#refusals)). |
| `clean_session` | No | `true` | `false` asks the broker to keep a session under the sink's client id, and needs `client_id`. The sink subscribes to nothing and resumes nothing, so the session gives it nothing. |
| `batch_records` | No | `1000` | Flush after this many buffered records, 1 to 1,000,000. |
| `max_bytes` | No | `0` (16 MiB) | Flush after this many buffered payload bytes, up to 1 GiB; 0 means 16 MiB. |
| `max_record_bytes` | No | `16777216` (16 MiB) | A longer record fails the sink before it is published; 1 to 268,435,455, MQTT's largest packet. |
| `linger_ms` | No | `0` | Flush a partial batch once it is this many milliseconds old, checked as records arrive, up to an hour; 0 disables linger. |
| `ack_timeout_ms` | No | `30000` | The longest a flush waits for its acknowledgements (at QoS 0, for the connection to take its messages), 1 ms to an hour. |

Every boolean takes only `true` or `false` and every number is checked against its range, so a typo fails the build of the job rather than reading as the default.

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

## The sink

The sink (`impls/mqtt/src/mqtt_sink.cpp`) publishes through libmosquitto's synchronous client (`impls/mqtt/include/clink/mqtt/mqtt_client.hpp`) on the task's own thread.

### Publishing and acknowledgements

Records are buffered and published in flushes: when `batch_records`, `max_bytes` or `linger_ms` is reached, and at every checkpoint barrier, which waits until the broker has acknowledged the flush, so a completed checkpoint covers only messages the broker took. At QoS 1 a message's acknowledgement is the broker's PUBACK and at QoS 2 its PUBCOMP. At QoS 0 the broker acknowledges nothing, and a flush ends once the connection has taken its messages. Against Mosquitto 2.1.2 in the gate's configuration, a flush has at most 20 messages unacknowledged at once, under MQTT 5 and 3.1.1 alike: the recovery cell killed between publishing and the acknowledgements finds exactly 20 of the parked flush's messages reached the broker. So the round trip to the broker bounds how fast a flush goes.

### Refusals

Under MQTT 5 (the default) every acknowledgement carries a reason code. A message the broker refused, one its ACL denies for instance, is answered with a code of 0x80 or above, and the flush fails once every message is answered, saying how many were refused and naming the broker's reason. MQTT 3.1.1 has no such code. A broker that drops a message under it acknowledges the message all the same, and the sink cannot tell: against Mosquitto, a publish its ACL denies is acknowledged and dropped. The live suite holds both, `ABrokerThatRefusesAMessageFailsTheFlushNamingItsReason` at QoS 1 and 2 under MQTT 5 and `UnderMqtt311ABrokerAcknowledgesAMessageItRefuses`. Run the sink at MQTT 3.1.1 only against a broker that speaks nothing newer.

### Failures

- Acknowledgements that have not all come within `ack_timeout_ms`, a connection that fails while a flush waits, and a publish libmosquitto refuses fail the flush at once. The unacknowledged messages' outcome is unknown, and they are only that: the live suite holds the network past the timeout and finds the broker delivers the messages the sink gave up on once it carries again. When the broker ends the connection with an MQTT 5 DISCONNECT, the error names its reason; the live suite has a second connection take over the sink's client id during a flush and finds `Session taken over` in the error.
- A connection that dropped while nothing was in flight on it, because no barrier came within the keepalive or the broker restarted, loses nothing: the sink reconnects, awaiting the broker's CONNACK, before it publishes the next flush, and only a reconnect that fails fails the flush, with nothing of it sent.

Each failure says how many of the flush's messages were acknowledged, refused and left unacknowledged. Any failure fails the sink: every later record, barrier and flush is refused, an empty flush included, until the sink is opened again, which the job's restart does. The restart publishes again what its last checkpoint did not cover. `close()` discards what is buffered and unacknowledged rather than waiting for it, and never throws; a clean end of input flushes before it.

### Bounds, connection and cancellation

A flush holds at most `batch_records` records and `max_bytes` bytes, and a record longer than `max_record_bytes` fails the sink before it is published, with nothing of the flush sent. Set `max_record_bytes` at or below the broker's own limit. Mosquitto 2.1.2 drops a message over its `message_size_limit`: under MQTT 3.1.1 it acknowledges the message all the same, so the message is lost, and under MQTT 5 it answers with reason code 0x95, which MQTT does not allow in an acknowledgement and libmosquitto rejects as a protocol error, so the flush fails with the message unknown (measured with `mosquitto_pub`, which publishes through libmosquitto as the sink does).

Every checkpoint barrier and every flush with nothing to send services the connection without blocking, so the keepalive's ping goes out while the sink idles. A broker drops a client it has heard nothing from for one and a half keepalives, and libmosquitto sends the ping only from its network loop, which the sink used to run only to flush a batch: a sink that published rarely, an alert's, was dropped after ninety quiet seconds at the default keepalive and failed the job at its next flush.

Waits for acknowledgements sleep at most 100 ms between looks at the task's cancel, so a cancelled task stops waiting at once.

### The client id

A broker disconnects a client when another connects with its client id. When `client_id` is unset, each sink therefore connects under a random id of its own, `clinksink` and 14 random letters and digits, 23 characters in all, the form every broker must accept. Before, every sink without a `client_id` connected as `clink-mqtt-sink-<subtask>`, so two jobs writing to one broker disconnected each other: run against that default, the live suite's two sinks built from the same options failed all 40 of their flushes. A `client_id` that is set still takes the subtask index, and must be the job's own. `clean_session='false'` asks the broker to keep a session under the client id, so it needs a `client_id`, and an empty `client_id` is refused.

## Delivery semantics

Source. MQTT has no position a source could store, so recovery is the broker's: the source holds a persistent session under a stable per-subtask client id, and when the session reconnects the broker delivers again every QoS 1 or 2 message it holds unacknowledged. The source therefore acknowledges a message only once a completed checkpoint covers it. `snapshot_offset` seals what arrived since the last barrier as that checkpoint's (through `CheckpointAckLedger`, persisting nothing), `notify_checkpoint_complete` makes it ready, and the next `produce()` or `close()` sends the PUBACKs, in the order the messages arrived, as MQTT requires. At QoS 2 the deferred packet is the PUBREC, which is what hands the message over; the PUBREL that follows is answered with PUBCOMP at once. A crash before the acknowledgements go out leaves the messages with the session, and they are delivered again. The record (`mqtt_source`) declares `OffsetModel::Acknowledgement`, and the delivery analyser reports a job over the source as `AT_LEAST_ONCE_SOURCE`: a sink may declare `at_least_once`, and `exactly_once` is refused.

Two rules come from MQTT itself. An acknowledgement goes out only on the connection its message arrived on: after a reconnect the broker sends every unacknowledged message again, under packet identifiers it may since have reused, so what the source still holds from an earlier connection is never acknowledged on a later one, and the copy delivered again is acknowledged instead. And MQTT has no negative acknowledgement, so an aborted checkpoint reconnects: only a new connection gets the broker to deliver that checkpoint's messages again. A message after the aborted ones is not acknowledged on the old connection either, since that would break the order. Both deliver again messages a later checkpoint may already cover, so the guarantee is at least once. `close()` acknowledges what completed checkpoints cover and leaves the rest with the session.

How much one checkpoint interval reads is the session's in-flight window, since the broker sends at most that many messages unacknowledged. Under MQTT 5 the source states it as its Receive Maximum (`receive_maximum`, default 10,000). Under MQTT 3.1.1 the broker's own setting decides: mosquitto's `max_inflight_messages` is 20 unless configured, so at 10-second checkpoints a subtask reads about two messages a second. While acknowledgements wait, the broker queues what it cannot yet send, and a broker that bounds that queue drops what overflows it: mosquitto's `max_queued_messages` defaults to 1,000. Measured against mosquitto 2.1.2 in its default configuration, an MQTT 3.1.1 subscriber holding its acknowledgements received 20 of 3,000 messages, and once it reconnected and acknowledged everything, 1,040 came back; the broker logged that it was dropping the rest. Size `receive_maximum`, and the broker's queue limits, for what arrives in one checkpoint interval.

Under MQTT 5 the source subscribes with Retain Handling 1, so retained messages come only with a subscription that is new: a source that resumes its session does not receive a retained message again. MQTT 3.1.1 has no such option, and a restart receives the retained message again.

Recovery stands on the session. A session the broker no longer holds, because it expired or because a broker that does not persist sessions restarted, takes what it held unacknowledged with it; on such a reconnect the source logs that the session was gone and subscribes again. The session never expires by default (`session_expiry_s`), so a job that is deleted leaves its sessions queueing messages on the broker until they are removed there. With `shared_group` set, a subtask's session also holds its messages when the subtask goes, and no remaining subtask receives them, so the coordinator refuses to rescale the source to fewer subtasks; the broker may keep those messages until the session expires. Without `shared_group` only subtask 0 subscribes, the subtasks a scale-down removes hold no session, and the source can be scaled down.

Sink. At QoS 1 (the default) and QoS 2 the sink is at-least-once end to end, under MQTT 5: every barrier waits until the broker has acknowledged everything published before it, and a flush that fails is replayed from the last checkpoint, publishing again messages that may already have reached the broker, so duplicates are possible and must be absorbed downstream. MQTT has no idempotent publish to de-duplicate them. Under MQTT 3.1.1 a broker that drops a message still acknowledges it, so a refused message is lost while the checkpoint completes (see [refusals](#refusals)). At QoS 0 the sink is at-most-once: the broker acknowledges nothing, and a message lost with a connection that drops after the flush has written it is gone while the checkpoint completes. The capability record (`mqtt`) declares `at_least_once`; see [the sink](#the-sink) for its failures and bounds.

## Limitations

- The source is at-least-once at QoS 1 and 2: messages a completed checkpoint covers whose acknowledgements had not gone out are delivered again after a crash, and an aborted checkpoint or a lost connection delivers again everything the session holds unacknowledged. At QoS 0 nothing is acknowledged and nothing delivered again.
- A restore from a checkpoint older than one the source acknowledged (commit-confirmed restore, a savepoint) does not see that checkpoint's messages again.
- One checkpoint interval reads at most the session's in-flight window, and while acknowledgements wait a broker that bounds its queue drops what overflows it (see above).
- With `shared_group` set, the source cannot be rescaled to fewer subtasks.
- The sink is at-least-once at QoS 1 and 2: a flush whose acknowledgements do not all come leaves the unacknowledged messages unknown, and the restart publishes them again. Under MQTT 3.1.1 a message the broker refuses is acknowledged and lost (see [refusals](#refusals)).
- A sink's `client_id`, when set, must be the job's own: two connections under one id disconnect each other, and the sink fails the flush it was waiting on.
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
- The same variable enables the sink's contract suite, `impls/mqtt/tests/test_mqtt_sink_contract_live.cpp` (`MqttSinkContractLive`), judged by what a subscriber of the test's own receives through the impl's MQTT 5 client: every message exactly as written at each QoS under MQTT 5 and 3.1.1, acknowledgements held past `ack_timeout_ms` by a TCP proxy (`tests/integration/tcp_fault_proxy.hpp`) and the unknown messages arriving once it carries again, a connection dropped during a flush failing the sink until it is opened again, a cancel ending a wait, a record over `max_record_bytes` refused before it is published, `close()` discarding without waiting, a refusal under MQTT 5 at QoS 1 and 2 and the acknowledged drop under MQTT 3.1.1 (on topics under `clink/denied/`, which `docker/mosquitto.acl` denies), the broker's reason when it ends the connection, the factory, and two sinks built from the same options keeping their own connections.
- `CLINK_MQTT_TLS_TEST_URL` (a `host:port` pointing at a TLS-enabled broker) enables the TLS round-trip test. `CLINK_MQTT_TLS_CA` may supply the CA certificate (PEM); without it, verification is skipped (self-signed dev cert: encrypt but do not authenticate).

The sink's recovery cells, `tests/integration/test_mqtt_sink_recovery.cpp` (`MqttSinkRecovery`, in `clink_integration_tests`), run a coordinator and two `clink_node` workers polling a feed the test serves into `mqtt_sink`, and SIGKILL the worker parked at `mqtt.sink_before_ack` (the flush's messages handed to the connection, its acknowledgements not yet awaited) or at `mqtt.sink_after_ack` (every message acknowledged and the barrier that asked for them not), at QoS 1 and, after the acknowledgements, at QoS 2. A subscriber of the test's own, subscribed at QoS 2 before the job starts, finds every record exactly as written, at most twice, and twice exactly those published after the restored checkpoint and before the kill. `CLINK_MQTT_RECOVERY_LOG_DIR` keeps the cluster's logs.

`docker/integration-services.yml` defines an `mqtt` service (`eclipse-mosquitto:2` on port 1883) with `docker/mosquitto.conf`, which raises the broker's queue per client so a test's subscriber that falls behind a burst is not dropped from, and `docker/mosquitto.acl`. `scripts/brokers-live.sh` runs the source's suites, live and offline, in its sources phase, and the sink's suites and the recovery cells in its sinks phase, against a one-off container of it, and fails on a skipped case or a short suite and when the phases between them leave a case out; the `brokers` CI workflow runs that script. The TLS case is left out there, since nothing provides the TLS broker it needs. `ONLY=mqtt scripts/run-all-live.sh` runs the live suites by hand inside the toolchain image.

In-process tests run without a broker. `test_mqtt_protocol.cpp` holds each packet the source's client sends, byte for byte, and the malformed ones it refuses; `test_mqtt_source_ack.cpp` runs the source against an in-process broker (`fake_mqtt_broker.hpp`) that records every acknowledgement in the order it came, covering the same cases as `MqttSourceAckLive` plus a message sent again on the same connection, a refused connection and the keepalive's ping. `test_factory_registration.cpp` checks the registrations, both records, the analyser's verdicts and the sink factory's refusals, and `test_mqtt_sink_logic.cpp` / `test_mqtt_source_logic.cpp` exercise the sink and source options. The test executable is `clink_mqtt_tests`, labelled `mqtt`:

```bash
ctest --test-dir build -L mqtt
```
