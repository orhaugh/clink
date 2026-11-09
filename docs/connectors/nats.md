# NATS JetStream

> Source and sink for NATS JetStream subjects, carrying each message body as a string.

## Overview

This connector integrates with NATS JetStream, the persistent, acknowledged streaming layer of NATS, via the libnats (nats.c) client. The source is a durable pull consumer that fetches batches from a subject and emits each message body as a `std::string`; the sink publishes each string record to a subject using asynchronous JetStream publishes. Only the string record type is supported, and records cross the wire as their raw byte body (the SQL frontend bridges this to a JSON object string). Core NATS pub/sub (fire-and-forget, at-most-once) is not targeted; this connector uses JetStream throughout.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| libnats (nats.c) | System package via apt (Debian: `libnats-dev`) / brew (macOS: `cnats`) | Not pinned by clink |

Arrow is not involved in this connector; records are plain string bodies, not Arrow batches.

## Enabling it

The connector is controlled by the `CLINK_WITH_NATS` CMake option (`AUTO` / `ON` / `OFF`, default `AUTO`). Under `AUTO` the target is built only if libnats is discovered (via `pkg-config libnats`, or a manual probe for `nats/nats.h` and the `nats` library). Under `ON` a missing libnats is a fatal configure error. The library is found through standard system paths (`/opt/homebrew`, `/usr/local`, `/usr`).

Install the client first:

- Debian: `apt-get install libnats-dev` (see `scripts/install-connector-deps.sh`)
- macOS: `brew install cnats`

```bash
cmake -S . -B build -DCLINK_WITH_NATS=ON
cmake --build build -j
```

When enabled, the build defines `CLINK_HAS_NATS` and produces the `clink::nats` target.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `nats_source_string` | Source | `std::string` |
| `nats_sink_string` | Sink | `std::string` |

## Configuration

All options are parsed in `impls/nats/src/register_factories.cpp` and map onto the `NatsSource::Options` / `NatsSink::Options` structs (`impls/nats/include/clink/nats/nats_source.hpp`, `impls/nats/include/clink/nats/nats_sink.hpp`) and the shared `NatsConnParams` (`impls/nats/include/clink/nats/connection_params.hpp`).

### Connection (source and sink)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `url` | No | `nats://localhost:4222` | NATS server URL. Comma-separated URLs are accepted by nats.c. |
| `user` | No | (empty) | Username for user/password auth. Leave blank for no-auth. |
| `password` | No | (empty) | Password for user/password auth. |
| `token` | No | (empty) | Token for token auth. Set whichever of user/password or token the server expects. |
| `client_name` | No | `clink` | Client connection name shown in NATS monitoring. |
| `connect_timeout_ms` | No | `2000` | The longest the connect may take, 1 to 300,000 ms (nats.c's own default). |
| `write_timeout_ms` | No | `5000` | The longest a write to the server may wait for the socket, 1 to 3,600,000 ms. Without it a server that stops reading would hold a publish, an acknowledgement or a fetch for as long as it stopped. |

The timeouts, the source's options and the sink's are read strictly: a number outside its range fails the job's build rather than reading as the default. Releases up to and including v0.15.0 read the source's own options leniently: `ack_wait_s='1.5'` read as 1, a typo as the default, and `max_ack_pending` past 2^31-1 wrapped to a negative number.

### Source (`nats_source_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `subject` | Yes | (none) | Subject to consume. Construction throws if empty. |
| `stream` | No | (empty) | Bind to this JetStream stream; if blank, the stream is resolved by subject. |
| `durable` | No | `clink` | Durable consumer name, shared across subtasks. |
| `batch` | No | `256` | Messages fetched per pull. 1 to 1000000. |
| `fetch_timeout_ms` | No | `1000` | Fetch wait in milliseconds; also bounds cancel latency. 1 to 600000. |
| `ack_wait_s` | No | `60` | Consumer AckWait in seconds. A message waiting for its checkpoint is kept in progress (`natsMsg_InProgress`) at half this period. 1 to 31536000 (a year). |
| `max_lease_s` | No | `3600` | How long one message is kept in progress; past it JetStream redelivers it once AckWait runs out. At least `ack_wait_s`, at most a year. |
| `max_ack_pending` | No | `2048` | The durable consumer's bound on held, unacknowledged messages, shared by every subtask on the durable. None is acknowledged before its checkpoint completes, so this is the most the job reads per checkpoint. 1 to 2147483647. |

### Sink (`nats_sink_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `subject` | Yes | (none) | Subject to publish to. Construction throws if empty. |
| `max_pending` | No | `4096` | The most messages awaiting the server's answers at once, 1 to 1,000,000; past it the sink waits for answers before it publishes more. |
| `max_pending_bytes` | No | `67108864` | The most bytes of messages awaiting answers at once, up to 4 GiB: nats.c keeps every unanswered message in memory. |
| `publish_timeout_ms` | No | `30000` | The longest a flush, or a wait at either window, waits for answers, 1 to 3,600,000 ms; past it the sink fails with the unanswered messages' outcome unknown. |

Authentication is configured through the connection options (`user`/`password` or `token`); leave them blank for an unauthenticated server.

TLS has no option of its own. When the server requires TLS, nats.c switches to it with a default context that loads the system's trusted CAs and verifies the server's certificate, so a server with a publicly trusted certificate works; there is no option for a private CA, a client certificate or skipping verification.

## SQL usage

The connector is mapped in `src/sql/physical_plan.cpp` under `connector='nats'` for both source and sink. The source binds to `nats_source_string` with the `json_string_to_row` codec; the sink binds to `nats_sink_string` with `row_to_json_string`. The sink is at-least-once only: `mode='upsert'` is rejected, and `exactly_once` is rejected because no producer dedup key is set.

```sql
CREATE TABLE orders (
    id     BIGINT,
    region STRING
) WITH (
    connector = 'nats',
    url       = 'nats://localhost:4222',
    subject   = 'clink.orders',
    stream    = 'ORDERS',
    durable   = 'clink-orders'
);
```

A sink table uses the same `connector='nats'` with at least `subject` set, for example `subject='clink.results'`. A sink table's options are checked when the job is planned (`check_nats_sink_options`): one the sink does not read is refused, naming those it reads, so a misspelt `publish_timeout` no longer leaves the default in force. The source's options pass, so one table can serve as both.

A NaN or infinite double in a row reaches this sink through the `row_to_json_string` bridge as `null`, or as `"NaN"`, `"Infinity"` or `"-Infinity"` when the sink table sets `json_non_finite='string'`; the sink handles it as it handles any null or string value. See [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Example

Based on `impls/nats/tests/test_nats_live.cpp`, using the typed classes directly:

```cpp
#include "clink/nats/nats_sink.hpp"
#include "clink/nats/nats_source.hpp"

using clink::nats::NatsSink;
using clink::nats::NatsSource;

// Sink: publish three records, then block until JetStream acks them all.
NatsSink::Options so;
so.conn.url = "nats://localhost:4222";
so.subject  = "clink.events";
NatsSink sink(std::move(so));
sink.open();

clink::Batch<std::string> out;
out.emplace(R"({"id":1,"v":"a"})");
out.emplace(R"({"id":2,"v":"b"})");
sink.on_data(out);
sink.flush();  // throws on publish-ack timeout so the job replays

// Source: durable pull consumer over the same subject/stream.
NatsSource::Options ro;
ro.conn.url = "nats://localhost:4222";
ro.subject  = "clink.events";
ro.stream   = "EVENTS";
ro.durable  = "clink-events";
NatsSource src(std::move(ro));
src.open();

clink::Emitter<std::string> em([](clink::StreamElement<std::string> e) -> bool {
    if (e.is_data()) {
        for (const auto& rec : e.as_data()) {
            // rec.value() is the raw message body
        }
    }
    return true;
});
src.produce(em);
```

A JetStream stream bound to the subject must already exist; the connectors do not provision streams (that is an admin task, comparable to a RabbitMQ exchange).

## The sink

The sink (`impls/nats/src/nats_sink.cpp`) publishes through a publisher that keeps nats.c to itself (`impls/nats/src/nats_publisher.cpp`, behind `clink/nats/nats_publisher.hpp`), so its failure paths can be tested against a scripted server.

### Publishing and answers

Each record is published asynchronously as it arrives (`js_PublishAsync`), and every checkpoint barrier waits until the server has answered each message published before it, so a completed checkpoint covers only stored messages. nats.c reports every answer, positive or negative, to a per-message handler, so the sink knows exactly how many messages were stored and refused. A positive answer (PubAck) means the stream stored the message as it is configured: a single-replica stream can lose an acknowledged message if its host fails before it reaches disk.

### Failures

- A refusal (a negative PubAck, such as a stream at its limit with the discard-new policy, or no stream bound to the subject) fails the flush once every message is answered, naming the server's reason. Before the error handler existed, nats.c dropped a refused message and counted the publish complete, so the flush succeeded over it.
- Answers that have not all come within `publish_timeout_ms`, a publish nats.c refuses (a message over the server's `max_payload`, a write past `write_timeout_ms`), and a closed connection fail the flush at once. The unanswered messages' outcome is unknown, and they are only that: the live suite holds the network past the timeout and finds the stream stored them once it carried again.

Each failure says how many of the messages published since the last complete flush were stored, refused, and left unanswered. Any failure fails the sink: every later record, barrier and flush is refused, an empty flush included, until the sink is opened again, which the job's restart does. The restart publishes again what its last checkpoint did not cover. `close()` does not wait for unanswered messages and never throws; a clean end of input flushes before it.

### Bounds and cancellation

At most `max_pending` messages, and `max_pending_bytes` of them, await answers at once; past either the sink waits for answers, within `publish_timeout_ms`, before it publishes more. nats.c keeps every unanswered message in memory, so without the byte bound a server that stops answering could hold the window's 4096 messages of up to its `max_payload` each. The connect waits at most `connect_timeout_ms`. nats.c writes to the socket with no deadline unless one is set, and a server that stops reading would hold a publish for as long as it stopped; `write_timeout_ms` ends the stalled write, and the sink fails. With the deadline removed, the live suite's stalled publish did not return before a 20 s limit. Waits for answers sleep at most 100 ms between looks at the task's cancel, so a cancelled task stops waiting at once, and during a stalled write within `write_timeout_ms`.

## Delivery semantics

Both directions are at-least-once.

- Source: a durable explicit-ack pull consumer, and the ack waits for the checkpoint that covers a message to complete. `snapshot_offset` buckets the messages emitted before each barrier against that checkpoint id, `notify_checkpoint_complete` (from the cluster's `CommitCheckpoint` dispatch) queues every bucket at or below the completed id for ack, and the next `produce()` turn, or `close()`, issues each `natsMsg_Ack`. `notify_checkpoint_aborted` releases a bucket without acking, so JetStream redelivers it. While a message waits, the source keeps it in progress: `natsMsg_InProgress` resets its AckWait at half that period, for at most `max_lease_s`, so a checkpoint slower than `ack_wait_s` no longer has its messages redelivered (it did: with a 2 s AckWait and a 6 s checkpoint, three messages were delivered again nine times). `close()` acknowledges what completed checkpoints cover and Naks everything else, held, uncommitted or aborted, so JetStream redelivers it at once; it used to acknowledge nothing and leave the rest to AckWait. A crash before completion, or between a completion and its ack, leaves the messages for redelivery after AckWait (duplicates). All nats.c calls stay on the `produce()` thread. The durable consumer's ack floor is persisted server-side, so recovery needs no local state and `restore_offset` is a no-op. `max_ack_pending` is the durable's bound on what is held, across every subtask, and so the most the job reads per checkpoint.
- Sink: see [the sink](#the-sink). It is at-least-once: JetStream de-duplicates only by `Nats-Msg-Id`, which the sink does not set, so a replay stores its messages again, and the SQL planner rejects `exactly_once` and `upsert`.

The capability record declares the source's position model as `acknowledgement`, and clink's delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused, because a checkpoint acknowledged late is delivered again.

## Limitations

- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String record type only; there is no int64 or Arrow channel for this connector.
- Sink requires a pre-existing JetStream stream bound to the target subject; stream provisioning is not performed by the connector.
- No producer dedup key (`Nats-Msg-Id`) is set, so exactly-once and upsert are unsupported (enforced by the SQL planner).
- The sink is at-least-once: a closed connection or a publish timeout leaves messages whose outcome is unknown, and the restart publishes them again.
- A PubAck means the stream stored the message as it is configured; a single-replica stream can lose acknowledged messages if its host fails before they reach disk.
- TLS is negotiated only as nats.c does when the server requires it, verified against the system's trusted CAs; there is no option for a private CA, a client certificate or skipping verification.
- The source acknowledges at checkpoint completion, so a crash between a completion and its ack delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged (commit-confirmed, a savepoint) does not see that checkpoint's messages again.
- `max_ack_pending` is shared by every subtask on the durable and caps what the job reads per checkpoint; a message held past `max_lease_s` is redelivered (a duplicate).
- `subject` is required for both source and sink; construction throws if it is empty.

## Testing

Offline tests in `impls/nats/tests/test_nats.cpp` cover required-parameter validation and clean failure against a dead endpoint, and `test_factory_registration.cpp` the factories, the source's and the sink's options read strictly, and the record. `NatsSinkFailures` (`test_nats_sink_failures.cpp`) drives the sink against a scripted server: a refusal, answers that do not come, a closed connection and a publish the library refuses, each followed by a record, a barrier and a flush; reopening; a cancel during a wait and before a publish; `close()`; and the count and byte windows. These need no broker.

The live tests in `impls/nats/tests/test_nats_live.cpp` are skipped unless `CLINK_NATS_TEST_ENDPOINT` is set to a JetStream-enabled NATS URL (for example `nats://localhost:4222`); each creates its stream up front. `NatsLive` covers the round trip and the sink's refusal. `NatsSourceAckLive` holds the source to its acknowledgement contract, judged by what another subscriber of the same durable receives once the source has gone, with a 2 s AckWait: an aborted checkpoint, a crash after a completion and before its ack, notifications out of order, close after a cancel, the `max_ack_pending` bound, and a checkpoint three times as slow as AckWait. The close and slow-checkpoint cases failed against the source before this contract, and pass.

`NatsSinkContractLive` (`test_nats_sink_contract_live.cpp`) holds the sink to its contract against the server, read through a JetStream connection of the test's own: messages stored exactly as written, a thousand messages within `max_pending=10`, a full stream refusing three of five publishes, publishes no stream receives, and, through a TCP proxy that blackholes the connection, a publish timeout whose messages the stream stores once the network carries again, a cancel during a wait, a write to a server that stops reading, and a connect to a peer that says nothing.

The `NatsSinkRecovery` cells (`tests/integration/test_nats_sink_recovery.cpp`) run a coordinator and two `clink_node` workers polling a feed the test serves into the sink, with worker 0 killed at `nats.sink_before_ack` (a barrier's messages published and the sink not yet waiting for their answers) and at `nats.sink_after_ack` (every message stored and the barrier not acknowledged), and a SQL job at parallelism 2. They read the stream through nats.c: every record is in it exactly as written, and the records in it twice are exactly those published after the checkpoint the job restored and before the kill.

`scripts/brokers-live.sh` runs the source's suites in its sources phase and the sink's suites and cells in its sinks phase, against the `nats` service of `docker/integration-services.yml` (`nats:2.10` with JetStream), as a one-off container on free loopback ports, and fails on a skipped case, a short suite, or a case of `clink_nats_tests` that neither phase ran; the `brokers` CI workflow runs it.

```bash
ctest --test-dir build -L nats          # offline cases; the live ones skip
scripts/brokers-live.sh build           # every broker connector, live
```
