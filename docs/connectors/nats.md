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

### Source (`nats_source_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `subject` | Yes | (none) | Subject to consume. Construction throws if empty. |
| `stream` | No | (empty) | Bind to this JetStream stream; if blank, the stream is resolved by subject. |
| `durable` | No | `clink` | Durable consumer name, shared across subtasks. |
| `batch` | No | `256` | Messages fetched per pull. |
| `fetch_timeout_ms` | No | `1000` | Fetch wait in milliseconds; also bounds cancel latency. |
| `ack_wait_s` | No | `60` | Consumer AckWait in seconds. A message waiting for its checkpoint is kept in progress (`natsMsg_InProgress`) at half this period. At least 1. |
| `max_lease_s` | No | `3600` | How long one message is kept in progress; past it JetStream redelivers it once AckWait runs out. At least `ack_wait_s`. |
| `max_ack_pending` | No | `2048` | The durable consumer's bound on held, unacknowledged messages, shared by every subtask on the durable. None is acknowledged before its checkpoint completes, so this is the most the job reads per checkpoint. At least 1. |

### Sink (`nats_sink_string`)

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `subject` | Yes | (none) | Subject to publish to. Construction throws if empty. |
| `max_pending` | No | `4096` | Asynchronous publish window; provides backpressure. |
| `publish_timeout_ms` | No | `30000` | Time to wait for pending publish acks on flush. |

Authentication is configured through the connection options (`user`/`password` or `token`); leave them blank for an unauthenticated server.

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

A sink table uses the same `connector='nats'` with at least `subject` set, for example `subject='clink.results'`.

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

## Delivery semantics

Both directions are at-least-once.

- Source: a durable explicit-ack pull consumer, and the ack waits for the checkpoint that covers a message to complete. `snapshot_offset` buckets the messages emitted before each barrier against that checkpoint id, `notify_checkpoint_complete` (from the cluster's `CommitCheckpoint` dispatch) queues every bucket at or below the completed id for ack, and the next `produce()` turn, or `close()`, issues each `natsMsg_Ack`. `notify_checkpoint_aborted` releases a bucket without acking, so JetStream redelivers it. While a message waits, the source keeps it in progress: `natsMsg_InProgress` resets its AckWait at half that period, for at most `max_lease_s`, so a checkpoint slower than `ack_wait_s` no longer has its messages redelivered (it did: with a 2 s AckWait and a 6 s checkpoint, three messages were delivered again nine times). `close()` acknowledges what completed checkpoints cover and Naks everything else, held, uncommitted or aborted, so JetStream redelivers it at once; it used to acknowledge nothing and leave the rest to AckWait. A crash before completion, or between a completion and its ack, leaves the messages for redelivery after AckWait (duplicates). All nats.c calls stay on the `produce()` thread. The durable consumer's ack floor is persisted server-side, so recovery needs no local state and `restore_offset` is a no-op. `max_ack_pending` is the durable's bound on what is held, across every subtask, and so the most the job reads per checkpoint.
- Sink: records are published asynchronously (`js_PublishAsync`) for throughput; `flush()` / `on_barrier()` call `js_PublishAsyncComplete` to block until the server has answered every pending publish, and throw on timeout so the job replays from the last checkpoint rather than dropping data. An answer can be a refusal (a negative PubAck, such as a stream limit, or no stream bound to the subject), which leaves the message in no stream: the sink counts refusals in a publish error handler and fails the flush when there was one, naming the subject and the server's reason. Before the handler, nats.c dropped a refused message and counted the publish as complete, so the flush succeeded over it. JetStream de-dup via `Nats-Msg-Id` is not set, so replay can duplicate. The SQL planner therefore rejects `exactly_once` and `upsert`.

The capability record declares the source's position model as `acknowledgement`, and clink's delivery analyser reports a job over this source as `AT_LEAST_ONCE_SOURCE`: a sink in it may declare `delivery_guarantee='at_least_once'`, and `exactly_once` is refused, because a checkpoint acknowledged late is delivered again.

## Limitations

- The sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it, whichever is added first. SQL and job graphs plan every sink as a subtask of its own; with the Dag API, put the other sink in a separate Dag (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- String record type only; there is no int64 or Arrow channel for this connector.
- Sink requires a pre-existing JetStream stream bound to the target subject; stream provisioning is not performed by the connector.
- No producer dedup key (`Nats-Msg-Id`) is set, so exactly-once and upsert are unsupported (enforced by the SQL planner).
- The source acknowledges at checkpoint completion, so a crash between a completion and its ack delivers that interval again (duplicates), and a restore from a checkpoint older than one it acknowledged (commit-confirmed, a savepoint) does not see that checkpoint's messages again.
- `max_ack_pending` is shared by every subtask on the durable and caps what the job reads per checkpoint; a message held past `max_lease_s` is redelivered (a duplicate).
- `subject` is required for both source and sink; construction throws if it is empty.

## Testing

Offline tests in `impls/nats/tests/test_nats.cpp` cover required-parameter validation and clean failure against a dead endpoint; they need no broker.

The live tests in `impls/nats/tests/test_nats_live.cpp` are skipped unless `CLINK_NATS_TEST_ENDPOINT` is set to a JetStream-enabled NATS URL (for example `nats://localhost:4222`); each creates its stream up front. `NatsLive` covers the round trip and the sink's refusal. `NatsSourceAckLive` holds the source to its acknowledgement contract, judged by what another subscriber of the same durable receives once the source has gone, with a 2 s AckWait: an aborted checkpoint, a crash after a completion and before its ack, notifications out of order, close after a cancel, the `max_ack_pending` bound, and a checkpoint three times as slow as AckWait. The close and slow-checkpoint cases failed against the source before this contract, and pass.

`scripts/brokers-live.sh` runs both suites, in its sources phase, against the `nats` service of `docker/integration-services.yml` (`nats:2.10` with JetStream), as a one-off container on free loopback ports, and fails on a skipped case or a short suite; the `brokers` CI workflow runs it.

```bash
ctest --test-dir build -L nats          # offline cases; the live ones skip
scripts/brokers-live.sh build           # every broker connector, live
```
