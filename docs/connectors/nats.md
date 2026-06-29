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

All options are parsed in `impls/nats/src/register_factories.cpp` and map onto the `NatsSource::Options` / `NatsSink::Options` structs (`include/clink/nats/nats_source.hpp`, `include/clink/nats/nats_sink.hpp`) and the shared `NatsConnParams` (`include/clink/nats/connection_params.hpp`).

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
| `ack_wait_s` | No | `60` | Consumer AckWait in seconds. Keep larger than the checkpoint interval. |
| `max_ack_pending` | No | `2048` | Upper bound on held, unacked messages. |

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

- Source: a durable explicit-ack pull consumer. Messages are acked at the checkpoint barrier (`snapshot_offset`); until then JetStream holds them (bounded by `max_ack_pending`) and redelivers any left unacked after a failure. The durable consumer's ack floor is persisted server-side, so recovery needs no local state and `restore_offset` is a no-op. Honest caveat noted in the source header: the ack happens at the barrier because the Source interface has no post-commit hook, so a crash between the barrier ack and checkpoint completion could drop those messages. Keep `ack_wait_s` larger than the checkpoint interval so held, unacked messages are not redelivered before a barrier.
- Sink: records are published asynchronously (`js_PublishAsync`) for throughput; `flush()` / `on_barrier()` call `js_PublishAsyncComplete` to block until the server has acked every pending publish, and throw on timeout so the job replays from the last checkpoint rather than dropping data. JetStream de-dup via `Nats-Msg-Id` is not set, so replay can duplicate. The SQL planner therefore rejects `exactly_once` and `upsert`.

## Limitations

- String record type only; there is no int64 or Arrow channel for this connector.
- Sink requires a pre-existing JetStream stream bound to the target subject; stream provisioning is not performed by the connector.
- No producer dedup key (`Nats-Msg-Id`) is set, so exactly-once and upsert are unsupported (enforced by the SQL planner).
- The source's barrier-time ack carries the standard at-least-once caveat: a crash between the barrier ack and checkpoint completion can drop in-flight messages.
- `subject` is required for both source and sink; construction throws if it is empty.

## Testing

Offline tests in `impls/nats/tests/test_nats.cpp` cover required-parameter validation and clean failure against a dead endpoint; they need no broker.

The publish-then-consume round-trip lives in `impls/nats/tests/test_nats_live.cpp` and is skipped unless `CLINK_NATS_TEST_ENDPOINT` is set to a JetStream-enabled NATS URL (for example `nats://localhost:4222`). The live test creates the stream up front via the nats.c API, publishes through the sink (acks awaited), then reads the messages back through the source's durable pull consumer.

```bash
CLINK_NATS_TEST_ENDPOINT=nats://localhost:4222 \
  ctest --test-dir build -L nats
```

The tests do not pin a Docker image; run any JetStream-enabled NATS server and point `CLINK_NATS_TEST_ENDPOINT` at it.
