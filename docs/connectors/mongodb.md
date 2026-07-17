# MongoDB

> Connects to MongoDB as a change-streams CDC source (`mongo_cdc_source`) and a bulk-write collection sink (`mongo_sink`), both on the string channel.

## Overview

This connector integrates MongoDB through the mongo-cxx-driver (`mongocxx` plus `bsoncxx`). The source watches a collection, database, or whole deployment via MongoDB change streams and emits each row-level change (insert, update, replace, delete) as one flat JSON object string carrying the document image plus `__op`, `__ns`, and `__id` metadata, the same envelope shape as the Postgres and MySQL CDC sources. The sink takes JSON-object record strings, parses each to BSON, and bulk-writes them into a collection, optionally as an upsert by a key field. Records flow on the `std::string` (JSON) channel; there is no Arrow or Parquet involvement in this connector.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| mongo-cxx-driver (`mongocxx` + `bsoncxx`) | System package via apt (Debian, `libmongocxx-dev`) / brew (macOS, `mongo-cxx-driver`) | Not pinned by clink |

The build discovers the driver three ways, most portable first: a pkg-config imported target (`libmongocxx1`), the driver's own CMake CONFIG package (`mongo::mongocxx_shared`), or a manual header and library probe under the usual Homebrew and `/usr/local` prefixes (see `impls/mongodb/CMakeLists.txt`). Arrow is not used by this connector.

## Enabling it

The connector is gated by the CMake cache variable `CLINK_WITH_MONGODB`, which defaults to `AUTO` (`CMakeLists.txt:65`):

- `AUTO`: build the connector only if mongo-cxx-driver is found, otherwise skip it quietly.
- `ON`: require the driver; configuration fails with a fatal error if it is not found.
- `OFF`: do not define the target at all.

There is no Arrow build flag for this connector. You need the mongo-cxx-driver headers and libraries present (apt `libmongocxx-dev` on Debian, brew `mongo-cxx-driver` on macOS).

```bash
cmake -S . -B build -DCLINK_WITH_MONGODB=ON
cmake --build build --parallel 10
```

When enabled, the build defines `CLINK_HAS_MONGODB` and reports `clink::mongodb - enabled`.

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `mongo_cdc_source` | Source | `std::string` (JSON change rows) |
| `mongo_sink` | Sink | `std::string` (JSON objects) |

Both are registered on the `string` channel in `impls/mongodb/src/register_factories.cpp`.

## Configuration

### `mongo_cdc_source`

Parsed in `register_factories.cpp` into `MongoCdcOptions` (`impls/mongodb/include/clink/mongodb/mongo_cdc_source.hpp`).

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `uri` | No | `mongodb://localhost:27017` | MongoDB connection string. Authentication is supplied through this URI (for example `mongodb://user:pass@host:27017/?authSource=admin`); there are no separate credential options. |
| `database` | No | empty | Watch scope. Empty watches the whole deployment. |
| `collection` | No | empty | Empty watches the whole `database`. Setting a collection requires `database` to be set, or `open()` throws. |
| `full_document` | No | `lookup` | Any value other than the literal `false` enables `full_document=updateLookup`, so updates carry the full post-image. Set to `false` to have updates carry only `updateDescription.updatedFields`. |
| `max_await_ms` | No | `1000` | Change-stream `maxAwaitTime` in milliseconds; also bounds cancel latency. |

The source also receives `subtask_idx` and `parallelism` from the build context (used for the single-reader behaviour described under Limitations).

### `mongo_sink`

Parsed in `register_factories.cpp` into `MongoSinkOptions` (`impls/mongodb/include/clink/mongodb/mongo_sink.hpp`).

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `uri` | No | `mongodb://localhost:27017` | MongoDB connection string. Authentication is supplied through this URI; there are no separate credential options. |
| `database` | Yes | empty | Target database. Construction throws if empty. |
| `collection` | Yes | empty | Target collection. Construction throws if empty. |
| `on_duplicate` | No | (insert) | The value `replace` turns each write into a `replace_one(filter={key_field: value}).upsert(true)`. Any other value (the default) does a plain `insert_one`. |
| `key_field` | No | `_id` | The field matched on for the upsert filter when `on_duplicate='replace'`. |
| `batch_records` | No | `1000` | Flush after this many buffered records. A value of `0` is clamped to `1`. |
| `linger_ms` | No | `0` | Linger: flush a partial batch once it is this many milliseconds old. `0` disables time-based flushing. |

## SQL usage

Not exposed through the SQL frontend. There is no `connector='mongodb'` (or similar) mapping in `src/sql/physical_plan.cpp`; use the programmatic API. Note that the sink consumes the same JSON-object record form the SQL row path produces (`row_to_json_string`), so an externally constructed JSON-string stream feeds it directly.

## Example

Based on `impls/mongodb/tests/test_mongo_live.cpp`. The sink writes JSON-object records with upsert-by-`_id` so a replayed or repeated key replaces rather than duplicates.

```cpp
#include "clink/mongodb/mongo_sink.hpp"

using clink::mongodb::make_mongo_sink;
using clink::mongodb::MongoSinkOptions;

MongoSinkOptions o;
o.uri = "mongodb://localhost:27017";
o.database = "test";
o.collection = "events";
o.upsert = true;          // on_duplicate='replace': replace_one upsert by key_field
o.key_field = "_id";

auto sink = make_mongo_sink(o);
sink->open();

clink::Batch<std::string> b;
b.emplace(std::string{R"({"_id":1,"v":"a"})"});
b.emplace(std::string{R"({"_id":2,"v":"b"})"});
sink->on_data(b);
sink->flush();

sink->close();
```

The CDC source is built with `make_mongo_cdc_source(MongoCdcOptions)`; set `uri`, `database`, and `collection`, call `open()`, then drive it with `produce(emitter)`. Each emitted record is a flat JSON object with the document image plus `__op`, `__ns`, and `__id`.

## Delivery semantics

Both ends are at-least-once.

- Source: the checkpoint cursor is the change-stream resume token, persisted on `snapshot_offset` and restored before `open()`. On restart the stream resumes after the checkpointed token, so events between the last checkpoint and a crash replay and the downstream sink must dedup. A resume token is only valid while the oplog still covers it; if the oplog has rolled past it the resume fails loudly on the first read (no silent gap) and a fresh start is needed.
- Sink: records are buffered and flushed on the count or linger threshold and on every checkpoint barrier. A failed flush throws, so the job replays the buffered batch from the last checkpoint. With `on_duplicate='replace'` and a stable `key_field`, the write is an idempotent `replace_one(...).upsert(true)`, which is effectively-once by key. A plain insert is at-least-once (replay re-inserts). Note one caveat in the code: under `on_duplicate='replace'`, a record that lacks `key_field` falls back to a plain insert and is therefore at-least-once, not idempotent. The bulk write is ordered, so the first error surfaces with no silent partial drop.

## Limitations

- The CDC source requires a replica set or sharded cluster; MongoDB change streams are not available on a standalone `mongod`.
- The change stream is a single reader. Only subtask 0 watches; other subtasks are dormant and emit nothing, so a parallel job does not open N duplicate streams. The source is therefore effectively single-reader regardless of parallelism.
- Source records are JSON-object strings only (the string channel); there is no typed or columnar source. Non-row change events (drop, rename, invalidate, `dropDatabase`, and similar) carry no document image and are skipped.
- The sink expects each input record to be a well-formed JSON object string. Malformed JSON throws during the flush.
- Reserved metadata keys (`__op`, `__ns`, `__id`) are added with no-overwrite semantics, so a data field literally named `__op` keeps its own value rather than the change metadata.
- Authentication and TLS options are expressed only through the connection URI; there are no separate credential or TLS parameters.
- Not exposed through the SQL frontend.

## Testing

A live integration test lives in `impls/mongodb/tests/test_mongo_live.cpp` and is gated on the environment variable `CLINK_MONGODB_TEST_URI`. It is skipped unless that variable points at a replica-set MongoDB (change streams require one). The test docstring suggests a `mongo:7 --replSet rs0` instance with `rs.initiate()`. The suite proves change-stream insert/update/delete produce the matching `__op` events, that a resume-token checkpoint resumes without gap or replay, that a burst larger than the per-`produce()` batch cap has no duplicate at the boundary, and that the sink upsert replaces rather than duplicates.

```bash
export CLINK_MONGODB_TEST_URI='mongodb://localhost:27017/?replicaSet=rs0'
ctest --test-dir build -L mongodb
```

A pure unit test, `test_mongo_event.cpp`, exercises the change-event-to-JSON mapping against hand-built events with no live MongoDB, and `test_factory_registration.cpp` confirms both factories are reachable through the registry on the string channel. These run without `CLINK_MONGODB_TEST_URI`.
