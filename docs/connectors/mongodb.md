# MongoDB

> Reads a collection's change stream (`mongo_cdc_source`) and writes documents into a collection (`mongo_sink`, inserts; `mongo_upsert_sink`, replacements by key), all on the string channel, and from SQL as `connector='mongodb'`, with every declared column converted strictly between BSON and its SQL type. The source captures changes only, from when it first opens; it takes no snapshot.

## Overview

The connector uses the mongo-cxx-driver (`mongocxx` plus `bsoncxx`). The source watches a collection, a database or the whole deployment through a MongoDB change stream and emits each insert, update, replace and delete as one flat JSON object: the event's document fields at the top level, plus `__op`, `__ns`, `__id` and `__image` metadata. It reads changes from the moment it first opens; what the collection held before then is not read. The sinks take JSON-object records, parse each to BSON, and write them into a collection in batches: `mongo_sink` inserts, and `mongo_upsert_sink` replaces the document with the record's key. Records flow on the `std::string` (JSON) channel; there is no Arrow or Parquet involvement.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| mongo-cxx-driver (`mongocxx` + `bsoncxx`) and mongo-c-driver | Built from source into the toolchain image by `scripts/install-connector-deps.sh` (Debian no longer packages the C++ driver); brew `mongo-cxx-driver` on macOS | Pinned in `scripts/versions.env`: C++ driver 4.4.0, C driver 2.3.1 |
| MongoDB server | Change streams need a replica set or sharded cluster | 4.0.7 or later for the source (it takes the stream's position when it opens); the gate runs `mongo:7` |

The build discovers the driver three ways, most portable first: a pkg-config imported target (`libmongocxx1`), the driver's own CMake CONFIG package (`mongo::mongocxx_shared`), or a manual header and library probe under the usual Homebrew and `/usr/local` prefixes (see `impls/mongodb/CMakeLists.txt`).

## Enabling it

The connector is gated by the CMake cache variable `CLINK_WITH_MONGODB`, which defaults to `AUTO`:

- `AUTO`: build the connector only if mongo-cxx-driver is found, otherwise skip it quietly.
- `ON`: require the driver; configuration fails if it is not found.
- `OFF`: do not define the target at all.

```bash
cmake -S . -B build -DCLINK_WITH_MONGODB=ON
cmake --build build --parallel 10
```

When enabled, the build defines `CLINK_HAS_MONGODB` and reports `clink::mongodb - enabled`.

## Factories

| Factory name | Direction | Record type | Capability record |
| --- | --- | --- | --- |
| `mongo_cdc_source` | Source | `std::string` (JSON change rows) | `mongo_cdc`: replays from its resume token, at least once |
| `mongo_sink` | Sink | `std::string` (JSON objects) | `mongo`: at least once |
| `mongo_upsert_sink` | Sink | `std::string` (JSON objects) | `mongo_upsert`: duplicates collapse on the key, under the conditions below |

All three are registered on the `string` channel in `impls/mongodb/src/register_factories.cpp`. Each refuses, when the job is built, a value outside its option's domain, naming the option.

## Configuration

### `mongo_cdc_source`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `uri` | No | `mongodb://localhost:27017` | Connection string. Credentials and TLS are supplied through it (for example `mongodb://user:pass@host:27017/?authSource=admin&tls=true`); there are no separate options. Errors do not repeat it. |
| `database` | No | empty | Watch scope. Empty watches the whole deployment. |
| `collection` | No | empty | Empty watches the whole `database`. A collection without a database is refused. |
| `full_document` | No | `updateLookup` | What an update event carries: `updateLookup` (`lookup` too), `whenAvailable`, `required`, or `off` (`false` too). See [update images](#update-images). |
| `max_await_ms` | No | `1000` | The change stream's `maxAwaitTime`, the longest a read waits, and how long one pass reads before it hands its events over, so it bounds how long a checkpoint barrier or a cancel waits. At least 1. |

### `mongo_sink` and `mongo_upsert_sink`

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `uri` | No | `mongodb://localhost:27017` | Connection string, as for the source. A write concern below `w=majority` in it is refused (see [delivery](#delivery-and-recovery)). |
| `database` | Yes | (none) | Target database. |
| `collection` | Yes | (none) | Target collection. |
| `key_field` | No | `_id` | `mongo_upsert_sink` (and `mongo_sink` with `on_duplicate='replace'`): the top-level field a record replaces by. A dotted path or an operator name is refused. `mongo_sink` without `on_duplicate='replace'` refuses it. |
| `on_duplicate` | No | `insert` | `mongo_sink` only: `insert`, or `replace` to write as `mongo_upsert_sink` does (its record still says at least once). |
| `batch_records` | No | `1000` | Write once this many records are buffered. At least 1. |
| `max_bytes` | No | `16777216` | Write once the buffered JSON reaches this many bytes. At least 1. |
| `linger_ms` | No | `0` | Write a partial batch once it is this old; `0` turns this off. |
| `write_timeout_ms` | No | `30000` | The `wtimeout` of the majority write concern, unless the URI sets one: a batch that cannot reach a majority fails within this bound. At least 1. |

Every barrier writes out what the sink buffered before it, and the checkpoint completes only after that write returns.

## Change rows

Each change event becomes one JSON object: the document fields, then four metadata fields.

| Field | Value |
| --- | --- |
| `__op` | `insert`, `update`, `replace` or `delete` |
| `__ns` | `"<database>.<collection>"` |
| `__id` | the changed document's `_id`, as the event's document key carries it (an ObjectId stays `{"$oid": "..."}`) |
| `__image` | what the document fields are: `full`, `lookup`, `partial` or `key` (below) |

| Operation | Document fields | `__image` |
| --- | --- | --- |
| insert, replace | the whole document as the change left it | `full` |
| update with a post-image (`whenAvailable` or `required` and the collection keeps post-images) | the whole document as this change left it | `full` |
| update with `updateLookup` | the document as the server found it when it looked it up, after the event | `lookup` |
| update with no document to read: `off`, a document deleted before its lookup, or `whenAvailable` without a post-image | only the fields the update set, top-level or dotted as the server names them; removed fields are not carried | `partial` |
| delete | the document key | `key` |

Values are relaxed Extended JSON: an ObjectId is `{"$oid": ...}`, a date `{"$date": ...}`, a decimal `{"$numberDecimal": ...}`, and an integer a plain number. A 64-bit integer small enough for 32 bits is written as a plain number, so a sink that parses the row again stores it as a 32-bit integer.

A document with a top-level field named `__op`, `__ns`, `__id` or `__image` stops the source with an error naming the field and the document, rather than letting the field pass for metadata.

### Update images

`updateLookup` does not return the update's own result. The server looks the document up when it reads the event, so the image can already carry later changes, and once the document is deleted there is nothing to look up: the row then carries only the fields the update set, marked `partial`. `MongoSourceRecoveryLive.UpdateLookupReadsTheDocumentAfterTheEvent` shows both: two updates read together both carry the second one's value, and an update followed by a delete carries only its own fields.

`whenAvailable` and `required` read the post-image MongoDB keeps for each change, on a collection created or modified with `changeStreamPreAndPostImages: {enabled: true}` (MongoDB 6.0 and later). Each update then carries the document as that change left it. `required` fails the stream when a post-image is missing; `whenAvailable` falls back to `partial`.

## Delivery and recovery

### Source

The checkpoint holds the change stream's resume token, and a restored source resumes after it, so the events after the checkpoint are delivered again: at least once. The token a checkpoint records covers only events a `produce()` pass already handed to the job. A pass reads at most 1,024 events, for at most `max_await_ms` plus one wait for an event, and never reads one it does not emit, so the token after it is that of the last event emitted or, once the stream has nothing more, the stream's position after the batch it read (`impls/mongodb/src/change_pass.hpp`). Barriers go in between passes. While changes kept arriving, a pass bound only by its cap read until the cap filled: at about 400 changes a second one held back the barriers for 4.2 seconds. The source used to read one event past the cap into the driver before it stopped, and the token then covered that event: a restore from a checkpoint taken there skipped it. Against the previous source, a restore from a checkpoint at the cap delivered events 1026 to 1100 of 1100 and lost 1025.

The source takes the stream's position as soon as it opens, so every checkpoint holds a token, the first included. A checkpoint taken before the first pass used to hold none, and a restore from it started at the current time: against the previous source it delivered none of the two changes made after the checkpoint.

In a checkpointed cluster job the source also records, when it first opens, the token its stream started from, in the job's checkpoint directory (`_jobs/<job>/receipts/mongo-cdc-start-op<operator>-run<floor>`, keyed by the run's checkpoint-id floor). A restart before any checkpoint of the run completed has no checkpoint to restore; the source resumes from that record instead of the current time, so the changes since the run started are read again. A new submission of the job has a new floor and starts at the current time. If the record cannot be written, the source fails to open rather than leave such a restart to skip changes.

The source never falls back to the current time:

- a restore whose state holds no token fails, naming the checkpoint;
- a token the server cannot resume from fails the restore: one whose history the oplog no longer holds (server code 286), one that is not this stream's (260), or one the server cannot find (280);
- a drop, rename, dropDatabase or invalidate event stops the source with an error naming it, since documents are removed or moved without a delete for each, or the stream ends; so does an event type the source does not know. To continue, start the job without restoring the source's state; it then starts at the current time.

The source and the sinks each connect through a client pool of their own. A pooled client waits for a server for up to `serverSelectionTimeoutMS` (30 seconds unless the URI says otherwise), so across an outage that ends within it the change stream resumes itself and a write waits for the server. A single client gave up at its first failed try: a server down for about ten seconds failed every restart at once, and the job spent its whole restart budget and failed. A stream error the driver cannot resume from, a longer outage among them, fails the `produce()` call, and the job restarts from its last checkpoint.

### Sinks

Every write uses `w=majority`: a checkpoint completes once the sink's batch is acknowledged, and a write acknowledged by fewer members can be rolled back in a failover after that checkpoint completed. `open()` refuses a URI that asks for `w=0`, `w=1`, a node count or a tag. The write's `wtimeout` is the URI's, or `write_timeout_ms`.

Any failure fails the flush, so the checkpoint does not complete and the job replays from the last completed one; nothing of a failed batch counts as written:

- a write concern error, though the server applied the batch: the replay writes the same records again (`MongoSinkLive.AWriteConcernErrorFailsTheFlushAndTheReplayConverges`);
- a connection dropped before the reply: with retryable writes (the driver's default) the driver sends the batch again under the same transaction number, which the server applies once; with `retryWrites=false` the flush fails and the replay converges (`MongoSinkLive.ADroppedConnectionIsRetriedOrFailsTheFlush`);
- a batch stopped part way: an ordered replacement batch stops at its first failing record, with the records before it written and those after not (`MongoSinkLive.AReplacementBatchStoppedPartWayFailsTheFlush`); an unordered insert batch attempts every record and reports every error.

**`mongo_sink` (insert).** At least once. A record without an `_id` gets a fresh one from the driver, so a replay inserts it again. A record that carries its own `_id` and is already stored exactly as it arrives is a write the sink made before a restore, so it is written without error, and the sink logs how many it recognised. A different document under a stored `_id`, or a duplicate on another unique index, fails the flush, naming the `_id`.

**`mongo_upsert_sink` (replace by key).** Each record replaces the document whose `key_field` equals its own, inserting it if there is none, in an ordered batch. Duplicates collapse onto the key only under these conditions, which the sink checks:

- every record carries `key_field` as a string, a 32- or 64-bit integer or an ObjectId, checked for the whole batch before anything of it is written. A null would match every document lacking the field, an array any document whose array holds the value, a floating-point number compares by value across numeric types, and an embedded document only in the same field order;
- a `key_field` other than `_id` has a unique index on that field alone, with no partial filter; `open()` refuses the sink without one, naming the index to create.

And under these, which it cannot check:

- a key converges to its latest record only within one sink subtask, or across subtasks when the stream reaching the sink is partitioned by the key;
- a replay rewrites keys to earlier records before it reaches their latest ones, so a reader can see a key go back and then forward again.

The record says `EffectivelyOnceIdempotent` on `key_field`, never exactly once.

## SQL

A table declared `connector='mongodb'` reads a change stream as a source and writes a collection as a sink. Every column is a top-level document field, typed: the declaration decides each value's SQL type, and a value it cannot hold exactly is refused, never rounded or carried as text. [`examples/mongodb-orders-sql`](https://github.com/orhaugh/clink/tree/main/examples/mongodb-orders-sql) runs the pipeline below end to end and checks its output against MongoDB.

```sql
CREATE TABLE order_changes (
    __op      VARCHAR,
    __id      VARCHAR,
    customer  VARCHAR,
    amount    DECIMAL(10, 2),
    placed_at TIMESTAMP(3)
) WITH (connector = 'mongodb', uri = 'env://MONGODB_URI',
        database = 'shop', collection = 'orders');

CREATE TABLE customer_totals (
    _id    VARCHAR PRIMARY KEY,
    orders BIGINT,
    total  DECIMAL(38, 2)
) WITH (connector = 'mongodb', uri = 'env://MONGODB_URI',
        database = 'shop', collection = 'customer_totals', mode = 'upsert');

INSERT INTO customer_totals
SELECT customer AS _id, COUNT(*) AS orders, SUM(amount) AS total
FROM order_changes WHERE __op = 'insert' GROUP BY customer;
```

### As a source

One row per change event, appended: an insert, update, replace or delete each becomes a row of the declared columns, filled from the event's document fields as the [change rows](#change-rows) describe them, with the event's kind in `__op`. The table is append-only, not a changelog. A change stream carries no before-image of an update, so there is no earlier row to retract; a query that needs the current state of each document reads the events with that in mind, as the example's totals do by summing the insert events, which no later change alters. `mode` other than `append` is refused.

The metadata are declared like any column, under reserved names: `__op`, `__ns` and `__image` as `VARCHAR`, and `__id`, the document's `_id`, as any type below that holds it. Another declared name starting with `__` is refused, and a document field named like the metadata stops the source, as it does outside SQL. A field the document does not have is NULL. `_id`, which cannot change, is filled from the event's key where the image lacks it (a `partial` update).

Each value is read from canonical Extended JSON, which keeps every BSON type apart, and converted for its column (`impls/mongodb/src/sql_row.hpp`):

| Declared type | BSON types it takes | Refused |
| --- | --- | --- |
| `BIGINT` | int32, int64 | double, decimal |
| `INTEGER`, `SMALLINT` | int32, int64 within the type's range | a value outside it |
| `DOUBLE` | double; int32; int64 within 2^53 | NaN and the infinities; an int64 past 2^53 |
| `REAL` | a double a float holds exactly; int32 within 2^24 | a double it would round |
| `DECIMAL(p, s)` | decimal128, int32, int64, kept as written | more than `p - s` integer or `s` fraction digits; NaN and the infinities; a double |
| `BOOLEAN` | bool | |
| `TIMESTAMP(p)`, `TIMESTAMP(p) WITH TIME ZONE` | date, as its UTC milliseconds | |
| `DATE` | a date at UTC midnight | a date with a time of day |
| `VARCHAR` | string; ObjectId (its 24 hex digits); int32 and int64 (their digits); an embedded document or array (its canonical Extended JSON text) | double, decimal, date, bool |

Binary data, BSON timestamps, regular expressions, code and the other BSON types are refused for every column, and so is a SQL type outside this table (`TINYINT`, `TIME`, `BYTEA`, arrays, maps and rows). A refused value stops the job with an error naming the namespace, the document's `_id`, the field, its BSON type and the column, and saying what to declare instead:

```text
mongo_cdc_source: change on shop.orders to document ObjectId(6aed4a20f2cc9658710ee2f9): field 'qty' is a double, which column qty (BIGINT) cannot hold; declare it DOUBLE
```

A restart meets the same event again; the way past it is a declaration that holds the value.

The source is one reader. The planner places it on a single subtask and keeps every operator behind it that is not keyed there too, so its events stay in order, and per key through a keyed operator such as a `GROUP BY`. The options it reads are `uri` (required), `database`, `collection`, `full_document` and `max_await_ms`, with the meanings [above](#mongo_cdc_source); any other is refused, naming the ones it reads.

### As a sink

Without `mode='upsert'`, `mongo_sink` inserts each row; with it, `mongo_upsert_sink` replaces the document whose key equals the row's, keyed by the table's PRIMARY KEY, which must be one column. A delete row removes the key's document and an `update_before` row is skipped, its `update_after` replacing the document, so a query that retracts, such as a top-N, keeps the collection equal to its answer. The key column must be `VARCHAR`, `BIGINT`, `INTEGER` or `SMALLINT`; a key other than `_id` needs a unique index on that field alone, as [above](#sinks). An append-only sink refuses a query that retracts, as for every connector.

Each column is written as the BSON type its SQL type maps to: `BIGINT` as int64, `INTEGER` and `SMALLINT` as int32, `DOUBLE` and `REAL` as double (NaN and the infinities as themselves), `DECIMAL` as decimal128 with the row's digits, `BOOLEAN` as bool, `TIMESTAMP` as a date, `DATE` as a date at UTC midnight and `VARCHAR` as a string; NULL is written as null. The declared columns are the document's fields, in declared order; a name starting with `__` is an ordinary field here. A value its type cannot hold fails the flush.

The options it reads are `uri` (required), `database` and `collection` (both required), `batch_records`, `max_bytes`, `linger_ms` and `write_timeout_ms`. It refuses `key_field` and `on_duplicate` (the PRIMARY KEY and `mode` decide), `delivery_guarantee='exactly_once'`, `changelog='true'`, `json_non_finite` and a full-refresh materialised view.

The guarantees are the sinks' own, [above](#sinks). Inserts are at least once; a row with an `_id` column is recognised when a replay writes it again, and one without is inserted again. Replacements converge on each key's latest row when a key's rows reach the sink in order, which holds behind a MongoDB source and through a keyed `GROUP BY`. The delivery analyser the Coordinator runs at submission reports a job into the upsert sink as `EFFECTIVELY_ONCE_REQUIRES_IDEMPOTENT_KEY`, with a warning that the key must identify a row, and one into the insert sink as `STATE_EXACTLY_ONCE_OUTPUT_AT_LEAST_ONCE`; neither as exactly once.

### Credentials and TLS

They go in the connection string, as outside SQL. Written `uri = 'env://MONGODB_URI'`, the string is read from each Worker's environment when the job is built there, so it is not part of the SQL, the submitted job or what the Coordinator serves; a variable that is unset or empty is refused when the job is built. A literal `uri` with a password in it is stored with the job like any other option. No error or log line of the connector repeats the URI.

## Limitations

- Changes only: the source reads from when it first opens and takes no snapshot of what the collection already holds.
- A single reader: only subtask 0 watches; other subtasks are dormant. There is no partitioning of a change stream across subtasks.
- A resume token the oplog no longer covers cannot resume; the job fails instead of skipping the gap.
- Drop, rename, dropDatabase and invalidate stop the source; so does a document field named like the metadata.
- `updateLookup` images are read after the event (see above).
- The start record that covers a restart before the first checkpoint needs a checkpointed cluster job; an in-process run without a checkpoint directory has none.
- Relaxed Extended JSON carries a small 64-bit integer as a plain number, so it comes back as a 32-bit integer when parsed again.
- The change stream needs a replica set or sharded cluster. A sharded collection's replacement needs its shard key in the filter, which `mongo_upsert_sink` does not add.
- Credentials and TLS are expressed only through the connection URI.
- Each sink must be the only sink on its chain: at every checkpoint barrier it writes out what it received before the barrier, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- In SQL: append-only change rows, with no changelog form; no type for binary data, BSON timestamps, regular expressions or the other BSON types, and nested documents and arrays only as `VARCHAR` text; an ObjectId read as `VARCHAR` is written back as its hex string, not an ObjectId; a single-column key for the upsert sink; no exactly-once.

## Testing

`scripts/mongodb-live.sh` is the connector's gate. It starts a single-member replica set of `docker/integration-services.yml`'s `mongodb` service (`mongo:7`, with `enableTestCommands` so the sink's failure cases can arm the server's `failCommand` fail point) on a fixed free loopback port, runs every suite below against it, and fails on a skipped case, a suite that ran fewer cases than it has, a missing binary or a server that never becomes primary. It keeps each run's report and output, the recovery cells' coordinator and worker logs, and a `provenance.txt` naming the commit, the build's configuration and the server, and no credentials. The `mongodb` CI workflow runs it.

| Suite | Where | What it holds |
| --- | --- | --- |
| `MongoEvent`, `MongoChangePass`, `MongoSourceRestore`, `MongoSinkRules`, `MongoFactoryRegistration`, `MongoCapabilityRecords` | `clink_mongodb_tests`, no server | the change rows and their metadata, the events that stop the source, the checkpoint-token rule against a stand-in cursor, the refusal to start at the current time, the sink's key, write concern and index rules, option validation, the records |
| `MongoLive` | `clink_mongodb_tests`, live | round trips: insert, update and delete events, a resume without gap, a burst across the per-pass cap, the sink's writes and replays |
| `MongoSourceRecoveryLive` | `clink_mongodb_tests`, live | checkpoints at the cap, after an idle pass and before the first pass, each restored by a second source that must deliver exactly the events after it; an unresumable token; drop, rename and a metadata collision; `updateLookup` and post-images |
| `MongoSinkLive` | `clink_mongodb_tests`, live | a replacement without its key, the unique index, write concerns below majority, a write concern error, a dropped connection, a batch stopped part way, per-key order, the byte bound, a password kept out of errors |
| `MongoSqlRow`, `MongoSqlSink` | `clink_mongodb_tests`, no server | every BSON-to-SQL pair the source converts and every one it refuses, the metadata columns, the declared-types rules; the sink's SQL-to-BSON writes, exact decimals, non-finite doubles, row kinds and refusals |
| `MongoSqlPlan` | `clink_mongodb_sql_tests`, no server | the source op (one reader, its declared types, every unkeyed operator behind it on one subtask), the insert and upsert sink ops, a retracting query needing the upsert sink, the analyser's verdict on each sink, and the plans and options refused |
| `MongoSqlLive` | `clink_mongodb_sql_tests`, live | embedded SQL pipelines: inserts, an update and a delete read as typed rows and written back as typed BSON; a value its column cannot hold failing the job; an upsert sink replacing and deleting by key under a top-N, and a decimal sum written exactly |
| `MongoRecovery` | `clink_integration_tests`, live | a coordinator and two workers running the source into a sink while a writer changes the collection: a worker killed before a write, after a write and with change events in flight (each after a checkpoint completed), a worker killed before any checkpoint, the server restarted, a mirror by key, and the same mirror as a SQL job submitted by `clink_submit_sql`; each verified by reading MongoDB directly, each kill landing while the source held documents or changes the destination did not |

```bash
scripts/mongodb-live.sh build
```

`ctest --test-dir build -L mongodb` runs the suites without a server; the live ones skip there.

The gate passed on commit `9acee127`, before the SQL support, in a local run on 2026-11-06: 56 of 56 cases, the six recovery cells among them. An earlier run on the same commit failed two cells because the machine suspended mid-cell and the workers' leases expired; it was run again with sleep prevented. The `mongodb` CI workflow has not yet run the gate.
