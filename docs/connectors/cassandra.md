# Apache Cassandra / ScyllaDB

> Two sinks that write JSON-object records into a Cassandra or ScyllaDB table: `cassandra_sink_string` inserts each record, and `cassandra_upsert_sink_string` maintains a table from a changelog, inserting and deleting by the table's primary key. Both bound the writes they hold, order every write to a row by a timestamp of their own, and fail rather than acknowledge a checkpoint over a write whose outcome is unknown. There is no source.

## Overview

Apache Cassandra and ScyllaDB share the CQL binary protocol, and the connector speaks it through the DataStax C/C++ driver (`cassandra.h`). Each record is a JSON object string, such as the output of the SQL `row_to_json_string` bridge, written with a prepared `INSERT INTO "<keyspace>"."<table>" JSON ?`: the server maps the object's fields to columns by name, as CQL folds them to lower case, and coerces each value to its column's type. Columns a record leaves out are written as null. The upsert sink deletes with a prepared `DELETE FROM "<keyspace>"."<table>" WHERE "<k1>" = fromJson(?) AND ...`, binding each key column's value as the record wrote it, so the server reads a key the same way for a delete as for an insert.

The sinks' code: the record and key rules in `impls/cassandra/src/cassandra_rules.cpp`, the bounded window of writes, the timestamps, the failed state and the table check in `impls/cassandra/src/sink_core.cpp`, the two sinks in `cassandra_sink.cpp` and `cassandra_upsert_sink.cpp`, and the driver calls behind the `CassandraWriteService` interface (`include/clink/cassandra/cassandra_service.hpp`) in `driver_service.cpp`. Tests substitute a model of the server for that interface.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| DataStax C/C++ driver (`cassandra-cpp-driver`, C API) | Debian image: built from source (CMake, needs libuv); macOS host: `brew install cassandra-cpp-driver` | 2.17.1 (pinned, `scripts/versions.env`) |
| libuv | Build dependency of the driver on the Debian image | system package, not pinned by clink |

The driver has no Debian package and no prebuilt arm64 artefact, so the image builds the exact tag from source while the host uses Homebrew at the same version. clink uses only the C-linkage API, so the from-source and Homebrew builds are ABI compatible.

The servers the gate runs against, pinned by digest in `scripts/cassandra-live.sh`, are Apache Cassandra 4.1.12 and ScyllaDB 6.2.3. Other versions speaking CQL native protocol v4 or later are likely to work but are not tested.

## Enabling it

The build option is `CLINK_WITH_CASSANDRA`, one of `AUTO` (default), `ON` or `OFF` (`CMakeLists.txt`). Under `AUTO` the build probes for the `cassandra.h` header and the `cassandra` library in the usual Homebrew and system locations and builds the target when both are found; under `ON` a missing driver fails the configure.

```bash
cmake -S . -B build -DCLINK_WITH_CASSANDRA=ON
```

## Factories

| Factory name | Record | What it writes |
| --- | --- | --- |
| `cassandra_sink_string` | `std::string`, a JSON object | Each record, as it is, with `INSERT ... JSON` |
| `cassandra_upsert_sink_string` | `std::string`, a JSON object with an optional `__row_kind` | Inserts and deletes by the table's primary key, netted while buffered |

## Configuration

Options are parsed in `impls/cassandra/src/register_factories.cpp` into `CassandraSink::Options`, `CassandraUpsertSink::Options` and `CassandraConnParams`. The sinks' constructors check the same rules, so a sink built in C++ and one built from SQL refuse the same values. Numbers are read whole: `9042x` or `abc` is refused, not read as 9042 or as the default.

| Option | Default | Range | Description |
| --- | --- | --- | --- |
| `contact_points` | `127.0.0.1` | | Comma-separated hosts. |
| `port` | `9042` | 1 to 65535 | The CQL native port. |
| `username`, `password` | none | both or neither | Password authentication. `env://VAR` reads a value from the worker's environment. |
| `keyspace` | required | | A CQL unquoted name: a letter, then letters, digits or underscores, at most 48 characters, folded to lower case. |
| `table` | required | | As `keyspace`. |
| `consistency` | `local_one` | `one`, `two`, `three`, `quorum`, `all`, `local_quorum`, `each_quorum`, `local_one` | The level each write must reach before the server acknowledges it. `any`, `serial` and `local_serial` are refused. |
| `connect_timeout_ms` | `5000` | 1 to 600000 | How long connecting may take. |
| `request_timeout_ms` | `12000` | 1 to 600000 | How long the driver waits for a node's answer to one write before it fails it as timed out. It also bounds how long closing waits for writes in flight. |
| `max_in_flight` | `1024` | 1 to 8192 | Writes handed to the server and not yet acknowledged, at most. 8192 is the driver's pending-request queue, which the sink never fills. |
| `max_in_flight_bytes` | `67108864` (64 MiB) | 1024 to 1073741824 | Record bytes in flight, at most; a larger record goes alone. |
| `key_columns` | required, upsert only | | The table's whole primary key, comma-separated. SQL sets it from the table's `PRIMARY KEY`. |
| `max_buffered_records` | `10000` | 1 to 1000000 | Upsert only: rows buffered, netted by key, before an early write-out. |
| `max_buffered_bytes` | `67108864` (64 MiB) | 1024 to 1073741824 | Upsert only: buffered bytes before an early write-out; a larger record is buffered alone and written at once. |

The bounds are on the records the sink holds, not on the process's memory: the driver, the batch being processed and the runtime use more.

## The table

The sinks do not create keyspaces or tables. When it opens, a sink connects, reads the table from the driver's schema metadata and refuses to start when the keyspace or the table does not exist. The upsert sink also refuses to start, naming the table's partition key and clustering columns, unless `key_columns` names exactly those columns, in any order:

- a subset would make a delete by part of the key remove other rows, and records differing only outside it would be netted as one row;
- a column outside the key would never match the table's identity.

A key column must be of a native scalar type: `tinyint`, `smallint`, `int`, `bigint`, `varint`, `decimal`, `float`, `double`, `text`, `varchar`, `ascii`, `boolean`, `uuid`, `timeuuid`, `timestamp`, `date`, `time`, `inet` or `blob`. A frozen collection, tuple, user-defined or custom type in the key is refused at open.

Names are CQL unquoted identifiers, folded to lower case and written double-quoted, so a column whose name is also a CQL keyword still works. A name that needs quoting, such as a mixed-case name created quoted, is refused.

## Records

`cassandra_sink_string` sends each record as it is, unread. The server refuses what it cannot map: an unknown column, a value of the wrong type, malformed JSON.

`cassandra_upsert_sink_string` checks every record of a batch before buffering any of it, and refuses, naming the record's position, the rule and the column but never a value, a record that:

- is not one JSON object, is not valid JSON, or holds a number clink's JSON reader cannot read (an integer of 2^64 or more, a number past a double's range);
- has two fields naming one column once folded to lower case. The server would keep one of them without saying which;
- carries a `__row_kind` other than `"insert"`, `"update_after"`, `"delete"` or `"update_before"`. A record without one is an insert; anything else is refused rather than written as an insert;
- lacks a key column, has it null, or has a value outside its type: an integer type takes a JSON integer within its range, `varint` a JSON integer, `decimal` a JSON number or string, `float` and `double` a JSON number, `text` a JSON string, `ascii` a string of ASCII characters, `boolean` `true` or `false`, `uuid` and `timeuuid` a string in the 8-4-4-4-12 hex form (a `timeuuid` of version 1), `timestamp`, `date` and `time` a string or an integer as CQL's `fromJson` reads them, `inet` a string, and `blob` a string of `0x` and hex digit pairs.

The insert carries the record's own text: unchanged when it has no `__row_kind`, and otherwise its other fields, each name and value exactly as written. No number passes through a double on its way out. Key values with Unicode, NUL, newlines or quotes are bound as the record wrote them.

A refused record fails the sink, and fails the job again each time the replay reaches it: there is no dead-letter option.

## Writes and bounds

Writes are asynchronous. The plain sink hands each record to the driver as it arrives; the upsert sink buffers the latest record per row and hands the buffer over at a barrier, or earlier when it reaches `max_buffered_records` rows or `max_buffered_bytes`. A write waits for room when `max_in_flight` writes, or `max_in_flight_bytes` of record text, are outstanding, so a slow server holds the input back rather than filling memory. A barrier returns once every write before it is acknowledged.

An early write-out keeps the changelog's meaning. Rows are written in the order of the records that last set them, and every write carries a timestamp from the sink's own clock, in microseconds and strictly increasing within the sink instance. The server keeps, for a row, the write with the highest timestamp, whatever order the writes arrive in. So a row's later record always wins over an earlier one: an insert then a delete leaves the row absent, a delete then an insert leaves it present with the insert's values, across early write-outs and barriers alike. An insert writes every column, those the record leaves out as null, so a row never keeps a value from before a delete.

## Acknowledgement and durability

A write is acknowledged when its coordinator reports it written at the configured `consistency`. That is what a barrier waits for, and all a checkpoint promises. At `local_one`, the default, one replica in the local data centre has the write in its commit log; whether that survives the loss of the node depends on the server's commit log sync settings, and other replicas catch up later. `local_quorum` or `quorum` waits for a majority of the replicas. `any` is refused because it acknowledges a write that no replica need hold, and the serial levels are refused because they govern lightweight transactions, which the sinks do not use.

The driver settings the contract rests on, set by the sink so that they do not depend on the driver's version:

- Timestamps: every statement carries the sink's timestamp. The driver header gives two contradictory defaults for its own timestamp generator, so the sink does not use it.
- Retries: the driver's default retry policy. A write the coordinator refuses as Unavailable goes to the next host once (nothing was written); a bootstrapping node is skipped. The sinks mark no statement idempotent, so the driver resends nothing that reached a node: a write timeout, an overloaded node, a request timeout or a connection lost after the write fails it, and the sink with it. No speculative executions are configured.
- Timeouts: `request_timeout_ms` per write, `connect_timeout_ms` per connection. The server's own write timeout (`write_request_timeout` in `cassandra.yaml`, `write_request_timeout_in_ms` in ScyllaDB's `scylla.yaml`) bounds how long a coordinator waits for replicas.
- Schema metadata on, and a pending-request queue of 8192.

## Failures

| Outcome | Meaning | Examples |
| --- | --- | --- |
| Not applied | No replica wrote it | Unavailable, Overloaded, a full driver queue, no host reachable (the driver reports `No hosts available` after its retry on the next host) |
| Ambiguous | It may or may not have been applied | A write timeout or write failure from the coordinator, the driver's request timeout, a connection lost after the write was sent |
| Permanent | The request itself is wrong | An unknown column, a value the server cannot coerce, a missing table, refused credentials or permissions |
| Cancelled | The task was cancelled | |

Any of them fails the sink. It gives up the writes still in flight, without waiting for them, and says how many it submitted and how many the server acknowledged since it opened, and how many buffered rows it drops. It then refuses every later record, flush and barrier, so it cannot acknowledge a checkpoint over writes it gave up. The job's restart replays every record after the last completed checkpoint. A cancelled task stops waiting within 50 ms.

`close()` writes nothing: a clean end of input has already flushed, and a failed or cancelled task must not write what it was handed. It releases the writes in flight without awaiting them and closes the session, which lets them finish first, within about `request_timeout_ms`. `open()` on a failed or closed sink starts afresh with a new session; on a sink that is open and has not failed it is refused.

## Delivery, ordering and the final state

Four separate properties:

- **Write attempts are at-least-once.** A restart replays every record after the last completed checkpoint, and the sinks write them again. A write whose outcome was unknown, applied or not, is written again with a newer timestamp. Stable keys make the rewrite an overwrite, not a duplicate row.
- **Records are coalesced within a buffered interval.** The upsert sink keeps only each row's latest record between write-outs; the plain sink coalesces nothing, and the server keeps the latest of its writes to a row.
- **The final state converges under stated assumptions.** Once the input stops and the replay is through, each row holds its last record's values, or is absent when that record deletes it, provided that every record of a row reaches one sink subtask in the order it was produced, the replay reproduces that order (a deterministic query over a replayable source), nothing else writes the table, and no writing process's clock runs behind the timestamp of an earlier write to the same row. A clock stepped back across a restart can let an older write win, which can leave a deleted row present.
- **Durability is that of the configured consistency level**, as above, and no more.

It does not hold when records of one row reach two sink subtasks, as with input not partitioned by the key, or when another writer changes the table: the sinks overwrite what is there, and a replay can overwrite a later change by another writer. Between a restart and the point the replay reaches, a row can show an older value than it held before the failure. Stable keys stop a replay from duplicating rows; they do not make delivery exactly-once, and the sinks make no conditional write or version check. The SQL frontend refuses an explicit `exactly_once`, and two-phase commit is not available.

## Security

Password authentication (`PasswordAuthenticator`) is supported through `username` and `password`, which must be set together. There is no TLS option: the connection is plain text, so reach the cluster over a network you trust. SQL refuses an option that looks like TLS configuration (`ssl*`, `tls*`, certificate, truststore or keystore options) rather than ignoring it.

Credentials never appear in a message the sinks write. Record values do not either: a refusal names the rule and the column, and a server's message is repeated on one line, at most 300 bytes, with every quoted span emptied and anything from a JSON parser's `[Source:` excerpt on cut, because Cassandra's messages quote the value they could not read.

## Metrics and logs

Under the connector name `cassandra`, direction `sink`:

| Metric | Counts |
| --- | --- |
| `clink_connector_writes_submitted_total` | Writes handed to the server |
| `clink_connector_records_total` | Writes the server acknowledged |
| `clink_connector_bytes_total` | Record bytes the server acknowledged |
| `clink_connector_writes_in_flight` (gauge) | Writes submitted and not yet settled |
| `clink_connector_errors_total` | Failures of the sink |
| `clink_connector_commit_latency_ns` | Time a barrier waited for the writes in flight |

The `cassandra.sink` log names the table, consistency and bounds at open, each failure with its cause, and a close that abandons writes.

## SQL usage

`connector='cassandra'` plans the plain sink, and `mode='upsert'` with a `PRIMARY KEY` plans the upsert sink, its `key_columns` taken from the key. The planner refuses an option the sink does not read, naming the ones it does, refuses `key_columns` (the key comes from `PRIMARY KEY`), the buffer bounds without `mode='upsert'`, TLS options, `changelog='true'` (the `__row_kind` marker is no column of the table) and `exactly_once`. A retracting query (GROUP BY, top rows, an outer join) needs `mode='upsert'`.

```sql
CREATE TABLE sensor_totals (
    sensor STRING,
    day    STRING,
    n      BIGINT
) WITH (
    connector = 'cassandra',
    contact_points = 'cass-1,cass-2',
    keyspace = 'telemetry',
    table = 'sensor_totals',
    mode = 'upsert',
    primary_key = 'sensor,day',
    consistency = 'local_quorum'
);
```

The Cassandra table's primary key must be exactly `(sensor, day)` in some arrangement of partition key and clustering columns, or the job stops when the sink opens.

A `DECIMAL` reaches the sink as a JSON number. Both servers read a JSON number for a `decimal` column as a double, so a value with more than about 17 significant digits is rounded by the server, though the sink sends the digits as written: 12345678901234567890.123456789 is stored as 1.2345678901234567E+19 by Cassandra 4.1.12 and as 12345678901234568000 by ScyllaDB 6.2.3. A JSON string keeps the decimal exactly on both. A NaN or infinite double arrives through the bridge as `null`, or as a string under `json_non_finite='string'`; see [NaN and infinity in JSON output](README.md#nan-and-infinity-in-json-output).

## Example

```cpp
#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/core/record.hpp"

using clink::Batch;
using clink::cassandra::CassandraSink;

CassandraSink::Options o;
o.conn.contact_points = "127.0.0.1";
o.keyspace = "clink_demo";
o.table = "sensor_out";
o.max_in_flight = 256;

CassandraSink sink(std::move(o));   // checks the options, touches no server
sink.open();                        // connects and checks the table; throws on failure

Batch<std::string> b;
b.emplace(R"({"id":1,"v":"a"})");
b.emplace(R"({"id":2,"v":"b"})");
sink.on_data(b);
sink.flush();   // waits for every write; throws, and fails the sink, on any failure
sink.close();   // writes nothing
```

## Earlier releases

Records an earlier release wrote correctly are written the same way, with the same rows. What changes:

- **Writes are bounded.** The plain sink held every write's future until the next barrier, and the upsert sink every distinct key; both now hold at most their bounds, and a full window holds the input back. Writes the driver used to refuse with a full request queue now wait.
- **A failure is terminal.** A failed flush used to clear what it held and leave the sink able to acknowledge the next barrier over the writes it dropped. It now refuses every later barrier until it opens again.
- **`close()` writes nothing.** It used to flush, and so wrote on a cancelled task too. A program driving a sink directly calls `flush()` before `close()`; the runtime already did.
- **Writes carry the sink's timestamps.** Ordering no longer depends on the driver's default generator.
- **The upsert sink checks its key and records.** A `key_columns` that is not the table's whole primary key used to be accepted, so a delete by a partition key alone removed the whole partition; it is now refused at open. An unknown `__row_kind` used to be written as an insert, two fields naming one column were written with whichever the server kept, and a delete put the key into the CQL text as a literal, which failed for a `uuid` key. All are now refused or bound. Numbers past a double's precision in the records' other fields were rounded on the way out; they now keep their digits.
- **Options are checked.** A malformed number used to fall back to its default and an unknown SQL option was ignored; both are refused now. `consistency`, `request_timeout_ms`, `max_in_flight`, `max_in_flight_bytes`, `max_buffered_records` and `max_buffered_bytes` are new. `username` without `password` is refused.
- **Metrics.** `records_total` counts acknowledged writes; it used to count the plain sink's writes as they were sent. `writes_submitted_total` and `writes_in_flight` are new.
- **Messages.** Refusals and failures no longer carry the record's text.

## Limitations

- Sink only; the sinks do not create keyspaces or tables.
- Each sink must be the only sink on its chain: at every barrier it writes out what it received before it, which holds only while it owns its chain's checkpoint, so `Dag::add_sink` refuses another sink beside it (see [checkpointing](../internals/checkpointing.md#guarantees-and-caveats)).
- At-least-once writes and final-state convergence under the assumptions above; no two-phase commit and no exactly-once delivery.
- The upsert sink needs the table's whole primary key, of native scalar types; collection, tuple, user-defined and custom key types are refused.
- Rows of one key written by two sink subtasks, or by another writer, can end at an older value.
- No TLS. Password authentication only.
- No sink-level retry: a failed or timed-out write fails the sink and the job restarts from its last checkpoint.
- A refused record fails the job on every replay; there is no dead-letter option.
- The upsert sink refuses records holding an integer of 2^64 or more anywhere, because clink's JSON reader cannot read one; the plain sink sends records unread.

## Testing

Offline, in `clink_cassandra_tests`: `CassandraRules` (names, consistency levels, key types, record preparation, message redaction) and, against a model of the server (`impls/cassandra/tests/cassandra_model.hpp`, which keeps the table by last-write-wins on the writes' timestamps and can hold writes and apply them in any order), `CassandraSinkOptions`, `CassandraSinkOpen`, `CassandraSinkBounds`, `CassandraSinkOrder` and `CassandraSinkFailure`: the options through the constructors and the factories, the table check, the bounds and backpressure, write order and netting across early write-outs, every failure class, cancellation, close and reopen, and the counts the metrics report. `SqlPhysical.CassandraOptionsAreCheckedWhenTheJobIsPlanned` covers the planner's option check.

Live, against a server: `CassandraLive` (the product and version, a long input through a small window, repeated writes to one row, deletes across early write-outs, composite keys with Unicode, NUL, newlines and integers past 2^53, the key check, a refused write, Unavailable, a write the server applied whose reply a proxy withheld, a lost connection, cancel and close, decimals), the upsert sink's cases and the `UpsertContractSuite` instantiation, all in `clink_cassandra_tests`; `CassandraSqlLive` in `clink_cassandra_sql_tests` (embedded SQL: an append query, a top row per group and a GROUP BY into `mode='upsert'`, a SQL key that is not the table's whole key); and `CassandraRecovery` in `clink_integration_tests`, four cells in which a coordinator and two workers poll a feed the test serves into the sinks, a worker is killed at a fault point (`cassandra_sink.in_flight`, `cassandra_upsert_sink.before_write`, `cassandra_upsert_sink.after_write`, the last also as a SQL job at parallelism 2), and the table is read back row by row, deleted rows included.

`scripts/cassandra-live.sh cassandra|scylladb [build-dir]` runs all of them against one pinned server and fails on any skip or short suite, keeping each run's report, the recovery cells' cluster logs and a provenance file in `build/cassandra-live-reports/<product>/`. Run the two products one after the other. The `cassandra` CI workflow runs the gate for both, one after the other; its first run passed both on commit 0080fb86, 74 of 74 each (run 42404581370).

The gate passed locally against both products on commit b373e82b, 74 of 74 each, on 2026-11-07 (reports not published): Apache Cassandra 4.1.12 and ScyllaDB 6.2.3 (`6.2.3-0.20250119.bff9ddde1283`). In each recovery cell the kill cost one restart, the restored job read 82 to 350 records again, and every row ended at its last record, the 14 rows whose last record deletes them absent. The two products behaved alike in every case, ScyllaDB answering a write to a keyspace with more replicas than nodes with Unavailable as Cassandra does. The previous sinks' defects (no bound on held writes, a failed flush that left the next barrier able to succeed, a partial key accepted for deletes, an unknown `__row_kind` written as an insert, a `uuid` key that could not be deleted) were found by reading the code and are each covered by a case above; they were not reproduced against a server.
