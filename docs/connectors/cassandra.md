# Apache Cassandra / ScyllaDB

> A sink that writes each input record (a JSON object string) into a Cassandra or ScyllaDB table. There is no source.

## Overview

The connector integrates Apache Cassandra and ScyllaDB, which share the same CQL binary protocol. It connects through the DataStax C/C++ driver using the C API (`cassandra.h`). Each input record is a JSON object string, for example the output of the SQL `row_to_json_string` codec, and the sink writes it with a prepared `INSERT INTO <keyspace>.<table> JSON ?` statement, so Cassandra maps the JSON object's fields to table columns by name and coerces the types. The single record channel is `std::string`; the sink does not use Arrow. Cassandra is a serving store rather than a streaming log, so the connector is a sink only.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| DataStax C/C++ driver (`cassandra-cpp-driver`, C API) | Debian image: built from source (CMake, needs libuv); macOS host: `brew install cassandra-cpp-driver` | 2.17.1 (pinned, `scripts/versions.env`) |
| libuv | Build dependency of the driver on the Debian image | system package, not pinned by clink |

The driver has no Debian package and no prebuilt arm64 artefact, so the image builds the exact tag from source while the host uses Homebrew at the same version. clink uses only the C-linkage API (`cassandra.h`), so the from-source and Homebrew builds are ABI compatible. Arrow is not used by this connector.

## Enabling it

The build option is `CLINK_WITH_CASSANDRA`, one of `AUTO` (default), `ON`, or `OFF` (`CMakeLists.txt`). Under `AUTO` the build probes for the `cassandra.h` header and the `cassandra` library in the usual Homebrew and system locations; if both are found the target is built, otherwise it is silently skipped. Under `ON` the configure step fails with a fatal error if the driver is not found. The driver must be present (`brew install cassandra-cpp-driver` on macOS, built from source on the Debian image).

```bash
cmake -S . -B build -DCLINK_WITH_CASSANDRA=ON
```

## Factories

| Factory name | Direction | Record type |
| --- | --- | --- |
| `cassandra_sink_string` | sink | `std::string` (JSON object per record) |

## Configuration

All options are parsed in `impls/cassandra/src/register_factories.cpp` and map onto `CassandraSink::Options` / `CassandraConnParams` (`impls/cassandra/include/clink/cassandra/cassandra_sink.hpp`, `connection_params.hpp`).

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `contact_points` | No | `127.0.0.1` | Comma-separated list of cluster contact-point hosts. |
| `port` | No | `9042` | CQL native transport port. |
| `username` | No | `""` | Authentication username. Leave blank for a cluster with no authentication. |
| `password` | No | `""` | Authentication password. Leave blank for a cluster with no authentication. |
| `connect_timeout_ms` | No | `5000` | Connection timeout in milliseconds. |
| `keyspace` | Yes | none | Target keyspace. The factory throws if empty. |
| `table` | Yes | none | Target table. The factory throws if empty. |

The sink does not provision schema. The keyspace and table must already exist with column names matching the JSON object's fields.

## SQL usage

Mapped in `src/sql/physical_plan.cpp` as `connector='cassandra'`. Each row is encoded to a JSON object string (`row_to_json_string`) and inserted on the `cassandra_sink_string` factory.

```sql
CREATE TABLE sensor_out (
    id  INT,
    v   STRING
) WITH (
    connector = 'cassandra',
    contact_points = '127.0.0.1',
    port = '9042',
    keyspace = 'clink_demo',
    table = 'sensor_out'
);
```

`mode='upsert'` with a PRIMARY KEY selects the changelog-aware upsert sink (`cassandra_upsert_sink_string`, see Delivery semantics), which applies deletes as well as upserts so a retracting query can maintain the table. `exactly_once` (2PC) is rejected; the default sink is at-least-once (idempotent for a stable primary key).

## Example

Based on `impls/cassandra/tests/test_cassandra_live.cpp`. The keyspace and table are assumed to already exist (the sink does not create schema).

```cpp
#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/core/record.hpp"

using clink::Batch;
using clink::cassandra::CassandraSink;

CassandraSink::Options o;
o.conn.contact_points = "127.0.0.1";
o.conn.port = 9042;
o.keyspace = "clink_demo";   // required
o.table = "sensor_out";      // required

CassandraSink sink(std::move(o));   // throws if keyspace or table is empty
sink.open();                        // throws on connect failure

Batch<std::string> b;
b.emplace(R"({"id":1,"v":"a"})");
b.emplace(R"({"id":2,"v":"b"})");
sink.on_data(b);
sink.flush();   // waits for every pending insert; throws on any failure
sink.close();
```

To obtain the sink from the plugin registry, look it up by the factory name `cassandra_sink_string` with the `keyspace` and `table` build-context parameters set.

## Delivery semantics

`cassandra_sink_string` (default): at-least-once. Inserts are executed asynchronously for throughput, and `flush()` / `on_barrier()` waits for every pending insert and throws on any failure, so the job replays from the last checkpoint. A CQL `INSERT` is an upsert keyed by the primary key, so re-delivery on replay overwrites rather than duplicates: the result is effectively-once for a stable primary key. The SQL frontend rejects an explicit `exactly_once` request, since true exactly-once delivery is not provided.

`cassandra_upsert_sink_string` (`mode='upsert'`): effectively-once on the sink table for a stable PRIMARY KEY and a deterministic defining query. Consumes the changelog by key - insert/update_after `INSERT ... JSON` (a CQL upsert), delete/update_before a per-row `DELETE ... WHERE <pk>=<value>` (CQL has no multi-row IN over a composite key) - netted by key within a flush. A CQL INSERT and a keyed DELETE are both idempotent, so a replay converges the table. Lets a retracting query (GROUP BY, TOP-N, outer join) maintain a Cassandra table. Not two-phase commit.

## Limitations

- Sink only. There is no source; Cassandra is a serving store, not an unbounded streaming log.
- Single record channel: `std::string`, where each record must be a JSON object string. There is no typed or Arrow channel.
- Rows are written with `INSERT ... JSON`, so column mapping is by JSON field name and Cassandra's own type coercion. The schema must already exist.
- The sink does not provision schema (no keyspace or table creation).
- `keyspace` and `table` are required; an empty value throws at construction.
- The default sink has no delete handling; use `mode='upsert'` (the changelog-aware `cassandra_upsert_sink_string`) for a retracting query that must apply deletes.
- Delivery is at-least-once (default) or effectively-once by PRIMARY KEY (`mode='upsert'`); two-phase-commit exactly-once is not available.

## Testing

Offline tests in `impls/cassandra/tests/test_cassandra.cpp` need no cluster: they cover required-parameter validation and clean failure against a dead endpoint.

The live integration test in `impls/cassandra/tests/test_cassandra_live.cpp` is gated on the environment variable `CLINK_CASSANDRA_TEST_CONTACT_POINTS` (for example `127.0.0.1`) and is skipped when it is unset. It provisions a keyspace and table via the C API, writes rows through the sink, and reads them back to assert the values. Run a Cassandra (or ScyllaDB) instance, point the variable at it, and run the `cassandra`-labelled tests:

```bash
CLINK_CASSANDRA_TEST_CONTACT_POINTS=127.0.0.1 \
  ctest --test-dir build -L cassandra
```

There is no fixed Docker image named in the connector tests; supply any reachable Cassandra or ScyllaDB endpoint.
