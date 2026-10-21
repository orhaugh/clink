# Neo4j

> A sink that writes rows to Neo4j through the Query API: each batch runs your Cypher statement once, in one transaction, with the rows as a typed `$rows` parameter. At least once; whether a replayed batch is harmless is decided by the statement.

## Overview

The Neo4j sink, `neo4j_sink`, takes SQL rows, encodes each batch as a list of maps in Neo4j's typed JSON, and posts it with a configured Cypher statement to `POST /db/<database>/query/v2`. The server runs the statement as one implicit transaction per batch, so a batch commits or rolls back as a whole. The sink is synchronous and ordered within a subtask: one request at a time, each batch sent only after the previous one committed.

The statement is yours. A typical one starts with `UNWIND $rows AS row` and then matches or merges nodes and relationships from `row.<column>`. Row values always travel as parameters; the sink never puts a value into the statement text.

The sink delivers at least once. A checkpoint barrier returns only after every row received before it is in a transaction the server reported committed, so a restore never skips a row. A batch can be applied twice: after a restore replays rows the server had already committed, and after a request whose reply was lost (the sink cannot tell whether that transaction committed, and retries it). A statement keyed by stable ids under uniqueness constraints converges to the same graph however often a batch is applied; `CREATE`, or an increment such as `SET n.count = n.count + 1`, does not. See [Replay-safe statements](#replay-safe-statements).

It is a sink only: no Neo4j source, lookup join or graph-algorithm surface.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| HTTP client | `clink::http_connector` (vendored cpp-httplib) | `v0.18.5` |
| TLS | OpenSSL, when the HTTP client was built with it | system |
| Neo4j server | yours | the Query API: introduced in 5.19, enabled by default from 5.25 |

What is tested: Neo4j **2026.09.0 Community**, a single instance in Docker, pinned by digest in [`docker/integration-services.yml`](https://github.com/orhaugh/clink/blob/main/docker/integration-services.yml), over HTTP and over HTTPS. Other versions, Enterprise, clusters and Aura are not tested. The sink uses nothing a 5.19 server lacks, but on a server older than 2026.07, which does not report a statement's query type, the [open check](#what-the-open-check-refuses) cannot refuse a read-only or schema statement. In a cluster, the endpoint must reach a server that can run writes against the database (server-side routing); Aura additionally serves HTTPS on port 443 only.

## Enabling it

`CLINK_WITH_NEO4J` (`AUTO` by default) builds the module whenever `clink::http_connector` is built, which is the default; it needs no library of its own. `clink_node`, `clink run` and the embedded engine install it. `clink --capabilities` lists its record as `neo4j`.

## SQL usage

```sql
CREATE TABLE transfers (
  event_id BIGINT, sender VARCHAR, recipient VARCHAR, amount DOUBLE, at TIMESTAMP(3)
) WITH (connector = 'kafka', format = 'json', brokers = 'kafka:9092', topic = 'transfers',
        group_id = 'graph-writer', auto_offset_reset = 'earliest');

CREATE TABLE graph (
  event_id BIGINT, sender VARCHAR, recipient VARCHAR, amount DOUBLE, at TIMESTAMP(3)
) WITH (
  connector = 'neo4j',
  endpoint  = 'http://neo4j:7474',
  database  = 'neo4j',
  username  = 'neo4j',
  password  = 'env://NEO4J_PASSWORD',
  statement = 'UNWIND $rows AS row
               MERGE (s:Account {id: row.sender})
               MERGE (r:Account {id: row.recipient})
               MERGE (t:Transfer {event_id: row.event_id})
                 ON CREATE SET t.amount = row.amount, t.at = row.at, t._created = true
               WITH s, r, t
               CALL (s, r, t) { WITH s, r, t WHERE t._created
                 CREATE (s)-[:SENT]->(t) CREATE (t)-[:TO]->(r) }
               REMOVE t._created'
);

INSERT INTO graph SELECT * FROM transfers;
```

Every column of the sink table becomes a key of each `row` map, so the table's columns are the statement's parameter contract. A table with one column is planned the same way. A query whose result retracts rows (an outer join, a top-N per key) is refused, as for any sink that only appends.

## Configuration

Options are parsed strictly when the sink is built: an unknown option is refused and the refusal lists the accepted ones, a number must be the whole value and within range. Any value may be written `env://VAR` and is read from the worker's environment; an unset or empty variable is refused (`neo4j.secret_unset`), and a refusal names such a value by its reference, never its contents.

| Option | Required | Default | Meaning |
| --- | --- | --- | --- |
| `endpoint` | yes | | `http://host[:port]` or `https://host[:port]`, nothing after the port. Credentials in the URL are refused. |
| `database` | no | `neo4j` | Letters, digits, `.`, `-` and `_`, starting with a letter or digit, at most 63 characters; percent-encoded in the path as well. |
| `statement` | yes | | The Cypher run once per batch, reading its rows from `$rows`. |
| `username`, `password` | no | | HTTP basic authentication. Given together, or not at all. |
| `bearer_token` | no | | `Authorization: Bearer`, for a server set up for SSO. Not with `username`. |
| `tls_ca_file` | no | system trust store | https only: a PEM file of the CAs that may sign the server's certificate. |
| `batch_size` | no | `1000` | Rows per batch, 1 to 100000. |
| `batch_bytes` | no | `4194304` | Encoded request bytes per batch, 1024 to 268435456. A single row that does not fit is refused (`neo4j.row_too_large`). |
| `flush_interval_ms` | no | `1000` | A partial batch older than this is sent when the next rows arrive; 0 turns it off. With no input arriving, rows wait for the next checkpoint barrier or the end of the input. |
| `connect_timeout_ms` | no | `5000` | Per connection attempt. |
| `request_timeout_ms` | no | `30000` | The whole of one attempt, from sending to the end of the reply. |
| `retry_window_ms` | no | `60000` | How long a batch's retries may go on after its first failure; 0 makes one attempt. |
| `max_response_bytes` | no | `1048576` | The most reply bytes read. |

The sink accepts and ignores the keys the SQL planner puts on every sink op (`schema_columns`, `primary_key`, `bounded` and the like). It refuses what would change its guarantee, here as well as in the planner, since a job built directly on the Dag never passes through the planner: `delivery_guarantee` other than `at_least_once`, `mode` other than `append` (so `mode='upsert'`), `changelog='true'`, and `write_mode='overwrite'`. `json_non_finite` does not apply: the sink writes typed values, and NaN and the infinities are stored as floats.

## Parameter contract

Each request sends:

```json
{"statement": "<your statement>",
 "parameters": {"rows": {"$type": "List", "_value": [
   {"$type": "Map", "_value": {"event_id": {"$type": "Integer", "_value": "41"}, ...}},
   ...]}}}
```

`$rows` is the batch, in arrival order, one map per row keyed by the table's column names. The statement may use no other parameter. Your statement decides everything else: labels, relationship types, which columns become properties.

## Type mapping

| SQL type | Neo4j value | Notes |
| --- | --- | --- |
| `BOOLEAN` | Boolean | |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | Integer | Exact across the whole 64-bit range: typed JSON carries the digits, not a double. An integral double or the digits of an integer as text are accepted; a fraction is refused. |
| `REAL`, `DOUBLE` | Float | NaN, `Infinity` and `-Infinity` are kept as Neo4j floats, as are `-0.0` and subnormals. An integer a double cannot hold exactly is refused. |
| `VARCHAR` | String | Valid UTF-8 only; a number or boolean is written as its exact text. |
| `DATE` | Date | Days since 1970-01-01, or `YYYY-MM-DD`; years 1 to 9999. |
| `TIMESTAMP(p)` | LocalDateTime | Millisecond precision, the engine's own; years 1 to 9999. |
| `TIMESTAMP(p) WITH TIME ZONE` | DateTime at offset `Z` | Sent as typed JSON `OffsetDateTime`. |
| `t ARRAY` | List | Elements typed by `t`, nested lists included. |
| `MAP<VARCHAR, t>` | Map | Keys must be strings: other key types are refused. |
| `ROW<...>` | Map | Keyed by the field names. |
| any, NULL | null | A missing column is null too. |
| `DECIMAL(p, s)` | refused | Neo4j has no decimal type: `CAST` to `VARCHAR` to keep every digit, or to `DOUBLE` to store it rounded. |
| `TIME`, `BYTEA` | refused | `CAST` to `VARCHAR`. |

A value that does not fit its declared type fails the batch with `neo4j.value_invalid`, naming the column and the kind of value it held, never the value.

What the server can store is Neo4j's rule, not the sink's: a property holds a primitive or a list of primitives with no nulls, so a map or a nested list is for the statement to take apart (`row.address.city`, `size(row.legs)`), and setting a property to null removes it.

## Transactions and checkpoints

- **One batch, one transaction.** A batch is one request and one implicit transaction: the server commits all of its rows or none. A constraint violation on the last row rolls back the first.
- **What counts as committed.** The sink reads the whole reply. A batch is committed only when the reply arrived complete, has `bookmarks` and has no `errors`. The HTTP status is not proof: the server sends `202` before it streams a result and reports an error that happens while it streams after the rows it had sent, with no bookmarks, having rolled the transaction back. A reply cut short, not JSON, larger than `max_response_bytes`, or with both bookmarks and errors, is an [unknown outcome](#retries-and-unknown-outcomes).
- **Barriers.** `on_barrier` sends the partial batch and returns only once it committed. The sink declares that its guarantee rests on its barrier hook (`Sink::gates_checkpoint_ack()`), so it must be the only sink on its chain: `Dag::add_sink` refuses another sink beside it, and SQL, clusters and the embedded engine give every sink a subtask of its own. A batch that fails at a barrier fails the subtask, and the checkpoint cannot complete; the rows stay buffered until they commit or the job fails, and a restore replays everything after the last completed checkpoint.
- **End of input.** The remaining rows are sent before the subtask finishes.
- **Cancellation.** A cancelled task sends nothing more. A request in flight is ended by a watcher thread that checks the task's cancel every 20 ms and interrupts the connection; it is the sink's only thread besides the task's, and holds no data. A connect under way finishes or times out first, so a cancel can wait up to `connect_timeout_ms` for that. The live suite blocks a barrier on a server that does not answer, with a 120-second request timeout, cancels it after one second, and requires the barrier to have returned within four seconds of starting.
- **No two-phase commit.** A Query API transaction lives on one server, is rolled back after 60 seconds idle, and cannot be resumed by a restarted worker, so it cannot carry a prepared batch across clink's recovery. The sink does not try.

## Retries and unknown outcomes

Every attempt ends in one of these, and only the first commits:

| Outcome | What happened | What the sink does |
| --- | --- | --- |
| committed | complete reply, bookmarks, no errors | moves on to the next batch |
| rolled back, retryable | the server reported an error a retry may not meet | retries |
| refused | the server reported an error a retry would meet again | fails the batch at once |
| not sent | the request never reached the server whole: no connection, a failed TLS handshake, a write that failed | retries; nothing was applied |
| unknown | the request reached the server and the reply was lost, cut short, malformed, or a `5xx` without a Neo4j error | counts it, logs it, retries it |
| TLS refused | the certificate or the host name did not verify | fails at once |

Which server errors are retryable follows the official Neo4j driver: every `Neo.TransientError.*` (a deadlock, an unavailable database, a memory limit) except `Transaction.Terminated` and `Transaction.LockClientStopped`, plus `Neo.ClientError.Cluster.NotALeader`, `Neo.ClientError.General.ForbiddenOnReadOnlyDatabase` and `Neo.ClientError.Security.AuthorizationExpired`. Everything else (statement, schema and type errors, authentication and authorisation) is refused.

Retries back off with full jitter from 100 ms, doubling to 5 seconds, until `retry_window_ms` has passed since the batch's first failure; then the batch fails with `neo4j.retries_exhausted`, and the error says how many attempts had an unknown outcome. Each attempt is bounded by `request_timeout_ms` as a whole.

**An unknown outcome may have committed.** The Neo4j driver itself calls this "transaction resolution unknown" and does not retry it. The sink does, because at least once already allows a repeat: the restore that would follow a failure replays the same rows. The retry, like the replay, applies a non-idempotent statement twice. Each such attempt is counted in `clink_neo4j_outcome_unknown_total` and logged with the batch's size, never its rows. A request that timed out may also still be running on the server when its retry starts; set the server's `db.transaction.timeout` no higher than `request_timeout_ms` to bound that.

## Replay-safe statements

A statement converges under replay when applying a batch twice, in either order and at the same time as another subtask, leaves the same graph as applying it once:

- **Key every node by a stable business id under a uniqueness constraint**, and reach it with `MERGE` on that id. `CREATE CONSTRAINT account_id FOR (a:Account) REQUIRE a.id IS UNIQUE`. The constraint is what makes concurrent `MERGE`s on one id serialise instead of creating two nodes.
- **Give each event a node keyed by its event id**, and create the event's relationships only in the transaction that creates that node, as the example above does with `ON CREATE SET t._created = true` and the conditional `CALL`: a replayed or concurrent duplicate finds the node and creates nothing. Neo4j Community has no relationship uniqueness constraint, so a bare `MERGE (a)-[:SENT]->(t)` is not proof against two concurrent transactions.
- **A mutable property needs an ordering rule**, or an older event replayed after a newer one overwrites it. Keep the position of the value (its event time and id) beside it and write only when the row is newer. Two things make that hold. Cypher runs each clause over all of a batch's rows before the next clause, so two rows for one key would both compare with the stored value and the later row would win; reduce the batch to the newest row per key first. And two subtasks could both read the old position; take the node's write lock before reading it:

```cypher
UNWIND $rows AS row
WITH row ORDER BY row.at DESC, row.event_id DESC
WITH row.account AS id, collect(row)[0] AS row
MERGE (a:Account {id: id})
SET a._lock = true
WITH a, row, (a.risk_at IS NULL OR row.at > a.risk_at OR
              (row.at = a.risk_at AND row.event_id > a.risk_event)) AS newer
SET a.risk = CASE WHEN newer THEN row.risk ELSE a.risk END,
    a.risk_at = CASE WHEN newer THEN row.at ELSE a.risk_at END,
    a.risk_event = CASE WHEN newer THEN row.event_id ELSE a.risk_event END
REMOVE a._lock
```

Concurrent writers that lock nodes in different orders can deadlock; Neo4j rolls one back with `Neo.TransientError.Transaction.DeadlockDetected`, and the sink retries it. The live suite meets this routinely.

## What the open check refuses

When a subtask opens, the sink runs `EXPLAIN <statement>` with an empty `$rows`: nothing executes, and the reply proves the credentials, the database and the syntax, and shows the statement's result columns, query type and plan. The sink refuses (`neo4j.statement_refused`) a statement that:

- **returns columns**: the sink reads no results, so returning rows only costs bandwidth and memory. End the statement without `RETURN`.
- **uses `CALL { ... } IN TRANSACTIONS`** (its plan has a `TransactionForeach` or `TransactionApply`): it commits its own inner transactions, so a failing batch would leave part of itself behind. Measured: with a duplicate in the second of three rows, the server reported "Transactions committed: 1" and kept the first.
- **only reads, or changes the schema** (query type `r` or `s`, on servers from 2026.07 that report it): create constraints and indexes before the job, not per batch.
- **uses a parameter other than `$rows`.**

A procedure that commits its own transactions is outside what the check can see; it breaks a batch's atomicity the same way.

## Security

- Credentials come from `username`/`password` or `bearer_token`, normally as `env://` references. They are sent only in the `Authorization` header, never logged, and never part of an error.
- `https://` endpoints verify the server's certificate against the system's trust store or `tls_ca_file`, and that the certificate names the endpoint's host. There is no switch to turn either check off. Against the tested server a mismatched name cannot work anyway: it answers a request for a name its certificate does not carry with `400 Invalid SNI`.
- An `https://` endpoint on a build without TLS is refused when the sink is built (`neo4j.tls_unavailable`).
- Errors name the database, the endpoint, the operation (checking the statement, or a batch by number and size) and the server's error code. The server's message is included for codes that cannot quote data (syntax, parameters, security, the database itself, transient errors) and withheld for those that can: a constraint violation names the conflicting property value, and a type or arithmetic error prints the value it met. The sink does not log a withheld message either.

## Metrics and logs

The sink's own series are in the task's registry, tagged with its `op_id`:

| Metric | Meaning |
| --- | --- |
| `clink_neo4j_rows_submitted_total`, `clink_neo4j_bytes_submitted_total` | rows and bytes in requests the server received whole, retries included: what was sent |
| `clink_neo4j_batches_committed_total`, `clink_neo4j_rows_committed_total` | what the server reported committed |
| `clink_neo4j_commit_latency_ns` | the committing attempt, request to reply (histogram) |
| `clink_neo4j_attempts_failed_total{verdict}` | attempts that did not commit: `rolled_back`, `refused`, `not_sent`, `unknown`, `tls_refused` |
| `clink_neo4j_retries_total` | attempts retried |
| `clink_neo4j_outcome_unknown_total` | attempts that may have committed with their reply lost |
| `clink_neo4j_buffered_rows`, `clink_neo4j_buffered_bytes` | the pending batch (gauges) |

The cross-connector series carry `connector="neo4j"`: `clink_connector_records_total`, `clink_connector_bytes_total` and `clink_connector_commit_latency_ns` count committed batches only, and `clink_connector_errors_total` counts failures.

Log lines, on the `sink.neo4j` channel: `neo4j sink open:` (subtask, database, endpoint, the server's version from the plan, batch limits), one warning per failed attempt with its outcome and the wait before the retry, a line when a batch commits after retries saying how many of them had an unknown outcome, and `neo4j sink closed:` or `neo4j sink cancelled:` with the counts and the rows not sent.

## Refusals

| Code | When |
| --- | --- |
| `neo4j.option_invalid` | an unknown option, a malformed or out-of-range value, a missing `endpoint`, `statement` or column list |
| `neo4j.secret_unset` | an `env://VAR` whose variable is unset or empty on the worker |
| `neo4j.guarantee_unsupported` | `delivery_guarantee` other than `at_least_once`, `mode='upsert'`, `changelog='true'`, `write_mode='overwrite'` |
| `neo4j.type_unsupported` | a `DECIMAL`, `TIME`, `BYTEA`, non-string-keyed `MAP` or unknown column type |
| `neo4j.tls_unavailable` | `https://` on a build without TLS |
| `neo4j.tls_verify` | the certificate or the host name did not verify, or the CA file did not load |
| `neo4j.statement_refused` | the open check refused the statement, or the server refused its syntax |
| `neo4j.authentication` | the server did not accept the credentials |
| `neo4j.authorisation` | valid credentials not allowed to run the statement (`Security.Forbidden`, `Security.CredentialsExpired`) |
| `neo4j.server_refused` | the server rolled a batch back for a reason a retry would meet again (a constraint, a type error) |
| `neo4j.retries_exhausted` | `retry_window_ms` passed without a commit |
| `neo4j.outcome_unknown` | the reply passed `max_response_bytes`: the batch may have committed, and retrying would end the same way |
| `neo4j.value_invalid` | a value that does not fit its declared column type |
| `neo4j.row_too_large` | one row larger than `batch_bytes` |
| `neo4j.cancelled` | the task was cancelled before the batch committed |

## Limitations

- At least once. A statement that is not replay-safe duplicates its effects after a restore or an unknown outcome.
- One implicit transaction per batch, one request at a time per subtask. Subtasks write concurrently, so the statement must tolerate concurrent batches (see [Replay-safe statements](#replay-safe-statements)).
- Batches from different subtasks, and from different sink tables, are separate transactions: a reader can see one table's batch before another's.
- Tested on Neo4j 2026.09.0 Community, single instance. Clusters, Enterprise and Aura are untested; role-based authorisation (`Security.Forbidden`) is Enterprise-only, so it is classified from its code but not exercised against a server.
- Rows that arrive while no barrier comes and the input is idle wait for the next barrier or the end of the input.
- A request the sink timed out may still commit on the server; bound it with `db.transaction.timeout`.

## Testing

Unit suites (`ctest --test-dir build -L neo4j`, no server): options and refusals, the encoding of every type (64-bit extremes, NaN, subnormals, invalid UTF-8, nested values), the judging of replies the pinned server really sent, and the sink against an in-process stand-in for the Query API (batching, retries, unknown outcomes, an oversized reply, the request deadline, a cancel during a stalled request). The shared-chain refusal is tested through the registered factory.

The live gate starts the pinned server twice over (HTTP, and HTTPS with a CA it generates), runs every Neo4j suite against it, and fails on any skipped case or missing suite:

```bash
cmake --build build --target clink_neo4j_tests clink_neo4j_sql_tests clink_integration_tests clink_node clink_submit_sql --parallel 10
scripts/neo4j-live.sh build
```

Its live cases, each checking the graph through a client of its own rather than through the sink: batched writes and a partial batch; every type read back; a syntax error, a wrong password and an expired password refused; the statements the open check refuses, refused by the server's own plan; a failing batch leaving none of its rows; a barrier whose flush fails never completing its checkpoint; the server stopped and started inside the retry window, and an outage longer than it; a commit whose reply is withheld by a fault proxy until the rows are visible to another client and then cut, retried, which a `MERGE` absorbs and a `CREATE` writes twice; a cancel while the server does not answer, returning within seconds; two concurrent writers and a replay converging on the replay-safe mapping; and a stale replay with concurrent writers leaving the newest value. The TLS cases verify the server with the generated CA and refuse an unrelated CA, the system store, and a host name the certificate does not carry. The recovery cells run a coordinator and two workers from Kafka through SQL into the server, killing a worker before a request and after a commit before its checkpoint, and restarting the server under a running job.

The gate's `neo4j` CI workflow runs the same script against the same pinned image on every push and keeps the reports, and on failure the server logs, as artefacts.

A passing live suite is evidence about this sink against one server build in one deployment shape. It is not a production qualification.

## Example

[`examples/kafka-to-neo4j/`](https://github.com/orhaugh/clink/tree/main/examples/kafka-to-neo4j) runs synthetic transfers from Kafka through clink into Neo4j on Docker Compose, computes a fan-in signal in a time window, kills a worker mid-run, and checks the graph against the produced events.
