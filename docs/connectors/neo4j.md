# Neo4j

> A sink that writes rows to Neo4j through the Query API: each batch runs your Cypher statement once, in one transaction, with the rows as a typed `$rows` parameter. At least once; whether a replayed batch is harmless is decided by the statement. And a [read-only lookup](#lookup) that enriches each row of a SQL stream with what one parameterised Cypher read returns for its key, as the graph is when the read runs.

## Overview

The Neo4j sink, `neo4j_sink`, takes SQL rows, encodes each batch as a list of maps in Neo4j's typed JSON, and posts it with a configured Cypher statement to `POST /db/<database>/query/v2`. The server runs the statement as one implicit transaction per batch, so a batch commits or rolls back as a whole. The sink is synchronous and ordered within a subtask: one request at a time, each batch sent only after the previous one committed.

The statement is yours. A typical one starts with `UNWIND $rows AS row` and then matches or merges nodes and relationships from `row.<column>`. Row values always travel as parameters; the sink never puts a value into the statement text.

The sink delivers at least once. A checkpoint barrier returns only after every row received before it is in a transaction the server reported committed, so a restore never skips a row. A batch can be applied twice: after a restore replays rows the server had already committed, and after a request whose reply was lost (the sink cannot tell whether that transaction committed, and retries it). A statement keyed by stable ids under uniqueness constraints converges to the same graph however often a batch is applied; `CREATE`, or an increment such as `SET n.count = n.count + 1`, does not. See [Replay-safe statements](#replay-safe-statements).

The same connector is also a [lookup](#lookup) for SQL lookup joins: a `connector='lookup'`, `function='neo4j'` table answers each probe row's key with zero or one row from a read-only statement. There is no Neo4j source and no graph-algorithm surface.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| HTTP client | `clink::http_connector` (vendored cpp-httplib) | `v0.18.5` |
| TLS | OpenSSL, when the HTTP client was built with it | system |
| Neo4j server | yours | the Query API: introduced in 5.19, enabled by default from 5.25 |

What is tested: Neo4j **2026.09.0 Community**, a single instance in Docker, pinned by digest in [`docker/integration-services.yml`](https://github.com/orhaugh/clink/blob/main/docker/integration-services.yml), over HTTP and over HTTPS, for the sink and the lookup. Other versions, Enterprise, clusters and Aura are not tested. The sink uses nothing a 5.19 server lacks, but on a server older than 2026.07, which does not report a statement's query type, the [open check](#what-the-open-check-refuses) cannot refuse a read-only or schema statement, and the lookup's check falls back to the plan alone. The lookup's `server_timeout_s` needs 2026.04 or later. In a cluster, the sink's endpoint must reach a server that can run writes against the database (server-side routing); Aura additionally serves HTTPS on port 443 only.

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
               WITH row.event_id AS id, collect(row)[0] AS row
               MERGE (s:Account {id: row.sender})
               MERGE (r:Account {id: row.recipient})
               MERGE (t:Transfer {event_id: id})
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
- **Reduce each batch to one row per event first**, as the example above does with `WITH row.event_id AS id, collect(row)[0] AS row`. One batch can hold an event twice when whatever writes the topic sent it twice, and the node is then created by that batch's own transaction for both copies: Cypher runs each clause over all of a batch's rows before the next, so both see `_created` and both create the relationships. Measured on the tested server: the statement without the reduction gave an event sent twice in one batch two `SENT` relationships, and the same event in two batches one.
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

## Lookup

A Neo4j lookup table enriches each row of a stream with what a parameterised, read-only Cypher statement returns for the row's key. It is current-state enrichment: each read sees the graph as committed when it runs.

### Lookup SQL usage

```sql
CREATE TABLE device_context (
  account           VARCHAR,       -- the key: the lookup side of the ON equality
  devices           BIGINT,        -- result columns: what the statement returns
  shared_accounts   BIGINT,
  evidence_accounts VARCHAR,
  matched           BOOLEAN,       -- metadata columns, each optional
  observed_at       TIMESTAMP(3),
  bookmark          VARCHAR
) WITH (
  connector = 'lookup',
  function  = 'neo4j',
  endpoint  = 'http://neo4j:7474',
  username  = 'reader',
  password  = 'env://NEO4J_READER_PASSWORD',
  statement = 'MATCH (a:Account {id: $key})
               WHERE EXISTS { (a)-[:USES_DEVICE]->(:Device) }
               WITH a, COLLECT {
                 MATCH (a)-[:USES_DEVICE]->(:Device)<-[:USES_DEVICE]-(o:Account)
                 WHERE o <> a
                 RETURN DISTINCT o.id AS id ORDER BY id LIMIT 1000
               } AS others
               RETURN COUNT { (a)-[:USES_DEVICE]->(:Device) } AS devices,
                      size(others) AS shared_accounts,
                      reduce(s = "", x IN others[0..10] |
                             s + CASE WHEN s = "" THEN "" ELSE "," END + x) AS evidence_accounts',
  matched_column     = 'matched',
  observed_at_column = 'observed_at',
  bookmark_column    = 'bookmark'
);

SELECT s.signal_id, s.account, d.devices, d.shared_accounts, d.evidence_accounts,
       d.matched, d.observed_at
FROM signals s
LEFT JOIN device_context d ON s.account = d.account;
```

- The lookup table goes on the right of an `INNER` or `LEFT` join, with one equality between a probe column and a lookup column, either way round. A lookup table on the left, a `RIGHT` or `FULL` join, any other `ON`, a lookup table inside a multi-way join and a lookup table read with `FROM` are refused when the query is planned. The probe side can be any table, common table expression or aliased subquery, a windowed aggregate included.
- `function = 'neo4j'` selects this connector's own operator, `neo4j_lookup_join_row`; the table's other options configure it.
- `LEFT JOIN`: every probe row comes out once, in input order. On a match the lookup's key column carries the probe's key and the result columns the statement's values; on a no-match the key and result columns are NULL. `INNER JOIN` drops the no-matches.
- A row's change kind passes through: a retraction is enriched and stays a retraction.

### Lookup options

Parsed as strictly as the sink's: an unknown option is refused with the accepted list, numbers must be whole and in range, and any value may be an `env://VAR` reference.

| Option | Required | Default | Meaning |
| --- | --- | --- | --- |
| `endpoint`, `database`, `username`, `password`, `bearer_token`, `tls_ca_file` | | | As for the [sink](#configuration). |
| `statement` | yes | | The read-only Cypher run once per probe row, taking the key as `$key`. |
| `matched_column` | no | | A declared `BOOLEAN` column: true on a match, false on a no-match. |
| `observed_at_column` | no | | A declared `TIMESTAMP` column (without time zone): clink's clock, in epoch milliseconds, when the reply arrived. NULL when no read ran. |
| `bookmark_column` | no | | A declared `VARCHAR` column: the bookmark the server returned with the read. NULL when no read ran. |
| `connect_timeout_ms` | no | `2000` | Per connection attempt. |
| `request_timeout_ms` | no | `5000` | One attempt, from sending to the end of the reply. |
| `max_retries` | no | `3` | Retries per lookup, 0 to 100. |
| `retry_window_ms` | no | `30000` | How long a lookup may go on retrying after its first failure; 0 makes one attempt. Retrying stops at whichever of the two bounds comes first. |
| `server_timeout_s` | no | not sent | Sent as the Query API's `maxExecutionTime`: the server ends a read that runs longer. The time includes planning the statement, which the first reads of a job wait for (see [Lookup baseline](#lookup-baseline)). Whole seconds, 1 to 3600, shorter than `request_timeout_ms`. Needs Neo4j 2026.04 or later; an older server refuses the request when the lookup opens. The server ignores a fraction of a second (0.5 sets no limit), which is why the option takes whole seconds. |
| `max_response_bytes` | no | `65536` | The most reply bytes read. |
| `max_in_flight` | no | `64` | Probe rows per subtask whose lookups have started and whose results are not yet emitted, 1 to 10000. |
| `max_concurrent_requests` | no | `4` | Requests per subtask at once, 1 to 64: a thread and a connection each. |

### Parameters and results

**The key.** The probe row's key is sent as the one parameter, `$key`, typed by the probe column's declared type: `TINYINT`, `SMALLINT`, `INTEGER` or `BIGINT` as an exact Integer, `VARCHAR` as a String. Both sides of the equality must be integers or both `VARCHAR`, and the lookup's key column at least as wide as the probe's. The key never enters the statement text, and the statement may use no other parameter. A NULL key is a no-match decided without a request: SQL equality never holds for NULL.

**Rows.** The statement must `RETURN` exactly the lookup table's result columns, by name (`AS`) and in any order: every declared column except the key and the metadata columns. Zero rows is a no-match. One row is a match. More than one row fails the lookup (`neo4j.multiple_rows`): clink does not choose between rows, so aggregate them in the statement (`count`, `collect`, `COUNT { }`, `COLLECT { }`) to make one row per key.

**Types.** Results are decoded strictly into the declared types:

| Neo4j value | Into | Rule |
| --- | --- | --- |
| Integer | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | exact; outside the type's range fails |
| Integer | `DOUBLE` | only when a double holds it exactly (up to 2^53, and some beyond) |
| Float | `DOUBLE` | NaN and the infinities kept |
| Boolean | `BOOLEAN` | |
| String | `VARCHAR` | valid UTF-8; a string starting with U+0001, which the engine reserves for decimals, fails |
| Null | any | SQL NULL, and still a match |

Any other pairing fails, among them a Float into an integer column (use `toInteger()` in the statement or declare `DOUBLE`) and every other Neo4j type: Node, Relationship, Path, List, Map, the temporal and spatial types, Duration and ByteArray. `REAL` is refused when the table is declared, since it would round a 64-bit Neo4j float. Return a node's properties rather than the node, and join a list into bounded text (the `reduce` above) rather than returning it. A failure (`neo4j.value_invalid`) names the column, its declared type and the Neo4j type it met, never the value.

### Read-only enforcement

Three layers, each doing something different:

1. **Read access mode, on every request.** Each lookup request carries `"accessMode": "READ"`. Neo4j documents the field as routing guidance for a cluster (the default is `WRITE`), not as authorisation. The tested server also enforces it: in read mode it refuses `CREATE`, `MERGE`, `SET`, `DELETE`, a schema command and a write procedure (`db.createLabel`) with `Neo.ClientError.Statement.AccessMode`, and the graph and schema are unchanged. The live suite pins that observation by sending each write directly; it is observed behaviour of the tested build, not a documented guarantee.
2. **The open check.** When a subtask opens, the lookup runs `EXPLAIN` on the statement with `$key` as NULL, in read mode: nothing executes. It refuses the statement (`neo4j.statement_refused`) unless the server classes it as a read (query type `r`, reported from 2026.07) and its plan holds no write or schema operator, no procedure call, no `LOAD CSV` and no `CALL { ... } IN TRANSACTIONS`. The server classes procedure calls, `LOAD CSV` and `IN TRANSACTIONS` as reads; the lookup refuses them anyway, because whether a procedure only reads cannot be established from the plan, and `LOAD CSV` fetches a file or URL from the server (the tested server still tries to, in read mode). The check also refuses result columns other than the declared ones and any parameter but `$key`, and warns when the plan scans a label or every node.
3. **The principal.** A user granted only reads is the control that does not rest on clink. Neo4j Community Edition has no roles (`SHOW ROLES` and `CREATE ROLE` answer `UnsupportedAdministrationCommand` on the tested build), so there a lookup's user can write when sent a write, and the two layers above are what prevent it. On an edition with roles, create a user whose role grants reads on the database it needs and nothing else, and give the lookup that user. That configuration is not tested here.

User-defined functions run inside the read transaction, so they cannot write to the graph; a plugin function with effects outside the graph is beyond what the plan shows.

### Bounds and failures

- **Threads.** `max_concurrent_requests` request threads per subtask, each with its own connection and a watcher that ends an attempt at `request_timeout_ms` or on a cancel. Nothing is started per row.
- **Input in flight and backpressure.** At most `max_in_flight` probe rows per subtask have a lookup started and not yet emitted. When that many are waiting, the operator takes no more input until the oldest finishes, which fills its input channel and holds back the operators upstream. The queue to the request threads holds at most one lookup per row in flight.
- **Replies.** Each reply is read up to `max_response_bytes`; a longer one fails the lookup (`neo4j.response_too_large`) without a retry.
- **The open check's time.** The check is held to `request_timeout_ms` and retried like a lookup, not held to `server_timeout_s`. When `server_timeout_s` is set, the check carries a `maxExecutionTime` of `request_timeout_ms` in whole seconds plus one, so a server that refuses the field is still found at open and the attempt's deadline always comes first. The tested server applies the limit only once it has planned the statement, and keeps no plan from a request it ends: an `EXPLAIN` given 1 second planned for 2.2 seconds, then failed, and the next attempt planned it again. Planning can take seconds on a server that has just started, while every subtask of every job plans its statement at once, so a check held to a read's limit would fail a statement that is fine and restart the job.
- **Retries.** A read is safe to repeat, so a refused connection, a lost or cut-short reply, an attempt that ran out of time, a `5xx`, a `429` and a retryable server error (classified as for the sink) are retried, with full-jitter backoff from 100 ms doubling to 5 seconds, until `max_retries` retries or `retry_window_ms` after the first failure, whichever comes first, and then the lookup fails (`neo4j.retries_exhausted`). A statement, type, authentication or authorisation error is not retried, nor is a read the server ended at `server_timeout_s` (`neo4j.timeout`).
- **Failure policy.** A lookup that ends without an answer fails the task, with an error naming the database, the endpoint and the cause, never the key, a value or a credential. None of an unavailable server, a timeout, a malformed or oversized reply, more than one row and a value of the wrong type is ever turned into a no-match, and a failed lookup emits no row. In a checkpointed job the failure restarts the job from its last completed checkpoint, within the job's restart budget (`--max-restarts-on-worker-loss`), and the replayed rows are looked up again; with the budget spent the job fails. In the recovery cells the restarts came under a second apart, so a restart budget is spent in seconds against a server that stays down: size the lookup's retry budget, not the restart budget, to the outage you want to ride out. The cells show both outcomes: an outage inside `retry_window_ms` passes with no restart, the rows read meanwhile held until the server answers and then emitted exactly once; an outage past it ends the job as FAILED, with exactly the rows read before the outage committed.
- **Cancellation.** When the task is cancelled, a request in flight is interrupted by its watcher within 20 ms, queued lookups fail without a request, and a backoff wait ends within 50 ms. A connect under way finishes or times out first, so the worst case is `connect_timeout_ms`. Measured on the tested server: a cancel during a slow read ended clink's wait in 18 ms. The server does not stop a read when its client goes away: in the same case it ran on until `server_timeout_s` ended it, 3.5 s with 3. Set `server_timeout_s` to bound the server's work as well as clink's.

### Checkpoints, watermarks and order

- **Order.** Results leave in input order, whatever order the replies come back in.
- **Markers.** A watermark leaves after every row that arrived before it and before every row that arrived after it, so it never overtakes the rows it covers. It does not stop lookups starting: it waits behind the rows ahead of it, and the rows behind it are looked up meanwhile, so a stream with a watermark after every record still runs up to `max_concurrent_requests` reads at once while it catches up. Before the operator forwards a checkpoint barrier, every lookup started before it has finished and its row has been emitted, so a checkpoint never holds a lookup in flight: in-flight work is drained before the barrier is acknowledged, not stored. The operator has no state, and a restore replays the input after the checkpoint and looks those rows up again.
- **Cost.** A barrier waits for the slowest lookup in flight, up to the retry budget, so slow or failing reads lengthen checkpoint alignment.
- **Latency.** While lookups are in flight the runner wakes the operator every 2 ms to emit those that have finished, so neither a result nor a watermark waiting behind it waits for the next input element.

### Consistency and replay

- **Current state.** A lookup sees what was committed when its read runs. One read is one statement under Neo4j's default read-committed isolation, not a snapshot of the graph across rows: two rows with the same key can see different states.
- **A replay can enrich differently.** After a restore, the replayed rows are looked up again, and if the graph changed in between they come out enriched with the newer state. A recovery cell changes the graph between two runs over the same events and sees every event enriched both ways in its at-least-once output. `clink run --explain` and the job's guarantee report say so: a Neo4j lookup makes the job's replay determinism `NONDETERMINISTIC`.
- **Not provided:** event-time graph snapshots, deterministic replay, and exactly-once *enriched* output. An exactly-once sink downstream commits each row once, carrying whatever its last attempt read. Capturing lookup results durably, and versioned graph snapshots, are what a reproducible decision would need; neither is part of this release.
- **When a row was enriched.** `observed_at` is clink's wall clock when the reply arrived: when the enrichment happened, as clink saw it. `bookmark` is the opaque token Neo4j returned for the read: a position in the database's transaction history, not a time. Passed to a later Neo4j request, it makes that request wait for at least that state; clink does not compare bookmarks.
- **What a bookmark does not prove.** A bookmark can require that the transactions it names are visible. It cannot show that an independent graph-writing job, a Neo4j sink in another clink job included, has written the event a row is about: that job's transactions are its own. Enrich from reference data that was in the graph before the stream started, as the example does, rather than from what another job is writing at the same time.
- **Downstream.** An event id alone does not say whether a later enrichment replaces an earlier one or stands beside it. Keep the latest (an upsert, or `MERGE` then `SET` on the event's key, as the example's signals do) when the current context is what matters; key each enrichment by the event and `observed_at` when every observation must be kept.

### Lookup metrics and logs

In the task's registry, tagged with the operator's `op_id`:

| Metric | Meaning |
| --- | --- |
| `clink_neo4j_lookup_requests_total` | HTTP attempts, retries included |
| `clink_neo4j_lookup_retries_total` | attempts retried |
| `clink_neo4j_lookup_matches_total`, `clink_neo4j_lookup_no_match_total` | answered lookups, by outcome |
| `clink_neo4j_lookup_null_keys_total` | NULL keys: no-matches decided without a request |
| `clink_neo4j_lookup_failures_total{code}` | lookups that failed, by error code; each fails the task |
| `clink_neo4j_lookup_latency_ns` | a lookup from queued to result, retries included (histogram) |
| `clink_neo4j_lookup_request_latency_ns` | an answered attempt, request to reply (histogram) |
| `clink_neo4j_lookup_in_flight`, `clink_neo4j_lookup_queued` | rows with a lookup started and not yet emitted, and those of them still waiting for a request thread (gauges) |

Log lines, on the `lookup.neo4j` channel: `neo4j lookup open:` (subtask, database, endpoint, the server's version from the plan, the bounds), a warning when the plan scans, one warning per failed attempt with its outcome and the wait before the retry, and `neo4j lookup closed:` with the counts. No line carries a key, a value or a credential.

### Lookup baseline

Measured with [`benchmarks/clink_neo4j_lookup_bench.cpp`](https://github.com/orhaugh/clink/blob/main/benchmarks/clink_neo4j_lookup_bench.cpp) on one machine: an Apple M2 Pro laptop (12 cores, 32 GB), Neo4j 2026.09.0 Community in Docker Desktop with the live suite's memory settings (512 MB heap, 128 MB page cache), clink on the host, HTTP over loopback. The graph is 1000 synthetic accounts, each using two devices of its own and one of 100 shared devices; each statement is warmed with 2000 unmeasured lookups, and each run is 5000 lookups. Two runs:

| Path | Requests at once | Lookups per second | p50 | p99 |
| --- | --- | --- | --- | --- |
| pool, point read | 1 | 1,220 to 1,250 | 0.8 ms | 1.2 ms |
| pool, point read | 4 | 3,000 | 1.3 ms | 2.4 to 2.7 ms |
| pool, two-hop device read | 1 | 1,140 to 1,160 | 0.8 ms | 1.4 to 1.6 ms |
| pool, two-hop device read | 4 | 2,870 to 2,950 | 1.3 ms | 2.7 to 2.8 ms |
| pool, two-hop device read | 8 | 3,880 to 3,950 | 1.9 ms | 5.8 to 6.1 ms |
| pool, two-hop device read | 16 | 5,100 to 5,400 | 2.5 to 2.7 ms | 9.8 to 10.1 ms |
| operator, two-hop, `max_in_flight` 64 | 4 | 3,370 to 3,410 | 18 ms | 28 ms |
| operator, two-hop, `max_in_flight` 64 | 16 | 5,700 to 6,200 | 10 ms | 21 ms |

In the pool rows each client waits for its own lookup, so the latency is one lookup's. In the operator rows rows are fed as fast as the operator takes them, so the latency, from handing a row in to its emission, includes the wait behind the other rows in flight: about `max_in_flight` divided by the throughput. One request costs about 0.8 ms here whether it is the point read or the two-hop read, so request round trips rather than graph work bound these figures, and concurrency is the lever until the server is saturated; at 16 requests the point read varied most between runs (3,600 to 5,800 per second). These figures are for comparing changes on this machine. They are not a capacity statement: the server shares the machine, and Docker Desktop's network path is part of every request.

The first lookups of a job are slower than the rest. Neo4j plans a statement the first time it runs it, and the open check's `EXPLAIN`, run with `$key` as NULL, does not leave a plan the lookups reuse. On the same machine, with the server's query caches cleared, the first lookup of a two-hop statement took 80 to 310 ms and the next ones 3 to 7 ms, and lookups that arrived while the first was being planned waited for it. Expect that after a job starts, and after the server's plan cache is cleared. Planning is slower on a server that has just started: a statement with a quantified path pattern and nested subqueries took 2.1 to 3.0 seconds to plan there, and about 0.5 seconds on a server that had been running. With the open checks of seven jobs planned at once on a server started moments before, that statement's check took 6.5 seconds. When jobs can start alongside the server, set `server_timeout_s` with room for the first read's planning.

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
| `neo4j.cancelled` | the task was cancelled before the batch committed, or before a lookup finished |
| `neo4j.multiple_rows` | a lookup's statement returned more than one row for a key |
| `neo4j.response_too_large` | a lookup's reply passed `max_response_bytes` |
| `neo4j.malformed_reply` | a lookup's reply named result columns other than the declared ones |
| `neo4j.timeout` | the server ended a lookup's read at `server_timeout_s` |

The sink's codes apply to the lookup too, with its own causes: `neo4j.statement_refused` for a statement the [read-only check](#read-only-enforcement) refuses, `neo4j.value_invalid` for a result outside its declared type or a key that does not fit its, `neo4j.retries_exhausted` for a lookup that ran out of its retry budget, `neo4j.type_unsupported` for a key or result column type the lookup does not take. A lookup's refusals begin `neo4j lookup:`.

## Limitations

- At least once. A statement that is not replay-safe duplicates its effects after a restore or an unknown outcome.
- One implicit transaction per batch, one request at a time per subtask. Subtasks write concurrently, so the statement must tolerate concurrent batches (see [Replay-safe statements](#replay-safe-statements)).
- Batches from different subtasks, and from different sink tables, are separate transactions: a reader can see one table's batch before another's.
- Tested on Neo4j 2026.09.0 Community, single instance. Clusters, Enterprise and Aura are untested; role-based authorisation (`Security.Forbidden`) is Enterprise-only, so it is classified from its code but not exercised against a server.
- Rows that arrive while no barrier comes and the input is idle wait for the next barrier or the end of the input.
- A request the sink timed out may still commit on the server; bound it with `db.transaction.timeout`.
- The lookup is current-state enrichment: no event-time snapshots, no deterministic replay, no cache, and no capture of what a lookup returned. A replay reads the graph again.
- A lookup returns zero or one row of scalar columns per key, on one equality. Multi-row results, graph values (nodes, relationships, paths, lists, maps, temporal values), composite keys and procedure calls are refused.
- One read per probe row: a stream with many rows for the same key reads it each time.
- A barrier waits for the lookups in flight before it is forwarded, so slow reads lengthen checkpoint alignment.
- Read-only enforcement on Community Edition rests on the read access mode and the open check; there is no read-only user to back them. Role-based setups are untested.

## Testing

Unit suites (`ctest --test-dir build -L neo4j`, no server): options and refusals, the encoding of every type (64-bit extremes, NaN, subnormals, invalid UTF-8, nested values), the judging of replies the pinned server really sent, and the sink against an in-process stand-in for the Query API (batching, retries, unknown outcomes, an oversized reply, the request deadline, a cancel during a stalled request). The shared-chain refusal is tested through the registered factory. For the lookup: its options and every refusal, the decoding of each result type at its bounds, how a read is judged and the statement contract the open check holds, and the operator against a stand-in that answers each key as scripted: a match with its metadata, a no-match and a NULL key, two rows refused, a wrong type, malformed and oversized replies, retries stopping at the count and at the window, the request deadline, refusals and the server timeout, an open check whose planning outlasts `server_timeout_s`, concurrency never above `max_concurrent_requests`, the operator holding its input at `max_in_flight`, replies out of order emitted in order with no watermark or barrier overtaking them, rows held through an outage and released in order once the server answers, a restored operator reading the graph as it is now, and cancellation and close within their bounds. The SQL suites check what the planner puts on the lookup operator, the join shapes it refuses, and LEFT and INNER pipelines end to end.

The live gate starts the pinned server twice over (HTTP, and HTTPS with a CA it generates), runs every Neo4j suite against it, and fails on any skipped case or missing suite:

```bash
cmake --build build --target clink_neo4j_tests clink_neo4j_sql_tests clink_integration_tests clink_node clink_submit_sql --parallel 10
scripts/neo4j-live.sh build
```

To look at a lookup by hand, `clink run --explain <file>.sql` prints the plan with its `LookupJoin` and the job's replay determinism; the worker log's `neo4j lookup open:` line shows the server version and the bounds in force, and `clink_neo4j_lookup_failures_total{code}` names what failed.

The lookup's live cases read values back exactly (64-bit extremes, text, NULL, a missing property), refuse a value outside its type and two rows, refuse at open a `CREATE`, `MERGE`, `SET`, `DETACH DELETE`, `FOREACH`, procedure call, `LOAD CSV`, `IN TRANSACTIONS` and schema statement with nothing run, send each kind of write to the server directly in read mode and see it refused with the graph, schema and labels unchanged and `SHOW ROLES` unsupported, end a slow read with the server's own timeout, cancel during a slow read, and refuse a reply past the byte bound; the TLS suite runs a lookup with the generated CA and refuses an unrelated one. The SQL live case enriches an embedded pipeline from the server. The lookup's recovery cells run a coordinator and two workers from Kafka through a SQL lookup join into Kafka: every event enriched as the reference graph says, a worker killed with a lookup parked in flight losing nothing, an outage inside the retry budget ridden out with no restart, an outage past it failing the job with only correctly enriched rows committed, a replay after a graph change enriching from the graph as it is then, and a lookup that fails ending the job without writing a no-match.

The sink's live cases, each checking the graph through a client of its own rather than through the sink: batched writes and a partial batch; every type read back; a syntax error, a wrong password and an expired password refused; the statements the open check refuses, refused by the server's own plan; a failing batch leaving none of its rows; a barrier whose flush fails never completing its checkpoint; the server stopped and started inside the retry window, and an outage longer than it; a commit whose reply is withheld by a fault proxy until the rows are visible to another client and then cut, retried, which a `MERGE` absorbs and a `CREATE` writes twice; a cancel while the server does not answer, returning within seconds; two concurrent writers and a replay converging on the replay-safe mapping; and a stale replay with concurrent writers leaving the newest value. The TLS cases verify the server with the generated CA and refuse an unrelated CA, the system store, and a host name the certificate does not carry. The recovery cells run a coordinator and two workers from Kafka through SQL into the server, killing a worker before a request and after a commit before its checkpoint, and restarting the server under a running job.

The gate's `neo4j` CI workflow runs the same script against the same pinned image on every push and keeps the reports, and on failure the server logs, as artefacts.

A passing live suite is evidence about this sink against one server build in one deployment shape. It is not a production qualification.

## Example

[`examples/kafka-to-neo4j/`](https://github.com/orhaugh/clink/tree/main/examples/kafka-to-neo4j) runs synthetic transfers from Kafka through clink into Neo4j on Docker Compose, computes a fan-in signal in a time window, enriches each signal with device relationships the graph held before the run through a Neo4j lookup, kills a worker mid-run, and checks the graph, the signals and their context against the produced events and the reference data.
