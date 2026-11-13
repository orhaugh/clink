# Relate

> A sink that writes rows into a [Relate](https://github.com/relatedb/relate) graph store opened in the engine's own process: each batch runs your Cypher statement once, in one transaction, with the rows as a typed `$rows` parameter. There is no server and no network. At least once; whether a replayed batch is harmless is decided by the statement.

## Overview

Relate is an embedded graph database: a library, not a server. The Relate sink, `relate_sink`, links that library into clink, opens the store named by `path` in the process that runs the sink, and passes each batch of SQL rows to a configured Cypher statement as a list of maps built with Relate's typed parameter builder. Relate runs the statement in one transaction per batch, so a batch commits or rolls back as a whole. The sink is synchronous and ordered within a subtask: one transaction at a time, each batch begun only after the previous one committed.

The statement is yours. A typical one starts with `UNWIND $rows AS row` and then matches or merges nodes and relationships from `row.<column>`. Row values always travel as parameters; the sink never puts a value into the statement text, and no value passes through text or JSON on the way.

The sink delivers at least once. A checkpoint barrier returns only after every row received before it is in a transaction Relate committed, so a restore never skips a row. A batch can be applied twice, when a restore replays rows that were already committed. A statement keyed by stable ids under uniqueness constraints converges to the same graph however often a batch is applied; `CREATE`, or an increment such as `SET n.count = n.count + 1`, does not. See [Replay-safe statements](#replay-safe-statements).

Because the store is in the same process, a commit either returned or the process died: there is no request whose reply can be lost, and so no outcome the sink cannot know.

What follows from Relate being embedded:

- **One writing process.** A store has one writing process, and every sink writing it must be in that process. See [One store, one process](#one-store-one-process).
- **In memory.** Relate holds the whole graph in the memory of the process that opened it, beside the engine's own.
- **No source and no lookup yet.** The connector is a sink. Reading the graph back into a SQL stream is not offered.

## Dependency and version

| Component | Provenance | Version |
| --- | --- | --- |
| Relate's C library (`relate.h`, `librelate_ffi.a`) | a release asset of `relatedb/relate`, per platform, pinned by sha256 in [`scripts/versions.env`](https://github.com/orhaugh/clink/blob/main/scripts/versions.env) and installed by `scripts/fetch-relate.sh` | `1.0.1` (C ABI 2) |

The library is linked statically, so a binary built with the connector needs nothing of Relate's at run time. On macOS it brings two system dependencies, CoreFoundation and libiconv, and adds about 26 MB to a binary. On Linux it needs only `libc`, `libm` and `libgcc_s`.

Relate's release signs a checksum list that covers its command-line binaries only. The library archives are not in it, so the sha256 in `scripts/versions.env` is the check: `scripts/fetch-relate.sh` refuses an archive that does not match, and a platform with no pin does not build the connector. Pins exist for macOS on arm64 and for Linux (glibc) on x86_64 and aarch64, and each equals the digest GitHub records for the asset. Relate's Intel macOS and Windows archives are not pinned.

What is tested: Relate **1.0.1** on macOS arm64 and on Linux aarch64 (Debian 13, g++ 14.2, glibc 2.41), with stores of both layouts (a directory, and a single `.rl` file). The Linux x86_64 archive is pinned but has not been built against. CI does not build the connector yet, because its toolchain image does not install the library. Relate describes itself as experimental.

## Enabling it

```bash
scripts/fetch-relate.sh        # relate.h and librelate_ffi.a into ${CLINK_DEPS_PREFIX}/relate-ffi
cmake -S . -B build            # CLINK_WITH_RELATE=AUTO finds it there
```

`CLINK_WITH_RELATE` (`AUTO` by default) builds the module when the library is under the dependency prefix or under `RELATE_FFI_ROOT`, and leaves it out otherwise; `ON` makes a missing library a configure error. `clink_node`, `clink run` and the embedded engine install it. `clink --capabilities` lists its record as `relate`.

## SQL usage

```sql
CREATE TABLE transfers (
  event_id VARCHAR, sender VARCHAR, recipient VARCHAR, amount BIGINT, at BIGINT
) WITH (connector = 'file', format = 'json', path = 'transfers.ndjson',
        event_time_column = 'at', watermark_lag_ms = '5000');

CREATE TABLE graph (
  event_id VARCHAR, sender VARCHAR, recipient VARCHAR, amount BIGINT, at BIGINT
) WITH (
  connector = 'relate',
  path      = 'store/payments.rl',
  schema    = 'CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE;
               CREATE CONSTRAINT transfer_id IF NOT EXISTS FOR (t:Transfer) REQUIRE t.id IS UNIQUE',
  statement = 'UNWIND $rows AS row
               WITH row.event_id AS id, collect(row)[0] AS row
               MERGE (s:Account {id: row.sender})
               MERGE (r:Account {id: row.recipient})
               MERGE (t:Transfer {id: id})
               WITH s, r, t, row, t.linked IS NULL AS first
               SET t.amount = row.amount, t.at = row.at, t.linked = true
               WITH s, r, t, first
               CALL (s, r, t, first) { WITH s, r, t, first WHERE first
                 CREATE (s)-[:SENT]->(t) CREATE (t)-[:TO]->(r) }'
);

INSERT INTO graph SELECT * FROM transfers;
```

The statement creates a transfer's relationships once, guarded by a mark on the transfer's node, so a replayed or concurrent copy of the row creates none; [Replay-safe statements](#replay-safe-statements) says why each line is there. Every column of the sink table becomes a key of each `row` map, so the table's columns are the statement's parameter contract. A table with one column is planned the same way. A query whose result retracts rows (an outer join, a top-N per key) is refused, as for any sink that only appends.

## Configuration

Options are parsed strictly when the sink is built: an unknown option is refused and the refusal lists the accepted ones, and a number must be the whole value and within range.

| Option | Required | Default | Meaning |
| --- | --- | --- | --- |
| `path` | yes | | The store: a directory, or a single file whose name ends in `.rl`. Created when it does not exist; its parent directory must. A relative path is read against the working directory of the process that runs the sink. `:memory:` and URIs are refused. |
| `statement` | yes | | The Cypher run once per batch, reading its rows from `$rows`. |
| `schema` | no | | Schema statements, separated by `;`, applied every time the sink opens and before the statement is checked. See [The schema option](#the-schema-option). |
| `batch_size` | no | `1000` | Rows per batch, 1 to 100000 (the most Relate takes in one list). |
| `batch_bytes` | no | `4194304` | Weight per batch, 1024 to 12582912, under the 16 MiB Relate allows one statement's parameters. A row weighs its names and strings plus 16 bytes for every value and container, and 8 more for a number or a boolean. A single row heavier than this is refused (`relate.row_too_large`). |
| `flush_interval_ms` | no | `1000` | A partial batch older than this is committed when the next rows arrive; 0 turns it off. With no input arriving, rows wait for the next checkpoint barrier or the end of the input. |
| `retry_window_ms` | no | `30000` | How long a batch's retries may go on after its first conflict; 0 makes one attempt. |

The sink accepts and ignores the keys the SQL planner puts on every sink op (`schema_columns`, `primary_key`, `bounded` and the like). It refuses what would change its guarantee, here as well as in the planner, since a job built directly on the Dag never passes through the planner: `delivery_guarantee` other than `at_least_once`, `mode='upsert'`, `changelog='true'` and `write_mode='overwrite'` (`relate.guarantee_unsupported`). `json_non_finite` is refused as meaningless: values are typed, and NaN and the infinities are stored as floats.

## Parameter contract

Each batch runs the statement with one parameter, `$rows`: a list holding one map per row, in arrival order, keyed by the table's column names, each value a Relate value of the column's declared type. The statement may use no other parameter. Your statement decides everything else: labels, relationship types, which columns become properties.

## Type mapping

| SQL type | Relate value | Notes |
| --- | --- | --- |
| `BOOLEAN` | BOOLEAN | |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | INTEGER | Exact across the whole 64-bit range. An integral double or the digits of an integer as text are accepted; a fraction is refused. |
| `REAL`, `DOUBLE` | FLOAT | NaN and the infinities are kept. An integer a double cannot hold exactly is refused. |
| `VARCHAR` | STRING | Valid UTF-8 only; a number or boolean is written as its exact text. |
| `DATE` | DATE | Days since 1970-01-01, or `YYYY-MM-DD`; years 1 to 9999. |
| `TIMESTAMP(p)` | LOCAL DATETIME | Millisecond precision, the engine's own; years 1 to 9999. |
| `TIMESTAMP(p) WITH TIME ZONE` | ZONED DATETIME at UTC | |
| `t ARRAY` | LIST | Elements typed by `t`, nested lists included; at most 100000 elements. |
| `MAP<VARCHAR, t>` | MAP | Keys must be strings of at most 4096 bytes with no NUL; other key types are refused. |
| `ROW<...>` | MAP | Keyed by the field names. |
| any, NULL | null | A missing column is null too. |
| `DECIMAL(p, s)` | refused | Relate has no decimal type: `CAST` to `VARCHAR` to keep every digit, or to `DOUBLE` to store it rounded. |
| `TIME`, `BYTEA` | refused | `CAST` to `VARCHAR`. |

A value that does not fit its declared type fails the task with `relate.value_invalid`, naming the column and the kind of value it held, never the value. A value Relate's builder itself refuses (text that is not UTF-8, a date that is not in the calendar) is reported the same way, with Relate's reason.

What a property can hold is Relate's rule, not the sink's: a map is for the statement to take apart (`row.address.city`).

## Transactions and checkpoints

- **One batch, one transaction.** The sink begins a transaction, runs the statement with the batch, and commits. Relate commits all of the batch's rows or none. A constraint violation on the last row rolls back the first.
- **What counts as committed.** The commit call returned without an error. It also returns a bookmark, `relate:<n>`, where `n` is the store's commit sequence; the sink keeps the last one for its summary line. A commit that wrote nothing, because every row was already in the graph, has no bookmark.
- **Barriers.** `on_barrier` commits the partial batch and returns only once it committed. The sink declares that its guarantee rests on its barrier hook (`Sink::gates_checkpoint_ack()`), so it must be the only sink on its chain: `Dag::add_sink` refuses another sink beside it, and SQL, clusters and the embedded engine give every sink a subtask of its own. A batch that fails at a barrier fails the subtask, and the checkpoint cannot complete; the rows stay buffered until they commit or the job fails, and a restore replays everything after the last completed checkpoint.
- **End of input.** The remaining rows are committed before the subtask finishes.
- **Cancellation.** A cancelled task begins nothing more, and rows still buffered are not written. Relate's C interface has no way to interrupt a statement, so a cancel that arrives while a batch is being committed waits for that batch.
- **No two-phase commit.** Relate has no way to prepare a transaction and commit it later, so a batch cannot be held across clink's recovery. The sink does not try.
- **Publication is batch by batch.** A job that fails part way leaves the batches it committed. A store that must hold a whole run or nothing needs a rule outside the sink, such as writing to a working path and moving it when the job has finished.
- **A killed process.** What a restore relies on, measured on the tested version outside clink: a process committing 50-node batches in a loop was killed with `SIGKILL` eight times, four times for each store layout. Each time a new process reopened the store at once, every batch the writer had reported committed was there, and no batch was there in part. A batch committed in the instant before the kill can be there without the writer having known; the sink's replay applies it again, which a replay-safe statement absorbs.

## Conflicts and retries

Several transactions can be open on one store at once: one from each sink subtask and each sink table that writes it. Relate lets one of two transactions that wrote the same node commit and refuses the other at its commit as a write-write conflict, marked as one a retry can win. The sink rolls the refused transaction back, waits a short pause that grows with each attempt, jittered and at most 500 ms, and runs the batch again; nothing of a refused attempt is in the store. Relate does not report every overlap as a conflict: two transactions that each create a relationship between the same two nodes both commit (see [Replay-safe statements](#replay-safe-statements)). Retries go on for `retry_window_ms` from the batch's first conflict, then the task fails with `relate.retries_exhausted`.

Every other refusal is final: a constraint violation, a type error or a statement Relate cannot run fails the task at once with `relate.batch_refused`, the batch rolled back whole.

## Replay-safe statements

A statement converges under replay when applying a batch twice, in either order and at the same time as another sink's batches, leaves the same graph as applying it once:

- **Key every node by a stable business id under a uniqueness constraint**, and reach it with `MERGE` on that id. Declare the constraint in the [schema option](#the-schema-option). The constraint is what makes two transactions merging one id meet as a conflict, and one of them retry, instead of creating two nodes. Measured on the tested version: four transactions merging the same 1000 ids at once left 1970 to 2515 duplicate nodes without the constraint, and none with it.
- **Decide on a node whether to create a relationship.** Relate has no uniqueness constraint for relationships, and two transactions that `MERGE` the same relationship at the same time both create it: four transactions merging the same 1000 relationships between existing nodes at once left 1765 to 2275 duplicates, with no conflict reported. Two transactions that write the same node do conflict. So read a mark on the event's node, set it, and create the relationships in a `CALL` that runs only when the mark was not set, as the [example](#sql-usage) does with `t.linked`. A concurrent copy writes the same node, is refused as a conflict, retries, finds the mark and creates nothing; a replayed copy finds the mark at once. Measured the same way, with three transactions creating relationships this way and a fourth merging the same nodes, first or at once: every relationship created, none twice. A mark set only when the `MERGE` creates the node (`ON CREATE SET`) is not enough when another table can create the node first: the owning statement then never sees it created, and never creates the relationships.
- **Reduce each batch to one row per event first** (`WITH row.event_id AS id, collect(row)[0] AS row`). Cypher runs each clause over all of a batch's rows before the next, so two rows for one event in one batch both read the mark before either sets it. Measured: a batch holding each of ten events twice created 40 relationships when the statement passed every row on, and the right 20 when it reduced them first.
- **A plain `MERGE` of a relationship is safe only when nothing can write that relationship at the same time**: one sink table writes it, at parallelism 1, from input that holds each event once. A replay after a restore is sequential, and finds what the first run wrote.
- **Do not rely on arrival order across tables.** Two sink tables on one store commit independently, so a row that points at another table's node can arrive first. `MERGE` the node by its id in both statements and let the table that owns its properties `SET` them.
- **A mutable property needs an ordering rule**, or an older event replayed after a newer one overwrites it. Keep the position of the value beside it and write only when the row is newer.

Measured through the sink on the tested version: batches applied again leave the same nodes and relationships, three jobs writing one store from an empty start converge on the graph their input implies, and two sinks writing the same sixty accounts concurrently for hundreds of batches end with sixty accounts and every conflict retried.

## The schema option

A sink that keys its nodes needs constraints, and an embedded store has no server to create them on beforehand. `schema` takes the statements that do it, separated by `;` (a `;` inside a quoted string, a backticked name or a comment does not separate):

```sql
schema = 'CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE;
          CREATE INDEX account_name IF NOT EXISTS FOR (a:Account) ON (a.name)'
```

Each one is applied every time the sink opens, in order, on its own. Write them with `IF NOT EXISTS`: without it the second open fails. Only schema statements are taken. Each is first explained, which runs nothing, and one that Relate does not class as a schema change is refused (`relate.schema_refused`) before it can write. Sinks opening on one store apply their schemas one sink at a time.

## What the open check refuses

Before any row is written, the sink runs the statement over an empty `$rows` inside a transaction and rolls it back. Nothing is written, even by a clause that comes before the `UNWIND`. Relate has by then parsed the statement, resolved its parameters and classed it, and the sink refuses (`relate.statement_refused`) a statement that:

- Relate cannot parse or plan;
- uses a parameter other than `$rows`;
- only reads;
- returns columns, which the sink would discard;
- changes the schema, which belongs in the `schema` option;
- uses `CALL { ... } IN TRANSACTIONS`, which cannot run inside the batch's transaction.

A store that cannot be opened is refused with `relate.store_unavailable`. Relate reports a store it cannot lock or read only as an internal error, without its reason, so the refusal lists the likely causes; setting `RELATE_FFI_LOG=1` in the environment makes Relate log its own reason to stderr.

## One store, one process

Relate gives a store one writing process, and refuses a second handle on it even to the process that holds the first. The sink therefore keeps one handle per store per process: the first sink to open a store opens it, every other sink on that store in the same process shares the handle with a connection of its own, and the last one to close releases it.

- In `clink run <file>.sql` and the embedded engine, every job is in one process, so any number of tables and subtasks can write one store.
- On a cluster, every sink on a store must be placed in the same Worker process. A sink in a second process fails at open with `relate.store_unavailable`. A store is a path on the machine that runs the sink: it is not shared between machines.

Another process can read a store while a job writes it only when the store is a directory. Measured on the tested version: a second process opened a directory store read-only beside its writer, and was refused a single `.rl` file. Relate documents such a read as a view of the store as it was when it was opened.

## Metrics and logs

The sink's own series are in the task's registry, tagged with its `op_id`:

| Metric | Meaning |
| --- | --- |
| `clink_relate_batches_committed_total`, `clink_relate_rows_committed_total` | what Relate committed |
| `clink_relate_commit_latency_ns` | the committing attempt, from its begin to its commit (histogram) |
| `clink_relate_attempts_failed_total{retryable}` | attempts that were rolled back, by whether Relate said a retry can succeed |
| `clink_relate_retries_total` | attempts retried |
| `clink_relate_buffered_rows`, `clink_relate_buffered_bytes` | the pending batch (gauges) |

The cross-connector series carry `connector="relate"`: `clink_connector_records_total`, `clink_connector_bytes_total` and `clink_connector_commit_latency_ns` count committed batches only, and `clink_connector_errors_total` counts failures.

Log lines, on the `sink.relate` channel: `relate sink open:` (subtask, store, the number of schema statements, batch limits), one warning per attempt that was rolled back with Relate's reason and the wait before the retry, a line when a batch commits after retries, and `relate sink closed:` or `relate sink cancelled:` with the counts, the rows not written and the last bookmark.

## Refusals

| Code | When |
| --- | --- |
| `relate.option_invalid` | an unknown option, a malformed or out-of-range value, a missing `path`, `statement` or column list, a `path` that is `:memory:` or a URI |
| `relate.guarantee_unsupported` | `delivery_guarantee` other than `at_least_once`, `mode='upsert'`, `changelog='true'`, `write_mode='overwrite'` |
| `relate.type_unsupported` | a `DECIMAL`, `TIME`, `BYTEA`, non-string-keyed `MAP` or unknown column type |
| `relate.store_unavailable` | the store could not be opened or created: its directory is missing, another process holds it, or the path is not a Relate store |
| `relate.schema_refused` | a `schema` statement that is not a schema statement, that Relate cannot parse, or that failed when applied |
| `relate.statement_refused` | the open check refused the statement |
| `relate.value_invalid` | a value that does not fit its declared column type, or that Relate's builder refused |
| `relate.row_too_large` | one row heavier than `batch_bytes` |
| `relate.batch_refused` | Relate rolled a batch back for a reason a retry would meet again (a constraint, a type error) |
| `relate.retries_exhausted` | `retry_window_ms` passed without a commit |
| `relate.cancelled` | the task was cancelled before the batch committed |
| `relate.engine_failure` | the connection to the store failed, or rows reached a sink that is not open |

## Limitations

- At least once. A statement that is not replay-safe duplicates its effects after a restore.
- One writing process per store, and the store is a local path.
- The whole graph is in the memory of the process that runs the sink.
- One transaction per batch, one at a time per subtask. Subtasks and tables write concurrently, so the statement must tolerate concurrent batches (see [Replay-safe statements](#replay-safe-statements)).
- No relationship uniqueness: concurrent transactions that create the same relationship both commit it.
- Batches from different subtasks, and from different sink tables, are separate transactions: a reader can see one table's batch before another's.
- A job that fails part way leaves the batches it committed.
- A statement in flight cannot be interrupted.
- Rows that arrive while no barrier comes and the input is idle wait for the next barrier or the end of the input.
- No source, no lookup and no changelog input.
- Tested on Relate 1.0.1, on macOS arm64 and Linux aarch64; not built on Linux x86_64 or in CI. No recovery cell kills a clink Worker around a commit yet; what Relate itself does when its process is killed is [measured above](#transactions-and-checkpoints).

## Testing

Two suites, both under `ctest --test-dir build -L relate`, and nothing in them is skipped: Relate is embedded, so no case needs a server.

- `clink_relate_tests`, the sink on its own: options and refusals, the declared types and each one written into a store and read back, the values that are refused, the batch weight against Relate's own limits, and the sink against real stores under the temporary directory: batching by rows, bytes and age, barriers, a replayed batch, the open check and the schema option, a refused batch rolled back whole with its rows still buffered, two sinks sharing one store and retrying their conflicts, a store that cannot be opened, cancellation, and both store layouts. A second process, `clink_relate_store_holder`, holds a store while a sink tries to open it, for each layout, and is then killed with `SIGKILL`, after which the sink opens the store and writes.
- `clink_relate_sql_tests`, through the SQL frontend: the op the planner builds and the plans it refuses, an embedded pipeline written into a store and run again into the same store, three jobs writing one store at once (events, a windowed aggregate pointing at its events, and links from an interval join) and converging, and a statement the open check refuses failing its job.

Every case reads the store back through Relate's C library on a connection of its own, so what it counts is what the store holds.

Not yet covered: a recovery cell that kills a clink Worker around a commit, Linux x86_64, and CI, whose toolchain image does not install Relate's library.
