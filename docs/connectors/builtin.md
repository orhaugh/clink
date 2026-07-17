# Built-in connectors (blackhole, changelog, print, collect, queryable_state)

Row-channel sinks compiled into the engine itself - no client library, no
`CLINK_WITH_*` knob, always available when `CLINK_BUILD_SQL=ON`. They are
selected by connector name in `CREATE TABLE ... WITH (connector='...')` and
take no further option. blackhole, changelog and print are registered by
`clink::sql::install()` (`src/sql/install.cpp`); collect is registered by the
embedded engine and only works there.

blackhole, changelog and print natively accept changelog streams (retracting
GROUP BY, Top-N, outer joins), which append-only sinks reject; collect is
append-only in v1.

## `blackhole`

Counts then drops every row (the runner's `records_in` metric still tallies
them). For benchmarks where sink I/O must not distort throughput, and for
driving a job whose output is irrelevant.

```sql
CREATE TABLE sink (bidder BIGINT, bid_count BIGINT) WITH (connector='blackhole');
```

## `changelog`

Nets a changelog stream (insert / delete / update_before / update_after) into
its final relation by full-row multiplicity - no primary key needed - and
writes the survivors on flush. For keyless changelog outputs, e.g. a
retracting aggregate joined to another stream.

## `print`

One JSON line per record to stdout, flushed per record. The demo and
debugging sink, and what a bare `SELECT` compiles onto under
`clink run` (see [embedded execution](../internals/embedded.md)). Append
rows print as plain JSON objects; changelog rows keep their meaning: a
non-insert kind prefixes the line and the `__row_kind` marker itself is
stripped from the printed object.

```text
{"usr":"ada","total":10}
-U {"usr":"ada","total":10}
+U {"usr":"ada","total":42}
```

Lines are written with a single `fwrite` each, so concurrent subtasks at
parallelism above 1 interleave whole lines, never fragments. Row order
across subtasks is not defined, same as any parallel sink.

## `queryable_state` (source)

State-as-table: a bounded source that snapshots another job's LIVE
queryable state through the Coordinator's JSON scan route and emits one
row per key - the served value document's fields become the row's
columns. One job's running aggregates are another job's table, with no
sink round-trip. SQL `aggregate_row` (unbounded GROUP BY, in-memory
path) exposes its state automatically, so any GROUP BY job is readable
this way while it runs.

```sql
CREATE TABLE live_totals (usr TEXT, total BIGINT)
WITH (connector='queryable_state', format='json',
      coordinator_port='8081', job_id='1');

SELECT usr, total FROM live_totals;   -- the CURRENT aggregate values
```

Options: `coordinator_port` (required - the Coordinator's HTTP port), `job_id`
(required), `coordinator_host` (default `127.0.0.1`), `role` (default the generic
subtask role), `slot` (default `agg`), `limit` (default 100000, the
route's cap), `batch_size` (default 256). The snapshot is taken once, at
the first produce; `truncated` results are cut at `limit`. Delivery is a
bounded read of a moving target - consistent per subtask scan, not a
global point-in-time snapshot.

## `collect` (embedded only)

Delivers the table's rows to the HOST PROCESS as typed Arrow record batches
- the results surface of the embedded engine and libclink. Read it in C++
via `EmbeddedEngine::collect_reader(table)` or from any language through
`clink_collect_stream()` (the Arrow C stream interface; zero-copy into
pyarrow, DuckDB, polars). One consumer per table; reads block until data,
end after the producing job finishes, and cancel when the engine closes.
Append-only in v1: a retracting (changelog) SELECT is rejected at bind.
Embedded-only by design - on a cluster there is no host process to deliver
to, and the submit fails loudly. See
[embedded execution](../internals/embedded.md).

```sql
CREATE TABLE results (user_id BIGINT, total BIGINT) WITH (connector='collect');
INSERT INTO results SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
```

(An unbounded GROUP BY emits the running total per input row - the last
batch row per key is the final answer, the same convention as the file
sink.)
