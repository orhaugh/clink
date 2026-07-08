# Built-in sinks (blackhole, changelog, print)

Three Row-channel sinks compiled into the SQL frontend itself - no client
library, no `CLINK_WITH_*` knob, always available when `CLINK_BUILD_SQL=ON`.
They are registered by `clink::sql::install()` (`src/sql/install.cpp`) and
selected by connector name in `CREATE TABLE ... WITH (connector='...')`; none
of them takes any further option.

All three natively accept changelog streams (retracting GROUP BY, Top-N,
outer joins), which append-only sinks reject.

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
