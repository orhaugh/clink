# SQL

A practical guide to the SQL clink runs: the statements, clauses, types and
functions it supports, and how to put them to work. This is the reference you
write pipelines from. For how the frontend turns SQL into operators (parse, bind,
plan, optimise), see the [SQL frontend internals](internals/sql-frontend.md).

clink runs **streaming SQL**. A query is a standing pipeline over a source that
may never end, not a one-shot request against a table at rest. You declare where
data comes from and where results go, then wire them with `INSERT INTO ... SELECT`;
the job runs until you stop it (or until a bounded source drains).

The grammar is PostgreSQL 16, parsed with libpg_query, plus a small set of
streaming extensions: window functions in `GROUP BY`, `MATCH_RECOGNIZE`, and the
AI table functions. Most of what clink cannot support is rejected at compile time
with a source position rather than failing halfway through a run. A few constructs
parse but are deliberately ignored; those are called out under
[What is not supported](#what-is-not-supported).

## Running a script

Put statements in a `.sql` file and run it:

```sh
clink run pipeline.sql
```

Statements execute top to bottom. A typical script declares a source table,
declares a sink table, then issues one `INSERT INTO sink SELECT ... FROM source`
to start the standing job. `clink run` executes the whole script embedded in a
single process. Two flags matter for production runs:

- `--checkpoint-dir <dir>` turns on periodic checkpointing. If the process dies, running the same command again resumes the job from its last checkpoint; `--fresh` starts it over.
- `--capture-dir <dir>` records the run so it can be replayed deterministically.

See [Embedded execution](internals/embedded.md) for the full CLI.

### Several statements as one job

Each `INSERT` starts a job of its own. To run several as one job, put them in a
statement set:

```sql
EXECUTE STATEMENT SET BEGIN
  INSERT INTO orders_copy SELECT * FROM orders_cdc;
  INSERT INTO customers_copy SELECT * FROM customers_cdc;
END;
```

The statements in a set compile as before and are submitted together: one
job, checkpointed as one, its sources and sinks side by side. A set holds
`INSERT` statements only. Tables that must share one reader need it, such as
PostgreSQL CDC tables on one replication slot
([several tables on one slot](connectors/postgres.md#several-tables-on-one-slot));
two separate jobs naming the same slot are refused, with the set named.

A Kafka table can ask for one reader per job with `shared_reader='true'`.
Every statement in the set that reads it then reads the same stream through
one consumer, cut at the same checkpoint barriers, and the topic is fetched
once rather than once per statement. Without it, each statement has a reader
of its own, and at a checkpoint one statement may have read further than
another. A shared reader ties the statements' pace together: a slow sink holds
it back for every statement that reads the table. Setting or clearing it
changes the job's graph, so start the job fresh rather than restoring a
checkpoint taken with the other setting, which the statements would resume
from one position rather than each from its own.

Each sink in a set is held to the guarantee its own table declares in
`delivery_guarantee`, through the sources it reads, and a sink table that
declares none is held to the strongest guarantee any table in the set
declares. A set can therefore write one table exactly once and another at
least once, as long as the at-least-once table says so: a Kafka table with
`delivery_guarantee='exactly_once'` beside a Neo4j table with
`delivery_guarantee='at_least_once'`. Left undeclared, the Neo4j table would
be held to exactly once and the set refused. The job reports what its weakest
sink gives; its guarantee report (`guarantee_reasons` on
`GET /api/v1/jobs/:id`) gives the guarantee each sink is held to and the one
its own sources let it give.

## Your first pipeline

Read newline-delimited JSON, keep the checkout clicks, write them out:

```sql
CREATE TABLE clicks (user_id BIGINT, url TEXT, ts BIGINT)
WITH (connector='file', format='json', path='/tmp/clicks.ndjson',
      event_time_column='ts', watermark_lag_ms='200');

CREATE TABLE checkout_hits (user_id BIGINT, url TEXT)
WITH (connector='file', format='json', path='/tmp/checkout.ndjson');

INSERT INTO checkout_hits
SELECT user_id, url
FROM clicks
WHERE url LIKE '%checkout%';
```

Everything else in this guide builds on that shape: a source table, a
transformation, a sink.

## Tables and connectors

A table is a named binding to an external source or sink. It carries a column
schema and a `WITH (...)` option bag:

```sql
CREATE TABLE [IF NOT EXISTS] <name> ( <col> <type> [, ...] )
WITH ( <key>='<value>' [, ...] )
```

Every `WITH` value is a single-quoted string literal, including numeric ones
(`watermark_lag_ms='200'`, not `200`). An option KEY containing a dot (the
connector passthroughs, such as Kafka's `kafka.<property>`) is an identifier
and takes double quotes: `"kafka.client.rack" = 'eu-central-1a'`. A
single-quoted key is a string literal, which the parser rejects. Columns are `name TYPE` pairs. An inline
`PRIMARY KEY` on a single column is honoured: it populates exactly the same
catalogue metadata as `WITH (primary_key='col')`, and if both forms are given
they must agree or the statement is rejected. A composite key is declared with
`WITH (primary_key='a,b')`; several inline `PRIMARY KEY` markers are rejected
with that form named. Every other column constraint clink cannot enforce -
`NOT NULL`, `CHECK`, `UNIQUE`, `DEFAULT`, `REFERENCES` - is rejected with a
diagnostic rather than parsed and ignored, as are table-level constraints.
Every column is nullable.

### Choosing the channel

clink moves records on one of two channels, chosen from the schema and options:

- **Row channel**: set `format='json'`, or declare more than one column. This is
  what you want almost always. Joins, `MATCH_RECOGNIZE` and the AI functions
  require it.
- **String channel**: a single `TEXT`/`VARCHAR` column with no `format='json'`,
  for raw line-oriented data.

A table with more than one column and no `format='json'` is a compile error, so
if in doubt, declare `format='json'`.

### Common options

The connector decides which options apply; these are the ones most pipelines set.
Per-connector options live in the [connector reference](connectors/README.md).

| Option | Meaning |
|---|---|
| `connector` | required; the source or sink kind (see below) |
| `format` | `'json'` selects the Row channel |
| `event_time_column` | the column carrying event time; enables windowing |
| `watermark_lag_ms` | how far watermarks trail the max seen event time (out-of-orderness) |
| `idle_timeout_ms` | advance watermarks when a source partition goes quiet |
| `allowed_lateness_ms` | grace period after a window fires (default `0`) |
| `late_records_to_dlq` | `'true'` routes fully-late records to a dead-letter path |
| `mode` | `'append'` (default), `'upsert'`, or `'cdc'` |
| `primary_key` | comma-separated key columns; required for `mode='upsert'` |
| `delivery_guarantee` | `'at_least_once'` (default) or `'exactly_once'` |
| `partition_by` | partition column(s) for a file or object-store sink |
| `json_non_finite` | how a sink that writes JSON writes NaN and the infinities: `'null'` (default) or `'string'` (`"NaN"`, `"Infinity"`, `"-Infinity"`); see [NaN and infinity in JSON output](connectors/README.md#nan-and-infinity-in-json-output) |

### Event time and watermarks

Windowing, interval joins and `MATCH_RECOGNIZE` are driven by **event time**, not
arrival time. Declare which column carries it and how far out of order it may be:

```sql
CREATE TABLE bids (auction BIGINT, price BIGINT, datetime BIGINT)
WITH (connector='kafka', format='json', topic='bids',
      brokers='localhost:9092',
      event_time_column='datetime', watermark_lag_ms='2000');
```

A watermark of "max event time seen minus `watermark_lag_ms`" then advances the
window machinery. A source with no `event_time_column` cannot drive event-time
windows.

### Connector catalogue

`connector` names the kind. Sources you can read from include `file`, `kafka`,
`rabbitmq`, `websocket`, `nats`, `pulsar`, `postgres`, `mysql`, `clickhouse`,
`parquet`, `s3_parquet` (and `gcs_parquet` / `azure_parquet` / `webhdfs_parquet`),
`kinesis`, `http_poll`, `pubsub`, `redis`, `iceberg`, `nexmark`, `lookup` and
`queryable_state`. Sinks you can write to include `file`, `kafka`, `rabbitmq`,
`nats`, `pulsar`, `pubsub`, `parquet`, `s3` / `s3_parquet` (and `gcs_parquet` /
`azure_parquet` / `webhdfs_parquet`), `iceberg`, `delta`, `postgres`, `mysql`,
`clickhouse`, `cassandra`, `redis`, `http`, `elasticsearch`, `opensearch`,
`splunk_hec` (alias `splunk`), `influxdb`, `prometheus`, `firehose`, `dynamodb`,
`changelog`, and the debugging sinks `print`, `collect` and `blackhole`. The
[connector catalogue](connectors/README.md) has the full list with each
connector's options and delivery guarantee.

A connector must be linked into the runtime you run. The planner always emits the
right factory name; a connector that is not linked fails at deploy time, not
compile time.

## Data types

Types are nullable everywhere. libpg_query normalises some spellings before clink
sees them, so several aliases resolve to the same type.

| SQL spellings | Stored as |
|---|---|
| `BIGINT`, `INT8` | 64-bit integer |
| `INTEGER`, `INT`, `INT4` | 32-bit integer |
| `SMALLINT`, `INT2` | 16-bit integer |
| `REAL`, `FLOAT4` | 32-bit float |
| `DOUBLE PRECISION`, `FLOAT8`, `DOUBLE` | 64-bit float |
| `BOOLEAN`, `BOOL` | boolean |
| `TEXT`, `VARCHAR`, `VARCHAR(n)`, `CHAR`, `STRING` | UTF-8 string (length modifiers ignored) |
| `NUMERIC` / `DECIMAL` `(p,s)` | exact fixed-point, up to 38 digits (default `(38,9)`) |
| `TIMESTAMP` `(p)` | timestamp, precision `p` in 0-9 (default microseconds) |
| `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | timestamp, UTC |
| `DATE` | date |
| `TIME` | time of day |
| `BYTEA` | binary |

Composite and array types (available as column types):

| SQL spelling | Meaning |
|---|---|
| `T[]`, `T[][]`, `T ARRAY` | array (nested per dimension) |
| `ARRAY<T>` | array (angle form) |
| `MAP<K, V>` | map |
| `ROW<f1 T1, f2 T2, ...>` | struct with named fields |
| `MULTISET<T>` | bag (list semantics) |

They nest, for example `MAP<TEXT, ARRAY<INT>>` or `ROW<id BIGINT, attrs MAP<TEXT,TEXT>>`.

```sql
CREATE TABLE t (
  id     BIGINT,
  amount DECIMAL(18,2),
  tags   MULTISET<TEXT>,
  addr   ROW<city TEXT, zip INT>
) WITH (connector='file', format='json', path='/tmp/t.ndjson');
```

## Selecting, filtering, projecting

`SELECT` chooses and computes columns; `WHERE` filters rows. Predicates support
the usual comparisons and combinators:

| Form | Notes |
|---|---|
| `=` `<>` `!=` `<` `<=` `>` `>=` | column-vs-literal, column-vs-column, or expression operands |
| `AND` `OR` `NOT` | boolean combinators |
| `flagged` | a `BOOLEAN` column on its own, as `flagged = true`: NULL does not hold; also in `CASE WHEN` |
| `x BETWEEN lo AND hi` | `x` must be a bare column |
| `x IN (v, ...)` / `NOT IN` | literal value lists |
| `x IS NULL` / `IS NOT NULL` | two-valued |
| `s LIKE 'pat'` | `%` and `_` wildcards (there is no `ILIKE`) |
| `EXISTS (subquery)` / `NOT EXISTS` | correlated single-table subquery |
| `col IN (subquery)` / `NOT IN` | single- or multi-column, NULL-aware |
| `col op (SELECT agg(x) FROM ...)` | scalar subquery, must be an aggregate |

```sql
SELECT user_id, url
FROM clicks
WHERE user_id BETWEEN 5 AND 10
  AND url LIKE '%checkout%'
  AND url IS NOT NULL;

SELECT * FROM orders
WHERE user_id IN (SELECT user_id FROM vips);
```

`WITH name AS (SELECT ...)` names a query that the rest of the statement reads
as a table, and each common table expression can read the ones before it. A
statement may read the same one more than once: each reference runs its query
on its own and reads its tables again, so a Kafka table read twice has a
reader for each read unless it declares `shared_reader='true'`.

```sql
WITH once AS (SELECT DISTINCT transaction_id, ts FROM transactions)
SELECT transaction_id FROM once WHERE ts > 100
UNION ALL
SELECT transaction_id FROM once WHERE ts <= 100;
```

## Functions

A function name is resolved as a user-defined function first (see
[Functions you define](#functions-you-define)) and as a built-in otherwise, so
a function you define shadows a built-in of the same name, and a built-in
added in a later release can never change what an existing script computes.
Which statements, functions and options the 1.x line holds stable is set out
on the [Compatibility](compatibility.md) page.

### String

| Function | Purpose |
|---|---|
| `upper(s)`, `lower(s)`, `initcap(s)` | case folding and title case |
| `length(s)`, `char_length(s)` | byte length |
| `s1 \|\| s2`, `concat(a, ...)` | concatenation (any NULL makes the result NULL) |
| `substring(s, start[, len])` | 1-based, bounds clamp |
| `position(needle IN haystack)` | 1-based index, `0` if absent |
| `replace(s, from, to)` | replace all occurrences |
| `btrim/ltrim/rtrim(s[, chars])` | trim (whitespace by default) |
| `lpad/rpad(s, len[, pad])` | pad or truncate to length |
| `left(s, n)`, `right(s, n)` | prefix / suffix |
| `split_part(s, delim, idx)` | 1-based field; `split_index` is 0-based |
| `regexp_extract(s, pattern[, group])` | first match (ECMAScript regex) |
| `starts_with(s, prefix)` | boolean |
| `reverse(s)`, `repeat(s, n)`, `ascii(s)`, `chr(n)` | byte utilities |

### Numeric

| Function | Purpose |
|---|---|
| `+ - * / %` | arithmetic (see [numeric model](#deterministic-by-design)) |
| `abs`, `sign`, `floor`, `ceil`/`ceiling` | rounding and sign |
| `round(x[, digits])`, `trunc(x[, digits])` | round / truncate to digits |
| `sqrt`, `exp`, `ln`, `log10` | transcendentals |
| `power(b, e)` / `pow(b, e)`, `mod(a, b)` | power and modulo |

### Date and time

Timestamps are handled as UTC integers, with no locale or timezone database, so
results are reproducible.

| Function | Purpose |
|---|---|
| `extract(<field> FROM ts)` | `year, month, day, hour, minute, second, dow, doy, quarter, epoch` |
| `date_trunc('<unit>', ts)` | truncate to `second`...`year` |
| `date_format(ts, '<fmt>')` | format with `yyyy MM dd HH mm ss` |
| `to_timestamp(s[, '<fmt>'])` | parse (default `'yyyy-MM-dd HH:mm:ss'`) |

```sql
SELECT id, EXTRACT(YEAR FROM ts) AS yr, DATE_TRUNC('hour', ts) AS hr FROM t;
```

### Conditional and null

| Form | Purpose |
|---|---|
| `CASE WHEN p THEN v [WHEN ...] [ELSE v] END` | searched CASE; only the first matching branch evaluates |
| `coalesce(a, ...)` | first non-NULL |
| `nullif(a, b)` | NULL if the two are equal |
| `greatest(...)`, `least(...)` | NULL-ignoring extremum |

### CAST

```sql
CAST(x AS <type>)
```

Supported targets are integer (`BIGINT`/`INT`/`SMALLINT`), float
(`DOUBLE PRECISION`/`REAL`), `NUMERIC`/`DECIMAL` (precision and scale carried
through), text (`TEXT`/`VARCHAR`) and `BOOLEAN`. Casting to date, timestamp or
`BYTEA` is not supported.

### JSON and collections

| Form | Purpose |
|---|---|
| `json_value(j, path)` | scalar at a path (as text) |
| `json_query(j, path)` | object/array at a path (as JSON text) |
| `json_exists(j, path)` | boolean |
| `json_object(k, v, ...)` | build a JSON object (a NaN or infinite value is written as `null`) |
| `ARRAY[e1, e2, ...]`, `a[i]` | array literal, 1-based subscript |
| `MAP(k1, v1, ...)`, `m['k']` | map literal and lookup |
| `ROW(v1, ...)`, `(r).f` | row literal and field access |
| `cardinality(x)`, `element(x)` | element count; sole element of a singleton |

JSON paths support `$`, `.key` and `[index]`.

<a id="deterministic-by-design"></a>

!!! note "Deterministic by design"
    Two things follow from clink keeping SQL results reproducible:

    - **No wall-clock functions.** `now()`, `current_timestamp`, `current_date`,
      `current_time`, `localtime` and `localtimestamp` are rejected. Drive time
      from an `event_time_column` instead. A replay then reproduces a run exactly.
    - **Exact numerics.** Integers are held as 64-bit and stay exact well past
      2^53; a fractional literal becomes an exact `DECIMAL`, not a lossy double;
      `SUM(BIGINT)` and `DECIMAL(p,s)` arithmetic are exact end to end (overflow
      and divide-by-zero yield NULL). Declare `DOUBLE PRECISION` if you would
      rather have speed than exactness.

## Aggregating

`GROUP BY` produces one row per key. A `SELECT` item may be a group key, an
aggregate, or any expression over them: arithmetic, `CASE`, casts and scalar
functions of aggregates, aggregates of different columns combined, and, in a
windowed `GROUP BY`, `window_start` and `window_end`. A column that is neither
grouped nor inside an aggregate is refused by name, and there is no `SELECT *`
with `GROUP BY`.

```sql
SELECT k, SUM(n) / 8 AS eighths,
       CAST(SUM(n) AS DOUBLE) / COUNT(*) AS mean,
       CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS size,
       window_end - window_start AS width
FROM events
GROUP BY k, TUMBLE(ts, INTERVAL '1' SECOND);
```

When a `SELECT` item is an expression, each distinct aggregate call is computed
once however many expressions use it.
An aggregate's argument must be a column reference (`SUM(price * qty)` is
refused; compute the product in a derived table first), and a window function
(`... OVER (...)`) cannot sit inside an expression of a grouped `SELECT`.

| Aggregate | Notes |
|---|---|
| `COUNT(x)`, `COUNT(*)` | row / non-null counts |
| `SUM(x)`, `AVG(x)` | `SUM` takes the type of `x`: an exact `BIGINT` over `BIGINT` or `INTEGER`, an exact `DECIMAL` over `DECIMAL`, a `DOUBLE` over `DOUBLE` or `REAL`, whole values included; `AVG` is `DOUBLE` |
| `MIN(x)`, `MAX(x)` | input type |
| `STDDEV[_POP\|_SAMP]`, `VARIANCE`, `VAR_POP`, `VAR_SAMP` | `DOUBLE` |
| `STRING_AGG(x[, sep])` / `LISTAGG` | string concatenation (default separator `,`) |
| `ARRAY_AGG(x)` / `COLLECT` | collect values into an array |
| `PERCENTILE(x, f)`, `APPROX_PERCENTILE(x, f)` | fraction `f` in `[0,1]` |

`HAVING` filters grouped results and requires a `GROUP BY`. It takes the same
expressions as the `SELECT` list, may name an aggregate the `SELECT` does not
compute (`HAVING COUNT(*) > 1` with no `COUNT(*)` selected), and may use an
aggregate item's alias. Its top level must be a predicate (a comparison,
`AND` / `OR` / `NOT`, `IS [NOT] NULL`, `LIKE`, `BETWEEN` or `IN`), as in `WHERE`.
`DISTINCT` inside an aggregate is supported only for `COUNT`, `STRING_AGG` and
`ARRAY_AGG`.

```sql
SELECT url, COUNT(*) AS n, COUNT(DISTINCT user_id) AS uniques
FROM clicks
GROUP BY url
HAVING n > 10;
```

!!! warning "Unbounded state"
    A plain (non-windowed) `GROUP BY`, `SELECT DISTINCT`, a stream-stream join, a
    set operation and a ranking (`ROW_NUMBER` and its kin) keep state for every key
    or row they have seen. Over an unbounded source such a query is refused when it
    is planned unless that state is bounded: a window on the aggregation or the
    ranking, a time-bounded `ON` condition on the join, or `state_ttl` on a table
    the query reads (`'30s'`, `'15m'`, `'1h'`, `'7d'` or a number of milliseconds),
    which releases a key once no row has touched it for that long, in event time
    unless the table sets `state_ttl_domain = 'processing_time'`. `state_ttl` does
    not bound a ranking. Ending the query with `ALLOW UNBOUNDED STATE` accepts the
    state as it is.

## Windows

Window aggregation groups by a time window. The window is a table-valued function
in the `GROUP BY`, taking the event-time column as its first argument. Sizes are
milliseconds as a plain integer, or an `INTERVAL '<n>' SECOND|MINUTE|HOUR|DAY`.

| Window | Arguments |
|---|---|
| `TUMBLE(time_col, size)` | fixed, non-overlapping |
| `HOP(time_col, size, slide)` | sliding; `size` then `slide` |
| `SESSION(time_col, gap)` | closes after `gap` of inactivity |
| `CUMULATE(time_col, step, size)` | growing; `step` then `size` |

Note the argument order: `HOP` takes size then slide, `CUMULATE` takes step then
size. Pass the same column you declared as `event_time_column`; watermarks only
advance on that column. The synthetic columns `window_start` and `window_end`
(epoch-ms `BIGINT`) are projectable whenever a window function is present.

```sql
SELECT bidder, COUNT(*) AS num, window_start AS ws, window_end AS we
FROM bids
GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder;
```

## Joins

A join reads as `FROM a JOIN b ON <condition>`. Only `ON` is accepted;
`USING (...)` and `NATURAL JOIN` are not supported. In clauses after the join,
refer to columns either qualified (`a.x`) or flattened (`a_x`). Each side may be
a table, a common table expression or an aliased subquery, `(SELECT ...) AS a`,
with every kind of join below.

**Inner equi-join.** The common case:

```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
```

**Interval join.** Both sides declare an `event_time_column`; the condition is an
equality plus a time band. State is bounded by the band, so this suits streams:

```sql
SELECT * FROM clicks c JOIN impressions i
  ON c.user_id = i.user_id
  AND c.click_ts BETWEEN i.imp_ts - INTERVAL '5' SECOND
                     AND i.imp_ts + INTERVAL '10' SECOND;
```

**Lookup (enrichment) join.** The right table is a dimension declared
`WITH (connector='lookup', function='<registered_fn>')`, resolved per row:

```sql
SELECT * FROM orders o JOIN customers c ON o.cust = c.id;
```

`INNER` drops the probe rows with no match; `LEFT` keeps them with the lookup's
columns NULL. One equality, the lookup table on the right. `function='neo4j'`
reads each key from Neo4j with a parameterised, read-only Cypher statement:
zero or one row per key, typed scalar columns, an explicit no-match, and a
failure that fails the job rather than becoming a no-match (see the
[Neo4j connector](connectors/neo4j.md#lookup)).

**Semi / anti joins** are expressed with `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS`
(see [filtering](#selecting-filtering-projecting)).

## Running totals and top-N

**`OVER` aggregates** compute a value per row against a partition, ordered by the
event-time column:

```sql
SELECT *, SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) AS running
FROM payments;
```

`OVER` supports `SUM/COUNT/AVG/MIN/MAX`, plus `FIRST_VALUE`, `LAST_VALUE` and
`LAG(col[, offset])`, with an optional `ROWS/RANGE BETWEEN n PRECEDING AND CURRENT ROW`
frame on the plain aggregates. A `RANGE` frame's `n` is in the units of the
`ORDER BY` column: `RANGE BETWEEN 3600000 PRECEDING AND CURRENT ROW` is the last
hour of an epoch-millisecond time, and an interval in seconds, minutes, hours or
days counts milliseconds, so `RANGE BETWEEN INTERVAL '28' DAY PRECEDING AND
CURRENT ROW` is the last 28 days. `LEAD`, `NTILE` and `FOLLOWING` frames are not
supported.

The query selects `*` beside its `OVER` aggregates; other select items are
refused, so project in an outer query. It reads a table that declares
`event_time_column`, with an optional `WHERE`, or a common table expression or
subquery over one that passes the time column through unchanged: through
`DISTINCT`, a filter, a projection that keeps or renames it, or another `OVER`
aggregate. A step that drops duplicates can therefore stand in front of a
window, and a window per account can feed a window per account and payee. A
derived table that computes, groups or joins carries no event-time column, and
is refused. Each partition's rows are emitted in event-time order as the
watermark passes them, each with the event time of its row, and a row that
arrives behind the watermark is dropped.

**Top-N per key** uses a ranking function in a subquery with an outer bound:

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn
  FROM clicks
) sub
WHERE rn <= 3;
```

`ROW_NUMBER`, `RANK` and `DENSE_RANK` are supported. The outer bound must be
`rn <= N`, `rn < N` or `rn = 1`.

## Pattern matching

`MATCH_RECOGNIZE` finds sequences of rows within a partition. When `ORDER BY`
names the table's event-time column (`event_time_column`, which a common table
expression or subquery can pass through), each partition's rows are matched in
event-time order: a row is held until the watermark passes it, then takes its
place among the partition's rows by time, rows with the same time in the order
they arrived. A match is therefore emitted once the watermark passes its last
row, and held rows are part of the job's checkpoint. A row that arrives at or
behind the watermark is late and dropped, as an `OVER` aggregate drops it.
Ordered by any other column, or over a table without an event-time column, rows
are matched in the order they arrive, so the input has to arrive in `ORDER BY`
order. It needs the Row channel (`format='json'`).

```sql
SELECT * FROM ticks MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY ts
  MEASURES FIRST(down.price) AS start_price, LAST(down.price) AS bottom
  PATTERN (start down+ up)
  DEFINE down AS price < PREV(price),
         up   AS price > PREV(price)
);
```

Supported: `PARTITION BY`, a single event-time `ORDER BY`, `MEASURES` with
`FIRST` / `LAST` / `CLASSIFIER()`, `PATTERN` with the quantifiers `+ * ?`, `{n}`
and `{n,m}`, per-row `DEFINE` predicates using `PREV(col)`, `ONE ROWS PER MATCH`
(default) or `ALL ROWS PER MATCH`, and `AFTER MATCH SKIP PAST LAST ROW`. A
pattern's rows are consecutive in the partition: `b{3}` is three rows in a row
that satisfy `b`, and a row between them that satisfies no variable ends the
attempt, which then starts again at the next row.

Because `PREV` is NULL on the first row of a partition, start the pattern on an
anchor variable (`PATTERN (start down+ up)`), not on a `PREV`-conditioned one.
Grouped sub-patterns, `PERMUTE`, reluctant quantifiers (`+?`), `PREV` with an
offset, and other `AFTER MATCH SKIP` modes are not supported.

## Views and materialised views

A **view** is a named query, expanded inline wherever it is referenced. It is
session-scoped, not persisted:

```sql
CREATE VIEW recent_checkouts AS
SELECT user_id, url FROM clicks WHERE url LIKE '%checkout%';
```

A **materialised view** maintains a query into a backing table. `freshness`
chooses how it is kept up to date: `'0'` (or `continuous`) keeps a live
maintenance job; a positive duration such as `'5m'` or `'1h'` recomputes on that
interval and atomically overwrites the backing.

```sql
CREATE MATERIALIZED VIEW user_totals
WITH ('freshness'='0', connector='file', format='json', path='/tmp/user_totals.ndjson')
AS SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id;

REFRESH MATERIALIZED VIEW user_totals;   -- interval (full-refresh) views only
```

## AI in SQL

clink treats models and vector search as first-class SQL. See the
[connector reference](connectors/README.md) for provider details.

**Declare a model** with typed inputs and outputs and a provider:

```sql
CREATE MODEL embedder
INPUT (text TEXT)
OUTPUT (vec DOUBLE PRECISION ARRAY)
WITH ('provider'='http', 'endpoint'='http://localhost:8900/embed');
```

Providers are `http` (POST features as a JSON row, read named output columns back; a
NaN or infinite feature is sent as `null`),
`onnx` (a local model file, opt-in build) and in-process closures.

**Score rows** with `ML_PREDICT`, which appends the model's output columns:

```sql
INSERT INTO scored
SELECT * FROM ML_PREDICT(TABLE reviews, MODEL sentiment, DESCRIPTOR(body));
```

**Retrieve by similarity** with `VECTOR_SEARCH`. It takes a query table and column,
a corpus table, the corpus vector column, and `top_k`, and appends a `score`:

```sql
INSERT INTO hits
SELECT * FROM VECTOR_SEARCH(
  TABLE queries, embedding,          -- query rows and their vector column
  documents, DESCRIPTOR(vec),        -- corpus and its vector column
  5,                                 -- top-k
  metric = 'cosine'                  -- or 'l2', 'dot'
);
```

`metric` is `cosine` (default), `l2` or `dot`. `corpus_refresh_ms` re-scans the
corpus on an interval so a growing corpus is picked up without a restart.

`filter_eq` scopes each query to the corpus rows whose columns equal the query's,
as a genuine pre-filter (it scores only the matching subset, so it does not lose
recall the way filtering a top-k afterwards would):

```sql
SELECT * FROM VECTOR_SEARCH(
  TABLE queries, embedding, documents, DESCRIPTOR(vec), 5,
  filter_eq = 'q_system:system'      -- query column : corpus column
);
```

A comma separates multiple bindings. A NULL query value imposes no constraint on
that binding.

## Functions you define

**Scalar UDF.** An expression over the named parameters, in SQL:

```sql
CREATE OR REPLACE FUNCTION with_tax(amount BIGINT) RETURNS BIGINT
AS 'amount + amount / 10' LANGUAGE SQL;
```

The body may use any built-in scalar function, `CASE` and arithmetic. A UDF is
then callable anywhere a built-in is.

**Aggregate UDF (UDAF)** is provided as a WebAssembly module (opt-in build):

```sql
CREATE OR REPLACE AGGREGATE weighted_sum(BIGINT)
(language = 'wasm', module = '/path/wsum.wasm', result_type = 'BIGINT', export = 'wsum');
```

A wasm UDAF works anywhere a built-in aggregate does, including windowed
`GROUP BY`. Remove either with `DROP FUNCTION` / `DROP AGGREGATE`.

## Set operations and ordering

`UNION` (distinct), `UNION ALL`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT` and
`EXCEPT ALL` combine two queries; the branches must match in column count and type.
Columns match by position, and the result takes the first query's column names.

`ORDER BY` requires a `LIMIT` at the top level (an unbounded sort has no meaning on
a stream), and its columns must be in the `SELECT` output. `LIMIT n [OFFSET m]`
bounds the result; `n` and `m` are non-negative integer literals, and `LIMIT 0`
writes nothing.

```sql
SELECT url FROM clicks_a
INTERSECT ALL
SELECT url FROM clicks_b;
```

!!! note "Global LIMIT semantics"
    `LIMIT` and `ORDER BY ... LIMIT` keep a single global stage when job
    parallelism is greater than one, so the bound applies to the query rather
    than to each subtask. A bare `LIMIT` may select different rows when arrival
    order changes. For repeatable row selection, use `ORDER BY` with enough
    columns to define a total order; rows tied on every ordering expression have
    no specified order.

## Managing the catalogue

| Statement | Purpose |
|---|---|
| `ALTER TABLE t ADD/DROP/ALTER COLUMN ...` | evolve a base table's schema |
| `ALTER TABLE t SET/RESET (k='v', ...)` | change `WITH` options |
| `ALTER TABLE t RENAME [COLUMN old] TO new` | rename a table or column |
| `DROP TABLE/VIEW/MATERIALIZED VIEW [IF EXISTS] n` | remove an object (kind must match) |
| `SHOW TABLES` | list registered tables |
| `ANALYZE TABLE t [(cols)]` | collect statistics for the optimiser (bounded sources) |
| `EXPLAIN <SELECT\|INSERT>` | print the optimised plan with row estimates |

`ALTER` applies to base tables only, and refuses to drop the event-time column, a
primary-key column, or the last column.

## What is not supported

Being explicit so you are not surprised at compile time:

- **Schema-qualified names.** Use unqualified names; `schema.table` is rejected in
  queries and its prefix dropped in DDL. The namespace is flat.
- **No wall-clock functions** (`now()` and friends) - drive time from event time.
- **No `INSERT ... VALUES`**, no `CREATE TABLE AS SELECT` / `SELECT INTO` (use a
  materialised view), no `ILIKE`, no `JOIN ... USING` / `NATURAL JOIN`, no
  `WATERMARK FOR ...` clause (use the `WITH` options), no temporal or `BYTEA` casts.
- **Silently accepted, then ignored:** `WITH CHECK OPTION` on a view; the options
  to `EXPLAIN ANALYZE` / `EXPLAIN VERBOSE` (no execution or timing). (Inline
  `PRIMARY KEY` is honoured; every other column constraint is rejected, never
  ignored. `HAVING` without `GROUP BY` is refused.)

## See also

- [SQL frontend internals](internals/sql-frontend.md) - how a query becomes operators.
- [Connectors](connectors/README.md) - per-connector options and formats.
- [Embedded execution](internals/embedded.md) - the `clink run` CLI, capture and checkpointing.
- [Capabilities](capabilities.md) - the full engine feature catalogue.
