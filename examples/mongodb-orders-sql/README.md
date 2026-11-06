# MongoDB orders through clink SQL

Orders in a MongoDB collection, read from its change stream by a clink SQL
pipeline and written back to MongoDB as two results:

- `shop.customer_totals`: each customer's orders placed and their exact
  decimal total, replaced by key as orders arrive (`mode='upsert'`);
- `shop.order_log`: every insert, update and delete, appended as it happened.

`run.sh` starts a single-member replica set, a Coordinator and a Worker,
submits [`pipeline.sql`](pipeline.sql), places 300 orders, updates 30 and
deletes 12 ([`scripts/load.js`](scripts/load.js)), and then checks the output
([`scripts/verify.js`](scripts/verify.js)). The workload records what it did in
`shop.orders_placed`, which clink never reads, and the check compares every
customer total and every logged change with that record, reading MongoDB
directly. Decimals are compared by value on the server, and their BSON type is
checked.

```bash
CLINK_BIN_DIR=../../build ./run.sh   # clink from a local build (clink_node and clink)
./run.sh                             # clink from CLINK_IMAGE, in Docker
CLEANUP=1 ./run.sh                   # either, removing everything at the end
```

A passing run ends with:

```text
PASS: 6 customer totals exact; 342 changes logged exactly once
```

The image route needs a runtime image built after the MongoDB SQL connector was
added; until a release carries it, use a local build.

## What the pipeline shows

- **Typed rows from BSON.** `order_changes` declares the fields it reads. Each
  value is converted strictly: the decimal amounts arrive as exact
  `DECIMAL(10, 2)` values and the dates as `TIMESTAMP(3)`, and a value its column
  cannot hold (a double in a `DECIMAL` column, a string in a `BIGINT` one) stops
  the job with an error naming the document, the field and the column.
- **An append-only change stream.** Each change is one row, its kind in `__op`
  and the document's `_id` in `__id`. The source does not ask for an update's
  before-image, so there is no retraction to apply: the totals sum the insert events, which no
  later change alters.
- **Typed BSON out.** The sinks write each column as the BSON type its SQL type
  maps to: `DECIMAL` as decimal128 with its digits, `TIMESTAMP` as a date,
  `BIGINT` as a 64-bit integer.
- **Credentials kept out of the SQL.** The tables' `uri` is `env://MONGODB_URI`,
  read from each Worker's environment, so the connection string is never part
  of the submitted job.

The contracts behind this, including what a restart replays and what the
upsert sink's guarantee depends on, are on the connector page,
[docs/connectors/mongodb.md](../../docs/connectors/mongodb.md).
