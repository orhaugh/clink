# PostgreSQL to clink to ClickHouse

clink captures a PostgreSQL table, its existing rows and then every committed
insert, update and delete, into ClickHouse, which keeps the table's current
state. A workload changes the table the whole time, and you kill the process
doing the capture in the middle of it. At the end the ClickHouse copy is
compared with PostgreSQL, value for value.

The services are stock PostgreSQL and ClickHouse images and the clink runtime
image. Nothing is compiled.

## Requirements

Docker with Compose v2 (`docker compose version`) and Python 3.9 or later. No
PostgreSQL or ClickHouse client on the host.

Host ports used: 8081 (clink Coordinator HTTP API), 8123 (ClickHouse HTTP) and
5432 (PostgreSQL). Set `CLINK_HTTP_PORT`, `CLICKHOUSE_HTTP_PORT` or
`POSTGRES_PORT` if one collides with something you run; the scripts follow the
first two.

This needs a clink image with the PostgreSQL CDC contract (`changelog_mode`),
which the published image gains at the next release. Until then, point
`CLINK_IMAGE` at an image built from this repository
(`docker build -t clink-runtime:local -f docker/Dockerfile.runtime .`).

## What runs

- `postgres` starts with `wal_level=logical` and, from `postgres-init.sql`, an
  `orders` table of 20,000 rows with `REPLICA IDENTITY FULL`, a publication
  `clink_orders` of its inserts, updates and deletes, a `clink_cdc` role that
  may replicate and read the table, and a `workload` procedure.
- `clickhouse` creates, from `clickhouse-init.sql`, the change log
  `orders_changes` (one row per change), the current-state table `orders`, a
  `ReplacingMergeTree(__version, __deleted)`, and a materialised view feeding
  the second from the first.
- `coordinator` and `worker` are a clink cluster; `submit` submits
  `pipeline.sql` with a checkpoint every two seconds.

`pipeline.sql` declares the PostgreSQL table with `mode='cdc'` and
`changelog_mode='append'`: clink creates the replication slot `clink_orders`,
emits the 20,000 rows as a snapshot, then streams every committed change from
the slot. Each change is a row carrying `__op`, `__version` (its WAL position,
which increases with every change to a key; 0 for a snapshot row) and
`__deleted`. The ClickHouse table keeps the row with the highest `__version`
per key and drops a key whose latest change is a delete. Capturing a column
added later needs `keep_rows='true'`, and the rows its backfill reads carry a
`__version` one above the row they refresh, so the same tables and queries
keep the latest row
([Adding a column](https://orhaugh.github.io/clink/connectors/postgres/#adding-a-column)).

## Run it

```bash
docker compose up -d          # the stack; pipeline.sql is submitted for you
./scripts/workload.sh 60      # a minute of inserts, updates and deletes
./scripts/verify.py           # compare ClickHouse with PostgreSQL
```

Kill the Worker while the workload is still running, then start it again:

```bash
docker compose kill worker    # SIGKILL: no flush, no cleanup
docker compose start worker
docker compose logs worker | grep 'source.postgres_cdc'
```

The restarted job resumes from the last completed checkpoint. The changes
emitted after it are written to ClickHouse a second time, identically, and the
`__version` absorbs them; none is missed, because the slot is confirmed only
up to a completed checkpoint.

Everything, unattended, including the kill, the verification and the cleanup:

```bash
./run.sh                      # about three minutes; cleans up even on failure
KEEP_UP=1 ./run.sh            # leave the stack running afterwards
```

## Read the current state

```bash
curl -u clink:clink 'http://localhost:8123/' --data-binary \
  "SELECT status, count(), sum(amount) FROM orders FINAL GROUP BY status ORDER BY status"
```

Without the materialised view, the same state comes from the change log:

```sql
SELECT id,
       argMax(status, __version) AS status,
       argMax(amount, __version) AS amount
FROM orders_changes
GROUP BY id
HAVING argMax(__deleted, __version) = 0;
```

Both rely on one row per key at its highest version: of two different rows
under one version, `argMax` returns either. The repeats an at-least-once
delivery writes are identical, and clink gives every later row of a key a
higher version, so this finds no key:

```sql
SELECT id, __version
FROM orders_changes
WHERE (id, __version) IN (SELECT id, max(__version) FROM orders_changes GROUP BY id)
GROUP BY id, __version
HAVING uniqExact((customer, status, amount, updated_at, rev, __deleted)) > 1;
```

## How it is checked

`scripts/verify.py` reads nothing clink reports. It commits a marker row to
PostgreSQL and waits until the marker reaches the change log through the
stream, so every change committed before it has been delivered. It then
compares every row of the PostgreSQL table, every column, with `orders FINAL`
and with the rebuild from `orders_changes`, and counts the change-log rows
written twice after the restart. `./run.sh --self-test` checks that the
comparison fails on a missing row, an extra row and a stale value.

## Clean up

```bash
./scripts/cleanup.sh
```

A replication slot outlives the job that reads it, and holds WAL until it is
dropped. Cancelling the job does not drop it, since a restarted job resumes
from it, so retiring a job for good is two steps, and the script does both
before removing the stack:

```bash
curl -X POST http://localhost:8081/api/v1/jobs/1/cancel
docker compose exec postgres psql -U postgres -d shop \
  -c "SELECT pg_drop_replication_slot('clink_orders')"
```

In a deployment, watch `pg_replication_slots` (`active`,
`confirmed_flush_lsn`, `wal_status`) and keep `max_slot_wal_keep_size` set, as
this example does: a slot that is never read again otherwise keeps WAL
forever. The connector page,
[PostgreSQL](https://orhaugh.github.io/clink/connectors/postgres/), has the
whole contract: requirements, types, recovery and delivery semantics.
