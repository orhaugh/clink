# ClickHouse sink benchmarks

The harness for the native ClickHouse sink's throughput figures. Every figure
is taken under a written premise and carries it: the rig (machine types,
cores, memory, the network between separate clink and ClickHouse hosts), the
server image digest, the clickhouse-cpp version and build type (Release; no
figure is taken against a Debug client), the table and its effective settings,
and the workload (compression, batch rows and bytes, parallelism, checkpoint
interval, source). Results are gitignored.

## The dataset

`table.sql` and `rows.sql`: twelve columns (three Int64, an Int32, two Float64,
a DateTime64(3), a Decimal(18,4), three Strings of low, medium and high
cardinality and a LowCardinality(String)), `PARTITION BY toYYYYMMDD(ts)
ORDER BY (k, ts)`, all rows in one day so one partition is active.

## Calibration: the server's own ceiling

```bash
./calibrate.sh                                         # local smoke run
CLICKHOUSE_HOST=<server> ROWS=10000000 ./calibrate.sh  # on a rig, from the client host
```

`clickhouse-client` sends the dataset as Native, LZ4-compressed, at the sink's
batch size (`BATCH_ROWS`, default 1,048,449) from `PARALLELISM` clients at once
(default 8). The input files are generated with `clickhouse-local` before the
timed window, and the run fails unless every row landed. A sink target above
70% of this ceiling is restated before clink is measured, never after.

A run on one machine, with the server and the client sharing it, checks the
harness and is marked `local_smoke_run` in its result; it is not a premise.
