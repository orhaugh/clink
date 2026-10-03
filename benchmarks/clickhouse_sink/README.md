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

## B1: the native sink against the legacy sink and a blackhole

Read `premise.md` first: it says what each figure means and when a campaign
counts as a B1 measurement. Then:

```bash
./run.sh                                          # local smoke run: three cells, three trials
TRIALS=1 ROWS=8000000 CELLS="native" ./run.sh     # a quicker check
```

On a rig, from the clink host, with the repository checked out at the same
path on the server host:

```bash
PREPARE_ONLY=1 ROWS=360000000 ./run.sh            # write the input once, before the timed runs
CLICKHOUSE_DOCKER_HOST=ssh://root@<server> CLICKHOUSE_HOST=<server private ip> \
  CLINK_IMAGE=ghcr.io/orhaugh/clink-runtime:sha-<12> \
  ROWS=360000000 WARMUP_S=60 COOLDOWN_S=30 RIG="<machine types, network link>" ./run.sh
```

Each cell runs on a freshly composed stack (ClickHouse, a Coordinator and a
Worker from the runtime image), in the order native, blackhole, legacy, since
the legacy sink takes its `batch_rows` from the native cell's measured mean
rows per INSERT. The input is a directory of Parquet files that
clickhouse-local writes from `rows.sql`, one disjoint share per subtask, about
100 bytes a row on disk; it is written once per size and kept in `data/`. The
rows are sized to the steady state the premise asks for: at the B1 target of
500,000 rows/s, ten minutes is 300 million rows before warm-up and cool-down.

A campaign writes `results/<campaign>/`: `premise.json`, one directory per
trial (the rendered `pipeline.sql`, the job's counter samples, the INSERTs
from `query_log`, the gate report, the Worker log and `trial.json`), and
`summary.json`, which also says whether the campaign stands as a B1
measurement and, if not, why. The script exits non-zero if any trial fails
its gate.

Knobs: `CELLS`, `TRIALS`, `ROWS`, `PARALLELISM`, `FILES_PER_SUBTASK`,
`CHECKPOINT_INTERVAL_MS`, `BATCH_ROWS`, `BATCH_BYTES`, `BATCH_INTERVAL_MS`,
`LEGACY_BATCH_ROWS`, `WARMUP_S`, `COOLDOWN_S`, `MIN_WINDOW_S`, `D2_ACCEPTED`,
`CLINK_IMAGE`, `CLINK_HTTP_PORT`, `KEEP_UP` (debugging only, never for a
measured run).

| File | Role |
|---|---|
| `premise.md` | The written premise B1 figures are taken under |
| `run.sh` | Writes the input, runs the cells, gates and records each trial |
| `verify.py` | The correctness gate: the landed table against `rows.sql` |
| `measure.py` | Counter sampling, the steady-state rates, CPU per row, the summary |
| `clink.yml` | The Coordinator, the Worker and the one-shot submit container |
| `sql/` | The source, one sink per cell, and the INSERT |
| `calibrate.sh` | The server's own ceiling, measured first |
| `compose.yml`, `table.sql`, `rows.sql`, `part-log.xml` | The server and the dataset, shared with calibration |
