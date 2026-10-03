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
./calibrate.sh                                       # local smoke run
CLICKHOUSE_DOCKER_HOST=ssh://root@<server> CLICKHOUSE_HOST=<server private ip> \
  ROWS=10000000 ./calibrate.sh                       # a rig, from the clink host
```

`clickhouse-client` sends the dataset as Native, LZ4-compressed, from
`PARALLELISM` clients at once (default 8), each as a series of INSERTs of
`INSERT_ROWS` rows, the shape the sink sends: one INSERT per subtask per batch
interval. The default, 62,500, is the B1 rate target's shape (500,000 rows/s
from 8 subtasks); after a campaign, `INSERT_ROWS=<the native cell's mean rows
per INSERT>` calibrates at what the sink actually sent. One large INSERT would
let the server form parts of a million rows and overstate the ceiling. On a rig
the server runs on the server host's Docker, the clients on this one, so the
figure includes the network between them. The input is generated with
`clickhouse-local` before the timed window, and the run fails unless every row
landed. A sink target above 70% of this ceiling is restated before clink is
measured, never after; `run.sh` takes the result as `CALIBRATION` and holds the
campaign to that rule.

A run whose clients and server share one Docker daemon checks the harness and
is marked `local_smoke_run` in its result; it is not a premise.

## B1: the native sink against the legacy sink and a blackhole

Read `premise.md` first: it says what each figure means and when a campaign
counts as a B1 measurement. Then:

```bash
./run.sh                                          # local smoke run: three cells, three trials
TRIALS=1 ROWS=8000000 CELLS="native" ./run.sh     # a quicker check
```

On a rig, from the clink host, with the repository checked out at the same
path on the server host, calibrate first, then size the input from the
ceiling. No sink lands rows faster than the server takes them, so ten minutes of
steady state at the ceiling, plus warm-up and cool-down, with half again in
hand, guarantees the window; `run.sh` refuses a rig campaign below that size:

```bash
# ROWS >= (600 + WARMUP_S + COOLDOWN_S) x ceiling x 1.5; at a ceiling of 1.2M rows/s, 1.25 billion
PREPARE_ONLY=1 ROWS=1250000000 ./run.sh           # write the input once, before the timed runs
CLICKHOUSE_DOCKER_HOST=ssh://root@<server> CLICKHOUSE_HOST=<server private ip> \
  CALIBRATION=results/calibration-<utc>.json CLINK_IMAGE=ghcr.io/orhaugh/clink-runtime:sha-<12> \
  ROWS=1250000000 WARMUP_S=60 COOLDOWN_S=30 RIG="<machine types, network link>" D2_ACCEPTED=1 ./run.sh
```

Each cell runs on a freshly composed stack (ClickHouse, a Coordinator and a
Worker from the runtime image), in the order native, blackhole, legacy, since
the legacy sink takes its `batch_rows` from the native cell's measured mean
rows per INSERT. The input is a directory of Parquet files that
clickhouse-local writes from `rows.sql`, one disjoint share per subtask, about
85 bytes a row on disk (a billion rows is about 80 GiB); it is written once per
size and kept in `data/`, and both hosts' free space is checked first.

A campaign writes `results/<campaign>/`: `premise.json`, one directory per
trial (the rendered `pipeline.sql`, the job's counter samples, the INSERTs
from `query_log`, the gate report, the Worker log and `trial.json`), and
`summary.json`, which also says whether the campaign stands as a B1
measurement and, if not, why: every rule of `premise.md` the campaign broke,
from a same-machine run or a missing calibration to an image that is not the
commit under test (`UNDER_TEST`, default the harness's own HEAD). A campaign
that stops part-way still gets a summary of the trials it ran. A campaign name
is used once; `RESUME=1` adds the missing trials to an existing one. The script
exits non-zero if any trial fails its gate.

Knobs: `CELLS`, `TRIALS`, `ROWS`, `PARALLELISM`, `FILES_PER_SUBTASK`,
`CHECKPOINT_INTERVAL_MS`, `BATCH_ROWS`, `BATCH_BYTES`, `BATCH_INTERVAL_MS`,
`LEGACY_BATCH_ROWS`, `WARMUP_S`, `COOLDOWN_S`, `MAX_RUNTIME_S`, `CALIBRATION`,
`UNDER_TEST`, `D2_ACCEPTED`, `RIG`, `CAMPAIGN`, `RESUME`, `CLINK_IMAGE`,
`CLINK_HTTP_PORT`, `KEEP_UP` (debugging only, never for a measured run).

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
