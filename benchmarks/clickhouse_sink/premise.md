# B1 premise: the native ClickHouse sink, row form

This is the contract every B1 figure is taken under, written before any
measured run. `run.sh` records the machine-readable half of it (versions,
settings, hosts, knobs) in each campaign's `premise.json`; a figure without
that file beside it is not a B1 figure.

## What B1 measures

The native sink (`insert_format='native'`) at parallelism 8, taking rows as
the engine delivers them today: the sink does not yet ingest columnar
batches, so every batch reaches it as rows. The targets come from the sink
plan (section 3.14) and are proposals until D2 accepts or changes them:

| Figure | Target |
|---|---|
| Native rate | at least 500,000 rows/s |
| Native rate over the legacy JSONEachRow sink's | at least 3x |
| Native sink-hop CPU | at most 4 core-seconds per 10^6 rows |

A campaign run before D2 is accepted (`D2_ACCEPTED=1`) is a sizing or smoke
run, and its summary says so.

## Calibration first

`calibrate.sh` measures the server's own insert ceiling from the client host
at the sink's batch size. A target above 70% of that ceiling is restated
before clink is measured, never after.

## The rig

Separate clink and ClickHouse hosts on a private network. Each result
records the clink host's and the server host's cores, memory, architecture
and kernel as Docker reports them; `RIG` adds what Docker cannot see: the
machine types and the network link between the hosts. A run with both on
one machine is marked `local_smoke_run` and checks the harness only.

## Versions

- ClickHouse 26.8, pinned by digest in `compose.yml`; the server's
  `version()` is recorded.
- clickhouse-cpp 2.6.2, built Release by `scripts/build-clickhouse-cpp.sh`
  whatever the toolchain's build type; the version comes from the image's
  build stamp. No figure is taken against a Debug client.
- clink: the runtime image named by `CLINK_IMAGE`, with its image id and its
  capability record, which carries the commit.

## The table and its settings

`table.sql`: twelve columns, MergeTree, `PARTITION BY toYYYYMMDD(ts) ORDER BY
(k, ts)`, every row in one day so one partition is active. The server runs
its defaults, with `part_log` on; `async_insert` is 0, which the native sink
checks at open. The table DDL as the server reports it, the fsync and part
thresholds from `system.merge_tree_settings`, and the insert settings from
`system.settings` are recorded per campaign.

## The workload

- **Dataset.** `rows.sql`: three Int64, an Int32, two Float64, a
  DateTime64(3), a Decimal(18,4), three Strings of low, medium and high
  cardinality, and a LowCardinality(String). The mean uncompressed row size
  is recorded from `system.parts` after each sink run.
- **Source.** A directory of Parquet files written once per size by
  clickhouse-local from `rows.sql`, `FILES_PER_SUBTASK` per subtask; file i
  is read by subtask i % 8, so the subtasks read disjoint rows of equal
  count. SQL has no generator that splits by subtask, which is why the plan's
  "generator" is a file source here. `ts` is carried as epoch-millisecond text
  and `lc` as a plain String, the same in every cell, and each file carries a
  null `event_time` column, which clink's Parquet row source requires.
- **Pipeline.** `INSERT INTO rows_out SELECT <the twelve columns> FROM
  rows_in`, parallelism 8, aligned checkpoints every 10 s into a volume the
  Coordinator and the Worker share. One Coordinator and one Worker with 64
  slots.
- **Native sink.** Its defaults, stated in the SQL: `batch_rows` 1,048,449,
  `batch_bytes` 64 MiB, `batch_interval_ms` 1000, LZ4.
- **Legacy sink.** JSONEachRow, with the native run's `batch_interval_ms`
  and, for `batch_rows`, the native cell's measured mean rows per INSERT in
  the same campaign (the median over its trials), not the legacy default of
  1000, which would move the ratio by an order of magnitude.
  `LEGACY_BATCH_ROWS` fixes it instead, and the premise records which.
- **Blackhole.** The same pipeline into `connector='blackhole'`. It sees the
  same row batches as the sinks, since its only producer is the row-form
  column binding every row sink has in front of it.

## Measurement

- **Trials.** Three per cell, each on a freshly composed stack (ClickHouse,
  Coordinator and Worker), the native cell first. The cell's figure is the
  median of the trials that passed the gate.
- **Sink rate.** Rows landed between the first and the last acknowledged
  INSERT of the steady state, from `system.query_log`. The steady state
  starts `WARMUP_S` after the first acknowledged INSERT and ends `COOLDOWN_S`
  before the last, and must span at least 600 s; a shorter one does not count
  toward B1.
- **Blackhole rate.** The blackhole sends no INSERT, so its rate is the rows
  reaching the sink operator, from clink's operator counters, over the same
  kind of window. It is reported for B2's ratio to the blackhole; B1 uses
  the blackhole cell only for CPU.
- **CPU.** cgroup v2 `usage_usec` of the Coordinator and the Worker
  containers, read before the submit and after the job ends, so the whole
  run is counted. Each cell's CPU is normalised per 10^6 rows first; a
  sink's hop is its median minus the blackhole cell's median. The server's
  cgroup CPU per 10^6 rows is reported beside it, and the CPU the INSERT
  queries themselves used, from `query_log`.

## Correctness gate

Every sink run is held to the dataset, not to anything the pipeline reports.
`verify.py` checks that the table holds exactly the produced number of rows,
every `k` from 0 to N-1 exactly once, and the same per-column and whole-row
checksums as `rows.sql` evaluated over the same range by clickhouse-local.
Duplicates (rows landed minus distinct `k`) are reported and must be 0. A
blackhole run must deliver every row to the sink operator and end
`COMPLETED_OK`. A trial that fails its gate has no figure, and the campaign
exits non-zero.

## What is not claimed

Only figures measured under this premise, with its `premise.json`. No
cross-engine or cross-client ratios: the legacy comparison is between two
clink sinks, under the same pipeline and premise.
