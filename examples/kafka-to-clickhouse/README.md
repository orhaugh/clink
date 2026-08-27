# Kafka to clink to ClickHouse

A stream-processing pipeline you could plausibly deploy, running on your
machine in about ten minutes. Sensor readings arrive on a Kafka topic, clink
aggregates them into ten-second event-time windows per sensor, and the results
land in ClickHouse as each window closes. Then you kill the process doing the
work, while data is still arriving, and check what survived.

Nothing is compiled. The services are stock Kafka and ClickHouse images plus
the published clink runtime image, which covers amd64 and arm64.

The full walkthrough, with what each step demonstrates and why, is
[Your first real clink pipeline](https://orhaugh.github.io/clink/tutorials/kafka-to-clickhouse/).
This README is the short form.

## Requirements

Docker with Compose v2 (`docker compose version`) and Python 3.9 or later.
No Kafka or ClickHouse client, and no clink build.

Host ports used: 8081 (clink Coordinator HTTP API) and 8123 (ClickHouse
HTTP). If either collides with something you run, set `CLINK_HTTP_PORT` or
`CLICKHOUSE_HTTP_PORT` and pass `--url` / `--coordinator` to the scripts
accordingly.

## Run it

```bash
docker compose up -d          # Kafka, ClickHouse, a clink Coordinator and
                              # Worker; pipeline.sql is submitted for you
./scripts/produce_events.py   # stream 2,440 readings in, about 50 s
./scripts/verify.py           # check ClickHouse against the expectation
```

Kill the Worker while the producer is still running, then start it again:

```bash
docker compose kill worker    # SIGKILL: no flush, no cleanup
docker compose start worker
docker compose logs coordinator | grep -iE 'lost|restart|restore'
```

Everything, unattended, including the kill and the verification:

```bash
./run.sh                      # about three minutes; cleans up even on failure
KEEP_UP=1 ./run.sh            # leave the stack running afterwards
```

Tear down:

```bash
docker compose down -v        # -v also drops the Kafka topic and the table
```

Kafka keeps what you send it, so running `produce_events.py` twice against
one stack doubles the input and the aggregates with it. `docker compose down -v`
starts clean.

## What is here

| File | What it is |
|---|---|
| `docker-compose.yml` | Kafka, ClickHouse, a clink Coordinator and Worker, and two one-shot containers: topic creation and pipeline submission |
| `pipeline.sql` | The pipeline: a Kafka source, a ClickHouse sink, and one `INSERT INTO ... SELECT` with a tumbling event-time window |
| `clickhouse-init.sql` | Creates `sensor_window_stats` as a `ReplacingMergeTree` keyed by `(sensor_id, window_start)` |
| `scripts/workload.py` | The workload's single definition: eight sensors, deterministic readings, and the expected window aggregates |
| `scripts/produce_events.py` | Streams the readings into Kafka through the broker container's console producer |
| `scripts/verify.py` | Recomputes the expectation from `workload.py` and compares it with ClickHouse |
| `run.sh` | The whole tutorial unattended, including the Worker kill; what CI runs |

## The workload

Eight temperature sensors, one reading per second of event time, five
minutes: 2,440 readings, 30 complete ten-second windows per sensor.

```json
{"sensor_id": "sensor-03", "ts": 1772352000000, "temp_c": 19.1}
```

`ts` is the event time in epoch milliseconds. Every value is a pure function
of the sensor and the tick, computed in integer tenths of a degree, so
`verify.py` can recompute the exact expected aggregates without trusting
anything the engine wrote.

`sensor-03`'s readings reach Kafka two seconds after the others', inside the
pipeline's three-second watermark lag, so they land in the window their
timestamp says they belong to rather than the one they arrived during.

## What the failure and recovery demonstrate

**Keyed state and source position recover on one cut.** After a SIGKILL
mid-stream, every window has its correct reading count, minimum, maximum and
average. The job restarts from the last completed checkpoint: the Kafka
offsets and the contents of every open window come from the same consistent
cut, so the records between that checkpoint and the crash are re-read and
re-aggregated without being lost or double-counted.

**Delivery into ClickHouse is at-least-once, not exactly-once.** Any row
emitted after the last completed checkpoint is emitted again after the
restart, so a window can appear in the table twice with identical values.
`verify.py` counts every window's copies, asserts they agree, and reports
how many were duplicated (often none here: with `batch_rows='1'` a fired
window is in ClickHouse immediately and covered by a checkpoint within two
seconds, so the vulnerable gap is thin). clink's ClickHouse
sink has no two-phase commit - the SQL planner rejects
`delivery_guarantee='exactly_once'` on it - which is why the table is a
`ReplacingMergeTree`: duplicates collapse by key on merge, and
`SELECT ... FINAL` collapses them at read time. The Coordinator says so at
submission time:

```
[coordinator.guarantee] job delivery guarantee:
  STATE_EXACTLY_ONCE_OUTPUT_AT_LEAST_ONCE (limited by sink 'clickhouse_sink')
```

For exactly-once output end to end, use a sink that can commit with the
checkpoint: clink's Kafka sink, its PostgreSQL sink through
`PREPARE TRANSACTION`, or its staged-commit file, Parquet and S3 sinks.

## The state is an open dataset

Ask the running job what it holds:

```bash
docker compose exec -e CLINK_LOG_LEVEL=off coordinator \
  clink state-query --job=1 --coordinator=coordinator:8081 \
  --sql="SELECT slot, COUNT(*) AS entries FROM state GROUP BY slot"
```

Eight entries in the `win` slot: one per sensor, the open window still
accumulating. Or export a checkpoint and read it with no clink involved:

```bash
docker compose exec coordinator sh -c '
  latest=$(ls /state/checkpoints/v1/0 | sed -n "s/checkpoint-\([0-9]*\)\.snap$/\1/p" | sort -n | tail -1)
  clink state-export --dir=/state/checkpoints/v1 --id=$latest \
    --out=/state/state.parquet --format=parquet'
docker compose cp coordinator:/state/state.parquet .
python3 -c "import duckdb; print(duckdb.sql(\"SELECT slot, key_group, decode(user_key) AS sensor FROM 'state.parquet' WHERE slot = 'win' ORDER BY sensor\"))"
```

Snapshots are Arrow IPC with a
[documented layout](https://orhaugh.github.io/clink/internals/state-snapshot-format/),
so pyarrow, DuckDB and Polars all open them directly.

## Running the same file elsewhere

Embedded, one process, no cluster:

```bash
docker compose run --rm --no-deps -v "$PWD/pipeline.sql:/pipeline.sql:ro" \
  coordinator clink run /pipeline.sql --checkpoint-dir=/state/embedded
```

On a real cluster, the same file with two more flags:

```bash
clink run pipeline.sql --coordinator-host <host> --coordinator-port 8081 \
  --checkpoint-dir <shared-path> --checkpoint-interval-ms 2000 --parallelism 4
```

At parallelism above one, give the topic that many partitions: clink assigns
partitions to source subtasks deterministically and hash-partitions the keyed
shuffle, so the aggregation stays correct at any parallelism.

## If something goes wrong

`run.sh` prints container state, service logs, the job's status and the
ClickHouse row counts on any failure. By hand:

```bash
docker compose ps -a                      # what is up, what exited
docker compose logs submit                # did the pipeline submit?
curl -s localhost:8081/api/v1/jobs        # is the job RUNNING?
curl -s localhost:8081/api/v1/jobs/1/operators | python3 -m json.tool | grep -E 'op_type|records_out'
docker compose logs worker | grep -v registry.replace
```

`records_out` per operator localises a stall immediately: zero at the source
means nothing is being read from Kafka, and non-zero everywhere but the sink
means ClickHouse is refusing the inserts.

One known intermittent: on a freshly created stack the Kafka source can
assign its partition and then read nothing at all - every `records_out`
stays 0 with no error anywhere
([issue #8](https://github.com/orhaugh/clink/issues/8), fixed in the tree
after v0.8.0; the published image can still hit it under a loaded host).
`docker compose restart worker` clears it; the job then consumes everything
from the beginning, exactly once into its aggregates, because nothing had
been read yet.
