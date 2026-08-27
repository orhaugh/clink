-- Kafka -> ten-second tumbling windows per sensor -> ClickHouse.
--
-- Submitted to the local cluster by the `submit` service in
-- docker-compose.yml. The same file runs embedded in one process with
--   clink run pipeline.sql
-- and on any cluster with
--   clink run pipeline.sql --coordinator-host <host> --coordinator-port <port>

-- Source: one JSON reading per Kafka record, e.g.
--   {"sensor_id": "sensor-03", "ts": 1772352000000, "temp_c": 19.1}
-- `ts` is the event time in epoch milliseconds. Watermarks trail the largest
-- ts seen by 3 s, which covers sensor-03's readings arriving 2 s late.
CREATE TABLE readings (
    sensor_id VARCHAR,
    ts        BIGINT,
    temp_c    DOUBLE
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'readings',
    group_id          = 'clink-tutorial',
    auto_offset_reset = 'earliest',
    event_time_column = 'ts',
    watermark_lag_ms  = '3000'
);

-- Sink: one row per (sensor, window), inserted as JSONEachRow into the
-- ClickHouse table created by clickhouse-init.sql. Delivery to ClickHouse is
-- at-least-once: rows written after the last completed checkpoint are
-- written again after a recovery, which is why that table is a
-- ReplacingMergeTree keyed by (sensor_id, window_start).
--
-- batch_rows='1' inserts each row as soon as the window fires. Eight rows
-- every ten seconds is nothing to batch, and it matters for correctness on
-- clink 0.8.0, whose ClickHouse sink flushes its buffer on a row-count or
-- time trigger rather than at the checkpoint barrier: a row still buffered
-- when the Worker dies is only safe if it had already been inserted. Later
-- engines flush at every barrier and the option becomes a plain tuning knob.
CREATE TABLE sensor_window_stats (
    sensor_id    VARCHAR,
    window_start BIGINT,
    window_end   BIGINT,
    readings     BIGINT,
    avg_temp_c   DOUBLE,
    min_temp_c   DOUBLE,
    max_temp_c   DOUBLE
) WITH (
    connector = 'clickhouse',
    format    = 'json',
    host      = 'clickhouse',
    port      = '9000',
    database  = 'default',
    table     = 'sensor_window_stats',
    user      = 'clink',
    password  = 'clink',
    batch_rows = '1'
);

-- The standing job. A window fires once the watermark passes its end, so the
-- rows arrive as the stream advances, not when the job stops.
INSERT INTO sensor_window_stats
SELECT sensor_id,
       window_start,
       window_end,
       COUNT(*)    AS readings,
       AVG(temp_c) AS avg_temp_c,
       MIN(temp_c) AS min_temp_c,
       MAX(temp_c) AS max_temp_c
FROM readings
GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), sensor_id;
