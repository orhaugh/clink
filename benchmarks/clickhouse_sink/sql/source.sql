-- The benchmark dataset (rows.sql), read from the Parquet parts run.sh writes
-- with clickhouse-local. The source reads a directory by subtask: part i goes
-- to subtask i % parallelism, so every subtask reads its own disjoint rows.
-- ts arrives as epoch-millisecond text, which a TIMESTAMP(3) column carries
-- through the row batcher as written; lc arrives as a plain String. Each file
-- also carries a null event_time column: the Parquet row source reads a row's
-- event time from it and refuses a file without one.
CREATE TABLE rows_in (
    k      BIGINT,
    a      BIGINT,
    b      BIGINT,
    i      INT,
    f1     DOUBLE,
    f2     DOUBLE,
    ts     TIMESTAMP(3),
    d      DECIMAL(18, 4),
    s_low  VARCHAR,
    s_mid  VARCHAR,
    s_high VARCHAR,
    lc     VARCHAR
) WITH (
    connector = 'parquet',
    path      = '{data_dir}'
);

