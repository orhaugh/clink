-- The legacy sink: JSONEachRow text. Its batch_rows is the native run's
-- measured mean rows per INSERT and its interval the native run's (premise.md).
CREATE TABLE rows_out (
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
    connector         = 'clickhouse',
    host              = '{ch_host}',
    port              = '{ch_port}',
    database          = '{ch_database}',
    table             = '{ch_table}',
    batch_rows        = '{batch_rows}',
    batch_interval_ms = '{batch_interval_ms}'
);

