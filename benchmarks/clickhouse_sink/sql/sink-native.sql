-- The native sink: Native blocks over the TCP protocol, LZ4-compressed.
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
    insert_format     = 'native',
    host              = '{ch_host}',
    port              = '{ch_port}',
    database          = '{ch_database}',
    table             = '{ch_table}',
    batch_rows        = '{batch_rows}',
    batch_bytes       = '{batch_bytes}',
    batch_interval_ms = '{batch_interval_ms}',
    compression       = 'lz4'
);

