-- The same pipeline with its output discarded: what the source, the
-- projection and the engine cost without a ClickHouse sink. Native and legacy
-- sink-hop CPU are each measured against it.
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
    connector = 'blackhole'
);

