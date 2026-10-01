-- The benchmark dataset (docs/clickhouse-sink-plan.md 3.14): twelve columns,
-- one active partition. {db} and {table} are substituted by the scripts.
CREATE TABLE {db}.{table}
(
    k      Int64,
    a      Int64,
    b      Int64,
    i      Int32,
    f1     Float64,
    f2     Float64,
    ts     DateTime64(3),
    d      Decimal(18, 4),
    s_low  String,
    s_mid  String,
    s_high String,
    lc     LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (k, ts)
