-- Runs once, when the ClickHouse container first starts.

-- The change log clink writes: one row per change, a snapshot row, an insert,
-- an update or a delete, the deleted row's values included. __version is the
-- change's WAL position (0 for a snapshot row), which increases with every
-- change to a key. With keep_rows='true' a row that would not outrank the
-- key's previous one, such as a row a backfill reads after a column is added,
-- carries a higher number instead, so a later row of a key always has the
-- higher version. clink delivers to ClickHouse at least once, so after a
-- recovery some changes are here twice, identically.
CREATE TABLE IF NOT EXISTS orders_changes
(
    id         Int64,
    customer   String,
    status     String,
    amount     Decimal(12, 2),
    updated_at DateTime64(3, 'UTC'),
    rev        Int32,
    __op       LowCardinality(String),
    __version  UInt64,
    __deleted  UInt8
)
ENGINE = MergeTree
ORDER BY (id, __version)
SETTINGS async_insert = 0;

-- The table's current state: the row with the highest __version per key, a
-- key whose latest change is a delete gone. Merges collapse versions in the
-- background; SELECT ... FINAL gives the current state whatever has merged.
CREATE TABLE IF NOT EXISTS orders
(
    id         Int64,
    customer   String,
    status     String,
    amount     Decimal(12, 2),
    updated_at DateTime64(3, 'UTC'),
    rev        Int32,
    __op       LowCardinality(String),
    __version  UInt64,
    __deleted  UInt8
)
ENGINE = ReplacingMergeTree(__version, __deleted)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS orders_mv TO orders AS
SELECT * FROM orders_changes;
