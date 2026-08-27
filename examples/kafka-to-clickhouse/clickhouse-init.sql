-- Runs once, when the ClickHouse container first starts.
--
-- ReplacingMergeTree keyed by (sensor_id, window_start): clink delivers to
-- ClickHouse at least once, so after a recovery the rows emitted since the
-- last completed checkpoint are inserted a second time, with identical
-- values. ClickHouse collapses such duplicates by key when it merges parts,
-- and `SELECT ... FROM sensor_window_stats FINAL` collapses them at read time.
-- inserted_at is not something clink writes; it records when each row
-- arrived, which is how a re-inserted row is told apart from the original.
CREATE TABLE IF NOT EXISTS sensor_window_stats
(
    sensor_id       String,
    window_start    Int64,                 -- epoch milliseconds, as clink emits it
    window_end      Int64,
    readings        UInt32,
    avg_temp_c      Float64,
    min_temp_c      Float64,
    max_temp_c      Float64,
    inserted_at     DateTime64(3) DEFAULT now64(3),
    window_start_ts DateTime64(3) MATERIALIZED fromUnixTimestamp64Milli(window_start)
)
ENGINE = ReplacingMergeTree
ORDER BY (sensor_id, window_start);
