CREATE TABLE events (k TEXT, v BIGINT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/events.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE out_t (k TEXT, total BIGINT, ws BIGINT, we BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT k, SUM(v) AS total, window_start AS ws, window_end AS we
FROM events GROUP BY HOP(ts, 2000, 1000), k;
