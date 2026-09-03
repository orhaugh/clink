CREATE TABLE events (k TEXT, v BIGINT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/events.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE out_t (k TEXT, n BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT k, COUNT(*) AS n FROM events GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k HAVING COUNT(*) > 2;
