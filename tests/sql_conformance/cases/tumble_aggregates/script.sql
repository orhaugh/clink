CREATE TABLE events (k TEXT, v BIGINT, label TEXT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/events.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE out_t (k TEXT, n BIGINT, nv BIGINT, total BIGINT, mean DOUBLE PRECISION, lo BIGINT, hi BIGINT,
                    joined TEXT, vals ARRAY<BIGINT>, uniques BIGINT, ws BIGINT, we BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT k, COUNT(*) AS n, COUNT(v) AS nv, SUM(v) AS total, AVG(v) AS mean, MIN(v) AS lo, MAX(v) AS hi,
       STRING_AGG(label, '|') AS joined, ARRAY_AGG(v) AS vals, COUNT(DISTINCT v) AS uniques,
       window_start AS ws, window_end AS we
FROM events
GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), k;
