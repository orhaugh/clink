CREATE TABLE clicks (user_id BIGINT, url TEXT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/clicks.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE imps (user_id BIGINT, ad TEXT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/imps.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE out_t (user_id BIGINT, url TEXT, ad TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT c.user_id, c.url, i.ad
FROM clicks c JOIN imps i
  ON c.user_id = i.user_id
 AND c.ts BETWEEN i.ts - INTERVAL '1' SECOND AND i.ts + INTERVAL '2' SECOND;
