CREATE TABLE ticks (symbol TEXT, price BIGINT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/ticks.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE out_t (symbol TEXT, start_price BIGINT, bottom BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT symbol, start_price, bottom FROM ticks MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY ts
  MEASURES FIRST(down.price) AS start_price, LAST(down.price) AS bottom
  PATTERN (start down+ up)
  DEFINE down AS price < PREV(price),
         up   AS price > PREV(price)
);
