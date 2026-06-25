-- Nexmark q12 (per-bidder bid count per 10s tumbling window) on Flink SQL,
-- BLACKHOLE sink variant. Discards output via the built-in blackhole connector so
-- the windowed-agg rate is measured without the Kafka output sink as a ceiling.
CREATE TABLE bid (
  auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `datetime` BIGINT,
  ts AS TO_TIMESTAMP_LTZ(`datetime`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '4' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-bid',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q12bh',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);
CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT) WITH ('connector' = 'blackhole');
INSERT INTO sink_q12
SELECT bidder, COUNT(*) AS bid_count
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, bidder
