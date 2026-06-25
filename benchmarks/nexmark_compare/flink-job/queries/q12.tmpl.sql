-- Nexmark q12 (per-bidder bid count per 10s tumbling window) on Flink SQL, the
-- event-time analogue matched to clink's GROUP BY TUMBLE(datetime, ...). Flink 2.x
-- windowing TVF. Reads shared nx-bid (kafka:29092), writes __OUT__ (run.sh
-- substitutes the output topic). Watermark lag 4s matches clink's watermark_lag_ms.
CREATE TABLE bid (
  auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `datetime` BIGINT,
  ts AS TO_TIMESTAMP_LTZ(`datetime`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '4' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-bid',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q12',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TABLE sink_q12 (
  bidder BIGINT, bid_count BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '__OUT__',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO sink_q12
SELECT bidder, COUNT(*) AS bid_count
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, bidder
