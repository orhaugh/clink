-- Nexmark q12 (per-bidder bid count per 10s tumbling window) on Flink SQL.
-- Event-time analogue of Nexmark's proctime q12, matched to clink's
-- GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder. Uses the Flink 2.x
-- windowing TVF (grouped-window GROUP BY TUMBLE was removed). Reads shared
-- nx-bid, writes nx-out-q12-flink.
CREATE TABLE bid (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel STRING,
  url STRING,
  `datetime` BIGINT,
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
  bidder BIGINT,
  bid_count BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-out-q12-flink',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO sink_q12
SELECT bidder, COUNT(*) AS bid_count
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, bidder
