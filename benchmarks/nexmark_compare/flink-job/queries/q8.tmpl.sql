-- Nexmark q8 (new users) on Flink SQL: persons + auctions created in the same 10s
-- tumbling window, joined on person.id = auction.seller. Flink 2.x windowing TVF
-- per side, then a regular join on the key + window bounds (the equivalent of
-- clink's window-equality residual). Two insert-only window aggregates -> an
-- insert-only join -> append Kafka sink. Reads nx-person + nx-auction
-- (kafka:29092), writes __OUT__. Watermark lag 4s matches clink.
CREATE TABLE person (
  id BIGINT, name STRING, `datetime` BIGINT,
  ts AS TO_TIMESTAMP_LTZ(`datetime`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '4' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-person',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q8-p',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TABLE auction (
  seller BIGINT, `datetime` BIGINT,
  ts AS TO_TIMESTAMP_LTZ(`datetime`, 3),
  WATERMARK FOR ts AS ts - INTERVAL '4' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-auction',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q8-a',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TABLE sink_q8 (
  id BIGINT, name STRING, starttime BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '__OUT__',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO sink_q8
SELECT P.id, P.name, CAST(UNIX_TIMESTAMP(CAST(P.ws AS STRING)) * 1000 AS BIGINT) AS starttime
FROM (
  SELECT id, name, window_start AS ws, window_end AS we
  FROM TABLE(TUMBLE(TABLE person, DESCRIPTOR(ts), INTERVAL '10' SECOND))
  GROUP BY id, name, window_start, window_end
) P
JOIN (
  SELECT seller, window_start AS ws, window_end AS we
  FROM TABLE(TUMBLE(TABLE auction, DESCRIPTOR(ts), INTERVAL '10' SECOND))
  GROUP BY seller, window_start, window_end
) A
ON P.id = A.seller AND P.ws = A.ws AND P.we = A.we
