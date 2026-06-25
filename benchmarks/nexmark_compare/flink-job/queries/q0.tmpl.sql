-- Nexmark q0 (stateless pass-through) on Flink SQL. Reads shared nx-bid (in-network
-- listener kafka:29092), writes __OUT__ (run.sh substitutes the output topic).
CREATE TABLE bid (
  auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `datetime` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-bid',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q0',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TABLE sink_q0 (
  auction BIGINT, bidder BIGINT, price BIGINT, `datetime` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '__OUT__',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO sink_q0 SELECT auction, bidder, price, `datetime` FROM bid
