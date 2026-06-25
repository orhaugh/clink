-- Nexmark q0 (stateless pass-through) on Flink SQL, BLACKHOLE sink variant. Reads
-- shared nx-bid (kafka:29092), discards output via Flink's built-in blackhole
-- connector so the read+process rate is measured without the Kafka output sink as
-- a ceiling (sampled via Flink's own records counter, not an output topic).
CREATE TABLE bid (
  auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `datetime` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'nx-bid',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q0bh',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);
CREATE TABLE sink_q0 (
  auction BIGINT, bidder BIGINT, price BIGINT, `datetime` BIGINT
) WITH ('connector' = 'blackhole');
INSERT INTO sink_q0 SELECT auction, bidder, price, `datetime` FROM bid
