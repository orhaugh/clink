-- Flink q0 latency variant: the SAME stateless pass-through relation as q0,
-- reading the PACED __IN__ topic (in-network listener kafka:29092) and writing
-- __OUT__ with producer linger pinned to 0 (the Java default, set explicitly
-- per the latency premise). latency.sh substitutes __IN__ and __OUT__.
CREATE TABLE bid (
  auction BIGINT, bidder BIGINT, price BIGINT, channel STRING, url STRING, `datetime` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '__IN__',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-q0lat',
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
  'properties.linger.ms' = '0',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO sink_q0 SELECT auction, bidder, price, `datetime` FROM bid
