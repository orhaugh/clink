-- clink q0 (stateless pass-through), reads shared nx-bid, writes nx-out-q0-clink.
-- Brokers is the host-mapped port (clink runs on the host); the Flink side uses
-- the in-network listener kafka:29092. Submitted via:
--   clink_submit_sql --file q0.sql --jm-host 127.0.0.1 --jm-port 8081
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-bid',
        group_id='clink-q0', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-out-q0-clink');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
