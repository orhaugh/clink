-- clink q0 (stateless pass-through). Reads shared nx-bid, writes __OUT__.
-- run.sh substitutes __OUT__ with the per-engine output topic.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-bid',
        group_id='clink-q0', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='__OUT__');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
