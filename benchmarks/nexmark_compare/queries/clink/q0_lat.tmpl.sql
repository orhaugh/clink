-- clink q0 latency variant: the SAME stateless pass-through relation as q0,
-- reading the PACED __IN__ topic instead of the preloaded nx-bid, and writing
-- __OUT__ with producer linger pinned to 0 so the number measures the engine,
-- not batching (pipeline.md, "Latency axis"). latency.sh substitutes __IN__,
-- __OUT__ and __BROKERS__.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__IN__',
        group_id='clink-q0lat', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__OUT__',
        linger_ms='0');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
