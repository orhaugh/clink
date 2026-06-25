-- clink q0 (stateless pass-through), BLACKHOLE sink variant. Reads shared nx-bid,
-- discards output (connector='blackhole' counts+drops every row) so the engine's
-- read+process rate is measured without the Kafka output connector as a ceiling.
-- Measured via the engine-side records_in counter (sample_rate.py), not an output
-- topic. __BROKERS__ substituted by the harness.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-bid',
        group_id='clink-q0bh', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='blackhole');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
