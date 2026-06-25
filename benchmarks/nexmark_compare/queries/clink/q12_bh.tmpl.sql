-- clink q12 (per-bidder bid count per 10s tumbling window), BLACKHOLE sink variant.
-- Discards output so the windowed-agg processing rate is measured without the
-- Kafka output connector as a ceiling (engine-side records_in via sample_rate.py).
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-bid',
        group_id='clink-q12bh', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT) WITH (connector='blackhole');
INSERT INTO sink_q12 SELECT bidder, COUNT(*) AS bid_count
  FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder;
