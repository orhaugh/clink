-- clink q12 (per-bidder bid count per 10s tumbling window). Reads shared nx-bid,
-- writes __OUT__ (run.sh substitutes the per-engine output topic). Correct on a
-- single-partition nx-bid (parallelism 1); multi-partition needs the
-- per-partition watermark refinement (see README).
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-bid',
        group_id='clink-q12', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__OUT__');
INSERT INTO sink_q12 SELECT bidder, COUNT(*) AS bid_count
  FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder;
