-- clink q12 (per-bidder bid count per 10s tumbling window), reads shared nx-bid,
-- writes nx-out-q12-clink. Run the producer with a LOW --tps (e.g. 1000) so the
-- event-time datetime spans many windows and they fire mid-drain.
--
-- KNOWN ISSUE (the cross-engine correctness gate's first finding): over a
-- multi-partition Kafka source clink emits TOO MANY panes (observed 280729 vs the
-- data-derived-correct 184767 that Flink produces) - it over-emits ~96k panes and
-- under-counts the total. The in-process generator path (clink_nexmark_bench q12)
-- is fine, so the bug is in the windowed-aggregate + Kafka-source watermark
-- interaction (likely early/partial window firing across partitions). Until fixed,
-- q12 is NOT quotable as a cross-engine ratio. See README.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-bid',
        group_id='clink-q12', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-out-q12-clink');
INSERT INTO sink_q12 SELECT bidder, COUNT(*) AS bid_count
  FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder;
