-- clink q12 (per-bidder bid count per 10s tumbling window), reads shared nx-bid,
-- writes nx-out-q12-clink. Run the producer with a LOW --tps (e.g. 1000) so the
-- event-time datetime spans many windows and they fire mid-drain.
--
-- CORRECT on a 1-PARTITION topic (use that at parallelism 1 - one subtask reads
-- one ordered partition): clink and Flink both emit exactly 184767 panes.
-- KNOWN ENGINE BUG on a MULTI-partition, key-distributed topic (each partition
-- spans the full time range): clink's single global watermark over the
-- interleaved partitions over-emits (~274k vs 184767). Root cause = no
-- per-partition watermarks in the Kafka source; gates parallelism>1. See README.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-bid',
        group_id='clink-q12', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q12 (bidder BIGINT, bid_count BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-out-q12-clink');
INSERT INTO sink_q12 SELECT bidder, COUNT(*) AS bid_count
  FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), bidder;
