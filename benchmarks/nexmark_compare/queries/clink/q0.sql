-- clink q0 (stateless pass-through)
-- NO event_time_column here, deliberately: q0 has no windowing, and Flink's
-- matching template declares no WATERMARK either. Declaring one made clink's
-- planner emit an extra assign_timestamps_row stage - its own task, thread and
-- channel hop - that Flink never ran, on the very query meant to isolate raw
-- per-record pipeline overhead., reads shared nx-bid, writes nx-out-q0-clink.
-- Brokers is the host-mapped port (clink runs on the host); the Flink side uses
-- the in-network listener kafka:29092. Submitted via:
--   clink_submit_sql --file q0.sql --coordinator-host 127.0.0.1 --coordinator-port 8081
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-bid',
        group_id='clink-q0', auto_offset_reset='earliest');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-out-q0-clink');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
