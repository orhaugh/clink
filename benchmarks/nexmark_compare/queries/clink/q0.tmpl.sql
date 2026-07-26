-- clink q0 (stateless pass-through). Reads shared nx-bid, writes __OUT__.
-- NO event_time_column: q0 has no windowing and Flink's matching template
-- declares no WATERMARK either. Declaring one made clink's planner emit an extra
-- assign_timestamps_row stage - its own task, thread and channel hop - that Flink
-- never ran, on the very query meant to isolate raw per-record pipeline overhead.
-- run.sh substitutes __OUT__ with the per-engine output topic.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-bid',
        group_id='clink-q0', auto_offset_reset='earliest');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__OUT__');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
