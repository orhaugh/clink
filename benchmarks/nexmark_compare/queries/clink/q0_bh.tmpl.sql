-- clink q0 (stateless pass-through)
-- NO event_time_column here, deliberately: q0 has no windowing, and Flink's
-- matching template declares no WATERMARK either. Declaring one made clink's
-- planner emit an extra assign_timestamps_row stage - its own task, thread and
-- channel hop - that Flink never ran, on the very query meant to isolate raw
-- per-record pipeline overhead., BLACKHOLE sink variant. Reads shared nx-bid,
-- discards output (connector='blackhole' counts+drops every row) so the engine's
-- read+process rate is measured without the Kafka output connector as a ceiling.
-- Measured via the engine-side records_in counter (sample_rate.py), not an output
-- topic. __BROKERS__ substituted by the harness.
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-bid',
        group_id='clink-q0bh', auto_offset_reset='earliest');
CREATE TABLE sink_q0 (auction BIGINT, bidder BIGINT, price BIGINT, datetime BIGINT)
  WITH (connector='blackhole');
INSERT INTO sink_q0 SELECT auction, bidder, price, datetime FROM bid;
