-- clink q6 (avg selling price per seller over their last 10 closed auctions).
-- Reads nx-bid + nx-auction (single-partition), writes the per-seller average as
-- a CHANGELOG into an upsert sink keyed by seller (__OUT__).
--
-- Pipeline: winning bid per auction (bid INNER JOIN auction on auction=id + the
-- interval residual b_datetime in [a_datetime, a_expires], then ROW_NUMBER top-1
-- by price) -> a last-10-per-seller AVG via the bounded ROWS frame, which lowers
-- to clink's last_n_agg operator (it consumes the winning-bid changelog and
-- re-emits the per-seller avg as the last-10 set slides).
--
-- SQL-ONLY by design: Flink has NO SQL form for q6 (its OVER does not consume
-- retractions, so the canonical Nexmark q6 ships only via Flink's DataStream
-- API). This template is the demonstration that clink expresses it in SQL; there
-- is no Flink counterpart in run.sh, so q6 is not in the gated comparison suite.
-- Verified over Kafka: 30,845 changelog records netting to 5,223 distinct sellers.
--
-- Run it (clink only):
--   clink_submit_sql --file q6-clink.sql --coordinator-host 127.0.0.1 --coordinator-port 8081
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-bid',
        group_id='clink-q6-b', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE auction (id BIGINT, itemname VARCHAR, initialbid BIGINT, reserve BIGINT, expires BIGINT, seller BIGINT, category BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='nx-auction',
        group_id='clink-q6-a', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q6 (seller BIGINT, avgp DOUBLE)
  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__OUT__',
        mode='upsert', primary_key='seller');
INSERT INTO sink_q6
SELECT seller, AVG(price) OVER (PARTITION BY seller ORDER BY close_dt
                                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS avgp
FROM (
  SELECT a_seller AS seller, b_price AS price, a_expires AS close_dt
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY b_auction ORDER BY b_price DESC) AS rn
    FROM (
      SELECT b_auction, b_price, a_seller, a_expires
      FROM bid AS B JOIN auction AS A ON B.auction = A.id
      WHERE b_datetime >= a_datetime AND b_datetime <= a_expires
    ) AS j
  ) AS r WHERE rn <= 1
) AS wins;
