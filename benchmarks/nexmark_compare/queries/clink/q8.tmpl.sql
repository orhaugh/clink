-- clink q8 (new users): persons and auctions created in the SAME 10s tumbling
-- window, joined on person.id = auction.seller. Two windowed-aggregate join
-- sides + a window-equality residual (rejects cross-window pairs). Reads shared
-- nx-person + nx-auction, writes __OUT__ (run.sh substitutes the output topic).
-- Single-partition sources at parallelism 1. The per-window COUNT(*) on each
-- side is unused (clink needs an aggregate in a GROUP BY SELECT); the grouping
-- is what matters.
CREATE TABLE person (id BIGINT, name VARCHAR, emailaddress VARCHAR, city VARCHAR, state VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-person',
        group_id='clink-q8-p', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE auction (id BIGINT, itemname VARCHAR, initialbid BIGINT, reserve BIGINT, expires BIGINT, seller BIGINT, category BIGINT, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='nx-auction',
        group_id='clink-q8-a', auto_offset_reset='earliest',
        event_time_column='datetime', watermark_lag_ms='4000');
CREATE TABLE sink_q8 (id BIGINT, name VARCHAR, starttime BIGINT)
  WITH (connector='kafka', format='json', brokers='localhost:9092', topic='__OUT__');
INSERT INTO sink_q8 SELECT P_id AS id, P_name AS name, P_starttime AS starttime
  FROM (SELECT id, name, COUNT(*) AS pc, window_start AS starttime, window_end AS endtime
        FROM person GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), id, name) AS P
  JOIN (SELECT seller, COUNT(*) AS ac, window_start AS astart, window_end AS aend
        FROM auction GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), seller) AS A
  ON P.id = A.seller WHERE P_starttime = A_astart AND P_endtime = A_aend;
