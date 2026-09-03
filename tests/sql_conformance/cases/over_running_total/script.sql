CREATE TABLE payments (user_id BIGINT, amount BIGINT, ts BIGINT) WITH (connector='file', format='json', path='${DIR}/payments.ndjson', event_time_column='ts', watermark_lag_ms='0');
CREATE TABLE out_t (user_id BIGINT, amount BIGINT, ts BIGINT, running BIGINT, n BIGINT, prev BIGINT, first_amt BIGINT, last2 BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT *,
       SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) AS running,
       COUNT(*) OVER (PARTITION BY user_id ORDER BY ts) AS n,
       LAG(amount) OVER (PARTITION BY user_id ORDER BY ts) AS prev,
       FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY ts) AS first_amt,
       SUM(amount) OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS last2
FROM payments;
