CREATE TABLE orders (id BIGINT, qty BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE TABLE out_t (id BIGINT, total BIGINT, quarter BIGINT, remainder BIGINT, negated BIGINT, label TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id, qty * price AS total, price / 4 AS quarter, price % 3 AS remainder, -qty AS negated,
       'order-' || CAST(id AS TEXT) AS label
FROM orders;
