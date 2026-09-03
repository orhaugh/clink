CREATE TABLE orders (id BIGINT, qty BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE TABLE out_t (id BIGINT, price BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, price FROM orders ORDER BY price DESC LIMIT 3;
