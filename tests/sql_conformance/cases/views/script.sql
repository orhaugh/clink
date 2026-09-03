CREATE TABLE orders (id BIGINT, qty BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE VIEW big_orders AS SELECT id, qty * price AS total FROM orders WHERE qty * price >= 50;
CREATE TABLE out_t (id BIGINT, total BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, total FROM big_orders;
