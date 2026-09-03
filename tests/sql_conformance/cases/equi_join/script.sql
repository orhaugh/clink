CREATE TABLE orders (oid BIGINT, cid BIGINT, amount BIGINT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE TABLE customers (cid BIGINT, name TEXT) WITH (connector='file', format='json', path='${DIR}/customers.ndjson');
CREATE TABLE out_t (oid BIGINT, name TEXT, amount BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT o.oid, c.name, o.amount FROM orders o JOIN customers c ON o.cid = c.cid;
