CREATE TABLE orders (id BIGINT, qty BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE OR REPLACE FUNCTION with_tax(amount BIGINT) RETURNS BIGINT AS 'amount + amount / 10' LANGUAGE SQL;
CREATE OR REPLACE FUNCTION shout(s TEXT) RETURNS TEXT AS 'upper(s) || ''!''' LANGUAGE SQL;
CREATE TABLE out_t (id BIGINT, taxed BIGINT, loud TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, with_tax(qty * price) AS taxed, shout(coalesce(note, 'none')) AS loud FROM orders;
