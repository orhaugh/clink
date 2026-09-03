-- A JSON source reads each column from the field of the same name, so the
-- table is declared with a misspelt column and RENAMEd onto the real field.
CREATE TABLE IF NOT EXISTS orders (id BIGINT, quantity_typo BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE TABLE IF NOT EXISTS orders (id BIGINT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
ALTER TABLE orders RENAME COLUMN quantity_typo TO qty;
ALTER TABLE orders DROP COLUMN note;
ALTER TABLE orders ADD COLUMN currency TEXT;
DROP TABLE IF EXISTS never_existed;
CREATE TABLE out_t (id BIGINT, qty BIGINT, currency TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, qty, coalesce(currency, 'GBP') AS currency FROM orders;
