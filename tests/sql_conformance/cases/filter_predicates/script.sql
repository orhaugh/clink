CREATE TABLE orders (id BIGINT, qty BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE TABLE out_t (id BIGINT, why TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id, 'and_or' AS why FROM orders WHERE (qty > 1 AND price < 50) OR id IN (7, 9)
UNION ALL SELECT id, 'not_between' AS why FROM orders WHERE NOT (price BETWEEN 10 AND 50)
UNION ALL SELECT id, 'like' AS why FROM orders WHERE note LIKE 'g%'
UNION ALL SELECT id, 'is_null' AS why FROM orders WHERE note IS NULL
UNION ALL SELECT id, 'is_not_null' AS why FROM orders WHERE note IS NOT NULL AND qty <> 3;
