CREATE TABLE orders (id BIGINT, qty BIGINT, price BIGINT, note TEXT) WITH (connector='file', format='json', path='${DIR}/orders.ndjson');
CREATE TABLE out_t (id BIGINT, band TEXT, note_or TEXT, nz BIGINT, hi BIGINT, lo BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id,
       CASE WHEN price >= 100 THEN 'high' WHEN price >= 20 THEN 'mid' ELSE 'low' END AS band,
       coalesce(note, 'none') AS note_or,
       nullif(qty, 1) AS nz,
       greatest(qty, price, 15) AS hi,
       least(qty, price, 15) AS lo
FROM orders;
