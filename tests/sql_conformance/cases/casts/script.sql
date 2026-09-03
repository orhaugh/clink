CREATE TABLE mixed (id BIGINT, txt TEXT, d DOUBLE PRECISION, b TEXT, n BIGINT) WITH (connector='file', format='json', path='${DIR}/mixed.ndjson');
-- Integer casts widen to BIGINT on the wire (INTEGER and SMALLINT targets both project BIGINT).
CREATE TABLE out_t (id BIGINT, as_big BIGINT, as_int BIGINT, as_small BIGINT, as_dbl DOUBLE PRECISION,
                    as_dec DECIMAL(10,2), as_txt TEXT, as_bool BOOLEAN, d_as_big BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id, CAST(txt AS BIGINT) AS as_big, CAST(txt AS INTEGER) AS as_int, CAST(n AS SMALLINT) AS as_small,
       CAST(txt AS DOUBLE PRECISION) AS as_dbl, CAST(d AS DECIMAL(10,2)) AS as_dec, CAST(n AS TEXT) AS as_txt,
       CAST(b AS BOOLEAN) AS as_bool, CAST(d AS BIGINT) AS d_as_big
FROM mixed;
