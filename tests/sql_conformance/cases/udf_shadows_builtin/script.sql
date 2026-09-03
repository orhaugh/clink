-- A user-defined function wins over a built-in of the same name, so a
-- built-in added in a later release can never change what this computes.
CREATE TABLE nums (id BIGINT, x DOUBLE PRECISION, n BIGINT) WITH (connector='file', format='json', path='${DIR}/nums.ndjson');
CREATE OR REPLACE FUNCTION abs(v BIGINT) RETURNS BIGINT AS 'v * 100' LANGUAGE SQL;
CREATE TABLE out_t (id BIGINT, shadowed BIGINT, still_builtin DOUBLE PRECISION) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, abs(n) AS shadowed, sign(n) AS still_builtin FROM nums;
