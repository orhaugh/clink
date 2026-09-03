CREATE TABLE nums (id BIGINT, x DOUBLE PRECISION, n BIGINT) WITH (connector='file', format='json', path='${DIR}/nums.ndjson');
-- sign() projects DOUBLE whatever its argument type; abs() and mod() keep BIGINT.
CREATE TABLE out_t (id BIGINT, ab DOUBLE PRECISION, sg DOUBLE PRECISION, fl DOUBLE PRECISION, ce DOUBLE PRECISION,
                    rd DOUBLE PRECISION, rd1 DOUBLE PRECISION, tr DOUBLE PRECISION, sq DOUBLE PRECISION,
                    pw DOUBLE PRECISION, md BIGINT, nabs BIGINT, nsign DOUBLE PRECISION) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id, abs(x) AS ab, sign(x) AS sg, floor(x) AS fl, ceil(x) AS ce, round(x) AS rd, round(x, 1) AS rd1,
       trunc(x) AS tr, sqrt(abs(x)) AS sq, power(2, 3) AS pw, mod(n, 5) AS md, abs(n) AS nabs, sign(n) AS nsign
FROM nums;
