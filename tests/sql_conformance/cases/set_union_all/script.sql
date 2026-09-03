CREATE TABLE a (v BIGINT) WITH (connector='file', format='json', path='${DIR}/a.ndjson');
CREATE TABLE b (v BIGINT) WITH (connector='file', format='json', path='${DIR}/b.ndjson');
CREATE TABLE out_t (v BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT v FROM a UNION ALL SELECT v FROM b;
