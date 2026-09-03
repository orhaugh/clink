CREATE TABLE a (v BIGINT) WITH (connector='file', format='json', path='${DIR}/a.ndjson');
CREATE TABLE b (v BIGINT) WITH (connector='file', format='json', path='${DIR}/b.ndjson');
-- EXCEPT ALL produces a changelog stream (a later row on the other side can retract an
-- earlier result), so the sink declares changelog='true' and each row carries its kind.
CREATE TABLE out_t (v BIGINT) WITH (connector='file', format='json', path='${OUT}', changelog='true');
INSERT INTO out_t SELECT v FROM a EXCEPT ALL SELECT v FROM b;
