CREATE TABLE things (id BIGINT, tags ARRAY<TEXT>, attrs MAP<TEXT, TEXT>, addr ROW<city TEXT, zip INT>) WITH (connector='file', format='json', path='${DIR}/things.ndjson');
CREATE TABLE out_t (id BIGINT, first_tag TEXT, n_tags BIGINT, colour TEXT, city TEXT, zip INT, tags ARRAY<TEXT>) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, tags[1] AS first_tag, cardinality(tags) AS n_tags, attrs['colour'] AS colour,
                         (addr).city AS city, (addr).zip AS zip, tags FROM things;
