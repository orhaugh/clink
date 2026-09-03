CREATE TABLE docs (id BIGINT, j TEXT, nums ARRAY<BIGINT>, m MAP<TEXT, TEXT>) WITH (connector='file', format='json', path='${DIR}/docs.ndjson');
CREATE TABLE out_t (id BIGINT, nm TEXT, tags TEXT, has_tags BOOLEAN, obj TEXT, second BIGINT, card BIGINT,
                    m_lookup TEXT, missing TEXT, literal ARRAY<BIGINT>, single BIGINT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id,
       json_value(j, '$.user.name') AS nm,
       json_query(j, '$.user.tags') AS tags,
       json_exists(j, '$.user.tags') AS has_tags,
       json_object('id', id, 'kind', 'doc') AS obj,
       nums[2] AS second,
       cardinality(nums) AS card,
       m['k1'] AS m_lookup,
       m['k2'] AS missing,
       ARRAY[1, 2, 3] AS literal,
       element(ARRAY[99]) AS single
FROM docs;
