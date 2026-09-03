CREATE TABLE words (id BIGINT, s TEXT, t TEXT) WITH (connector='file', format='json', path='${DIR}/words.ndjson');
CREATE TABLE out_t (id BIGINT, up TEXT, low TEXT, cap TEXT, len BIGINT, joined TEXT, cat TEXT, sub TEXT,
                    pos BIGINT, rep TEXT, trimmed TEXT, ltrimmed TEXT, padded TEXT, rpadded TEXT,
                    lft TEXT, rgt TEXT, part TEXT, rev TEXT, rpt TEXT, asc_code BIGINT, ch TEXT,
                    starts BOOLEAN, rx TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id, upper(s), lower(s), initcap(s), length(s), s || '!' AS joined, concat(t, '/', t) AS cat,
       substring(s, 2, 3) AS sub, position('l' IN s) AS pos, replace(s, 'l', 'L') AS rep,
       btrim(s) AS trimmed, ltrim(s) AS ltrimmed, lpad(t, 6, '*') AS padded, rpad(t, 6, '-') AS rpadded,
       left(s, 3) AS lft, right(s, 3) AS rgt, split_part(t, '-', 2) AS part, reverse(t) AS rev,
       repeat(t, 2) AS rpt, ascii(t) AS asc_code, chr(65) AS ch, starts_with(s, 'He') AS starts,
       regexp_extract(t, '([a-z])X', 1) AS rx
FROM words;
