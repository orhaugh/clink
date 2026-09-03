CREATE TABLE money (id BIGINT, amount DECIMAL(18,2)) WITH (connector='file', format='json', path='${DIR}/money.ndjson');
CREATE TABLE out_t (id BIGINT, plus DECIMAL(18,2), triple DECIMAL(18,2), tenth DECIMAL(18,2), lit DECIMAL(18,2)) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t SELECT id, amount + 0.1 AS plus, amount * 3 AS triple, amount / 10 AS tenth, 0.1 + 0.2 AS lit FROM money;
