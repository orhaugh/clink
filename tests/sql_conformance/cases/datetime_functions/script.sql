CREATE TABLE stamps (id BIGINT, s TEXT) WITH (connector='file', format='json', path='${DIR}/stamps.ndjson');
-- Timestamps are UTC integers: date_trunc projects the truncated instant as BIGINT epoch millis.
CREATE TABLE out_t (id BIGINT, yr BIGINT, mon BIGINT, dy BIGINT, hr BIGINT, mi BIGINT, sec BIGINT, dow BIGINT,
                    doy BIGINT, q BIGINT, ep BIGINT, hour_start BIGINT, formatted TEXT) WITH (connector='file', format='json', path='${OUT}');
INSERT INTO out_t
SELECT id,
       EXTRACT(YEAR FROM to_timestamp(s)) AS yr, EXTRACT(MONTH FROM to_timestamp(s)) AS mon,
       EXTRACT(DAY FROM to_timestamp(s)) AS dy, EXTRACT(HOUR FROM to_timestamp(s)) AS hr,
       EXTRACT(MINUTE FROM to_timestamp(s)) AS mi, EXTRACT(SECOND FROM to_timestamp(s)) AS sec,
       EXTRACT(DOW FROM to_timestamp(s)) AS dow, EXTRACT(DOY FROM to_timestamp(s)) AS doy,
       EXTRACT(QUARTER FROM to_timestamp(s)) AS q, EXTRACT(EPOCH FROM to_timestamp(s)) AS ep,
       date_trunc('hour', to_timestamp(s)) AS hour_start,
       date_format(to_timestamp(s), 'yyyy/MM/dd HH:mm') AS formatted
FROM stamps;
