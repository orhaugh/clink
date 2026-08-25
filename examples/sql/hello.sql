-- clink in one command.
--
--   clink run examples/sql/hello.sql
--
-- Reads a small stream of device readings, groups them by region, and
-- prints the result. No broker, no database, no configuration: the source
-- is a file that ships alongside this script, and a bare SELECT prints to
-- stdout.

CREATE TABLE readings (
    ts     BIGINT,
    region VARCHAR,
    device VARCHAR,
    reading DOUBLE
) WITH (
    connector = 'file',
    format    = 'json',
    path      = 'examples/sql/events.ndjson'
);

SELECT region,
       COUNT(*)      AS readings,
       AVG(reading)  AS avg_reading,
       MAX(reading)  AS peak
FROM readings
GROUP BY region;
