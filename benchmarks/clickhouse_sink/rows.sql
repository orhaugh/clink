-- Rows for the dataset, by number: one day of timestamps, so one partition;
-- strings of low (10), medium (10,000) and high (unique) cardinality.
SELECT
    toInt64(number)                                              AS k,
    toInt64(number * 7)                                          AS a,
    toInt64(number % 1000003)                                    AS b,
    toInt32(number % 2147483647)                                 AS i,
    number / 3.0                                                 AS f1,
    sqrt(toFloat64(number))                                      AS f2,
    toDateTime64('2026-10-01 00:00:00', 3, 'UTC') + toIntervalMillisecond(number % 86400000) AS ts,
    toDecimal64(number % 100000000, 4) / 7                       AS d,
    concat('low-', toString(number % 10))                        AS s_low,
    concat('mid-', toString(number % 10000))                     AS s_mid,
    concat('high-', hex(sipHash64(number)), '-', toString(number)) AS s_high,
    concat('lc-', toString(number % 100))                        AS lc
FROM numbers({offset}, {rows})
