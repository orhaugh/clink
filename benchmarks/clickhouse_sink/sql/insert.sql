INSERT INTO rows_out
SELECT k, a, b, i, f1, f2, ts, d, s_low, s_mid, s_high, lc
FROM rows_in;
