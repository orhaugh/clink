#!/usr/bin/env python3
"""The correctness gate for the ClickHouse sink benchmarks.

The landed table is held to the dataset's definition, not to anything the
pipeline reports about itself. rows.sql over numbers(0, N), evaluated by
clickhouse-local when run.sh writes the input, gives the expected side: the
row count and an order-independent checksum of each column and of each whole
row. The table must hold exactly N rows, every k from 0 to N-1 once, and the
same checksums. A per-column checksum names the column that went wrong; the
whole-row checksum catches values that landed in the wrong row.

  verify.py checksum-sql --rows N           the expected side, over rows.sql
  verify.py checksum-sql --table DB.TABLE   the landed side
  verify.py gate --expected E --actual A --rows N [--out F]

Both checksum queries print one JSONEachRow line; gate compares two of them
and exits 1 unless every check holds. A run whose gate fails has no figure.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# Each column as hashed on both sides. The forms make the expected side
# (rows.sql: ts is DateTime64(3, 'UTC'), lc a plain String) and the landed
# side (table.sql: ts DateTime64(3), lc LowCardinality(String)) hash the same
# value: doubles by their bits, the timestamp by its epoch milliseconds, the
# decimal by its text at the shared Decimal(18, 4) type.
COLUMNS = [
    ("k", "k"),
    ("a", "a"),
    ("b", "b"),
    ("i", "i"),
    ("f1", "reinterpretAsUInt64(f1)"),
    ("f2", "reinterpretAsUInt64(f2)"),
    ("ts", "toUnixTimestamp64Milli(ts)"),
    ("d", "toString(d)"),
    ("s_low", "s_low"),
    ("s_mid", "s_mid"),
    ("s_high", "s_high"),
    ("lc", "toString(lc)"),
]


def checksum_sql(source):
    parts = [
        "count() AS rows",
        "uniqExact(k) AS distinct_k",
        "min(k) AS min_k",
        "max(k) AS max_k",
        "sum(cityHash64({})) AS h_row".format(", ".join(e for _, e in COLUMNS)),
    ]
    parts += ["sum(cityHash64({})) AS h_{}".format(e, n) for n, e in COLUMNS]
    return "SELECT\n    {}\nFROM {}\nFORMAT JSONEachRow".format(",\n    ".join(parts), source)


def rows_source(rows):
    with open(os.path.join(HERE, "rows.sql")) as f:
        body = f.read()
    body = body.replace("{offset}", "0").replace("{rows}", str(rows))
    # rows.sql opens with comment lines; inside a subquery they are harmless,
    # but the closing parenthesis must not land on one.
    return "(\n{}\n)".format(body.rstrip())


def load_one(path):
    with open(path) as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    if len(lines) != 1:
        raise SystemExit("verify: {} holds {} result lines, expected one".format(path, len(lines)))
    return json.loads(lines[0])


def as_int(v):
    # JSONEachRow quotes 64-bit integers by default.
    return int(v)


def gate(expected, actual, rows):
    checks = []

    def check(name, ok, detail):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    landed = as_int(actual["rows"])
    distinct = as_int(actual["distinct_k"])
    check("expected_side_is_the_dataset", as_int(expected["rows"]) == rows,
          "rows.sql over numbers(0, {}) gave {} rows".format(rows, as_int(expected["rows"])))
    check("row_count", landed == rows, "{} landed, {} produced".format(landed, rows))
    check("every_key_once", distinct == rows,
          "{} distinct k, {} produced".format(distinct, rows))
    check("key_range", as_int(actual["min_k"]) == 0 and as_int(actual["max_k"]) == rows - 1,
          "k in [{}, {}], expected [0, {}]".format(actual["min_k"], actual["max_k"], rows - 1))
    mismatched = [n for n, _ in COLUMNS if str(actual["h_" + n]) != str(expected["h_" + n])]
    check("column_checksums", not mismatched,
          "mismatched: {}".format(", ".join(mismatched)) if mismatched else "all {} equal".format(len(COLUMNS)))
    check("row_checksum", str(actual["h_row"]) == str(expected["h_row"]),
          "whole-row checksum {}".format("equal" if str(actual["h_row"]) == str(expected["h_row"]) else "differs"))
    return {
        "passed": all(c["ok"] for c in checks),
        "rows_produced": rows,
        "rows_landed": landed,
        "duplicates": landed - distinct,
        "missing_keys": rows - distinct,
        "mismatched_columns": mismatched,
        "checks": checks,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    cs = sub.add_parser("checksum-sql", help="print a checksum query")
    side = cs.add_mutually_exclusive_group(required=True)
    side.add_argument("--rows", type=int, help="the expected side: rows.sql over numbers(0, ROWS)")
    side.add_argument("--table", help="the landed side: DB.TABLE")
    g = sub.add_parser("gate", help="compare a landed checksum with the expected one")
    g.add_argument("--expected", required=True)
    g.add_argument("--actual", required=True)
    g.add_argument("--rows", type=int, required=True)
    g.add_argument("--out")
    a = ap.parse_args()

    if a.cmd == "checksum-sql":
        print(checksum_sql(rows_source(a.rows) if a.rows is not None else a.table))
        return 0

    report = gate(load_one(a.expected), load_one(a.actual), a.rows)
    text = json.dumps(report, indent=2)
    if a.out:
        with open(a.out, "w") as f:
            f.write(text + "\n")
    for c in report["checks"]:
        print("verify: {:<30} {}  {}".format(c["check"], "ok  " if c["ok"] else "FAIL", c["detail"]))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
