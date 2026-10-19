#!/usr/bin/env python3
"""Compare ClickHouse with PostgreSQL, the table clink captures.

    ./scripts/verify.py                 # waits up to three minutes to catch up
    ./scripts/verify.py --timeout 60
    ./scripts/verify.py --self-test     # check the comparison itself

PostgreSQL is the oracle: nothing here reads what clink thinks it wrote. The
script commits a marker row to PostgreSQL and waits until it reaches the
ClickHouse change log through the stream; every change committed before the
marker has then been delivered. It then reads the orders table from
PostgreSQL and compares it, row by row and value by value, with two readings
of ClickHouse:

  * the ReplacingMergeTree `orders` with FINAL, the current state that
    ClickHouse maintains from the change log; and
  * the change log `orders_changes` itself, rebuilt by taking the row with
    the highest __version for every id and dropping ids whose latest change
    is a delete. This is the query a reader without the materialised view
    would run.

It also counts change-log rows written twice: clink delivers to ClickHouse at
least once, so after a Worker restart the changes since the last completed
checkpoint are written again, identically, and both readings above absorb
them.

Exit status 0 means both readings equal the PostgreSQL table.
"""

import argparse
import base64
import decimal
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

MARKER_ID = 1_000_000_000

PG_ROWS = (
    "SELECT id, customer, status, amount::text, "
    "(extract(epoch FROM updated_at) * 1000)::bigint, rev FROM orders ORDER BY id"
)
CH_FINAL = (
    "SELECT id, customer, status, toString(amount), toUnixTimestamp64Milli(updated_at), rev "
    "FROM orders FINAL ORDER BY id FORMAT TabSeparated"
)
CH_REBUILT = (
    "SELECT id, argMax(customer, __version), argMax(status, __version), "
    "toString(argMax(amount, __version)), toUnixTimestamp64Milli(argMax(updated_at, __version)), "
    "argMax(rev, __version) FROM orders_changes GROUP BY id "
    "HAVING argMax(__deleted, __version) = 0 ORDER BY id FORMAT TabSeparated"
)


def parse_row(fields):
    """(id, customer, status, amount, updated_at ms, rev), comparable."""
    id_, customer, status, amount, at_ms, rev = fields
    return (int(id_), customer, status, decimal.Decimal(amount), int(at_ms), int(rev))


def compare(want, got, label):
    """Differences between two id-ordered row lists, as readable lines."""
    problems = []
    w = {r[0]: r for r in want}
    g = {r[0]: r for r in got}
    for id_ in sorted(w.keys() - g.keys())[:5]:
        problems.append(f"{label}: id {id_} is in PostgreSQL but missing")
    for id_ in sorted(g.keys() - w.keys())[:5]:
        problems.append(f"{label}: id {id_} is not in PostgreSQL (deleted, or never there)")
    differing = [id_ for id_ in sorted(w.keys() & g.keys()) if w[id_] != g[id_]]
    for id_ in differing[:5]:
        problems.append(f"{label}: id {id_} differs: PostgreSQL {w[id_]}, ClickHouse {g[id_]}")
    missing = len(w.keys() - g.keys())
    extra = len(g.keys() - w.keys())
    if missing or extra or differing:
        problems.append(f"{label}: {missing} missing, {extra} extra, {len(differing)} differing")
    return problems


class ClickHouse:
    def __init__(self, url, user, password):
        self.url = url.rstrip("/") + "/"
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def query(self, sql):
        req = urllib.request.Request(self.url, data=sql.encode(), method="POST")
        req.add_header("Authorization", self.auth)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"ClickHouse refused the query: {e.read().decode()[:300]}") from e

    def rows(self, sql):
        return [line.split("\t") for line in self.query(sql).splitlines() if line]


def psql(sql):
    """Run SQL in the postgres container; every row as a '|'-separated line."""
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "shop",
         "-v", "ON_ERROR_STOP=1", "-AtF", "\t", "-c", sql],
        cwd=here, check=True, capture_output=True, text=True,
    ).stdout
    return [line.split("\t") for line in out.splitlines() if line]


def self_test():
    """The comparison catches a missing row, an extra row and a changed value."""
    base = [(1, "c", "new", decimal.Decimal("5.50"), 1000, 1), (2, "d", "paid", decimal.Decimal("7"), 2000, 2)]
    cases = [
        ("identical", base, list(base), True),
        ("trailing zeros are the same decimal", base,
         [(1, "c", "new", decimal.Decimal("5.5"), 1000, 1), base[1]], True),
        ("a missing row", base, base[:1], False),
        ("an extra row", base, base + [(3, "e", "new", decimal.Decimal("1"), 3000, 1)], False),
        ("a stale value", base, [base[0], (2, "d", "new", decimal.Decimal("7"), 2000, 1)], False),
    ]
    fails = 0
    for name, want, got, should_pass in cases:
        passed = not compare(want, got, "self-test")
        verdict = "as it should" if passed == should_pass else "WRONG"
        print(f"self-test: {name}: {'pass' if passed else 'fail'}, {verdict}")
        fails += passed != should_pass
    return 1 if fails else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default=f"http://localhost:{os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')}")
    parser.add_argument("--user", default="clink")
    parser.add_argument("--password", default="clink")
    parser.add_argument("--timeout", type=int, default=180, help="seconds to wait for the marker")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()

    ch = ClickHouse(args.url, args.user, args.password)
    psql(f"INSERT INTO orders VALUES ({MARKER_ID}, 'marker', 'marker', 0, now(), 1) "
         "ON CONFLICT (id) DO UPDATE SET rev = orders.rev + 1, updated_at = now()")
    marker_rev = psql(f"SELECT rev FROM orders WHERE id = {MARKER_ID}")[0][0]
    deadline = time.monotonic() + args.timeout
    while True:
        seen = ch.query(f"SELECT count() FROM orders_changes WHERE id = {MARKER_ID} "
                        f"AND rev = {marker_rev} AND __op != 'snapshot'").strip()
        if seen not in ("", "0"):
            break
        if time.monotonic() > deadline:
            print(f"verify: the marker row did not reach ClickHouse within {args.timeout} s")
            return 1
        time.sleep(1)

    want = [parse_row(r) for r in psql(PG_ROWS)]
    final = [parse_row(r) for r in ch.rows(CH_FINAL)]
    rebuilt = [parse_row(r) for r in ch.rows(CH_REBUILT)]
    problems = compare(want, final, "orders FINAL") + compare(want, rebuilt, "rebuilt from orders_changes")

    log_rows = int(ch.query("SELECT count() FROM orders_changes").strip())
    distinct = int(ch.query("SELECT uniqExact((id, __version, __op)) FROM orders_changes").strip())
    snapshot_rows = int(ch.query("SELECT countIf(__op = 'snapshot') FROM orders_changes").strip())
    print(f"PostgreSQL orders:              {len(want)}")
    print(f"ClickHouse orders FINAL:        {len(final)}")
    print(f"rebuilt from the change log:    {len(rebuilt)}")
    print(f"change-log rows:                {log_rows} ({snapshot_rows} from the snapshot, "
          f"{log_rows - distinct} written twice after a restart)")
    for p in problems:
        print(f"MISMATCH {p}")
    if problems:
        return 1
    print("verify: ClickHouse matches PostgreSQL, value for value")
    return 0


if __name__ == "__main__":
    sys.exit(main())
