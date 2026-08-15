#!/usr/bin/env python3
"""Prove the QUAL-02 oracle can FAIL.

An oracle that has only ever been run against healthy data is not
evidence of anything - QUAL-01's verifier passed four separate times
while being wrong, and each defect was found only by making it judge
something it should have rejected. So each of the three failure modes is
injected here deliberately, and the oracle must name it.

Runs against a throwaway Postgres in Docker:
  ./test_oracle.py            # starts and removes its own container
  DSN=postgresql://...  ./test_oracle.py   # or use an existing server
"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from verifier import sample  # noqa: E402

import psycopg2  # noqa: E402

CONTAINER = "qual02-oracle-test"
PASSWORD = "oracle-test"
OWN_DSN = f"postgresql://postgres:{PASSWORD}@localhost:55432/postgres"


def start_postgres():
    subprocess.run(["docker", "rm", "-f", CONTAINER],
                   capture_output=True, check=False)
    subprocess.run(
        ["docker", "run", "-d", "--name", CONTAINER,
         "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-p", "55432:5432",
         "postgres:16", "postgres", "-c", "max_prepared_transactions=50"],
        check=True, capture_output=True)
    for _ in range(60):
        r = subprocess.run(["docker", "exec", CONTAINER, "pg_isready", "-U", "postgres", "-q"],
                           capture_output=True)
        if r.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("test postgres never became ready")


def reset(cur):
    cur.execute("DROP TABLE IF EXISTS public.q2_out")
    cur.execute("CREATE TABLE public.q2_out (event_id text, k bigint, amount bigint)")


def fill(cur, partition: int, lo: int, hi: int):
    """A contiguous committed prefix [lo, hi) for one partition."""
    cur.executemany(
        "INSERT INTO public.q2_out VALUES (%s, %s, %s)",
        [(f"p{partition}-{s}", s % 100, s) for s in range(lo, hi)])


def kinds(findings):
    return sorted({f["kind"] for f in findings})


def main() -> int:
    own = "DSN" not in os.environ
    if own:
        print("test: starting a throwaway postgres")
        start_postgres()
    dsn = os.environ.get("DSN", OWN_DSN)

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    failures = []

    def check(name, expected_kinds, produced_high):
        with conn.cursor() as cur:
            findings, stats = sample(cur, produced_high)
        got = kinds(findings)
        ok = got == sorted(expected_kinds)
        print(f"  {'PASS' if ok else 'FAIL'} {name}: expected {sorted(expected_kinds)}, got {got}")
        if not ok:
            failures.append((name, sorted(expected_kinds), got,
                             json.dumps(findings)[:400]))
        return stats

    high = {0: 1000, 1: 1000}

    print("test: a clean committed prefix is judged clean")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 0, 500)
        fill(cur, 1, 0, 400)
    stats = check("clean", [], high)
    assert stats["rows_total"] == 900, stats

    print("test: a duplicated row is named a duplicate")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO public.q2_out VALUES ('p0-250', 50, 250)")
    check("duplicate", ["duplicate"], high)

    print("test: a missing row is named a gap")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 0, 500)
        cur.execute("DELETE FROM public.q2_out WHERE event_id = 'p0-100'")
    check("gap", ["gap"], high)

    print("test: a prefix that does not start at 0 is named a gap")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 10, 500)
    check("gap-not-from-zero", ["gap"], high)

    print("test: output ahead of the generator is named foreign")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 0, 500)
    check("foreign-ahead", ["foreign"], {0: 100, 1: 100})

    print("test: an id from an unproduced partition is named foreign")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 0, 500)
        cur.execute("INSERT INTO public.q2_out VALUES ('p9-1', 1, 1)")
    check("foreign-partition", ["foreign_partition"], high)

    print("test: an unparseable id is named foreign rather than ignored")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 0, 500)
        cur.execute("INSERT INTO public.q2_out VALUES ('garbage', 1, 1)")
    check("foreign-token", ["foreign_partition"], high)

    print("test: prepared transactions are counted")
    with conn.cursor() as cur:
        reset(cur)
        fill(cur, 0, 0, 10)
    other = psycopg2.connect(dsn)
    with other.cursor() as cur:
        cur.execute("INSERT INTO public.q2_out VALUES ('p0-10', 1, 1)")
        cur.execute("PREPARE TRANSACTION 'clink_test_sub0_1'")
    other.close()
    with conn.cursor() as cur:
        _, stats = sample(cur, high)
    ok = stats["prepared_xacts"] == 1
    print(f"  {'PASS' if ok else 'FAIL'} prepared-visible: {stats['prepared_xacts']}")
    if not ok:
        failures.append(("prepared-visible", 1, stats["prepared_xacts"], ""))
    with conn.cursor() as cur:
        cur.execute("ROLLBACK PREPARED 'clink_test_sub0_1'")

    conn.close()
    if own:
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)

    if failures:
        print(f"\ntest: {len(failures)} ORACLE DEFECT(S)")
        for name, exp, got, detail in failures:
            print(f"  {name}: expected {exp}, got {got} {detail}")
        return 1
    print("\ntest: the oracle names every injected defect and passes clean data")
    return 0


if __name__ == "__main__":
    sys.exit(main())
