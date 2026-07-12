#!/usr/bin/env python3
"""Smoke-test an INSTALLED pyclink wheel against its BUNDLED libclink.

Unlike test_pyclink.py (which imports the source package and points at a repo
build tree), this script must be run from OUTSIDE the repo against a pip-installed
wheel, with CLINK_LIB unset, so it exercises exactly what a `pip install pyclink`
user gets: the library bundled inside the wheel, found by pyclink's own loader.

    pip install dist/pyclink-*.whl
    cd /tmp && python -m pyclink_wheel_smoke   # or: python path/to/wheel_smoke.py

Exits non-zero on any failure, so CI can gate on it.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile


def main() -> int:
    if os.environ.get("CLINK_LIB"):
        print("wheel_smoke: refusing to run with CLINK_LIB set - it would hide the "
              "bundled library this test exists to check", file=sys.stderr)
        return 2

    import pyclink

    bundled = pyclink._bundled_library()
    if not bundled:
        print(f"wheel_smoke: no libclink bundled next to {pyclink.__file__}", file=sys.stderr)
        return 1
    print("pyclink   :", pyclink.__file__)
    print("bundled   :", bundled)

    with tempfile.TemporaryDirectory() as d:
        orders = os.path.join(d, "orders.ndjson")
        rows = [(1, 10), (2, 20), (1, 30), (2, 5), (1, 7)]
        with open(orders, "w") as fh:
            for uid, amt in rows:
                fh.write(json.dumps({"user_id": uid, "amount": amt}) + "\n")

        # No lib_path: the engine must load the bundled library.
        with pyclink.Engine() as e:
            e.execute(
                f"""
                CREATE TABLE orders (user_id BIGINT, amount BIGINT)
                  WITH (connector='file', format='json', path='{orders}');
                CREATE TABLE results (user_id BIGINT, total BIGINT)
                  WITH (connector='collect');
                INSERT INTO results SELECT user_id, SUM(amount) AS total
                  FROM orders GROUP BY user_id
                """
            )
            table = e.collect("results").read_all()
            e.await_all()

    got = dict(zip(table.column("user_id").to_pylist(), table.column("total").to_pylist()))
    # SUM(BIGINT) is exact int64: user 1 -> 47, user 2 -> 25.
    want = {1: 47, 2: 25}
    if got != want:
        print(f"wheel_smoke: wrong result {got}, expected {want}", file=sys.stderr)
        return 1
    print("wheel_smoke OK:", got)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
