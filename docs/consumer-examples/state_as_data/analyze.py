#!/usr/bin/env python3
"""Analyse an exported clink keyed-state Parquet from outside the engine.

The export schema is (op_id, key_group, slot, user_key, value_bytes): one row
per keyed state entry. `user_key` is the decoded key - here the user id as bytes
- so it joins directly to reference data; `value_bytes` is the raw operator
accumulator (decoding it needs the operator's accumulator schema, a follow-on).

Shows two things a running job's state gives you for free once it is an open
dataset: a join of the live keys to a reference table, and key-group skew (how
evenly the keyspace is spread across subtasks). Uses DuckDB when available (the
headline: SQL straight over the Parquet), else falls back to pyarrow.

    python3 analyze.py <state.parquet>
"""

from __future__ import annotations

import sys

import pyarrow.parquet as pq

# A tiny reference table - in a real notebook this is your users / accounts dim.
REFERENCE = {"0": "alice", "1": "bob", "10": "carol", "50": "erin", "99": "zoe"}


def _decode_key(v: object) -> str:
    # Most keys are printable (the decoded user id); an internal slot can hold
    # non-text bytes, so fall back to hex rather than crash.
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return "0x" + v.hex()
    return str(v)


def main(path: str) -> int:
    table = pq.read_table(path)
    print(f"state dataset: {table.num_rows} keyed entries, columns {table.schema.names}\n")

    try:
        import duckdb  # noqa: PLC0415
    except ImportError:
        duckdb = None

    if duckdb is not None:
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE ref(user_id VARCHAR, name VARCHAR); "
            "INSERT INTO ref VALUES "
            + ",".join(f"('{k}','{v}')" for k, v in REFERENCE.items())
        )
        con.execute(f"CREATE VIEW state AS SELECT * FROM read_parquet('{path}')")
        print("live keys joined to the reference table (DuckDB over the Parquet):")
        rows = con.execute(
            "SELECT r.name, CAST(s.user_key AS VARCHAR) AS user_id, s.key_group "
            "FROM state s JOIN ref r ON CAST(s.user_key AS VARCHAR) = r.user_id "
            "WHERE s.slot = 'agg' ORDER BY user_id::INTEGER"
        ).fetchall()
        for name, uid, kg in rows:
            print(f"  {name:<8} user {uid:<4} -> key_group {kg}")
        print("\nkey-group skew (entries per key_group, top 5):")
        for kg, n in con.execute(
            "SELECT key_group, COUNT(*) c FROM state WHERE slot='agg' "
            "GROUP BY key_group ORDER BY c DESC, key_group LIMIT 5"
        ).fetchall():
            print(f"  key_group {kg:<3} : {n} keys")
        return 0

    # pyarrow-only fallback (no DuckDB installed).
    print("(DuckDB not installed - pyarrow fallback; `pip install duckdb` for the SQL path)\n")
    d = table.to_pydict()
    print("live keys joined to the reference table:")
    for uk, kg, slot in zip(d["user_key"], d["key_group"], d["slot"]):
        key = _decode_key(uk)
        if slot == "agg" and key in REFERENCE:
            print(f"  {REFERENCE[key]:<8} user {key:<4} -> key_group {kg}")
    skew: dict[int, int] = {}
    for kg, slot in zip(d["key_group"], d["slot"]):
        if slot == "agg":
            skew[kg] = skew.get(kg, 0) + 1
    print("\nkey-group skew (entries per key_group, top 5):")
    for kg, n in sorted(skew.items(), key=lambda kv: (-kv[1], kv[0]))[:5]:
        print(f"  key_group {kg:<3} : {n} keys")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: analyze.py <state.parquet>", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
