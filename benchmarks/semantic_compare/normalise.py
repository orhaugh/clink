#!/usr/bin/env python3
"""Normalise and compare two engines' drained sink outputs (QUAL-07).

The comparator is the campaign's false-pass surface: a rule that maps two
genuinely different rows to one string passes everything, silently. Every
rule here therefore does the LEAST it can:

  * JSON objects are parsed and re-serialised with sorted keys - field
    ORDER is presentation, field NAMES and VALUES are semantics.
  * Integer-valued floats collapse to integers (49975.0 == 49975) -
    engines disagree on decimal rendering of the same number, and JSON
    has one number type anyway.
  * Nothing else. Timestamps, strings, and non-integer floats pass
    through byte-exact; a tolerance, where a query's class allows one,
    is applied by the COMPARATOR per declared field, never by silent
    rounding in the normaliser.

Judgement classes (declared per query in queries.json, with reasons):

  append       every output row is final: normalise, sort, byte-diff.
  materialised the query emits an UPDATE stream (unbounded GROUP BY,
               top-N): engines legitimately differ in emit cadence, so
               the streams are folded by primary key (last write wins)
               and the FINAL IMAGES are compared. The key columns are
               part of the declaration.
  tolerance    like append or materialised, but named float fields
               compare within a declared absolute epsilon (reduction
               order across parallelism makes float sums
               run-dependent). Everything undeclared stays exact.
"""
import json
import math


def normalise_value(v):
    if isinstance(v, float) and math.isfinite(v) and v == int(v):
        return int(v)
    return v


def normalise_row(line):
    """One drained sink line -> canonical string, or None for blanks.
    Raises ValueError on non-JSON: a corrupt sink line must fail the
    comparison, never be skipped into a pass."""
    line = line.strip()
    if not line:
        return None
    obj = json.loads(line)
    if not isinstance(obj, dict):
        raise ValueError(f"sink line is not a JSON object: {line[:80]}")
    return json.dumps({k: normalise_value(v) for k, v in sorted(obj.items())},
                      sort_keys=True, separators=(",", ":"))


def load_rows(path):
    rows = []
    with open(path) as fh:
        for line in fh:
            n = normalise_row(line)
            if n is not None:
                rows.append(n)
    return rows


def materialise(rows, key_fields):
    """Fold an update stream to final images: last write per key wins.
    Order within the file IS the update order per key (a Kafka sink
    partition preserves per-key order when keyed; the drain writes in
    offset order per partition and the fold is per key)."""
    final = {}
    for r in rows:
        obj = json.loads(r)
        try:
            key = tuple(obj[k] for k in key_fields)
        except KeyError as e:
            raise ValueError(f"materialised row lacks key field {e}: {r[:80]}") from e
        final[key] = r
    return sorted(final.values())


def within_tolerance(a_row, b_row, tol_fields, epsilon):
    """Exact match on everything except the declared float fields, which
    compare within epsilon. Both rows are canonical JSON strings."""
    a, b = json.loads(a_row), json.loads(b_row)
    if set(a) != set(b):
        return False
    for k in a:
        if k in tol_fields:
            try:
                if abs(float(a[k]) - float(b[k])) > epsilon:
                    return False
            except (TypeError, ValueError):
                return False
        elif a[k] != b[k]:
            return False
    return True


def compare(a_path, b_path, *, mode, key_fields=(), tol_fields=(), epsilon=0.0):
    """Compare two drained sinks. Returns (equal, detail) where detail
    names counts and up to three sample divergences - a diff nobody can
    see is a diff nobody can diagnose."""
    a_rows, b_rows = load_rows(a_path), load_rows(b_path)
    if mode == "materialised":
        a_cmp, b_cmp = materialise(a_rows, key_fields), materialise(b_rows, key_fields)
    else:
        a_cmp, b_cmp = sorted(a_rows), sorted(b_rows)

    if len(a_cmp) != len(b_cmp):
        return False, (f"row-count mismatch: {len(a_cmp)} vs {len(b_cmp)} "
                       f"(raw {len(a_rows)} vs {len(b_rows)})")

    samples = []
    for i, (x, y) in enumerate(zip(a_cmp, b_cmp)):
        if x == y:
            continue
        if tol_fields and within_tolerance(x, y, set(tol_fields), epsilon):
            continue
        samples.append(f"row {i}: {x} != {y}")
        if len(samples) == 3:
            break
    if samples:
        return False, f"{len(samples)}+ divergent rows, first: " + " | ".join(samples)
    return True, f"{len(a_cmp)} rows identical" + (
        f" ({len(a_rows)} updates folded)" if mode == "materialised" else "")
