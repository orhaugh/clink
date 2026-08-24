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
               top-N): engines legitimately differ in emit cadence and
               revision count, so each engine's upsert topic is reduced
               to its FINAL STATE by read_upsert_topic.py --json (last
               write per broker key, tombstoned keys removed - the
               reduction both sinks' upsert contract guarantees) and the
               two states are compared BY KEY. Key-paired, never
               position-paired: a canonical row string sorts by its
               alphabetically-first field, so a float wobble in a field
               like avgp would misalign a positional zip.
  tolerance    a per-field annotation on either mode: named float fields
               compare within a declared absolute epsilon (reduction
               order across parallelism makes float sums run-dependent,
               and two engines legitimately render the same IEEE double
               differently). Everything undeclared stays exact.
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


def load_state(path, key_fields):
    """A read_upsert_topic.py --json dump -> {declared key tuple: canonical
    row string}. The reducer already folded the changelog (last write per
    broker key, tombstoned keys removed). The BROKER key is deliberately
    discarded here: the engines encode it differently (one concatenates
    the primary-key columns, the other writes a JSON object), so it can
    never pair rows across engines - the declared key columns, present in
    every value, can. Each surviving row goes through normalise_row so
    integer-valued floats collapse identically on both sides. Raises on a
    dump that carries an error or no state (a failed drain must fail the
    comparison, never read as an empty-but-equal one), on a row missing a
    key column, and on two live rows sharing one declared key (the
    primary key was not a key - a defect, not a tie to shrug at)."""
    with open(path) as fh:
        dump = json.load(fh)
    if "error" in dump:
        raise ValueError(f"upsert drain failed for {dump.get('topic')}: {dump['error']}")
    if "state" not in dump:
        raise ValueError(f"not an upsert state dump: {path}")
    state = {}
    for broker_key, value in dump["state"].items():
        row = normalise_row(value)
        obj = json.loads(row)
        try:
            key = tuple(normalise_value(obj[k]) for k in key_fields)
        except KeyError as e:
            raise ValueError(f"state row lacks declared key field {e}: {row[:80]}") from e
        if key in state and state[key] != row:
            raise ValueError(
                f"two live rows share declared key {key}: {state[key][:60]} vs {row[:60]}")
        state[key] = row
    return state


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
    """Compare two engines' outputs. Returns (equal, detail) where detail
    names counts and up to three sample divergences - a diff nobody can
    see is a diff nobody can diagnose. append: two drained sink files,
    normalised, sorted, multiset-compared. materialised: two upsert state
    dumps, compared by the declared key extracted from each value."""
    if mode == "materialised":
        return compare_states(a_path, b_path, key_fields=key_fields,
                              tol_fields=tol_fields, epsilon=epsilon)

    a_rows, b_rows = sorted(load_rows(a_path)), sorted(load_rows(b_path))
    if len(a_rows) != len(b_rows):
        return False, f"row-count mismatch: {len(a_rows)} vs {len(b_rows)}"

    samples = []
    for i, (x, y) in enumerate(zip(a_rows, b_rows)):
        if x == y:
            continue
        if tol_fields and within_tolerance(x, y, set(tol_fields), epsilon):
            continue
        samples.append(f"row {i}: {x} != {y}")
        if len(samples) == 3:
            break
    if samples:
        return False, f"{len(samples)}+ divergent rows, first: " + " | ".join(samples)
    return True, f"{len(a_rows)} rows identical"


def compare_states(a_path, b_path, *, key_fields, tol_fields=(), epsilon=0.0):
    """Compare two upsert state dumps BY DECLARED KEY - the pairing the
    changelog contract defines, engine-independent (unlike the broker key
    encoding) and immune to the positional misalignment a sorted-string
    zip suffers when a tolerance field sorts early in the canonical row."""
    if not key_fields:
        raise ValueError("materialised comparison requires the declared key fields")
    a_state = load_state(a_path, key_fields)
    b_state = load_state(b_path, key_fields)
    only_a = sorted(set(a_state) - set(b_state))
    only_b = sorted(set(b_state) - set(a_state))
    if only_a or only_b:
        def name(side, keys):
            first = ", ".join(str(k) for k in keys[:3])
            return f"{len(keys)} keys only in {side} (first: {first})"
        parts = [name(s, k) for s, k in (("a", only_a), ("b", only_b)) if k]
        return False, "key-set mismatch: " + "; ".join(parts)

    samples = []
    for key in sorted(a_state):
        x, y = a_state[key], b_state[key]
        if x == y:
            continue
        if tol_fields and within_tolerance(x, y, set(tol_fields), epsilon):
            continue
        samples.append(f"key {key}: {x} != {y}")
        if len(samples) == 3:
            break
    if samples:
        return False, f"{len(samples)}+ divergent keys, first: " + " | ".join(samples)
    return True, f"{len(a_state)} final images identical"
