#!/usr/bin/env python3
"""QUAL-11 gate 4: the migration's effect, read from the STATE ITSELF.

Why not from the output stream. The obvious signal - the first
post-boundary row for a key shows the migrated range fields collapsing
onto that row's own amount - rides a Kafka sink that is at-least-once
and buffers. QUAL-11's first working run proved that unreliable: the job
restored correctly (counts continued from where the savepoint left off)
but a worker kill discarded the sink's buffer before it flushed, so the
earliest SURVIVING rows began after a restart and already carried a
folded range. The evidence was destroyed by an unrelated, legitimate
fault, and a gate that can be erased that way measures nothing.

So this reads the savepoints. Both sides are dumped with
`clink state-cat --json`, which renders each keyed entry's raw value
(hex for binary), and the comparison is arithmetic on those bytes:

  v1 value: [count i64][sum i64]                      (16 bytes)
  v2 value: [count i64][sum i64][vmin i64][vmax i64]  (32 bytes)

For every key present in the v1 savepoint:

  * CARRIED   - the v2 entry exists, is 32 bytes, and its count is at
                least the v1 count (the job may have folded more events
                between the restore and the second savepoint). A missing
                key, a 16-byte value, or a count that went BACKWARDS is
                state loss.
  * PREDICTED - for a key the job has not touched since the restore
                (v2 count == v1 count), the migrated fields must be
                EXACTLY the empty-range sentinels the registered
                migration writes: vmin == INT64_MAX, vmax == INT64_MIN,
                with count and sum carried byte-for-byte. That is the
                migration's pure output, checked without asking the
                engine what it thinks it did.

A key touched since the restore cannot show the sentinels any more (the
fold overwrote them), so it counts toward CARRIED but not toward
PREDICTED. If NO key is untouched, the predicted-output half has no
evidence and says so - it never passes by default.

Usage:
  migration_effect.py --v1-dump v1.json --v2-dump v2.json --out effect.json
"""
import argparse
import json
import sys

SENTINEL_MIN = 2**63 - 1
SENTINEL_MAX = -(2**63)


def i64(b, off):
    v = int.from_bytes(b[off:off + 8], "little", signed=True)
    return v


def parse_value(rendered):
    """state-cat renders a value as 0x<hex> (binary) or "..." (printable).
    Returns raw bytes, or None if it is not the hex form this state uses."""
    if not rendered.startswith("0x"):
        return None
    try:
        return bytes.fromhex(rendered[2:])
    except ValueError:
        return None


def load_entries(path, slot_filter="account_state"):
    """{key bytes hex -> value bytes} for one slot across every operator."""
    with open(path) as fh:
        dump = json.load(fh)
    out = {}
    for op in dump.get("operators", []):
        for slot in op.get("slots", []):
            if slot.get("slot") != slot_filter:
                continue
            for e in slot.get("entries", []):
                val = parse_value(e.get("value", ""))
                if val is None:
                    continue
                out[e.get("key", "")] = val
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v1-dump", required=True)
    ap.add_argument("--v2-dump", required=True)
    ap.add_argument("--slot", default="account_state")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    result = {"slot": args.slot}
    try:
        v1 = load_entries(args.v1_dump, args.slot)
        v2 = load_entries(args.v2_dump, args.slot)
    except Exception as e:  # noqa: BLE001
        result["error"] = str(e)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=1)
        print(json.dumps(result))
        return 1

    carried = lost = 0
    predicted_ok = predicted_bad = untouched = 0
    samples = []
    for key, b1 in v1.items():
        if len(b1) < 16:
            continue
        c1, s1 = i64(b1, 0), i64(b1, 8)
        b2 = v2.get(key)
        if b2 is None or len(b2) != 32:
            lost += 1
            if len(samples) < 5:
                samples.append({"key": key, "why": "no 32-byte v2 entry for a key the "
                                                   "savepoint held", "v1_count": c1})
            continue
        c2, s2 = i64(b2, 0), i64(b2, 8)
        if c2 < c1 or s2 < s1:
            lost += 1
            if len(samples) < 5:
                samples.append({"key": key, "v1_count": c1, "v2_count": c2,
                                "why": "count or sum went BACKWARDS across the migration"})
            continue
        carried += 1
        if c2 == c1:
            # Untouched since the restore: the migration's own output is
            # still intact and must be exactly what its pure function
            # writes.
            untouched += 1
            vmin, vmax = i64(b2, 16), i64(b2, 24)
            if s2 == s1 and vmin == SENTINEL_MIN and vmax == SENTINEL_MAX:
                predicted_ok += 1
            else:
                predicted_bad += 1
                if len(samples) < 5:
                    samples.append({"key": key, "vmin": vmin, "vmax": vmax,
                                    "why": "an untouched key's migrated fields are not the "
                                           "sentinels the migration writes"})

    result.update(v1_keys=len(v1), v2_keys=len(v2), carried=carried, lost=lost,
                  untouched=untouched, predicted_ok=predicted_ok,
                  predicted_bad=predicted_bad, samples=samples)
    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=1)
    print(json.dumps({k: v for k, v in result.items() if k != "samples"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
