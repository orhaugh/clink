#!/usr/bin/env python3
"""The example job's expected result, computed without clink.

Reads nothing the job wrote: it enumerates the feed's records from
feed.record(), applies the job's filter and its one-second tumbling windows by
hand, and prints one JSON object per window and user. --shape=sink prints the
rows the job's sink table holds; --shape=window prints the rows the window
operator emits, which also carry window_end, for clink incident-verify
--expected.

Usage: oracle.py --records=N [--shape=sink|window]
"""

import argparse
import json
from collections import defaultdict

from feed import record


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, required=True)
    ap.add_argument("--shape", choices=["sink", "window"], default="sink")
    args = ap.parse_args()
    totals = defaultdict(lambda: [0, 0])
    for seq in range(1, args.records + 2):
        r = record(seq, args.records)
        if r["amount"] <= 3:
            continue
        start = r["ts"] // 1000 * 1000
        t = totals[(r["usr"], start)]
        t[0] += r["amount"]
        t[1] += 1
    for (usr, start), (total, n) in sorted(totals.items()):
        row = {"usr": usr, "window_start": start, "total": total, "n": n}
        if args.shape == "window":
            row["window_end"] = start + 1000
        print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
