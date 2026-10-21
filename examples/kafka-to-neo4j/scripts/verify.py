#!/usr/bin/env python3
"""Check the graph against the workload, without trusting the pipeline.

    ./scripts/verify.py              # check once, exit 0 when the graph is exact
    ./scripts/verify.py --wait 300   # keep checking for up to 300 s

What must hold, recomputed from scripts/workload.py:
  * every account once;
  * every transfer once, with its amount and time, exactly one SENT from its
    sender and exactly one TO its recipient;
  * exactly the fan-in signals the one-minute windows imply, each with its
    window, its counts, its supporting event ids and its rule version, and
    exactly one FLAGS to its account; no other signal.

A replay after the Worker kill writes some batches twice. The pipeline's
statements are keyed by stable ids under uniqueness constraints, so the graph
must still come out exact; a duplicate here is a failure.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cypher  # noqa: E402
import workload  # noqa: E402


def problems() -> list:
    out = []
    stream = workload.transfers()
    want_accounts = sorted({t["sender"] for t in stream} | {t["recipient"] for t in stream})
    got_accounts = [r[0] for r in cypher.run("MATCH (a:Account) RETURN a.id ORDER BY a.id")]
    if got_accounts != want_accounts:
        dupes = sorted({a for a in got_accounts if got_accounts.count(a) > 1})
        out.append(f"accounts: {len(got_accounts)} in the graph, {len(want_accounts)} expected"
                   + (f"; duplicated: {dupes[:5]}" if dupes else ""))

    rows = cypher.run(
        "MATCH (t:Transfer) OPTIONAL MATCH (s)-[sent:SENT]->(t) OPTIONAL MATCH (t)-[to:TO]->(r) "
        "RETURN t.event_id, t.amount, t.at, collect(s.id), collect(r.id), "
        "count(DISTINCT sent), count(DISTINCT to) ORDER BY t.event_id")
    got = {r[0]: r for r in rows}
    if len(rows) != len(got):
        out.append("transfers: an event id appears on more than one node")
    missing = [t["event_id"] for t in stream if t["event_id"] not in got]
    if missing:
        out.append(f"transfers: {len(missing)} of {len(stream)} missing (first: {missing[:5]})")
    wrong = []
    for t in stream:
        r = got.get(t["event_id"])
        if r is None:
            continue
        ok = (r[1] == t["amount"] and r[2] == t["at"] and r[3] == [t["sender"]] and r[4] == [t["recipient"]]
              and r[5] == 1 and r[6] == 1)
        if not ok:
            wrong.append(t["event_id"])
    if wrong:
        out.append(f"transfers: {len(wrong)} differ from the events produced (first: {wrong[:5]})")
    extra = sorted(set(got) - {t["event_id"] for t in stream})
    if extra:
        out.append(f"transfers: {len(extra)} that were never produced (first: {extra[:5]})")

    want = workload.expected_signals(stream)
    sig_rows = cypher.run(
        "MATCH (s:Signal) OPTIONAL MATCH (s)-[f:FLAGS]->(a:Account) "
        "RETURN s.id, s.account, s.window_start, s.window_end, s.distinct_senders, s.transfers, "
        "s.event_ids, s.rule_version, collect(a.id), count(f) ORDER BY s.id")
    seen = set()
    for r in sig_rows:
        sid = r[0]
        if sid in seen:
            out.append(f"signals: {sid} appears twice")
        seen.add(sid)
        w = want.get(sid)
        if w is None:
            out.append(f"signals: {sid} was raised but the windows do not imply it")
            continue
        ok = (r[1] == w["account"] and r[2] == w["window_start"] and r[3] == w["window_end"]
              and r[4] == w["distinct_senders"] and r[5] == w["transfers"] and r[6] == w["event_ids"]
              and r[7] == workload.RULE_VERSION and r[8] == [w["account"]] and r[9] == 1)
        if not ok:
            out.append(f"signals: {sid} differs from what its window implies")
    for sid in sorted(set(want) - seen):
        out.append(f"signals: {sid} is missing")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--wait", type=float, default=0.0,
                    help="keep checking for this many seconds before giving up")
    args = ap.parse_args()
    deadline = time.monotonic() + args.wait
    while True:
        try:
            found = problems()
        except Exception as e:  # the server may still be starting
            found = [f"could not query Neo4j: {e}"]
        if not found:
            stream = workload.transfers()
            print(f"verify: OK. {len(stream)} transfers, "
                  f"{len({t['sender'] for t in stream} | {t['recipient'] for t in stream})} accounts and "
                  f"{len(workload.expected_signals(stream))} signals, each exactly once, "
                  "every relationship and property as the events imply")
            return 0
        if time.monotonic() >= deadline:
            print("verify: FAILED")
            for p in found:
                print(f"  - {p}")
            return 1
        time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
