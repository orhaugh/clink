#!/usr/bin/env python3
"""Independent expected results for the nexmark queries, over a fixed tiny dataset.

WHY THIS EXISTS. The nexmark queries kept regressing, and each regression was
caught late - by a cross-engine run against Flink, or not at all:

  * an unpartitioned aggregate was fanned out and returned one answer PER
    SUBTASK (q7, q15: 396 rows where 99 is correct, each MAX over a quarter of
    the data),
  * a predicate's expression operands were understood by the filter operators
    but not by a CASE's WHEN, so q14 emitted zero rows,
  * a filter threshold excluded every row, so the gate compared 0 against 0 and
    passed.

None of those were caught by the test suite, because the suite had no test that
ran the nexmark queries. This provides the expected output so it can.

WHY A SEPARATE ORACLE. Expectations generated from clink's own output would bake
in whatever it currently does, including the bugs. These are computed here, from
the input data, by plain Python - a second implementation, deliberately naive and
readable, that knows nothing about clink. Where a query's semantics are subtle
(window assignment, session gaps, distinct counting) the arithmetic is written
out rather than delegated, so a disagreement is a disagreement about semantics
that a reader can adjudicate.

Run to regenerate the C++ literals:  python3 tests/data/nexmark_oracle.py
"""

import json
from collections import OrderedDict, defaultdict

# ---------------------------------------------------------------------------
# The dataset. Small enough to reason about by hand, wide enough to exercise
# every operator shape: three tumbling windows, a session gap, duplicate
# (bidder, auction) pairs for dedup, ties for ranking, two categories for the
# join, and one bid whose price falls below q14's threshold.
# ---------------------------------------------------------------------------

WINDOW_MS = 10_000

PERSONS = [
    dict(id=1000, name="alice", emailaddress="a@x", city="york", state="ny", datetime=500),
    dict(id=1001, name="bob", emailaddress="b@x", city="leeds", state="ca", datetime=600),
    dict(id=1002, name="carol", emailaddress="c@x", city="bath", state="wa", datetime=700),
]

AUCTIONS = [
    dict(id=1968, itemname="lamp", initialbid=10, reserve=100, expires=30000,
         seller=1000, category=10, datetime=800),
    dict(id=2001, itemname="desk", initialbid=20, reserve=200, expires=30000,
         seller=1001, category=10, datetime=900),
    dict(id=2091, itemname="rug", initialbid=30, reserve=300, expires=30000,
         seller=1002, category=20, datetime=1000),
]

# 12 bids. datetime drives the window; price drives the filters and ranking.
# Auction ids 1968 and 2091 are multiples of 123, so q2's MOD(auction, 123) = 0
# actually selects rows - an expectation of zero rows would prove nothing.
#   window 0 = [0, 10000)   window 1 = [10000, 20000)   window 2 = [20000, 30000)
BIDS = [
    dict(auction=1968, bidder=1000, price=100, channel="apple", url="https://a/b/c/i.htm", datetime=1000),
    dict(auction=1968, bidder=1001, price=300, channel="google", url="https://a/b/d/i.htm", datetime=2000),
    dict(auction=2001, bidder=1002, price=200, channel="apple", url="https://a/b/c/i.htm", datetime=3000),
    dict(auction=2001, bidder=1000, price=8, channel="baidu", url="https://a/b/e/i.htm", datetime=4001),
    dict(auction=2091, bidder=1001, price=500, channel="facebook", url="https://a/b/f/i.htm", datetime=5000),
    dict(auction=1968, bidder=1000, price=700, channel="apple", url="https://a/b/c/i.htm", datetime=11000),
    dict(auction=1968, bidder=1002, price=400, channel="google", url="https://a/b/d/i.htm", datetime=12000),
    dict(auction=2001, bidder=1001, price=400, channel="apple", url="https://a/b/c/i.htm", datetime=13000),
    dict(auction=2091, bidder=1002, price=900, channel="other", url="https://a/b/g/i.htm", datetime=14001),
    dict(auction=1968, bidder=1001, price=600, channel="apple", url="https://a/b/c/i.htm", datetime=21000),
    dict(auction=2001, bidder=1000, price=250, channel="google", url="https://a/b/d/i.htm", datetime=22000),
    dict(auction=2091, bidder=1000, price=250, channel="apple", url="https://a/b/c/i.htm", datetime=23000),
]


def win(dt):
    return (dt // WINDOW_MS) * WINDOW_MS


# ---------------------------------------------------------------------------
# Per-record queries.
# ---------------------------------------------------------------------------

def q0():
    return [dict(auction=b["auction"], bidder=b["bidder"], price=b["price"],
                 channel=b["channel"], url=b["url"], datetime=b["datetime"]) for b in BIDS]


def q1():
    # 0.908 is an EXACT decimal in clink, so the product is exact: price * 908 / 1000.
    return [dict(auction=b["auction"], bidder=b["bidder"],
                 price=(b["price"] * 908) / 1000.0, datetime=b["datetime"]) for b in BIDS]


def q2():
    return [dict(auction=b["auction"], price=b["price"])
            for b in BIDS if b["auction"] % 123 == 0]


def q14():
    out = []
    for b in BIDS:
        if (b["price"] * 908) / 1000.0 > 250:
            out.append(dict(auction=b["auction"], bidder=b["bidder"],
                            price=(b["price"] * 908) / 1000.0,
                            bidtimetype="even" if b["datetime"] % 2 == 0 else "odd"))
    return out


def q21():
    mapping = {"apple": "0", "google": "1", "facebook": "2", "baidu": "3"}
    return [dict(auction=b["auction"], bidder=b["bidder"], price=b["price"],
                 channel=mapping.get(b["channel"], b["channel"])) for b in BIDS]


def q22():
    # SPLIT_INDEX(url, '/', 3): zero-based, so "https://a/b/c/i.htm" splits to
    # ["https:", "", "a", "b", "c", "i.htm"] and index 3 is "b".
    return [dict(auction=b["auction"], bidder=b["bidder"], price=b["price"],
                 dir=b["url"].split("/")[3]) for b in BIDS]


# ---------------------------------------------------------------------------
# Windowed aggregates. A window is emitted only once the watermark passes its
# end, and the harness drives the watermark to +inf at end of stream, so every
# window that has data is emitted.
# ---------------------------------------------------------------------------

def q7():
    # GLOBAL per window: no grouping column. This is the shape that returned one
    # row per subtask.
    out = []
    for w in sorted({win(b["datetime"]) for b in BIDS}):
        rows = [b for b in BIDS if win(b["datetime"]) == w]
        out.append(dict(price=max(r["price"] for r in rows),
                        bidder=min(r["bidder"] for r in rows)))
    return out


def q12():
    out = []
    groups = defaultdict(int)
    for b in BIDS:
        groups[(win(b["datetime"]), b["bidder"])] += 1
    for (w, bidder), n in sorted(groups.items()):
        out.append(dict(bidder=bidder, bid_count=n))
    return out


def q15():
    out = []
    for w in sorted({win(b["datetime"]) for b in BIDS}):
        rows = [b for b in BIDS if win(b["datetime"]) == w]
        out.append(dict(total=len(rows),
                        distinct_bidder=len({r["bidder"] for r in rows}),
                        distinct_auction=len({r["auction"] for r in rows})))
    return out


def q16():
    out = []
    groups = defaultdict(list)
    for b in BIDS:
        groups[(win(b["datetime"]), b["channel"])].append(b)
    for (w, ch), rows in sorted(groups.items()):
        out.append(dict(channel=ch, total=len(rows),
                        distinct_bidder=len({r["bidder"] for r in rows})))
    return out


def q17():
    out = []
    groups = defaultdict(list)
    for b in BIDS:
        groups[(win(b["datetime"]), b["auction"])].append(b)
    for (w, auc), rows in sorted(groups.items()):
        prices = [r["price"] for r in rows]
        out.append(dict(auction=auc, total=len(rows), minp=min(prices), maxp=max(prices),
                        avgp=sum(prices) / len(prices)))
    return out


def q11():
    # SESSION per bidder with a 10s gap: a new session starts when the gap since
    # the previous bid by that bidder is >= 10s.
    out = []
    by_bidder = defaultdict(list)
    for b in BIDS:
        by_bidder[b["bidder"]].append(b["datetime"])
    for bidder in sorted(by_bidder):
        times = sorted(by_bidder[bidder])
        count = 1
        for prev, cur in zip(times, times[1:]):
            if cur - prev >= WINDOW_MS:
                out.append(dict(bidder=bidder, bid_count=count))
                count = 1
            else:
                count += 1
        out.append(dict(bidder=bidder, bid_count=count))
    return out


# ---------------------------------------------------------------------------
# Joins. Both sides are bounded here, so the join is the full inner product on
# the key.
# ---------------------------------------------------------------------------

def q3():
    out = []
    for a in AUCTIONS:
        if a["category"] != 10:
            continue
        for p in PERSONS:
            if p["id"] == a["seller"]:
                out.append(dict(name=p["name"], city=p["city"], state=p["state"], id=a["id"]))
    return out


def q20():
    out = []
    for b in BIDS:
        for a in AUCTIONS:
            if a["id"] == b["auction"]:
                out.append(dict(auction=b["auction"], bidder=b["bidder"], price=b["price"],
                                itemname=a["itemname"]))
    return out


def q4():
    # AVG(price) per category over auction JOIN bid. A non-windowed aggregate, so
    # it emits a changelog; the expected value is the FINAL state per category.
    sums = defaultdict(list)
    for b in BIDS:
        for a in AUCTIONS:
            if a["id"] == b["auction"]:
                sums[a["category"]].append(b["price"])
    return [dict(category=c, avgp=sum(v) / len(v)) for c, v in sorted(sums.items())]


# ---------------------------------------------------------------------------
# Changelog queries: the expected value is the FINAL retained state, since the
# stream revises rows it already emitted.
# ---------------------------------------------------------------------------

def q18():
    # Latest bid per (bidder, auction) by datetime DESC, keeping one row.
    latest = {}
    for b in BIDS:
        k = (b["bidder"], b["auction"])
        if k not in latest or b["datetime"] > latest[k]["datetime"]:
            latest[k] = b
    return [dict(auction=b["auction"], bidder=b["bidder"], price=b["price"],
                 channel=b["channel"], url=b["url"], datetime=b["datetime"])
            for b in sorted(latest.values(), key=lambda r: (r["bidder"], r["auction"]))]


def q19():
    # Top 10 bids per auction by price DESC. Every auction here has fewer than 10
    # bids, so the final state is every bid, grouped by auction.
    out = []
    by_auction = defaultdict(list)
    for b in BIDS:
        by_auction[b["auction"]].append(b)
    for auc in sorted(by_auction):
        rows = sorted(by_auction[auc], key=lambda r: -r["price"])[:10]
        for r in rows:
            out.append(dict(auction=r["auction"], bidder=r["bidder"], price=r["price"],
                            channel=r["channel"], url=r["url"], datetime=r["datetime"]))
    return out


def q5():
    # Hot item: the single most-bid-on auction per 10s window sliding every 2s.
    # A bid at t belongs to every window [s, s+10000) with s = t - t%2000 - 2000*k
    # for k in 0..4 that contains t. Changelog; the expected value is the final
    # top-1 per window start.
    #
    # There is NO "s >= 0" filter, and there was one here until 2026-07-28. It was
    # copied from the engine, which clamped a hop's first pane to the epoch - so on
    # this one point the oracle was not independent, it was a restatement of the
    # code, and it agreed with a defect instead of catching it. A 10s window sliding
    # 2s has panes starting at -8000, -6000, -4000, -2000 and 0; a bid at t=1000
    # falls inside all five, and every one of them is a window of this query.
    per_window = defaultdict(lambda: defaultdict(int))
    for b in BIDS:
        t = b["datetime"]
        first = (t // 2000) * 2000 - (WINDOW_MS - 2000)
        for k in range(5):
            s = first + 2000 * k
            if s <= t < s + WINDOW_MS:
                per_window[s][b["auction"]] += 1
    out = []
    for s in sorted(per_window):
        counts = per_window[s]
        best = max(counts.items(), key=lambda kv: (kv[1], -kv[0]))
        out.append(dict(wstart=s, auction=best[0], num=best[1]))
    return out


def cumulate():
    # NOT a nexmark query. CUMULATE is the one window kind nexmark never exercises,
    # so without this it is the only kind with no end-to-end coverage against an
    # independent expectation - which is exactly how the hopping window shipped
    # untested.
    #
    # Cumulative count per bidder over a 10s window stepping 2s. Each pane shares
    # the window's start and ends at a successive step, so a bid contributes to
    # every pane of its window that ends after it - which means the pane count
    # depends on where in the window the bid falls, unlike TUMBLE or a divisible HOP.
    step = 2000
    panes = defaultdict(int)
    for b in BIDS:
        t = b["datetime"]
        anchor = (t // WINDOW_MS) * WINDOW_MS
        end = anchor + step
        while end <= anchor + WINDOW_MS:
            if t < end:
                panes[(anchor, end, b["bidder"])] += 1
            end += step
    return [dict(wstart=ws, wend=we, bidder=bidder, num=n)
            for (ws, we, bidder), n in sorted(panes.items())]


QUERIES = OrderedDict([
    ("q0", q0), ("q1", q1), ("q2", q2), ("q3", q3), ("q4", q4), ("q5", q5),
    ("q7", q7), ("q11", q11), ("q12", q12), ("q14", q14), ("q15", q15),
    ("q16", q16), ("q17", q17), ("q18", q18), ("q19", q19), ("q20", q20),
    ("q21", q21), ("q22", q22), ("cumulate", cumulate),
])


def _norm(v):
    """An integral float renders as an integer, matching how the engine's JSON
    output is canonicalised on the C++ side. Without this, 227.0 and 227 compare
    unequal for reasons that have nothing to do with the query."""
    if isinstance(v, float) and v == int(v):
        return int(v)
    return v


def cxx_lines(name, rows):
    """Render as a C++ initializer of JSON strings, sorted for a stable literal."""
    ser = sorted(
        json.dumps({k: _norm(v) for k, v in r.items()}, sort_keys=True, separators=(",", ":"))
        for r in rows)
    body = ",\n         ".join('R"(' + s + ')"' for s in ser) if ser else ""
    return f'    {{"{name}",\n        {{{body}}}}},'


def main():
    print("// GENERATED by tests/data/nexmark_oracle.py - do not edit by hand.")
    print("// Regenerate:  python3 tests/data/nexmark_oracle.py > "
          "tests/data/nexmark_expected.inc")
    print("//")
    print("// Expected output per nexmark query over the fixed dataset in that script,")
    print("// computed independently of clink so a disagreement is a real disagreement.")
    print("// For changelog queries (q4, q5, q18, q19) this is the FINAL retained state.")
    print()
    print("// clang-format off")
    print("inline const std::map<std::string, std::vector<std::string>>& nexmark_expected() {")
    print("    static const std::map<std::string, std::vector<std::string>> kExpected = {")
    for name, fn in QUERIES.items():
        print(cxx_lines(name, fn()))
    print("    };")
    print("    return kExpected;")
    print("}")
    print("// clang-format on")


if __name__ == "__main__":
    main()
