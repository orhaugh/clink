#!/usr/bin/env python3
"""Per-record end-to-end latency for the latency axis (pipeline.md).

latency[i] = broker append time of OUTPUT record i minus broker append time of
INPUT record i. Both topics are 1-partition LogAppendTime on the same broker,
so both timestamps come from one clock and the resolution is Kafka's
millisecond granularity. The positional join is only valid for an
order-preserving single-partition pass-through; that is not assumed - every
position's content (auction, bidder, price, datetime) is compared between the
input and output records, and any mismatch fails the run.

Gates (each failure -> ok=false, no quotable number):
  - count:   output count == expected input count
  - content: zero positional content mismatches
  - pace:    achieved input rate within 5% of --rate

Stats are computed over the steady window of positions
[head_trim*N, (1-tail_trim)*N); a per-5s-bucket p50/p99 series over the WHOLE
run is recorded so trim adequacy is inspectable rather than trusted.

The output topic is drained FIRST (it can be consumed while the engine is
still producing); the input topic is drained after the pacer finishes.
"""
import argparse
import json
import time

from confluent_kafka import Consumer


def to_i(v):
    if isinstance(v, bool):
        raise ValueError("bool is not an int field")
    if isinstance(v, int):
        return v
    return int(float(v))


def content_key(obj):
    # Hashed content tuple: positional equality is what the gate checks, and a
    # 64-bit hash keeps 3.7M-record runs in memory (a collision could only
    # mask a mismatch, never invent one, and is negligible at this scale).
    return hash((to_i(obj["auction"]), to_i(obj["bidder"]), to_i(obj["price"]),
                 to_i(obj["datetime"])))


def drain(brokers, topic, expected, quiet_timeout):
    """Consume `topic` until `expected` records (or quiet timeout). Returns
    (broker_append_ts_ms[], content_key[]) in partition order."""
    c = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"nx-lat-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    ts, keys = [], []
    last_seen = time.time()
    try:
        while len(ts) < expected:
            msgs = c.consume(num_messages=10000, timeout=1.0)
            if not msgs:
                if time.time() - last_seen > quiet_timeout:
                    break
                continue
            last_seen = time.time()
            for m in msgs:
                if m.error():
                    continue
                ts.append(m.timestamp()[1])
                keys.append(content_key(json.loads(m.value())))
    finally:
        c.close()
    return ts, keys


def pct(sorted_win, q):
    if not sorted_win:
        return None
    i = min(len(sorted_win) - 1, int(q * len(sorted_win)))
    return sorted_win[i]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9092")
    ap.add_argument("--in-topic", required=True)
    ap.add_argument("--out-topic", required=True)
    ap.add_argument("--expected", type=int, required=True)
    ap.add_argument("--rate", type=float, required=True, help="target input events/s (pace gate)")
    ap.add_argument("--head-trim", type=float, default=0.20)
    ap.add_argument("--tail-trim", type=float, default=0.05)
    ap.add_argument("--quiet-timeout", type=float, default=30.0)
    ap.add_argument("--query", default="q0_lat")
    ap.add_argument("--engine", default="")
    ap.add_argument("--out", default="", help="also write the result JSON here")
    args = ap.parse_args()

    # Output first: this runs while the engine is still producing and returns
    # at (or quiet-timeout after) the last output record.
    out_ts, out_keys = drain(args.brokers, args.out_topic, args.expected, args.quiet_timeout)
    # Input after: the pacer has finished by the time the output drained.
    in_ts, in_keys = drain(args.brokers, args.in_topic, args.expected, args.quiet_timeout)

    n = min(len(in_ts), len(out_ts))
    mismatches = sum(1 for i in range(n) if in_keys[i] != out_keys[i])
    ok_count = len(out_ts) == args.expected and len(in_ts) == args.expected
    ok_content = mismatches == 0 and n == args.expected

    # Achieved input rate over the middle of the paced stream (the pacer's
    # first/last moments include producer spin-up/flush).
    achieved = 0.0
    if len(in_ts) >= 10:
        lo, hi = int(0.05 * len(in_ts)), int(0.95 * len(in_ts)) - 1
        if in_ts[hi] > in_ts[lo]:
            achieved = (hi - lo) / ((in_ts[hi] - in_ts[lo]) / 1000.0)
    ok_pace = abs(achieved - args.rate) <= 0.05 * args.rate

    result = {
        "query": args.query,
        "engine": args.engine,
        "in_topic": args.in_topic,
        "out_topic": args.out_topic,
        "count": len(out_ts),
        "expected": args.expected,
        "mismatches": mismatches,
        "achieved_in_rate": round(achieved, 1),
        "target_rate": args.rate,
        "ok_count": ok_count,
        "ok_content": ok_content,
        "ok_pace": ok_pace,
        "ok": ok_count and ok_content and ok_pace,
        "head_trim": args.head_trim,
        "tail_trim": args.tail_trim,
    }

    if n > 0:
        lat = [out_ts[i] - in_ts[i] for i in range(n)]
        lo = int(args.head_trim * n)
        hi = int((1.0 - args.tail_trim) * n)
        win = sorted(lat[lo:hi])
        result.update(
            {
                "n_steady": len(win),
                "p50_ms": pct(win, 0.50),
                "p90_ms": pct(win, 0.90),
                "p99_ms": pct(win, 0.99),
                "p999_ms": pct(win, 0.999),
                "max_ms": win[-1] if win else None,
                "mean_ms": round(sum(win) / len(win), 2) if win else None,
            }
        )
        # Whole-run 5s-bucket series (by input append time) for trim
        # inspection: shows where warm-up ends without trusting the trim.
        buckets = {}
        t_base = in_ts[0]
        for i in range(n):
            buckets.setdefault((in_ts[i] - t_base) // 5000, []).append(lat[i])
        series = []
        for b in sorted(buckets):
            w = sorted(buckets[b])
            series.append(
                {"t_s": int(b) * 5, "n": len(w), "p50_ms": pct(w, 0.50), "p99_ms": pct(w, 0.99)}
            )
        result["buckets_5s"] = series

    print(
        json.dumps(
            {
                k: result.get(k)
                for k in (
                    "engine",
                    "count",
                    "expected",
                    "mismatches",
                    "achieved_in_rate",
                    "p50_ms",
                    "p99_ms",
                    "p999_ms",
                    "max_ms",
                    "ok",
                )
            }
        )
    )
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(result, fh)


if __name__ == "__main__":
    main()
