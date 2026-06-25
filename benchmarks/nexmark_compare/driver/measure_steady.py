#!/usr/bin/env python3
"""Drain an output topic and measure steady-state throughput, engine-agnostically.

Throughput is computed from each record's BROKER append timestamp (the output
topics are created with message.timestamp.type=LogAppendTime), NOT from when this
consumer receives them - so the number is independent of consumer speed and is
measured identically for both engines.

Reports, per the premise (pipeline.md):
  - count + ok (== expected): the correctness gate.
  - steady_eps: records/sec over the middle (1-2*warmup_frac) of the output by
    append time, discarding the warm-up ramp and the end-of-drain tail on BOTH
    engines identically. For q0 (1 output per input) this is input-events/sec;
    for a windowed query it is output-records/sec (only meaningful when windows
    fire mid-stream, i.e. low --tps).
  - cold_eps: count / full append-time span (ramp included), for context.

  measure_steady.py --brokers localhost:9092 --topic nx-out-q0 --expected 460000
"""
import argparse
import json
import time

from confluent_kafka import Consumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--expected", type=int, required=True)
    ap.add_argument("--warmup-frac", type=float, default=0.1)
    ap.add_argument("--quiet-timeout", type=float, default=40.0)
    args = ap.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.brokers,
            "group.id": f"nx-measure-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    ts = []  # broker append timestamps (ms)
    last_seen = time.time()
    try:
        while len(ts) < args.expected:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_seen > args.quiet_timeout:
                    break
                continue
            if msg.error():
                continue
            ts.append(msg.timestamp()[1])
            last_seen = time.time()
    finally:
        consumer.close()

    n = len(ts)
    ts.sort()
    result = {
        "topic": args.topic,
        "count": n,
        "expected": args.expected,
        "ok": n == args.expected,
        "cold_eps": 0.0,
        "steady_eps": 0.0,
        "warmup_frac": args.warmup_frac,
    }
    if n >= 2 and ts[-1] > ts[0]:
        result["cold_eps"] = round(n / ((ts[-1] - ts[0]) / 1000.0), 1)
    if n >= 10:
        lo_i = int(args.warmup_frac * n)
        hi_i = int((1.0 - args.warmup_frac) * n) - 1
        if hi_i > lo_i and ts[hi_i] > ts[lo_i]:
            steady_count = hi_i - lo_i
            result["steady_eps"] = round(steady_count / ((ts[hi_i] - ts[lo_i]) / 1000.0), 1)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
