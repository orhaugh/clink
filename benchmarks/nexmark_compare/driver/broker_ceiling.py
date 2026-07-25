#!/usr/bin/env python3
"""Measure how fast the Kafka broker can SERVE the benchmark input.

Why this exists. Both engines read the same single unlimited broker container,
whose CPU is charged to neither engine's account. Three of the four drain rates
recorded before this control existed clustered in 718k-851k rec/s across two
engines and two queries, while neither engine used more than about a third of a
12-vCPU box. That is the signature of a shared input ceiling rather than of two
engines that happen to perform alike.

If the broker serves at roughly the rate the engines drain at, then the benchmark
is measuring Kafka and every engine-versus-engine throughput ratio taken from it
is meaningless - no amount of engine optimisation moves a number set upstream of
the engine. Printing this as a control row alongside the engines is what stops
that mistake being made silently.

Deliberately a floor, not a ceiling, in one respect: this consumes with a single
consumer and does nothing per record, so a well-parallelised engine reading the
same topic across N partitions may legitimately exceed it. Read it as "the broker
serves AT LEAST this fast"; an engine drain rate close to it is the warning sign.

  broker_ceiling.py --brokers localhost:9092 --topic nx-bid [--max-records N]
      -> JSON {records, seconds, rate, bytes, mb_per_s}
"""
import argparse
import json
import sys
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9092")
    ap.add_argument("--topic", default="nx-bid")
    ap.add_argument("--max-records", type=int, default=0, help="0 = drain to end of topic")
    ap.add_argument("--timeout-s", type=float, default=120.0)
    a = ap.parse_args()

    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError:
        print(json.dumps({"error": "confluent_kafka not installed",
                          "hint": "pip install confluent-kafka, or run this inside the "
                                  "kafka container with kafka-consumer-perf-test"}))
        return 3

    c = Consumer({
        "bootstrap.servers": a.brokers,
        "group.id": f"broker-ceiling-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # Big fetches: measuring the broker's serve rate, not the client's
        # round-trip behaviour under a conservative default.
        "fetch.min.bytes": 1048576,
        "fetch.wait.max.ms": 100,
        "queued.max.messages.kbytes": 1048576,
    })

    md = c.list_topics(a.topic, timeout=20)
    if a.topic not in md.topics or md.topics[a.topic].error is not None:
        print(json.dumps({"error": f"topic {a.topic} not available"}))
        return 4
    parts = list(md.topics[a.topic].partitions.keys())

    # End offsets first, so completion is decided out of band rather than by a
    # quiet period - the same reason the engines should not be scored on their
    # own counters going quiet.
    total_available = 0
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(a.topic, p), timeout=20)
        total_available += max(0, hi - lo)
    target = min(total_available, a.max_records) if a.max_records else total_available

    c.assign([TopicPartition(a.topic, p) for p in parts])

    n = 0
    nbytes = 0
    t0 = None
    deadline = time.time() + a.timeout_s
    while n < target and time.time() < deadline:
        msgs = c.consume(num_messages=10000, timeout=1.0)
        if not msgs:
            continue
        if t0 is None:
            t0 = time.time()  # first byte: excludes assignment and metadata
        for m in msgs:
            if m.error():
                continue
            n += 1
            v = m.value()
            if v:
                nbytes += len(v)
    dt = max(time.time() - (t0 or time.time()), 1e-9)
    c.close()

    print(json.dumps({
        "records": n,
        "available": total_available,
        "seconds": round(dt, 3),
        "rate": round(n / dt, 1),
        "bytes": nbytes,
        "mb_per_s": round(nbytes / dt / 1048576, 1),
        "partitions": len(parts),
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
