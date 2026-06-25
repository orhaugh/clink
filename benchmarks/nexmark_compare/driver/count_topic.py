#!/usr/bin/env python3
"""Count records on a Kafka topic until an expected total (or a quiet timeout).

Used to drain a query's output topic and time it. Reports the count, whether it
reached the expected total, and the first-record-to-last-record wall (a coarse
drain time; the steady-state measurement is layered on in a later increment).

  count_topic.py --brokers localhost:9092 --topic nx-out-q0 --expected 460000
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
    ap.add_argument("--quiet-timeout", type=float, default=30.0, help="stop after N s with no new records")
    args = ap.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.brokers,
            "group.id": f"nx-counter-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    n = 0
    first = None
    last_seen = time.time()
    try:
        while n < args.expected:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_seen > args.quiet_timeout:
                    break
                continue
            if msg.error():
                continue
            if first is None:
                first = time.time()
            n += 1
            last_seen = time.time()
    finally:
        consumer.close()
    elapsed = (last_seen - first) if first else 0.0
    print(
        json.dumps(
            {
                "topic": args.topic,
                "count": n,
                "expected": args.expected,
                "ok": n == args.expected,
                "first_to_last_s": round(elapsed, 3),
            }
        )
    )


if __name__ == "__main__":
    main()
