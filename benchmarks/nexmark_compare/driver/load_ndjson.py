#!/usr/bin/env python3
"""Load the per-type Nexmark NDJSON (from nexmark_dump) into Kafka topics.

One canonical dataset, generated once, written to nx-person / nx-auction / nx-bid
so BOTH engines read identical bytes and neither re-derives the stream (see
pipeline.md). Each line is one JSON object; the message value is the raw JSON line
and the key is a stable per-type field so partition placement is deterministic.

  load_ndjson.py --dir /tmp/nx --bootstrap localhost:9092 --prefix nx-
"""
import argparse
import json
import sys

from confluent_kafka import Producer

# (file stem, topic suffix, key field) per Nexmark type.
TYPES = [
    ("person", "person", "id"),
    ("auction", "auction", "id"),
    ("bid", "bid", "auction"),
]


def load(path, topic, key_field, producer):
    n = 0
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            key = str(json.loads(line).get(key_field, "")).encode()
            # Retry on a full local queue: drain delivery callbacks then re-produce.
            while True:
                try:
                    producer.produce(topic, value=line.encode(), key=key)
                    break
                except BufferError:
                    producer.poll(0.5)
            n += 1
            if n % 10_000 == 0:
                producer.poll(0)
    producer.flush()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="directory of {person,auction,bid}.ndjson")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--prefix", default="nx-", help="topic name prefix")
    args = ap.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 50,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 1_000_000,
            "acks": "1",
        }
    )
    total = 0
    counts = {}
    for stem, suffix, key_field in TYPES:
        path = f"{args.dir}/{stem}.ndjson"
        topic = f"{args.prefix}{suffix}"
        n = load(path, topic, key_field, producer)
        counts[topic] = n
        total += n
        print(f"loaded {n} -> {topic}", file=sys.stderr)
    print(json.dumps({"total": total, "topics": counts}))


if __name__ == "__main__":
    main()
