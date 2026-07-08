#!/usr/bin/env python3
"""Paced replay of a pre-generated NDJSON file into one Kafka topic.

The latency axis (pipeline.md, "Latency axis") needs the input to ARRIVE at a
sustained wall-clock rate while the engine is running, unlike the throughput
axis which preloads the topic before either engine starts. This replays the
deterministic bid.ndjson at --rate events/s: same file, same order, same
target rate for both engines, so each engine meets an identical paced stream.

Records are produced without a key: latency v1 is single-partition by premise,
so partition placement does not apply. The producer's own linger (2ms) shapes
arrival identically for both engines and cannot affect the measurement -
input timestamps are broker LogAppendTime, stamped on arrival.

  pace_ndjson.py --file /tmp/nx/bid.ndjson --topic nx-bid-lat \
      --bootstrap localhost:9092 --rate 50000 --report pace.json
"""
import argparse
import json
import time

from confluent_kafka import Producer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, required=True, help="target events/s")
    ap.add_argument("--limit", type=int, default=0, help="stop after N lines (0 = whole file)")
    ap.add_argument("--report", default="", help="write a JSON pacing report here")
    args = ap.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 2,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 1_000_000,
            "acks": "1",
        }
    )

    tick_s = 0.005  # 5ms pacing tick
    # Cap catch-up bursts (e.g. after this process is descheduled) at 4 ticks'
    # quota so a stall never turns into one giant burst.
    max_burst = max(1, int(args.rate * tick_s * 4))
    sent = 0
    t0 = time.monotonic()
    with open(args.file) as f:
        eof = False
        while not eof:
            target = int((time.monotonic() - t0) * args.rate)
            quota = min(target - sent, max_burst)
            for _ in range(max(0, quota)):
                if args.limit and sent >= args.limit:
                    eof = True
                    break
                line = f.readline()
                if not line:
                    eof = True
                    break
                line = line.rstrip("\n")
                if not line:
                    continue
                # Retry on a full local queue: drain delivery callbacks then
                # re-produce (same discipline as load_ndjson.py).
                while True:
                    try:
                        producer.produce(args.topic, value=line.encode())
                        break
                    except BufferError:
                        producer.poll(0.1)
                sent += 1
            producer.poll(0)
            if not eof:
                time.sleep(tick_s)
    producer.flush()
    wall = time.monotonic() - t0
    report = {
        "sent": sent,
        "wall_s": round(wall, 3),
        "achieved_rate": round(sent / wall, 1) if wall > 0 else 0.0,
        "target_rate": args.rate,
    }
    print(json.dumps(report))
    if args.report:
        with open(args.report, "w") as fh:
            json.dump(report, fh)


if __name__ == "__main__":
    main()
