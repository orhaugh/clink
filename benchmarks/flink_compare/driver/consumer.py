#!/usr/bin/env python3
"""Consume the engine's output topic and measure wall-time + record count.

Two metrics:
  - wall_seconds: monotonic-clock window from first received output
    record to last received output record.
  - record_count: total output records observed.

Output topic carries 24-byte little-endian (window_end_ms, key, sum)
triples; one record per (key, window) pair. Expected count =
n_keys * n_windows.
"""

import argparse
import json
import os
import struct
import sys
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_output_topic(brokers: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": brokers})
    md = admin.list_topics(timeout=10)
    if topic in md.topics:
        fut = admin.delete_topics([topic], operation_timeout=30)
        for _, f in fut.items():
            try:
                f.result()
            except Exception as e:
                print(f"warning: delete_topics({topic}): {e}", file=sys.stderr)
        for _ in range(30):
            md = admin.list_topics(timeout=5)
            if topic not in md.topics:
                break
            time.sleep(1)
    # The broker controller may still be cleaning up the deleted topic
    # when list_topics already stops reporting it. create_topics then
    # fails with TOPIC_ALREADY_EXISTS until the cleanup completes. Retry
    # until create succeeds or we hit a hard ceiling.
    for attempt in range(60):
        fut = admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)],
            operation_timeout=30,
        )
        try:
            for _, f in fut.items():
                f.result()
            return
        except Exception as e:
            msg = str(e)
            if "TOPIC_ALREADY_EXISTS" in msg or "marked for deletion" in msg:
                time.sleep(1)
                continue
            raise
    raise RuntimeError(f"create_topics({topic}) kept failing with TOPIC_ALREADY_EXISTS")


def consume(brokers: str, topic: str, expected: int, timeout_s: float, engine: str,
            engine_start_epoch: float | None) -> dict:
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"bench-consumer-{engine}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 50,
    })
    consumer.subscribe([topic])

    count = 0
    first_epoch = None
    last_epoch = None
    deadline = time.monotonic() + timeout_s
    quiet_deadline = None
    while count < expected and time.monotonic() < deadline:
        msgs = consumer.consume(num_messages=10_000, timeout=1.0)
        if not msgs:
            if first_epoch is not None and quiet_deadline is None:
                quiet_deadline = time.monotonic() + 60
            elif quiet_deadline is not None and time.monotonic() > quiet_deadline:
                print(
                    f"consumer: 60s quiet after {count} records; stopping early",
                    file=sys.stderr,
                )
                break
            continue
        quiet_deadline = None
        now = time.time()
        if first_epoch is None:
            first_epoch = now
        last_epoch = now
        for m in msgs:
            if m.error():
                continue
            if len(m.value()) >= 24:
                count += 1
    consumer.close()

    # wall_seconds preferentially measured from engine-start (captured by
    # the harness right before the engine job was submitted) to the time
    # the last output record landed on the broker. Falls back to the
    # consumer-window if engine_start_epoch wasn't provided. The
    # engine-start anchor includes job-submit + cluster bring-up + the
    # full data scan; the consumer-window only captures the tail emit
    # burst, so the two metrics can differ by 1-3 orders of magnitude
    # for ProcessWindow-style jobs that batch their emits.
    if engine_start_epoch is not None and last_epoch is not None:
        wall = max(0.0, last_epoch - engine_start_epoch)
    elif first_epoch is not None and last_epoch is not None:
        wall = last_epoch - first_epoch
    else:
        wall = 0.0
    return {
        "engine": engine,
        "record_count": count,
        "expected_count": expected,
        "wall_seconds": wall,
        "throughput_rec_per_s": (count / wall) if wall > 0 else 0,
        "engine_start_epoch": engine_start_epoch,
        "first_record_epoch": first_epoch,
        "last_record_epoch": last_epoch,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default=os.environ.get("BENCH_KAFKA_BROKERS", "localhost:9092"))
    p.add_argument("--topic", default=os.environ.get("BENCH_OUTPUT_TOPIC", "bench-out"))
    p.add_argument("--engine", required=True, choices=["flink", "clink"])
    p.add_argument("--expected", type=int, default=100_000,
                   help="n_keys * n_windows; default matches 1000 keys * 100 windows")
    p.add_argument("--timeout-seconds", type=float, default=600.0)
    p.add_argument("--partitions", type=int, default=4)
    p.add_argument("--reset-topic", action="store_true",
                   help="Delete + recreate the output topic before consuming")
    p.add_argument("--out", default="-", help="JSON output path, or '-' for stdout")
    p.add_argument("--engine-start-epoch", type=float, default=None,
                   help="Engine submission time (time.time() seconds since epoch). "
                        "wall_seconds is measured from this anchor instead of from "
                        "the first received record.")
    args = p.parse_args()

    if args.reset_topic:
        ensure_output_topic(args.brokers, args.topic, args.partitions)
        # Give Kafka a beat to settle the metadata.
        time.sleep(2)

    result = consume(args.brokers, args.topic, args.expected, args.timeout_seconds,
                     args.engine, args.engine_start_epoch)

    payload = json.dumps(result, indent=2)
    if args.out == "-":
        print(payload)
    else:
        with open(args.out, "w") as fh:
            fh.write(payload + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
