#!/usr/bin/env python3
"""Pre-populate the input Kafka topic with N synthetic 24-byte records.

Both engines re-read this topic from offset 0. We finish producing
before either engine starts so the producer is not on the hot path.

Record layout (24 bytes, little-endian):
    int64 ts_ms
    int64 key
    int64 value
"""

import argparse
import os
import struct
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


RECORD_FMT = "<qqq"  # 3 x int64 LE


def ensure_topic(brokers: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": brokers})
    md = admin.list_topics(timeout=10)
    if topic in md.topics:
        # Delete + recreate so partition count is fresh.
        fut = admin.delete_topics([topic], operation_timeout=30)
        for t, f in fut.items():
            try:
                f.result()
            except Exception as e:
                print(f"warning: delete_topics({t}): {e}", file=sys.stderr)
        # Wait for delete to propagate.
        for _ in range(30):
            md = admin.list_topics(timeout=5)
            if topic not in md.topics:
                break
            time.sleep(1)
    fut = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)],
        operation_timeout=30,
    )
    for t, f in fut.items():
        f.result()


def produce(brokers: str, topic: str, records: int, keys: int, windows: int,
            partitions: int) -> None:
    # ts_ms grid: span exactly `windows` 1-second windows.
    # Each record's ts increments by step_ns within a window.
    window_size_ms = 1000
    step_us = (window_size_ms * 1000 * windows) / records  # in microseconds
    print(
        f"producer: records={records} keys={keys} windows={windows} "
        f"step={step_us:.3f}us per record; topic={topic}",
        file=sys.stderr,
    )

    producer = Producer({
        "bootstrap.servers": brokers,
        "linger.ms": 50,
        "batch.size": 1 << 20,
        "compression.type": "none",
        "acks": "1",
        "queue.buffering.max.messages": 2_000_000,
        "queue.buffering.max.kbytes": 1024 * 1024,
    })

    pack = struct.Struct(RECORD_FMT).pack
    start = time.monotonic()
    progress_every = max(1, records // 20)
    for i in range(records):
        ts_ms = int((i * step_us) // 1000)
        key = i % keys
        value = (i % 7) + 1  # bounded payload so sums don't grow without bound
        buf = pack(ts_ms, key, value)
        # BufferError means the local producer queue is full. Drain it
        # by polling with a small timeout, then retry. This is the
        # normal backpressure mechanism in confluent-kafka.
        while True:
            try:
                producer.produce(topic, value=buf)
                break
            except BufferError:
                producer.poll(0.1)
        if (i + 1) % 10000 == 0:
            producer.poll(0)
        if (i + 1) % progress_every == 0:
            elapsed = time.monotonic() - start
            print(
                f"producer: {i + 1}/{records} rec, "
                f"{(i + 1) / elapsed:,.0f} rec/s",
                file=sys.stderr,
            )
    producer.flush(60)

    # Trailer records: one per partition at ts well past the data tail.
    # Each parallel source subtask reads one partition, so per-subtask
    # watermark advances only if that partition has a record past the
    # last data ts. Without these the global watermark stalls at
    # last_data_ts - 1 and the final ~9% of data windows never fire.
    # The trailers themselves don't produce output records: their
    # window [200000, 201000) can never fire because no further record
    # advances the watermark past 201000.
    trailer_ts_ms = (windows * window_size_ms) + 100_000
    for partition in range(partitions):
        buf = pack(trailer_ts_ms, 0, 0)
        producer.produce(topic, value=buf, partition=partition)
    producer.flush(30)
    print(
        f"producer: wrote {partitions} trailer records at ts_ms={trailer_ts_ms} "
        f"(one per partition; advances watermark so all data windows fire)",
        file=sys.stderr,
    )

    elapsed = time.monotonic() - start
    print(
        f"producer: done in {elapsed:.2f}s = {records / elapsed:,.0f} rec/s",
        file=sys.stderr,
    )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", default=os.environ.get("BENCH_KAFKA_BROKERS", "localhost:9092"))
    p.add_argument("--topic", default=os.environ.get("BENCH_INPUT_TOPIC", "bench-in"))
    p.add_argument("--records", type=int, default=10_000_000)
    p.add_argument("--keys", type=int, default=1000)
    p.add_argument("--windows", type=int, default=100)
    p.add_argument("--partitions", type=int, default=4)
    args = p.parse_args()

    ensure_topic(args.brokers, args.topic, args.partitions)
    produce(args.brokers, args.topic, args.records, args.keys, args.windows,
            args.partitions)
    return 0


if __name__ == "__main__":
    sys.exit(main())
