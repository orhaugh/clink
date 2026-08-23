#!/usr/bin/env python3
"""QUAL-01 workload generator. Runs on the ops host, outside the clink
failure domain. Produces deterministic events (see detspec.py) to the
input topic at a controlled rate, and records per-partition progress to
disk so the verifier knows the produced high-water marks.

Exactly-once input discipline: the producer runs idempotent
(enable.idempotence), and on restart the generator does NOT trust its own
progress file - it reads the actual tail of each partition from the
broker and resumes from the true last sequence + 1, so a crash between
produce and progress-flush cannot double-produce an event id.

Usage:
  generator.py --brokers HOST:9092 --topic qual01-in --rate 2000 \
               --partitions 4 --keys 100000 --seed 20260815 \
               --progress /qual/progress.json [--duration-s N]
"""
import argparse
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from detspec import Spec  # noqa: E402

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402


def true_high_water(brokers: str, topic: str, partitions: int) -> dict:
    """Read the last event's seq per partition from the broker - the
    authoritative resume point after a generator restart."""
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": "qual01-generator-resume",
        "enable.auto.commit": False,
    })
    high = {}
    for p in range(partitions):
        low, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if hi <= low:
            high[p] = 0
            continue
        consumer.assign([TopicPartition(topic, p, hi - 1)])
        msg = consumer.poll(10)
        if msg is None or msg.error():
            raise RuntimeError(f"cannot read tail of partition {p}")
        event = json.loads(msg.value())
        # event_id is "p<part>-<seq>"; resume AFTER it.
        high[p] = int(event["event_id"].split("-", 1)[1]) + 1
    consumer.close()
    return high


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", required=True)
    ap.add_argument("--topic", default="qual01-in")
    ap.add_argument("--rate", type=int, default=2000, help="events/sec TOTAL")
    ap.add_argument("--partitions", type=int, default=4)
    ap.add_argument("--keys", type=int, default=100000)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--base-ms", type=int, required=True,
                    help="event-time epoch base (ms); fixed per campaign")
    ap.add_argument("--max-jitter-ms", type=int, default=1500)
    ap.add_argument("--window-ms", type=int, default=10000)
    # 0 (the default) keeps the fixed key space every campaign before
    # QUAL-05 used. Non-zero makes the key space TURN OVER: see detspec.
    ap.add_argument("--key-epoch-ms", type=int, default=0)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--duration-s", type=int, default=0, help="0 = run until stopped")
    ap.add_argument("--stop-file", default="",
                    help="stop producing, flush and write final progress when "
                         "this file appears (default: <progress>.stop). The "
                         "spawn discipline starts this process with SIGINT "
                         "ignored, so a file is the stop delivery the campaign "
                         "relies on; see verifier.py's docstring.")
    args = ap.parse_args()

    per_part_rate = max(1, args.rate // args.partitions)
    spec = Spec(args.seed, args.partitions, args.keys, per_part_rate,
                args.base_ms, args.max_jitter_ms, args.window_ms,
                args.key_epoch_ms)

    producer = Producer({
        "bootstrap.servers": args.brokers,
        "enable.idempotence": True,
        "linger.ms": 20,
        "batch.num.messages": 10000,
    })

    seqs = true_high_water(args.brokers, args.topic, args.partitions)
    print(f"generator: resuming from {seqs}", flush=True)

    spec_record = {
        "seed": args.seed, "partitions": args.partitions, "keys": args.keys,
        "events_per_sec_per_partition": per_part_rate, "base_ms": args.base_ms,
        "max_jitter_ms": args.max_jitter_ms, "window_ms": args.window_ms,
        "topic": args.topic,
    }
    with open(args.progress + ".spec", "w") as f:
        json.dump(spec_record, f, indent=2)

    stop_file = args.stop_file or (args.progress + ".stop")
    signal.signal(signal.SIGINT, signal.default_int_handler)  # re-arm over inherited SIG_IGN
    started = time.time()
    last_flush = started
    produced_since_report = 0
    try:
        while not os.path.exists(stop_file):
            tick_started = time.time()
            if args.duration_s and tick_started - started >= args.duration_s:
                break
            for p in range(args.partitions):
                for _ in range(per_part_rate // 10):  # 100ms micro-batches
                    payload = spec.event_json(p, seqs[p])
                    while True:
                        try:
                            producer.produce(args.topic, payload.encode(), partition=p)
                            break
                        except BufferError:
                            producer.poll(0.05)
                    seqs[p] += 1
                    produced_since_report += 1
            producer.poll(0)
            now = time.time()
            if now - last_flush >= 5:
                producer.flush(30)
                tmp = args.progress + ".tmp"
                with open(tmp, "w") as f:
                    json.dump({"produced_high": {str(k): v for k, v in seqs.items()},
                               "wallclock": now}, f)
                os.replace(tmp, args.progress)
                rate = produced_since_report / (now - last_flush)
                print(f"generator: {sum(seqs.values())} total, {rate:.0f} ev/s", flush=True)
                last_flush = now
                produced_since_report = 0
            sleep_left = 0.1 - (time.time() - tick_started)
            if sleep_left > 0:
                time.sleep(sleep_left)
    except KeyboardInterrupt:
        pass
    producer.flush(60)
    tmp = args.progress + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"produced_high": {str(k): v for k, v in seqs.items()},
                   "wallclock": time.time()}, f)
    os.replace(tmp, args.progress)
    print(f"generator: done at {seqs}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
