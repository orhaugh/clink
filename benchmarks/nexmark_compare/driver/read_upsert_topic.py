#!/usr/bin/env python3
"""Reduce a keyed, tombstoned Kafka topic to its FINAL STATE.

WHY. Three nexmark queries emit a changelog rather than an append-only stream: q5
(top-1 per sliding window - a new leader retracts the old one), q18 and q19 (dedup
and ranking per key - same). For those, comparing OUTPUT ROW COUNTS between two
engines is meaningless: the count depends on how many times a row was revised on
the way to the answer, which is an implementation detail, not the answer.

What IS comparable is the state the changelog converges to. Both engines write it
the same way - clink's kafka_upsert_sink_string and Flink's upsert-kafka both key
each message by the primary key and emit an EMPTY PAYLOAD as a delete, the
log-compaction tombstone convention - so reducing the topic to last-value-per-key,
with tombstoned keys removed, gives each engine's final answer in a form that can
be compared directly.

That is a stronger check than the append-only gate's row count, because it compares
values and not just how many there are.

  python3 driver/read_upsert_topic.py --bootstrap localhost:9092 --topic nx-out-q18-clink
  ... --json      # emit the reduced state for diffing
"""

import argparse
import json
import sys

from confluent_kafka import Consumer, TopicPartition


def read_topic(bootstrap, topic, timeout_s=20.0, idle_s=3.0):
    """Every message in the topic, in offset order per partition."""
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"upsert-reader-{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    md = c.list_topics(topic, timeout=10.0)
    if topic not in md.topics or md.topics[topic].error is not None:
        c.close()
        raise RuntimeError(f"topic {topic} not found")
    parts = list(md.topics[topic].partitions.keys())
    c.assign([TopicPartition(topic, p, 0) for p in parts])

    msgs = []
    eof = set()
    import time
    t0 = time.time()
    last_msg = t0
    while time.time() - t0 < timeout_s and len(eof) < len(parts):
        m = c.poll(0.5)
        if m is None:
            # Partitions with no data never emit EOF for an empty assignment on
            # some broker versions, so an idle period ends the read.
            if time.time() - last_msg > idle_s:
                break
            continue
        if m.error():
            # PARTITION_EOF is how a fully-read partition reports itself.
            if "_PARTITION_EOF" in str(m.error()) or m.error().code() == -191:
                eof.add(m.partition())
                continue
            raise RuntimeError(str(m.error()))
        msgs.append((m.partition(), m.offset(), m.key(), m.value()))
        last_msg = time.time()
    c.close()
    msgs.sort(key=lambda t: (t[0], t[1]))
    return msgs


def reduce_to_state(msgs):
    """Last value per key; a key whose last message is a tombstone is absent.

    Ordering is per partition, which is what the upsert contract guarantees: all
    messages for one key hash to one partition, so last-write-wins is well defined
    per key even though there is no global order.
    """
    state = {}
    tombstones = 0
    for _, _, key, value in msgs:
        k = key.decode("utf-8", "replace") if key is not None else None
        if value is None or len(value) == 0:
            tombstones += 1
            state.pop(k, None)
            continue
        state[k] = value.decode("utf-8", "replace")
    return state, tombstones


def canonical(state):
    """key -> value with the value's JSON keys sorted, so two engines that emit
    the same row with different field order still compare equal."""
    out = {}
    for k, v in state.items():
        try:
            parsed = json.loads(v)
            if isinstance(parsed, dict):
                parsed.pop("__row_kind", None)
                v = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
        except json.JSONDecodeError:
            pass
        out[k] = v
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--timeout", type=float, default=20.0)
    args = ap.parse_args()

    try:
        msgs = read_topic(args.bootstrap, args.topic, args.timeout)
    except Exception as e:  # noqa: BLE001 - the caller only needs the reason
        print(json.dumps({"error": str(e), "topic": args.topic}))
        return 1
    state, tombstones = reduce_to_state(msgs)
    canon = canonical(state)
    if args.json:
        print(json.dumps({"topic": args.topic, "messages": len(msgs),
                          "tombstones": tombstones, "live_keys": len(canon),
                          "state": canon}, sort_keys=True))
    else:
        print(f"  {args.topic}: {len(msgs)} messages, {tombstones} tombstones, "
              f"{len(canon)} live keys")
    return 0


if __name__ == "__main__":
    sys.exit(main())
