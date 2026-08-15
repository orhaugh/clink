#!/usr/bin/env python3
"""Independently recount ONE window from the raw input topic.

When the oracle and the engine disagree, there are three possible
culprits and the verdict file cannot tell them apart: the engine
processed the input wrongly, the oracle's expectation is wrong, or the
input on the broker is not what the spec says was produced. This
resolves it by going back to the bytes actually in Kafka.

It reports three counts per key for one window:

  spec      what the deterministic function says should exist
  input     what is ACTUALLY on the input topic (read and re-aggregated
            here, independently of both the engine and the spec)
  output    what the engine committed to the output topic

  input == spec != output  ->  the engine is wrong
  input == output != spec  ->  the spec/oracle is wrong
  all three differ         ->  the input itself is not what was intended

Usage:
  recount.py --brokers ... --in-topic qual01-in --out-topic qual01-out \\
             --spec /qual/progress.json.spec --window-start <ms> [--partitions N]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from detspec import Spec  # noqa: E402

from confluent_kafka import Consumer, TopicPartition  # noqa: E402


def read_input_window(brokers, topic, partitions, spec, w_start, w_end, slack):
    """Re-aggregate the actual input events falling in [w_start, w_end).

    Reads only the offset slice that can contain them. The generator
    produces each partition sequentially into a freshly created topic, so
    offset and sequence advance together; the slack either side covers
    the jitter and any offset skew, and the exact filter is the event's
    own timestamp, never the offset.
    """
    lo_seq, hi_seq = spec.seq_range_for_window(w_start)
    lo_seq = max(0, lo_seq - slack)
    hi_seq = hi_seq + slack

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": "qual01-recount-readonly",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })

    counts = {}          # key -> [count, sum]
    seen_ids = set()     # to detect duplicate events ON THE INPUT topic
    dupes_on_input = 0
    scanned = 0

    for p in range(partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        start = min(max(low, lo_seq), max(low, high))
        consumer.assign([TopicPartition(topic, p, start)])
        while True:
            msg = consumer.poll(10)
            if msg is None:
                break
            if msg.error():
                break
            scanned += 1
            ev = json.loads(msg.value())
            seq = int(ev["event_id"].split("-", 1)[1])
            if seq > hi_seq:
                break
            ts = int(ev["ts"])
            if w_start <= ts < w_end:
                if ev["event_id"] in seen_ids:
                    dupes_on_input += 1
                seen_ids.add(ev["event_id"])
                k = int(ev["k"])
                c, s = counts.get(k, (0, 0))
                counts[k] = (c + 1, s + int(ev["amount"]))
    consumer.close()
    return counts, scanned, dupes_on_input


def read_output_window(brokers, topic, w_start):
    """Every committed output record for this window, by key."""
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": "qual01-recount-out-readonly",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "auto.offset.reset": "earliest",
    })
    md = consumer.list_topics(topic, timeout=15)
    parts = list(md.topics[topic].partitions.keys())

    # Bounded by the high-water mark AS IT IS NOW, not by going quiet.
    # The pipeline is still running while this diagnostic reads, so a
    # consumer that stops when the topic goes idle never stops at all -
    # it just keeps pace with the producer forever. The window under
    # examination is long closed, so everything relevant is already
    # below these offsets.
    ends = {}
    for p in parts:
        _, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        ends[p] = hi
    consumer.assign([TopicPartition(topic, p, 0) for p in parts])
    remaining = {p for p in parts if ends[p] > 0}

    out = {}
    occurrences = {}
    while remaining:
        msg = consumer.poll(10)
        if msg is None:
            break
        if msg.error():
            break
        if msg.offset() >= ends[msg.partition()] - 1:
            remaining.discard(msg.partition())
        rec = json.loads(msg.value())
        if int(rec["ws"]) != w_start:
            continue
        k = int(rec["k"])
        occurrences[k] = occurrences.get(k, 0) + 1
        out[k] = (int(rec["cnt"]), int(rec["total"]))
    consumer.close()
    return out, occurrences


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", required=True)
    ap.add_argument("--in-topic", default="qual01-in")
    ap.add_argument("--out-topic", default="qual01-out")
    ap.add_argument("--spec", required=True)
    ap.add_argument("--window-start", type=int, required=True)
    ap.add_argument("--slack", type=int, default=5000)
    ap.add_argument("--report", default="")
    args = ap.parse_args()

    sd = json.load(open(args.spec))
    spec = Spec(sd["seed"], sd["partitions"], sd["keys"],
                sd["events_per_sec_per_partition"], sd["base_ms"],
                sd["max_jitter_ms"], sd["window_ms"])
    w_start = args.window_start
    w_end = w_start + sd["window_ms"]

    print(f"recount: window [{w_start}, {w_end})", flush=True)

    inp, scanned, dupes_on_input = read_input_window(
        args.brokers, args.in_topic, sd["partitions"], spec, w_start, w_end, args.slack)
    print(f"recount: scanned {scanned} input records, "
          f"{len(inp)} keys in window, {dupes_on_input} duplicate ids ON THE INPUT",
          flush=True)

    outp, occurrences = read_output_window(args.brokers, args.out_topic, w_start)
    print(f"recount: {len(outp)} keys in the engine's output for this window", flush=True)

    # The spec's own expectation, using a produced_high high enough to
    # cover the whole window (the generator is far past it by now).
    high = {p: 10 ** 12 for p in range(sd["partitions"])}
    expected = spec.expected_for_window(w_start, high)

    keys = set(inp) | set(outp) | set(expected)
    engine_wrong = spec_wrong = both_wrong = agree = 0
    samples = []
    for k in sorted(keys):
        i = inp.get(k)
        o = outp.get(k)
        e = expected.get(k)
        if i == o == e:
            agree += 1
        elif i == e and o != e:
            engine_wrong += 1
            if len(samples) < 8:
                samples.append({"key": k, "input": i, "spec": e, "engine": o,
                                "verdict": "engine disagrees with both"})
        elif i == o and o != e:
            spec_wrong += 1
            if len(samples) < 8:
                samples.append({"key": k, "input": i, "spec": e, "engine": o,
                                "verdict": "spec disagrees with both"})
        else:
            both_wrong += 1
            if len(samples) < 8:
                samples.append({"key": k, "input": i, "spec": e, "engine": o,
                                "verdict": "all three differ"})

    multi = {k: n for k, n in occurrences.items() if n > 1}
    report = {
        "window_start": w_start,
        "keys_total": len(keys),
        "all_three_agree": agree,
        "engine_disagrees_with_input_and_spec": engine_wrong,
        "spec_disagrees_with_input_and_engine": spec_wrong,
        "all_three_differ": both_wrong,
        "duplicate_ids_on_input_topic": dupes_on_input,
        "keys_emitted_more_than_once": len(multi),
        "input_records_scanned": scanned,
        "samples": samples,
    }
    print(json.dumps(report, indent=2))
    if args.report:
        with open(args.report, "w") as f:
            json.dump(report, f, indent=2)

    print()
    if engine_wrong and not spec_wrong:
        print("recount: THE ENGINE IS WRONG - the input topic agrees with the spec")
    elif spec_wrong and not engine_wrong:
        print("recount: THE ORACLE IS WRONG - the input topic agrees with the engine")
    elif not engine_wrong and not spec_wrong and not both_wrong:
        print("recount: no disagreement in this window")
    else:
        print("recount: mixed - read the samples")
    return 0


if __name__ == "__main__":
    sys.exit(main())
