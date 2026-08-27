#!/usr/bin/env python3
"""Feed the tutorial's producer output into the rig's broker.

    produce_events.py --stdout | rig_produce.py --brokers <host:9092> --topic readings

The tutorial's own producer writes `key<TAB>json` lines, which locally go to
the Kafka container's console producer. The rig has no such container to
exec into, so this reads the identical lines and produces them with a real
Kafka client. Transport only: the workload, its pacing and its deliberate
out-of-order sensor all stay in produce_events.py, which is the thing the
check exists to run unchanged.
"""

import argparse
import sys

from confluent_kafka import Producer


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brokers", required=True)
    ap.add_argument("--topic", required=True)
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.brokers, "linger.ms": 20,
                         "enable.idempotence": True})
    sent = 0
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        key, _, value = line.partition("\t")
        producer.produce(args.topic, key=key.encode(), value=value.encode(),
                         on_delivery=on_delivery)
        sent += 1
        if sent % 500 == 0:
            producer.poll(0)
    producer.flush(60)
    if errors:
        print(f"rig_produce: {len(errors)} delivery error(s); first: {errors[0]}", file=sys.stderr)
        return 1
    print(f"rig_produce: {sent} records produced to {args.topic}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
