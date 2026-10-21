#!/usr/bin/env python3
"""Stream the example's transfers into Kafka.

    ./scripts/produce_events.py                  # every transfer, about 100/s
    ./scripts/produce_events.py --part first     # the first half only
    ./scripts/produce_events.py --part second    # the rest
    ./scripts/produce_events.py --stdout         # print key<TAB>json lines instead

The records go through the Kafka container's own console producer (docker
compose exec kafka kafka-console-producer.sh), so this machine needs nothing
but Docker and Python 3. Each record is keyed by its sender.

Run it once per stack: Kafka keeps what you send, so a second run doubles the
input. `docker compose down -v` starts clean.
"""

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import workload  # noqa: E402

EXAMPLE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=float, default=100.0,
                    help="transfers per second (default 100; 0 = as fast as possible)")
    ap.add_argument("--part", choices=["all", "first", "second"], default="all")
    ap.add_argument("--stdout", action="store_true")
    args = ap.parse_args()

    stream = workload.transfers()
    half = len(stream) // 2
    if args.part == "first":
        stream = stream[:half]
    elif args.part == "second":
        stream = stream[half:]

    if args.stdout:
        out, proc = sys.stdout, None
    else:
        proc = subprocess.Popen(
            ["docker", "compose", "--project-directory", EXAMPLE_DIR, "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server=kafka:9092",
             f"--topic={workload.TOPIC}", "--property", "parse.key=true",
             "--property", "key.separator=\t"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
        out = proc.stdin
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    for n, t in enumerate(stream, start=1):
        out.write(f"{t['sender']}\t{json.dumps(t, separators=(',', ':'))}\n")
        if interval > 0 and n % 10 == 0:
            out.flush()
            time.sleep(10 * interval)
    out.flush()
    if proc is not None:
        proc.stdin.close()
        if proc.wait() != 0:
            print("produce_events: the Kafka producer failed. Is the stack running?", file=sys.stderr)
            return 1
    print(f"produce_events: sent {len(stream)} transfers ({args.part}), event time "
          f"{workload.fmt_ts(stream[0]['at'])} to {workload.fmt_ts(stream[-1]['at'])} UTC",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
