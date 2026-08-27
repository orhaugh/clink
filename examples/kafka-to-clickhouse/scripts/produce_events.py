#!/usr/bin/env python3
"""Stream the workload into the tutorial's Kafka topic.

    ./scripts/produce_events.py             # about 50 readings/s, roughly 50 s in all
    ./scripts/produce_events.py --rate 200  # faster
    ./scripts/produce_events.py --stdout    # print `key<TAB>json` lines instead

By default the readings go through the Kafka container's own console producer
(docker compose exec kafka kafka-console-producer.sh), so this machine needs
nothing but Docker and Python 3. Each record is keyed by sensor id.

Run it once per stack. Kafka keeps what you send, so a second run doubles the
input and the aggregates with it; `docker compose down -v` starts clean.
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


def producer_command(bootstrap_server: str) -> list:
    return [
        "docker", "compose", "--project-directory", EXAMPLE_DIR, "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-console-producer.sh",
        f"--bootstrap-server={bootstrap_server}",
        f"--topic={workload.TOPIC}",
        "--property", "parse.key=true",
        "--property", "key.separator=\t",
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=float, default=50.0,
                    help="readings per second (default 50; 0 = as fast as possible)")
    ap.add_argument("--stdout", action="store_true",
                    help="print key<TAB>json lines instead of writing to Kafka")
    ap.add_argument("--bootstrap-server", default="kafka:9092",
                    help="broker address as seen from inside the kafka container (default kafka:9092)")
    args = ap.parse_args()

    if args.stdout:
        out = sys.stdout
        proc = None
    else:
        try:
            proc = subprocess.Popen(producer_command(args.bootstrap_server),
                                    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
        except FileNotFoundError:
            print("produce_events: docker not found on PATH", file=sys.stderr)
            return 1
        out = proc.stdin

    per_tick = len(workload.SENSORS)
    interval = per_tick / args.rate if args.rate > 0 else 0.0
    started = time.monotonic()
    next_tick_at = started
    sent = 0
    tick_of_last_report = -1
    print(f"produce_events: {workload.TOTAL_READINGS} readings from {len(workload.SENSORS)} sensors, "
          f"event time {workload.fmt_ts(workload.START_MS)} to "
          f"{workload.fmt_ts(workload.START_MS + (workload.TOTAL_TICKS - 1) * workload.TICK_MS)} UTC",
          file=sys.stderr)
    try:
        for r in workload.arrivals():
            out.write(f"{r['sensor_id']}\t{json.dumps(r, separators=(',', ':'))}\n")
            sent += 1
            if sent % per_tick == 0:
                tick = sent // per_tick
                if interval > 0:
                    next_tick_at += interval
                    delay = next_tick_at - time.monotonic()
                    if delay > 0:
                        out.flush()
                        time.sleep(delay)
                if tick % 30 == 0 and tick != tick_of_last_report:
                    tick_of_last_report = tick
                    print(f"produce_events: {sent:>5} / {workload.TOTAL_READINGS} readings sent, "
                          f"event time {workload.fmt_ts(workload.START_MS + tick * workload.TICK_MS)}",
                          file=sys.stderr)
        out.flush()
    except BrokenPipeError:
        print("produce_events: the Kafka producer went away. Is the stack running? "
              "(docker compose up -d, then docker compose ps)", file=sys.stderr)
        return 1
    if proc is not None:
        proc.stdin.close()
        rc = proc.wait()
        if rc != 0:
            print(f"produce_events: kafka-console-producer exited with status {rc}", file=sys.stderr)
            return 1
    print(f"produce_events: done, {sent} readings in {time.monotonic() - started:.1f} s", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
