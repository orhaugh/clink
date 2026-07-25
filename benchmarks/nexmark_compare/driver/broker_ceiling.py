#!/usr/bin/env python3
"""Measure how fast the Kafka broker can SERVE the benchmark input.

Why this exists. Both engines read the same single unlimited broker container,
whose CPU is charged to neither engine's account. If the broker serves at roughly
the rate the engines drain at, the benchmark is measuring Kafka and every
engine-versus-engine throughput ratio taken from it is meaningless - no amount of
engine optimisation moves a number set upstream of the engine. Printing this as a
control row alongside the engines is what stops that being missed silently.

Runs `kafka-consumer-perf-test` INSIDE the broker container, deliberately.

The obvious implementation - a Python consumer loop over confluent_kafka - does
not work, and fails in a way that looks like a result. confluent_kafka builds a
Python object per message and the counting loop holds the GIL, so the measurement
is bounded by Python, not by Kafka. Measured here: one Python consumer reported
250k rec/s and FOUR reported 77k/s. Parallelism making a broker three times
slower is impossible; both figures were measuring the interpreter. The JVM tool
in the image is purpose-built, releases no such bottleneck, and reads from inside
the same docker network the engines use.

Still a floor in one respect: it does no per-record work beyond counting, so an
engine should not be expected to beat it - only to fall short of it. An engine
drain rate approaching this number means the run is input-bound.

  broker_ceiling.py --container nxcompare-kafka-1 --topic nx-bid --threads 4
      -> JSON {records, seconds, rate, mb_per_s, threads}
"""
import argparse
import json
import re
import subprocess
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", default="nxcompare-kafka-1")
    ap.add_argument("--bootstrap", default="localhost:9092",
                    help="as seen from INSIDE the container")
    ap.add_argument("--topic", default="nx-bid")
    ap.add_argument("--messages", type=int, default=0, help="0 = whole topic")
    ap.add_argument("--threads", type=int, default=4,
                    help="match the engines' parallelism; a 1-thread control reads as a "
                         "far lower ceiling than the truth")
    ap.add_argument("--timeout-s", type=float, default=300.0)
    a = ap.parse_args()

    messages = a.messages
    if messages <= 0:
        # Whole topic: sum the end offsets so the tool has a concrete target.
        try:
            out = subprocess.run(
                ["docker", "exec", a.container, "kafka-run-class",
                 "kafka.tools.GetOffsetShell", "--broker-list", a.bootstrap,
                 "--topic", a.topic, "--time", "-1"],
                capture_output=True, text=True, timeout=60).stdout
            messages = sum(int(l.rsplit(":", 1)[1]) for l in out.splitlines() if ":" in l)
        except Exception as e:
            print(json.dumps({"error": f"offset probe failed: {e}"}))
            return 3
    if messages <= 0:
        print(json.dumps({"error": "topic empty or offsets unreadable"}))
        return 4

    try:
        r = subprocess.run(
            ["docker", "exec", a.container, "kafka-consumer-perf-test",
             "--bootstrap-server", a.bootstrap, "--topic", a.topic,
             "--messages", str(messages), "--threads", str(a.threads),
             "--group", f"broker-ceiling-{messages}"],
            capture_output=True, text=True, timeout=a.timeout_s)
    except Exception as e:
        print(json.dumps({"error": f"perf test failed: {e}"}))
        return 5

    # ONE summary row: start.time, end.time, data.consumed.in.MB, MB.sec,
    # data.consumed.in.nMsg, nMsg.sec, ...
    #
    # --show-detailed-stats is deliberately NOT passed. It switches the output to
    # per-interval, per-thread rows whose rate columns are windowed, and parsing
    # those as a summary produced 11k rec/s at 1.4 MB/s for a local broker - a
    # figure wrong by two orders of magnitude that still looked like a result.
    best = None
    for line in r.stdout.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 6:
            continue
        try:
            mb_sec = float(parts[3]); nmsg = float(parts[4]); nmsg_sec = float(parts[5])
        except ValueError:
            continue  # header row
        if nmsg_sec > 0 and (best is None or nmsg > best[1]):
            best = (mb_sec, nmsg, nmsg_sec)

    if best is None:
        print(json.dumps({"error": "could not parse perf output",
                          "stdout_tail": r.stdout[-400:], "stderr_tail": r.stderr[-400:]}))
        return 6

    mb_sec, nmsg, nmsg_sec = best
    print(json.dumps({
        "records": int(nmsg),
        "seconds": round(nmsg / nmsg_sec, 3) if nmsg_sec else 0,
        "rate": round(nmsg_sec, 1),
        "mb_per_s": round(mb_sec, 1),
        "threads": a.threads,
        "tool": "kafka-consumer-perf-test",
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
