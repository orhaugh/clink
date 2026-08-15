#!/usr/bin/env python3
"""QUAL-01 independent verifier. Runs on the ops host, outside the clink
failure domain. Consumes the pipeline's output topic with
isolation.level=read_committed (so aborted transactions are invisible,
exactly as a downstream consumer would see them) and continuously
compares every closed window against the expectation recomputed from the
deterministic spec. Clink is never consulted.

Counted per closed window, per key:
  missing     expected a result, none arrived (after the grace horizon)
  duplicate   the same (key, window) arrived more than once with the SAME
              value - an exactly-once violation even though the data agrees
  conflicting the same (key, window) arrived with DIFFERENT values
  incorrect   arrived once, value differs from the recomputed expectation
  foreign     a (key, window) the spec says cannot exist

Verdict JSON is rewritten every evaluation pass; the final write on
shutdown is the campaign's correctness record.

Usage:
  verifier.py --brokers HOST:9092 --topic qual01-out \
              --spec /qual/progress.json.spec --progress /qual/progress.json \
              --verdict /qual/verdict.json [--grace-s 60]
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from detspec import Spec  # noqa: E402

from confluent_kafka import Consumer  # noqa: E402


def load_spec(path: str) -> Spec:
    with open(path) as f:
        s = json.load(f)
    return Spec(s["seed"], s["partitions"], s["keys"],
                s["events_per_sec_per_partition"], s["base_ms"],
                s["max_jitter_ms"], s["window_ms"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", required=True)
    ap.add_argument("--topic", default="qual01-out")
    ap.add_argument("--spec", required=True)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--verdict", required=True)
    ap.add_argument("--grace-s", type=int, default=120,
                    help="how long after a window is fully produced before "
                         "an absent result counts as missing (must cover "
                         "watermark lag + checkpoint interval + recovery)")
    ap.add_argument("--eval-every-s", type=int, default=30)
    args = ap.parse_args()

    while not os.path.exists(args.spec):
        time.sleep(1)
    spec = load_spec(args.spec)

    consumer = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": "qual01-verifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
    consumer.subscribe([args.topic])

    # observed[(key, ws)] -> list of (cnt, total) occurrences (bounded: the
    # exactly-once contract makes >3 occurrences the same defect as 3).
    observed = {}
    fully_evaluated = set()   # windows already judged and retired
    # The candidate window cursor advances from the spec's earliest
    # possible window, driven by the PRODUCED high-water marks - never by
    # what arrived on the output topic. A window whose output never
    # arrives at all (the failure mode that matters most) is judged
    # missing exactly like a partially-missing one.
    window_cursor = spec.window_start(spec.base_ms - spec.max_jitter_ms)
    totals = {"missing": 0, "duplicate": 0, "conflicting": 0,
              "incorrect": 0, "foreign": 0, "correct_windows": 0,
              "output_records": 0}
    defects = []              # bounded sample of concrete defects
    window_ready_at = {}      # ws -> wallclock when fully produced was seen
    last_eval = time.time()

    def write_verdict(final=False):
        verdict = dict(totals)
        verdict["final"] = final
        verdict["evaluated_windows"] = len(fully_evaluated)
        verdict["pending_pairs"] = len(observed)
        verdict["defect_sample"] = defects[:200]
        verdict["wallclock"] = time.time()
        tmp = args.verdict + ".tmp"
        with open(tmp, "w") as f:
            json.dump(verdict, f, indent=2)
        os.replace(tmp, args.verdict)

    def evaluate():
        try:
            with open(args.progress) as f:
                produced_high = {int(k): v for k, v in
                                 json.load(f)["produced_high"].items()}
        except (OSError, ValueError, KeyError):
            return
        now = time.time()
        # Which windows are ready to judge? Every spec window whose full
        # input is produced AND whose grace has elapsed since we first saw
        # that - independent of whether any output arrived for it.
        nonlocal window_cursor
        while spec.window_fully_produced(window_cursor, produced_high):
            window_ready_at.setdefault(window_cursor, now)
            window_cursor += spec.window_ms
        ready = [ws for ws, t in window_ready_at.items()
                 if now - t >= args.grace_s and ws not in fully_evaluated]
        for ws in sorted(ready):
            expected = spec.expected_for_window(ws, produced_high)
            ok = True
            for key, (ecnt, esum) in expected.items():
                occurrences = observed.pop((key, ws), [])
                if not occurrences:
                    totals["missing"] += 1
                    ok = False
                    if len(defects) < 200:
                        defects.append({"kind": "missing", "key": key, "ws": ws,
                                        "expected": [ecnt, esum]})
                    continue
                distinct = set(occurrences)
                if len(occurrences) > 1:
                    kind = "duplicate" if len(distinct) == 1 else "conflicting"
                    totals[kind] += 1
                    ok = False
                    if len(defects) < 200:
                        defects.append({"kind": kind, "key": key, "ws": ws,
                                        "occurrences": occurrences[:5]})
                if occurrences[-1] != (ecnt, esum):
                    totals["incorrect"] += 1
                    ok = False
                    if len(defects) < 200:
                        defects.append({"kind": "incorrect", "key": key, "ws": ws,
                                        "expected": [ecnt, esum],
                                        "got": list(occurrences[-1])})
            # Anything left observed for this window was not expected at all.
            foreign_here = [k for (k, w) in observed.keys() if w == ws]
            for key in foreign_here:
                occurrences = observed.pop((key, ws))
                totals["foreign"] += 1
                ok = False
                if len(defects) < 200:
                    defects.append({"kind": "foreign", "key": key, "ws": ws,
                                    "occurrences": occurrences[:5]})
            if ok:
                totals["correct_windows"] += 1
            fully_evaluated.add(ws)
            window_ready_at.pop(ws, None)
        write_verdict()
        print(f"verifier: {totals}", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    row = json.loads(msg.value())
                    pair = (int(row["k"]), int(row["ws"]))
                    occ = observed.setdefault(pair, [])
                    if len(occ) < 5:
                        occ.append((int(row["cnt"]), int(row["total"])))
                    totals["output_records"] += 1
                except (ValueError, KeyError):
                    totals["foreign"] += 1
                    if len(defects) < 200:
                        defects.append({"kind": "foreign",
                                        "raw": str(msg.value())[:200]})
            if time.time() - last_eval >= args.eval_every_s:
                evaluate()
                last_eval = time.time()
    except KeyboardInterrupt:
        pass
    evaluate()
    write_verdict(final=True)
    print("verifier: final verdict written", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
