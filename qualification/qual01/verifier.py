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

Shutdown protocol: the campaign requests the final verdict by creating
the stop file (--stop-file, default <verdict>.stop). A file is the only
delivery the spawn discipline guarantees: start_on_host backgrounds this
process with `&` under a non-interactive shell, which POSIX starts with
SIGINT IGNORED - and Python does not install its KeyboardInterrupt
handler over an inherited SIG_IGN, so a polite pkill -INT never reached
the loop. qual01-20260818e ran 2h with a perfect oracle and summarised
INCONCLUSIVE because of exactly that: 235,668 tail pairs, final=false,
the campaign's 1200s grace spent waiting for a signal that was being
ignored. SIGINT is re-armed explicitly below as a courtesy for
interactive use, but the stop file is the contract.

Usage:
  verifier.py --brokers HOST:9092 --topic qual01-out \
              --spec /qual/progress.json.spec --progress /qual/progress.json \
              --verdict /qual/verdict.json [--grace-s 60] [--stop-file P]
"""
import argparse
import json
import os
import signal
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
    ap.add_argument("--stop-file", default="",
                    help="finalise and exit when this file appears "
                         "(default: <verdict>.stop)")
    args = ap.parse_args()
    stop_file = args.stop_file or (args.verdict + ".stop")
    # Re-arm SIGINT even if it was inherited ignored (see module docstring);
    # the stop file remains the delivery the campaign relies on.
    signal.signal(signal.SIGINT, signal.default_int_handler)

    while not os.path.exists(args.spec):
        time.sleep(1)
    spec = load_spec(args.spec)

    # A UNIQUE group per run, and an explicit close at the end.
    #
    # A fixed group id cost a false FAIL the first time this harness was
    # dry-run: the previous run's member had not yet aged out, the new
    # one was assigned a subset of partitions, and 317 records it never
    # read were reported as MISSING output. A verifier that manufactures
    # the very defect it is hunting is worse than no verifier. This
    # oracle recomputes everything from the spec and holds its own
    # observations in memory, so re-reading the whole topic from the
    # beginning is always the correct behaviour.
    group = f"qual01-verifier-{os.getpid()}-{int(time.time())}"
    consumer = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": group,
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
    window_ready_offsets = {}  # ws -> {partition: high-water offset at that moment}
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

    def topic_end_offsets():
        """High-water offset per partition, or None if the topic cannot be
        read. Also requires that this consumer is assigned EVERY partition,
        since a partial assignment would make absence meaningless."""
        try:
            meta = consumer.list_topics(args.topic, timeout=10)
            total_partitions = len(meta.topics[args.topic].partitions)
        except Exception:
            return None
        assignment = consumer.assignment()
        if not assignment or len(assignment) < total_partitions:
            return None
        ends = {}
        for tp in assignment:
            try:
                _low, high = consumer.get_watermark_offsets(tp, timeout=10,
                                                            cached=False)
            except Exception:
                return None
            ends[tp.partition] = high
        return ends

    def read_past(snapshot) -> bool:
        """Whether this consumer has read past every offset that existed
        when `snapshot` was taken.

        This is the sound form of "caught up" for a LIVE stream. Requiring
        the consumer to sit exactly at the high-water mark never succeeds
        while the pipeline is still producing - the first cloud run of this
        harness deferred judgement forever and would have soaked overnight
        for a vacuous verdict. What actually matters is weaker and
        achievable: everything that existed when the window's grace timer
        started has since been read, so an absence now is a real absence."""
        if not snapshot:
            return False
        assignment = consumer.assignment()
        if not assignment:
            return False
        try:
            positions = {tp.partition: tp.offset for tp in consumer.position(list(assignment))}
        except Exception:
            return False
        for partition, end in snapshot.items():
            pos = positions.get(partition)
            if pos is None or pos < 0 or pos < end:
                return False
        return True

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
        # ONE offset snapshot per pass, shared by every window that becomes
        # ready in it: they all become ready at this same instant, and a
        # per-window snapshot cost a broker round trip per partition per
        # window, which stalled the verifier outright once a few hundred
        # windows came ready together.
        pass_ends = None
        while spec.window_fully_produced(window_cursor, produced_high):
            if window_cursor not in window_ready_at:
                if pass_ends is None:
                    pass_ends = topic_end_offsets()
                if pass_ends is None:
                    # The consumer group has not finished assigning yet.
                    # Recording readiness now would store a null snapshot
                    # that read_past can never satisfy, leaving those
                    # windows permanently unjudgeable - which is how the
                    # first clean cloud run sat at zero judged windows
                    # while output streamed past it. Leave the cursor
                    # where it is and retry on the next pass.
                    break
                window_ready_at[window_cursor] = now
                # The offsets that existed the moment this window's grace
                # began. Judging waits until they have all been read.
                window_ready_offsets[window_cursor] = pass_ends
            window_cursor += spec.window_ms
        ready = [ws for ws, t in window_ready_at.items()
                 if now - t >= args.grace_s and ws not in fully_evaluated
                 and read_past(window_ready_offsets.get(ws))]
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
            window_ready_offsets.pop(ws, None)
        write_verdict()
        print(f"verifier: {totals}", flush=True)

    try:
        while not os.path.exists(stop_file):
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
    print("verifier: stop requested; finalising", flush=True)
    evaluate()
    write_verdict(final=True)
    consumer.close()  # leave the group cleanly; see the group-id note above
    print("verifier: final verdict written", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
