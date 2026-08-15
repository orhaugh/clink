#!/usr/bin/env python3
"""Self-test for the QUAL-01 oracle. The verdict of a multi-day campaign
is only worth what this maths is worth, so the expectation logic is
checked against a naive, independent computation over a simulated
stream - including the boundary behaviour that a wrong answer would hide
inside (jitter across window edges, partial production, window closure).

Run: python3 qualification/qual01/test_oracle.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from detspec import Spec  # noqa: E402

FAILURES = []


def check(name, condition, detail=""):
    if condition:
        print(f"ok   {name}")
    else:
        print(f"FAIL {name} {detail}")
        FAILURES.append(name)


def naive_expected(spec, w_start, produced_high):
    """The obvious O(n) computation over every produced event - the
    reference the optimised windowed version must agree with."""
    out = {}
    w_end = w_start + spec.window_ms
    for p in range(spec.partitions):
        for seq in range(produced_high.get(p, 0)):
            key, amount, ts = spec.event(p, seq)
            if w_start <= ts < w_end:
                c, s = out.get(key, (0, 0))
                out[key] = (c + 1, s + amount)
    return out


def main() -> int:
    base = 1_700_000_000_000
    spec = Spec(seed=42, partitions=4, keys=50,
                events_per_sec_per_partition=100, base_ms=base,
                max_jitter_ms=1500, window_ms=10_000)

    # Determinism: the same (partition, seq) is always the same event.
    check("event is a pure function",
          all(spec.event(1, s) == spec.event(1, s) for s in range(1000)))

    # Distinct partitions do not collide into identical streams.
    check("partitions differ",
          [spec.event(0, s) for s in range(50)] != [spec.event(1, s) for s in range(50)])

    # Jitter stays inside its declared bound - the campaign's watermark
    # premise depends on this exactly.
    worst = 0
    for p in range(spec.partitions):
        for s in range(2000):
            _, _, ts = spec.event(p, s)
            ideal = base + (s * 1000) // spec.eps
            worst = max(worst, ideal - ts)
    check("jitter within bound", 0 <= worst <= spec.max_jitter_ms,
          f"worst={worst}")

    produced = {p: 5000 for p in range(spec.partitions)}

    # The candidate seq range must not miss any event that lands in the
    # window - the failure mode that would silently under-count the
    # expectation and turn correct output into "foreign".
    for w_index in range(1, 6):
        w_start = base + w_index * spec.window_ms
        fast = spec.expected_for_window(w_start, produced)
        slow = naive_expected(spec, w_start, produced)
        check(f"window {w_index} matches naive computation", fast == slow,
              f"fast={len(fast)} keys, slow={len(slow)} keys")

    # Non-empty, or the agreement above is vacuous.
    w_start = base + 2 * spec.window_ms
    expected = spec.expected_for_window(w_start, produced)
    total_events = sum(c for c, _ in expected.values())
    check("window is non-empty", total_events > 0, f"events={total_events}")
    # Each window should hold roughly window_ms * rate * partitions events.
    nominal = spec.window_ms // 1000 * spec.eps * spec.partitions
    check("window population is plausible",
          0.5 * nominal <= total_events <= 1.5 * nominal,
          f"events={total_events} nominal={nominal}")

    # Closure: a window is not judged until every partition has produced
    # past the last seq that could contribute to it. Judging early is how
    # a verifier invents "missing" results for output that was never due.
    _, hi = spec.seq_range_for_window(w_start)
    check("not fully produced when one partition lags",
          not spec.window_fully_produced(w_start, {0: hi, 1: hi, 2: hi, 3: hi - 1}))
    check("fully produced when all partitions pass the horizon",
          spec.window_fully_produced(w_start, {p: hi for p in range(4)}))

    # Partial production must not be counted as if complete: the
    # expectation for a half-produced window is strictly smaller.
    half = {p: 2500 for p in range(spec.partitions)}
    late_w = base + 40 * spec.window_ms   # beyond what `half` produced
    check("beyond-production window expects nothing",
          spec.expected_for_window(late_w, half) == {})

    # A window fully inside the produced range must be identical whether
    # the generator has since produced more - a closed window's
    # expectation is stable, which is what lets the verifier retire it.
    stable_w = base + 3 * spec.window_ms
    e1 = spec.expected_for_window(stable_w, {p: 5000 for p in range(4)})
    e2 = spec.expected_for_window(stable_w, {p: 9000 for p in range(4)})
    check("closed window expectation is stable under later production", e1 == e2)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURES: {FAILURES}")
        return 1
    print("oracle self-test: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
