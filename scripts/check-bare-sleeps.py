#!/usr/bin/env python3
"""No NEW sleep-instead-of-a-condition in the integration tests.

A bare `sleep_for` in a multi-process test is a guess at how long something takes.
It is the single largest source of noise in this suite: the test passes alone in
two seconds and fails when a hundred multi-process tests run back to back, or when
a container build is using the same machine - and the failure is always reported as
a defect in whatever the test was actually checking, not as a bad wait. A whole day
went into chasing thirteen "failures" that were an unrelated process holding seven
cores.

A sleep INSIDE a polling loop is fine and is not counted - that is the poll
interval of a proper wait, where the deadline is a failure bound rather than a
delay. What this counts is a sleep standing on its own, in place of waiting for the
condition the next line actually depends on.

This is a RATCHET, not a clean-slate rule. There are dozens of these in the
pre-harness tests, and rewriting them all at once is a bigger change than it is
worth; what matters is that the number never goes UP. The baseline records the
count per file:

  * a file over its baseline           -> fail, with the count
  * a file not in the baseline at all  -> fail (a new test must not add one)
  * a file UNDER its baseline          -> fail too, asking for the baseline to be
                                          tightened, so the ratchet cannot rust

The last case is deliberate. A baseline that is allowed to drift above reality
stops being a ratchet and becomes a rubber stamp.

What to write instead:
  * a port to accept        -> clink::itest::await_port_accepting(port)
  * a worker to register    -> spawn the coordinator with spawn_logged(), then
                               await_log_matches(log, " slots=", n)
  * anything else observable-> await_condition([&]{ ... })
  * a whole cluster         -> tests/integration/cluster_harness.hpp, which does
                               all of the above and is what new tests should use
"""

import glob
import os
import re
import sys

BASELINE = "scripts/bare-sleeps-baseline.txt"

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

LOOP_HEADER = re.compile(r"^\s*(\}\s*)?(while|for|do)\b")


def _indent(line):
    return len(line) - len(line.lstrip())


def is_poll_interval(lines, i):
    """True if the sleep at line i is the poll interval of an enclosing loop.

    Determined by BLOCK STRUCTURE, not by nearby words. The first cut of this
    treated any `await` within eight preceding lines as proof, which let a bare
    sleep hide simply by sitting after an ASSERT_TRUE(...await...) - it passed two
    of its own mutation tests for that reason. Indentation is a far better proxy
    for "inside a loop body" and is what the repo's formatting guarantees.
    """
    target = _indent(lines[i])
    for j in range(i - 1, max(-1, i - 60), -1):
        line = lines[j]
        if not line.strip() or line.lstrip().startswith("//"):
            continue
        if _indent(line) < target:
            return bool(LOOP_HEADER.match(line))
    return False


def is_comment(line):
    """A line that is only a comment.

    Needed because this file's own explanatory comments mention sleep_for, and so do
    several tests' - test_fault_recovery.cpp's header describes the sleeps it does NOT
    use, and was counted as having one. A detector that flags prose is a detector
    people learn to work around by not writing the prose.
    """
    return line.lstrip().startswith(("//", "*", "/*", "#"))


def bare_sleeps(path):
    """Count sleeps that are NOT the poll interval of a wait."""
    with open(path, errors="replace") as fh:
        lines = fh.readlines()
    return sum(
        1
        for i, line in enumerate(lines)
        if "sleep_for" in line and not is_comment(line) and not is_poll_interval(lines, i)
    )


def read_baseline():
    counts = {}
    if not os.path.exists(BASELINE):
        return counts
    with open(BASELINE) as fh:
        for line in fh:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            count, path = line.split(None, 1)
            counts[path.strip()] = int(count)
    return counts


baseline = read_baseline()
over, unlisted, under = [], [], []

for path in sorted(glob.glob("tests/integration/*.cpp")):
    n = bare_sleeps(path)
    # A file that has dropped to ZERO still has to be reported, so its baseline
    # entry is removed. Skipping n == 0 outright was the first cut, and it meant
    # the ratchet rusted at exactly the moment someone fixed a file's LAST sleep:
    # the entry stayed behind as headroom for a future one.
    if n == 0 and baseline.get(path, 0) == 0:
        continue
    if n > 0 and path not in baseline:
        unlisted.append((path, n))
    elif n > baseline.get(path, 0):
        over.append((path, n, baseline[path]))
    elif n < baseline[path]:
        under.append((path, n, baseline[path]))

if over or unlisted:
    for path, n, was in over:
        print(f"{path}: {n} bare sleep(s), baseline is {was}.")
    for path, n in unlisted:
        print(f"{path}: {n} bare sleep(s) and no baseline entry.")
    print()
    print("A sleep here is a guess at how long something takes. Wait for the condition:")
    print("  a port accepting      -> clink::itest::await_port_accepting(port)")
    print("  a worker registering  -> spawn_logged() + await_log_matches(log, \" slots=\", n)")
    print("  anything else         -> await_condition([&]{ ... })")
    print("  a whole cluster       -> tests/integration/cluster_harness.hpp")
    print()
    print(f"If a sleep genuinely IS the thing under test, raise its entry in {BASELINE}")
    print("and say why in the commit message.")
    sys.exit(1)

if under:
    print("Good news, and an action: these are now BELOW their baseline.")
    for path, n, was in under:
        print(f"  {path}: {was} -> {n}")
    print()
    print(f"Tighten {BASELINE} to the new counts so the ratchet keeps holding.")
    sys.exit(1)

total = sum(baseline.values())
print(f"check-bare-sleeps: no new sleep-instead-of-a-condition ({total} grandfathered).")
