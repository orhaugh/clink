#!/usr/bin/env python3
"""An integration test's wait on a child must outlast the child's own timeout.

The failure this catches is not a hang, it is a LOST DIAGNOSTIC. clink_submit_job
takes --wait-timeout-s and, on expiry, exits non-zero with an account of why:

    no JobCompleted after 15s: connection closed by the coordinator

If the test's own wait_for_exit(submit_pid, ...) is shorter than that, the test
always trips first and reports "submitter did not exit within 12s" - true,
uninformative, and pointing at the harness rather than the product. The
submitter's reason is never printed, because the submitter had not finished
saying it.

Five sites were in that state before this check existed, and one of them cost a
diagnosis: a whole-label failure read as a harness timeout until the run was
repeated and the submitter's own message turned out to be sitting three seconds
past the deadline the test enforced.

Widening the outer wait does NOT soften a gate. A child that self-times-out exits
non-zero, so every assertion on its exit code fails exactly as before - with the
reason attached instead of without it.

Deliberately textual. It pairs each --wait-timeout-s=N with the next wait on a
SUBMITTER pid in the same file, which is the order they appear in every one of
these tests, and skips a pair whose wait is immediately followed by a kill (that
is bounding cleanup, not waiting for the child to speak). It catches the obvious
case cheaply rather than parsing C++.

Only submitter pids, because --wait-timeout-s is clink_submit_job's flag and the
same tests also spawn clink_rescale_job and clink_cancel_job, which do not take
it. Pairing across children gave three false positives on the first run - each a
short, correct wait on a different tool that happened to follow a submit. If
another tool grows a self-timeout, add its pid naming here; the check cannot infer
which child a flag belongs to from text alone.

Written in Python, not awk: the pre-commit hook runs on macOS, whose awk has no
three-argument match(), and the first cut of this check silently reported every
file as a violation because of it.
"""

import glob
import os
import re
import sys

INNER = re.compile(r"--wait-timeout-s=(\d+)")
OUTER = re.compile(r"wait_for(?:_exit)?\(\s*&?([A-Za-z0-9_]*pid[A-Za-z0-9_]*)\s*,\s*(\d+)s")

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

violations = []

for path in sorted(glob.glob("tests/integration/*.cpp")):
    with open(path, errors="replace") as fh:
        lines = fh.readlines()

    inner = None
    inner_line = 0
    for n, line in enumerate(lines, start=1):
        m = INNER.search(line)
        if m:
            inner = int(m.group(1))
            inner_line = n
            continue
        w = OUTER.search(line)
        if not w:
            continue
        if "submit" not in w.group(1):
            # A different child, which does not take --wait-timeout-s. Leave the
            # pending inner timeout in place for the submitter wait that follows.
            continue
        outer = int(w.group(2))
        if inner is not None and outer <= inner:
            # A wait followed immediately by a kill is bounding cleanup.
            tail = "".join(lines[n : n + 3])
            if "kill_quietly" not in tail:
                violations.append(
                    f"{path}:{n}: waits {outer}s on a child whose own "
                    f"--wait-timeout-s is {inner}s (declared line {inner_line}).\n"
                    f"    The child cannot reach its own timeout, so its reason "
                    f"for failing is never printed."
                )
        inner = None

if violations:
    for v in violations:
        print(v)
    print()
    print(f"check-nested-timeouts: {len(violations)} wait(s) shorter than the child's own timeout.")
    print("Give the outer wait a margin over the inner one so the child's own message survives.")
    sys.exit(1)

print("check-nested-timeouts: every child wait outlasts the child's own timeout.")
