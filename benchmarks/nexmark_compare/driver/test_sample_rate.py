#!/usr/bin/env python3
"""Checks on the sampler's frontier, which decides every rate this harness reports.

Run directly:  python3 driver/test_sample_rate.py

The frontier answers one question - how many INPUT EVENTS has the engine read so
far - and every throughput figure is that number divided by a time. It was
defined as a max over every operator's records_in and records_out, which equals
the input only for a pipeline of 1:1 and N:1 stages. A 1:N stage races ahead of
it: a HOP window emits one row per overlapping pane, a ranking operator emits a
retraction alongside each insert. The frontier then reaches the target before the
source has finished reading, the drain is timed over a fraction of the input, and
the query is reported faster than the engine ran it. Nexmark q5 is both shapes at
once and was measured 6x faster than a bare projection over the same stream.

These cases pin the corrected rule and, as importantly, pin that the queries
measured before it are unaffected - so the existing q0 / q12 history stays
comparable to anything measured now.
"""

import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("sr", os.path.join(HERE, "sample_rate.py"))
sr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sr)

failures = []


def check(name, got, want):
    if got == want:
        print(f"  ok    {name}: {got:,}" if isinstance(got, int) else f"  ok    {name}: {got}")
    else:
        print(f"  FAIL  {name}: got {got!r}, want {want!r}")
        failures.append(name)


def stub(payload):
    sr.get_json = lambda url: payload


# --- clink -----------------------------------------------------------------

# A 1:N pipeline. The source has read 3M; the HOP window has emitted 9M panes and
# the ranking operator 12M rows. The frontier is the 3M actually read.
stub({"operators": [
    {"records_in": 0, "records_out": 3_000_000},            # source
    {"records_in": 3_000_000, "records_out": 3_000_000},    # decode (1:1)
    {"records_in": 3_000_000, "records_out": 9_000_000},    # HOP window (1:N)
    {"records_in": 9_000_000, "records_out": 12_000_000},   # ranking (delete+insert)
]})
check("clink, 1:N pipeline reports input read", sr.clink_frontier("b", "1")[0], 3_000_000)

# The q12 shape - 1:1 then N:1 - must be identical to the old definition, or every
# recorded before/after number moves onto a different premise than it was measured on.
stub({"operators": [
    {"records_in": 0, "records_out": 7_360_000},
    {"records_in": 7_360_000, "records_out": 7_360_000},
    {"records_in": 7_360_000, "records_out": 184_767},      # windowed aggregate (N:1)
]})
check("clink, N:1 pipeline unchanged", sr.clink_frontier("b", "1")[0], 7_360_000)

# Multi-source (a join): the target is the largest stream, not the sum, matching
# how the harness sets --target from the bid count alone.
stub({"operators": [
    {"records_in": 0, "records_out": 7_360_000},            # bid source
    {"records_in": 0, "records_out": 300_000},              # auction source
    {"records_in": 7_660_000, "records_out": 900_000},      # join
]})
check("clink, multi-source takes the largest", sr.clink_frontier("b", "1")[0], 7_360_000)

# Before any counter is published there is no source-shaped operator. Fall back to
# the old max rather than report no progress forever and time out every run.
stub({"operators": [{"records_in": 5, "records_out": 5}]})
check("clink, no source shape falls back", sr.clink_frontier("b", "1")[0], 5)

stub({"operators": []})
check("clink, empty operator list", sr.clink_frontier("b", "1")[0], 0)

# --- Flink -----------------------------------------------------------------

# Source vertex plus a 1:N window vertex. Flink SQL only chains 1:1 operators onto
# a source, so the source vertex's write-records is the input count.
stub({"duration": 4000, "vertices": [
    {"metrics": {"read-records": 0, "write-records": 3_000_000}},
    {"metrics": {"read-records": 3_000_000, "write-records": 9_000_000}},
]})
got, clock = sr.flink_frontier("b", "j")
check("flink, 1:N pipeline reports input read", got, 3_000_000)
check("flink, uses the job's own clock", clock, 4.0)

stub({"duration": 12000, "vertices": [
    {"metrics": {"read-records": 0, "write-records": 7_360_000}},
    {"metrics": {"read-records": 7_360_000, "write-records": 184_767}},
]})
check("flink, N:1 pipeline unchanged", sr.flink_frontier("b", "j")[0], 7_360_000)

# Metrics not populated yet (Flink's fetcher lags job start).
stub({"duration": 100, "vertices": [{"metrics": {}}]})
check("flink, unpopulated metrics", sr.flink_frontier("b", "j")[0], 0)

print()
if failures:
    print(f"{len(failures)} FAILED: {', '.join(failures)}")
    sys.exit(1)
print("all frontier checks passed")
