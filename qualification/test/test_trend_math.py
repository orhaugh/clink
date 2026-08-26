#!/usr/bin/env python3
"""QUAL-10's trend math against series whose answer is known in advance.

This file exists because of one failure mode: a leak criterion that cannot
fail. Statistics are easy to write so that everything passes - widen a band,
use a slope that a sawtooth flattens, compare endpoints that happen to line
up - and the campaign then reports twelve green hours having tested nothing.
So every band is driven from BOTH sides here: a synthetic leak must fail it,
and the healthy shapes must not.

The healthy shapes are specific to this campaign and worth naming, because
each one has broken a naive criterion:

  - a SAWTOOTH: RSS climbs through a warm cache and resets when a fault
    replaces the process. Least squares reads this as a trend; it is not.
  - a STEP at a restart boundary: a new incarnation settles at a different
    level. Flat within, and the across-test is what judges it.
  - NOISE: a flat series with real jitter must not trip a tight band.
"""
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "qual10"))
import trend  # noqa: E402

failures = []
checks = 0


def check(name, got, want):
    global checks
    checks += 1
    if got == want:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}: got {got!r}, wanted {want!r}")
        failures.append(name)


def series(n, hours, fn, noise=0.0, seed=1):
    r = random.Random(seed)
    return [(i * hours / n, fn(i * hours / n) + r.gauss(0, noise))
            for i in range(n)]


# ---- the slope itself -----------------------------------------------------

flat = series(300, 12, lambda h: 500.0, noise=3.0)
check("a flat noisy series has a slope near zero",
      abs(trend.slope_pct_per_hour(flat)) < 0.15, True)

ramp = series(300, 12, lambda h: 500.0 * (1 + 0.01 * h), noise=3.0)
s = trend.slope_pct_per_hour(ramp)
check("an injected 1%/h ramp is measured at ~1%/h", 0.85 < s < 1.15, True)

# The sawtooth is the shape a healthy run actually has.
saw = series(600, 12, lambda h: 500.0 + (h % 3.0) * 20.0, noise=2.0)
check("a reset sawtooth is not reported as a climb",
      trend.slope_pct_per_hour(saw) < 1.0, True)

spiky = series(300, 12, lambda h: 500.0, noise=2.0)
for i in range(0, 300, 17):  # ~6% wild outliers
    spiky[i] = (spiky[i][0], 5000.0)
check("outliers do not drag the slope (Theil-Sen, not least squares)",
      abs(trend.slope_pct_per_hour(spiky)) < 0.5, True)

# ---- within-incarnation judgement -----------------------------------------

def inc(points, i):
    return [(i, h, v) for h, v in points]


flat_incs = trend.split_incarnations(
    inc(series(200, 4, lambda h: 400.0, noise=2.0), 1)
    + inc(series(200, 4, lambda h: 400.0, noise=2.0), 2))
judged, found = trend.judge_within(flat_incs, max_pct_per_hour=0.5, min_hours=2)
check("two flat incarnations are both judged", judged, 2)
check("and neither is flagged", found, [])

leaky = trend.split_incarnations(
    inc(series(200, 4, lambda h: 400.0 * (1 + 0.02 * h), noise=2.0), 1))
_, found = trend.judge_within(leaky, max_pct_per_hour=0.5, min_hours=2)
check("a 2%/h climb inside one incarnation is flagged", len(found), 1)

# A survivor whose subtasks were redeployed onto it steps up ONCE and then
# holds. That is a new high-water mark, not a trend; the claim under test is
# "no monotonic growth", and a step that settles is not monotonic growth.
step_then_flat = trend.split_incarnations(
    inc(series(400, 4, lambda h: 400.0 if h < 1.0 else 640.0, noise=2.0), 1))
_, found = trend.judge_within(step_then_flat, max_pct_per_hour=0.5, min_hours=2)
check("a single step that then holds flat is not drift", found, [])

# But a process that keeps climbing after the step IS drifting: the second
# half rises too, and that is what the settled-half rule reads.
step_then_climb = trend.split_incarnations(
    inc(series(400, 4, lambda h: (400.0 if h < 1.0 else 640.0) + 400.0 * 0.02 * max(0, h - 1.0), noise=2.0), 1))
_, found = trend.judge_within(step_then_climb, max_pct_per_hour=0.5, min_hours=2)
check("a step followed by a continued climb is still flagged", len(found), 1)

short = trend.split_incarnations(inc(series(20, 0.4, lambda h: 400.0), 1))
judged, found = trend.judge_within(short, max_pct_per_hour=0.5, min_hours=2)
check("an incarnation too short to judge is not judged", judged, 0)
check("and is not counted as evidence either way", found, [])

# ---- across-incarnation judgement: the per-recovery leak -------------------

def plateau_run(levels, per=20):
    out = []
    for i, lvl in enumerate(levels):
        r = random.Random(100 + i)
        for k in range(per):
            out.append((i, i * 2 + k * 0.05, lvl + r.gauss(0, 1.0)))
    return trend.split_incarnations(out)


steady = plateau_run([400, 402, 399, 401, 400, 398])
r = trend.judge_across(steady, max_growth_ratio=1.15)
check("plateaus that wander but do not march are OK", r["verdict"], "OK")

climbing = plateau_run([400, 460, 520, 580, 640, 700])
r = trend.judge_across(climbing, max_growth_ratio=1.15)
check("a plateau that rises every restart is GROWTH", r["verdict"], "GROWTH")
check("and the growth ratio is reported", r["growth_ratio"] > 1.5, True)

# The patient leak: too small to trip the ratio, but it never once goes down.
ratchet = plateau_run([400, 404, 408, 412, 416, 420])
r = trend.judge_across(ratchet, max_growth_ratio=1.15)
check("a small but relentless ratchet is caught by the sign test",
      r["verdict"], "GROWTH")
check("even though its total growth stays inside the ratio",
      r["growth_ratio"] <= 1.15, True)

one_step = plateau_run([400, 460, 458, 461, 459, 460])
r = trend.judge_across(one_step, max_growth_ratio=1.15)
check("a single step that then holds is not a ratchet", r["ratcheting"], False)

r = trend.judge_across(plateau_run([400, 401]), max_growth_ratio=1.15)
check("too few incarnations says INSUFFICIENT, never OK", r["verdict"],
      "INSUFFICIENT")

# ---- gaps: a dead sampler must never read as a flat engine ----------------

hours = [i * 15 / 3600 for i in range(200)]
check("a dense series has no gap finding",
      trend.gap_report(hours, max_gap_minutes=5)["ok"], True)

with_hole = hours[:100] + [h + 0.5 for h in hours[100:]]
check("a 30-minute hole is not silently tolerated",
      trend.gap_report(with_hole, max_gap_minutes=5)["ok"], False)

print(f"\n{checks - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
