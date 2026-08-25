#!/usr/bin/env python3
"""QUAL-10's verdict arithmetic: is this series flat, or is it climbing?

Two decisions shape all of it.

ROBUST SLOPE, NOT LEAST SQUARES. The healthy shape here is a sawtooth: RSS
climbs through a warm cache and drops when a fault replaces the process.
Least squares is pulled around by those resets and by any single outlier
sample, so it reports trends that are artefacts of the chaos schedule.
Theil-Sen - the median of pairwise slopes - ignores them, and its
breakdown point (29%) comfortably covers a run where a third of the
samples sit either side of a restart.

WITHIN, THEN ACROSS. A restart resets a process, so a single slope over
the whole run measures the chaos schedule rather than the engine. The two
questions are genuinely different objects:

  - WITHIN an incarnation: does this process's footprint climb while it
    runs? That is a steady-state leak, per record or per checkpoint.
  - ACROSS incarnations: does each new process settle HIGHER than the last?
    That is a per-recovery leak - sockets not closed, threads not joined,
    per-job maps never erased - and it is invisible within any single
    incarnation, because each one looks flat on its own.

The second test is the one this campaign exists for. A criterion that only
looked within incarnations would pass an engine that leaked a megabyte per
restart forever.
"""
from __future__ import annotations


def median(xs):
    s = sorted(x for x in xs if x is not None)
    if not s:
        return None
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def theil_sen(points, max_pairs=200_000):
    """Median pairwise slope, in y-units per x-unit.

    Pairs are strided rather than randomly sampled when the series is long:
    a deterministic subsample makes the verdict reproducible from the same
    evidence, which a campaign that publishes its numbers needs.
    """
    pts = [(x, y) for x, y in points if y is not None]
    n = len(pts)
    if n < 3:
        return None
    total_pairs = n * (n - 1) // 2
    stride = 1 if total_pairs <= max_pairs else int((total_pairs / max_pairs) ** 0.5) + 1
    slopes = []
    for i in range(0, n, stride):
        for j in range(i + stride, n, stride):
            dx = pts[j][0] - pts[i][0]
            if dx > 0:
                slopes.append((pts[j][1] - pts[i][1]) / dx)
    return median(slopes)


def slope_pct_per_hour(points):
    """Theil-Sen slope as a percentage of the series' own level, per hour.

    Relative rather than absolute so one band covers a 3 GB worker and a
    200 MB coordinator: "0.5%/h" means the same risk in both, where
    "2 MB/h" does not.
    """
    s = theil_sen(points)
    if s is None:
        return None
    level = median([y for _, y in points if y is not None])
    if not level:
        return None
    return s / level * 100.0


def split_incarnations(samples):
    """[(incarnation_id, [(hours, value)])], oldest first.

    Keyed on the process's start time, so a pid reused after a kill does
    not silently glue two different processes into one series.
    """
    out, order = {}, []
    for inc, h, v in samples:
        if inc not in out:
            out[inc] = []
            order.append(inc)
        out[inc].append((h, v))
    return [(i, out[i]) for i in order]


def judge_within(incarnations, max_pct_per_hour, min_hours):
    """Every incarnation that ran long enough must be flat while it ran."""
    findings, judged = [], 0
    for inc, pts in incarnations:
        if not pts:
            continue
        span = max(p[0] for p in pts) - min(p[0] for p in pts)
        if span < min_hours:
            continue  # too short to say anything; not evidence either way
        judged += 1
        s = slope_pct_per_hour(pts)
        if s is None:
            continue
        if s > max_pct_per_hour:
            findings.append({
                "incarnation": inc, "slope_pct_per_hour": round(s, 4),
                "span_hours": round(span, 2), "limit": max_pct_per_hour,
            })
    return judged, findings


def judge_across(incarnations, max_growth_ratio, min_samples=5):
    """Do successive incarnations settle higher than their predecessors?

    Compares PLATEAUS (the median of each incarnation's samples) rather
    than endpoints: an endpoint lands wherever the fault happened to fall
    in the sawtooth, and comparing two arbitrary points of two sawtooths
    measures the schedule, not the engine.

    Two ways to fail, because a leak can be steep or patient. A big total
    rise first-to-last is the obvious one. A persistent upward RATCHET -
    most successive pairs increasing - catches the leak too small to clear
    the ratio in one run but which would, given a week.
    """
    plateaus = [(inc, median([v for _, v in pts]))
                for inc, pts in incarnations if len(pts) >= min_samples]
    plateaus = [(i, p) for i, p in plateaus if p is not None]
    if len(plateaus) < 3:
        return {"judged": len(plateaus), "verdict": "INSUFFICIENT",
                "plateaus": [p for _, p in plateaus]}
    first, last = plateaus[0][1], plateaus[-1][1]
    ratio = (last / first) if first else None
    rising = sum(1 for a, b in zip(plateaus, plateaus[1:]) if b[1] > a[1])
    pairs = len(plateaus) - 1
    ratcheting = pairs >= 4 and rising / pairs > 2 / 3
    ok = (ratio is not None and ratio <= max_growth_ratio) and not ratcheting
    return {
        "judged": len(plateaus),
        "verdict": "OK" if ok else "GROWTH",
        "first_plateau": first, "last_plateau": last,
        "growth_ratio": round(ratio, 4) if ratio else None,
        "limit_ratio": max_growth_ratio,
        "rising_pairs": rising, "pairs": pairs,
        "ratcheting": ratcheting,
        "plateaus": [round(p, 2) for _, p in plateaus],
    }


def gap_report(hours_sorted, max_gap_minutes):
    """The longest hole in the series.

    A dead sampler and a flat engine produce the same picture - nothing
    changing - so a gap past the threshold has to read INCONCLUSIVE rather
    than pass. This is the same rule QUAL-12 applies to an unexercised row.
    """
    if len(hours_sorted) < 2:
        return {"max_gap_minutes": None, "ok": False}
    gaps = [(b - a) * 60.0 for a, b in zip(hours_sorted, hours_sorted[1:])]
    worst = max(gaps)
    return {"max_gap_minutes": round(worst, 2),
            "limit_minutes": max_gap_minutes,
            "ok": worst <= max_gap_minutes}
