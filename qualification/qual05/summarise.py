#!/usr/bin/env python3
"""QUAL-05 result logic: turn a run's retained evidence into a verdict.

The campaign claims that a job whose key space keeps turning over holds
BOUNDED state. Three things have to be true together for that to mean
anything, and this module refuses a PASS unless all three are:

  1. the subject arm's state PLATEAUED - flat within a written band over
     the steady-state window, not merely "did not crash";
  2. the CONTROL arm GREW - the identical workload with retention removed.
     Without that the plateau is unfalsifiable: a workload that never
     accumulated anything also produces a flat line, and would pass;
  3. the output stayed EXACTLY correct - every event folded once, every
     key's count right - while retention was releasing state underneath it.

Anything less is INCONCLUSIVE rather than PASS. A campaign that cannot
fail is worth nothing, and the control arm is what makes this one able to.
"""
import argparse
import json
import os
import sys

# The coordinator-side protocol points this campaign arms. The sink is an
# upsert sink, which has no prepare/commit window of its own, so the
# sink.* points are not in scope here - the same reasoning as QUAL-04.
TWOPC_POINTS = (
    "coordinator.before_completed_marker",
    "coordinator.after_completed_marker",
)

MANDATORY_EVENTS = (
    "worker_sigkill",
    "coordinator_restart",
    "broker_restart",
    "network_latency",
    "partition_from_coordinator",
) + tuple(f"twopc_fired:{p}" for p in TWOPC_POINTS)

# The plateau band. Both are written here rather than discovered after the
# run, which is the point of deciding a pass criterion in advance.
#
# PLATEAU_DRIFT_MAX: the fitted trend, extrapolated across the whole
# steady-state window, may move the state level by at most this fraction of
# its mean. A genuinely bounded job drifts a little as the population
# breathes around its equilibrium; one that is still growing does not stay
# inside a quarter of its own mean.
#
# The two spread statistics do different jobs and both are gated.
#
# PLATEAU_BAND_MAX is p90/p10 - how stable the LEVEL is. Max/min alone
# cannot answer that: one restart transient decides it. A run measured on
# the local rig sat at 1.4 MiB with p90/p10 of 1.29 and max/min of 1.90,
# because a single sample landed mid-recovery. The level was flat; the
# extremes were chaos. A genuinely climbing series has a large p90/p10
# (the control arm's ramp is over 3), so this stays a real gate.
#
# PLATEAU_EXCURSION_MAX is max/min - how far the worst transient went. Kept
# because a run that ends flat having tripled in the middle is not
# something to certify quietly, and p90/p10 would forgive it.
PLATEAU_DRIFT_MAX = 0.25
PLATEAU_BAND_MAX = 1.5
PLATEAU_EXCURSION_MAX = 2.5
# The control arm has to have grown by at least this multiple for its
# comparison to be worth anything.
CONTROL_MIN_RATIO = 3.0
# Fewest steady-state samples that can support a trend at all.
MIN_PLATEAU_SAMPLES = 8


def read_kv(path):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def read_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return {}


def read_series(path):
    """[(t_seconds, bytes)] from the campaign's state samples."""
    out = []
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            parts = line.strip().split(",")
            if len(parts) < 2:
                continue
            try:
                out.append((float(parts[0]), float(parts[1])))
            except ValueError:
                continue
    return out


def plateau(series):
    """Fit the steady-state window and report whether it is flat.

    Least squares on (t, bytes). `drift` is the fitted change across the
    window as a fraction of the mean, which is the scale-free way to ask
    "is this level, or is it still climbing" without hard-coding a byte
    figure the workload would invalidate.
    """
    n = len(series)
    if n < MIN_PLATEAU_SAMPLES:
        return {"ok": False, "reason": f"only {n} steady-state samples", "samples": n}
    ts = [p[0] for p in series]
    ys = [p[1] for p in series]
    mean_t = sum(ts) / n
    mean_y = sum(ys) / n
    if mean_y <= 0:
        return {"ok": False, "reason": "mean state size is zero", "samples": n}
    denom = sum((t - mean_t) ** 2 for t in ts)
    slope = 0.0 if denom == 0 else sum((ts[i] - mean_t) * (ys[i] - mean_y) for i in range(n)) / denom
    span = max(ts) - min(ts)
    drift = (slope * span) / mean_y
    ordered = sorted(ys)
    pct = lambda q: ordered[min(n - 1, int(q * (n - 1)))]  # noqa: E731
    p10, p90 = pct(0.10), pct(0.90)
    band = (p90 / p10) if p10 > 0 else float("inf")
    excursion = (max(ys) / min(ys)) if min(ys) > 0 else float("inf")
    return {
        "ok": (abs(drift) <= PLATEAU_DRIFT_MAX
               and band <= PLATEAU_BAND_MAX
               and excursion <= PLATEAU_EXCURSION_MAX),
        "samples": n,
        "mean_bytes": mean_y,
        "slope_bytes_per_hour": slope * 3600.0,
        "drift": drift,
        "band": band,
        "excursion": excursion,
        "window_s": span,
        "reason": "",
    }


def count_faults(path):
    counts = {}
    if not os.path.exists(path):
        return counts
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            f = rec.get("fault")
            if f:
                counts[f] = counts.get(f, 0) + 1
    return counts


def coordinator_restart_pids_stable(path):
    """A coordinator restart is only evidence of recovery if the workers did
    not die with it. The chaos controller records both PID sets."""
    ok = 0
    total = 0
    if not os.path.exists(path):
        return ok, total
    with open(path) as fh:
        for line in fh:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("fault") != "coordinator_restart":
                continue
            total += 1
            before = rec.get("worker_pids_before")
            after = rec.get("worker_pids_after")
            if before and after and before == after:
                ok += 1
    return ok, total


def build(out_dir, run_id, duration_h, profile):
    verdict = read_json(os.path.join(out_dir, "q5-verdict.json"))
    verif = read_kv(os.path.join(out_dir, "verification.txt"))
    comp = read_kv(os.path.join(out_dir, "completeness.txt"))
    quiesce = read_kv(os.path.join(out_dir, "final-quiesce.txt"))
    catchup = read_kv(os.path.join(out_dir, "catchup.txt"))
    control = read_kv(os.path.join(out_dir, "control.txt"))
    retention = read_kv(os.path.join(out_dir, "retention.txt"))
    series = read_series(os.path.join(out_dir, "state-series-steady.csv"))
    faults = count_faults(os.path.join(out_dir, "q5-chaos.jsonl"))

    job_gone = os.path.exists(os.path.join(out_dir, "job-gone.txt"))
    chaos_died = os.path.exists(os.path.join(out_dir, "chaos-died.txt"))
    oracle_dirty = os.path.exists(os.path.join(out_dir, "oracle-dirty.txt"))

    findings = verdict.get("findings", [])
    stuck = bool(verdict.get("stuck", False))

    # --- 1. the plateau -----------------------------------------------------
    pl = plateau(series)

    # --- 2. the control arm -------------------------------------------------
    control_first = float(control.get("control_first_bytes", 0) or 0)
    control_last = float(control.get("control_last_bytes", 0) or 0)
    control_ratio = (control_last / control_first) if control_first > 0 else 0.0
    control_grew = control_ratio >= CONTROL_MIN_RATIO

    # --- 3. retention actually released -------------------------------------
    expired_total = int(retention.get("retention_expired_total", 0) or 0)
    retention_engaged = expired_total > 0

    # --- 4. correctness -----------------------------------------------------
    caught_up = catchup.get("caught_up", "yes") == "yes"
    produced = int(comp.get("produced_total", 0) or 0)
    sum_n = int(comp.get("sum_n", 0) or 0)
    null_rows = int(comp.get("rows_with_null_n", 0) or 0)
    missing = int(comp.get("keys_missing", 0) or 0)
    wrong = int(comp.get("keys_wrong_n", 0) or 0)
    fabricated = int(comp.get("keys_fabricated", 0) or 0)
    checked = int(comp.get("keys_checked", 0) or 0)

    have_endstate = bool(comp)
    exact = have_endstate and produced > 0 and produced == sum_n
    # Fabricated keys and NULL counts are defects however far behind the
    # pipeline is; missing and wrong counts are only judgeable once it has
    # caught up, because a pipeline still reading has legitimately absent
    # keys. QUAL-04 learned this the expensive way.
    keys_clean = fabricated == 0 and null_rows == 0
    if caught_up:
        keys_clean = keys_clean and missing == 0 and wrong == 0
    meaningful = checked > 0

    # --- 5. fault coverage --------------------------------------------------
    gaps = [e for e in MANDATORY_EVENTS if faults.get(e, 0) == 0]
    pid_ok, pid_total = coordinator_restart_pids_stable(
        os.path.join(out_dir, "q5-chaos.jsonl")
    )
    if pid_total > 0 and pid_ok == 0:
        gaps.append("coordinator_restart with stable worker PIDs")

    quiesced = quiesce.get("quiesced", "no") == "yes"

    hard_fail = (
        bool(findings)
        or stuck
        or oracle_dirty
        or job_gone
        # Exactness is only decidable once the pipeline has finished
        # reading: a run still catching up has folded fewer events than the
        # generator produced, and that is incompleteness, not loss.
        or (caught_up and have_endstate and not exact)
        or (have_endstate and not keys_clean)
    )

    if hard_fail:
        result = "FAIL"
    elif not have_endstate or not meaningful or not caught_up or not exact:
        result = "INCONCLUSIVE"
    elif not pl["ok"]:
        result = "INCONCLUSIVE"
    elif not control_grew:
        result = "INCONCLUSIVE"
    elif not retention_engaged:
        result = "INCONCLUSIVE"
    elif gaps or chaos_died or not quiesced:
        result = "INCONCLUSIVE"
    else:
        result = "PASS"

    lines = []
    a = lines.append
    a(f"# QUAL-05 - state TTL steady state ({run_id})")
    a("")
    a(f"- duration: {duration_h}h soak, profile `{profile}`")
    a(f"- events produced: {produced}")
    a(f"- events folded: {sum_n}")
    a(f"- keys judged: {checked}")
    a("")
    a("## Did state stay bounded")
    a("")
    if pl.get("samples", 0):
        a(f"- steady-state samples: {pl['samples']} over {pl.get('window_s', 0) / 60:.0f} min")
        a(f"- mean live state: {pl.get('mean_bytes', 0) / (1024 * 1024):.1f} MiB")
        a(f"- fitted trend: {pl.get('slope_bytes_per_hour', 0) / (1024 * 1024):.2f} MiB/hour")
        a(f"- drift across the window: {pl.get('drift', 0) * 100:.1f}% of mean "
          f"(band +/-{PLATEAU_DRIFT_MAX * 100:.0f}%)")
        a(f"- level stability (p90/p10): {pl.get('band', 0):.2f} (band {PLATEAU_BAND_MAX})")
        a(f"- worst excursion (max/min): {pl.get('excursion', 0):.2f} "
          f"(band {PLATEAU_EXCURSION_MAX})")
    else:
        a(f"- NO steady-state samples: {pl.get('reason', 'unknown')}")
    a(f"- plateau: {'yes' if pl['ok'] else 'NO'}")
    a("")
    a("## Would it have grown without retention")
    a("")
    a(f"- control arm first sample: {control_first / (1024 * 1024):.1f} MiB")
    a(f"- control arm last sample: {control_last / (1024 * 1024):.1f} MiB")
    a(f"- growth: {control_ratio:.1f}x over {control.get('control_window_s', '?')}s "
      f"(needs >= {CONTROL_MIN_RATIO}x)")
    a(f"- control grew: {'yes' if control_grew else 'NO - the plateau above proves nothing'}")
    a("")
    a("## Was retention doing the work")
    a("")
    a(f"- keys released by retention: {expired_total}")
    a(f"- keys under retention at the end: {retention.get('retention_tracked_keys', '?')}")
    # The workload's key space turns over at a known rate, so the live
    # population is predictable in advance. Reported rather than gated:
    # it is a sanity check on whether the plateau sits where the
    # arithmetic says it should, and a wildly different level means the
    # run measured something other than what was designed.
    pred_entries = verif.get("predicted_live_distinct_entries")
    pred_groups = verif.get("predicted_live_groups")
    if pred_entries and pred_groups:
        predicted = int(pred_entries) + int(pred_groups)
        tracked = int(retention.get("retention_tracked_keys", 0) or 0)
        a(f"- predicted live population: {predicted} "
          f"({pred_entries} distinct entries + {pred_groups} groups)")
        if tracked > 0:
            a(f"- measured against prediction: {tracked / predicted:.2f}x")
    a("")
    a("## Correctness")
    a("")
    a(f"- caught up before judging: {'yes' if caught_up else 'NO'}")
    a(f"- exact fold (produced == folded): {'yes' if exact else 'NO'}")
    a(f"- keys missing: {missing}")
    a(f"- keys with a wrong count: {wrong}")
    a(f"- keys the engine invented: {fabricated}")
    a(f"- rows with a NULL count: {null_rows}")
    a("")
    a("## Faults applied")
    a("")
    for name in sorted(faults):
        a(f"- {name}: {faults[name]}")
    if gaps:
        a("")
        a(f"- COVERAGE GAPS: {', '.join(gaps)}")
    a("")
    a(f"- oracle samples: {verdict.get('samples', 0)}")
    a(f"- oracle findings: {len(findings)}")
    a(f"- quiesced: {'yes' if quiesced else 'NO'}")
    if chaos_died:
        a("- chaos controller died before the soak ended")
    if job_gone:
        a("- the job disappeared from the coordinator")
    a("")
    a(f"**RESULT: {result}**")
    a("")
    return "\n".join(lines) + "\n", result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--duration-h", default="?")
    ap.add_argument("--profile", default="?")
    args = ap.parse_args()
    text, _result = build(args.out_dir, args.run_id, args.duration_h, args.profile)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
