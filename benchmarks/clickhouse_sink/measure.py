#!/usr/bin/env python3
"""Measurement for the ClickHouse sink benchmarks (premise.md says what each
figure means; this file computes them).

  measure.py sample --base URL --out F         poll the job until it ends
  measure.py inserts-sql --table DB.TABLE      the query_log query for INSERTs
  measure.py record ... --out F                one trial's result
  measure.py legacy-batch-rows --campaign DIR  the native cell's mean rows per INSERT
  measure.py summarise --campaign DIR          the campaign against the targets

The rate of a sink cell is taken at the server: rows landed between the first
and the last acknowledged INSERT of the steady state, from system.query_log.
The blackhole cell sends no INSERT, so its rate is taken from clink's own
counter of rows reaching the sink operator, over the same kind of window. CPU
is the cgroup usage of the clink containers (Coordinator and Worker) across
the whole run, per 10^6 rows; a sink's hop is its CPU minus the blackhole
cell's. Values come in as arguments, never interpolated into code.
"""
import argparse
import glob
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request

TERMINAL = ("COMPLETED_OK", "FAILED", "CANCELLED")

# The B1 targets (docs/clickhouse-sink-plan.md 3.14), proposed until D2 accepts them.
B1_MIN_ROWS_PER_S = 500_000
B1_MIN_RATIO_TO_LEGACY = 3.0
B1_MAX_SINK_HOP_CPU_S_PER_1E6 = 4.0
MIN_TRIALS = 3
# The premise B1 is held to (premise.md); a campaign run off it is not B1.
B1_PREMISE = {
    "parallelism": 8,
    "checkpoint_interval_ms": 10000,
    "native.batch_rows": 1048449,
    "native.batch_bytes": 67108864,
    "native.batch_interval_ms": 1000,
    "native.compression": "lz4",
}
MIN_WINDOW_S = 600             # ten minutes of steady state for a sink rate
CALIBRATION_HEADROOM = 0.7     # a target above 70% of the ceiling is restated first
MIN_CLINK_RATE_WINDOW_S = 10   # below this, clink's counters give no rate


# --- sampling the job -------------------------------------------------------

def get_json(url, timeout=5.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def job_list(base):
    d = get_json(base + "/api/v1/jobs")
    return d.get("jobs", d) if isinstance(d, dict) else d


def is_sink(op):
    return op.get("kind") == "sink" or str(op.get("op_type", "")).endswith("_sink") or \
        str(op.get("op_type", "")).endswith("_sink_row")


def counters(ops):
    """Rows out of the sources and into the sinks, or None when a sink's
    figure is stale: the Coordinator reports a stale operator's counters as 0
    when it could not read the Worker, which is not a value."""
    sinks = [o for o in ops if is_sink(o)]
    if any(o.get("stale") for o in sinks):
        return None
    sources = [o for o in ops if int(o.get("records_in") or 0) == 0 and int(o.get("records_out") or 0) > 0
               and not is_sink(o)]
    return (sum(int(o.get("records_out") or 0) for o in sources),
            sum(int(o.get("records_in") or 0) for o in sinks),
            sorted({str(o.get("op_type")) for o in sinks}))


def cmd_sample(a):
    deadline = time.monotonic() + a.max_runtime
    job = None
    while job is None:
        try:
            jobs = job_list(a.base)
            ids = [int(j["id"]) for j in jobs if isinstance(j, dict) and "id" in j]
            if ids:
                job = max(ids)
        except (urllib.error.URLError, OSError, ValueError):
            pass
        if job is None:
            if time.monotonic() > deadline:
                raise SystemExit("measure: no job appeared at {}".format(a.base))
            time.sleep(0.5)

    t0 = time.monotonic()
    samples, status, sink_types, last_ops = [], "UNKNOWN", [], None
    final_seen = False
    while True:
        now = time.monotonic()
        try:
            ops = get_json("{}/api/v1/jobs/{}/operators".format(a.base, job)).get("operators", [])
            st = [j for j in job_list(a.base) if isinstance(j, dict) and int(j.get("id", -1)) == job]
            status = str(st[0].get("status")) if st else status
            got = counters(ops)
            if got is not None:
                src, snk, sink_types = got
                samples.append([round(now - t0, 3), round(time.time(), 3), src, snk])
                last_ops = ops
        except (urllib.error.URLError, OSError, ValueError):
            pass
        if status in TERMINAL:
            if final_seen:
                break
            final_seen = True  # one more sample: counters settle after the status flips
        elif now > deadline:
            status = "TIMEOUT"
            break
        time.sleep(a.interval)

    detail = None
    try:
        detail = get_json("{}/api/v1/jobs/{}".format(a.base, job))
    except (urllib.error.URLError, OSError, ValueError):
        pass
    out = {"job_id": job, "status": status, "sink_op_types": sink_types, "interval_s": a.interval,
           "columns": ["t_s", "unix_s", "source_records_out", "sink_records_in"],
           "samples": samples, "final_operators": last_ops, "job": detail}
    with open(a.out, "w") as f:
        json.dump(out, f)
    print("measure: job {} {} after {:.1f} s, {} rows into the sink".format(
        job, status, samples[-1][0] if samples else 0.0, samples[-1][3] if samples else 0))
    return 0 if status == "COMPLETED_OK" else 2


# --- rates --------------------------------------------------------------------

def inserts_sql(table):
    # One line per INSERT into the table that ended, in the order they ended:
    # its finish time, the rows it wrote, the server CPU it used, the time the
    # server held it back for too many parts, and whether it was acknowledged.
    return (
        "SELECT toUnixTimestamp64Micro(event_time_microseconds) AS t_us, written_rows AS rows, "
        "ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us, "
        "ProfileEvents['DelayedInsertsMilliseconds'] AS delayed_ms, "
        "type = 'QueryFinish' AS ok "
        "FROM system.query_log "
        "WHERE query_kind = 'Insert' AND has(tables, '{t}') AND type != 'QueryStart' "
        "ORDER BY t_us FORMAT JSONEachRow"
    ).format(t=table)


def insert_settings_sql(table):
    # The settings one acknowledged INSERT actually ran with: each sink sets its
    # own per statement, so the session defaults do not describe them.
    return (
        "SELECT Settings FROM system.query_log "
        "WHERE query_kind = 'Insert' AND has(tables, '{t}') AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds LIMIT 1 FORMAT JSONEachRow"
    ).format(t=table)


def steady_window(points, warmup, cooldown):
    """points: [(t_seconds, value)] in time order. The steady state runs from
    `warmup` seconds after the first point to `cooldown` seconds before the
    last; returns the points inside it."""
    if not points:
        return []
    lo, hi = points[0][0] + warmup, points[-1][0] - cooldown
    return [p for p in points if lo <= p[0] <= hi]


def server_rate(inserts, warmup, cooldown):
    ok = [i for i in inserts if int(i["ok"])]
    failed = len(inserts) - len(ok)
    pts = [(int(i["t_us"]) / 1e6, int(i["rows"])) for i in ok]
    win = steady_window(pts, warmup, cooldown)
    res = {
        "source": "system.query_log",
        "inserts_total": len(ok),
        "inserts_failed": failed,
        "rows_acknowledged": sum(r for _, r in pts),
        "mean_rows_per_insert_all": round(sum(r for _, r in pts) / len(pts), 1) if pts else None,
        "server_insert_query_cpu_s": round(sum(int(i["cpu_us"]) for i in ok) / 1e6, 3),
        "delayed_inserts_ms": sum(int(i.get("delayed_ms") or 0) for i in inserts),
    }
    if len(win) < 2 or win[-1][0] <= win[0][0]:
        res.update({"rows_per_s": None, "window_s": 0.0, "inserts_in_window": len(win),
                    "mean_rows_per_insert_steady": None,
                    "note": "fewer than two acknowledged INSERTs in the steady window"})
        return res
    # Rows landed between the first and the last acknowledged INSERT of the
    # window: the rows of every INSERT after the first, over the time between.
    span = win[-1][0] - win[0][0]
    res.update({
        "rows_per_s": round(sum(r for _, r in win[1:]) / span),
        "window_s": round(span, 3),
        "inserts_in_window": len(win),
        "mean_rows_per_insert_steady": round(sum(r for _, r in win) / len(win), 1),
        "inserts_per_s_steady": round((len(win) - 1) / span, 2),
    })
    return res


def clink_rate(samples, warmup, cooldown):
    """The rate rows reach the sink operator, from clink's counters: the
    window opens `warmup` seconds after the first row arrives and closes
    `cooldown` seconds before the counter reaches its final value."""
    rows = [(s[0], s[3]) for s in samples]
    final = max((v for _, v in rows), default=0)
    started = [p for p in rows if p[1] > 0]
    if not started or final == 0:
        return {"source": "clink operators", "rows_per_s": None, "window_s": 0.0, "rows_into_sink": final}
    t_first = started[0][0]
    t_done = next(t for t, v in rows if v == final)
    win = [p for p in rows if t_first + warmup <= p[0] <= t_done - cooldown]
    span = win[-1][0] - win[0][0] if len(win) >= 2 else 0.0
    if span < MIN_CLINK_RATE_WINDOW_S:
        return {"source": "clink operators", "rows_per_s": None, "window_s": round(span, 3),
                "rows_into_sink": final,
                "note": "the steady window is under {} s, too short for a rate".format(MIN_CLINK_RATE_WINDOW_S)}
    return {"source": "clink operators", "rows_per_s": round((win[-1][1] - win[0][1]) / span),
            "window_s": round(span, 3), "rows_into_sink": final,
            "first_row_to_last_s": round(t_done - t_first, 3)}


# --- one trial --------------------------------------------------------------------

def read_json(path):
    with open(path) as f:
        return json.load(f)


def read_ndjson(path):
    with open(path) as f:
        return [json.loads(ln) for ln in f if ln.strip()]


def per_million(seconds, rows):
    return round(seconds / rows * 1e6, 4) if rows else None


def cmd_record(a):
    samples = read_json(a.samples)
    trial = {
        "variant": a.variant,
        "trial": a.trial,
        "rows": a.rows,
        "image_id": a.image_id,
        "job_status": samples["status"],
        "sink_op_types": samples["sink_op_types"],
        "wall_s": samples["samples"][-1][0] if samples["samples"] else None,
        "batch": {"batch_rows": a.batch_rows, "batch_bytes": a.batch_bytes,
                  "batch_interval_ms": a.batch_interval_ms},
        "window": {"warmup_s": a.warmup, "cooldown_s": a.cooldown},
        "clink_rate": clink_rate(samples["samples"], a.warmup, a.cooldown),
    }
    clink_s = (a.clink_cpu_usec[1] - a.clink_cpu_usec[0]) / 1e6
    server_s = (a.server_cpu_usec[1] - a.server_cpu_usec[0]) / 1e6
    trial["cpu"] = {
        "clink_s": round(clink_s, 3),
        "clink_s_per_1e6_rows": per_million(clink_s, a.rows),
        # Cores busy on average over the run: a reading that went wrong shows here.
        "clink_cores_avg": round(clink_s / trial["wall_s"], 2) if trial["wall_s"] else None,
        "server_s": round(server_s, 3),
        "server_s_per_1e6_rows": per_million(server_s, a.rows) if a.variant != "blackhole" else None,
        "from": "cgroup v2 cpu.stat usage_usec, whole run, read before submit and after the job ended",
    }
    if a.inserts:
        trial["rate"] = server_rate(read_ndjson(a.inserts), a.warmup, a.cooldown)
    else:
        trial["rate"] = dict(trial["clink_rate"])
    if a.gate:
        trial["gate"] = read_json(a.gate)
    else:
        rows_in = trial["clink_rate"].get("rows_into_sink")
        trial["gate"] = {"passed": rows_in == a.rows and samples["status"] == "COMPLETED_OK",
                         "rows_produced": a.rows, "rows_into_sink": rows_in,
                         "checks": [{"check": "rows_into_sink", "ok": rows_in == a.rows,
                                     "detail": "{} rows reached the blackhole, {} produced".format(rows_in, a.rows)}]}
    if samples["status"] != "COMPLETED_OK":
        trial["gate"]["passed"] = False
        trial["gate"].setdefault("checks", []).append(
            {"check": "job_completed", "ok": False, "detail": "job ended {}".format(samples["status"])})
    if a.row_bytes:
        trial["mean_row_bytes_uncompressed"] = float(a.row_bytes)
    if a.insert_settings:
        got = read_ndjson(a.insert_settings)
        trial["insert_settings"] = got[0].get("Settings") if got else None
    with open(a.out, "w") as f:
        json.dump(trial, f, indent=2)
    r = trial["rate"]
    print("measure: {} t{}: {} rows/s over {} s ({}), clink CPU {} s per 10^6 rows, gate {}".format(
        a.variant, a.trial, r.get("rows_per_s"), r.get("window_s"), r["source"],
        trial["cpu"]["clink_s_per_1e6_rows"], "passed" if trial["gate"]["passed"] else "FAILED"))
    return 0


# --- the campaign -------------------------------------------------------------------

def trials_of(campaign):
    out = []
    for p in sorted(glob.glob(os.path.join(campaign, "*", "trial.json"))):
        out.append(read_json(p))
    return out


def med(values):
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


def cmd_legacy_batch_rows(a):
    native = [t for t in trials_of(a.campaign) if t["variant"] == "native" and t["gate"]["passed"]]
    m = med(t["rate"].get("mean_rows_per_insert_steady") for t in native)
    if m is None:
        raise SystemExit("measure: no passed native trial in {} to take the legacy batch_rows from".format(a.campaign))
    print(int(round(m)))
    return 0


def premise_value(premise, dotted):
    node = premise
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def measurement_reasons(premise, cells, trials):
    """Every reason the campaign cannot stand as a B1 measurement; empty when
    it can. Each is a rule of premise.md or of the plan behind it."""
    reasons = []
    if premise.get("local_smoke_run"):
        reasons.append("local smoke run: the server and clink share one machine")
    elif not premise.get("rig"):
        reasons.append("RIG is not stated: the machine types and the network link are part of the premise")
    if not premise.get("d2_targets_accepted"):
        reasons.append("D2 not accepted: the targets are still proposals")

    # The fixed premise.
    for key, want in B1_PREMISE.items():
        got = premise_value(premise, key)
        if got != want:
            reasons.append("{} is {}, the premise fixes {}".format(key, got, want))
    if "legacy" in cells:
        if premise_value(premise, "legacy.batch_rows_from") != "native cell mean rows per INSERT":
            reasons.append("the legacy batch_rows was set by hand, not taken from the native cell")
        if premise_value(premise, "legacy.batch_interval_ms") != premise_value(premise, "native.batch_interval_ms"):
            reasons.append("the legacy batch_interval_ms differs from the native one")

    # What is under test.
    if premise_value(premise, "clickhouse_cpp.build_type") != "Release":
        reasons.append("the clickhouse-cpp build type is not recorded as Release")
    build = premise_value(premise, "clink_capabilities.build") or {}
    if build.get("git_sha") != premise.get("under_test"):
        reasons.append("the image is commit {}, not the commit under test {}".format(
            str(build.get("git_sha"))[:12], str(premise.get("under_test"))[:12]))
    if build.get("git_clean") is not True:
        reasons.append("the image was built from a tree with uncommitted changes")
    if build.get("fault_injection"):
        reasons.append("the image is built with fault injection")
    if premise_value(premise, "harness.dirty"):
        reasons.append("the harness has uncommitted changes")

    # Calibration first, and the 70% rule.
    cal = premise.get("calibration")
    if not cal:
        reasons.append("no calibration: the server's ceiling is measured before clink")
    else:
        ceiling = cal.get("rows_per_second") or 0
        cal_premise = cal.get("premise", {})
        if cal_premise.get("local_smoke_run"):
            reasons.append("the calibration is a local smoke run")
        server_version = premise_value(premise, "server.version.version")
        if server_version and cal_premise.get("server_version") != server_version:
            reasons.append("the calibration ran against server {}, the campaign against {}".format(
                cal_premise.get("server_version"), server_version))
        if CALIBRATION_HEADROOM * ceiling < B1_MIN_ROWS_PER_S:
            reasons.append("the rate target {} is above 70% of the ceiling {}: restate it before measuring".format(
                B1_MIN_ROWS_PER_S, ceiling))

    # The trials.
    for v, c in cells.items():
        if c["trials_passed"] < MIN_TRIALS:
            reasons.append("{}: {} passed trial(s), {} needed".format(v, c["trials_passed"], MIN_TRIALS))
        if c["trials_passed"] < c["trials"]:
            reasons.append("{}: {} trial(s) failed the gate".format(v, c["trials"] - c["trials_passed"]))
        # The ten-minute window is the sink rates' rule. The blackhole cell is
        # there for the CPU subtraction over the same rows and ends sooner.
        if v != "blackhole" and c["window_s_min"] < MIN_WINDOW_S:
            reasons.append("{}: steady window {:.0f} s, under {} s".format(v, c["window_s_min"], MIN_WINDOW_S))
        if c.get("inserts_failed"):
            reasons.append("{}: {} INSERT(s) failed and were retried".format(v, c["inserts_failed"]))
        if c.get("delayed_inserts_ms"):
            reasons.append("{}: the server delayed INSERTs for {} ms (too many parts)".format(
                v, c["delayed_inserts_ms"]))
    for v in ("native", "blackhole", "legacy"):
        if v not in cells:
            reasons.append("no {} cell".format(v))
    stamps = {(t.get("image_id"), t.get("rows")) for t in trials}
    if stamps != {(premise.get("clink_image_id"), premise.get("rows"))}:
        reasons.append("the trials do not all share the campaign's image and input size")
    return reasons


def cmd_summarise(a):
    premise = read_json(os.path.join(a.campaign, "premise.json"))
    trials = trials_of(a.campaign)
    cells = {}
    for v in ("native", "blackhole", "legacy"):
        ts = [t for t in trials if t["variant"] == v]
        if not ts:
            continue
        good = [t for t in ts if t["gate"]["passed"]]
        cells[v] = {
            "trials": len(ts),
            "trials_passed": len(good),
            "rows_per_s_median": med(t["rate"].get("rows_per_s") for t in good),
            "rows_per_s_each": [t["rate"].get("rows_per_s") for t in good],
            "window_s_min": min((t["rate"].get("window_s") or 0.0 for t in good), default=0.0),
            "clink_cpu_s_per_1e6_median": med(t["cpu"]["clink_s_per_1e6_rows"] for t in good),
            "clink_cpu_s_per_1e6_each": [t["cpu"]["clink_s_per_1e6_rows"] for t in good],
            "server_cpu_s_per_1e6_median": med(t["cpu"]["server_s_per_1e6_rows"] for t in good),
            "mean_rows_per_insert_median": med(t["rate"].get("mean_rows_per_insert_steady") for t in good),
            "inserts_failed": sum(t["rate"].get("inserts_failed") or 0 for t in good),
            "delayed_inserts_ms": sum(t["rate"].get("delayed_inserts_ms") or 0 for t in good),
            "batch": ts[0]["batch"],
        }

    def hop(v):
        if v in cells and "blackhole" in cells:
            s, b = cells[v]["clink_cpu_s_per_1e6_median"], cells["blackhole"]["clink_cpu_s_per_1e6_median"]
            if s is not None and b is not None:
                return round(s - b, 4)
        return None

    derived = {"native_sink_hop_cpu_s_per_1e6": hop("native"), "legacy_sink_hop_cpu_s_per_1e6": hop("legacy")}
    if "native" in cells and "legacy" in cells and cells["legacy"]["rows_per_s_median"]:
        derived["native_to_legacy_rate"] = round(
            (cells["native"]["rows_per_s_median"] or 0) / cells["legacy"]["rows_per_s_median"], 2)
    if "native" in cells and "blackhole" in cells and cells["blackhole"]["rows_per_s_median"]:
        derived["native_to_blackhole_rate"] = round(
            (cells["native"]["rows_per_s_median"] or 0) / cells["blackhole"]["rows_per_s_median"], 3)

    n = cells.get("native", {})
    hop_native = derived["native_sink_hop_cpu_s_per_1e6"]
    b1 = {
        "rows_per_s": {"target": ">= {}".format(B1_MIN_ROWS_PER_S), "value": n.get("rows_per_s_median"),
                       "met": (n.get("rows_per_s_median") or 0) >= B1_MIN_ROWS_PER_S},
        "ratio_to_legacy": {"target": ">= {}".format(B1_MIN_RATIO_TO_LEGACY),
                            "value": derived.get("native_to_legacy_rate"),
                            "met": (derived.get("native_to_legacy_rate") or 0) >= B1_MIN_RATIO_TO_LEGACY},
        "sink_hop_cpu_s_per_1e6": {"target": "<= {}".format(B1_MAX_SINK_HOP_CPU_S_PER_1E6),
                                   "value": hop_native,
                                   "met": hop_native is not None and 0 <= hop_native <= B1_MAX_SINK_HOP_CPU_S_PER_1E6},
    }
    reasons = measurement_reasons(premise, cells, trials)

    summary = {"campaign": os.path.basename(os.path.normpath(a.campaign)), "cells": cells,
               "derived": derived, "b1": b1, "is_measurement": not reasons, "not_a_measurement_because": reasons}
    with open(os.path.join(a.campaign, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print("\n{:<10} {:>7} {:>12} {:>9} {:>14} {:>14} {:>12}".format(
        "cell", "trials", "rows/s", "window s", "clink CPU/1e6", "server CPU/1e6", "rows/INSERT"))
    for v, c in cells.items():
        print("{:<10} {:>7} {:>12} {:>9.0f} {:>14} {:>14} {:>12}".format(
            v, "{}/{}".format(c["trials_passed"], c["trials"]), str(c["rows_per_s_median"]), c["window_s_min"],
            str(c["clink_cpu_s_per_1e6_median"]), str(c["server_cpu_s_per_1e6_median"]),
            str(c["mean_rows_per_insert_median"])))
    print("\nB1, native sink:")
    for k, v in b1.items():
        print("  {:<24} {:>10}  target {:<10} {}".format(k, str(v["value"]), v["target"], "met" if v["met"] else "not met"))
    if reasons:
        print("\nNot a B1 measurement:")
        for r in reasons:
            print("  - " + r)
    else:
        print("\nA B1 measurement under premise.md.")
    print("\nsummary: {}".format(os.path.join(a.campaign, "summary.json")))
    return 0


def coerce(v):
    if v in ("true", "false"):
        return v == "true"
    try:
        return int(v)
    except ValueError:
        return v


def cmd_premise(a):
    """Merge values into the campaign's premise.json: --set k=v (a scalar),
    --text k=FILE (a file's text), --json k=FILE (a JSON document). A dotted
    key nests: --set clickhouse_cpp.version=2.6.2."""
    p = read_json(a.out) if os.path.exists(a.out) else {}

    def put(key, value):
        node = p
        parts = key.split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value

    for kv in a.set or []:
        k, _, v = kv.partition("=")
        put(k, coerce(v))
    for kv in a.text or []:
        k, _, path = kv.partition("=")
        with open(path) as f:
            put(k, f.read().strip())
    for kv in a.json or []:
        k, _, path = kv.partition("=")
        with open(path) as f:
            text = f.read().strip()
        try:
            put(k, json.loads(text))
        except ValueError:  # JSONEachRow: one document per line
            put(k, [json.loads(ln) for ln in text.splitlines() if ln.strip()])
    with open(a.out, "w") as f:
        json.dump(p, f, indent=2)
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sample")
    s.add_argument("--base", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--interval", type=float, default=0.5)
    s.add_argument("--max-runtime", type=float, default=7200)

    q = sub.add_parser("inserts-sql")
    q.add_argument("--table", required=True)
    qs = sub.add_parser("insert-settings-sql")
    qs.add_argument("--table", required=True)

    r = sub.add_parser("record")
    r.add_argument("--variant", required=True, choices=["native", "blackhole", "legacy"])
    r.add_argument("--trial", type=int, required=True)
    r.add_argument("--rows", type=int, required=True)
    r.add_argument("--image-id", required=True)
    r.add_argument("--samples", required=True)
    r.add_argument("--insert-settings")
    r.add_argument("--inserts")
    r.add_argument("--gate")
    r.add_argument("--row-bytes")
    r.add_argument("--clink-cpu-usec", type=int, nargs=2, required=True, metavar=("BEFORE", "AFTER"))
    r.add_argument("--server-cpu-usec", type=int, nargs=2, required=True, metavar=("BEFORE", "AFTER"))
    r.add_argument("--batch-rows", type=int, required=True)
    r.add_argument("--batch-bytes", type=int)
    r.add_argument("--batch-interval-ms", type=int, required=True)
    r.add_argument("--warmup", type=float, required=True)
    r.add_argument("--cooldown", type=float, required=True)
    r.add_argument("--out", required=True)

    lb = sub.add_parser("legacy-batch-rows")
    lb.add_argument("--campaign", required=True)

    sm = sub.add_parser("summarise")
    sm.add_argument("--campaign", required=True)

    pr = sub.add_parser("premise")
    pr.add_argument("--out", required=True)
    pr.add_argument("--set", action="append")
    pr.add_argument("--text", action="append")
    pr.add_argument("--json", action="append")

    a = ap.parse_args()
    if a.cmd == "premise":
        return cmd_premise(a)
    if a.cmd == "sample":
        return cmd_sample(a)
    if a.cmd == "inserts-sql":
        print(inserts_sql(a.table))
        return 0
    if a.cmd == "insert-settings-sql":
        print(insert_settings_sql(a.table))
        return 0
    if a.cmd == "record":
        return cmd_record(a)
    if a.cmd == "legacy-batch-rows":
        return cmd_legacy_batch_rows(a)
    return cmd_summarise(a)


if __name__ == "__main__":
    sys.exit(main())
