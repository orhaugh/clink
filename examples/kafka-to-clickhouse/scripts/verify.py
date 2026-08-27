#!/usr/bin/env python3
"""Check ClickHouse against what the workload says it must contain.

    ./scripts/verify.py                 # waits up to two minutes for the pipeline to catch up
    ./scripts/verify.py --timeout 30

The expectation is recomputed here from scripts/workload.py, independently of
anything clink wrote. Every closed window is compared: its reading count, min
and max must match exactly and its average to within 1e-6. The script also
reports how many rows ClickHouse holds per window: after a Worker restart some
windows are present twice, because clink delivers to ClickHouse at least once.
Those copies must carry identical values.

Exit status 0 means every check passed.
"""

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import workload  # noqa: E402

AVG_TOLERANCE = 1e-6

# Raw rows grouped by key: how many copies of each window ClickHouse holds, and
# whether those copies agree with each other. Not FINAL, on purpose: the point
# is to see the duplicates before ReplacingMergeTree collapses them. The
# aggregate aliases differ from the column names because ClickHouse resolves
# an alias before the column it shadows, which turns `any(x) AS x` inside
# another aggregate into an illegal nested aggregation.
QUERY = """
SELECT sensor_id,
       window_start,
       count()                                                             AS copies,
       uniqExact((window_end, readings, avg_temp_c, min_temp_c, max_temp_c)) AS variants,
       any(window_end)  AS we,
       any(readings)    AS n,
       any(avg_temp_c)  AS avg_c,
       any(min_temp_c)  AS lo,
       any(max_temp_c)  AS hi
FROM sensor_window_stats
GROUP BY sensor_id, window_start
ORDER BY sensor_id, window_start
FORMAT JSONEachRow
"""


def query(url: str, user: str, password: str, sql: str) -> list:
    req = urllib.request.Request(url.rstrip("/") + "/", data=sql.encode(), method="POST")
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode()
    except urllib.error.HTTPError as e:
        # ClickHouse puts the reason in the body; without it a 500 says nothing.
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode(errors='replace').strip()}") from None
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def fetch(url, user, password):
    rows = {}
    for r in query(url, user, password, QUERY):
        rows[(r["sensor_id"], int(r["window_start"]))] = r
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"),
                    help="ClickHouse HTTP endpoint (default http://localhost:8123, or $CLICKHOUSE_URL)")
    ap.add_argument("--user", default="clink")
    ap.add_argument("--password", default="clink")
    ap.add_argument("--timeout", type=float, default=120.0,
                    help="seconds to wait for every closed window to appear (default 120)")
    args = ap.parse_args()

    expected = workload.expected_windows()
    deadline = time.monotonic() + args.timeout
    last_report = None
    rows = {}
    while True:
        try:
            rows = fetch(args.url, args.user, args.password)
        except (urllib.error.URLError, OSError, RuntimeError) as e:
            print(f"verify: cannot query ClickHouse at {args.url}: {e}", file=sys.stderr)
            print("verify: is the stack running? (docker compose ps)", file=sys.stderr)
            return 2
        present = sum(1 for k in expected if k in rows)
        if present != last_report:
            print(f"verify: {present} / {len(expected)} closed windows in ClickHouse")
            last_report = present
        if present == len(expected) or time.monotonic() >= deadline:
            break
        time.sleep(2)

    failures = []
    missing = [k for k in expected if k not in rows]
    for sensor, ws in missing[:10]:
        failures.append(f"missing: {sensor} window {workload.fmt_ts(ws)}")
    if len(missing) > 10:
        failures.append(f"... and {len(missing) - 10} more missing windows")

    foreign = [k for k in rows if k not in expected and k[1] != workload.OPEN_WINDOW_START_MS]
    for sensor, ws in foreign[:10]:
        failures.append(f"unexpected: {sensor} window {workload.fmt_ts(ws)} (not part of the workload)")
    if any(k[1] == workload.OPEN_WINDOW_START_MS for k in rows):
        failures.append(f"unexpected: the window starting {workload.fmt_ts(workload.OPEN_WINDOW_START_MS)} "
                        "fired, but the watermark never passes its end")

    copies_total = 0
    duplicated = []
    for key, exp in expected.items():
        r = rows.get(key)
        if r is None:
            continue
        sensor, ws = key
        label = f"{sensor} window {workload.fmt_ts(ws)}"
        copies_total += int(r["copies"])
        if int(r["copies"]) > 1:
            duplicated.append((label, int(r["copies"])))
        if int(r["variants"]) != 1:
            failures.append(f"{label}: {r['copies']} copies with {r['variants']} different values")
        count, avg, lo, hi = exp
        if int(r["we"]) != ws + workload.WINDOW_MS:
            failures.append(f"{label}: window_end {r['we']}, expected {ws + workload.WINDOW_MS}")
        if int(r["n"]) != count:
            failures.append(f"{label}: readings {r['n']}, expected {count}")
        if abs(float(r["avg_c"]) - avg) > AVG_TOLERANCE:
            failures.append(f"{label}: avg_temp_c {r['avg_c']}, expected {avg:.4f}")
        if float(r["lo"]) != lo:
            failures.append(f"{label}: min_temp_c {r['lo']}, expected {lo}")
        if float(r["hi"]) != hi:
            failures.append(f"{label}: max_temp_c {r['hi']}, expected {hi}")

    checked = len(expected) - len(missing)
    print(f"verify: {checked} windows compared with the expectation recomputed from workload.py")
    if not missing and not failures:
        print("verify: every window's reading count, min and max match exactly; every average within 1e-6")
    if duplicated:
        print(f"verify: {copies_total} rows for {checked} windows: {len(duplicated)} windows were inserted "
              f"more than once, all copies identical (at-least-once delivery after a restart)")
        for label, n in duplicated[:8]:
            print(f"        {label}: {n} copies")
        if len(duplicated) > 8:
            print(f"        ... and {len(duplicated) - 8} more")
        print("        SELECT ... FROM sensor_window_stats FINAL collapses them to one row per window")
    elif checked:
        print(f"verify: {copies_total} rows for {checked} windows: no duplicates")

    if failures:
        print("verify: FAIL", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        if missing:
            print("  (windows still missing after the wait: is the job running? "
                  "docker compose logs submit coordinator worker)", file=sys.stderr)
        if any("readings" in f for f in failures):
            print("  (readings off by a multiple: produce_events.py run more than once on this stack? "
                  "docker compose down -v starts clean)", file=sys.stderr)
        return 1
    print("verify: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
