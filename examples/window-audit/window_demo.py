#!/usr/bin/env python3
"""The window-audit example's driver (see README.md; run.sh runs it).

Each scenario runs the windowed job on a fresh local cluster from the build,
against Kafka and ClickHouse on compose (the destination-reconciliation
example's stack and harness), feeds it events laid out in event time phase by
phase, collects a bundle, and audits it: `clink incident-scope` exports the
scope, reference_windows.py computes the windows over the job's whole lineage
read back from Kafka, and `clink incident-reconcile --reference` compares. A
second derivation checks the reference: the windows the scenario designed,
computed from its events' designed fates, which the events read back with
Kafka's console consumer must match.

Python 3 standard library only; Docker for the services.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RECONCILE = HERE.parent / "destination-reconciliation"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(RECONCILE))
import reconcile_demo  # noqa: E402
import window_workload as workload  # noqa: E402
from reconcile_demo import COMMANDS, Failed, logged  # noqa: E402

TABLE_DDL = """CREATE TABLE demo.{table} (
    account      String,
    window_start Int64,
    window_end   Int64,
    events       Int64,
    net          Decimal(38, 2),
    units        Int64
) ENGINE = MergeTree ORDER BY (account, window_start)
SETTINGS async_insert = 0, non_replicated_deduplication_window = {window}"""

# Defects put into the job on purpose, as (what the job says, what it says
# instead). Each is written faithfully by the sink: only a reference computed
# outside the job tells.
WINDOW_VARIANTS = {
    # Window assignment: the windows are cut on when the device sent the
    # event, not when it happened.
    "wrong-time-column": ("event_time_column = 'ts'", "event_time_column = 'sent_ts'"),
    # The aggregate: refunds add to the net amount instead of taking from it.
    "wrong-sign": ("CASE WHEN kind = 'refund' THEN -amount ELSE amount END",
                   "CASE WHEN kind = 'refund' THEN amount ELSE amount END"),
    # Late events: a window is held open five seconds past its end, so events
    # the specification drops are folded in, and windows close later.
    "late-treatment": ("watermark_lag_ms  = '2000'",
                       "watermark_lag_ms  = '2000',\n    allowed_lateness_ms = '5000'"),
    # Not a defect: a configuration the audit does not support. A quiet
    # partition leaves the watermark after five seconds of wall-clock time.
    "idleness": ("watermark_lag_ms  = '2000'",
                 "watermark_lag_ms  = '2000',\n    idle_timeout_ms   = '5000'"),
}


class WindowCluster(reconcile_demo.Cluster):
    """Three source subtasks for the three partitions, one partition each."""
    job_template = HERE / "job.sql.in"
    parallelism = workload.PARTITIONS
    variants = WINDOW_VARIANTS

    def start(self, topic, table, ch_port):
        sql = (HERE / "job.sql.in").read_text()
        if self.variant == "wrong-time-column":
            # The window must be cut on the column the watermark advances on.
            sql = sql.replace("TUMBLE(ts,", "TUMBLE(sent_ts,").replace(
                "SELECT account, ts, units,", "SELECT account, sent_ts, units,")
            path = self.dir / "job.sql.in"
            path.write_text(sql)
            self.job_template = path
        super().start(topic, table, ch_port)

    def recorded_watermarks(self):
        """checkpoint -> the least watermark the sink's subtasks recorded at its
        barrier (None when any recorded none), for the completed checkpoints."""
        out = {}
        done = set(self.completed())
        by_epoch = {}
        for f in (self.dir / "capture").glob("op-*/subtask-*/epoch-*.sink.json"):
            epoch = int(f.name.split("-", 1)[1].split(".", 1)[0])
            try:
                wm = json.loads(f.read_text()).get("watermark_ms")
            except (OSError, ValueError):
                continue
            by_epoch.setdefault(epoch, []).append(wm)
        for epoch, wms in by_epoch.items():
            if epoch in done and len(wms) == self.parallelism:
                out[epoch] = None if any(w is None for w in wms) else min(wms)
        return out

    def sink_rows(self, epoch):
        total = 0
        for f in (self.dir / "capture").glob("op-*/subtask-*/epoch-%d.sink.json" % epoch):
            total += int(json.loads(f.read_text()).get("records", {}).get("seen", 0))
        return total

    def await_watermark(self, target, exact=False, holds=2, timeout=120):
        """Waits until the last `holds` completed checkpoints all recorded a
        watermark at (or, exact, equal to) `target`; returns the first of them."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            marks = self.recorded_watermarks()
            done = sorted(marks)
            tail = done[-holds:]
            if len(tail) == holds and all(
                    marks[c] is not None and (marks[c] == target if exact else marks[c] >= target)
                    for c in tail):
                return tail[0]
            time.sleep(0.2)
        raise Failed("the recorded watermark did not %s %d (last: %s)"
                     % ("hold at" if exact else "reach", target,
                        sorted(self.recorded_watermarks().items())[-3:]))

    def await_settled(self, holds=2, timeout=120):
        """Waits until the last `holds` completed checkpoints recorded one and
        the same watermark: the job has read what was produced and its
        watermark has stopped. Returns the first of them."""
        self.await_more(1, timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            marks = self.recorded_watermarks()
            tail = sorted(marks)[-holds:]
            if len(tail) == holds and marks[tail[0]] is not None and \
                    all(marks[c] == marks[tail[0]] for c in tail):
                return tail[0]
            time.sleep(0.2)
        raise Failed("the recorded watermark did not settle (last: %s)"
                     % sorted(self.recorded_watermarks().items())[-3:])

    def await_more(self, n, timeout=120):
        have = len(self.completed())
        self.await_completed(have + n, timeout)

    def await_quiet(self, n=3, timeout=120):
        """Waits until the last `n` completed checkpoints' intervals were all
        empty at the sink: the run has stopped writing."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            marks = self.recorded_watermarks()
            done = sorted(marks)
            if len(done) >= n and all(self.sink_rows(c) == 0 for c in done[-n:]):
                return done[-n:]
            time.sleep(0.3)
        raise Failed("the run did not go quiet")


class Demo(reconcile_demo.Demo):
    def create_table(self, table, window=1000):
        self.services.ch(TABLE_DDL.format(table=table, window=window))

    def produce(self, topic, events):
        self.services.kafka("/opt/kafka/bin/kafka-console-producer.sh",
                            "--bootstrap-server=kafka:9092", "--topic=%s" % topic,
                            "--property", "parse.key=true", "--property", "key.separator=\t",
                            input_text="".join(e.line() for e in events))

    def reconcile(self, label, bundle, checkpoint, port=None, extra=()):
        """incident-reconcile with no --key: a windowed job's key is its
        windows' identity, which the bundle names."""
        report = self.results / "reports" / ("%s.json" % label)
        expected = self.results / "expected" / ("%s.ndjson" % label)
        args = [str(self.bin / "clink"), "incident-reconcile", str(bundle), "--host=127.0.0.1",
                "--port=%d" % (port or self.services.ch_port), "--user=reconciler",
                "--password-env=CLINK_RECONCILE_PASSWORD", "--checkpoint=%d" % checkpoint,
                "--report=%s" % report, "--expected-out=%s" % expected, *extra]
        env = dict(os.environ, CLINK_RECONCILE_PASSWORD=reconcile_demo.RECONCILE_PASSWORD)
        t0 = time.time()
        r = subprocess.run(logged(args, label), capture_output=True, text=True, env=env, timeout=600)
        elapsed = time.time() - t0
        (self.results / "reports" / ("%s.txt" % label)).write_text(r.stdout + r.stderr)
        data = json.loads(report.read_text()) if report.exists() and report.stat().st_size else None
        if data is not None:
            data["_elapsed_s"] = elapsed
        return r.returncode, data, r.stdout + r.stderr

    def reference(self, label, scope_path, run_start=0, extra=()):
        out = self.results / "references" / ("%s.reference.ndjson" % label)
        args = [self.reference_python, str(HERE / "reference_windows.py"),
                "--scope=%s" % scope_path, "--bootstrap=127.0.0.1:%d" % self.services.kafka_port,
                "--out=%s" % out, "--run-start-offset=ALL=%d" % run_start, *extra]
        t0 = time.time()
        r = subprocess.run(logged(args, label), capture_output=True, text=True, timeout=900,
                           env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
        elapsed = time.time() - t0
        (self.results / "references" / ("%s.reference.txt" % label)).write_text(r.stdout + r.stderr)
        return r.returncode, out, elapsed, (r.stdout + r.stderr).strip()

    def scope_window(self, label, bundle, checkpoint):
        out = self.results / "references" / ("%s.scope.json" % label)
        args = [str(self.bin / "clink"), "incident-scope", str(bundle),
                "--checkpoint=%d" % checkpoint, "--out=%s" % out]
        t0 = time.time()
        r = subprocess.run(logged(args, label), capture_output=True, text=True, timeout=120)
        elapsed = time.time() - t0
        (self.results / "references" / ("%s.scope.txt" % label)).write_text(r.stdout + r.stderr)
        return r.returncode, out, elapsed, (r.stdout + r.stderr).strip()

    def run(self):
        self.say("Starting Kafka and ClickHouse (compose project %s)" % self.services.project)
        self.services.up()
        self.setup_clickhouse()
        versions = {
            "clickhouse": self.services.ch("SELECT version()").strip(),
            "kafka_image": "apache/kafka:3.9.1",
            "clink": next((line.split(None, 1)[1] for line in subprocess.run(
                [str(self.bin / "clink"), "--capabilities"], capture_output=True,
                text=True).stdout.splitlines() if line.startswith("version ")), ""),
        }
        (self.results / "versions.json").write_text(json.dumps(versions, indent=2))
        self.note("ClickHouse %s, clink %s" % (versions["clickhouse"], versions["clink"]))
        import window_scenarios  # noqa: E402  (beside this file)
        for name, fn in window_scenarios.ALL:
            if not self.want(name):
                continue
            self.say("Scenario: %s" % name)
            try:
                fn(self)
            except Exception as e:  # a scenario's own Failed, or anything it did not foresee
                self.outcome(name, "the scenario ran", "to the end", "stopped", False,
                             "%s: %s" % (type(e).__name__, e))
        failed = [o for o in self.outcomes if not o["passed"]]
        (self.results / "scenarios.json").write_text(json.dumps(self.outcomes, indent=2))
        (self.results / "commands.json").write_text(json.dumps(COMMANDS, indent=2))
        self.say("%d check(s), %d failed" % (len(self.outcomes), len(failed)))
        return 0 if not failed and self.outcomes else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--project", default="clink-window-audit")
    ap.add_argument("--kafka-port", type=int, default=29292)
    ap.add_argument("--clickhouse-port", type=int, default=29200)
    ap.add_argument("--rate", type=int, default=200)
    ap.add_argument("--only", default="")
    ap.add_argument("--reference-python", default=sys.executable)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    demo = Demo(args)
    try:
        return demo.run()
    finally:
        if not args.keep:
            demo.services.down()


if __name__ == "__main__":
    sys.exit(main())
