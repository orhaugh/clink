"""The example's scenarios (see README.md), in the order run.sh runs them.

Each takes the Demo and records outcomes with demo.outcome(); a Failed
exception stops the scenario and fails it.
"""
import json
import subprocess
import threading
import time
from pathlib import Path

import workload
from reconcile_demo import Cluster, Failed, HERE

REPO = HERE.parent.parent


class Feeder:
    """Produces events start_id.. at `rate` a second through one console
    producer, until stop() or `count` events."""

    def __init__(self, demo, topic, start_id, count, rate):
        self.demo, self.topic = demo, topic
        self.next_id, self.end = start_id, start_id + count
        self.rate = rate
        self.stop_flag = False
        self.proc = subprocess.Popen(
            ["docker", "compose", "--project-directory", str(HERE), "-f",
             str(HERE / "docker-compose.yml"), "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server=kafka:9092",
             "--topic=%s" % topic, "--property", "parse.key=true", "--property", "key.separator=\t"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True,
            env=demo.services.env)
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        tick = 0.1
        per_tick = max(1, self.rate // 10)
        while not self.stop_flag and self.next_id < self.end:
            t0 = time.time()
            upto = min(self.end, self.next_id + per_tick)
            for i in range(self.next_id, upto):
                self.proc.stdin.write("%s\t%s\n" % workload.record(i))
            self.proc.stdin.flush()
            self.next_id = upto
            time.sleep(max(0.0, tick - (time.time() - t0)))

    def stop(self):
        self.stop_flag = True
        self.thread.join(timeout=30)
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        return self.next_id


def verify_offline(demo, scenario, bundle):
    """incident-verify on the bundle with the network denied, as the incident
    example does: offline verification stays network-free, and reconciliation
    is the separate, explicit read."""
    report = demo.results / "reports" / ("%s-offline-verify.json" % scenario)
    script = (". '%s/scripts/network-isolation.sh'; isolation_establish || "
              "{ echo \"isolation: $ISOLATION_STATE: $ISOLATION_DETAIL\" >&2; exit 4; }; "
              "offline '%s/clink' incident-verify '%s' --profile=replay --report='%s' >/dev/null"
              % (REPO, demo.bin, bundle, report))
    r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=600)
    if r.returncode == 4:
        demo.outcome(scenario, "incident-verify with the network denied", "a verdict", "not run",
                     False, r.stderr.strip())
        return
    data = json.loads(report.read_text())
    replay = {c["id"]: c["verdict"] for c in data["checks"]}
    demo.outcome(scenario, "incident-verify --profile=replay with the network denied",
                 "exit 0 or 3, never 1", "exit %d (%s)" % (r.returncode, data["result"]),
                 r.returncode in (0, 3), "bundle.artifacts %s, replay.determinism %s" % (
                     replay.get("bundle.artifacts"), replay.get("replay.determinism")))


def healthy(demo):
    topic, table = "payments-healthy", "payments_healthy"
    demo.services.create_topic(topic)
    demo.create_table(table)
    cluster = Cluster(demo, "healthy")
    cluster.start(topic, table, demo.services.ch_port)
    feeder = Feeder(demo, topic, 1, 200_000, demo.feed_rate).start()
    try:
        cluster.await_completed(6, timeout=180)
        bundle = demo.work / "bundles" / "healthy"
        info = cluster.collect(bundle)
        n = info["checkpoint"]
        demo.note("collected checkpoint %d while the job runs: %d files, %d bytes" %
                  (n, info["files"], info["bytes"]))
        code, report, text = demo.reconcile("healthy", bundle, n)
        later = max(cluster.completed())
        demo.outcome("healthy", "incident-reconcile on checkpoint %d" % n, "exit 0", "exit %d" % code,
                     code == 0, text.strip().splitlines()[-1] if text.strip() else "")
        if report is not None and code in (0, 1):
            ind = demo.independent("healthy", topic, table, report)
            demo.agree("healthy", "the reconciliation", code, report, ind)
        demo.outcome("healthy", "the job kept writing while it was read", "a later checkpoint",
                     "checkpoint %d completed" % later, later > n)
        verify_offline(demo, "healthy", bundle)
        demo.healthy = {"bundle": bundle, "checkpoint": n, "topic": topic, "table": table}
    finally:
        feeder.stop()
        cluster.stop()


def rows_counts(report):
    c = next(c for c in report["checks"] if c["id"] == "reconcile.rows").get("counts", {})
    return (c.get("missing"), c.get("changed"), c.get("unexpected"), c.get("duplicated"))


def scope_rows(demo, label):
    """The reconciled scope's expected rows, from incident-reconcile's export."""
    path = demo.results / "expected" / ("%s.ndjson" % label)
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def identity(demo):
    h = demo.healthy
    bundle, n, table = h["bundle"], h["checkpoint"], h["table"]
    code, _, text = demo.reconcile("identity-wrong-run", bundle, n, extra=["--job=2"])
    demo.outcome("identity", "another job's id (--job=2)", "exit 2 (refused)", "exit %d" % code, code == 2,
                 text.strip().splitlines()[-2] if text.strip() else "")
    code, _, _ = demo.reconcile("identity-wrong-checkpoint", bundle, 999999)
    demo.outcome("identity", "a checkpoint the bundle does not hold as completed", "exit 2", "exit %d" % code,
                 code == 2)
    code, _, _ = demo.reconcile("identity-wrong-table", bundle, n, extra=["--table=payments_other"])
    demo.outcome("identity", "another table named (--table)", "exit 2", "exit %d" % code, code == 2)
    # The same name, another table: dropped and recreated, as a redeploy does.
    s = demo.services
    s.ch("RENAME TABLE demo.%s TO demo.%s_hidden" % (table, table))
    s.ch("CREATE TABLE demo.%s AS demo.%s_hidden" % (table, table))
    try:
        code, _, text = demo.reconcile("identity-recreated-table", bundle, n)
        demo.outcome("identity", "the table recreated under the same name (another UUID)", "exit 2",
                     "exit %d" % code, code == 2 and "UUID" in text)
    finally:
        s.ch("DROP TABLE demo.%s SYNC" % table)
        s.ch("RENAME TABLE demo.%s_hidden TO demo.%s" % (table, table))
    code, _, _ = demo.reconcile("identity-restored", bundle, n)
    demo.outcome("identity", "the original table back", "exit 0", "exit %d" % code, code == 0)


def corruption(demo):
    """Each kind of discrepancy, applied on purpose to the example's own
    disposable table, one after another; the reported counts must be the
    cumulative ones, and the independent check must agree each time."""
    h = demo.healthy
    bundle, n, table, topic = h["bundle"], h["checkpoint"], h["table"], h["topic"]
    rows = scope_rows(demo, "healthy")
    big = [r for r in rows if int(r["amount"][1:].split("@")[0]) >= 500]
    if len(big) < 6:
        raise Failed("the scope holds fewer than six rows to corrupt")
    a, b, c, d, e, f = (int(r["event_id"][1:]) for r in big[:6])
    s = demo.services
    steps = [
        ("equal count and total, two rows changed",
         ["ALTER TABLE demo.{t} UPDATE amount = amount + 1, amount_cents = amount_cents + 100 "
          "WHERE event_id = %d SETTINGS mutations_sync = 2" % a,
          "ALTER TABLE demo.{t} UPDATE amount = amount - 1, amount_cents = amount_cents - 100 "
          "WHERE event_id = %d SETTINGS mutations_sync = 2" % b], (0, 2, 0, 0)),
        ("a row written twice", ["INSERT INTO demo.{t} SELECT * FROM demo.{t} WHERE event_id = %d" % c],
         (0, 2, 0, 1)),
        ("an extra row for a key of the scope",
         ["INSERT INTO demo.{t} SELECT event_id, account, kind, amount + 5, amount_cents + 500, fee, "
          "flagged, note, occurred_at FROM demo.{t} WHERE event_id = %d" % d], (0, 2, 1, 1)),
        ("a row deleted", ["ALTER TABLE demo.{t} DELETE WHERE event_id = %d SETTINGS mutations_sync = 2" % e],
         (1, 2, 1, 1)),
        ("a float changed in its last bits",
         ["ALTER TABLE demo.{t} UPDATE fee = fee + 1e-9 WHERE event_id = %d SETTINGS mutations_sync = 2" % f],
         (1, 3, 1, 1)),
    ]
    for i, (what, sqls, want) in enumerate(steps, 1):
        for sql in sqls:
            s.ch(sql.format(t=table))
        label = "corruption-%d" % i
        code, report, _ = demo.reconcile(label, bundle, n)
        got = rows_counts(report) if report and code == 1 else None
        demo.outcome("corruption", "%s: missing/changed/unexpected/duplicated" % what,
                     "exit 1, %s" % (want,), "exit %d, %s" % (code, got), code == 1 and got == want)
        ind = demo.independent(label, topic, table, report)
        demo.agree("corruption", "%s:" % what, code, report, ind)
        if i == 1:
            demo.outcome("corruption", "the swap left the scope's count and total as they were",
                         "equal", "count %d/%d, cents %d/%d" % (ind["count_observed"], ind["count_reference"],
                                                                ind["sum_cents_observed"], ind["sum_cents_reference"]),
                         ind["count_observed"] == ind["count_reference"] and
                         ind["sum_cents_observed"] == ind["sum_cents_reference"])


def missing_evidence(demo):
    h = demo.healthy
    cluster_dir = demo.work / "healthy"
    out = demo.work / "bundles" / "healthy-no-captures"
    r = subprocess.run([str(demo.bin / "clink"), "incident-collect",
                        "--checkpoint-dir=%s" % (cluster_dir / "checkpoints"), "--job=1",
                        "--ha-dir=%s" % (cluster_dir / "ha"), "--include-state",
                        "--checkpoint=%d" % h["checkpoint"], "--out=%s" % out],
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        raise Failed("collection without captures failed: %s" % r.stderr[-800:])
    code, _, text = demo.reconcile("missing-evidence-no-captures", out, h["checkpoint"])
    demo.outcome("missing evidence", "a bundle collected without --include-captures", "exit 2 (refused)",
                 "exit %d" % code, code == 2 and "no sink interval evidence" in text)


def limits(demo):
    h = demo.healthy
    code, report, _ = demo.reconcile("limits-rows", h["bundle"], h["checkpoint"],
                                     extra=["--max-rows-per-query=1"])
    rows = next((c for c in report["checks"] if c["id"] == "reconcile.rows"), {}) if report else {}
    demo.outcome("limits", "a read past --max-rows-per-query", "exit 3, reconcile.rows unavailable",
                 "exit %d, %s" % (code, rows.get("verdict")), code == 3 and rows.get("verdict") == "unavailable")
    # A server that stops answering: the proxy forwards nothing either way.
    import proxy as fault_proxy
    p = fault_proxy.FaultProxy(demo.services.ch_port)
    p.stall()
    try:
        t0 = time.time()
        code, report, _ = demo.reconcile("limits-timeout", h["bundle"], h["checkpoint"], port=p.port,
                                         extra=["--query-timeout-ms=1000"])
        took = time.time() - t0
    finally:
        p.close()
    demo.outcome("limits", "a destination that stops answering (--query-timeout-ms=1000)", "exit 3",
                 "exit %d after %.1f s" % (code, took), code == 3)


def truncated(demo):
    topic, table = "payments-truncated", "payments_truncated"
    demo.services.create_topic(topic)
    demo.create_table(table)
    cluster = Cluster(demo, "truncated", capture_records=5)
    cluster.start(topic, table, demo.services.ch_port)
    feeder = Feeder(demo, topic, 1, 200_000, demo.feed_rate).start()
    try:
        cluster.await_completed(3, timeout=180)
        bundle = demo.work / "bundles" / "truncated"
        n = cluster.collect(bundle)["checkpoint"]
        code, report, text = demo.reconcile("truncated", bundle, n)
        scope = next((c for c in report["checks"] if c["id"] == "reconcile.scope"), {}) if report else {}
        demo.outcome("missing evidence", "a sink capture truncated at --capture-records=5",
                     "exit 3, reconcile.scope unavailable", "exit %d, %s" % (code, scope.get("verdict")),
                     code == 3 and scope.get("verdict") == "unavailable")
    finally:
        feeder.stop()
        cluster.stop()


def expired_history(demo):
    """The reconciliation reads no source history; the independent check
    does, and once Kafka has deleted part of the range it cannot run."""
    h = demo.healthy
    _, report, _ = demo.reconcile("expired-before", h["bundle"], h["checkpoint"])
    ranges = report["scope"]["source"]["ranges"]
    r0 = ranges[0]
    cut = (r0["from"] + r0["to"]) // 2
    demo.services.delete_before(h["topic"], r0["partition"], cut)
    code, report, _ = demo.reconcile("expired-after", h["bundle"], h["checkpoint"])
    ind = demo.independent("expired-after", h["topic"], h["table"], report)
    demo.outcome("expired history", "the independent check once offsets [%d, %d) of partition %d are deleted"
                 % (r0["from"], cut, r0["partition"]), "unavailable", ind["verdict"],
                 ind["verdict"] == "unavailable", ind.get("why", ""))
    demo.outcome("expired history", "incident-reconcile, which reads only the bundle and the destination",
                 "exit 1 (the corruption above), not unavailable", "exit %d" % code, code == 1)


def restore_scenario(demo, name, faults, window=1000):
    """worker-1 runs the job alone and dies at a fault point; the job restores
    on worker-2. The completed checkpoint before the death, the restored
    run's first and a later one are each reconciled, and the independent
    check must agree with every verdict."""
    topic, table = "payments-%s" % name, "payments_%s" % name.replace("-", "_")
    demo.services.create_topic(topic)
    demo.create_table(table, window)
    cluster = Cluster(demo, name, worker1_faults=faults, worker2_late=True)
    cluster.start(topic, table, demo.services.ch_port)
    feeder = Feeder(demo, topic, 1, 200_000, demo.feed_rate).start()
    try:
        w1 = cluster.procs["worker-1"]
        try:
            code = w1.wait(timeout=180)
        except subprocess.TimeoutExpired:
            raise Failed("worker-1 never reached its fault point")
        log = (cluster.dir / "logs" / "worker-1.log").read_text(errors="replace")
        point = faults.split("=", 1)[0]
        demo.outcome(name, "worker-1 died at %s" % point, "exit 1 at the point",
                     "exit %d, fired %s" % (code, ("fired: " + point) in log), code == 1 and ("fired: " + point) in log)
        deadline = time.time() + 180
        restored = None
        while time.time() < deadline:
            rec = cluster.markers / "RESTORED"
            if rec.exists():
                kv = dict(line.split("=", 1) for line in rec.read_text().split() if "=" in line)
                restored = (int(kv.get("restore", 0)), int(kv.get("next", 0)))
                after = [c for c in cluster.completed() if c >= restored[1]]
                if restored[0] > 0 and len(after) >= 3:
                    break
            time.sleep(0.5)
        else:
            raise Failed("the job did not complete three checkpoints after restoring (%s)" % (restored,))
        before, nxt = restored
        after = [c for c in cluster.completed() if c >= nxt]
        demo.note("the job restored from checkpoint %d and numbers from %d; completed since: %s" %
                  (before, nxt, after[:5]))
        bundle = demo.work / "bundles" / name
        cluster.collect(bundle)
        for label, ckpt in (("before", before), ("first-after", after[0]), ("later", after[2])):
            code, report, text = demo.reconcile("%s-%s" % (name, label), bundle, ckpt)
            ind = demo.independent("%s-%s" % (name, label), topic, table, report) if report else None
            required = "exit 0" if label != "first-after" else "exit 0 or 1, as the destination is"
            ok = code == 0 if label != "first-after" else code in (0, 1)
            demo.outcome(name, "checkpoint %d (%s the restore)" % (ckpt, label.replace("-", " ")),
                         required, "exit %d" % code, ok)
            if ind is not None and code in (0, 1):
                demo.agree(name, "checkpoint %d" % ckpt, code, report, ind)
            if label == "first-after" and code == 1:
                rows = next(c for c in report["checks"] if c["id"] == "reconcile.rows")
                attributed = any("restored from checkpoint" in d for d in rows["details"])
                demo.outcome(name, "its duplicates are put down to the restore", "attributed",
                             "attributed" if attributed else "not attributed", attributed,
                             "%d duplicated" % rows["counts"]["duplicated"])
    finally:
        feeder.stop()
        cluster.stop()


def crash_after_publish(demo):
    restore_scenario(demo, "crash-after-publish", "checkpoint.after_publish=exit:1@40")


def crash_before_publish(demo):
    restore_scenario(demo, "crash-before-publish", "checkpoint.before_publish=exit:1@40")


def crash_after_ack(demo):
    restore_scenario(demo, "crash-after-ack", "clickhouse.after_end_insert=exit:1@60")


def ambiguous_retry(demo, name, window):
    """ClickHouse takes an INSERT and its answer is lost: the proxy forwards
    the rows and withholds the reply, then resets the connection. The sink is
    left in doubt and resends."""
    import proxy as fault_proxy
    topic, table = "payments-%s" % name, "payments_%s" % name.replace("-", "_")
    demo.services.create_topic(topic)
    demo.create_table(table, window)
    p = fault_proxy.FaultProxy(demo.services.ch_port)
    cluster = Cluster(demo, name)
    cluster.start(topic, table, p.port)
    feeder = Feeder(demo, topic, 1, 200_000, demo.feed_rate).start()
    try:
        cluster.await_completed(3, timeout=180)
        p.hold_next_insert()
        deadline = time.time() + 60
        while time.time() < deadline and not p.held_quiet(0.5):
            time.sleep(0.1)
        if not p.held_quiet(0.5):
            raise Failed("no INSERT reached its end with its reply withheld")
        p.reset_held()
        demo.note("the server took an INSERT whose reply was withheld, and the connection was reset")
        held_at = max(cluster.completed())
        cluster.await_completed(len(cluster.completed()) + 3, timeout=180)
        bundle = demo.work / "bundles" / name
        cluster.collect(bundle)
        # The interval whose ledger holds the INSERT left in doubt.
        doubted = None
        for f in sorted((cluster.dir / "capture").glob("op-*/subtask-*/epoch-*.sink.json")):
            doc = json.loads(f.read_text())
            for ins in (doc.get("sink") or {}).get("inserts") or []:
                if ins.get("in_doubt"):
                    doubted = (doc["checkpoint"], ins)
        if doubted is None:
            raise Failed("no interval's ledger records an INSERT in doubt")
        ckpt, ins = doubted
        demo.note("checkpoint %d's interval holds INSERT seq %d (%d rows), resent %s" %
                  (ckpt, ins["seq"], ins["rows"], ins.get("resend")))
        code, report, text = demo.reconcile(name, bundle, ckpt)
        ind = demo.independent(name, topic, table, report)
        rows = next(c for c in report["checks"] if c["id"] == "reconcile.rows")
        if window > 0:
            demo.outcome(name, "a resend under its own token, to a table with a deduplication log",
                         "exit 0, the resend named", "exit %d, resend %s" % (code, ins.get("resend")),
                         code == 0 and ins.get("resend") == "deduplicated" and
                         any("none is duplicated" in d for d in rows["details"]))
        else:
            dup = rows.get("counts", {}).get("duplicated")
            demo.outcome(name, "a resend to a table that keeps no deduplication log",
                         "exit 1, the INSERT's %d rows duplicated and put down to the resend" % ins["rows"],
                         "exit %d, %s duplicated, resend %s" % (code, dup, ins.get("resend")),
                         code == 1 and dup == ins["rows"] and ins.get("resend") == "may_duplicate" and
                         any("in doubt" in d for d in rows["details"]))
        demo.agree(name, "the reconciliation", code, report, ind)
    finally:
        feeder.stop()
        cluster.stop()
        p.close()


def retry_with_log(demo):
    ambiguous_retry(demo, "retry-dedup-log", 1000)


def retry_without_log(demo):
    ambiguous_retry(demo, "retry-no-log", 0)


def cpu_seconds(pid):
    """A running process's CPU time (user and system), from ps."""
    out = subprocess.run(["ps", "-o", "cputime=", "-p", str(pid)], capture_output=True,
                         text=True).stdout.strip()
    if not out:
        return None
    days = 0
    if "-" in out:
        d, out = out.split("-", 1)
        days = int(d)
    parts = [float(x) for x in out.split(":")]
    secs = 0.0
    for x in parts:
        secs = secs * 60 + x
    return days * 86400 + secs


def dir_bytes(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def overhead(demo):
    """One bounded workload, the same job with the flight recorder off and
    on, three times each, alternating: time to land every row, the Workers'
    CPU, what the recorder wrote; medians reported. Then the cost of
    reconciling the largest checkpoint scope of the last run with it on."""
    import statistics
    count = 40_000
    want = sum(1 for i in range(1, count + 1) if workload.reference_row(workload.event(i)))
    topic = "payments-overhead"
    demo.services.create_topic(topic)
    demo.services.produce(topic, range(1, count + 1))
    runs = {"off": [], "on": []}
    reconcile_cost = None
    for rep in range(3):
        for mode in ("off", "on"):
            name = "overhead-%s-%d" % (mode, rep)
            table = "payments_%s" % name.replace("-", "_")
            demo.create_table(table)
            cluster = Cluster(demo, name, capture=(mode == "on"))
            t0 = time.time()
            cluster.start(topic, table, demo.services.ch_port)
            landed = 0
            while time.time() - t0 < 600:
                landed = int(demo.services.ch("SELECT count() FROM demo.%s" % table).strip() or 0)
                if landed >= want:
                    break
                time.sleep(0.2)
            wall = time.time() - t0
            cpu = sum(cpu_seconds(cluster.procs[w].pid) or 0.0 for w in ("worker-1", "worker-2"))
            runs[mode].append({"rows": landed, "seconds_to_land": round(wall, 2),
                               "worker_cpu_seconds": round(cpu, 2),
                               "capture_bytes": dir_bytes(cluster.dir / "capture"),
                               "capture_files": sum(1 for _ in (cluster.dir / "capture").rglob("*")
                                                    if _.is_file())})
            if mode == "on" and rep == 2:
                bundle = demo.work / "bundles" / "overhead"
                info = cluster.collect(bundle)
                best, best_rows = None, -1
                for f in (cluster.dir / "capture").glob("op-*/subtask-*/epoch-*.sink.json"):
                    doc = json.loads(f.read_text())
                    if doc["checkpoint"] in cluster.completed() and doc["records"]["seen"] > best_rows:
                        best, best_rows = doc["checkpoint"], doc["records"]["seen"]
                code, report, _ = demo.reconcile("overhead", bundle, best)
                rows = next(c for c in report["checks"] if c["id"] == "reconcile.rows")
                reconcile_cost = {"checkpoint": best, "exit": code,
                                  "rows_compared": rows["counts"]["expected"],
                                  "queries": rows["counts"]["queries"],
                                  "seconds": round(report["_elapsed_s"], 3),
                                  "bundle_bytes": info["bytes"], "bundle_files": info["files"]}
            cluster.stop()
            if landed < want:
                raise Failed("the %s run landed %d of %d rows" % (name, landed, want))
    med = {}
    for mode in ("off", "on"):
        med[mode] = {k: statistics.median(r[k] for r in runs[mode])
                     for k in ("seconds_to_land", "worker_cpu_seconds", "capture_bytes", "capture_files")}
    results = {"events": count, "rows_owed": want, "parallelism": 2, "checkpoint_interval_ms": 1000,
               "runs": runs, "median": med, "reconcile": reconcile_cost,
               "cpu_increase_pct_median": round(100.0 * (med["on"]["worker_cpu_seconds"] -
                                                         med["off"]["worker_cpu_seconds"]) /
                                                max(med["off"]["worker_cpu_seconds"], 0.01), 1)}
    (demo.results / "measurements.json").write_text(json.dumps(results, indent=2))
    demo.note("recorder off, medians of 3: %(seconds_to_land)s s to land, %(worker_cpu_seconds)s Worker CPU s"
              % med["off"])
    demo.note("recorder on,  medians of 3: %(seconds_to_land)s s to land, %(worker_cpu_seconds)s Worker CPU s, "
              "%(capture_bytes)d bytes in %(capture_files)d files" % med["on"])
    demo.note("reconciling checkpoint %(checkpoint)d: %(rows_compared)d rows, %(queries)d queries, "
              "%(seconds)s s" % reconcile_cost)
    demo.outcome("overhead", "every run landed every row it owes", "%d rows x 6" % want,
                 ", ".join(str(r["rows"]) for m in ("off", "on") for r in runs[m]),
                 all(r["rows"] == want for m in ("off", "on") for r in runs[m]))
    demo.outcome("overhead", "the reconciliation of the largest scope", "exit 0",
                 "exit %d" % reconcile_cost["exit"], reconcile_cost["exit"] == 0)


ALL = [
    ("healthy", healthy),
    ("identity", identity),
    ("missing-evidence", missing_evidence),
    ("limits", limits),
    ("corruption", corruption),
    ("expired-history", expired_history),
    ("truncated", truncated),
    ("crash-after-publish", crash_after_publish),
    ("crash-before-publish", crash_before_publish),
    ("crash-after-ack", crash_after_ack),
    ("retry-dedup-log", retry_with_log),
    ("retry-no-log", retry_without_log),
    ("overhead", overhead),
]
