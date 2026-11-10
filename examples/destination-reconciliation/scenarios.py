"""The example's scenarios (see README.md), in the order run.sh runs them.

Each takes the Demo and records outcomes with demo.outcome(); a Failed
exception stops the scenario and fails it.
"""
import json
import subprocess
import threading
import time
from decimal import Decimal
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
        scode, scope_path = demo.scope("healthy-scope", bundle, n)
        rcode, ref, _, rtext = demo.reference("healthy-reference", scope_path)
        acode, areport, _ = demo.reconcile("healthy-audit", bundle, n, extra=["--reference=%s" % ref])
        v = verdicts(areport)
        demo.outcome("healthy", "the audit on selected keys, the job still writing",
                     "exit 0, every check passed, the table outside the keys not established",
                     "scope exit %d, reference exit %d, audit exit %d, %s" % (scode, rcode, acode, sorted(set(v.values()))),
                     scode == 0 and rcode == 0 and acode == 0 and set(v.values()) == {"passed"} and
                     not areport["coverage"]["complete_established"], rtext[-160:])
        wcode, wreport, _ = demo.reconcile("healthy-whole", bundle, n, extra=["--coverage=complete-table"])
        wv = verdicts(wreport)
        demo.outcome("healthy", "a whole-table read of a table the job is still writing",
                     "exit 3, destination.coverage unavailable", "exit %d, %s" % (wcode, wv.get("destination.coverage")),
                     wcode == 3 and wv.get("destination.coverage") == "unavailable",
                     next((c["reason"] for c in wreport["checks"] if c["id"] == "destination.coverage"), "")[:200]
                     if wreport else "")
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
    scode, scope_path = demo.scope("expired-scope", h["bundle"], h["checkpoint"])
    rcode, ref, _, rtext = demo.reference("expired-reference", scope_path)
    demo.outcome("expired history", "the reference over the expired range", "exit 3: incomplete, saying the "
                 "offsets are no longer in the topic", "exit %d" % rcode,
                 scode == 0 and rcode == 3 and "no longer in the topic" in rtext, rtext[-200:])
    code, report, _ = demo.reconcile("expired-audit", h["bundle"], h["checkpoint"],
                                     extra=["--reference=%s" % ref])
    v = verdicts(report)
    demo.outcome("expired history", "the audit with that reference",
                 "reference.scope unavailable; exit 1, as the corruption above still fails",
                 "exit %d, reference.scope %s" % (code, v.get("reference.scope")),
                 code == 1 and v.get("reference.scope") == "unavailable")


def recreated_topic(demo):
    """The topic deleted and created again under its name while no Worker
    runs the job. The reconciliation reads no source and keeps its verdict;
    the independent check finds another log than the one the source recorded
    and does not read it; and the job, restoring on a Worker started after,
    refuses to seek the old topic's positions in the new one, so none of the
    new topic's events reaches the table through them."""
    name = "recreated-topic"
    topic, table = "payments-recreated", "payments_recreated"
    demo.services.create_topic(topic)
    demo.create_table(table)
    cluster = Cluster(demo, name, defer_worker2=True)
    cluster.start(topic, table, demo.services.ch_port)
    feeder = Feeder(demo, topic, 1, 200_000, demo.feed_rate).start()
    try:
        cluster.await_completed(6, timeout=180)
        bundle = demo.work / "bundles" / name
        n = cluster.collect(bundle)["checkpoint"]
        feeder.stop()
        code, report, _ = demo.reconcile(name + "-before", bundle, n)
        demo.outcome(name, "checkpoint %d, before the topic is created again" % n, "exit 0",
                     "exit %d" % code, code == 0)
        if report is not None and code in (0, 1):
            demo.agree(name, "checkpoint %d" % n, code, report,
                       demo.independent(name + "-before", topic, table, report))

        w1 = cluster.procs["worker-1"]
        w1.kill()
        w1.wait(timeout=60)
        old = demo.services.topic_log(topic)
        demo.services.recreate_topic(topic)
        new = demo.services.topic_log(topic)
        demo.outcome(name, "the topic deleted and created again under its name", "another topic id",
                     "%s, then %s" % (old and old["topic_id"], new and new["topic_id"]),
                     old is not None and new is not None and new["topic_id"] != old["topic_id"])
        demo.services.produce(topic, range(1_000_001, 1_002_001))

        code, report, _ = demo.reconcile(name + "-after", bundle, n)
        demo.outcome(name, "incident-reconcile, which reads only the bundle and the destination",
                     "exit 0", "exit %d" % code, code == 0)
        ind = demo.independent(name + "-after", topic, table, report)
        demo.outcome(name, "the independent check on the topic created again",
                     "unavailable: another log", ind["verdict"],
                     ind["verdict"] == "unavailable" and "Kafka now has" in ind.get("why", ""),
                     ind.get("why", ""))
        scode, scope_path = demo.scope(name + "-scope", bundle, n)
        rcode, ref, _, rtext = demo.reference(name + "-reference", scope_path)
        demo.outcome(name, "the reference over the topic created again",
                     "exit 3: incomplete, the topic deleted and created again", "exit %d" % rcode,
                     scode == 0 and rcode == 3 and "deleted and created again" in rtext, rtext[-200:])
        code, report, _ = demo.reconcile(name + "-audit", bundle, n, extra=["--reference=%s" % ref])
        v = verdicts(report)
        demo.outcome(name, "the audit with that reference", "exit 3, reference.scope unavailable",
                     "exit %d, reference.scope %s" % (code, v.get("reference.scope")),
                     code == 3 and v.get("reference.scope") == "unavailable")

        cluster.worker(2)
        log = cluster.dir / "logs" / "worker-2.log"
        deadline = time.time() + 120
        refusal = ""
        while time.time() < deadline and not refusal:
            text = log.read_text(errors="replace") if log.exists() else ""
            refusal = next((line for line in text.splitlines()
                            if "deleted and created again" in line), "")
            time.sleep(0.5)
        refusal = refusal[refusal.find("KafkaSource:"):] if "KafkaSource:" in refusal else refusal
        named = bool(refusal) and old["topic_id"] in refusal and new["topic_id"] in refusal
        demo.outcome(name, "the job restoring on worker-2", "refuses the old topic's positions, "
                     "naming both topic ids", "refused" if refusal else "not refused", named,
                     refusal[:400])
        time.sleep(5)
        leaked = int(demo.services.ch("SELECT count() FROM demo.%s WHERE event_id > 1000000"
                                      % table).strip())
        demo.outcome(name, "rows from the new topic's events in the table", "0", str(leaked),
                     leaked == 0)
    finally:
        feeder.stop()
        cluster.stop()


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
    on, five times each, alternating: time to land every row; the Workers'
    CPU and what the recorder wrote, once a checkpoint has completed after
    the rows landed; medians reported. Then the cost of reconciling the
    largest checkpoint scope of the last run with it on."""
    import statistics
    count = 40_000
    want = sum(1 for i in range(1, count + 1) if workload.reference_row(workload.event(i)))
    topic = "payments-overhead"
    demo.services.create_topic(topic)
    demo.services.produce(topic, range(1, count + 1))
    runs = {"off": [], "on": []}
    reconcile_cost = None
    audit_cost = None
    for rep in range(5):
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
            # The recorder writes an epoch when the next barrier passes, so the
            # work it does for these rows is done only once a checkpoint
            # completes after they landed. Both modes wait for one, so their
            # CPU covers the same span.
            done = len(cluster.completed())
            cluster.await_completed(done + 1, timeout=120)
            cpu = sum(cpu_seconds(cluster.procs[w].pid) or 0.0 for w in ("worker-1", "worker-2"))
            runs[mode].append({"rows": landed, "seconds_to_land": round(wall, 2),
                               "worker_cpu_seconds": round(cpu, 2),
                               "capture_bytes": dir_bytes(cluster.dir / "capture"),
                               "capture_files": sum(1 for _ in (cluster.dir / "capture").rglob("*")
                                                    if _.is_file())})
            if mode == "on" and rep == 4:
                # Two empty intervals after the audited checkpoint, for the
                # whole-table audit below.
                cluster.await_completed(len(cluster.completed()) + 3, timeout=120)
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
            if mode == "on" and rep == 4:
                audit_cost = measure_audit(demo, {"name": name, "table": table, "bundle": bundle,
                                                  "checkpoint": completed_in(bundle)[-3]})
    med = {}
    for mode in ("off", "on"):
        med[mode] = {k: statistics.median(r[k] for r in runs[mode])
                     for k in ("seconds_to_land", "worker_cpu_seconds", "capture_bytes", "capture_files")}
    results = {"events": count, "rows_owed": want, "parallelism": 2, "checkpoint_interval_ms": 1000,
               "runs": runs, "median": med, "reconcile": reconcile_cost, "audit": audit_cost,
               "cpu_increase_pct_median": round(100.0 * (med["on"]["worker_cpu_seconds"] -
                                                         med["off"]["worker_cpu_seconds"]) /
                                                max(med["off"]["worker_cpu_seconds"], 0.01), 1)}
    (demo.results / "measurements.json").write_text(json.dumps(results, indent=2))
    demo.note("recorder off, medians of 5: %(seconds_to_land)s s to land, %(worker_cpu_seconds)s Worker CPU s"
              % med["off"])
    demo.note("recorder on,  medians of 5: %(seconds_to_land)s s to land, %(worker_cpu_seconds)s Worker CPU s, "
              "%(capture_bytes)d bytes in %(capture_files)d files" % med["on"])
    demo.note("reconciling checkpoint %(checkpoint)d: %(rows_compared)d rows, %(queries)d queries, "
              "%(seconds)s s" % reconcile_cost)
    demo.outcome("overhead", "every run landed every row it owes", "%d rows x 10" % want,
                 ", ".join(str(r["rows"]) for m in ("off", "on") for r in runs[m]),
                 all(r["rows"] == want for m in ("off", "on") for r in runs[m]))
    demo.outcome("overhead", "the reconciliation of the largest scope", "exit 0",
                 "exit %d" % reconcile_cost["exit"], reconcile_cost["exit"] == 0)
    demo.note("auditing the whole run of %(rows)d rows: scope %(scope_seconds)s s, reference "
              "%(reference_seconds)s s (%(records_read)d records read, %(artifact_bytes)d bytes), "
              "audit %(audit_seconds)s s" % audit_cost)
    demo.outcome("overhead", "the whole run audited against the reference, the table read whole",
                 "reference exit 0, audit exit 0", "reference exit %d, audit exit %d"
                 % (audit_cost["reference_exit"], audit_cost["exit"]),
                 audit_cost["reference_exit"] == 0 and audit_cost["exit"] == 0)


def measure_audit(demo, run):
    """The cost of the independent audit on one bounded run: exporting the
    scope, computing the reference over the whole run's source range, and the
    audit with its whole-table read."""
    demo.services.ch("OPTIMIZE TABLE demo.%s FINAL" % run["table"])
    t0 = time.time()
    code, scope_path = demo.scope("overhead-audit", run["bundle"], run["checkpoint"], whole_run=True)
    scope_s = time.time() - t0
    if code != 0:
        raise Failed("incident-scope exited %d" % code)
    rcode, ref, ref_s, text = demo.reference("overhead-audit", scope_path, 0)
    acode, report, _ = audit(demo, run, "overhead-audit", ref)
    records = sum(int(e.get("records", 0)) for line in Path(ref).read_text(encoding="utf-8").splitlines()
                  if "end" in (o := json.loads(line)) for e in o["end"]["read"])
    return {"checkpoint": run["checkpoint"], "rows": len(artifact_rows(ref)), "records_read": records,
            "artifact_bytes": Path(ref).stat().st_size, "scope_seconds": round(scope_s, 3),
            "reference_seconds": round(ref_s, 3), "reference_exit": rcode,
            "audit_seconds": round(report["_elapsed_s"], 3) if report else None, "exit": acode,
            "table_rows_read": report["coverage"]["table_rows_read"] if report else None}


# -- the independent reference audit --------------------------------------------------

def completed_in(bundle):
    """The completed checkpoints a bundle holds markers for."""
    return sorted(int(m.name.split("-", 1)[1]) for m in (Path(bundle) / "checkpoint" / "_jobs" / "1").glob("COMPLETED-*")
                  if m.name.split("-", 1)[1].isdigit())


def verdicts(report):
    return {c["id"]: c["verdict"] for c in report["checks"]} if report else {}


def counts(report, check):
    c = next((c for c in (report or {}).get("checks", []) if c["id"] == check), None)
    return c.get("counts", {}) if c else {}


def kept(e, variant):
    """Whether the job, with `variant`'s defect, writes a row for event e."""
    kinds = ("purchase", "refund", "chargeback") if variant == "extra-output" else ("purchase", "refund")
    floor = 1000 if variant == "faulty-filter" else 100
    return e["kind"] in kinds and e["cents"] >= floor


def bounded_run(demo, name, ids, variant=None):
    """One bounded run into a table dedicated to it: the events `ids` are in a
    topic of their own before the job starts, the job reads them all, two more
    checkpoints complete with nothing in them, the cluster stops, and the
    operator merges the table's parts so nothing changes it while it is read."""
    topic, table = "payments-" + name, "payments_" + name.replace("-", "_")
    s = demo.services
    s.create_topic(topic)
    demo.create_table(table)
    s.produce(topic, ids)
    writes = sum(1 for i in ids if kept(workload.event(i), variant))
    cluster = Cluster(demo, name, variant=variant)
    cluster.start(topic, table, s.ch_port)
    try:
        cluster.await_completed(3, timeout=180)
        deadline, last, steady = time.time() + 300, -1, 0
        while time.time() < deadline:
            n = int(s.ch("SELECT count() FROM demo.%s" % table).strip() or 0)
            steady = steady + 1 if n == last else 0
            last = n
            if n == writes and steady >= 4:
                break
            time.sleep(0.5)
        else:
            raise Failed("the run into %s did not settle at %d rows (it holds %d)" % (table, writes, last))
        cluster.await_completed(len(cluster.completed()) + 3, timeout=120)
        bundle = demo.work / "bundles" / name
        cluster.collect(bundle)
    finally:
        cluster.stop()
    s.ch("OPTIMIZE TABLE demo.%s FINAL" % table)
    # The audited checkpoint has two more after it in the bundle, both empty:
    # the bundle's own evidence that the run had stopped writing.
    checkpoint = completed_in(bundle)[-3]
    demo.note("bounded run %s: %d events, %d rows written, audited at checkpoint %d"
              % (name, len(ids), writes, checkpoint))
    return {"name": name, "topic": topic, "table": table, "bundle": bundle,
            "checkpoint": checkpoint, "writes": writes, "ids": ids}


def reference_for(demo, run, label, whole_run=True, checkpoint=None, extra=()):
    """incident-scope, then the example's reference over the scope it wrote."""
    ckpt = checkpoint or run["checkpoint"]
    code, scope_path = demo.scope(label, run["bundle"], ckpt, whole_run)
    if code != 0:
        raise Failed("incident-scope exited %d for %s" % (code, label))
    rcode, ref, secs, text = demo.reference(label, scope_path, 0 if whole_run else None, extra)
    return rcode, ref, scope_path, secs, text


def audit(demo, run, label, ref, coverage="complete-table", extra=()):
    args = ["--reference=%s" % ref, "--coverage=%s" % coverage, *extra]
    if coverage == "selected-keys":
        args.append("--from-run-start")
    return demo.reconcile(label, run["bundle"], run["checkpoint"], extra=args)


def source_events(demo, scope_path):
    """Every event in the scope's ranges, read back with Kafka's own console
    consumer (the Java client, not the reference's librdkafka). The example's
    topics have no transactions, so a range's offsets are all records; a
    range that starts at the run's start is read from 0, where they begin."""
    doc = json.loads(Path(scope_path).read_text())
    events = []
    for src in doc["sources"]:
        for r in src["partitions"]:
            start = r["from"] if r["from"] is not None else 0
            got = demo.services.read_range(src["topic"], r["partition"], start, r["to"])
            if [o for o, _ in got] != list(range(start, r["to"])):
                raise Failed("%s[%d] [%d, %d) could not be read back whole"
                             % (src["topic"], r["partition"], start, r["to"]))
            for _, value in got:
                v = json.loads(value, parse_float=Decimal)
                events.append({"event_id": v["event_id"], "kind": v["kind"], "account": v["account"],
                               "cents": int(Decimal(v["amount"]) * 100), "ts": v["ts"], "note": v["note"]})
    return events


def artifact_rows(path):
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        o = json.loads(line)
        if "row" in o:
            rows.append(o["row"])
    return rows


def independent_audit_check(demo, scenario, label, scope_path, ref_path, report, variant):
    """The audit's verdict, checked from the source: the reference artifact's
    rows must be workload.reference_row() of the events read back with the
    console consumer, and reference.capture's counts the ones `variant`'s
    defect gives those events."""
    events = source_events(demo, scope_path)
    want = {}
    for e in events:
        row = workload.reference_row(e)
        if row is not None:
            want[row["event_id"]] = row
    got = artifact_rows(ref_path)
    same = len(got) == len(want) and {r["event_id"]: r for r in got} == want
    demo.outcome(scenario, label + ": the reference artifact is the source's transformation, computed again",
                 "same rows", "same rows" if same else "%d rows against %d" % (len(got), len(want)), same)
    ref_keys = {e["event_id"] for e in events if kept(e, None)}
    job_keys = {e["event_id"] for e in events if kept(e, variant)}
    changed = len(ref_keys & job_keys) if variant in ("wrong-projection", "equal-totals") else 0
    expect = {"missing": len(ref_keys - job_keys), "changed": changed,
              "unexpected": len(job_keys - ref_keys), "duplicated": 0}
    c = counts(report, "reference.capture")
    observed = {k: c.get(k) for k in expect}
    demo.outcome(scenario, label + ": reference.capture's counts agree with the source",
                 str(expect), str(observed), observed == expect)
    return expect


def audit_healthy(demo):
    """A faithful job, a reference that agrees, and a dedicated table read
    whole: every required check passes."""
    ids = list(range(1, 3001))
    run = bounded_run(demo, "audit-healthy", ids)
    demo.audit_run = run
    rcode, ref, scope_path, secs, text = reference_for(demo, run, "audit-healthy")
    demo.outcome("audit-healthy", "the reference", "complete (exit 0)", "exit %d" % rcode, rcode == 0, text)
    code, report, _ = audit(demo, run, "audit-healthy", ref)
    v = verdicts(report)
    every = all(x == "passed" for x in v.values())
    demo.outcome("audit-healthy", "incident-reconcile --reference --coverage=complete-table",
                 "exit 0, every check passed", "exit %d, %s" % (code, sorted(set(v.values()))),
                 code == 0 and every and "destination.coverage" in v)
    demo.outcome("audit-healthy", "coverage", "the table read whole",
                 str(report["coverage"]["complete_established"]), report["coverage"]["complete_established"])
    independent_audit_check(demo, "audit-healthy", "audit-healthy", scope_path, ref, report, None)
    code, report, _ = audit(demo, run, "audit-healthy-selected", ref, coverage="selected-keys")
    cov = report["coverage"] if report else {}
    demo.outcome("audit-healthy", "the same audit on selected keys", "exit 0, coverage not complete",
                 "exit %d, complete %s" % (code, cov.get("complete_established")),
                 code == 0 and cov.get("complete_established") is False and
                 "not established" in cov.get("complete_reason", ""))
    run["reference"] = ref
    run["scope"] = scope_path


def audit_destination(demo):
    """Faults in the destination alone, on the healthy run's dedicated table: a
    row of a key neither side names, which only the whole table shows; then a
    changed value and a duplicate, which fail the delivery and not the
    transformation."""
    run = demo.audit_run
    s = demo.services
    s.ch("INSERT INTO demo.%s SELECT 999999999, 'acct-99', 'purchase', 5.00, 500, 14.5, false, NULL, "
         "toDateTime64('2026-01-01 00:00:00', 3, 'UTC') SETTINGS insert_deduplicate = 0" % run["table"])
    code, report, _ = audit(demo, run, "audit-unrelated-whole", run["reference"])
    v = verdicts(report)
    demo.outcome("audit-destination", "an unrelated key, the table read whole",
                 "exit 1: destination.coverage failed, 1 unexpected, the rest passed",
                 "exit %d, %s, unexpected %s" % (code, v.get("destination.coverage"),
                                                counts(report, "destination.coverage").get("unexpected")),
                 code == 1 and v.get("destination.coverage") == "failed" and
                 counts(report, "destination.coverage").get("unexpected") == 1 and
                 all(v[k] == "passed" for k in ("reference.capture", "reconcile.rows", "reference.destination")))
    code, report, _ = audit(demo, run, "audit-unrelated-selected", run["reference"], coverage="selected-keys")
    cov = report["coverage"] if report else {}
    demo.outcome("audit-destination", "the same, on selected keys", "exit 0, saying rows of other keys were not read",
                 "exit %d: %s" % (code, cov.get("complete_reason", "")[:90]),
                 code == 0 and not cov.get("complete_established") and "not established" in cov.get("complete_reason", ""))
    s.ch("ALTER TABLE demo.%s DELETE WHERE event_id = 999999999 SETTINGS mutations_sync = 2" % run["table"])
    # Kept, so the faults below can be undone for the scenarios after them.
    s.ch("CREATE TABLE demo.%s_kept AS demo.%s" % (run["table"], run["table"]))
    s.ch("INSERT INTO demo.%s_kept SELECT * FROM demo.%s" % (run["table"], run["table"]))
    victim = min(int(r["event_id"][1:]) for r in artifact_rows(run["reference"]))
    s.ch("ALTER TABLE demo.%s UPDATE note = 'changed' WHERE event_id = %d SETTINGS mutations_sync = 2"
         % (run["table"], victim))
    s.ch("INSERT INTO demo.%s SELECT * FROM demo.%s WHERE event_id = (SELECT max(event_id) FROM demo.%s) "
         "SETTINGS insert_deduplicate = 0" % (run["table"], run["table"], run["table"]))
    code, report, _ = audit(demo, run, "audit-delivery", run["reference"])
    v = verdicts(report)
    rel = {f["relationship"] for f in report["audit"]["findings"]} if report else set()
    rows = counts(report, "reconcile.rows")
    demo.outcome("audit-destination", "a changed value and a duplicate in the destination",
                 "exit 1: the delivery failed (1 changed, 1 duplicated), the transformation passed",
                 "exit %d, reference.capture %s, reconcile.rows %s (changed %s, duplicated %s), findings %s"
                 % (code, v.get("reference.capture"), v.get("reconcile.rows"), rows.get("changed"),
                    rows.get("duplicated"), sorted(rel)),
                 code == 1 and v.get("reference.capture") == "passed" and v.get("reconcile.rows") == "failed" and
                 rows.get("changed") == 1 and rows.get("duplicated") == 1 and "delivery" in rel and
                 "transformation" not in rel)
    s.ch("TRUNCATE TABLE demo.%s" % run["table"])
    s.ch("INSERT INTO demo.%s SELECT * FROM demo.%s_kept SETTINGS insert_deduplicate = 0" % (run["table"], run["table"]))
    s.ch("OPTIMIZE TABLE demo.%s FINAL" % run["table"])


def audit_refusals(demo):
    """References that cannot be compared: one computed for another scope, an
    unfinished one, one that says it is incomplete, and a table past the bound
    of a whole read."""
    run = demo.audit_run
    earlier = max(c for c in completed_in(run["bundle"]) if c < run["checkpoint"])
    _, other, _, _, _ = reference_for(demo, run, "audit-other-scope", checkpoint=earlier)
    code, _, text = audit(demo, run, "audit-wrong-scope", other)
    demo.outcome("audit-refusals", "a reference computed for checkpoint %d, audited at %d" % (earlier, run["checkpoint"]),
                 "exit 2 (refused), naming the checkpoint", "exit %d" % code,
                 code == 2 and "another scope" in text and "checkpoint" in text)
    cut = demo.results / "references" / "audit-unfinished.reference.ndjson"
    lines = Path(run["reference"]).read_bytes().split(b"\n")
    cut.write_bytes(b"\n".join(lines[:-2]) + b"\n")
    code, _, text = audit(demo, run, "audit-unfinished", cut)
    demo.outcome("audit-refusals", "a reference without its end line", "exit 2 (refused)", "exit %d" % code,
                 code == 2 and "no end line" in text)
    rcode, short, _, _, rtext = reference_for(demo, run, "audit-bounded-reference", extra=["--max-records=10"])
    code, report, _ = audit(demo, run, "audit-incomplete", short)
    v = verdicts(report)
    demo.outcome("audit-refusals", "a reference whose read reached its bound",
                 "the reference exits 3; the audit exits 3, reference.scope unavailable",
                 "reference exit %d, audit exit %d, %s" % (rcode, code, v.get("reference.scope")),
                 rcode == 3 and code == 3 and v.get("reference.scope") == "unavailable", rtext[-160:])
    code, report, _ = audit(demo, run, "audit-table-bound", run["reference"], extra=["--max-table-rows=10"])
    v = verdicts(report)
    demo.outcome("audit-refusals", "a whole read past --max-table-rows", "exit 3, destination.coverage unavailable",
                 "exit %d, %s" % (code, v.get("destination.coverage")),
                 code == 3 and v.get("destination.coverage") == "unavailable")


def faulty_transformation(demo, variant, scenario):
    """A job with a defect writes what it computes, faithfully: reconciliation
    of its own record with the destination passes, and only the reference
    fails it."""
    ids = list(range(1, 3001))
    run = bounded_run(demo, scenario, ids, variant=variant)
    plain, _, _ = demo.reconcile(scenario + "-reconcile-only", run["bundle"], run["checkpoint"],
                                 extra=["--from-run-start"])
    demo.outcome(scenario, "incident-reconcile without a reference", "exit 0: the delivery was faithful",
                 "exit %d" % plain, plain == 0)
    rcode, ref, scope_path, _, text = reference_for(demo, run, scenario)
    code, report, _ = audit(demo, run, scenario, ref)
    v = verdicts(report)
    rel = [f["relationship"] for f in report["audit"]["findings"]] if report else []
    demo.outcome(scenario, "incident-reconcile --reference", "exit 1, the transformation named, the delivery passed",
                 "exit %d, reference.capture %s, reconcile.rows %s, findings %s"
                 % (code, v.get("reference.capture"), v.get("reconcile.rows"), rel),
                 rcode == 0 and code == 1 and v.get("reference.capture") == "failed" and
                 v.get("reconcile.rows") == "passed" and rel and rel[0] == "transformation" and
                 "delivery" not in rel)
    expect = independent_audit_check(demo, scenario, scenario, scope_path, ref, report, variant)
    return run, report, expect


def wrong_projection(demo):
    faulty_transformation(demo, "wrong-projection", "wrong-projection")


def faulty_filter(demo):
    faulty_transformation(demo, "faulty-filter", "faulty-filter")


def extra_output(demo):
    faulty_transformation(demo, "extra-output", "extra-output")


def equal_totals(demo):
    run, report, expect = faulty_transformation(demo, "equal-totals", "equal-totals")
    ref_rows = artifact_rows(demo.results / "references" / "equal-totals.reference.ndjson")
    ref_count = len(ref_rows)
    ref_total = sum(int(r["amount"][1:].split("@")[0]) for r in ref_rows)
    got = demo.services.ch("SELECT count(), toString(sum(toInt64(amount * 100))) FROM demo.%s" % run["table"]).split()
    demo.outcome("equal-totals", "the destination's count and total against the reference's",
                 "equal (%d rows, %d cents)" % (ref_count, ref_total), "%s rows, %s cents" % (got[0], got[1]),
                 int(got[0]) == ref_count and int(got[1]) == ref_total)


def empty_output(demo):
    """A run whose every event the filter drops: the expected output is
    genuinely empty. It passes only when the whole table is read and is empty,
    and fails when the table holds anything."""
    ids = [i for i in range(1, 6001) if workload.reference_row(workload.event(i)) is None][:1500]
    run = bounded_run(demo, "empty-output", ids)
    rcode, ref, _, _, text = reference_for(demo, run, "empty-output")
    demo.outcome("empty-output", "the reference", "complete and empty", "exit %d, %d rows" % (rcode, len(artifact_rows(ref))),
                 rcode == 0 and not artifact_rows(ref), text)
    code, report, _ = audit(demo, run, "empty-selected", ref, coverage="selected-keys")
    v = verdicts(report)
    demo.outcome("empty-output", "on selected keys", "exit 3: nothing selected, the destination not established",
                 "exit %d, reference.destination %s" % (code, v.get("reference.destination")),
                 code == 3 and v.get("reference.destination") == "unavailable")
    code, report, _ = audit(demo, run, "empty-whole", ref)
    demo.outcome("empty-output", "the empty table read whole", "exit 0", "exit %d" % code, code == 0)
    demo.services.ch("INSERT INTO demo.%s SELECT 999999998, 'acct-98', 'refund', 2.00, 200, 5.8, false, NULL, "
                     "toDateTime64('2026-01-01 00:00:00', 3, 'UTC') SETTINGS insert_deduplicate = 0"
                     % run["table"])
    code, report, _ = audit(demo, run, "empty-whole-not-empty", ref)
    v = verdicts(report)
    demo.outcome("empty-output", "a row in the table that should be empty", "exit 1, destination.coverage failed",
                 "exit %d, %s" % (code, v.get("destination.coverage")),
                 code == 1 and v.get("destination.coverage") == "failed")


ALL = [
    ("healthy", healthy),
    ("identity", identity),
    ("missing-evidence", missing_evidence),
    ("limits", limits),
    ("corruption", corruption),
    ("expired-history", expired_history),
    ("recreated-topic", recreated_topic),
    ("audit-healthy", audit_healthy),
    ("audit-destination", audit_destination),
    ("audit-refusals", audit_refusals),
    ("wrong-projection", wrong_projection),
    ("faulty-filter", faulty_filter),
    ("extra-output", extra_output),
    ("equal-totals", equal_totals),
    ("empty-output", empty_output),
    ("truncated", truncated),
    ("crash-after-publish", crash_after_publish),
    ("crash-before-publish", crash_before_publish),
    ("crash-after-ack", crash_after_ack),
    ("retry-dedup-log", retry_with_log),
    ("retry-no-log", retry_without_log),
    ("overhead", overhead),
]
