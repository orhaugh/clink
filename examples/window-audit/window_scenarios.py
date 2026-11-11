"""The window-audit example's scenarios (README.md lists them).

Each feeds the job events laid out in event time, phase by phase, waiting
between phases for what the scenario needs the job to have done: the
watermark the sink recorded at a completed checkpoint to reach a value, or a
number of checkpoints to pass. That is what makes a record's fate decided by
the evidence rather than by how the job happened to interleave its partitions.
"""
import json
import time
from decimal import Decimal
from pathlib import Path

import window_workload as workload
from reconcile_demo import Failed
from window_demo import WindowCluster
from window_workload import BASE_MS, LAG_MS, SIZE_MS, Timeline

E = 30_000  # the window boundary the main timeline's watermark stops exactly at


# -- helpers -------------------------------------------------------------------------

def verdicts(report):
    return {c["id"]: c["verdict"] for c in (report or {}).get("checks", [])}


def counts(report, check):
    c = next((c for c in (report or {}).get("checks", []) if c["id"] == check), None)
    return c.get("counts", {}) if c else {}


def findings(report):
    return [f["relationship"] for f in (report or {}).get("audit", {}).get("findings", [])]


def window_audit(report):
    return (report or {}).get("audit", {}).get("window", {})


def artifact_parts(path):
    rows, windows, end = [], [], None
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        o = json.loads(line)
        if "row" in o:
            rows.append(o["row"])
        elif "window" in o:
            windows.append(o["window"])
        elif "end" in o:
            end = o["end"]
    return rows, windows, end


def start_run(demo, name, variant=None, faults="", defer_worker2=False, worker2_late=False):
    topic, table = "payments-" + name, "windows_" + name.replace("-", "_")
    demo.services.create_topic(topic, workload.PARTITIONS)
    demo.create_table(table)
    cluster = WindowCluster(demo, name, variant=variant, worker1_faults=faults,
                            defer_worker2=defer_worker2, worker2_late=worker2_late)
    cluster.start(topic, table, demo.services.ch_port)
    return {"name": name, "topic": topic, "table": table, "cluster": cluster}


def main_timeline(demo, run, timeline, marks, faithful=True, on_idle=None):
    """The main timeline, phase by phase, recording in `marks` the completed
    checkpoints the scenarios audit at:

    1. Every partition steady to E, then each one's last event at E + lag: the
       watermark stops exactly at E, so the window ending at E closes and the
       one starting there stays open. `exact` is a checkpoint recorded there.
    2. Records for the window that closed, one at its last millisecond, which
       are dropped, and one at E itself, folded into the next window.
    3. Partition 2 goes quiet while 0 and 1 run 28 seconds ahead: with no
       idleness it holds the watermark at E, so no window closes. A record on
       partition 0 far behind its own partition's maximum is still folded, its
       window held open. `idle` is a checkpoint recorded in that stretch.
       `on_idle` runs then (a Worker killed, with the windows after E open).
    4. Partition 2 catches up and all three run on to E + 50 s; the windows
       up to E + 40 s close. Then nothing more, and the run goes quiet.

    A job with a deliberate defect (faithful False) may assign windows on
    another time, so the waits are for its recorded watermark to settle rather
    than for the value a faithful job reaches."""
    c = run["cluster"]
    topic = run["topic"]
    t = timeline
    phase1 = t.steady([0, 1, 2], 0, E) + [t.at(p, E + LAG_MS, kind="purchase") for p in (0, 1, 2)]
    demo.produce(topic, phase1)
    if faithful:
        marks["exact"] = c.await_watermark(BASE_MS + E, exact=True, holds=2)
        demo.note("phase 1: the watermark held at exactly E (%d) from checkpoint %d"
                  % (BASE_MS + E, marks["exact"]))
    else:
        marks["exact"] = c.await_settled(2)
    phase2 = [t.at(0, E - 5_000, fate="dropped", kind="purchase"),
              t.at(1, E - 1, fate="dropped", kind="refund"),
              t.at(2, E, fate="folded", kind="purchase")]
    demo.produce(topic, phase2)
    c.await_more(2)
    phase3 = t.steady([0, 1], E + LAG_MS + 100, E + 30_000) + [
        t.at(0, E + 1_000, fate="folded", kind="purchase")]
    demo.produce(topic, phase3)
    if faithful:
        marks["idle"] = c.await_watermark(BASE_MS + E, exact=True, holds=3)
    else:
        c.await_more(2)
        marks["idle"] = c.await_settled(2)
    c.await_more(1)
    demo.note("phase 3: partition 2 quiet; checkpoint %d recorded the watermark there" % marks["idle"])
    if on_idle is not None:
        on_idle()
    phase4 = t.steady([2], E + LAG_MS + 100, E + 30_000) + t.steady([0, 1, 2], E + 30_000, E + 50_000)
    demo.produce(topic, phase4)
    if faithful:
        c.await_watermark(BASE_MS + E + 40_000, holds=1, timeout=240)
    else:
        c.await_more(2, timeout=240)
        c.await_settled(2, timeout=240)
    marks["quiet"] = c.await_quiet(3)
    return phase1 + phase2 + phase3 + phase4


def collect(demo, run, extra=()):
    bundle = demo.work / "bundles" / run["name"]
    run["cluster"].collect(bundle, extra=extra)
    run["bundle"] = bundle
    return bundle


def reference_for(demo, run, label, checkpoint, extra=()):
    code, scope_path, scope_s, text = demo.scope_window(label, run["bundle"], checkpoint)
    if code != 0:
        raise Failed("incident-scope exited %d for %s: %s" % (code, label, text[-300:]))
    rcode, ref, ref_s, rtext = demo.reference(label, scope_path, 0, extra)
    return {"scope": scope_path, "reference": ref, "exit": rcode, "text": rtext,
            "scope_s": scope_s, "reference_s": ref_s}


def audit(demo, run, label, checkpoint, ref, coverage="selected-keys", extra=()):
    return demo.reconcile(label, run["bundle"], checkpoint,
                          extra=["--reference=%s" % ref, "--coverage=%s" % coverage, *extra])


def read_back(demo, scope_path):
    """Every record of the scope's ranges, read with Kafka's own console
    consumer: (partition, event_id). The example's topics have no
    transactions, so a range's offsets are all records, from 0."""
    doc = json.loads(Path(scope_path).read_text())
    got = []
    for src in doc["sources"]:
        for r in src["partitions"]:
            records = demo.services.read_range(src["topic"], r["partition"], 0, r["to"])
            if [o for o, _ in records] != list(range(0, r["to"])):
                raise Failed("%s[%d] [0, %d) could not be read back whole"
                             % (src["topic"], r["partition"], r["to"]))
            got += [(r["partition"], workload.event_id_of(v)) for _, v in records]
    return doc, got


def designed_check(demo, scenario, label, scope_path, ref_path, events, lateness=0):
    """The reference's rows against the windows the scenario designed: the
    events read back from Kafka (each where it was meant to land), their
    designed fates, and the watermark the scope says windows are final by."""
    doc, got = read_back(demo, scope_path)
    by_id = workload.events_by_value(events)
    misplaced = [i for p, i in got if i not in by_id or by_id[i].partition != p]
    demo.outcome(scenario, label + ": every event read back is where the scenario put it",
                 "0 misplaced", "%d misplaced of %d" % (len(misplaced), len(got)), not misplaced)
    final_by = doc["finality"]["watermark_ms"]
    want = workload.design_windows([by_id[i] for _, i in got if i in by_id], final_by, lateness)
    rows, _, _ = artifact_parts(ref_path)
    have = {(r["account"][1:], int(r["window_start"][1:]), int(r["window_end"][1:])):
            [int(r["events"][1:]), int(Decimal(r["net"][1:].split("@")[0])),
             int(r["units"][1:])] for r in rows}
    same = have == want
    detail = ""
    if not same:
        diff = sorted(set(have) ^ set(want)) or sorted(k for k in want if have.get(k) != want[k])
        detail = "first difference %s: reference %s, design %s" % (
            diff[0], have.get(diff[0]), want.get(diff[0]))
    demo.outcome(scenario, label + ": the reference's final windows are the designed ones",
                 "%d windows" % len(want), "%d windows%s" % (len(have), "" if same else ", differ"),
                 same, detail)
    return same


# -- the scenarios -------------------------------------------------------------------

def every_passed(report):
    v = verdicts(report)
    return bool(v) and all(x == "passed" for x in v.values())


def not_passed(report):
    return sorted(k for k, x in verdicts(report).items() if x != "passed")


def windows_main(demo):
    """The main timeline on a faithful job: windows across many checkpoints,
    keys and partitions; out-of-order events within the lag; events on window
    boundaries; the watermark exactly at a window's end; late events before
    and after their window closed; a quiet partition holding every window
    open. Audited at the checkpoint the watermark stopped exactly at E, at one
    in the quiet stretch, and at the end with the table read whole. A second
    bundle collected without the run's first intervals has lost the history
    a window depends on."""
    name = "windows-main"
    run = start_run(demo, name)
    marks = {}
    try:
        events = main_timeline(demo, run, Timeline(seed=1), marks)
        collect(demo, run)
        cut = demo.work / "bundles" / "windows-main-from-3"
        run["cluster"].collect(cut, extra=["--epochs=3-1000000"])
    finally:
        run["cluster"].stop()
    demo.services.ch("OPTIMIZE TABLE demo.%s FINAL" % run["table"])
    demo.main_run = dict(run, events=events, marks=marks)
    # The first of the quiet checkpoints: two empty intervals follow it.
    end = marks["quiet"][0]
    for label, ckpt, coverage in (("main-exact", marks["exact"], "selected-keys"),
                                  ("main-idle", marks["idle"], "selected-keys"),
                                  ("main-end", end, "complete-table")):
        ref = reference_for(demo, run, label, ckpt)
        demo.outcome(name, label + ": the reference", "complete (exit 0)", "exit %d" % ref["exit"],
                     ref["exit"] == 0, ref["text"][-200:])
        code, report, text = audit(demo, run, label, ckpt, ref["reference"], coverage)
        wa = window_audit(report)
        demo.outcome(name, label + ": incident-reconcile --reference (%s)" % coverage,
                     "exit 0, every check passed", "exit %d, %s" % (code, not_passed(report)),
                     code == 0 and every_passed(report))
        marks_at = {m["checkpoint"]: m for m in wa.get("watermarks", [])}
        equal = sum(1 for m in marks_at.values() if m["recorded_ms"] == m["source_bound_ms"])
        demo.note("%s: checkpoint %d, %s final, %s open, %s undetermined; the recorded watermark "
                  "equals the source's bound at %d of %d lineage checkpoints"
                  % (label, ckpt, wa.get("final"), wa.get("open"), wa.get("undetermined"), equal,
                     len(marks_at)))
        designed_check(demo, name, label, ref["scope"], ref["reference"], events)
        if label == "main-exact":
            demo.outcome(name, "main-exact: the recorded watermark is exactly the window's end",
                         str(BASE_MS + E), str(marks_at.get(ckpt, {}).get("recorded_ms")),
                         marks_at.get(ckpt, {}).get("recorded_ms") == BASE_MS + E)
            _, windows, _ = artifact_parts(ref["reference"])
            opened = {(w["identity"]["window_start"], w["status"]) for w in windows}
            demo.outcome(name, "main-exact: the window starting at E is open, not final",
                         "open", str(sorted(st for s_, st in opened if s_ == "I%d" % (BASE_MS + E))),
                         ("I%d" % (BASE_MS + E), "open") in opened)
        if label == "main-idle":
            rows = artifact_parts(ref["reference"])[0]
            past = sum(1 for r in rows if int(r["window_end"][1:]) > BASE_MS + E)
            demo.outcome(name, "main-idle: nothing past E is final while partition 2 is quiet",
                         "0 final windows end past E", str(past), past == 0)
        if label == "main-end":
            rec = wa.get("records", {})
            demo.outcome(name, "main-end: the late events are accounted for",
                         "2 dropped late, 0 undetermined",
                         "%s dropped, %s undetermined" % (rec.get("dropped_late"), rec.get("undetermined")),
                         rec.get("dropped_late") == 2 and rec.get("undetermined") == 0)
            demo.outcome(name, "main-end: the table read whole", "complete coverage established",
                         str(report["coverage"]["complete_established"] if report else None),
                         bool(report) and report["coverage"]["complete_established"])
            demo.main_run.update(checkpoint=end, reference=ref["reference"], scope=ref["scope"])
            # The same audit on selected keys says what it did not read.
            code, report, _ = audit(demo, run, "main-end-selected", end, ref["reference"])
            cov = (report or {}).get("coverage", {})
            demo.outcome(name, "main-end on selected keys", "exit 0, the rest of the table not established",
                         "exit %d, complete %s" % (code, cov.get("complete_established")),
                         code == 0 and cov.get("complete_established") is False and
                         "not established" in cov.get("complete_reason", ""))
    # The history a window depends on, missing from a bundle.
    code, _, _, text = demo.scope_window("main-from-3", cut, end)
    demo.outcome(name, "a bundle without the run's first intervals: incident-scope",
                 "exit 3, the lineage not whole", "exit %d" % code,
                 code == 3 and "interval evidence" in text, text[-160:])
    code, report, _ = demo.reconcile("main-from-3", cut, end, extra=["--coverage=complete-table"])
    v = verdicts(report)
    demo.outcome(name, "a bundle without the run's first intervals: incident-reconcile",
                 "exit 3, reconcile.scope unavailable", "exit %d, %s" % (code, v.get("reconcile.scope")),
                 code == 3 and v.get("reconcile.scope") == "unavailable")


def windows_unexpected(demo):
    """The main run's dedicated table with a window neither the reference nor
    the job names: only the whole-table read sees it."""
    run = demo.main_run
    s = demo.services
    s.ch("INSERT INTO demo.%s SETTINGS insert_deduplicate = 0 VALUES ('acct-99', %d, %d, 1, 1.00, 1)"
         % (run["table"], BASE_MS, BASE_MS + SIZE_MS))
    try:
        code, report, _ = audit(demo, run, "unexpected-whole", run["checkpoint"], run["reference"],
                                "complete-table")
        v = verdicts(report)
        demo.outcome("windows-unexpected", "an unrelated window, the table read whole",
                     "exit 1: destination.coverage failed, 1 unexpected, the rest passed",
                     "exit %d, %s, unexpected %s" % (code, v.get("destination.coverage"),
                                                    counts(report, "destination.coverage").get("unexpected")),
                     code == 1 and v.get("destination.coverage") == "failed" and
                     counts(report, "destination.coverage").get("unexpected") == 1 and
                     all(v[k] == "passed" for k in v if k != "destination.coverage"))
        code, report, _ = audit(demo, run, "unexpected-selected", run["checkpoint"], run["reference"])
        cov = (report or {}).get("coverage", {})
        demo.outcome("windows-unexpected", "the same, on selected keys",
                     "exit 0, saying rows of other keys were not read",
                     "exit %d: %s" % (code, cov.get("complete_reason", "")[:80]),
                     code == 0 and not cov.get("complete_established") and
                     "not established" in cov.get("complete_reason", ""))
    finally:
        s.ch("ALTER TABLE demo.%s DELETE WHERE account = 'acct-99' SETTINGS mutations_sync = 2"
             % run["table"])
        s.ch("OPTIMIZE TABLE demo.%s FINAL" % run["table"])


def windows_missing_history(demo):
    """Records the main run's windows depend on are deleted from the topic:
    the reference cannot read its lineage whole, says so, and the audit is
    partial."""
    run = demo.main_run
    demo.services.delete_before(run["topic"], 1, 50)
    ref = reference_for(demo, run, "missing-history", run["checkpoint"])
    code, report, _ = audit(demo, run, "missing-history", run["checkpoint"], ref["reference"],
                            "complete-table")
    v = verdicts(report)
    demo.outcome("windows-missing-history", "partition 1's first 50 records deleted",
                 "the reference exits 3; the audit exits 3, reference.scope unavailable",
                 "reference exit %d, audit exit %d, %s" % (ref["exit"], code, v.get("reference.scope")),
                 ref["exit"] == 3 and code == 3 and v.get("reference.scope") == "unavailable",
                 ref["text"][-160:])


def recovery_run(demo, name, faults="", kill_at_idle=False):
    """The main timeline with worker-1 running the job alone and dying: at a
    fault point, or killed while partition 2 is quiet and the windows after
    E hold part of their records. The job restores on worker-2."""
    run = start_run(demo, name, faults=faults, worker2_late=True)
    c = run["cluster"]
    marks = {}

    def kill():
        w1 = c.procs["worker-1"]
        if w1.poll() is None:
            w1.kill()
            w1.wait()
            demo.note("worker-1 killed with the windows after E open")

    try:
        events = main_timeline(demo, run, Timeline(seed=2), marks,
                               on_idle=kill if kill_at_idle else None)
        collect(demo, run)
    finally:
        c.stop()
    demo.services.ch("OPTIMIZE TABLE demo.%s FINAL" % run["table"])
    rec = c.markers / "RESTORED"
    restored = rec.read_text().strip() if rec.exists() else ""
    demo.outcome(name, "the job restored", "a RESTORED record", restored or "none", bool(restored))
    end = marks["quiet"][0]
    ref = reference_for(demo, run, name, end)
    demo.outcome(name, "the reference", "complete (exit 0)", "exit %d" % ref["exit"], ref["exit"] == 0,
                 ref["text"][-200:])
    code, report, _ = audit(demo, run, name, end, ref["reference"], "complete-table")
    restores = (report or {}).get("scope", {}).get("restores", [])
    demo.outcome(name, "the lineage passes through the restore", ">= 1 restore",
                 json.dumps(restores), len(restores) >= 1)
    designed_check(demo, name, name, ref["scope"], ref["reference"], events)
    return code, report


def windows_recovery(demo):
    """worker-1 killed with windows partly accumulated: the restored windows
    carry on from the checkpoint, and every window is final once, exactly."""
    name = "windows-recovery"
    code, report = recovery_run(demo, name, kill_at_idle=True)
    demo.outcome(name, "incident-reconcile --reference (complete-table)", "exit 0, every check passed",
                 "exit %d, %s" % (code, not_passed(report)), code == 0 and every_passed(report))


def windows_crash_after_output(demo):
    """worker-1 dies right after an INSERT of window rows is acknowledged,
    before its checkpoint completes: the restored run writes those windows
    again. The transformation is right; the destination holds the duplicates,
    which the audit reports and puts down to the restore."""
    name = "windows-crash-after-output"
    code, report = recovery_run(demo, name, faults="clickhouse.after_end_insert=exit:1@2")
    v = verdicts(report)
    rows = counts(report, "reconcile.rows")
    rd = counts(report, "reference.destination")
    details = next((c["details"] for c in report["checks"] if c["id"] == "reconcile.rows"), []) if report else []
    ok = (code == 1 and all(v.get(k) == "passed" for k in
                            ("reference.semantics", "reference.watermark", "reference.finality",
                             "reference.capture")) and
          v.get("reconcile.rows") == "failed" and rows.get("duplicated", 0) > 0 and
          rows.get("missing") == 0 and rows.get("changed") == 0 and rows.get("unexpected") == 0 and
          rd.get("duplicated") == rows.get("duplicated") and rd.get("missing") == 0 and
          rd.get("changed") == 0 and rd.get("unexpected") == 0)
    demo.outcome(name, "incident-reconcile --reference (complete-table)",
                 "exit 1: the transformation passed; duplicates only, in the delivery",
                 "exit %d, %s, duplicated %s/%s" % (code, not_passed(report), rows.get("duplicated"),
                                                   rd.get("duplicated")), ok)
    attributed = any("lineage passes the restore" in d for d in details)
    demo.outcome(name, "the duplicates are put down to the restore", "attributed",
                 "attributed" if attributed else "not attributed", attributed)
    demo.outcome(name, "the findings", "delivery, not transformation", str(findings(report)),
                 "delivery" in findings(report) and "transformation" not in findings(report))


def windows_empty(demo):
    """Nothing but chargebacks: no window holds a payment, the job writes
    nothing, and the dedicated table read whole is empty, as the reference
    says."""
    name = "windows-empty"
    run = start_run(demo, name)
    t = Timeline(seed=3)
    try:
        demo.produce(run["topic"], t.steady([0, 1, 2], 0, 25_000, kind="chargeback"))
        run["cluster"].await_settled(2)
        quiet = run["cluster"].await_quiet(3)
        collect(demo, run)
    finally:
        run["cluster"].stop()
    ref = reference_for(demo, run, name, quiet[0])
    rows, windows, _ = artifact_parts(ref["reference"])
    demo.outcome(name, "the reference", "complete, no rows, no windows",
                 "exit %d, %d rows, %d windows" % (ref["exit"], len(rows), len(windows)),
                 ref["exit"] == 0 and not rows and not windows)
    code, report, _ = audit(demo, run, name, quiet[0], ref["reference"], "complete-table")
    demo.outcome(name, "incident-reconcile --reference (complete-table)",
                 "exit 0, the table read whole and empty",
                 "exit %d, %s, %s row(s) read" % (code, not_passed(report),
                                                 (report or {}).get("coverage", {}).get("table_rows_read")),
                 code == 0 and every_passed(report) and report["coverage"]["table_rows_read"] == 0)


def defect(demo, variant, what):
    """A job with a deliberate defect writes what it computes, faithfully: the
    delivery check over its whole lineage passes, and only the reference
    fails it, naming the transformation."""
    name = "windows-" + variant
    run = start_run(demo, name, variant=variant)
    marks = {}
    try:
        main_timeline(demo, run, Timeline(seed=4), marks, faithful=False)
        collect(demo, run)
    finally:
        run["cluster"].stop()
    demo.services.ch("OPTIMIZE TABLE demo.%s FINAL" % run["table"])
    end = marks["quiet"][0]
    plain, report, _ = demo.reconcile(name + "-reconcile-only", run["bundle"], end,
                                      extra=["--from-run-start"])
    demo.outcome(name, "incident-reconcile without a reference, over the whole lineage",
                 "exit 0: the delivery was faithful", "exit %d, %s" % (plain, not_passed(report)),
                 plain == 0)
    ref = reference_for(demo, run, name, end)
    code, report, _ = audit(demo, run, name, end, ref["reference"], "complete-table")
    v = verdicts(report)
    rel = findings(report)
    ok = (ref["exit"] == 0 and code == 1 and rel and rel[0] == "transformation" and
          "delivery" not in rel and v.get("reconcile.rows") == "passed" and
          all(v.get(k) == "failed" for k in what))
    demo.outcome(name, "incident-reconcile --reference (complete-table)",
                 "exit 1, %s failed, the delivery passed" % ", ".join(what),
                 "exit %d, failed %s, findings %s" % (code, sorted(k for k, x in v.items() if x == "failed"), rel),
                 ok)
    c = counts(report, "reference.capture")
    demo.note("%s: reference.capture missing %s, changed %s, unexpected %s; %s"
              % (name, c.get("missing"), c.get("changed"), c.get("unexpected"),
                 next((x["reason"] for x in report["checks"] if x["id"] == "reference.semantics"), "")[:150]))


def windows_wrong_time_column(demo):
    defect(demo, "wrong-time-column", ["reference.semantics", "reference.capture"])


def windows_wrong_sign(demo):
    defect(demo, "wrong-sign", ["reference.capture"])


def windows_late_treatment(demo):
    defect(demo, "late-treatment", ["reference.semantics", "reference.capture"])


def windows_unsupported(demo):
    """A source that lets a quiet partition drop out of the watermark after a
    stretch of wall-clock time (idle_timeout_ms): which windows close then
    depends on time the records do not show, so incident-scope refuses it by
    name. The delivery is still checked."""
    name = "windows-unsupported"
    run = start_run(demo, name, variant="idleness")
    try:
        demo.produce(run["topic"], Timeline(seed=5).steady([0, 1, 2], 0, 25_000))
        run["cluster"].await_settled(2)
        quiet = run["cluster"].await_quiet(2)
        collect(demo, run)
    finally:
        run["cluster"].stop()
    code, _, _, text = demo.scope_window(name, run["bundle"], quiet[-1])
    demo.outcome(name, "incident-scope", "exit 3, refusing idleness by name", "exit %d" % code,
                 code == 3 and "idle_timeout_ms" in text, text[-200:])
    # A window the audit does not model names no identity, so the operator
    # names the key.
    plain, report, _ = demo.reconcile(name + "-reconcile-only", run["bundle"], quiet[-1],
                                      extra=["--from-run-start", "--key=account,window_start,window_end"])
    demo.outcome(name, "incident-reconcile without a reference, over the run",
                 "exit 0: the delivery is checked", "exit %d, %s" % (plain, not_passed(report)),
                 plain == 0)


def windows_cost(demo):
    """The audit's cost on a bounded workload of ten minutes of event time
    across three partitions, 60,000 events: the flight recorder off and on
    (the Workers' CPU, what the recorder and the bundle hold), then exporting
    the scope, computing the reference over the whole lineage, and the audit
    with its whole-table read."""
    import statistics
    from scenarios import cpu_seconds, dir_bytes  # the reconciliation example's
    t = Timeline(seed=6)
    events = t.steady([0, 1, 2], 0, 600_000, per_second=33) + [
        t.at(p, 602_000, kind="purchase") for p in (0, 1, 2)]
    topic = "payments-cost"
    demo.services.create_topic(topic, workload.PARTITIONS)
    demo.produce(topic, events)
    final_end = BASE_MS + 600_000
    runs = {"off": [], "on": []}
    measured = None
    for rep in range(3):
        for mode in ("off", "on"):
            name = "cost-%s-%d" % (mode, rep)
            table = "windows_" + name.replace("-", "_")
            demo.create_table(table)
            cluster = WindowCluster(demo, name, capture=(mode == "on"))
            try:
                t0 = time.time()
                cluster.start(topic, table, demo.services.ch_port)
                while time.time() - t0 < 600:
                    last = demo.services.ch("SELECT max(window_end) FROM demo.%s" % table).strip()
                    if last and last != "0" and int(last) >= final_end:
                        break
                    time.sleep(0.2)
                wall = time.time() - t0
                cluster.await_completed(len(cluster.completed()) + 1, timeout=120)
                cpu = sum(cpu_seconds(cluster.procs[w].pid) or 0.0 for w in ("worker-1", "worker-2"))
                runs[mode].append({"seconds_to_last_window": round(wall, 2),
                                   "worker_cpu_seconds": round(cpu, 2),
                                   "capture_bytes": dir_bytes(cluster.dir / "capture")})
                if mode == "on" and rep == 2:
                    quiet = cluster.await_quiet(3)
                    bundle = demo.work / "bundles" / "cost"
                    info = cluster.collect(bundle)
            finally:
                cluster.stop()
            if mode == "on" and rep == 2:
                run = {"name": "cost", "bundle": bundle, "table": table}
                demo.services.ch("OPTIMIZE TABLE demo.%s FINAL" % table)
                ref = reference_for(demo, run, "cost", quiet[0])
                code, report, _ = audit(demo, run, "cost", quiet[0], ref["reference"], "complete-table")
                measured = {"events": len(events), "bundle_bytes": info["bytes"],
                            "bundle_files": info["files"],
                            "scope_seconds": round(ref["scope_s"], 3),
                            "reference_seconds": round(ref["reference_s"], 3),
                            "reference_exit": ref["exit"],
                            "audit_seconds": round(report["_elapsed_s"], 3) if report else None,
                            "audit_exit": code,
                            "final_windows": window_audit(report).get("final"),
                            "lineage_checkpoints": len(window_audit(report).get("watermarks", [])),
                            "table_rows_read": (report or {}).get("coverage", {}).get("table_rows_read")}
    summary = {mode: {k: statistics.median(r[k] for r in rs) for k in rs[0]} for mode, rs in runs.items()}
    out = {"runs": runs, "median": summary, "audit": measured}
    (demo.results / "cost.json").write_text(json.dumps(out, indent=2))
    demo.note("recorder off: median %.2f s to the last window, %.2f CPU s; on: %.2f s, %.2f CPU s, "
              "%d bytes captured" % (summary["off"]["seconds_to_last_window"],
                                     summary["off"]["worker_cpu_seconds"],
                                     summary["on"]["seconds_to_last_window"],
                                     summary["on"]["worker_cpu_seconds"],
                                     summary["on"]["capture_bytes"]))
    demo.note("audit of %s events over %s lineage checkpoints: scope %.3f s, reference %.3f s, "
              "audit with the whole table %.3f s, %s final windows"
              % (measured["events"], measured["lineage_checkpoints"], measured["scope_seconds"],
                 measured["reference_seconds"], measured["audit_seconds"], measured["final_windows"]))
    demo.outcome("windows-cost", "the measured audit", "reference exit 0, audit exit 0",
                 "reference exit %d, audit exit %d" % (measured["reference_exit"], measured["audit_exit"]),
                 measured["reference_exit"] == 0 and measured["audit_exit"] == 0)


ALL = [
    ("windows-main", windows_main),
    ("windows-unexpected", windows_unexpected),
    ("windows-missing-history", windows_missing_history),
    ("windows-recovery", windows_recovery),
    ("windows-crash-after-output", windows_crash_after_output),
    ("windows-empty", windows_empty),
    ("windows-wrong-time-column", windows_wrong_time_column),
    ("windows-wrong-sign", windows_wrong_sign),
    ("windows-late-treatment", windows_late_treatment),
    ("windows-unsupported", windows_unsupported),
    ("windows-cost", windows_cost),
]
