"""The audit-runner example's scenarios (see README.md), in the order run.sh
runs them.

Each takes the Demo and records outcomes with demo.outcome(); a Failed
exception stops the scenario and fails it. None accepts "the runner stayed
up" as a result: each holds the runner's published reports and status
against what the scenario did, and the steady run against an independent
check of the source and the destination.
"""
import json
import statistics
import subprocess
import time
from decimal import Decimal
from pathlib import Path

import workload
from reconcile_demo import Cluster, Failed
from scenarios import Feeder, cpu_seconds, dir_bytes


def _state(config):
    return Path(json.loads(Path(config).read_text())["state_dir"])


def _wait(what, cond, timeout, step=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(step)
    raise Failed("timed out waiting for %s" % what)


def _all_audited(demo, cluster, config, since=0):
    """Every completed checkpoint from the runner's start has a report."""
    st = _state(config)
    try:
        start = json.loads((st / "state.json").read_text()).get("start")
    except (OSError, ValueError):
        return False
    done = [c for c in cluster.completed() if start is not None and c >= start and c > since]
    have = {r["checkpoint"] for r in demo.reports(st)}
    return bool(done) and set(done) <= have


def _independent(demo, topic, table, reports):
    """Reads the verified source range back from Kafka with its console
    consumer and the destination with the administrator's client, and
    derives the rows the job owed from the events' own fields
    (workload.reference_from_value): nothing of clink's or the runner's."""
    ends = {}
    for r in reports:
        if r["outcome"] not in ("passed", "empty"):
            continue
        for s in r["scope"]["sources"]:
            for p in s["partitions"]:
                ends[p["partition"]] = max(ends.get(p["partition"], 0), p["to"])
    owed = {}
    read = 0
    for part, end in sorted(ends.items()):
        got = demo.services.read_range(topic, part, 0, end)
        if [o for o, _ in got] != list(range(0, end)):
            raise Failed("partition %d: offsets [0, %d) could not all be read back" % (part, end))
        read += len(got)
        for _, value in got:
            row = workload.reference_from_value(value)
            if row is not None:
                owed[int(row["event_id"][1:])] = row
    ids = sorted(owed)
    landed = 0
    distinct = 0
    for i in range(0, len(ids), 5000):
        chunk = ",".join(str(x) for x in ids[i:i + 5000])
        c, u = demo.services.ch("SELECT count(), uniqExact(event_id) FROM demo.%s WHERE event_id IN (%s)"
                                % (table, chunk)).split()
        landed += int(c)
        distinct += int(u)
    return {"partitions": ends, "records_read": read, "rows_owed": len(owed), "rows_landed": landed,
            "distinct_landed": distinct,
            "rows_reported": sum(int(r["scope"]["expected_rows"] or 0) for r in reports
                                 if r["outcome"] in ("passed", "empty"))}


def _unaudited(demo, cluster, state):
    have = {r["checkpoint"] for r in demo.reports(state)}
    try:
        start = json.loads((state / "state.json").read_text()).get("start")
    except (OSError, ValueError):
        start = None
    if start is None:
        return len(cluster.completed())
    return sum(1 for c in cluster.completed() if c >= start and c not in have)


def _pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]


# -- the steady run ------------------------------------------------------------------------------

def steady(demo):
    """The job runs and keeps producing; the runner polls; every checkpoint is
    audited once, in order, and the coverage it claims is the source range an
    independent read finds and the destination holds."""
    topic, table = "audit-steady", "audit_steady"
    demo.services.create_topic(topic)
    demo.create_table(table)
    cluster = Cluster(demo, "runner-steady", interval_ms=2000)
    # Ten checkpoints two seconds apart: twenty seconds for a checkpoint to be
    # collected before its successor's completion purges what its scope reads.
    # runner-expired is the same runner with two.
    cluster.retained = 10
    cluster.start(topic, table, demo.services.ch_port)
    config = demo.runner_config(cluster, limits={"retain_passed": 3})
    state = _state(config)
    poll = demo.audit_spawn(config, label="runner-steady poll")
    feeder = Feeder(demo, topic, 1, 9000, demo.feed_rate).start()
    backlog = []
    runner_cpu = None
    try:
        t0 = time.time()
        while feeder.thread.is_alive() and time.time() - t0 < 120:
            # Completed checkpoints without a report, counted from outside.
            backlog.append(_unaudited(demo, cluster, state))
            time.sleep(1)
        produced = feeder.stop() - 1
        # The last rows land and are completed by a checkpoint, then the job
        # stops and the runner audits what remains.
        want = sum(1 for i in range(1, produced + 1) if workload.reference_row(workload.event(i)))
        _wait("every row to land", lambda: int(demo.services.ch(
            "SELECT count() FROM demo.%s" % table).strip() or 0) >= want, 120)
        cluster.await_completed(len(cluster.completed()) + 2, timeout=60)
        cluster.stop()
        _wait("the runner to audit every completed checkpoint",
              lambda: _all_audited(demo, cluster, config), 300)
        runner_cpu = cpu_seconds(poll.pid)
    finally:
        feeder.stop()
        cluster.stop()
        code = demo.stop_poll(poll)
    demo.note("the poll runner stopped on SIGTERM with exit %s" % code)
    reports = demo.reports(state)
    start = json.loads((state / "state.json").read_text())["start"]
    completed = [c for c in cluster.completed() if c >= start]
    checkpoints = [r["checkpoint"] for r in reports]
    demo.outcome("runner-steady", "every completed checkpoint audited exactly once",
                 "%d checkpoints, %d reports, none twice" % (len(completed), len(completed)),
                 "%d reports for %d checkpoints" % (len(reports), len(set(checkpoints))),
                 sorted(checkpoints) == completed and len(set(checkpoints)) == len(checkpoints))
    outcomes = {}
    for r in reports:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
    demo.outcome("runner-steady", "every audit verified", "passed or empty (no record in its range)",
                 json.dumps(outcomes, sort_keys=True), set(outcomes) <= {"passed", "empty"} and
                 outcomes.get("passed", 0) >= 5)
    scode, s = demo.status(config)
    holes = [c["holes"] for c in s["coverage"]]
    demo.outcome("runner-steady", "the status once the job stopped", "healthy, exit 0, no gap, no hole",
                 "%s, exit %d, %d gap(s), holes %s" % (s["state"], scode, len(s["gaps"]), holes),
                 scode == 0 and s["state"] == "healthy" and not s["gaps"] and all(h == [] for h in holes))
    cov = {c["partition"]: c["verified"] for c in s["coverage"]}
    ind = _independent(demo, topic, table, reports)
    (demo.results / "independent" / "runner-steady.json").write_text(json.dumps(ind, indent=2))
    demo.outcome("runner-steady", "the coverage claimed is the source range read back",
                 "one range per partition, from the run's start to its last audited offset",
                 json.dumps(cov, sort_keys=True),
                 all(v == [[None, ind["partitions"][p]]] for p, v in cov.items()) and
                 sorted(cov) == sorted(ind["partitions"]))
    demo.outcome("runner-steady", "the independent check agrees",
                 "rows owed = rows the audits compared = rows in the destination, each once",
                 "owed %(rows_owed)d, compared %(rows_reported)d, landed %(rows_landed)d (%(distinct_landed)d distinct), "
                 "%(records_read)d records read back" % ind,
                 ind["rows_owed"] == ind["rows_reported"] == ind["rows_landed"] == ind["distinct_landed"] > 0)
    verified_ids = {r["id"] for r in reports if r["outcome"] in ("passed", "empty")}
    kept = [p.name for p in (state / "audits").iterdir() if p.is_dir() and p.name in verified_ids]
    demo.outcome("runner-steady", "passed evidence is bounded",
                 "at most retain_passed (3) directories of verified audits, every report kept",
                 "%d kept, %d reports" % (len(kept), len(reports)), len(kept) <= 3 and len(reports) == len(completed))
    # The runner's own cost, from its reports and the process.
    walls = [r["costs"]["wall_s"] for r in reports]
    cpus = [r["costs"]["cpu_s"] for r in reports]
    measured = {
        "audits": len(reports), "checkpoint_interval_ms": 2000, "events": produced,
        "audit_wall_s": {"median": statistics.median(walls), "p95": _pct(walls, 0.95), "max": max(walls)},
        "audit_child_cpu_s": {"median": statistics.median(cpus), "p95": _pct(cpus, 0.95)},
        "steps_wall_s_median": {k: statistics.median(float(r["steps"][k].get("seconds") or 0)
                                                      for r in reports if k in r["steps"])
                                for k in ("collect", "scope", "reference", "verify", "reconcile")},
        "bundle_bytes_median": statistics.median(int(r["costs"]["bundle_bytes"] or 0) for r in reports),
        "reference_records_total": sum(int(r["costs"]["reference_records"] or 0) for r in reports),
        "source_records_in_range": ind["records_read"],
        "destination_queries_total": sum(int(r["costs"]["destination_queries"] or 0) for r in reports),
        "rows_compared_total": ind["rows_reported"],
        "runner_process_cpu_s": runner_cpu,
        "children_cpu_s_total": round(sum(cpus), 3),
        "state_dir_bytes": dir_bytes(state),
        "backlog_waiting_max": max(backlog) if backlog else None,
        "collect_tries_max": max(int(r["steps"]["collect"].get("tries") or 1) for r in reports),
        "collect_refused_changed": sum(int(r["steps"]["collect"].get("tries") or 1) - 1 for r in reports),
        "sustainable_checkpoints_per_minute": round(60.0 / statistics.median(walls), 1),
    }
    (demo.results / "measurements-steady.json").write_text(json.dumps(measured, indent=2))
    demo.note("audits: %d; per audit median %.2f s wall (p95 %.2f), %.2f s CPU in its children; "
              "steps (median s): %s" % (measured["audits"], measured["audit_wall_s"]["median"],
                                        measured["audit_wall_s"]["p95"],
                                        measured["audit_child_cpu_s"]["median"],
                                        json.dumps(measured["steps_wall_s_median"])))
    demo.note("source records reread by the references: %d for %d in the verified range; destination "
              "queries %d; runner process CPU %s s; state directory %d bytes; backlog at most %s"
              % (measured["reference_records_total"], measured["source_records_in_range"],
                 measured["destination_queries_total"], runner_cpu, measured["state_dir_bytes"],
                 measured["backlog_waiting_max"]))
    demo.outcome("runner-steady", "each source record read once by the references",
                 "records reread = records in the verified range",
                 "%d reread, %d in range" % (measured["reference_records_total"], ind["records_read"]),
                 measured["reference_records_total"] == ind["records_read"])




# -- discrepancies -------------------------------------------------------------------------------

def _feed(demo, cluster, topic, table, first, count, settle=2):
    """Produces events first.. (count of them), waits for every row they owe
    to land, then for `settle` more checkpoints, so a completed checkpoint
    holds them."""
    feeder = Feeder(demo, topic, first, count, demo.feed_rate).start()
    _wait("the feed", lambda: not feeder.thread.is_alive(), 300)
    last = feeder.stop() - 1
    want = sum(1 for i in range(1, last + 1) if workload.reference_row(workload.event(i)))
    _wait("every row to land", lambda: int(demo.services.ch(
        "SELECT count() FROM demo.%s" % table).strip() or 0) >= want, 180)
    cluster.await_completed(len(cluster.completed()) + settle, timeout=120)
    return last


def _start(demo, name, variant=None, **cluster_over):
    topic, table = "audit-%s" % name, "audit_%s" % name.replace("-", "_")
    demo.services.create_topic(topic)
    demo.create_table(table)
    cluster = Cluster(demo, "runner-%s" % name, variant=variant,
                      interval_ms=cluster_over.pop("interval_ms", 2000))
    for k, v in cluster_over.items():
        setattr(cluster, k, v)
    cluster.start(topic, table, demo.services.ch_port)
    return cluster, topic, table


def _settle(demo, config, label, passes=6):
    """Runs passes, as a scheduler would, until no audit waits to be tried
    again (an audit has one attempt per pass); returns the last exit."""
    code = None
    for _ in range(passes):
        code, _ = demo.audit(config, label=label)
        _, s = demo.status(config)
        due = [_parse(r["next_attempt_at"]) for r in s["backlog"]["retrying"] if r.get("next_attempt_at")]
        if not due:
            break
        time.sleep(max(0.0, max(due) - time.time()) + 0.5)
    return code


def _by_outcome(reports):
    out = {}
    for r in reports:
        out.setdefault(r["outcome"], []).append(r["checkpoint"])
    return out


def defect(demo):
    """The job computes amount_cents at the wrong scale; it writes what it
    computes faithfully. The runner, run as a scheduler would run it, reports
    a discrepancy in the transformation for every scope that holds rows, and
    the delivery as sound."""
    cluster, topic, table = _start(demo, "defect", variant="wrong-projection")
    config = demo.runner_config(cluster)
    try:
        _feed(demo, cluster, topic, table, 1, 1500)
    finally:
        cluster.stop()
    code, out = demo.audit(config, label="runner-defect run")
    reports = demo.reports(_state(config))
    by = _by_outcome(reports)
    with_rows = [r for r in reports if (r["scope"] or {}).get("expected_rows")]
    rel = set()
    delivery_ok = True
    for r in with_rows:
        rel |= {f["relationship"] for f in r["findings"]}
        rec = json.loads((_state(config) / r["evidence"]["reconcile_report"]).read_text())
        delivery_ok &= next(c["verdict"] for c in rec["checks"] if c["id"] == "reconcile.rows") == "passed"
    demo.outcome("runner-defect", "every scope with rows is a discrepancy in the transformation",
                 "discrepancy for each, findings transformation (and reference-destination), the delivery passing",
                 "%s; relationships %s; delivery %s" % (json.dumps({k: len(v) for k, v in by.items()}),
                                                         sorted(rel), "passing" if delivery_ok else "failing"),
                 bool(with_rows) and all(r["outcome"] == "discrepancy" for r in with_rows) and
                 "transformation" in rel and "delivery" not in rel and delivery_ok)
    scode, s = demo.status(config)
    demo.outcome("runner-defect", "the status", "discrepancy, exit 1",
                 "%s, exit %d, %d unacknowledged" % (s["state"], scode, s["unacknowledged_discrepancies"]),
                 scode == 1 and s["state"] == "discrepancy")


def corruption(demo):
    """The job is sound; one row is changed in the destination after it was
    written, before its checkpoint was audited. Its audit reports the
    delivery, not the transformation; later scopes pass; the discrepancy
    stays until it is acknowledged, and stays listed after."""
    cluster, topic, table = _start(demo, "corruption")
    config = demo.runner_config(cluster)
    try:
        _feed(demo, cluster, topic, table, 1, 1000)
        code_a, _ = demo.audit(config, label="runner-corruption run 1")
        a_reports = demo.reports(_state(config))
        demo.outcome("runner-corruption", "the first audits, before any corruption", "exit 0, every scope verified",
                     "exit %d, %s" % (code_a, json.dumps({k: len(v) for k, v in _by_outcome(a_reports).items()})),
                     code_a == 0 and set(_by_outcome(a_reports)) <= {"passed", "empty"})
        _feed(demo, cluster, topic, table, 1001, 1000)
        victim = next(i for i in range(1001, 2001) if workload.reference_row(workload.event(i)))
        demo.services.ch("ALTER TABLE demo.%s UPDATE amount = amount + 1, amount_cents = amount_cents + 100 "
                         "WHERE event_id = %d SETTINGS mutations_sync = 2" % (table, victim))
        code_b, _ = demo.audit(config, label="runner-corruption run 2")
        _feed(demo, cluster, topic, table, 2001, 1000)
    finally:
        cluster.stop()
    code_c, _ = demo.audit(config, label="runner-corruption run 3")
    reports = demo.reports(_state(config))
    disc = [r for r in reports if r["outcome"] == "discrepancy"]
    named = []
    if len(disc) == 1:
        rec = json.loads((_state(config) / disc[0]["evidence"]["reconcile_report"]).read_text())
        named = sorted({d["key"]["event_id"] for d in rec.get("discrepancies", [])})
        rels = sorted({f["relationship"] for f in disc[0]["findings"]})
    else:
        rels = []
    demo.outcome("runner-corruption", "the changed row's scope, and only it, is a discrepancy",
                 "one discrepancy naming event %d, in the delivery and reference-destination, not the transformation"
                 % victim, "%d discrepancy(ies), naming %s, relationships %s" % (len(disc), named, rels),
                 len(disc) == 1 and named == [str(victim)] and "delivery" in rels and "transformation" not in rels)
    later = [r for r in reports if disc and r["checkpoint"] > disc[0]["checkpoint"]]
    scode, s = demo.status(config)
    demo.outcome("runner-corruption", "later healthy scopes do not erase it",
                 "later scopes verified, status discrepancy (exit 1) with the last verification after it",
                 "%d later, %s; status %s exit %d, last verified %s, last discrepancy %s" % (
                     len(later), sorted({r["outcome"] for r in later}), s["state"], scode,
                     (s["last_verified"] or {}).get("checkpoint"), (s["last_discrepancy"] or {}).get("checkpoint")),
                 bool(later) and {r["outcome"] for r in later} <= {"passed", "empty"} and code_b == 1 and
                 code_c == 1 and scode == 1 and disc and s["last_verified"]["checkpoint"] > disc[0]["checkpoint"])
    evidence = _state(config) / disc[0]["evidence"]["dir"] / "bundle" / "manifest.json" if disc else None
    demo.outcome("runner-corruption", "its evidence is kept", "the audit's bundle and reports retained",
                 "present" if evidence and evidence.exists() else "absent", bool(evidence and evidence.exists()))
    demo.audit(config, "acknowledge", ["--audit=%s" % disc[0]["id"] if disc else "--audit=none",
                                       "--note=the row was changed by hand in the example"], label="acknowledge")
    scode, s = demo.status(config)
    demo.outcome("runner-corruption", "acknowledged, it stays listed", "healthy, exit 0, the discrepancy listed as acknowledged",
                 "%s, exit %d, %s" % (s["state"], scode, [bool(x["acknowledged"]) for x in s["discrepancies"]]),
                 scode == 0 and s["discrepancies"] and all(x["acknowledged"] for x in s["discrepancies"]))


# -- the runner killed ---------------------------------------------------------------------------

def crash(demo):
    """The runner is killed (SIGKILL) while a reference runs, and again while
    a reconciliation runs, with the job producing throughout. Started again,
    it stops what it left running, runs the interrupted step again, and
    publishes one result per scope: none skipped, none verified twice, none
    claimed before it was finished."""
    cluster, topic, table = _start(demo, "crash", retained=10)
    config = demo.runner_config(cluster)
    state = _state(config)
    feeder = Feeder(demo, topic, 1, 6000, demo.feed_rate).start()
    kills = []
    try:
        for step in ("reference", "reconcile"):
            poll = demo.audit_spawn(config, label="runner-crash poll")
            _wait("two published audits", lambda: len(demo.reports(state)) >= 2 + 2 * len(kills), 120, 0.2)
            rec = _wait("the runner inside %s" % step, lambda: (
                lambda r: r if r and r.get("step") == step else None)(
                    json.loads((state / "inflight.json").read_text())
                    if (state / "inflight.json").exists() else None), 120, 0.02)
            published = {r["id"] for r in demo.reports(state)}
            import os
            import signal
            os.killpg(poll.pid, signal.SIGKILL)
            poll.wait()
            alive = _alive(rec["pid"])
            kills.append({"step": step, "audit": rec["audit"], "child": rec["pid"],
                          "child_alive_after_kill": alive,
                          "audit_published_before_kill": rec["audit"] in published})
            code, _ = demo.audit(config, label="runner-crash restart after %s" % step)
            kills[-1]["child_alive_after_restart"] = _alive(rec["pid"])
        poll = demo.audit_spawn(config, label="runner-crash poll")
        produced = feeder.stop() - 1
        want = sum(1 for i in range(1, produced + 1) if workload.reference_row(workload.event(i)))
        _wait("every row to land", lambda: int(demo.services.ch(
            "SELECT count() FROM demo.%s" % table).strip() or 0) >= want, 120)
        cluster.await_completed(len(cluster.completed()) + 2, timeout=60)
    finally:
        feeder.stop()
        cluster.stop()
    demo.stop_poll(poll)
    for _ in range(30):
        demo.audit(config, label="runner-crash run")
        if _all_audited(demo, cluster, config):
            break
    (demo.results / "independent" / "runner-crash-kills.json").write_text(json.dumps(kills, indent=2))
    reports = demo.reports(state)
    start = json.loads((state / "state.json").read_text())["start"]
    completed = [c for c in cluster.completed() if c >= start]
    ids = [r["id"] for r in reports]
    demo.outcome("runner-crash", "one result per scope across two kills",
                 "every completed checkpoint once, no id twice",
                 "%d reports, %d checkpoints, %d distinct ids" % (len(reports), len(completed), len(set(ids))),
                 sorted(r["checkpoint"] for r in reports) == completed and len(set(ids)) == len(ids))
    for k in kills:
        r = next((x for x in reports if x["id"] == k["audit"]), None)
        demo.outcome("runner-crash", "the audit killed inside %s" % k["step"],
                     "not published before the kill, published once after, its child stopped",
                     "published before %s; after: %s, attempt %s; child alive after the kill %s, after the restart %s"
                     % (k["audit_published_before_kill"], r and r["outcome"], r and r["attempt"],
                        k["child_alive_after_kill"], k["child_alive_after_restart"]),
                     not k["audit_published_before_kill"] and r is not None and
                     r["outcome"] in ("passed", "empty") and not k["child_alive_after_restart"])
    scode, s = demo.status(config)
    demo.outcome("runner-crash", "the status after the restarts", "healthy, exit 0, no gap",
                 "%s, exit %d, %d gap(s), verified %s, events orphan-stopped %d"
                 % (s["state"], scode, len(s["gaps"]), s["verified_checkpoints"],
                    len(demo.events(state, "orphan-stopped"))),
                 scode == 0 and not s["gaps"] and len(s["verified_checkpoints"]) == 1)
    ind = _independent(demo, topic, table, reports)
    demo.outcome("runner-crash", "the independent check agrees",
                 "rows owed = rows the audits compared = rows in the destination, each once",
                 "owed %(rows_owed)d, compared %(rows_reported)d, landed %(rows_landed)d (%(distinct_landed)d distinct)" % ind,
                 ind["rows_owed"] == ind["rows_reported"] == ind["rows_landed"] == ind["distinct_landed"] > 0)


def _alive(pid):
    import os
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, PermissionError):
        return False
    # A zombie or another process that reused the id: ask ps for its command.
    r = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
    return r.returncode == 0 and not r.stdout.strip().startswith("Z")


# -- a dependency down ---------------------------------------------------------------------------

def outage(demo):
    """ClickHouse is paused after checkpoints complete and before their
    audits read it. Those audits are partial and wait to be tried again;
    nothing is published while the destination is down; once it is back,
    their next attempts pass, and each report keeps the failed attempt."""
    cluster, topic, table = _start(demo, "outage")
    # Each audit has one attempt per cycle, so the pass made while the
    # destination is down spends one attempt of each; an outage that outlasts
    # every attempt leaves the scopes partial gaps.
    config = demo.runner_config(cluster, limits={"retry_backoff_s": [30], "query_timeout_ms": 2000,
                                                 "reconcile_timeout_s": 40})
    state = _state(config)
    paused = None
    try:
        _feed(demo, cluster, topic, table, 1, 800)
        code0, _ = demo.audit(config, label="runner-outage run 1")
        before = {r["id"] for r in demo.reports(state)}
        _feed(demo, cluster, topic, table, 801, 800)
        cluster.stop()
        demo.services.compose("pause", "clickhouse")
        paused = (time.time(), None)
        code1, _ = demo.audit(config, label="runner-outage run while paused")
        during = {r["id"] for r in demo.reports(state)}
        scode1, s1 = demo.status(config)
    finally:
        demo.services.compose("unpause", "clickhouse", check=False)
        paused = (paused[0], time.time()) if paused else None
        cluster.stop()
    # Once the retries are due, a pass retries them against the destination.
    due = [_parse(r["next_attempt_at"]) for r in s1["backlog"]["retrying"] if r.get("next_attempt_at")]
    time.sleep(max(0.0, max(due, default=time.time()) - time.time()) + 1)
    code2, _ = demo.audit(config, label="runner-outage run after")
    reports = demo.reports(state)
    retried = [r for r in reports if r["attempt"] > 1]
    demo.outcome("runner-outage", "nothing published while the destination was down",
                 "no new report during the pause; status incomplete with audits waiting to retry",
                 "%d new, status %s exit %d, %d retrying" % (len(during - before), s1["state"], scode1,
                                                              len(s1["backlog"]["retrying"])),
                 code0 == 0 and during == before and scode1 == 3 and s1["state"] == "incomplete" and
                 len(s1["backlog"]["retrying"]) >= 1)
    ok_after = all(r["outcome"] in ("passed", "empty") for r in reports)
    late = [r for r in reports if r["outcome"] == "passed" and r["id"] not in before and
            _parse(r["steps"]["reconcile"]["at"]) < paused[1]]
    demo.outcome("runner-outage", "after it, the retries pass and keep the failed attempt",
                 "every scope verified; each retried report's first attempt partial, the destination unreadable; "
                 "no pass observed during the pause",
                 "%s; %d retried, first attempts %s; passes during the pause %d" % (
                     json.dumps({k: len(v) for k, v in _by_outcome(reports).items()}), len(retried),
                     sorted({r["attempts"][0]["outcome"] for r in retried}), len(late)),
                 code2 == 0 and ok_after and retried and
                 all(r["attempts"][0]["outcome"] == "partial" and "could not be read" in r["attempts"][0]["reason"]
                     for r in retried) and not late)


def _end_offsets(demo, topic):
    ends = {}
    for line in demo.services.kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server=kafka:9092",
                                    "--topic=%s" % topic, check=False).stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3:
            ends[int(parts[1])] = int(parts[2])
    if not ends:
        raise Failed("kafka-get-offsets named no partition of %s" % topic)
    return ends


def _parse(iso):
    import calendar
    return calendar.timegm(time.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S")) + int(iso[20:23]) / 1000.0


# -- bounds that cannot be met -------------------------------------------------------------------

def budget(demo):
    """Bounds set below what the scopes need: rows per destination query, the
    source records a reference may read, a reconciliation's time. Each audit
    that meets one is partial or an error, never a pass; scopes with no row
    are still verified empty."""
    cluster, topic, table = _start(demo, "budget")
    try:
        _feed(demo, cluster, topic, table, 1, 1000)
    finally:
        cluster.stop()
    cases = [
        ("rows", {"max_rows_per_query": 5, "max_attempts": 1}, "Limit for result exceeded"),
        ("records", {"reference_records": 10, "max_attempts": 1}, "bound"),
        ("time", {"reconcile_timeout_s": 0.5, "max_attempts": 2, "retry_backoff_s": [0]}, "time limit"),
    ]
    for name, limits, phrase in cases:
        config = demo.runner_config(cluster, name="budget-%s" % name, limits=limits)
        code = _settle(demo, config, "runner-budget %s" % name)
        reports = demo.reports(_state(config))
        with_rows = [r for r in reports if (r["scope"] or {}).get("expected_rows") or
                     r["outcome"] not in ("passed", "empty")]
        bad = [r for r in with_rows if r["outcome"] in ("passed", "empty")]
        said = [r for r in with_rows if phrase in json.dumps(r)]
        scode, s = demo.status(config)
        demo.outcome("runner-budget", "%s bound: %s" % (name, json.dumps(limits)),
                     "every scope with rows partial or an error naming the bound; status incomplete, exit 3",
                     "%s; %d of %d name it; status %s exit %d" % (
                         json.dumps({k: len(v) for k, v in _by_outcome(reports).items()}), len(said),
                         len(with_rows), s["state"], scode),
                     with_rows and not bad and len(said) == len(with_rows) and scode == 3 and code == 3)


# -- selection and expiry ------------------------------------------------------------------------

def latest_only(demo):
    """Several checkpoints wait; latest-only audits the newest and records
    every one it passed over as skipped, so coverage shows the gap."""
    cluster, topic, table = _start(demo, "latest", interval_ms=1000)
    try:
        _feed(demo, cluster, topic, table, 1, 1200, settle=1)
    finally:
        cluster.stop()
    config = demo.runner_config(cluster, selection="latest-only")
    code, _ = demo.audit(config, label="runner-latest-only run")
    reports = demo.reports(_state(config))
    by = _by_outcome(reports)
    newest = max(cluster.completed())
    scode, s = demo.status(config)
    skipped_gap = [g for g in s["gaps"] if g["outcome"] == "skipped"]
    demo.outcome("runner-latest-only", "the newest audited, every older one recorded as skipped",
                 "checkpoint %d verified, %d skipped, status incomplete exit 3 with the gap" % (
                     newest, newest - min(cluster.completed())),
                 "%s; gaps %s; status %s exit %d" % (json.dumps({k: v[:3] + (["..."] if len(v) > 3 else [])
                                                                for k, v in by.items()}),
                                                     [(g["first"], g["last"], g["count"]) for g in skipped_gap],
                                                     s["state"], scode),
                 code == 3 and scode == 3 and by.get("skipped") and
                 sorted(by["skipped"]) == sorted(c for c in cluster.completed() if c != newest) and
                 [c for v in (by.get("passed", []), by.get("empty", [])) for c in v] == [newest] and
                 skipped_gap and skipped_gap[0]["first"] == min(cluster.completed()) and
                 skipped_gap[-1]["last"] == max(c for c in cluster.completed() if c != newest))
    demo.outcome("runner-latest-only", "coverage claims only the audited scope",
                 "one verified range per partition, not from the run's start",
                 json.dumps({c["partition"]: c["verified"] for c in s["coverage"]}),
                 all(len(c["verified"]) == 1 and c["verified"][0][0] is not None for c in s["coverage"]))


def expired(demo):
    """The Workers keep two checkpoints; the runner starts after many have
    completed. Those whose state was purged are gaps, by name; the ones it
    reaches in time are verified; none expired is reported verified."""
    cluster, topic, table = _start(demo, "expired", interval_ms=1000, retained=2)
    config = demo.runner_config(cluster)
    try:
        feeder = Feeder(demo, topic, 1, 4000, demo.feed_rate).start()
        cluster.await_completed(10, timeout=120)
        at_start = cluster.completed()
        code, _ = demo.audit(config, label="runner-expired run")
        demo.audit(config, label="runner-expired run 2")
        feeder.stop()
    finally:
        cluster.stop()
    reports = demo.reports(_state(config))
    by = _by_outcome(reports)
    scode, s = demo.status(config)
    exp = sorted(by.get("expired", []))
    demo.outcome("runner-expired", "checkpoints purged before collection are gaps, by name",
                 "the older checkpoints expired, naming the purge; later ones verified; status incomplete, exit 3",
                 "%s; status %s exit %d; reasons %s" % (
                     json.dumps({k: len(v) for k, v in by.items()}), s["state"], scode,
                     sorted({r["reason"][:60] for r in reports if r["outcome"] == "expired"})),
                 len(exp) >= 5 and set(at_start[:-3]) <= set(exp) and
                 all("purged" in r["reason"] for r in reports if r["outcome"] == "expired") and
                 (by.get("passed") or by.get("empty")) and scode == 3 and
                 any(g["outcome"] == "expired" for g in s["gaps"]))


def history_expired(demo):
    """The source records of the first scopes are deleted from the topic
    before the runner reads them. The references say so; those audits are
    partial; the later ones pass."""
    cluster, topic, table = _start(demo, "history")
    try:
        _feed(demo, cluster, topic, table, 1, 600)
        cut = cluster.completed()[-1]
        first_feed = _end_offsets(demo, topic)
        _feed(demo, cluster, topic, table, 601, 600)
    finally:
        cluster.stop()
    # Everything the first feed wrote, in every partition, is gone.
    for p, off in first_feed.items():
        demo.services.delete_before(topic, p, off)
    config = demo.runner_config(cluster, limits={"max_attempts": 2, "retry_backoff_s": [0]})
    code = _settle(demo, config, "runner-history run")
    reports = demo.reports(_state(config))
    early = [r for r in reports if r["checkpoint"] <= cut and (r["scope"] or {}).get("expected_rows")]
    late = [r for r in reports if r["checkpoint"] > cut]
    scode, s = demo.status(config)
    demo.outcome("runner-history-expired", "scopes whose source history is gone are partial, by name",
                 "the early scopes with rows partial, naming expired or deleted offsets; later ones verified; exit 3",
                 "early %s; later %s; status %s exit %d" % (
                     sorted({r["outcome"] for r in early}), sorted({r["outcome"] for r in late}), s["state"], scode),
                 early and all(r["outcome"] == "partial" and "expired or deleted" in r["reason"] for r in early)
                 and late and {r["outcome"] for r in late} <= {"passed", "empty"} and scode == 3)


# -- what it costs the job -----------------------------------------------------------------------

def cost(demo):
    """The same paced workload, three times in each of three modes: the flight
    recorder off; on; and on with the runner polling the job throughout. A
    run feeds 12,000 events at 400 a second into a topic of its own, with
    checkpoints every two seconds. Measured: how long after the feed ends the
    last row lands, the Workers' CPU, and the runner's CPU (its process and
    the steps it ran). The recorder's cost is the second mode against the
    first; the runner's, the third against the second."""
    count, rate = 12_000, 400
    want = sum(1 for i in range(1, count + 1) if workload.reference_row(workload.event(i)))
    runs = {"recorder-off": [], "recorder-on": [], "runner": []}
    for rep in range(3):
        for mode in runs:
            name = "cost-%s-%d" % (mode, rep)
            topic, table = "audit-%s" % name, "audit_%s" % name.replace("-", "_")
            demo.services.create_topic(topic)
            demo.create_table(table)
            cluster = Cluster(demo, name, capture=(mode != "recorder-off"), interval_ms=2000)
            poll = config = None
            try:
                cluster.start(topic, table, demo.services.ch_port)
                if mode == "runner":
                    config = demo.runner_config(cluster, limits={"poll_interval_s": 1})
                    poll = demo.audit_spawn(config, label="cost poll")
                feeder = Feeder(demo, topic, 1, count, rate).start()
                _wait("the feed", lambda: not feeder.thread.is_alive(), 300, 0.1)
                fed = time.time()
                feeder.stop()
                landed = _wait("every row to land", lambda: int(demo.services.ch(
                    "SELECT count() FROM demo.%s" % table).strip() or 0) >= want and time.time(), 300, 0.1)
                lag = landed - fed
                cluster.await_completed(len(cluster.completed()) + 1, timeout=120)
                cpu = sum(cpu_seconds(cluster.procs[w].pid) or 0.0 for w in ("worker-1", "worker-2"))
                runner_cpu = audits = None
                if poll is not None:
                    own = cpu_seconds(poll.pid) or 0.0
                    demo.stop_poll(poll)
                    poll = None
                    reports = demo.reports(_state(config))
                    audits = len(reports)
                    runner_cpu = round(own + sum(float(r["costs"]["cpu_s"] or 0) for r in reports), 2)
            finally:
                if poll is not None:
                    demo.stop_poll(poll)
                cluster.stop()
            runs[mode].append({"seconds_to_land_after_feed": round(lag, 2), "worker_cpu_seconds": round(cpu, 2),
                               "runner_cpu_seconds": runner_cpu, "audits_published": audits})
    med = {m: {k: statistics.median(r[k] for r in v) for k in ("seconds_to_land_after_feed", "worker_cpu_seconds")}
           for m, v in runs.items()}
    med["runner"]["runner_cpu_seconds"] = statistics.median(r["runner_cpu_seconds"] for r in runs["runner"])
    med["runner"]["audits_published"] = statistics.median(r["audits_published"] for r in runs["runner"])
    results = {"events": count, "rate": rate, "rows_owed": want, "parallelism": 2, "checkpoint_interval_ms": 2000,
               "runs": runs, "median": med}
    (demo.results / "measurements-cost.json").write_text(json.dumps(results, indent=2))
    for m in runs:
        demo.note("%-12s medians of 3: the last row %.2f s after the feed, %.2f Worker CPU s%s" % (
            m, med[m]["seconds_to_land_after_feed"], med[m]["worker_cpu_seconds"],
            "; runner %.2f CPU s for %d audits" % (med[m]["runner_cpu_seconds"], med[m]["audits_published"])
            if m == "runner" else ""))
    demo.outcome("runner-cost", "every run landed every row it owes, and the runner audited while it ran",
                 "%d rows x 9; at least one audit in each run with the runner" % want,
                 "audits per run with the runner: %s" % [r["audits_published"] for r in runs["runner"]],
                 all(r["audits_published"] for r in runs["runner"]))


# -- windowed jobs: one increment at a time --------------------------------------------------
#
# The window-audit example's job (ten-second tumbling windows per account over
# three partitions, two source subtasks), fed live: each event is produced as
# its event time comes round, so windows close while the runner audits. The
# Workers keep three checkpoints, so the job's whole lineage is not there to
# audit: each audit is an increment, from the reference's own state at the
# checkpoint before. The independent check is the windows the scenario
# designed, from the events read back from Kafka, and the destination.

import sys as _sys  # noqa: E402

_WINDOW_AUDIT = Path(__file__).resolve().parent.parent / "window-audit"
_sys.path.insert(0, str(_WINDOW_AUDIT))
import window_demo  # noqa: E402
import window_workload as wwork  # noqa: E402
from window_demo import WindowCluster  # noqa: E402

IDENTITY = ["account", "window_start", "window_end"]


class LiveFeeder:
    """Feeds events through one console producer as their event time comes
    round: one second of event time per second of wall time."""

    def __init__(self, demo, topic, events):
        self.events = list(events)
        self.stop_flag = False
        self.paused = False
        self.sent = 0
        self.proc = subprocess.Popen(
            ["docker", "compose", "--project-directory", str(_RECONCILE), "-f",
             str(_RECONCILE / "docker-compose.yml"), "exec", "-T", "kafka",
             "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server=kafka:9092",
             "--topic=%s" % topic, "--property", "parse.key=true", "--property", "key.separator=\t"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True,
            env=demo.services.env)
        import threading
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.t0 = time.time()
        self.thread.start()
        return self

    def _run(self):
        for e in self.events:
            due = self.t0 + (e.ts - wwork.BASE_MS) / 1000.0
            while not self.stop_flag and (self.paused or time.time() < due):
                time.sleep(0.05)
            if self.stop_flag:
                return
            self.proc.stdin.write(e.line())
            self.proc.stdin.flush()
            self.sent += 1

    def pause(self, seconds):
        """Holds the feed for `seconds`: event time does not wait, so the
        events due meanwhile go out together when it resumes."""
        self.paused = True
        time.sleep(seconds)
        self.paused = False

    def done(self):
        return not self.thread.is_alive()

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


_RECONCILE = Path(__file__).resolve().parent.parent / "destination-reconciliation"


def _wstart(demo, name, variant=None, retained=3, **kw):
    topic, table = "wpay-%s" % name, "windows_%s" % name.replace("-", "_")
    demo.services.create_topic(topic, wwork.PARTITIONS)
    demo.services.ch(window_demo.TABLE_DDL.format(table=table, window=1000))
    cluster = WindowCluster(demo, "runner-%s" % name, variant=variant, interval_ms=2000, **kw)
    cluster.retained = retained
    cluster.start(topic, table, demo.services.ch_port)
    return cluster, topic, table


def _wconfig(demo, cluster, name="audit", **over):
    ref = {"command": [demo.reference_python, str(_WINDOW_AUDIT / "reference_windows.py"),
                       "--scope={scope}", "--out={out}",
                       "--bootstrap=127.0.0.1:%d" % demo.services.kafka_port,
                       "--state-out={state_out}", "--max-records={max_records}",
                       "--deadline-s={deadline_s}"],
           "with_state_in": ["--state-in={state_in}"],
           "when_run_start": ["--run-start-offset=ALL=0"]}
    ref.update(over.pop("reference", {}))
    return demo.runner_config(cluster, name=name, key=IDENTITY, reference=ref, **over)


def _timeline(seed, seconds, flush_at):
    """Steady events on every partition for `seconds` of event time, none of
    them late whatever order the job reads the partitions in, then one event
    per partition at `flush_at` seconds, which closes the windows before it."""
    t = wwork.Timeline(seed=seed)
    events = t.steady([0, 1, 2], 0, seconds * 1000, per_second=4)
    events += [t.at(p, flush_at * 1000, kind="purchase") for p in (0, 1, 2)]
    return events


def _designed_and_landed(demo, topic, table, reports, events, state):
    """The windows the scenario designed, from the events read back from
    Kafka over the audited range, final by the last audited checkpoint's
    recorded watermark; and what the destination holds for them."""
    verified = [r for r in reports if r["outcome"] in ("passed", "empty")]
    last = max(verified, key=lambda r: r["checkpoint"])
    scope = json.loads((state / last["evidence"]["dir"] / "scope.json").read_text()) \
        if last.get("evidence", {}).get("dir") else None
    ends = {p["partition"]: p["to"] for s in last["scope"]["sources"] for p in s["partitions"]}
    by_id = wwork.events_by_value(events)
    got = []
    for part, end in sorted(ends.items()):
        records = demo.services.read_range(topic, part, 0, end)
        if [o for o, _ in records] != list(range(0, end)):
            raise Failed("%s[%d] [0, %d) could not be read back whole" % (topic, part, end))
        got += [by_id[wwork.event_id_of(v)] for _, v in records if wwork.event_id_of(v) in by_id]
    final_by = scope["finality"]["watermark_ms"] if scope else None
    design = wwork.design_windows(got, final_by)
    out = demo.services.ch(
        "SELECT account, window_start, window_end, events, toInt64(net * 100), units FROM demo.%s "
        "WHERE window_end <= %d ORDER BY account, window_start" % (table, final_by or 0), fmt="TSV")
    landed = {}
    for line in out.splitlines():
        a, st, en, ev, cents, units = line.split("\t")
        landed.setdefault((a, int(st), int(en)), []).append([int(ev), int(cents), int(units)])
    return design, landed, final_by, len(got)


def _restored(cluster):
    """The job's RESTORED record (restore=, next= lines), or None."""
    p = cluster.markers / "RESTORED"
    if not p.exists():
        return None
    out = {}
    for line in p.read_text().splitlines():
        k, _, v = line.partition("=")
        if v.strip().isdigit():
            out[k.strip()] = int(v)
    return out


def _increment_rows(state, reports):
    """Every final window the increments' references gave, from the artifacts
    the runner kept."""
    rows = {}
    for r in reports:
        ref = state / "audits" / r["id"] / "reference.ndjson"
        if not ref.exists():
            continue
        for line in ref.read_text().splitlines():
            o = json.loads(line)
            if "row" in o:
                k = (o["row"]["account"][1:], int(o["row"]["window_start"][1:]),
                     int(o["row"]["window_end"][1:]))
                rows.setdefault(k, []).append([int(o["row"]["events"][1:]),
                                               int(Decimal(o["row"]["net"][1:].split("@")[0])),
                                               int(o["row"]["units"][1:])])
    return rows


def windows_steady(demo):
    """A windowed job fed live, its Workers keeping three checkpoints, the
    runner polling: every checkpoint audited once, each an increment from the
    reference's own state at the one before, and the windows they owe,
    together, exactly the windows the scenario designed and the destination
    holds, each once."""
    name = "windows-steady"
    cluster, topic, table = _wstart(demo, name)
    config = _wconfig(demo, cluster, limits={"retain_passed": 100000})
    state = _state(config)
    events = _timeline(11, 60, 75)
    poll = demo.audit_spawn(config, label="%s poll" % name)
    feeder = LiveFeeder(demo, topic, events).start()
    try:
        _wait("the feed", feeder.done, 200, 0.5)
        cluster.await_watermark(wwork.BASE_MS + 73_000, timeout=120)
        cluster.await_more(2)
        cluster.stop()
        _wait("the runner to audit every completed checkpoint",
              lambda: _all_audited(demo, cluster, config), 300)
    finally:
        feeder.stop()
        cluster.stop()
        demo.stop_poll(poll)
    reports = demo.reports(state)
    start = json.loads((state / "state.json").read_text())["start"]
    completed = [c for c in cluster.completed() if c >= start]
    by = _by_outcome(reports)
    demo.outcome(name, "every completed checkpoint audited exactly once, each verified",
                 "%d checkpoints, passed or empty" % len(completed),
                 "%d reports, %s" % (len(reports), json.dumps({k: len(v) for k, v in by.items()})),
                 sorted(r["checkpoint"] for r in reports) == completed and set(by) <= {"passed", "empty"}
                 and by.get("passed"))
    chain = [(r["checkpoint"], r["identity"]["window"]["since"]) for r in reports]
    prev = [None] + [c for c, _ in chain[:-1]]
    demo.outcome(name, "a seed, then an increment from each checkpoint's predecessor",
                 "since = the previous checkpoint after the first",
                 "%d chained of %d" % (sum(1 for (c, s), p in zip(chain, prev) if s == p), len(chain)),
                 [s for _, s in chain] == prev)
    scode, s = demo.status(config)
    demo.outcome(name, "the status", "healthy, exit 0, the chain whole",
                 "%s, exit %d, %s" % (s["state"], scode, json.dumps(s.get("window"))),
                 scode == 0 and s["window"] and not s["window"]["chain_broken"])
    design, landed, final_by, n = _designed_and_landed(demo, topic, table, reports, events, state)
    have = _increment_rows(state, reports)
    demo.outcome(name, "the increments' windows are the designed windows, each owed once",
                 "%d designed windows" % len(design),
                 "%d windows from %d increments, each once: %s" % (
                     len(have), len(reports), all(len(v) == 1 for v in have.values())),
                 {k: v[0] for k, v in have.items()} == design and all(len(v) == 1 for v in have.values()))
    demo.outcome(name, "the destination holds the designed windows, each once",
                 "%d windows" % len(design), "%d windows" % len(landed),
                 {k: v for k, v in landed.items()} == {k: [v] for k, v in design.items()})
    walls = [r["costs"]["wall_s"] for r in reports if r["outcome"] in ("passed", "empty")]
    third = max(1, len(walls) // 3)
    early, late = statistics.median(walls[:third]), statistics.median(walls[-third:])
    reread = sum(int(r["costs"]["reference_records"] or 0) for r in reports)
    measured = {"audits": len(reports), "increment_wall_s_median": statistics.median(walls),
                "first_third_median_s": early, "last_third_median_s": late,
                "records_reread": reread, "records_in_range": n, "final_by": final_by}
    (demo.results / "measurements-windows-steady.json").write_text(json.dumps(measured, indent=2))
    demo.note("increments: median %.2f s, first third %.2f s, last third %.2f s; %d records reread "
              "for %d in the range" % (measured["increment_wall_s_median"], early, late, reread, n))
    demo.outcome(name, "each source record read once, and an increment's cost does not grow",
                 "records reread = records in range; last third's median within 1.5x the first's",
                 "%d = %d; %.2f s vs %.2f s" % (reread, n, late, early), reread == n and late <= 1.5 * early)


def windows_restore(demo):
    """worker-1 runs the windowed job alone and is killed in a quiet stretch,
    no window closing; the job restores on worker-2 and carries on. The
    chain resumes from the reference's state at the restore point."""
    name = "windows-restore"
    cluster, topic, table = _wstart(demo, name, worker2_late=True, defer_worker2=True)
    config = _wconfig(demo, cluster, limits={"retain_passed": 100000})
    state = _state(config)
    events = _timeline(12, 50, 65)
    poll = demo.audit_spawn(config, label="%s poll" % name)
    feeder = LiveFeeder(demo, topic, events).start()
    try:
        _wait("25 s of the feed", lambda: time.time() - feeder.t0 > 25, 60, 0.5)
        feeder.paused = True
        cluster.await_more(3)
        w1 = cluster.procs["worker-1"]
        w1.kill()
        w1.wait()
        demo.note("worker-1 killed in a quiet stretch")
        cluster.worker(2)
        _wait("the job to restore", lambda: (cluster.markers / "RESTORED").exists(), 120)
        feeder.paused = False
        _wait("the feed", feeder.done, 200, 0.5)
        cluster.await_watermark(wwork.BASE_MS + 63_000, timeout=180)
        cluster.await_more(2)
        cluster.stop()
        _wait("the runner to audit every completed checkpoint",
              lambda: _all_audited(demo, cluster, config), 300)
    finally:
        feeder.stop()
        cluster.stop()
        demo.stop_poll(poll)
    restored = _restored(cluster) or {"restore": None, "next": 1 << 62}
    reports = demo.reports(state)
    by = _by_outcome(reports)
    after = [r for r in reports if r["checkpoint"] >= restored["next"]]
    first = min(after, key=lambda r: r["checkpoint"]) if after else None
    demo.outcome(name, "the chain resumes from the reference's state at the restore point",
                 "the restored run's first audit an increment from checkpoint %d" % restored["restore"],
                 "since %s" % (first and first["identity"]["window"]["since"]),
                 first is not None and first["identity"]["window"]["since"] == restored["restore"])
    demo.outcome(name, "every scope verified, the chain whole", "passed or empty, no break",
                 json.dumps({k: len(v) for k, v in by.items()}),
                 set(by) <= {"passed", "empty"} and not demo.status(config)[1]["window"]["chain_broken"])
    design, landed, _, _ = _designed_and_landed(demo, topic, table, reports, events, state)
    have = _increment_rows(state, reports)
    demo.outcome(name, "the increments' windows and the destination are the designed windows",
                 "%d windows, each once" % len(design), "%d from the increments, %d landed" % (len(have), len(landed)),
                 {k: v[0] for k, v in have.items()} == design and all(len(v) == 1 for v in have.values())
                 and landed == {k: [v] for k, v in design.items()})


def windows_defect(demo):
    """The job adds refunds to the net amount. Every increment that owes
    windows is a transformation discrepancy, the delivery sound."""
    name = "windows-defect"
    cluster, topic, table = _wstart(demo, name, variant="wrong-sign")
    config = _wconfig(demo, cluster, limits={"retain_passed": 100000})
    events = _timeline(13, 30, 45)
    poll = demo.audit_spawn(config, label="%s poll" % name)
    feeder = LiveFeeder(demo, topic, events).start()
    try:
        _wait("the feed", feeder.done, 200, 0.5)
        cluster.await_watermark(wwork.BASE_MS + 43_000, timeout=120)
        cluster.await_more(2)
        cluster.stop()
        _wait("the runner to audit every completed checkpoint",
              lambda: _all_audited(demo, cluster, config), 300)
    finally:
        feeder.stop()
        cluster.stop()
        demo.stop_poll(poll)
    state = _state(config)
    reports = demo.reports(state)
    owing = [r for r in reports if (r["scope"] or {}).get("expected_rows")]
    # The defect changes a window's net amount only where a refund was
    # folded into it: exactly those increments are discrepancies.
    refunds = {(e.account, (e.ts // wwork.SIZE_MS) * wwork.SIZE_MS) for e in events
               if e.kind == "refund" and e.fate == "folded"}
    wrong = []
    rel = set()
    for r in owing:
        rel |= {f["relationship"] for f in r["findings"]}
        ref = state / "audits" / r["id"] / "reference.ndjson"
        keys = set()
        for line in (ref.read_text().splitlines() if ref.exists() else []):
            o = json.loads(line)
            if "row" in o:
                keys.add((o["row"]["account"][1:], int(o["row"]["window_start"][1:])))
        has_refund = bool(keys & refunds)
        if (r["outcome"] == "discrepancy") != has_refund:
            wrong.append((r["checkpoint"], r["outcome"], has_refund))
    scode, s = demo.status(config)
    demo.outcome(name, "exactly the increments owing a window with a refund are discrepancies, in "
                 "the transformation", "discrepancy where a refund was folded, passed elsewhere; "
                 "findings not the delivery; status exit 1",
                 "%d owing: %s; %d misjudged %s; relationships %s; status %s exit %d" % (
                     len(owing), sorted({r["outcome"] for r in owing}), len(wrong), wrong[:3],
                     sorted(rel), s["state"], scode),
                 owing and not wrong and any(r["outcome"] == "discrepancy" for r in owing)
                 and "transformation" in rel and "delivery" not in rel and scode == 1)


def windows_behind(demo):
    """The runner is stopped while the Workers, keeping two checkpoints, purge
    the one its newest held state is at. Started again, it says the chain is
    broken, by name, and verifies nothing after it."""
    name = "windows-behind"
    cluster, topic, table = _wstart(demo, name, retained=2)
    config = _wconfig(demo, cluster)
    state = _state(config)
    events = _timeline(14, 50, 65)
    feeder = LiveFeeder(demo, topic, events).start()
    poll = demo.audit_spawn(config, label="%s poll" % name)
    try:
        _wait("two verified increments", lambda: sum(1 for r in demo.reports(state)
                                                     if r["outcome"] in ("passed", "empty")) >= 3, 120, 0.5)
        demo.stop_poll(poll)
        held = json.loads((state / "state.json").read_text())["window"]["states"]
        newest = max(int(c) for c in held)
        cluster.await_more(6)
        poll = demo.audit_spawn(config, label="%s poll again" % name)
        _wait("the feed", feeder.done, 200, 0.5)
        cluster.await_more(2)
        cluster.stop()
        _wait("the runner to reach every completed checkpoint",
              lambda: _all_audited(demo, cluster, config), 300)
    finally:
        feeder.stop()
        cluster.stop()
        demo.stop_poll(poll)
    reports = demo.reports(state)
    first_break = min((r["checkpoint"] for r in reports if "chain is broken" in r["reason"]), default=None)
    later = [r for r in reports if first_break is not None and r["checkpoint"] >= first_break]
    scode, s = demo.status(config)
    broken = (s.get("window") or {}).get("chain_broken")
    demo.outcome(name, "after the runner fell behind, the chain is broken by name and nothing after "
                 "it is verified", "from the first break, partial or expired; status incomplete, the "
                 "break named (the runner's newest state was at checkpoint %d)" % newest,
                 "%s; status %s exit %d; break %s" % (
                     json.dumps({k: len(v) for k, v in _by_outcome(later).items()}), s["state"], scode,
                     broken and broken["why"][:120]),
                 later and all(r["outcome"] in ("partial", "expired") for r in later) and broken
                 and "fell behind" in broken["why"] and scode == 3)


def windows_cost(demo):
    """The cost of an increment against the whole lineage's audit, on a job
    whose Workers keep every checkpoint so the whole lineage can be audited
    at the end: the increments' wall time stays flat while the whole
    lineage's scope and reference grow with history."""
    name = "windows-cost"
    cluster, topic, table = _wstart(demo, name, retained=100000)
    config = _wconfig(demo, cluster)
    events = _timeline(15, 120, 135)
    poll = demo.audit_spawn(config, label="%s poll" % name)
    feeder = LiveFeeder(demo, topic, events).start()
    try:
        _wait("the feed", feeder.done, 300, 0.5)
        cluster.await_watermark(wwork.BASE_MS + 133_000, timeout=120)
        cluster.await_more(2)
        cluster.stop()
        _wait("the runner to audit every completed checkpoint",
              lambda: _all_audited(demo, cluster, config), 300)
    finally:
        feeder.stop()
        cluster.stop()
        demo.stop_poll(poll)
    reports = demo.reports(_state(config))
    last = max(r["checkpoint"] for r in reports)
    bundle = demo.work / "bundles" / name
    cluster.collect(bundle)
    t0 = time.time()
    scope_path = demo.results / ("%s-whole.scope.json" % name)
    r = subprocess.run([str(demo.bin / "clink"), "incident-scope", str(bundle), "--checkpoint=%d" % last,
                        "--out=%s" % scope_path], capture_output=True, text=True, timeout=300)
    scope_s = time.time() - t0
    t0 = time.time()
    ref_path = demo.results / ("%s-whole.reference.ndjson" % name)
    rr = subprocess.run([demo.reference_python, str(_WINDOW_AUDIT / "reference_windows.py"),
                         "--scope=%s" % scope_path, "--bootstrap=127.0.0.1:%d" % demo.services.kafka_port,
                         "--out=%s" % ref_path, "--run-start-offset=ALL=0"],
                        capture_output=True, text=True, timeout=900)
    ref_s = time.time() - t0
    walls = [x["costs"]["wall_s"] for x in reports if x["outcome"] == "passed"]
    refs = [float(x["steps"]["reference"]["seconds"]) for x in reports if x["outcome"] == "passed"]
    measured = {"increments": len(reports), "increment_wall_s_median": statistics.median(walls),
                "increment_reference_s_median": statistics.median(refs),
                "whole_lineage_checkpoints": last, "whole_scope_s": round(scope_s, 3),
                "whole_reference_s": round(ref_s, 3), "whole_scope_exit": r.returncode,
                "whole_reference_exit": rr.returncode, "events": len(events)}
    (demo.results / "measurements-windows-cost.json").write_text(json.dumps(measured, indent=2))
    demo.note("an increment: median %.2f s (its reference %.2f s); the whole lineage of %d checkpoints: "
              "scope %.2f s, reference %.2f s" % (measured["increment_wall_s_median"],
                                                   measured["increment_reference_s_median"], last,
                                                   scope_s, ref_s))
    demo.outcome(name, "the whole lineage can be audited here, for the comparison",
                 "scope exit 0, reference exit 0", "scope exit %d, reference exit %d" % (r.returncode, rr.returncode),
                 r.returncode == 0 and rr.returncode == 0, (r.stderr + rr.stderr)[-200:])

ALL = [
    ("runner-steady", steady),
    ("runner-defect", defect),
    ("runner-corruption", corruption),
    ("runner-crash", crash),
    ("runner-outage", outage),
    ("runner-budget", budget),
    ("runner-latest-only", latest_only),
    ("runner-expired", expired),
    ("runner-history-expired", history_expired),
    ("runner-cost", cost),
    ("runner-windows-steady", windows_steady),
    ("runner-windows-restore", windows_restore),
    ("runner-windows-defect", windows_defect),
    ("runner-windows-behind", windows_behind),
    ("runner-windows-cost", windows_cost),
]
