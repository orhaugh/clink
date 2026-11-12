"""The state-inspection example's scenarios (README.md, "Scenarios")."""
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from inspect_demo import Cluster, Feed, Failed, expected_changes, fold, measure, read_dataset

ACCOUNTS = 300


def opening():
    """Every account opened with a deposit; every third withdraws."""
    out = [(a, "deposit", 100 + a) for a in range(1, ACCOUNTS + 1)]
    out += [(a, "withdraw", 7) for a in range(1, ACCOUNTS + 1, 3)]
    return out


def known_changes():
    """20 accounts added, 15 closed and 50 changed; the rest untouched."""
    out = [(a, "deposit", 50) for a in range(ACCOUNTS + 1, ACCOUNTS + 21)]
    out += [(a, "close", 0) for a in range(1, 16)]
    out += [(a, "deposit", 5) for a in range(100, 150)]
    return out


def compare_slots(demo, scenario, label, report, expected):
    ok = True
    for slot, want in expected.items():
        got = demo.slot(report, slot)
        observed = None if got is None else {k: got[k] for k in ("added", "removed", "changed", "unchanged")}
        ok &= demo.outcome(scenario, "%s: %s slot compared exactly" % (label, slot),
                           "status compared, %s" % want,
                           "status %s, %s" % (got and got["status"], observed),
                           got is not None and got["status"] == "compared" and observed == want)
    return ok


# ---- a running job ------------------------------------------------------------------

def running(demo):
    s = "inspect-running"
    topic = "si-running"
    demo.kafka.create(topic)
    c = Cluster(demo, s, topic)
    c.start()
    try:
        job = c.submit()
        demo.kafka.send(topic, opening())

        # Exported while the job reads a feed: each attempt either publishes
        # a complete dataset or is refused, never something in between.
        trickle = [(a, "deposit", 1) for _ in range(4) for a in range(1, ACCOUNTS + 1)]
        feed = Feed(demo.kafka, topic, trickle, batch=40, pause=0.2)
        feed.start()
        time.sleep(2)
        attempts, refusals, live = 0, {}, None
        while attempts < 40 and live is None:
            attempts += 1
            out = c.dir / "datasets" / ("live-%d" % attempts)
            code, j = demo.export(c, job, out)
            if code == 0 and feed.running:
                live = out
            elif code == 1:
                refusals[j.get("kind", "?")] = refusals.get(j.get("kind", "?"), 0) + 1
            elif code != 0:
                demo.outcome(s, "an export while the job runs", "exit 0 or a refusal (1)", code, False,
                             json.dumps(j)[:300])
                break
            time.sleep(0.1)
        demo.outcome(s, "an export while the job runs publishes a complete dataset",
                     "complete while the feed runs", "%s after %d attempt(s), refusals %s" % (
                         "published" if live else "none", attempts, refusals), live is not None)
        demo.costs["live_export_attempts"] = {"attempts": attempts, "refusals": refusals}
        if live is not None:
            demo.check_state(s, "the live export", live, topic)
        feed.halt.set()
        feed.join()
        if feed.error:
            raise Failed("the feed failed: %s" % feed.error)

        a = demo.caught_up(c, job, "a")
        demo.check_state(s, "dataset a", a, topic)
        _, state_a, _ = read_dataset(a)
        demo.kafka.send(topic, known_changes())
        b = demo.caught_up(c, job, "b")
        demo.check_state(s, "dataset b", b, topic)
        _, state_b, _ = read_dataset(b)

        # Known changes between them.
        code, report = demo.diff(s, "a-b", a, b)
        demo.outcome(s, "state-diff a b exits 1 (differences)", 1, code, code == 1)
        if report:
            compare_slots(demo, s, "a to b", report, expected_changes(state_a, state_b))
            src = [x for x in report["slots"] if x["scope"] == "operator"]
            demo.outcome(s, "a to b: the source's positions compared per subtask, each changed",
                         "compared, 2 changed", [(x["status"], x["changed"]) for x in src],
                         len(src) == 1 and src[0]["status"] == "compared" and src[0]["changed"] == 2)
            hexed = all(isinstance(sm.get("user_key_hex"), str) and "user_key_bytes" in sm
                        for x in report["slots"] for sm in x["samples"])
            demo.outcome(s, "samples are hex with lengths, bounded", "hex, at most 20 per slot",
                         max((len(x["samples"]) for x in report["slots"]), default=0),
                         hexed and all(len(x["samples"]) <= 20 for x in report["slots"]))
        again = c.dir / "datasets" / "a-again"
        demo.export(c, job, again, checkpoint=read_dataset(a)[0]["checkpoint"]["id"])
        code, report = demo.diff(s, "a-a", a, again)
        demo.outcome(s, "the same checkpoint exported twice: no differences", "exit 0", code,
                     code == 0 and report and report["established"])

        # Moved elsewhere, it reads and compares the same.
        moved = demo.work / "moved" / "b-elsewhere"
        moved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(b, moved.parent / "copying")
        (moved.parent / "copying").rename(moved)
        sql = "SELECT slot, scope, COUNT(*) AS n, SUM(value_len) AS bytes FROM state GROUP BY slot, scope"
        here = demo.query(s, "b-summary", b, sql)
        there = demo.query(s, "moved-summary", moved, sql)
        demo.outcome(s, "the moved dataset queries the same", "same rows, exit 0",
                     "exit %d/%d, same %s" % (here.returncode, there.returncode, here.stdout == there.stdout),
                     here.returncode == 0 and here.stdout == there.stdout and here.stdout != "")
        code_m, rep_m = demo.diff(s, "a-moved", a, moved)
        code_b, rep_b = demo.diff(s, "a-b-again", a, b)
        strip = lambda r: {k: v for k, v in r.items() if k not in ("a", "b")} if r else None
        demo.outcome(s, "the moved dataset compares the same", "same exit and report",
                     "exit %d/%d" % (code_m, code_b), code_m == code_b and strip(rep_m) == strip(rep_b))

        # Two readers outside clink's tooling, and clink's own SQL.
        _, by_pyarrow, _ = read_dataset(moved)
        demo.outcome(s, "pyarrow reads the moved dataset as the expected state",
                     "%d accounts, equal" % len(state_b), "%d accounts" % len(by_pyarrow),
                     by_pyarrow == state_b)
        con = duckdb.connect()
        rows = con.execute(
            "SELECT slot, COUNT(*), COUNT(DISTINCT hex(user_key)) FROM read_parquet(?) "
            "WHERE scope = 'keyed' GROUP BY slot ORDER BY slot", [str(moved / "entries.parquet")]).fetchall()
        demo.outcome(s, "duckdb counts each slot's accounts", [("balance", len(state_b), len(state_b)),
                                                               ("count", len(state_b), len(state_b))],
                     rows, rows == [("balance", len(state_b), len(state_b)), ("count", len(state_b), len(state_b))])
        q = demo.query(s, "balances", moved,
                       "SELECT user_key_hex, value_hex FROM state WHERE slot = 'balance'")
        from_sql = {}
        for line in q.stdout.splitlines():
            r = json.loads(line)
            from_sql[int.from_bytes(bytes.fromhex(r["user_key_hex"]), "little", signed=True)] = \
                int.from_bytes(bytes.fromhex(r["value_hex"]), "little", signed=True)
        demo.outcome(s, "state-query's balances decode to the expected ones", "%d equal" % len(state_b),
                     "%d rows, exit %d" % (len(from_sql), q.returncode),
                     q.returncode == 0 and from_sql == {k: v[1] for k, v in state_b.items()})

        # A Worker killed while the feed runs; the job restores from its last
        # completed checkpoint and numbers its checkpoints on from the RESTORED
        # record's `next`.
        restored_file = c.dir / "checkpoints" / "_jobs" / str(job) / "RESTORED"
        extra = [(a, "deposit", 3) for a in range(200, 261)] + \
                [(a, "deposit", 9) for a in range(ACCOUNTS + 21, ACCOUNTS + 41)]
        feed = Feed(demo.kafka, topic, extra * 3, batch=20, pause=0.15)
        feed.start()
        time.sleep(1.0)
        c.kill("worker-1")
        c.worker()
        feed.join()
        deadline = time.time() + 120
        while not restored_file.exists() and time.time() < deadline:
            time.sleep(0.2)
        record = dict(line.split("=", 1) for line in restored_file.read_text().split() if "=" in line) \
            if restored_file.exists() else {}
        if "next" not in record:
            raise Failed("the job recorded no restore after worker-1 was killed")
        c.await_marker(job, lambda cid, m: cid >= int(record["next"]), "after the restore")
        cc = demo.caught_up(c, job, "c")
        doc_c, state_c, _ = read_dataset(cc)
        demo.outcome(s, "dataset c is of the restored run", "restored from %s, numbered from %s" % (
                         record["restore"], record["next"]),
                     doc_c["selection"]["restored"],
                     doc_c["selection"]["restored"] == {"restore": int(record["restore"]), "next": int(record["next"])}
                     and doc_c["checkpoint"]["id"] >= int(record["next"]))
        demo.check_state(s, "dataset c (after the Worker was killed)", cc, topic)
        code, report = demo.diff(s, "b-c", b, cc)
        demo.outcome(s, "b to c: the restore is in the lineage", "the restore named",
                     report and report["lineage"]["notes"],
                     code == 1 and report and any("restored from checkpoint %s" % record["restore"] in n
                                                  for n in report["lineage"]["notes"]))
        if report:
            compare_slots(demo, s, "b to c", report, expected_changes(state_b, state_c))

        # Rescaled with nothing new to read: entries move, nothing changes.
        out = c.ctl("rescale-op", "--job-id=%d" % job, "--op=accounts", "--parallelism=4")
        demo.note("rescale-op: %s" % out.strip()[-300:])
        gen_c = doc_c["checkpoint"]["generation"]
        cid = c.await_marker(job, lambda cid, m: m["generation"] > gen_c and len(m["subtasks"]) == 7,
                             "at the new layout")
        # A rescale restarts the job in a new generation; wait until it has
        # caught up again (nothing new was produced).
        time.sleep(2)
        d = c.dir / "datasets" / "d"
        code, _ = demo.export(c, job, d, checkpoint=cid)
        demo.check_state(s, "dataset d (after the rescale)", d, topic)
        code, report = demo.diff(s, "c-d", cc, d)
        if report:
            keyed = [x for x in report["slots"] if x["scope"] == "keyed"]
            moved_entries = sum(x["relocated"] for x in keyed)
            demo.outcome(s, "c to d: no keyed entry added, removed or changed, and some relocated",
                         "0 changes, relocated > 0", "changes %d, relocated %d" % (
                             sum(x["added"] + x["removed"] + x["changed"] for x in keyed), moved_entries),
                         all(x["status"] == "compared" for x in keyed) and moved_entries > 0 and
                         sum(x["added"] + x["removed"] + x["changed"] for x in keyed) == 0)
            others = [x for x in report["slots"] if x["scope"] != "keyed"]
            demo.outcome(s, "c to d: operator state compared per subtask or declared not comparable",
                         "compared with no change, or subtasks-changed", [(x["status"], x["changed"]) for x in others],
                         all((x["status"] == "compared" and x["added"] + x["removed"] + x["changed"] == 0)
                             or x["status"] == "subtasks-changed" for x in others))
            demo.outcome(s, "c to d is never reported as a difference", "exit 0 or 3", code, code in (0, 3))
    finally:
        c.stop()


# ---- schema version 2 ---------------------------------------------------------------

def schema(demo):
    s = "inspect-schema"
    topic = "si-schema"
    demo.kafka.create(topic)
    c = Cluster(demo, s, topic)
    c.start()
    try:
        job = c.submit()
        demo.kafka.send(topic, opening())
        v1 = demo.caught_up(c, job, "v1")
        demo.check_state(s, "the version 1 dataset", v1, topic)
        out = c.ctl("stop", "--job-id=%d" % job)
        import re
        m = re.search(r"savepoint_checkpoint_id=(\d+)", out)
        if not m:
            raise Failed("the job did not stop at a checkpoint: %s" % out[-300:])
        # Version 2's Workers: the balance in hundredths, migrated on restore.
        for name in [n for n in c.procs if n.startswith("worker-")]:
            c.stop_worker(name)
        c.schema = 2
        c.worker()
        c.worker()
        job2 = c.submit(restore=(str(c.dir / "checkpoints" / "_jobs" / str(job)), int(m.group(1))))
        v2 = demo.caught_up(c, job2, "v2")
        demo.check_state(s, "the version 2 dataset (balances in hundredths)", v2, topic, scale=100)
        code, report = demo.diff(s, "v1-v2", v1, v2)
        bal = report and demo.slot(report, "balance")
        cnt = report and demo.slot(report, "count")
        demo.outcome(s, "the balance slot is not compared across the schema change", "schema-changed, exit 3",
                     "%s, exit %d" % (bal and bal["status"], code),
                     code == 3 and bal and bal["status"] == "schema-changed" and bal["changed"] == 0
                     and "balance v1" in bal["note"] and "balance v2" in bal["note"])
        demo.outcome(s, "the count slot, unchanged in schema, is compared", "compared, no change",
                     cnt and (cnt["status"], cnt["changed"]), cnt and cnt["status"] == "compared" and
                     cnt["changed"] == 0 and cnt["unchanged"] == len(read_dataset(v1)[1]))
        code, report = demo.diff(s, "v1-v2-raw", v1, v2, extra=("--raw-bytes",))
        bal = report and demo.slot(report, "balance")
        nonzero = sum(1 for _, b in read_dataset(v1)[1].values() if b != 0)
        demo.outcome(s, "--raw-bytes compares the balance's bytes, labelled raw",
                     "raw-bytes, %d changed, every sample raw, exit 1" % nonzero,
                     "%s, %s changed, exit %d" % (bal and bal["comparison"], bal and bal["changed"], code),
                     code == 1 and bal and bal["comparison"] == "raw-bytes" and bal["changed"] == nonzero
                     and all(x.get("raw") for x in bal["samples"]))
    finally:
        c.stop()


# ---- faults, on copies of a stopped job's checkpoints -----------------------------

def flip(path, at=None):
    data = bytearray(path.read_bytes())
    i = len(data) // 2 if at is None else at
    data[i] ^= 0x40
    path.write_bytes(bytes(data))


def faults(demo):
    s = "inspect-faults"
    topic = "si-faults"
    demo.kafka.create(topic)
    c = Cluster(demo, s, topic)
    c.start()
    try:
        job = c.submit()
        demo.kafka.send(topic, opening())
        demo.caught_up(c, job, "running")
        out = c.ctl("stop", "--job-id=%d" % job)
    finally:
        c.stop()
    import re
    final = int(re.search(r"savepoint_checkpoint_id=(\d+)", out).group(1))
    ck = c.dir / "checkpoints"
    work = demo.work / s
    clink = demo.clink

    def export(src, out, *extra):
        r = demo.offline(clink("state-export", "--checkpoint-dir=%s" % src, "--job=%d" % job,
                               "--checkpoint=%d" % final, "--ha-dir=%s" % (c.dir / "ha"),
                               "--out=%s" % out, "--json", *extra), s)
        try:
            return r.returncode, json.loads(r.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            return r.returncode, {"stderr": r.stderr[-300:]}

    base = work / "base"
    code, exported = export(ck, base)
    demo.outcome(s, "the stopped job's final checkpoint exports complete", "exit 0", code, code == 0)
    demo.check_state(s, "the final checkpoint", base, topic)
    # A finished job's manifest is gone, so the graph is not in the dataset;
    # its operators are named from the identities the marker records.
    named = {o.get("graph_id"): o.get("named_by")
             for o in json.loads((base / "dataset.json").read_text())["operators"]}
    demo.outcome(s, "the finished job's last checkpoint names its operators from the marker",
                 "accounts and source, by layout", named,
                 named == {"accounts": "layout", "source": "layout"})

    # The digest the export printed identifies the dataset: a rewrite of
    # dataset.json that keeps every hash it records is caught by it alone.
    digest = exported.get("digest", "")
    r = demo.offline(clink("state-cat", "--dataset=%s" % base, "--digest=%s" % digest), s)
    demo.outcome(s, "the dataset is the one with the digest the export printed", "exit 0",
                 r.returncode, r.returncode == 0 and len(digest) == 64)
    rewritten = work / "base-rewritten"
    shutil.copytree(base, rewritten)
    doc = json.loads((rewritten / "dataset.json").read_text())
    doc["in_doubt"]["external_outcomes"] = "verified"
    (rewritten / "dataset.json").write_text(json.dumps(doc, indent=2))
    plain = demo.offline(clink("state-cat", "--dataset=%s" % rewritten), s)
    pinned = demo.offline(clink("state-cat", "--dataset=%s" % rewritten, "--digest=%s" % digest), s)
    demo.outcome(s, "a rewritten dataset.json passes its own hashes but not the recorded digest",
                 "exit 0 without, exit 2 with --digest", "exit %d / %d" % (plain.returncode, pinned.returncode),
                 plain.returncode == 0 and pinned.returncode == 2 and "digest" in pinned.stderr)
    state_dir = ck / "_state" / str(job)
    gen = max(int(p.name[1:]) for p in state_dir.iterdir() if p.name.startswith("v"))
    snap = lambda root, p: root / "_state" / str(job) / ("v%d" % gen) / str(p) / ("checkpoint-%d.snap" % final)

    # A participant's snapshot missing.
    miss = work / "ck-missing"
    shutil.copytree(ck, miss)
    snap(miss, 3).unlink()
    code, j = export(miss, work / "missing")
    demo.outcome(s, "a missing participant refuses the export", "exit 1, incomplete, names participant 3",
                 "exit %d, %s" % (code, j.get("kind")),
                 code == 1 and j.get("kind") == "incomplete" and "participant 3" in j.get("reason", "")
                 and not (work / "missing").exists())
    code, j = export(miss, work / "missing-partial", "--allow-partial")
    doc = json.loads((work / "missing-partial" / "dataset.json").read_text()) if code == 3 else {}
    demo.outcome(s, "--allow-partial publishes it marked partial", "exit 3, complete false, the reason",
                 "exit %d, complete %s" % (code, doc.get("complete")),
                 code == 3 and doc.get("complete") is False and
                 any("participant 3" in x for x in doc.get("incomplete", [])))
    q = demo.offline(clink("state-query", "--dataset=%s" % (work / "missing-partial"), "--sql=SELECT 1 FROM state"), s,
                     loopback=True)
    demo.outcome(s, "state-query refuses a partial dataset", "exit 3", q.returncode, q.returncode == 3)
    q = demo.offline(clink("state-query", "--dataset=%s" % (work / "missing-partial"), "--allow-partial",
                           "--sql=SELECT scope, COUNT(*) AS n FROM state GROUP BY scope"), s, loopback=True)
    demo.outcome(s, "--allow-partial queries it and says it is partial", "exit 0, PARTIAL on stderr",
                 "exit %d" % q.returncode, q.returncode == 0 and "PARTIAL" in q.stderr)
    code, rep = demo.diff(s, "base-partial", base, work / "missing-partial")
    demo.outcome(s, "a comparison with a partial dataset is not established", "exit 3, nothing compared",
                 "exit %d" % code, code == 3 and rep and not rep["established"] and not rep["slots"])

    # A corrupt snapshot, and a damaged dataset.
    bad = work / "ck-corrupt"
    shutil.copytree(ck, bad)
    flip(snap(bad, 2))
    code, j = export(bad, work / "corrupt")
    demo.outcome(s, "a corrupt snapshot refuses the export", "exit 1, the sidecar check named",
                 "exit %d, %s" % (code, j.get("kind")),
                 code == 1 and "checkpoint.participants" in j.get("reason", "") and not (work / "corrupt").exists())
    damaged = work / "base-damaged"
    shutil.copytree(base, damaged)
    flip(damaged / "entries.parquet")
    for verb, argv in (("state-query", ["--dataset=%s" % damaged, "--sql=SELECT 1 FROM state"]),
                       ("state-diff", ["--a=%s" % base, "--b=%s" % damaged]),
                       ("state-cat", ["--dataset=%s" % damaged])):
        r = demo.offline(clink(verb, *argv), s, loopback=verb == "state-query")
        demo.outcome(s, "%s refuses a dataset changed after export" % verb, "exit 2, hash named",
                     "exit %d" % r.returncode, r.returncode == 2 and "hash" in r.stderr)
    linked = work / "base-link"
    os.symlink(base, linked)
    r = demo.offline(clink("state-cat", "--dataset=%s" % linked), s)
    demo.outcome(s, "a dataset reached through a link is refused", "exit 2", r.returncode, r.returncode == 2)

    # Bounds: a refusal or a partial, never a truncated complete.
    code, j = export(ck, work / "few-entries", "--max-entries=10")
    demo.outcome(s, "--max-entries refuses a checkpoint past it", "exit 1, incomplete",
                 "exit %d, %s" % (code, j.get("kind")), code == 1 and j.get("kind") == "incomplete")
    code, j = export(ck, work / "few-entries-partial", "--max-entries=10", "--allow-partial")
    demo.outcome(s, "--max-entries with --allow-partial is a partial", "exit 3", code, code == 3)
    code, j = export(ck, work / "few-bytes", "--max-bytes=2000")
    demo.outcome(s, "--max-bytes refuses the collection", "exit 1, bound",
                 "exit %d, %s" % (code, j.get("kind")), code == 1 and j.get("kind") == "bound")
    code, rep = demo.diff(s, "limit", base, base, extra=("--max-entries=10",))
    demo.outcome(s, "a diff that reaches its limit is not established, never 'no differences'",
                 "exit 3, limit reached", "exit %d, limit %s" % (code, rep and rep["limit_reached"]),
                 code == 3 and rep and rep["limit_reached"] and not rep["established"])


# ---- retention ------------------------------------------------------------------------

def retention(demo):
    s = "inspect-retention"
    topic = "si-retention"
    demo.kafka.create(topic)
    c = Cluster(demo, s, topic, retained=2, interval_ms=300)
    c.start()
    try:
        job = c.submit()
        demo.kafka.send(topic, opening())
        feed = Feed(demo.kafka, topic, [(a, "deposit", 1) for _ in range(10) for a in range(1, ACCOUNTS + 1)],
                    batch=60, pause=0.1)
        feed.start()
        time.sleep(2)
        outcomes = {"complete": 0}
        published = []
        n = 0
        while feed.running and n < 60:
            ids = sorted(c.markers(job))
            if len(ids) < 4:
                time.sleep(0.2)
                continue
            # The newest completed checkpoint, which the Workers purge within
            # a checkpoint or two; the one before it, usually purged already;
            # and one long purged whose marker remains.
            for pick in (ids[-1], ids[-2], ids[0]):
                n += 1
                out = c.dir / "datasets" / ("r-%d-%d" % (n, pick))
                code, j = demo.export(c, job, out, checkpoint=pick)
                if code == 0:
                    outcomes["complete"] += 1
                    published.append(out)
                elif code == 1:
                    outcomes[j.get("kind", "?")] = outcomes.get(j.get("kind", "?"), 0) + 1
                else:
                    outcomes["exit %d" % code] = outcomes.get("exit %d" % code, 0) + 1
        feed.halt.set()
        feed.join()
        demo.costs["retention_race"] = outcomes
        demo.outcome(s, "every attempt published a complete dataset or was refused", "exit 0 or 1",
                     outcomes, all(k in ("complete", "changed", "incomplete", "missing") for k in outcomes))
        demo.outcome(s, "a checkpoint whose state was purged is refused", "incomplete > 0",
                     outcomes.get("incomplete", 0), outcomes.get("incomplete", 0) > 0)
        demo.outcome(s, "the newest checkpoint is exported while the Workers purge behind it",
                     "complete > 0", outcomes["complete"], outcomes["complete"] > 0)
        ok = 0
        for p in published:
            ok += bool(demo.check_state(s, "retention dataset %s" % p.name, p, topic))
        demo.outcome(s, "every published dataset is one checkpoint's state, never mixed",
                     "%d of %d" % (len(published), len(published)), "%d of %d" % (ok, len(published)),
                     published and ok == len(published))
    finally:
        c.stop()


# ---- one SQL job on each backend ---------------------------------------------------

BACKEND_SQL = """CREATE TABLE events (account BIGINT, amount BIGINT) WITH (
    connector = 'kafka', format = 'json', brokers = '@BROKERS@', topic = '@TOPIC@',
    group_id = '@GROUP@', auto_offset_reset = 'earliest');
CREATE TABLE totals (account BIGINT, n BIGINT, total BIGINT) WITH (connector = 'blackhole');
INSERT INTO totals SELECT account, COUNT(*) AS n, SUM(amount) AS total FROM events GROUP BY account;
"""

BACKENDS = [("file", None), ("rocksdb", "rocksdb://{dir}/rocksdb"),
            ("changelog", "changelog+file://{dir}/changelog"),
            ("changelog-rocksdb", "changelog+rocksdb://{dir}/changelog-rocksdb")]


def backends(demo):
    """The same SQL job over the same records on four state backends. The file
    backend's dataset is the reference: RocksDB's and the changelog's must hold
    exactly its keyed entries. A changelog whose materialisations live in
    RocksDB is the case this release does not export."""
    import re
    s = "inspect-backends"
    topic = "si-backends"
    demo.kafka.create(topic)
    demo.kafka.send_json(topic, ['{"account": %d, "amount": %d}' % (a, a * k)
                                 for k in (1, 2, 3) for a in range(1, 51)])
    c = Cluster(demo, s, topic)
    c.start()
    jobs = {}
    try:
        for label, uri in BACKENDS:
            sql = (BACKEND_SQL.replace("@BROKERS@", "127.0.0.1:%d" % demo.kafka.kafka_port)
                   .replace("@TOPIC@", topic).replace("@GROUP@", "%s-%s" % (topic, label)))
            path = c.dir / ("job-%s.sql" % label)
            path.write_text(sql)
            args = [str(demo.bin / "clink_submit_sql"), "--file", str(path), "--coordinator-host",
                    "127.0.0.1", "--coordinator-port", str(c.http), "--name", "%s-%s" % (s, label),
                    "--checkpoint-dir", str(c.dir / "checkpoints"), "--checkpoint-interval-ms", "1000"]
            if uri:
                args += ["--state-backend", uri.format(dir=c.dir)]
            r = subprocess.run(args, capture_output=True, text=True, timeout=120)
            (c.dir / "logs" / ("submit-%s.log" % label)).write_text(r.stdout + r.stderr)
            m = re.search(r'"job_id":(\d+)', r.stdout)
            if r.returncode != 0 or not m:
                raise Failed("the %s job was refused: %s" % (label, (r.stdout + r.stderr)[-800:]))
            jobs[label] = int(m.group(1))
        datasets = {}
        for label, _ in BACKENDS[:3]:
            datasets[label] = demo.caught_up(c, jobs[label], label)
        c.await_marker(jobs["changelog-rocksdb"], lambda cid, m: cid >= 3, "for changelog over RocksDB")
        for label, path in datasets.items():
            doc = json.loads((path / "dataset.json").read_text())
            backends_seen = sorted({p["backend"] for p in doc["participants"]})
            keys = set()
            for row in pq.read_table(path / "entries.parquet").to_pylist():
                if row["scope"] == "keyed" and row["slot"] == "agg":
                    keys.add(row["user_key"].decode())
            demo.outcome(s, "the %s backend's checkpoint exports complete, every account in it" % label,
                         "complete, backend %s, accounts 1-50" % label.split("-")[0],
                         "complete %s, backends %s, %d accounts" % (doc["complete"], backends_seen, len(keys)),
                         doc["complete"] and backends_seen == [label] and
                         keys == {str(a) for a in range(1, 51)})
        for label in ("rocksdb", "changelog"):
            code, report = demo.diff(s, "file-%s" % label, datasets["file"], datasets[label])
            keyed = [x for x in (report or {}).get("slots", []) if x["scope"] == "keyed"]
            demo.outcome(s, "the %s backend holds exactly the file backend's keyed entries" % label,
                         "keyed slots compared, 0 added, removed or changed; never exit 1",
                         "exit %d, %s" % (code, [(x["slot"], x["status"], x["added"] + x["removed"] + x["changed"],
                                                  x["unchanged"]) for x in keyed]),
                         code in (0, 3) and keyed and all(x["status"] == "compared" and
                                                         x["added"] + x["removed"] + x["changed"] == 0 and
                                                         x["unchanged"] == 50 for x in keyed))
        code, j = demo.export(c, jobs["changelog-rocksdb"], c.dir / "datasets" / "changelog-rocksdb")
        demo.outcome(s, "a changelog whose materialisations are in RocksDB is refused as unsupported",
                     "exit 1, unsupported", "exit %d, %s" % (code, j.get("kind")),
                     code == 1 and j.get("kind") == "unsupported")
        code, j = demo.export(c, jobs["changelog-rocksdb"], c.dir / "datasets" / "changelog-rocksdb-partial",
                              extra=("--allow-partial",))
        doc = json.loads((c.dir / "datasets" / "changelog-rocksdb-partial" / "dataset.json").read_text()) \
            if code == 3 else {}
        demo.outcome(s, "--allow-partial publishes it, never complete, saying why",
                     "exit 3, complete false, unsupported listed",
                     "exit %d, complete %s, %d reason(s)" % (code, doc.get("complete"), len(doc.get("unsupported", []))),
                     code == 3 and doc.get("complete") is False and bool(doc.get("unsupported")))
    finally:
        c.stop()


# ---- cost ---------------------------------------------------------------------------------

def cost(demo):
    s = "inspect-cost"
    topic = "si-cost"
    accounts = int(os.environ.get("CLINK_SI_COST_ACCOUNTS", "100000"))
    demo.kafka.create(topic)
    c = Cluster(demo, s, topic)
    c.start()
    try:
        job = c.submit()
        records = [(a, "deposit", a % 1000) for a in range(1, accounts + 1)]
        records += [(a, "withdraw", 1) for a in range(1, accounts + 1, 2)]
        demo.kafka.send(topic, records)
        first = demo.caught_up(c, job, "first", timeout=600)
        demo.kafka.send(topic, [(a, "deposit", 1) for a in range(1, accounts + 1, 10)])
        demo.caught_up(c, job, "second", timeout=600)
        cid_first = json.loads((first / "dataset.json").read_text())["checkpoint"]["id"]
        cid_second = json.loads((c.dir / "datasets" / "second" / "dataset.json").read_text())["checkpoint"]["id"]
        out = c.ctl("stop", "--job-id=%d" % job)
    finally:
        c.stop()
    ck = c.dir / "checkpoints"
    clink = demo.clink
    sizes = {}
    for label, cid in (("first", cid_first), ("second", cid_second)):
        snaps = list((ck / "_state" / str(job)).glob("v*/*/checkpoint-%d.snap" % cid))
        sizes[label] = sum(p.stat().st_size for p in snaps)
    runs = {}
    for label, cid in (("first", cid_first), ("second", cid_second)):
        out = demo.work / s / ("measured-%s" % label)
        code, stdout, _, secs, rss = measure(clink("state-export", "--checkpoint-dir=%s" % ck, "--job=%d" % job,
                                                   "--checkpoint=%d" % cid, "--ha-dir=%s" % (c.dir / "ha"),
                                                   "--out=%s" % out, "--json", "--max-bytes=4294967296"))
        j = json.loads(stdout.strip().splitlines()[-1]) if code == 0 else {}
        runs[label] = {"exit": code, "seconds": round(secs, 3), "peak_rss_bytes": rss,
                       "entries": j.get("entries"), "dataset_bytes": j.get("bytes"),
                       "entries_parquet_bytes": (out / "entries.parquet").stat().st_size if code == 0 else None,
                       "snapshot_bytes": sizes[label]}
    demo.check_state(s, "the measured dataset", demo.work / s / "measured-first", topic)
    code, stdout, _, secs, rss = measure(clink("state-diff", "--a=%s" % (demo.work / s / "measured-first"),
                                               "--b=%s" % (demo.work / s / "measured-second"), "--json"))
    rep = json.loads(stdout) if stdout else {}
    diff_cost = {"exit": code, "seconds": round(secs, 3), "peak_rss_bytes": rss,
                 "entries_read": rep.get("entries_read"), "totals": rep.get("totals")}
    code_q, out_q, _, secs_q, rss_q = measure(clink("state-query", "--dataset=%s" % (demo.work / s / "measured-first"),
                                                    "--sql=SELECT slot, COUNT(*) AS n FROM state GROUP BY slot"))
    by_slot = {}
    for line in out_q.splitlines():
        row = json.loads(line)
        by_slot[row["slot"]] = by_slot.get(row["slot"], []) + [row["n"]]
    demo.outcome(s, "an aggregate over many input batches prints each group's final row once",
                 "count and balance each %d, once" % accounts,
                 {k: v for k, v in by_slot.items() if k in ("count", "balance")},
                 by_slot.get("count") == [accounts] and by_slot.get("balance") == [accounts])
    demo.costs["export"] = runs
    demo.costs["diff"] = diff_cost
    demo.costs["query"] = {"exit": code_q, "seconds": round(secs_q, 3), "peak_rss_bytes": rss_q}
    changed = len(range(1, accounts + 1, 10))
    demo.outcome(s, "the measured runs succeed", "exports 0, diff 1, query 0",
                 "exports %s, diff %d, query %d" % ([r["exit"] for r in runs.values()], code, code_q),
                 all(r["exit"] == 0 for r in runs.values()) and code == 1 and code_q == 0)
    demo.outcome(s, "the measured diff finds exactly the accounts changed", "%d changed per slot" % changed,
                 diff_cost["totals"], (diff_cost["totals"] or {}).get("changed") == 2 * changed + 2)
    for label, r in runs.items():
        demo.note("export %s: %s entries, %.2f s, peak RSS %s MiB, dataset %s MiB, snapshots %s MiB" % (
            label, r["entries"], r["seconds"], r["peak_rss_bytes"] and round(r["peak_rss_bytes"] / 2**20, 1),
            r["dataset_bytes"] and round(r["dataset_bytes"] / 2**20, 2), round(r["snapshot_bytes"] / 2**20, 2)))
    demo.note("diff: %s entries read, %.2f s, peak RSS %s MiB" % (
        diff_cost["entries_read"], diff_cost["seconds"], diff_cost["peak_rss_bytes"] and round(diff_cost["peak_rss_bytes"] / 2**20, 1)))
    demo.note("query: %.2f s, peak RSS %s MiB" % (secs_q, rss_q and round(rss_q / 2**20, 1)))


ALL = [("inspect-running", running), ("inspect-schema", schema), ("inspect-faults", faults),
       ("inspect-retention", retention), ("inspect-backends", backends), ("inspect-cost", cost)]
