"""`record` and `check`: the workflow itself.

Every build process runs under the run's proven isolation and within its
limits; the gate's own reading (hashes, the reference, comparisons) runs in
Python and trusts neither build. The tools build (`--tools-clink`) exports,
verifies and lists checkpoint datasets; it is named explicitly like the others
and identified in the report.
"""

import json
import os
import shutil
import time

from . import EXIT_FAIL, EXIT_NOT_ESTABLISHED, EXIT_PASS
from . import compare, definition, evidence, reference, workload
from .builds import Build
from .cluster import Cluster, Limits, RunStopped
from .report import FAILED, PASSED, UNAVAILABLE, UNSUPPORTED, Report


class Usage(Exception):
    pass


def _fresh(path, what):
    path = os.path.abspath(path)
    if os.path.lexists(path):
        raise Usage("%s %s already exists; it must be a fresh directory" % (what, path))
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        raise Usage("%s's parent %s is not a directory" % (what, parent))
    return path


class Gate:
    def __init__(self, isolation, limits, interrupted, timeout_s):
        self.isolation = isolation
        self.limits = limits
        self.interrupted = interrupted
        self.deadline = time.monotonic() + timeout_s
        self.timeout_s = timeout_s
        self.commands = []

    # ---- the tools build ------------------------------------------------------------

    def tools(self, tools, *args, timeout_s=120):
        """Runs a build's clink outside any cluster: the tools build's dataset
        commands, or a build's own check-savepoint."""
        import subprocess
        if self.interrupted():
            raise RunStopped("interrupted", self.interrupted())
        argv = self.isolation.wrap([tools.clink] + list(args))
        left = max(1.0, min(timeout_s, self.deadline - time.monotonic()))
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=left,
                               stdin=subprocess.DEVNULL,
                               env={"PATH": os.environ.get("PATH", ""), "CLINK_LOG_LEVEL": "off"})
            code, out, err = r.returncode, r.stdout, r.stderr
        except subprocess.TimeoutExpired:
            code, out, err = None, "", "timed out after %g s" % left
        self.commands.append({"build": tools.name, "argv": argv, "exit": code})
        return code, out, err

    def export(self, tools, checkpoints, job, checkpoint, out):
        code, o, e = self.tools(tools, "state-export", "--checkpoint-dir=%s" % checkpoints,
                                "--job=%d" % job, "--checkpoint=%d" % checkpoint, "--out=%s" % out)
        digest = None
        for line in (o + e).split("\n"):
            if line.strip().startswith("digest "):
                digest = line.split()[-1]
        return code == 0 and digest is not None, digest, (o + e).strip()

    def catalog(self, tools, dataset, digest):
        code, o, e = self.tools(tools, "state-cat", "--dataset=%s" % dataset, "--digest=%s" % digest,
                                "--json", "--max-rows=0")
        if code != 0:
            raise RuntimeError("state-cat exited %s: %s" % (code, (e or o).strip()[-600:]))
        return json.loads(o)

    def cluster(self, build, root):
        c = Cluster(build, self.isolation.wrap, root, self.limits, self.interrupted,
                    commands=self.commands)
        c.deadline = min(c.deadline, self.deadline)
        return c


def _watermark_reached(cluster, job, wm):
    def ready():
        w = cluster.keyed_watermarks(job)
        return w is not None and all(x is not None and x >= wm for x in w)
    return ready


def _ops(reported):
    """The operators a running job reports, with the job-global subtask
    indices each runs."""
    return [dict({k: o.get(k) for k in ("uid", "op_id", "kind", "node", "parallelism", "keyed")},
                 subtasks=sorted(s.get("subtask_idx") for s in o.get("subtasks") or []))
            for o in reported]


def _snapshots(root):
    out = []
    for d, _, files in os.walk(root):
        out += [os.path.join(d, f) for f in files if f.endswith(".snap")]
    return sorted(out)


def _requirements(defn, build):
    """Unmet capability requirements, as reasons."""
    out = []
    if build.manifest is None:
        return ["%s's capability manifest is unavailable (%s)" % (
            build.name, build.identity["manifest_unavailable_because"])]
    for path, want in sorted(defn.requires.items()):
        got = definition.capability(build.manifest, path)
        if got is definition.MISSING:
            out.append("%s does not report %s" % (build.name, path))
        elif got != want:
            out.append("%s reports %s = %r, the test requires %r" % (build.name, path, got, want))
    return out


# ---- record ---------------------------------------------------------------------------

def record(args, isolation, interrupted):
    defn = definition.load(args.test)
    out = _fresh(args.out, "--out")
    work = _fresh(args.work or out + ".work", "--work")
    baseline = Build("baseline", args.baseline_clink, args.baseline_node, args.baseline_job)
    tools = Build("tools", args.tools_clink, tools=True)
    limits = Limits(args.run_timeout_s, args.max_rss_mib, args.max_output_mib)
    gate = Gate(isolation, limits, interrupted, args.timeout_s)
    os.makedirs(work)
    report = Report("record")
    report.facts["limits"] = dict(limits.document(), timeout_s=args.timeout_s)
    report.facts["artifacts"] = {"root": work}
    try:
        return _record(defn, out, work, baseline, tools, gate, report)
    finally:
        report.facts["commands"] = gate.commands
        report.write(os.path.join(work, "record.json"), os.path.join(work, "record.txt"))


def _record(defn, out, work, baseline, tools, gate, report):
    state = isolation_state(gate.isolation)
    report.facts["isolation"] = state
    if gate.isolation.state != "enforced":
        print("record: isolation is %s: %s; nothing ran" % (gate.isolation.state, gate.isolation.detail))
        return EXIT_NOT_ESTABLISHED
    ids = {b.name: b.measure(gate.isolation.wrap) for b in (baseline, tools)}
    report.facts["builds"] = ids
    unmet = _requirements(defn, baseline)
    if unmet:
        print("record: the baseline does not meet the test's requirements: %s" % "; ".join(unmet))
        return EXIT_NOT_ESTABLISHED
    staged = os.path.join(work, "evidence")
    os.makedirs(staged)
    with open(os.path.join(staged, "test.json"), "wb") as f:
        f.write(defn.text)
    names = workload.write(defn.input, os.path.join(staged, "input"))
    ref = reference.compute(defn)
    reference.save(ref, os.path.join(staged, "reference.json"))
    cut = defn.input["savepoint_after_segment"]
    wm = ref["last_watermark_before_savepoint"]
    c = gate.cluster(baseline, os.path.join(work, "baseline"))
    started = time.monotonic()
    try:
        workload.publish(os.path.join(staged, "input"), c.feed, names[:cut])
        c.start()
        ok, job, text = c.submit()
        if not ok:
            print("record: the baseline refused the job: %s" % text[-800:])
            return EXIT_NOT_ESTABLISHED
        c.wait(_watermark_reached(c, job, wm), "the operators to reach the savepoint's watermark")
        operators = _ops(c.operators(job))
        sp, text = c.savepoint(job)
        if sp is None:
            print("record: the savepoint failed: %s" % text[-800:])
            return EXIT_NOT_ESTABLISHED
        ok, digest, text = gate.export(tools, c.checkpoints, job, sp, os.path.join(staged, "checkpoint"))
        if not ok:
            print("record: the savepoint did not export as a complete dataset: %s" % text[-1200:])
            return EXIT_NOT_ESTABLISHED
        c.cancel(job)
    except RunStopped as e:
        print("record: the baseline run stopped (%s): %s" % (e.kind, e.detail))
        return EXIT_NOT_ESTABLISHED
    finally:
        c.stop()
    os.makedirs(os.path.join(staged, "output"))
    for name in ("windows.txt", "idle.txt"):
        src = os.path.join(c.out, name)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(staged, "output", name))
        else:
            open(os.path.join(staged, "output", name), "w").close()
    rows, bad = compare.read_rows(os.path.join(staged, "output"), "baseline")
    before = compare.multiset_diff(ref["rows_before_savepoint"], rows)
    saved = compare.decode_state(gate.catalog(tools, os.path.join(staged, "checkpoint"), digest),
                                 compare.owners_of(operators))
    at_cut = compare.state_diff(ref["state_at_savepoint"], saved)
    checks = {
        "output_before_savepoint": {"verdict": PASSED if before["equal"] and not bad else FAILED,
                                    "detail": compare.describe_rows(before), "diff": before,
                                    "malformed": bad[:10]},
        "state_at_savepoint": {"verdict": PASSED if at_cut["equal"] else FAILED,
                               "detail": compare.describe_state_diff(at_cut), "diff": at_cut},
    }
    for k, v in checks.items():
        report.set("record." + k, v["verdict"], v["detail"])
    doc = {
        "test": {"name": defn.name, "sha256": defn.sha256},
        "baseline": ids["baseline"],
        "tools": ids["tools"],
        "isolation": state,
        "checkpoint": {"job": job, "id": sp, "dataset_digest": digest,
                       "last_watermark": wm, "operators": operators},
        "input": {"segments": names, "savepoint_after_segment": cut},
        "recorded_checks": checks,
        "record_seconds": round(time.monotonic() - started, 2),
    }
    digest_set = evidence.write_index(staged, doc)
    os.rename(staged, out)
    print("record: evidence %s" % out)
    print("  digest %s" % digest_set)
    for k, v in checks.items():
        print("  %s: %s (%s)" % (k, v["verdict"], v["detail"]))
    return EXIT_PASS if all(v["verdict"] == PASSED for v in checks.values()) else EXIT_FAIL


def isolation_state(iso):
    return {"state": iso.state, "mechanism": iso.mechanism, "detail": iso.detail}


# ---- check ----------------------------------------------------------------------------

def check(args, isolation, interrupted):
    defn_error = None
    try:
        defn = definition.load(args.test)
    except definition.Unsupported as e:
        defn, defn_error = None, str(e)
    out = _fresh(args.out, "--out")
    baseline = Build("baseline", args.baseline_clink, args.baseline_node, args.baseline_job)
    candidate = Build("candidate", args.candidate_clink, args.candidate_node, args.candidate_job)
    tools = Build("tools", args.tools_clink, tools=True)
    limits = Limits(args.run_timeout_s, args.max_rss_mib, args.max_output_mib)
    gate = Gate(isolation, limits, interrupted, args.timeout_s)
    os.makedirs(out)
    report = Report("check")
    report.facts["limits"] = dict(limits.document(), timeout_s=args.timeout_s)
    report.facts["artifacts"] = {"root": out, "runs": os.path.join(out, "runs"),
                                 "commands": os.path.join(out, "commands.json")}
    report.facts["evidence"] = {"path": os.path.abspath(args.evidence), "digest": args.evidence_digest}
    try:
        _check(defn, defn_error, args, baseline, candidate, tools, gate, report, out)
    except RunStopped as e:
        report.fill_unavailable("not run: %s (%s)" % (e.kind, e.detail))
    finally:
        why = interrupted()
        report.fill_unavailable("not run: %s" % ("interrupted (%s)" % why if why else
                                                  "an earlier step could not complete"))
        with open(os.path.join(out, "commands.json"), "w") as f:
            json.dump(gate.commands, f, indent=1)
        doc, text = report.write(os.path.join(out, "report.json"), os.path.join(out, "report.txt"))
        print(text, end="")
    return doc["exit"]


def _check(defn, defn_error, args, baseline, candidate, tools, gate, report, out):
    ev_root = os.path.abspath(args.evidence)
    try:
        before = evidence.fingerprint(ev_root) if os.path.isdir(ev_root) else None
    except evidence.EvidenceInvalid:
        before = None

    # Evidence integrity, before anything runs.
    doc = None
    if defn is None:
        report.set("evidence.integrity", UNSUPPORTED, "the test definition is unsupported: %s" % defn_error)
        return
    try:
        doc = evidence.verify(ev_root, args.evidence_digest)
        with open(os.path.join(ev_root, "test.json"), "rb") as f:
            recorded_test = f.read()
        if doc["test"]["sha256"] != defn.sha256 or recorded_test != defn.text:
            report.set("evidence.integrity", FAILED,
                       "the evidence was recorded for test definition %s, not the one given (%s)"
                       % (doc["test"]["sha256"][:16], defn.sha256[:16]))
            return
        report.set("evidence.integrity", PASSED, "digest, %d files and the dataset digest verified"
                   % len(doc.get("files", {})))
    except evidence.EvidenceUnsupported as e:
        report.set("evidence.integrity", UNSUPPORTED, str(e))
        return
    except (evidence.EvidenceInvalid, OSError, KeyError, TypeError) as e:
        report.set("evidence.integrity", FAILED, str(e))
        return

    try:
        _check_verified(defn, doc, ev_root, args, baseline, candidate, tools, gate, report, out)
    finally:
        try:
            after = evidence.fingerprint(ev_root)
            changed = sorted(k for k in set(before or {}) | set(after)
                             if (before or {}).get(k) != after.get(k))
        except evidence.EvidenceInvalid as e:
            changed = [str(e)]
        if before is None:
            report.set("evidence.unchanged", UNAVAILABLE, "the evidence could not be hashed before the runs")
        elif changed:
            report.set("evidence.unchanged", FAILED, "%d file(s) changed during the check, first %s"
                       % (len(changed), changed[0]))
        else:
            report.set("evidence.unchanged", PASSED, "every file hashes as it did before the runs")


def _check_verified(defn, doc, ev_root, args, baseline, candidate, tools, gate, report, out):
    iso = gate.isolation
    report.facts["isolation"] = isolation_state(iso)
    if iso.state != "enforced":
        report.set("isolation", UNAVAILABLE if iso.state != "not-enforced" else FAILED,
                   "%s: %s" % (iso.state, iso.detail))
        return
    report.set("isolation", PASSED, iso.detail)

    ids = {b.name: b.measure(iso.wrap) for b in (baseline, candidate, tools)}
    report.facts["builds"] = ids
    differs = baseline.same_files_as(doc.get("baseline") or {})
    if differs:
        report.set("builds.identity", FAILED, "the baseline given is not the build that recorded the "
                   "evidence: " + "; ".join(differs), recorded=doc.get("baseline"))
    else:
        unknown = [b.name for b in (baseline, candidate) if not b.identity["self_reported"]["git_sha"]]
        report.set("builds.identity", PASSED, "hashed; the baseline is the build that recorded the "
                   "evidence%s" % ("; provenance unavailable for %s" % ", ".join(unknown) if unknown else ""))
    unmet = _requirements(defn, baseline) + _requirements(defn, candidate)
    if unmet:
        report.set("builds.capabilities", UNSUPPORTED, "; ".join(unmet))
    else:
        report.set("builds.capabilities", PASSED, "both report %s" % (
            ", ".join("%s = %r" % kv for kv in sorted(defn.requires.items())) or "what the test needs"))

    work = os.path.join(out, "runs")
    os.makedirs(work)
    with open(os.path.join(ev_root, "reference.json")) as f:
        ref = json.load(f)
    ckpt = doc["checkpoint"]

    # Readability, by the tools build, from a scratch copy.
    ev_ds = os.path.join(work, "evidence-dataset")
    shutil.copytree(os.path.join(ev_root, "checkpoint"), ev_ds, symlinks=True)
    try:
        ev_state = compare.decode_state(gate.catalog(tools, ev_ds, ckpt["dataset_digest"]),
                                        compare.owners_of(ckpt.get("operators") or []))
        if not ev_state["complete"]:
            report.set("evidence.readable", FAILED, "the evidence's dataset is partial")
            return
        if ev_state["undecoded"]:
            # Readable, but not all of it in the job's encodings: the runs go
            # on, so whatever else differs is still seen.
            report.set("evidence.readable", FAILED, "%d row(s) of the evidence are in none of the job's "
                       "documented encodings, first: %s" % (len(ev_state["undecoded"]),
                                                            ev_state["undecoded"][0]))
        else:
            report.set("evidence.readable", PASSED, "verified and decoded: %d open window(s), %d timer(s), "
                       "%d account(s) seen" % (len(ev_state["windows"]), len(ev_state["timers"]),
                                                len(ev_state["last_seen"])))
    except (RuntimeError, ValueError) as e:
        report.set("evidence.readable", FAILED, str(e))
        return

    # The candidate's own schema check, snapshot by snapshot.
    snaps = _snapshots(os.path.join(ev_ds, "checkpoint"))
    verdicts = []
    for s in snaps:
        code, o, e = gate.tools(candidate, "check-savepoint", "--file=%s" % s,
                                "--expected=%s" % candidate.job, "--quiet")
        verdicts.append((os.path.relpath(s, ev_ds), code, (o + e).strip()[-600:]))
    bad = [v for v in verdicts if v[1] != 0]
    if not snaps:
        report.set("schema.candidate", UNAVAILABLE, "the evidence holds no snapshot")
    elif not bad:
        report.set("schema.candidate", PASSED, "check-savepoint accepts all %d snapshot(s)" % len(snaps))
    elif all(v[1] == 2 for v in bad):
        report.set("schema.candidate", UNAVAILABLE, "the candidate's check-savepoint could not load its "
                   "job: %s" % bad[0][2], results=verdicts)
    else:
        first = next(v for v in bad if v[1] != 2)
        report.set("schema.candidate", FAILED, "%d of %d snapshot(s) refused (exit %s): %s" % (
            len([v for v in bad if v[1] != 2]), len(snaps), first[1], first[2]), results=verdicts)

    runs = {}
    for b in (baseline, candidate):
        runs[b.name] = _continuation(b, doc, ev_root, ev_ds, ev_state, ref, tools, gate, report, work)

    _compare(runs, ref, ev_root, report)


def _continuation(build, doc, ev_root, ev_ds, ev_state, ref, tools, gate, report, work):
    """Restores `build` from its own copy of the evidence, takes a savepoint of
    the restored job before it reads anything, then delivers the continuation."""
    name = build.name
    run = {"rows": None, "final": None}
    ckpt = doc["checkpoint"]
    root = os.path.join(work, name)
    restore_root = os.path.join(root, "restore")
    os.makedirs(root)
    shutil.copytree(os.path.join(ev_ds, "checkpoint"), restore_root, symlinks=True)
    restore = (os.path.join(restore_root, "_jobs", str(ckpt["job"])), ckpt["id"])
    c = gate.cluster(build, os.path.join(root, "cluster"))
    names = doc["input"]["segments"]
    cut = doc["input"]["savepoint_after_segment"]
    stage = "restore"
    try:
        c.start()
        ok, job, text = c.submit(restore=restore)
        if not ok:
            why = text.split("reject=", 1)[1].strip() if "reject=" in text else text[-1200:]
            report.set("restore." + name, FAILED, "the %s refused to restore checkpoint %d before "
                       "running anything: %s" % (name, ckpt["id"], why), submission=text[-4000:])
            report.set("recovery." + name, UNAVAILABLE, "the restore was refused")
            return run
        c.wait(lambda: c.operators(job), "the restored job's operators")
        ops = _ops(c.operators(job))
        sp, text = c.savepoint(job)
        if sp is None:
            report.set("restore." + name, FAILED, "the restored job took no savepoint: %s" % text[-800:])
            report.set("recovery." + name, UNAVAILABLE, "the restore could not be confirmed")
            return run
        restored_ds = os.path.join(root, "restored-dataset")
        ok, digest, text = gate.export(tools, c.checkpoints, job, sp, restored_ds)
        if not ok:
            report.set("restore." + name, FAILED, "the restored job's savepoint %d did not export as a "
                       "complete dataset: %s" % (sp, text[-800:]))
            report.set("recovery." + name, UNAVAILABLE, "the restore could not be confirmed")
            return run
        owners = compare.owners_of(ops)
        restored = compare.decode_state(gate.catalog(tools, restored_ds, digest), owners)
        fidelity = compare.state_diff(ev_state, restored)
        identity = _identity(ckpt.get("operators") or [], ops, ev_state, restored)
        verdict = PASSED if fidelity["equal"] and not identity else FAILED
        why = ("restored savepoint %d holds the evidence's state, logically, under the same operators"
               % sp) if verdict == PASSED else "; ".join(identity + (
                   [] if fidelity["equal"] else ["after the restore: " + compare.describe_state_diff(fidelity)]))
        report.set("restore." + name, verdict, why, savepoint=sp, fidelity=fidelity, operators=ops)
        if name == "candidate":
            code, o, e = gate.tools(tools, "state-diff", "--a=%s" % ev_ds, "--b=%s" % restored_ds, "--json")
            raw = _json_or_text(o or e)
            first = (raw.get("reasons") or [""])[0] if isinstance(raw, dict) else ""
            report.set("state.raw", PASSED if code == 0 else (FAILED if code == 1 else UNAVAILABLE),
                       "raw bytes against the evidence's: state-diff exit %s%s" % (
                           code, ", " + first if first else ""), report=raw)
        stage = "recovery"
        workload.publish(os.path.join(ev_root, "input"), c.feed, names[cut:])
        done = c.wait(lambda: c.finished(job), "the job to finish")
        if not done[0]:
            report.set("recovery." + name, FAILED, "the job failed after the restore: %s" % done[1])
            return run
        final_id = max(c.completed(job) or [0])
        if final_id <= sp:
            report.set("recovery." + name, FAILED, "the job finished without a checkpoint after the "
                       "restored savepoint %d" % sp)
            return run
        final_ds = os.path.join(root, "final-dataset")
        ok, digest, text = gate.export(tools, c.checkpoints, job, final_id, final_ds)
        if not ok:
            report.set("recovery." + name, FAILED, "the final checkpoint %d did not export: %s"
                       % (final_id, text[-800:]))
            return run
        run["final"] = compare.decode_state(gate.catalog(tools, final_ds, digest), owners)
        run["final_copies"] = run["final"]["copies"]
        rows, bad = compare.read_rows(c.out, name)
        run["rows"], run["malformed"] = rows, bad
        report.set("recovery." + name, PASSED, "the continuation (%d segments) completed; final "
                   "checkpoint %d; %d rows written" % (len(names) - cut, final_id, len(rows)),
                   peak_rss_mib=round(c.peak_rss_mib, 1))
        return run
    except RunStopped as e:
        verdict = FAILED if e.kind == "crash" else UNAVAILABLE
        report.set(stage + "." + name, verdict, "the %s run stopped during the %s (%s): %s"
                   % (name, stage, e.kind, e.detail), stopped=e.kind)
        if stage == "restore":
            report.set("recovery." + name, UNAVAILABLE, "the restore did not complete")
        if e.kind == "interrupted":
            raise
        return run
    finally:
        c.stop()


def _identity(recorded_ops, ops, ev_state, restored):
    """Every operator that holds state in the evidence must be in the restored
    job under the same id and uid, and hold its slots there."""
    out = []
    by_id = {str(o.get("op_id")): o for o in ops if o.get("op_id")}
    rec_by_id = {str(o.get("op_id")): o for o in recorded_ops if o.get("op_id")}
    holders = sorted({op for ids in ev_state["slots"].values() for op in ids})
    for op in holders:
        uid = (rec_by_id.get(op) or {}).get("uid") or "?"
        if op not in by_id:
            now = [o.get("uid") or "(no uid)" for o in ops if o.get("node") == (rec_by_id.get(op) or {}).get("node")]
            out.append("operator %s (uid %s) holds state in the evidence and is not in the restored "
                       "job%s: the restore was accepted, and no operator reads that state" % (
                           op, uid, (" (its node %s has uid %s now)" % ((rec_by_id.get(op) or {}).get("node"), now[0]))
                           if now else ""))
        elif (by_id[op].get("uid") or "") != (rec_by_id.get(op) or {}).get("uid", by_id[op].get("uid") or ""):
            out.append("operator %s has uid %r, the evidence's %r" % (op, by_id[op].get("uid"), uid))
    for slot, ids in ev_state["slots"].items():
        if restored["slots"].get(slot) and sorted(restored["slots"][slot]) != sorted(ids):
            out.append("slot %s is held by operator(s) %s after the restore, %s in the evidence"
                       % (slot, ",".join(restored["slots"][slot]), ",".join(ids)))
    return out


def _side(baseline_ref, candidate_ref):
    if baseline_ref == FAILED and candidate_ref == PASSED:
        return "; the reference agrees with the candidate, not the baseline"
    if baseline_ref == PASSED and candidate_ref == FAILED:
        return "; the reference agrees with the baseline, not the candidate"
    if baseline_ref == FAILED and candidate_ref == FAILED:
        return "; the reference agrees with neither"
    return ""


def _json_or_text(s):
    try:
        return json.loads(s)
    except ValueError:
        return s[-2000:]


def _compare(runs, ref, ev_root, report):
    b, c = runs["baseline"], runs["candidate"]
    prefix, bad = compare.read_rows(os.path.join(ev_root, "output"), "evidence")
    for name, r in (("candidate", c), ("baseline", b)):
        cid = "reference." + name
        if r["rows"] is None:
            report.set(cid, UNAVAILABLE, "the %s's continuation did not complete" % name)
            continue
        rows = compare.multiset_diff(ref["rows"], prefix + r["rows"])
        state = compare.state_diff(ref["state_at_end"], r["final"])
        ok = rows["equal"] and state["equal"] and not r.get("malformed") and not bad
        report.set(cid, PASSED if ok else FAILED,
                   "output: %s; final state: %s%s" % (
                       compare.describe_rows(rows), compare.describe_state_diff(state),
                       "; %d malformed line(s)" % len(r.get("malformed") or []) if r.get("malformed") else ""),
                   rows=rows, state=state, malformed=(r.get("malformed") or [])[:10])
    if b["rows"] is None or c["rows"] is None:
        why = "the %s's continuation did not complete" % ("baseline" if b["rows"] is None else "candidate")
        report.set("behaviour.output", UNAVAILABLE, why)
        report.set("behaviour.state", UNAVAILABLE, why)
    else:
        side = _side(report.verdict("reference.baseline"), report.verdict("reference.candidate"))
        rows = compare.multiset_diff(b["rows"], c["rows"])
        report.set("behaviour.output", PASSED if rows["equal"] else FAILED,
                   "candidate against baseline: " + compare.describe_rows(rows) +
                   ("" if rows["equal"] else side), rows=rows)
        state = compare.state_diff(b["final"], c["final"])
        copies = (b.get("final_copies") or 0, c.get("final_copies") or 0)
        report.set("behaviour.state", PASSED if state["equal"] else FAILED,
                   "candidate against baseline, logically: " + compare.describe_state_diff(state) +
                   ("" if state["equal"] else side) +
                   ("; operator-state copies held by subtasks that do not run their operator, left "
                    "out: baseline %d, candidate %d" % copies if any(copies) else ""),
                   state=state, copies={"baseline": copies[0], "candidate": copies[1]})
    report.set("replay.determinism", UNSUPPORTED,
               "the supported job is a compiled job and the evidence carries no captures; replay is "
               "not part of this contract")
