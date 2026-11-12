#!/usr/bin/env python3
"""Qualification of the upgrade validation workflow (examples/upgrade-check).

Each scenario records evidence with a baseline, checks a candidate against it
with `clink-audit upgrade`, and compares the report with what the scenario
expects. Everything is kept under WORK: each scenario's evidence, its digest
(outside the evidence), the report, the runs and their logs, and
results/scenarios.json with every expectation and what was observed.

Builds, all named explicitly:
    head    CLINK_BIN_DIR's clink, clink_node and examples/upgrade_check_job.so
    variant head's binaries with examples/upgrade_check_job_<variant>.so
    prior   CLINK_UC_PRIOR_DIR's prefix/bin/clink, clink_node and
            job/upgrade_check_job.so (the pinned prior release, run.sh builds it)
Fixtures (wrappers around head's clink, written under WORK/fixtures):
    clink-without-capability  reports no build.checkpoint_meta_version
    clink-forgets-restore     drops --restore-from-dir and its id from `run`

QUICK=1 runs the bounded subset CI runs; ONLY=<name>[,<name>] runs those.
"""

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
AUDIT = os.path.join(REPO, "python", "clink-audit")
TEST = os.path.join(HERE, "test.json")
BIN = os.path.abspath(os.environ.get("CLINK_BIN_DIR", os.path.join(REPO, "build")))
PRIOR = os.environ.get("CLINK_UC_PRIOR_DIR", "")
WORK = os.path.abspath(os.environ["WORK"])
RESULTS = os.path.join(WORK, "results")
QUICK = os.environ.get("QUICK") == "1"
ONLY = {s for s in os.environ.get("ONLY", "").split(",") if s}

QUICK_SET = {"control", "wrong-aggregate", "lost-timers", "schema", "uid-changed", "wrong-digest",
             "timeout", "agreeing-and-wrong"}


def head(job="upgrade_check_job"):
    return {"clink": os.path.join(BIN, "clink"), "node": os.path.join(BIN, "clink_node"),
            "job": os.path.join(BIN, "examples", job + ".so")}


def variant(v):
    return head("upgrade_check_job_" + v)


def prior():
    return {"clink": os.path.join(PRIOR, "prefix", "bin", "clink"),
            "node": os.path.join(PRIOR, "prefix", "bin", "clink_node"),
            "job": os.path.join(PRIOR, "job", "upgrade_check_job.so")}


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


class Qualification:
    def __init__(self):
        self.outcomes = []
        self.costs = {}
        self.evidence = {}
        os.makedirs(os.path.join(RESULTS, "digests"), exist_ok=True)
        os.makedirs(os.path.join(WORK, "fixtures"), exist_ok=True)

    # ---- running the workflow ------------------------------------------------------

    def audit(self, argv, name, interrupt_after=None):
        env = dict(os.environ, PYTHONPATH=AUDIT, PYTHONDONTWRITEBYTECODE="1")
        log = os.path.join(RESULTS, "logs", name + ".log")
        os.makedirs(os.path.dirname(log), exist_ok=True)
        cmd = [sys.executable, "-m", "clink_audit", "upgrade"] + argv
        with open(os.path.join(RESULTS, "commands.txt"), "a") as f:
            f.write("%s: %s\n" % (name, " ".join(cmd)))
        t0 = time.monotonic()
        with open(log, "wb") as out:
            p = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, env=env)
            if interrupt_after is not None:
                time.sleep(interrupt_after)
                p.send_signal(signal.SIGINT)
            code = p.wait(timeout=1800)
        return code, time.monotonic() - t0

    def builds_args(self, role, b):
        return ["--%s-clink=%s" % (role, b["clink"]), "--%s-node=%s" % (role, b["node"]),
                "--%s-job=%s" % (role, b["job"])]

    def record(self, name, baseline):
        if name in self.evidence:
            return self.evidence[name]
        out = os.path.join(WORK, "evidence", name)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        code, secs = self.audit(["record", "--test=%s" % TEST, "--out=%s" % out,
                                 "--tools-clink=%s" % head()["clink"]] +
                                self.builds_args("baseline", baseline), "record-" + name)
        if not os.path.isfile(os.path.join(out, "evidence.json")):
            raise RuntimeError("record %s published no evidence (exit %d); see results/logs" % (name, code))
        digest = sha(os.path.join(out, "evidence.json"))
        with open(os.path.join(RESULTS, "digests", name + ".sha256"), "w") as f:
            f.write(digest + "\n")
        self.costs["record " + name] = {"seconds": round(secs, 1), "exit": code,
                                        "evidence_bytes": sum(os.path.getsize(os.path.join(d, f))
                                                              for d, _, fs in os.walk(out) for f in fs)}
        self.evidence[name] = (out, digest, code)
        return self.evidence[name]

    def check(self, name, ev, baseline, candidate, extra=(), test=TEST, digest=None,
              interrupt_after=None):
        out = os.path.join(WORK, "checks", name)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        argv = ["check", "--test=%s" % test, "--evidence=%s" % ev[0],
                "--evidence-digest=%s" % (digest or ev[1]), "--out=%s" % out,
                "--tools-clink=%s" % head()["clink"]]
        argv += self.builds_args("baseline", baseline) + self.builds_args("candidate", candidate)
        code, secs = self.audit(argv + list(extra), "check-" + name, interrupt_after)
        report = None
        if os.path.isfile(os.path.join(out, "report.json")):
            with open(os.path.join(out, "report.json")) as f:
                report = json.load(f)
        self.costs["check " + name] = {"seconds": round(secs, 1), "exit": code}
        return code, report, out

    # ---- outcomes -----------------------------------------------------------------

    def expect(self, scenario, what, expected, observed, ok):
        self.outcomes.append({"scenario": scenario, "check": what, "expected": expected,
                              "observed": observed, "ok": bool(ok)})
        print("  %s %s: %s" % ("ok  " if ok else "FAIL", what, observed))

    def verdicts(self, report):
        return {c["id"]: c["verdict"] for c in (report or {}).get("checks", [])}

    def reason(self, report, cid):
        for c in (report or {}).get("checks", []):
            if c["id"] == cid:
                return c["reason"]
        return ""

    def outcome(self, s, code, report, exit_want, verdicts_want, reasons=()):
        v = self.verdicts(report)
        self.expect(s, "exit", exit_want, code, code == exit_want)
        for cid, want in verdicts_want.items():
            self.expect(s, cid, want, "%s: %s" % (v.get(cid), self.reason(report, cid)[:240]),
                        v.get(cid) == want)
        for cid, needle in reasons:
            r = self.reason(report, cid)
            self.expect(s, "%s names %r" % (cid, needle), needle, r[:240], needle in r)
        if report and v.get("evidence.integrity") == "passed":
            self.expect(s, "evidence.unchanged", "passed", v.get("evidence.unchanged"),
                        v.get("evidence.unchanged") == "passed")
        if report and v.get("isolation") is not None and v.get("evidence.integrity") == "passed":
            self.expect(s, "isolation proven", "passed", v.get("isolation"), v.get("isolation") == "passed")

    def write(self):
        with open(os.path.join(RESULTS, "scenarios.json"), "w") as f:
            json.dump(self.outcomes, f, indent=1)
        with open(os.path.join(RESULTS, "costs.json"), "w") as f:
            json.dump(self.costs, f, indent=1, sort_keys=True)
        failed = [o for o in self.outcomes if not o["ok"]]
        lines = ["upgrade-check qualification: %s, %d of %d expectations met" % (
            "QUALIFIED" if not failed else "NOT QUALIFIED", len(self.outcomes) - len(failed),
            len(self.outcomes))]
        lines += ["  FAIL %s / %s: expected %s, observed %s" % (o["scenario"], o["check"], o["expected"],
                                                              o["observed"]) for o in failed]
        with open(os.path.join(RESULTS, "summary.txt"), "w") as f:
            f.write("\n".join(lines) + "\n")
        print("\n".join(lines))
        return 0 if not failed and self.outcomes else 1


# ---- fixtures ------------------------------------------------------------------------

def fixture_without_capability():
    p = os.path.join(WORK, "fixtures", "clink-without-capability")
    with open(p, "w") as f:
        f.write("#!%s\n" % sys.executable)
        f.write("import json, os, subprocess, sys\n"
                "real = %r\n"
                "if sys.argv[1:] == ['--capabilities-json']:\n"
                "    m = json.loads(subprocess.run([real] + sys.argv[1:], capture_output=True, text=True).stdout)\n"
                "    m['build'].pop('checkpoint_meta_version', None)\n"
                "    print(json.dumps(m))\n"
                "    sys.exit(0)\n"
                "os.execv(real, [real] + sys.argv[1:])\n" % head()["clink"])
    os.chmod(p, 0o755)
    return dict(head(), clink=p)


def fixture_forgets_restore():
    p = os.path.join(WORK, "fixtures", "clink-forgets-restore")
    with open(p, "w") as f:
        f.write("#!%s\n" % sys.executable)
        f.write("import os, sys\n"
                "real = %r\n"
                "args = [a for a in sys.argv[1:] if not a.startswith('--restore-from-')]\n"
                "os.execv(real, [real] + args)\n" % head()["clink"])
    os.chmod(p, 0o755)
    return dict(head(), clink=p)


def copy_evidence(ev, name):
    dst = os.path.join(WORK, "evidence-copies", name)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copytree(ev[0], dst, symlinks=True)
    return dst


def leftover_nodes(path):
    r = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True)
    return [line.strip() for line in r.stdout.splitlines() if "clink_node" in line and path in line]


# ---- scenarios ----------------------------------------------------------------------

def scenarios(q):
    H = head()

    def run(name):
        return (not ONLY or name in ONLY) and (not QUICK or name in QUICK_SET)

    ev = None

    def evidence_head():
        return q.record("head", H)

    if run("control"):
        print("control: the same build as baseline and candidate")
        ev = evidence_head()
        q.expect("control", "record exit", 0, ev[2], ev[2] == 0)
        code, r, _ = q.check("control", ev, H, H)
        q.outcome("control", code, r, 0, {"restore.candidate": "passed", "recovery.candidate": "passed",
                                          "behaviour.output": "passed", "behaviour.state": "passed",
                                          "reference.candidate": "passed"})

    if run("compatible"):
        print("compatible: the same semantics written differently")
        code, r, _ = q.check("compatible", evidence_head(), H, variant("compatible"))
        q.outcome("compatible", code, r, 0, {"reference.candidate": "passed"})

    if run("prior-release") and PRIOR:
        print("prior-release: the pinned prior release as the baseline, this build as the candidate")
        P = prior()
        evp = q.record("prior", P)
        q.expect("prior-release", "record exit", 0, evp[2], evp[2] == 0)
        code, r, _ = q.check("prior-release", evp, P, H)
        q.outcome("prior-release", code, r, 0, {"builds.identity": "passed", "restore.candidate": "passed",
                                                "reference.candidate": "passed",
                                                "behaviour.state": "passed"})
        ids = (r or {}).get("builds", {})
        q.expect("prior-release", "the version pair", "baseline and candidate commits named",
                 "%s -> %s" % (ids.get("baseline", {}).get("self_reported", {}).get("git_sha"),
                               ids.get("candidate", {}).get("self_reported", {}).get("git_sha")),
                 ids.get("baseline", {}).get("self_reported", {}).get("git_sha") and
                 ids.get("candidate", {}).get("self_reported", {}).get("git_sha"))
    elif run("prior-release"):
        q.expect("prior-release", "prior release built", "CLINK_UC_PRIOR_DIR", "not set", False)

    defects = [
        ("wrong-aggregate", "wrong_aggregate", {"restore.candidate": "passed", "behaviour.output": "failed",
                                               "reference.candidate": "failed", "reference.baseline": "passed"}, ()),
        ("window-boundary", "window_boundary", {"behaviour.output": "failed", "reference.candidate": "failed"}, ()),
        ("lost-timers", "lost_timers", {"restore.candidate": "failed", "reference.candidate": "failed"},
         (("restore.candidate", "timers"),)),
        ("schema", "schema", {"schema.candidate": "failed", "restore.candidate": "failed",
                              "recovery.candidate": "unavailable"},
         (("restore.candidate", "before running anything"), ("restore.candidate", "no migration path"))),
        ("uid-changed", "uid_changed", {"restore.candidate": "failed", "reference.candidate": "failed"},
         (("restore.candidate", "not in the restored job"),)),
        ("uid-missing", "uid_missing", {"restore.candidate": "failed"},
         (("restore.candidate", "not in the restored job"),)),
        ("crash-restore", "crash_restore", {"restore.candidate": "failed", "recovery.candidate": "unavailable"},
         (("restore.candidate", "SIGKILL"),)),
        ("crash-continue", "crash_continue", {"restore.candidate": "passed", "recovery.candidate": "failed"},
         (("recovery.candidate", "SIGKILL"),)),
    ]
    for name, v, want, reasons in defects:
        if not run(name):
            continue
        print("%s: candidate %s" % (name, v))
        code, r, out = q.check(name, evidence_head(), H, variant(v))
        q.outcome(name, code, r, 1, want, reasons)
        if name == "schema":
            produced = os.path.join(out, "runs", "candidate", "cluster", "out")
            files = [f for f in os.listdir(produced) if os.path.getsize(os.path.join(produced, f))] \
                if os.path.isdir(produced) else []
            q.expect(name, "the refused candidate wrote no output", "nothing", files or "nothing", not files)

    if run("agreeing-and-wrong"):
        # Two builds with the same defect agree with each other; only the
        # independent reference tells them apart from a correct one.
        print("agreeing-and-wrong: baseline and candidate both built from wrong_aggregate")
        B = variant("wrong_aggregate")
        evv = q.record("agreeing-and-wrong", B)
        q.expect("agreeing-and-wrong", "record flags the baseline's output against the reference", 1,
                 evv[2], evv[2] == 1)
        code, r, _ = q.check("agreeing-and-wrong", evv, B, B)
        q.outcome("agreeing-and-wrong", code, r, 1, {"behaviour.output": "passed",
                                                     "behaviour.state": "passed",
                                                     "reference.candidate": "failed"})

    if run("memory-windows"):
        # The accumulator type registered nowhere: the fluent window found no
        # codec and kept its windows in memory, so a restore started them
        # empty. It now finds the built-in text codec and keeps them.
        print("memory-windows: a window whose accumulator type the job never registered")
        B = variant("memory_windows")
        evv = q.record("memory-windows", B)
        q.expect("memory-windows", "record exit", 0, evv[2], evv[2] == 0)
        code, r, _ = q.check("memory-windows", evv, B, B)
        q.outcome("memory-windows", code, r, 0, {"restore.candidate": "passed",
                                                 "reference.candidate": "passed"})

    if run("string-timer-keys"):
        # Timers keyed as text were routed by the key group of the text at a
        # restore that kept the layout, and about half were dropped; a
        # same-layout restore now keeps every timer the subtask checkpointed.
        print("string-timer-keys: timers keyed as text survive a restore")
        B = variant("string_timer_keys")
        evv = q.record("string-timer-keys", B)
        q.expect("string-timer-keys", "record exit", 0, evv[2], evv[2] == 0)
        code, r, _ = q.check("string-timer-keys", evv, B, B)
        q.outcome("string-timer-keys", code, r, 0, {"restore.candidate": "passed",
                                                    "reference.candidate": "passed"})

    if run("fresh-run"):
        print("fresh-run: a candidate whose submission forgets the restore")
        code, r, _ = q.check("fresh-run", evidence_head(), H, fixture_forgets_restore())
        q.outcome("fresh-run", code, r, 1, {"restore.candidate": "failed"},
                  (("restore.candidate", "position"),))

    if run("wrong-digest"):
        print("wrong-digest: a digest that is not the recorded one")
        code, r, out = q.check("wrong-digest", evidence_head(), H, H, digest="0" * 64)
        q.outcome("wrong-digest", code, r, 1, {"evidence.integrity": "failed",
                                               "restore.candidate": "unavailable"})
        q.expect("wrong-digest", "no build ran", "no runs", os.path.isdir(os.path.join(out, "runs")),
                 not os.path.isdir(os.path.join(out, "runs")))

    if run("truncated-evidence"):
        print("truncated-evidence: an input segment cut short")
        e = evidence_head()
        dst = copy_evidence(e, "truncated")
        seg = os.path.join(dst, "input", "segment-000015.txt")
        with open(seg, "rb") as f:
            data = f.read()
        with open(seg, "wb") as f:
            f.write(data[:len(data) // 2])
        code, r, _ = q.check("truncated-evidence", (dst, e[1]), H, H)
        q.outcome("truncated-evidence", code, r, 1, {"evidence.integrity": "failed"},
                  (("evidence.integrity", "truncated"),))

    if run("unsupported-version"):
        print("unsupported-version: evidence in a version this release does not read")
        e = evidence_head()
        dst = copy_evidence(e, "version2")
        idx = os.path.join(dst, "evidence.json")
        doc = json.load(open(idx))
        doc["version"] = 2
        with open(idx, "w") as f:
            json.dump(doc, f)
        code, r, _ = q.check("unsupported-version", (dst, sha(idx)), H, H)
        q.outcome("unsupported-version", code, r, 3, {"evidence.integrity": "unsupported"})

    if run("other-test"):
        print("other-test: a test definition other than the one recorded")
        t = json.load(open(TEST))
        t["input"]["seed"] = 8
        other = os.path.join(WORK, "fixtures", "other-test.json")
        with open(other, "w") as f:
            json.dump(t, f, indent=1)
        code, r, _ = q.check("other-test", evidence_head(), H, H, test=other)
        q.outcome("other-test", code, r, 1, {"evidence.integrity": "failed"},
                  (("evidence.integrity", "recorded for test definition"),))

    if run("missing-capability"):
        print("missing-capability: a candidate that does not report a required capability")
        code, r, _ = q.check("missing-capability", evidence_head(), H, fixture_without_capability())
        q.outcome("missing-capability", code, r, 3, {"builds.capabilities": "unsupported",
                                                     "reference.candidate": "passed"},
                  (("builds.capabilities", "checkpoint_meta_version"),))

    if run("other-baseline") and PRIOR:
        print("other-baseline: a baseline that is not the build that recorded the evidence")
        code, r, _ = q.check("other-baseline", evidence_head(), prior(), H)
        q.outcome("other-baseline", code, r, 1, {"builds.identity": "failed"},
                  (("builds.identity", "not the build that recorded"),))

    for name, extra, kind in (("timeout", ["--run-timeout-s=2"], "timeout"),
                              ("memory-limit", ["--max-rss-mib=24"], "memory"),
                              ("output-limit", ["--max-output-mib=0.1"], "output")):
        if not run(name):
            continue
        print("%s: %s" % (name, " ".join(extra)))
        code, r, _ = q.check(name, evidence_head(), H, H, extra=extra)
        q.outcome(name, code, r, 3, {"reference.candidate": "unavailable"})
        stopped = [c["id"] for c in (r or {}).get("checks", [])
                   if c["verdict"] == "unavailable" and "(%s)" % kind in c["reason"]]
        q.expect(name, "a run stopped by the %s bound, never passed" % kind, "named",
                 stopped or "none", bool(stopped))

    if run("interrupted"):
        print("interrupted: SIGINT to the gate while a build runs")
        code, r, out = q.check("interrupted", evidence_head(), H, H, interrupt_after=4)
        q.outcome("interrupted", code, r, 3, {})
        reasons = " ".join(c["reason"] for c in (r or {}).get("checks", []))
        q.expect("interrupted", "the report says interrupted", "interrupted", "interrupted" in reasons,
                 "interrupted" in reasons)
        time.sleep(1)
        left = leftover_nodes(out)
        q.expect("interrupted", "no build process left running", "none", left or "none", not left)


def main():
    q = Qualification()
    hashes = {}
    try:
        scenarios(q)
    finally:
        for name, (path, digest, _) in q.evidence.items():
            hashes[name] = sha(os.path.join(path, "evidence.json")) == digest
        for name, same in hashes.items():
            q.expect("evidence", "evidence %s unchanged by the whole qualification" % name, True, same, same)
        # Build identities, from the control's report where it ran.
        rep = os.path.join(WORK, "checks", "control", "report.json")
        if os.path.isfile(rep):
            shutil.copyfile(rep, os.path.join(RESULTS, "control-report.json"))
    return q.write()


if __name__ == "__main__":
    sys.exit(main())
