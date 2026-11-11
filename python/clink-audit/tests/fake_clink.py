#!/usr/bin/env python3
"""A scripted stand-in for `clink` and for a reference, for the runner's tests.

Installed into a test's directory as `clink` and `reference`, it reads
plan.json beside itself and answers incident-collect, incident-scope,
incident-verify and incident-reconcile, or a reference run, the way the plan
says for each checkpoint and each attempt. It logs every call, with its
argument array and the names of the environment variables it was given, to
calls.ndjson beside it. It writes the files the real commands write, with
the fields the runner reads, and nothing it does not need.
"""
import fcntl
import json
import os
import signal
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLAN = json.loads((HERE / "plan.json").read_text())


def flags(argv):
    out, pos = {}, []
    for a in argv:
        if a.startswith("--"):
            k, eq, v = a[2:].partition("=")
            out[k] = v if eq else True
        else:
            pos.append(a)
    return out, pos


def count(name):
    """How many times this call was made before, kept in counts.json."""
    with open(HERE / "counts.lock", "a+") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        p = HERE / "counts.json"
        counts = json.loads(p.read_text()) if p.exists() else {}
        n = counts.get(name, 0)
        counts[name] = n + 1
        p.write_text(json.dumps(counts))
    return n


def log(kind, argv):
    with open(HERE / "calls.ndjson", "a") as f:
        f.write(json.dumps({"kind": kind, "argv": argv, "env": sorted(os.environ),
                            "pid": os.getpid()}) + "\n")


def plan_for(cp):
    p = dict(PLAN.get("default", {}))
    p.update(PLAN.get("checkpoints", {}).get(str(cp), {}))
    return p


def pick(value, n):
    """A per-attempt list picks the nth (the last repeats)."""
    if isinstance(value, list):
        return value[min(n, len(value) - 1)]
    return value


def hang():
    while True:
        time.sleep(1)


def refuse(kind, reason):
    print("incident-collect: refused: " + reason, file=sys.stderr)
    print(json.dumps({"refused": True, "kind": kind, "reason": reason}))
    sys.exit(1)


def collect(argv):
    f, _ = flags(argv)
    cp = int(f["checkpoint"])
    p = plan_for(cp)
    n = count("collect-%d" % cp)
    how = pick(p.get("collect", "ok"), n)
    out = Path(f["out"])
    time.sleep(p.get("collect_sleep", 0))
    if out.exists():
        refuse("other", "%s already exists; a bundle is never written over" % out)
    marker = Path(f["checkpoint-dir"]) / "_jobs" / f["job"] / ("COMPLETED-%d" % cp)
    if how == "missing" or not marker.exists():
        refuse("missing", "checkpoint %d has no COMPLETED marker; nothing was published" % cp)
    if how in ("changed", "bound", "timeout", "other"):
        refuse(how, "the evidence %s (fake)" % how)
    if how == "hang":
        tmp = out.parent / (".%s.incomplete-%d-1" % (out.name, os.getpid()))
        tmp.mkdir(parents=True, exist_ok=True)
        hang()
    tmp = out.parent / (".%s.incomplete-%d-2" % (out.name, os.getpid()))
    tmp.mkdir(parents=True)
    (tmp / "payload.bin").write_bytes(b"x" * int(p.get("bundle_bytes", 64)))
    absent = []
    if how == "expired":
        absent = [{"role": "interval_state", "path": "checkpoint/_state/1/v1/0/checkpoint-%d.snap" % (cp - 1),
                   "reason": "not present in the job's state directory"}]
    manifest = {"format": "clink-incident-bundle", "version": 1,
                "job": {"id": int(f["job"]), "commit_confirmation": False},
                "selection": {"checkpoint": cp, "engine_choice": cp, "generation": 1,
                              "restored": p.get("restored")},
                "collector": {"clink_version": "fake"}, "absent": absent}
    (tmp / "manifest.json").write_text(json.dumps(manifest))
    os.rename(tmp, out)
    print(json.dumps({"bundle": str(out), "checkpoint": cp, "files": 2,
                      "bytes": int(p.get("bundle_bytes", 64)), "seconds": 0.01, "notes": []}))


def bundle_checkpoint(path):
    return json.loads((Path(path) / "manifest.json").read_text())["selection"]["checkpoint"]


def scope(argv):
    f, pos = flags(argv)
    cp = int(f["checkpoint"])
    assert bundle_checkpoint(pos[0]) == cp
    p = plan_for(cp)
    n = count("scope-%d" % cp)
    how = pick(p.get("scope", "ok"), n)
    out = Path(f["out"])
    if how == "unsupported":
        print("incident-scope: the job keeps state across records in a form the audit does not support",
              file=sys.stderr)
        sys.exit(3)
    if how == "error":
        print("incident-scope: something unforeseen", file=sys.stderr)
        sys.exit(2)
    doc = {"format": "clink-audit-scope", "version": 1, "checkpoint": cp,
           "graph_sha256": p.get("graph", "g1"), "scope_sha256": "s%d" % cp, "sink_op": "7",
           "attempts": ["a1"], "starts_after": p.get("starts_after", cp - 1),
           "starts_at_run_start": p.get("ranges", [[0, None, 0]])[0][1] is None,
           "sources": [{"topic": "payments", "cluster_id": "C1", "topic_id": p.get("topic_id", "T1"),
                        "same_log": None,
                        "partitions": [{"partition": r[0], "from": r[1], "to": r[2]}
                                       for r in p.get("ranges", [[0, (cp - 1) * 10, cp * 10]])]}],
           "sources_complete": how != "not-whole"}
    if how == "windowed":
        doc["version"] = 2
        doc["transform"] = {"kind": "tumble"}
    out.write_text(json.dumps(doc))
    if how == "not-whole":
        print("incident-scope: the scope's source positions are not whole", file=sys.stderr)
        sys.exit(3)


def verify(argv):
    f, pos = flags(argv)
    cp = bundle_checkpoint(pos[0])
    p = plan_for(cp)
    n = count("verify-%d" % cp)
    how = pick(p.get("verify", 0), n)
    if how == "hang":
        hang()
    checks = [{"id": "bundle.manifest", "verdict": "passed", "reason": ""},
              {"id": "checkpoint.participants", "verdict": {0: "passed", 1: "failed", 3: "unavailable"}[how],
               "reason": "fake verdict %d" % how}]
    if f.get("report"):
        Path(f["report"]).write_text(json.dumps({"checks": checks, "exit_code": how}))
    sys.exit(how)


def reconcile(argv):
    f, pos = flags(argv)
    cp = int(f["checkpoint"])
    assert bundle_checkpoint(pos[0]) == cp
    p = plan_for(cp)
    n = count("reconcile-%d" % cp)
    how = pick(p.get("reconcile", "pass"), n)
    rows = int(p.get("rows", 10))
    time.sleep(p.get("reconcile_sleep", 0))
    for path in p.get("reconcile_touch", []):
        Path(path).write_text("{}")  # a checkpoint completing while the runner works
    if how == "hang":
        hang()
    if how == "crash":
        os.kill(os.getpid(), signal.SIGKILL)
    if f.get("password-env") and f["password-env"] not in os.environ:
        print("incident-reconcile: --password-env names %s, which is not set" % f["password-env"],
              file=sys.stderr)
        sys.exit(2)
    ref = f.get("reference")
    ref_rows = None
    if ref:
        lines = Path(ref).read_text().splitlines()
        try:
            end = json.loads(lines[-1])["end"]
        except (ValueError, KeyError, IndexError):
            print("incident-reconcile: the reference artifact has no end line", file=sys.stderr)
            sys.exit(2)
        ref_rows = end["rows"]
    if how == "refused":
        print("incident-reconcile: refused: the table's UUID is not the evidence's", file=sys.stderr)
        sys.exit(2)
    passed = lambda i, **c: {"id": i, "verdict": "passed", "reason": "", "counts": c or None}
    checks = [passed("bundle.manifest"), passed("reconcile.scope", expected_rows=rows),
              passed("reconcile.destination")]
    findings, code = [], 0
    if how == "pass":
        checks.append(passed("reconcile.rows", queries=2, expected=rows, matched=rows))
    elif how == "delivery":
        checks.append({"id": "reconcile.rows", "verdict": "failed", "reason": "1 changed",
                       "counts": {"queries": 2, "changed": 1}})
        findings.append({"relationship": "delivery", "summary": "the destination does not hold what the sink received (1 changed)"})
        code = 1
    elif how in ("destination-down", "bound"):
        checks[-1] = {"id": "reconcile.destination", "verdict": "unavailable",
                      "reason": "the destination could not be read: %s" % (
                          "Connection refused" if how == "destination-down" else "Limit for result exceeded")}
        checks.append({"id": "reconcile.rows", "verdict": "unavailable", "reason": "not run"})
        code = 3
    if ref:
        complete = end.get("complete")
        if not complete:
            checks.append({"id": "reference.scope", "verdict": "unavailable",
                           "reason": "the reference is incomplete by its own account: " + "; ".join(end.get("incomplete", []))})
            code = max(code, 3) if code != 1 else 1
        elif how == "transformation":
            checks.append(passed("reference.scope", rows=ref_rows))
            checks.append({"id": "reference.capture", "verdict": "failed", "reason": "changed"})
            findings.append({"relationship": "transformation", "summary": "the job's sink received other rows than the reference computes"})
            code = 1
        else:
            checks.append(passed("reference.scope", rows=ref_rows))
            checks.append(passed("reference.capture"))
            if rows == 0 and how == "pass":
                checks = [c for c in checks if c["id"] != "reconcile.rows"]
                checks.append({"id": "reconcile.rows", "verdict": "unavailable",
                               "reason": "the scope's intervals held no rows"})
                checks.append({"id": "reference.destination", "verdict": "unavailable",
                               "reason": "the reference and the captured input are both empty"})
                code = 3
            elif code == 0:
                checks.append(passed("reference.destination"))
    report = {"format": "clink-reconcile-report", "version": 1, "exit_code": code,
              "result": {0: "satisfied", 1: "failed", 3: "partial"}[code], "checks": checks,
              "scope": {"expected_rows": rows}, "coverage": {"mode": "selected-keys", "keys_selected": rows},
              "audit": {"findings": findings}}
    Path(f["report"]).write_text(json.dumps(report))
    sys.exit(code)


def reference(argv):
    f, _ = flags(argv)
    sc = json.loads(Path(f["scope"]).read_text())
    cp = sc["checkpoint"]
    p = plan_for(cp)
    n = count("reference-%d" % cp)
    how = pick(p.get("reference", "complete"), n)
    if os.environ.get("REFERENCE_SECRET"):
        print("connecting with %s" % os.environ["REFERENCE_SECRET"], file=sys.stderr)
    time.sleep(p.get("reference_sleep", 0))
    if how == "hang":
        hang()
    if how == "fail":
        print("reference: cannot reach the broker", file=sys.stderr)
        sys.exit(1)
    rows = int(p.get("rows", 10))
    records = int(p.get("records", rows))
    out = Path(f["out"])
    with open(out, "w") as w:
        w.write(json.dumps({"format": "clink-reference-result", "version": 1, "scope": sc,
                            "implementation": {"name": "fake-reference", "version": "1"}}) + "\n")
        for i in range(rows if how != "huge" else 200000):
            w.write(json.dumps({"event_id": "I%d" % i, "pad": "p" * 40}) + "\n")
        if how == "malformed":
            return
        complete = how not in ("incomplete",)
        end = {"rows": rows, "complete": complete,
               "incomplete": [] if complete else ["payments[0]: offsets [0, 10) are no longer in the topic: expired or deleted"],
               "read": [{"topic": "payments", "partition": 0, "records": records}]}
        w.write(json.dumps({"end": end}) + "\n")
    sys.exit(0 if complete else 3)


def main():
    argv = sys.argv[1:]
    kind = "reference" if Path(sys.argv[0]).name == "reference" else argv[0]
    log(kind, argv)
    if kind == "reference":
        return reference(argv)
    return {"incident-collect": collect, "incident-scope": scope, "incident-verify": verify,
            "incident-reconcile": reconcile}[kind](argv[1:])


if __name__ == "__main__":
    main()
