"""The report, `clink-upgrade-report` version 1: one verdict per check, and a
result that is a pass only when every required check completed and passed.

A check is passed, failed, unavailable (it could not run, or ran and could not
decide: a timeout, a limit, an earlier failure it depends on) or unsupported
(the evidence or a build asks for something this release does not validate).
A failure stays a failure whatever else could not run. Informational checks
(the baseline against the reference, raw bytes, replay) are reported and
never decide. The result is:

    passed           every required check passed               exit 0
    failed           any check failed                          exit 1
    not established  no failure, and a required check did not  exit 3
                     pass
"""

import datetime
import json

from . import EXIT_FAIL, EXIT_NOT_ESTABLISHED, EXIT_PASS, REPORT_FORMAT, REPORT_VERSION

PASSED, FAILED, UNAVAILABLE, UNSUPPORTED = "passed", "failed", "unavailable", "unsupported"

# The checks the contract requires, in order. Informational checks are
# reported beside them and never decide the result.
REQUIRED = (
    ("evidence.integrity", "the evidence is the set recorded, whole and unchanged since"),
    ("isolation", "loopback-only network isolation, proven before any build ran"),
    ("builds.identity", "both builds identified by hash; the baseline is the build that recorded "
                        "the evidence"),
    ("builds.capabilities", "both builds report the capabilities the test requires"),
    ("evidence.readable", "the tools build verifies and decodes the evidence's checkpoint"),
    ("schema.candidate", "the candidate's own check-savepoint accepts every snapshot for its job"),
    ("restore.baseline", "the baseline restores the evidence's checkpoint to the state it holds"),
    ("restore.candidate", "the candidate restores the evidence's checkpoint to the state it holds, "
                          "every operator that holds state present under the same identity"),
    ("recovery.baseline", "the baseline completes the continuation"),
    ("recovery.candidate", "the candidate completes the continuation"),
    ("behaviour.output", "the candidate's continuation output equals the baseline's"),
    ("behaviour.state", "the candidate's final logical state equals the baseline's"),
    ("reference.candidate", "the candidate's output and final state equal the independent "
                            "reference's"),
    ("evidence.unchanged", "the evidence is unchanged after both runs"),
)
INFORMATIONAL = (
    ("reference.baseline", "the baseline's output and final state against the reference"),
    ("state.raw", "the candidate's restored checkpoint against the evidence's, raw bytes "
                  "(clink state-diff)"),
    ("replay.determinism", "replay of a captured epoch"),
)


class Report:
    def __init__(self, kind):
        self.kind = kind
        self.checks = {}
        self.started = datetime.datetime.now(datetime.timezone.utc)
        self.facts = {}

    def set(self, check_id, verdict, reason, **details):
        assert verdict in (PASSED, FAILED, UNAVAILABLE, UNSUPPORTED), verdict
        self.checks[check_id] = {"id": check_id, "verdict": verdict, "reason": reason,
                                 "details": details}
        return verdict == PASSED

    def verdict(self, check_id):
        c = self.checks.get(check_id)
        return c["verdict"] if c else None

    def passed(self, *ids):
        return all(self.verdict(i) == PASSED for i in ids)

    def fill_unavailable(self, why):
        for cid, _ in REQUIRED + INFORMATIONAL:
            if cid not in self.checks:
                self.set(cid, UNAVAILABLE, why)

    def result(self):
        required = [cid for cid, _ in REQUIRED]
        informational = {cid for cid, _ in INFORMATIONAL}
        # Any failure decides, required or recorded beside them; an
        # informational check is reported and never decides.
        if any(c["verdict"] == FAILED for cid, c in self.checks.items() if cid not in informational):
            return "failed", EXIT_FAIL
        if all(self.verdict(c) == PASSED for c in required):
            return "passed", EXIT_PASS
        return "not established", EXIT_NOT_ESTABLISHED

    def document(self):
        result, code = self.result()
        titles = dict(REQUIRED + INFORMATIONAL)
        required = {cid for cid, _ in REQUIRED}
        ordered = [cid for cid, _ in REQUIRED + INFORMATIONAL if cid in self.checks]
        ordered += sorted(c for c in self.checks if c not in ordered)
        return {
            "format": REPORT_FORMAT,
            "version": REPORT_VERSION,
            "kind": self.kind,
            "started": self.started.isoformat(),
            "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "result": result,
            "exit": code,
            "scope": ("compatibility of the candidate for this test definition, this evidence and "
                      "this configuration; it is not a claim that every job or every checkpoint "
                      "upgrades"),
            "checks": [dict(self.checks[c], title=titles.get(c, ""), required=c in required)
                       for c in ordered],
            **self.facts,
        }

    def write(self, path_json, path_text):
        doc = self.document()
        with open(path_json, "w") as f:
            json.dump(doc, f, indent=1, sort_keys=True)
            f.write("\n")
        text = render_text(doc)
        with open(path_text, "w") as f:
            f.write(text)
        return doc, text


def render_text(doc):
    lines = ["upgrade %s: %s (exit %d)" % (doc["kind"], doc["result"].upper(), doc["exit"])]
    b = doc.get("builds") or {}
    for name in ("baseline", "candidate", "tools"):
        ident = b.get(name)
        if not ident:
            continue
        sr = ident.get("self_reported") or {}
        lines.append("  %-9s %s, %s; clink %s, job %s" % (
            name, sr.get("version") or "version unavailable", ident.get("provenance", ""),
            ident["files"]["clink"]["sha256"][:12] if "clink" in ident.get("files", {}) else "-",
            ident["files"]["job"]["sha256"][:12] if "job" in ident.get("files", {}) else "-"))
    ev = doc.get("evidence") or {}
    if ev:
        lines.append("  evidence  %s, digest %s" % (ev.get("path", ""), (ev.get("digest") or "")[:16]))
    lines.append("  scope     %s" % doc["scope"])
    for c in doc["checks"]:
        mark = {"passed": "ok  ", "failed": "FAIL", "unavailable": "n/a ", "unsupported": "uns "}[c["verdict"]]
        lines.append("  %s %-20s %s%s" % (mark, c["id"], c["reason"], "" if c["required"] else " (informational)"))
    lim = doc.get("limits")
    if lim:
        lines.append("  limits    %s" % ", ".join("%s=%s" % kv for kv in sorted(lim.items())))
    if doc.get("artifacts"):
        lines.append("  retained  %s" % doc["artifacts"].get("root", ""))
    return "\n".join(lines) + "\n"
