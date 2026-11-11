"""The runner's status, from its state directory alone.

It says what was last attempted and last verified, the coverage established
and its gaps, the backlog, the discrepancies not yet acknowledged, the checks
that could not finish, how old the last verification is, and what the runner
costs. It never lets a later pass hide an earlier discrepancy or gap: those
stay until an operator acknowledges them, and the acknowledgement is kept
beside them.

States, most severe first, with the exit status `run`, `poll` and `status`
share:

    discrepancy         1  a verification found the destination or the
                           job's output differing, not yet acknowledged
    stopped             3  the runner stopped starting audits (disk, or the
                           job's checkpoints were replaced)
    incomplete          3  a scope could not be verified (partial, error,
                           unsupported, expired, skipped), not acknowledged,
                           or an audit's last attempt did not finish and it
                           waits to be tried again
    behind              3  more eligible checkpoints wait than backlog_warn,
                           or the oldest waits longer than stale_after_s
    stale               3  the last verification is older than stale_after_s
    no-eligible-scope   5  no checkpoint to audit has completed yet
    healthy             0  every scope selected so far is verified
"""

import json
import os
import time

from .runner import completed_markers
from .state import StateDir, now_iso, read_json

EXIT = {"discrepancy": 1, "stopped": 3, "incomplete": 3, "behind": 3, "stale": 3,
        "no-eligible-scope": 5, "healthy": 0}
ORDER = ["discrepancy", "stopped", "incomplete", "behind", "stale", "no-eligible-scope", "healthy"]


def _parse_iso(text):
    if not text:
        return None
    try:
        base = time.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
        import calendar
        return calendar.timegm(base) + (int(text[20:23]) / 1000.0 if len(text) > 22 else 0)
    except (ValueError, TypeError):
        return None


def _acked(acks, audits=(), first=None, last=None):
    for a in acks.values():
        if a.get("audit") and a["audit"] in audits:
            return a
        rng = a.get("checkpoints")
        if rng and first is not None and rng[0] <= first and last <= rng[1]:
            return a
    return None


def compute(state_dir, now=None):
    d = StateDir(state_dir)
    st = d.load()
    now = time.time() if now is None else now
    if st is None:
        return {"state": "no-eligible-scope", "exit": EXIT["no-eligible-scope"], "conditions": [
            "this state directory has not run yet"], "at": now_iso(now)}
    acks = d.acknowledgements()
    lim = st.get("limits", {})
    conditions = []

    discrepancies = []
    for x in st.get("discrepancies", []):
        ack = _acked(acks, [x["audit"]], x["checkpoint"], x["checkpoint"])
        discrepancies.append(dict(x, acknowledged=ack))
    open_disc = [x for x in discrepancies if not x["acknowledged"]]

    gaps = []
    for g in st.get("gaps", []):
        ack = _acked(acks, g.get("audits") or [], g["first"], g["last"])
        gaps.append(dict(g, acknowledged=ack))
    open_gaps = [g for g in gaps if not g["acknowledged"] and g["outcome"] != "discrepancy"]

    completed = completed_markers(st["job"]["checkpoint_dir"], st["job"]["id"],
                                  "CONFIRMED" if st.get("commit_confirmation") is True else "COMPLETED") or []
    start = st.get("start")
    handled = set(st.get("handled_above", []))
    eligible = [c for c in completed if start is not None and c >= start]
    in_progress = {int(k): v for k, v in st.get("audits", {}).items()}
    waiting = [c for c in eligible if c > st.get("handled_upto", 0) and c not in handled]
    collected = [c for c, a in in_progress.items() if "collect" in a.get("steps", {})]
    retrying = [{"checkpoint": c, "audit": a["id"], "attempt": a["attempt"],
                 "next_attempt_at": now_iso(a["next_attempt_at"]) if a.get("next_attempt_at") else None,
                 "last": (a.get("attempts") or [{}])[-1].get("reason", "")[:300]}
                for c, a in sorted(in_progress.items()) if a.get("attempts")]
    oldest_wait_s = None
    if waiting:
        # How long the oldest waiting checkpoint has been complete, from its marker.
        marker = os.path.join(st["job"]["checkpoint_dir"], "_jobs", str(st["job"]["id"]),
                              "COMPLETED-%d" % waiting[0])
        try:
            oldest_wait_s = max(0.0, now - os.stat(marker).st_mtime)
        except OSError:
            pass

    lv = st.get("last_verified")
    age = None
    if lv:
        t = _parse_iso(lv.get("at"))
        age = None if t is None else max(0.0, now - t)

    if open_disc:
        conditions.append("discrepancy")
    if st.get("stopped"):
        conditions.append("stopped")
    # An audit waiting to be tried again could not finish its last attempt.
    broken = (st.get("window") or {}).get("broken")
    if open_gaps or retrying or broken:
        conditions.append("incomplete")
    if (len(waiting) > lim.get("backlog_warn", 10) or
            (oldest_wait_s is not None and oldest_wait_s > lim.get("stale_after_s", 900))):
        conditions.append("behind")
    if eligible and (age is None or age > lim.get("stale_after_s", 900)) and waiting:
        conditions.append("stale")
    if not eligible:
        conditions.append("no-eligible-scope")
    state = next((s for s in ORDER if s in conditions), "healthy")

    coverage = []
    for k, cov in sorted(st.get("coverage", {}).items()):
        verified = cov.get("verified", [])
        holes = [[a[1], b[0]] for a, b in zip(verified, verified[1:]) if b[0] is not None and b[0] > a[1]]
        coverage.append({"topic": cov["topic"], "topic_id": cov["topic_id"], "cluster_id": cov["cluster_id"],
                         "partition": cov["partition"], "verified": verified, "holes": holes})

    out = {
        "format": "clink-audit-status", "version": 1, "at": now_iso(now),
        "state": state, "exit": EXIT[state], "conditions": conditions,
        "job": st["job"], "selection": st.get("selection"), "start": start,
        "config_fingerprint": st.get("config_fingerprint"), "graph_sha256": st.get("graph"),
        "last_attempted": st.get("last_attempted"), "last_verified": lv,
        "last_verified_age_s": None if age is None else round(age, 1),
        "verified_checkpoints": st.get("verified_checkpoints", []),
        "coverage": coverage, "coverage_mode": "selected-keys: rows of other keys are not established",
        "gaps": gaps, "unacknowledged_gaps": len(open_gaps),
        "discrepancies": discrepancies, "unacknowledged_discrepancies": len(open_disc),
        "last_discrepancy": st.get("last_discrepancy"),
        "backlog": {"waiting": len(waiting), "oldest": waiting[0] if waiting else None,
                    "newest": waiting[-1] if waiting else None,
                    "oldest_wait_s": None if oldest_wait_s is None else round(oldest_wait_s, 1),
                    "collected": len(collected), "retrying": retrying,
                    "note": st.get("backlog_note")},
        "stopped": st.get("stopped"),
        "kind": st.get("kind"),
        "window": None if st.get("kind") != "windowed" else {
            "states_held": len((st.get("window") or {}).get("states", {})),
            "newest_state": max((int(c) for c in (st.get("window") or {}).get("states", {})), default=None),
            "chain_broken": broken},
        "resources": {"last_cycle": st.get("last_cycle"), "totals": st.get("totals"),
                      "disk_bytes": d.usage(), "disk_limit": lim.get("disk_bytes"),
                      "retained_passed": len(st.get("retained_passed", [])),
                      "retain_passed": lim.get("retain_passed"),
                      "failure_evidence": _failure_evidence(d, st)},
        "recent": st.get("recent", [])[-10:],
    }
    return out


def _failure_evidence(d, st):
    """Audit directories kept as evidence of a result that did not pass: the
    runner's own (marked), published, and not among the passed it retains."""
    if not d.audits.is_dir():
        return []
    passed = set(st.get("retained_passed", []))
    running = {a["id"] for a in st.get("audits", {}).values()}
    out = []
    for p in d.audits.iterdir():
        if p.name in passed or p.name in running or not d.owns(p, p.name):
            continue
        out.append(p.name)
    return sorted(out)


def _ago(s):
    if s is None:
        return "never"
    if s < 120:
        return "%.0f s ago" % s
    if s < 7200:
        return "%.0f min ago" % (s / 60)
    return "%.1f h ago" % (s / 3600)


def _span(first, last):
    return str(first) if first == last else "%s-%s" % (first, last)


def render_text(s):
    lines = []
    job = s.get("job") or {}
    lines.append("clink-audit: job %s at %s, %s selection" % (job.get("id"), job.get("checkpoint_dir"),
                                                             s.get("selection")))
    lines.append("state: %s (exit %d)%s" % (s["state"], s["exit"],
                                            "; also " + ", ".join(c for c in s["conditions"] if c != s["state"])
                                            if len(s["conditions"]) > 1 else ""))
    if s.get("stopped"):
        lines.append("stopped: %s: %s" % (s["stopped"].get("reason"), s["stopped"].get("detail", "")))
    w = s.get("window")
    if w:
        lines.append("windowed: the reference's state held at %d checkpoint(s), the newest %s%s" % (
            w["states_held"], w["newest_state"],
            ("; the chain is BROKEN at checkpoint %s: %s" % (w["chain_broken"]["at"], w["chain_broken"]["why"][:200]))
            if w.get("chain_broken") else ""))
    lv = s.get("last_verified")
    lines.append("last verified: %s" % ("checkpoint %d (%s), %s" % (lv["checkpoint"], lv["outcome"],
                                                                     _ago(s.get("last_verified_age_s")))
                                        if lv else "none yet"))
    la = s.get("last_attempted")
    if la:
        lines.append("last attempted: checkpoint %d: %s%s" % (la["checkpoint"], la.get("outcome", "under way"),
                                                               (": " + la["reason"][:160]) if la.get("reason") else ""))
    vc = s.get("verified_checkpoints") or []
    if vc:
        lines.append("verified checkpoints: %s" % ", ".join(_span(a, b) for a, b in vc))
    for c in s.get("coverage", []):
        lines.append("coverage: %s[%d] (topic id %s): %s%s" % (
            c["topic"], c["partition"], c["topic_id"],
            ", ".join("[%s, %s)" % ("run start" if a is None else a, b) for a, b in c["verified"]),
            ("; not verified: " + ", ".join("[%s, %s)" % (a, b) for a, b in c["holes"])) if c["holes"] else ""))
    if s.get("coverage"):
        lines.append("  (%s)" % s["coverage_mode"])
    for g in s.get("gaps", []):
        lines.append("gap: checkpoint%s %s, %s: %s%s" % ("s" if g["first"] != g["last"] else "",
                                                       _span(g["first"], g["last"]), g["outcome"],
                                                       g["reason"][:200],
                                                       " [acknowledged]" if g.get("acknowledged") else ""))
    for x in s.get("discrepancies", []):
        lines.append("discrepancy: checkpoint %d: %s (%s)%s" % (
            x["checkpoint"], "; ".join(f.get("relationship", "") + ": " + f.get("summary", "")[:160]
                                       for f in x.get("findings", [])[:3]) or x["reason"][:200],
            x["report"], " [acknowledged]" if x.get("acknowledged") else ""))
    b = s.get("backlog") or {}
    lines.append("backlog: %d eligible checkpoint(s) waiting%s, %d collected%s" % (
        b.get("waiting", 0), (" (oldest %s, complete %s)" % (b["oldest"], _ago(b.get("oldest_wait_s"))))
        if b.get("oldest") is not None else "", b.get("collected", 0),
        ("; " + b["note"]) if b.get("note") else ""))
    for r in b.get("retrying", []):
        lines.append("retrying: checkpoint %d, attempt %d, next %s: %s" % (r["checkpoint"], r["attempt"],
                                                                         r["next_attempt_at"], r["last"][:160]))
    res = s.get("resources") or {}
    lc = res.get("last_cycle") or {}
    lines.append("resources: last cycle %.1f s wall, %.1f s CPU, %d audit(s); disk %.1f MiB of %.1f MiB; "
                 "%d passed audit(s) kept of %s, %d other audit(s) kept as evidence" % (
                     lc.get("wall_s") or 0, lc.get("cpu_s") or 0, lc.get("published") or 0,
                     (res.get("disk_bytes") or 0) / 1048576.0, (res.get("disk_limit") or 0) / 1048576.0,
                     res.get("retained_passed", 0), res.get("retain_passed"),
                     len(res.get("failure_evidence", []))))
    return "\n".join(lines)


def write(state_dir, now=None):
    s = compute(state_dir, now)
    from .state import atomic_write_json
    atomic_write_json(StateDir(state_dir).status_file, s)
    return s
