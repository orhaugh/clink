#!/usr/bin/env python3
"""Judge a half-time evidence bundle against a clink release candidate.

    scripts/check-half-time-evidence.py <bundle> --candidate <commit>

half-time is the showcase that runs on one machine rather than in CI: its
seventeen services outgrow a hosted runner, and it needs a clink built from
source. Its evidence.sh runs it against a candidate and writes a bundle (see
half-time's README, "Release evidence"); scripts/check-showcases.sh asks this
script whether the bundle shows half-time passing against exactly that
candidate. It does when:

- every file the bundle lists in MANIFEST.sha256 has the hash listed, and no
  file is unlisted;
- the bundle is for the candidate: the clink checkout was at it with nothing
  changed, clink and clink_node reported it as the commit they were built
  from, clean, and the riders job, the one half-time compiles, was built
  against an install of it;
- half-time ran from a commit with no local changes, with every scene;
- the run and each of the four checkers exited 0, every verdict held, and the
  stores were held to all three guarantees, exactly once, at least once and
  upsert;
- the Worker kill, the savepoint and the Coordinator failover each ran, and
  the bundle shows what they did: jobs restarting in the Coordinator's log
  after the kill, the savepoint's checkpoint id, the standby leading and
  recovering the jobs; no scene failed, and a skipped scene is not a pass.

Prints PASS or FAIL with every reason, and exits 0 or 1.
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

FORMAT = "half-time-evidence/1"
CHECKERS = ("check_run", "check_round2", "check_round3", "check_riders")
GUARANTEES = ("exactly_once", "at_least_once", "upsert")
SCENES = ("worker_loss", "savepoint", "coordinator_failover")
SHA = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def judge(bundle, candidate):
    """The reasons the bundle does not show half-time passing against the
    candidate (none when it does), and a one-line summary of what it shows."""
    bundle = Path(bundle)
    if not (bundle / "evidence.json").is_file():
        return [f"no evidence: {bundle} holds no evidence.json"], ""
    try:
        ev = json.loads((bundle / "evidence.json").read_text())
    except ValueError as e:
        return [f"evidence.json does not parse: {e}"], ""
    if not isinstance(ev, dict) or ev.get("format") != FORMAT:
        return [f"evidence.json is not {FORMAT}"], ""
    problems = []

    listed = {}
    manifest = bundle / "MANIFEST.sha256"
    if manifest.is_file():
        for line in manifest.read_text().splitlines():
            digest, _, name = line.partition("  ")
            if name.startswith("/") or ".." in Path(name).parts:
                problems.append(f"the manifest names {name}, outside the bundle")
                continue
            listed[name] = digest
    else:
        problems.append("no MANIFEST.sha256")
    for name, digest in sorted(listed.items()):
        path = bundle / name
        if not path.is_file():
            problems.append(f"{name} is listed but absent")
        elif sha256(path) != digest:
            problems.append(f"{name} does not match its hash")
    present = {p.relative_to(bundle).as_posix() for p in bundle.rglob("*") if p.is_file()}
    for name in sorted(present - set(listed) - {"MANIFEST.sha256"}):
        problems.append(f"{name} is not in the manifest")

    def text(name):
        """A file the bundle lists, or None (and a reason) when it does not."""
        if not name or name not in listed or not (bundle / name).is_file():
            problems.append(f"{name or 'a file evidence.json names'} is not in the bundle")
            return None
        return (bundle / name).read_text(errors="replace")

    if ev.get("candidate") != candidate:
        problems.append(f"the bundle is for clink {ev.get('candidate')}, not the candidate {candidate}")
    clink = ev.get("clink") or {}
    source = clink.get("source") or {}
    if source.get("head") != candidate or source.get("clean") is not True:
        problems.append(f"the clink checkout was at {source.get('head')}"
                        f"{'' if source.get('clean') is True else ', with local changes'}, not the candidate")
    binaries = clink.get("binaries") or {}
    for name in ("clink", "clink_node"):
        b = binaries.get(name) or {}
        if b.get("commit") != candidate or b.get("clean") is not True:
            problems.append(f"{name} reported {b.get('commit') or 'no commit'}"
                            f"{'' if b.get('clean') is True else ' (not clean)'}, not the candidate")
    for name in ("clink", "clink_node", "clink_submit_sql"):
        if not SHA256.fullmatch(str((binaries.get(name) or {}).get("sha256", ""))):
            problems.append(f"no hash recorded for {name}")

    riders = ev.get("riders") or {}
    if riders.get("commit") != candidate:
        problems.append(f"the riders job was built against clink {riders.get('commit') or 'of no recorded commit'}"
                        f" ({riders.get('built_against')}), not the candidate")

    half_time = ev.get("half_time") or {}
    if not SHA.fullmatch(str(half_time.get("commit", ""))):
        problems.append("no half-time commit recorded")
    if half_time.get("clean") is not True:
        problems.append(f"half-time had local changes ({len(half_time.get('changes') or [])} paths, "
                        f"see {half_time.get('diff') or 'evidence.json'}): a release check needs a committed half-time")
    env = (ev.get("invocation") or {}).get("env") or {}
    if env.get("SCENES") != "1":
        problems.append(f"the run had SCENES={env.get('SCENES')}, not every scene")

    if (ev.get("run") or {}).get("exit_code") != 0:
        problems.append(f"run.sh exited {(ev.get('run') or {}).get('exit_code')}")
    checks = {c.get("checker"): c for c in ev.get("checks") or [] if isinstance(c, dict)}
    for name in CHECKERS:
        c = checks.get(name)
        if c is None:
            problems.append(f"{name} did not run")
            continue
        if c.get("exit_code") != 0:
            problems.append(f"{name} exited {c.get('exit_code')}")
        text(c.get("output"))
    trace = ev.get("trace_check") or {}
    if trace.get("ran") and trace.get("exit_code") != 0:
        problems.append("the protocol trace was not accepted")

    counts = {}
    per_checker = {}
    verdicts = text((ev.get("verdicts") or {}).get("file"))
    for line in (verdicts or "").splitlines():
        if not line.strip():
            continue
        try:
            v = json.loads(line)
        except ValueError:
            problems.append("verdicts.jsonl has a line that does not parse")
            continue
        g = v.get("guarantee")
        counts[g] = counts.get(g, 0) + 1
        per_checker[v.get("checker")] = per_checker.get(v.get("checker"), 0) + 1
        if v.get("ok") is not True:
            problems.append(f"{v.get('checker')}: {v.get('line')} did not hold ({g})")
    if verdicts is not None:
        for name in CHECKERS:
            if not per_checker.get(name):
                problems.append(f"{name} recorded no verdicts")
        for g in GUARANTEES:
            if not counts.get(g):
                problems.append(f"no store was held to {g.replace('_', ' ')}")

    scenes = {}
    for s in ev.get("scenes") or []:
        if isinstance(s, dict):
            scenes.setdefault(s.get("scene"), s)
            if s.get("status") == "failed":
                problems.append(f"the {s.get('scene')} scene failed")
    for name in SCENES:
        s = scenes.get(name)
        if s is None:
            problems.append(f"no record that the {name} scene ran")
        elif s.get("status") != "executed":
            problems.append(f"the {name} scene was {s.get('status')}, which is not a pass")
    logs = ev.get("logs") or {}
    if (scenes.get("worker_loss") or {}).get("status") == "executed":
        coordinator = text(logs.get("coordinator"))
        if coordinator is not None and "awaiting_restart" not in coordinator:
            problems.append("the Coordinator's log shows no job restarting after the Worker kill")
    if (scenes.get("savepoint") or {}).get("status") == "executed":
        s = scenes["savepoint"]
        if not str(s.get("savepoint_checkpoint_id", "")).isdigit() or "ok=1" not in str(s.get("stop_reply", "")):
            problems.append(f"the savepoint scene shows no savepoint taken: {s.get('stop_reply')!r}")
        if not str(s.get("resume_reply", "")).strip():
            problems.append("the savepoint scene shows no job resumed from it")
    if (scenes.get("coordinator_failover") or {}).get("status") == "executed":
        standby = text(logs.get("standby"))
        if standby is not None and ("became leader" not in standby or "recovered job_id=" not in standby):
            problems.append("the standby's log shows no takeover: it never led, or recovered no job")

    summary = (f"half-time {str(half_time.get('commit', ''))[:12]}; scenes "
               + ", ".join(f"{s.get('scene')} {s.get('status')}" for s in ev.get("scenes") or [])
               + "; verdicts held: "
               + ", ".join(f"{g.replace('_', ' ')} {n}" for g, n in sorted(counts.items()))
               + f"; run started {(ev.get('run') or {}).get('started_utc')}")
    return problems, summary


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("bundle", help="the bundle directory evidence.sh wrote")
    ap.add_argument("--candidate", required=True, help="the full commit of the clink release candidate")
    args = ap.parse_args()
    if not SHA.fullmatch(args.candidate):
        ap.error("--candidate takes the full 40-character commit")
    problems, summary = judge(args.bundle, args.candidate)
    if problems:
        print(f"half-time evidence for clink {args.candidate[:12]}: FAIL")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print(f"half-time evidence for clink {args.candidate[:12]}: PASS ({summary})")


if __name__ == "__main__":
    main()
