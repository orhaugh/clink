"""The bounded, resumable audit runner.

One cycle (`clink-audit run`, or one turn of `poll`):

1. Stop any orphan a crashed runner left, publish any audit whose result was
   reached but not recorded, and read the operator's acknowledgements.
2. Discover the job's completed checkpoints from its completion markers, the
   files the engine's restore rule reads, and select the ones to audit:
   every one in order (sequential), or the newest, recording every older one
   it passes over as skipped (latest-only).
3. Collect each selected checkpoint's bundle at once. A Worker keeps only its
   newest few checkpoints' state, so collection cannot wait for the audits
   before it; the bundle is self-contained, and the slower steps run from it.
4. Audit each collected checkpoint: its scope (offline), the operator's
   reference (reading Kafka), verification under network isolation (offline)
   and reconciliation against the destination (read-only, selected keys).
5. Publish each audit's result once, by rename, under an identity that does
   not change across restarts, and keep coverage, gaps and discrepancies.

Each step's result is recorded once its output is in place, so a restart
resumes at the step that was running. Unfinished work is never verified work:
coverage comes only from published results that passed.
"""

import hashlib
import json
import os
import re
import shutil
import time
from pathlib import Path

from . import proc
from .isolation import Isolation
from .state import (STATE_FORMAT, STATE_VERSION, StateDir, atomic_write_json, now_iso, read_json)

REPORT_FORMAT = "clink-audit-report"
REPORT_VERSION = 1
RUNNER_NAME = "clink-audit"
RUNNER_VERSION = "0.1.0"

_MARKER = re.compile(r"^(COMPLETED|CONFIRMED)-(0|[1-9][0-9]*)$")

# Outcomes. passed and empty are verified coverage; the rest are gaps, and a
# discrepancy also stays unresolved until an operator acknowledges it.
PASSED, EMPTY = "passed", "empty"
DISCREPANCY = "discrepancy"
PARTIAL, ERROR, UNSUPPORTED = "partial", "error", "unsupported"
EXPIRED, SKIPPED = "expired", "skipped"
VERIFIED = (PASSED, EMPTY)

RECENT_KEPT = 50
# How many completed checkpoints back a windowed bundle reaches, at most, for
# an increment to span a failed one.
REACH_BACK = 8


def fault(point):
    """A crash point for the tests, which replace this function. It does
    nothing in a runner."""


def completed_markers(checkpoint_dir, job, prefix="COMPLETED"):
    d = Path(checkpoint_dir) / "_jobs" / str(job)
    try:
        names = os.listdir(d)
    except FileNotFoundError:
        return None
    out = []
    for n in names:
        m = _MARKER.match(n)
        if m and m.group(1) == prefix:
            out.append(int(m.group(2)))
    return sorted(out)


def audit_id(cfg, checkpoint):
    basis = "%s|%d|%d|%s" % (cfg.checkpoint_dir, cfg.job_id, checkpoint, cfg.fingerprint)
    return "c%010d-%s" % (checkpoint, hashlib.sha256(basis.encode()).hexdigest()[:12])


def _last_line(path, limit=1 << 20):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - limit))
            lines = f.read().rstrip(b"\n").split(b"\n")
    except OSError:
        return None
    return lines[-1] if lines else None


def reference_end(path):
    """The artifact's end line, or None when it has none (unfinished)."""
    line = _last_line(path)
    if not line:
        return None
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    end = obj.get("end") if isinstance(obj, dict) else None
    return end if isinstance(end, dict) else None


def _merge(ranges):
    """Merges [from, to) ranges; from None is the run's start."""
    key = (lambda r: -1 if r[0] is None else r[0])
    out = []
    for lo, hi in sorted(ranges, key=key):
        if out and (lo is None or (out[-1][1] is not None and lo <= out[-1][1])):
            out[-1][1] = max(out[-1][1], hi)
            if lo is None:
                out[-1][0] = None
        else:
            out.append([lo, hi])
    return out


class Runner:
    def __init__(self, cfg, isolation=None, clock=time.time, interrupt=None, log=print):
        self.cfg = cfg
        self.dir = StateDir(cfg.state_dir)
        self.isolation = isolation
        self.clock = clock
        self.interrupt = interrupt  # returns a reason to stop now, or ""
        self.log = log
        self.st = None
        self.completed = []
        self.deadline = None
        self.cycle_stats = {}

    # -- state ---------------------------------------------------------------------------
    def _new_state(self):
        return {
            "format": STATE_FORMAT, "version": STATE_VERSION, "created_at": now_iso(),
            "runner": {"name": RUNNER_NAME, "version": RUNNER_VERSION},
            "job": {"id": self.cfg.job_id, "checkpoint_dir": str(self.cfg.checkpoint_dir)},
            "start": None, "config_fingerprint": self.cfg.fingerprint,
            "config_history": [{"fingerprint": self.cfg.fingerprint, "at": now_iso(),
                                "from_checkpoint": None}],
            "limits": self.cfg.limits, "selection": self.cfg.selection,
            "audits": {}, "handled_upto": 0, "handled_above": [],
            "highest_completed": 0, "coverage": {}, "verified_checkpoints": [],
            "gaps": [], "discrepancies": [], "recent": [],
            "last_verified": None, "last_attempted": None, "last_discrepancy": None,
            "graph": None, "graph_history": [], "commit_confirmation": None,
            "retained_passed": [], "passed_reports": [], "stopped": None,
            "kind": None, "window": {"states": {}, "broken": None},
            "last_cycle": None, "totals": {"audits": 0, "cpu_s": 0.0, "wall_s": 0.0,
                                           "reference_records": 0, "destination_queries": 0,
                                           "bundle_bytes": 0},
        }

    def save(self):
        self.dir.save(self.st)

    def load(self):
        st = self.dir.load()
        if st is None:
            st = self._new_state()
        job = st["job"]
        if job["id"] != self.cfg.job_id or job["checkpoint_dir"] != str(self.cfg.checkpoint_dir):
            raise ValueError("%s audits job %s at %s, not job %d at %s: use another state directory"
                             % (self.dir.path, job["id"], job["checkpoint_dir"], self.cfg.job_id,
                                self.cfg.checkpoint_dir))
        self.st = st
        st["limits"] = self.cfg.limits
        st["selection"] = self.cfg.selection
        st.setdefault("kind", None)
        st.setdefault("window", {"states": {}, "broken": None})

    # -- the cycle ----------------------------------------------------------------------------
    def cycle(self):
        t0 = time.monotonic()
        cpu0 = _cpu()
        self.deadline = time.monotonic() + self.cfg.limits["run_seconds"]
        self.cycle_stats = {"audits": 0, "published": 0, "collected": 0, "interrupted": ""}
        self.dir.ensure()
        self.load()
        self.dir.event("cycle-start", pid=os.getpid())
        self._stop_orphan()
        self._list()
        # A cycle audits what was eligible when it began; checkpoints that
        # complete while it runs are collected at once, so retention cannot
        # take them, and audited by the next cycle. A scheduler's run ends.
        self.cycle_limit = self.completed[-1] if self.completed else 0
        self._recover()
        self._config_change()
        self._advance_handled()
        self.save()
        if not self._reset():
            self._collect_new()
            # Each audit at most one attempt per cycle: a retry waits for a
            # later cycle once its backoff is spent, so attempts are spread
            # over time, and a dependency that is down costs one attempt per
            # audit per cycle, which bounds a scheduler's run.
            tried = set()
            while not self._out_of_time():
                todo = [cp for cp in self._runnable() if cp not in tried]
                if not todo:
                    break
                tried.add(todo[0])
                self._audit(todo[0])
                self._collect_new()
        wall = time.monotonic() - t0
        cpu = _cpu() - cpu0
        self.st["last_cycle"] = {"at": now_iso(), "wall_s": round(wall, 3), "cpu_s": round(cpu, 3),
                                 "disk_bytes": self.dir.usage(), **self.cycle_stats}
        self.save()
        self.dir.event("cycle-end", **self.st["last_cycle"])

    def _out_of_time(self):
        if self.interrupt is not None:
            why = self.interrupt()
            if why:
                self.cycle_stats["interrupted"] = why
                return True
        if time.monotonic() >= self.deadline:
            self.cycle_stats["interrupted"] = "the run's time (run_seconds) was spent"
            return True
        return False

    def _stop_reason(self):
        if self.interrupt is not None:
            why = self.interrupt()
            if why:
                return "interrupted: " + why
        if time.monotonic() >= self.deadline:
            return "interrupted: the run's time (run_seconds) was spent"
        return ""

    # -- recovery ------------------------------------------------------------------------------
    def _stop_orphan(self):
        rec = self.dir.inflight()
        if not rec:
            return
        pid = rec.get("pid")
        needle = rec.get("audit_dir", "")
        if pid and needle and proc.process_matches(pid, needle):
            proc.kill_group(int(pid))
            self.dir.event("orphan-stopped", pid=pid, step=rec.get("step"), audit=rec.get("audit"))
        self.dir.clear_inflight()

    def _recover(self):
        for cp_text, a in list(self.st["audits"].items()):
            report = self.dir.reports / ("%s.json" % a["id"])
            if report.is_file():
                # Published, and the crash came before the state recorded it.
                rep = read_json(report)
                if isinstance(rep, dict) and rep.get("id") == a["id"]:
                    self._record_published(a, rep)
                    self.dir.event("adopted", audit=a["id"], checkpoint=a["checkpoint"])
                    continue
            d = self.dir.audit_dir(a["id"])
            if d.is_dir():
                self._clear_leftovers(d)
                # A step recorded with its output gone is run again.
                for step, out in (("collect", "bundle/manifest.json"), ("scope", "scope.json")):
                    if step in a["steps"] and not (d / out).exists():
                        a["steps"].pop(step, None)
            else:
                a["steps"] = {}
        self.save()

    def _clear_leftovers(self, d):
        """What a stopped child or a crash left half-written in an audit's
        own directory: the collector's temporary bundle, a partial reference,
        a temporary file."""
        for p in d.iterdir():
            if p.is_symlink():
                continue
            if p.name.startswith(".bundle.incomplete-") and p.is_dir():
                shutil.rmtree(p)
            elif p.is_file() and (".tmp-" in p.name or p.name.endswith(".partial")):
                p.unlink(missing_ok=True)

    def _config_change(self):
        fp = self.cfg.fingerprint
        if self.st["config_fingerprint"] == fp:
            return
        old = self.st["config_fingerprint"]
        restarted = []
        for cp_text, a in list(self.st["audits"].items()):
            # Unfinished work under the old configuration has no result; it
            # starts again under the new one, with the new identity.
            self.dir.remove_audit_dir(a["id"])
            del self.st["audits"][cp_text]
            restarted.append(a["checkpoint"])
        self.st["config_fingerprint"] = fp
        self.st["config_history"].append({"fingerprint": fp, "at": now_iso(),
                                          "from_checkpoint": self.st["handled_upto"] + 1})
        self.dir.event("config-changed", old=old, new=fp, restarted=restarted)

    # -- discovery and selection --------------------------------------------------------------------
    def _marker_prefix(self):
        if self.cfg.commit_confirmation == "yes" or self.st.get("commit_confirmation") is True:
            return "CONFIRMED"
        return "COMPLETED"

    def _reset(self):
        return bool(self.st["stopped"]) and self.st["stopped"].get("reason") == "job-reset"

    def _list(self):
        found = completed_markers(self.cfg.checkpoint_dir, self.cfg.job_id, self._marker_prefix())
        self.completed = found or []
        if found is None:
            if not self.cycle_stats.get("no_markers"):
                self.cycle_stats["no_markers"] = True
                self.dir.event("no-markers", checkpoint_dir=str(self.cfg.checkpoint_dir))
            return
        if not found:
            return
        newest = found[-1]
        if newest < self.st["highest_completed"]:
            # Checkpoint ids only grow within a job: this is another job, or
            # its directory was cleared. Nothing is audited against the old
            # record; the operator starts a new state directory.
            self.st["stopped"] = {"reason": "job-reset", "at": now_iso(),
                                  "detail": "the newest completed checkpoint is %d, below %d, which "
                                            "this state directory saw: the job's checkpoint "
                                            "directory was replaced or cleared" % (newest, self.st["highest_completed"])}
            self.dir.event("job-reset", newest=newest, seen=self.st["highest_completed"])
            self.save()
            return
        self.st["highest_completed"] = newest
        if self.st["start"] is None:
            start_at = self.cfg.start_at
            if start_at == "earliest":
                start = found[0]
            elif start_at == "latest":
                start = newest
            else:
                start = int(start_at)
            self.st["start"] = start
            self.st["handled_upto"] = max(0, start - 1)
            before = [c for c in found if c < start]
            if before:
                self._gap(before[0], before[-1], "before-start",
                          "completed before this state directory began auditing (start_at %s)" % start_at,
                          count=len(before))
            self.dir.event("started", start=start, start_at=str(start_at))
        self.save()

    def _eligible(self):
        return [c for c in self.completed if self.st["start"] is not None and c >= self.st["start"]]

    def _pending(self):
        handled = set(self.st["handled_above"])
        return [c for c in self._eligible() if c > self.st["handled_upto"] and c not in handled
                and str(c) not in self.st["audits"]]

    def _skip_older(self):
        pending = self._pending()
        if len(pending) < 2:
            return
        newest = pending[-1]
        for c in pending[:-1]:
            a = self._new_audit(c, create_dir=False)
            a["outcome"] = SKIPPED
            a["reason"] = "passed over by the latest-only selection, which audited checkpoint %d" % newest
            self._publish(a)

    def _runnable(self):
        now = self.clock()
        out = []
        for cp_text in sorted(self.st["audits"], key=int):
            a = self.st["audits"][cp_text]
            if a.get("next_attempt_at") and a["next_attempt_at"] > now:
                continue
            if int(cp_text) > getattr(self, "cycle_limit", 0):
                continue
            out.append(int(cp_text))
        return out

    # -- collection, promptly ------------------------------------------------------------------------
    def _collect_new(self):
        if self._reset():
            return
        self._list()
        if self._reset():
            return
        if self.cfg.selection == "latest-only":
            self._skip_older()
            pending = self._pending()[-1:]
        else:
            pending = self._pending()
        for c in pending:
            if self._out_of_time():
                return
            waiting = sum(1 for a in self.st["audits"].values() if "collect" in a["steps"])
            if waiting >= self.cfg.limits["max_pending"]:
                self._note_backlog("max_pending (%d) collected bundles are waiting for their audits"
                                   % self.cfg.limits["max_pending"])
                return
            if not self._disk_ok():
                return
            a = self._new_audit(c)
            self.st["audits"][str(c)] = a
            self.save()
            self._step_collect(a)
            if a.get("outcome"):
                self._publish(a)
            elif self.st["kind"] is None and "collect" in a["steps"]:
                # Whether the job is windowed decides how far back the next
                # bundles must reach, so learn it from the first one now.
                self._step_scope(a)
                if a.get("outcome"):
                    self._publish(a)

    def _note_backlog(self, why):
        if self.st.get("backlog_note") != why:
            self.st["backlog_note"] = why
            self.dir.event("backlog", reason=why)

    def _disk_ok(self):
        lim = self.cfg.limits
        used = self.dir.usage()
        free = shutil.disk_usage(self.dir.path).free
        need = lim["bundle_bytes"]
        if used + need <= lim["disk_bytes"] and free - need >= lim["min_free_bytes"]:
            if self.st["stopped"] and self.st["stopped"].get("reason") == "disk":
                self.dir.event("resumed", reason="disk")
                self.st["stopped"] = None
            return True
        detail = ("the state directory holds %d bytes of %d (disk_bytes), and %d are free (min_free_bytes "
                  "%d); a bundle may take %d. The evidence of audits that did not pass is kept, so no "
                  "new audit starts until there is room" % (used, lim["disk_bytes"], free,
                                                            lim["min_free_bytes"], need))
        if not self.st["stopped"]:
            self.dir.event("stopped", reason="disk", detail=detail)
        self.st["stopped"] = {"reason": "disk", "at": now_iso(), "detail": detail}
        self.save()
        return False

    def _new_audit(self, checkpoint, create_dir=True):
        aid = audit_id(self.cfg, checkpoint)
        if create_dir:
            self.dir.audit_dir(aid, create=True)
        return {"id": aid, "checkpoint": checkpoint, "config": self.cfg.fingerprint,
                "created_at": now_iso(), "attempt": 1, "attempts": [], "steps": {},
                "next_attempt_at": None, "outcome": None, "reason": ""}

    def _previous_completed(self, checkpoint):
        earlier = [c for c in self.completed if c < checkpoint]
        return earlier[-1] if earlier else 0

    # -- steps ---------------------------------------------------------------------------------------
    def _run(self, a, step, argv, timeout_s, env_names=(), watch=None, extra_env=None):
        d = self.dir.audit_dir(a["id"])
        remaining = max(1.0, self.deadline - time.monotonic())

        def started(pid):
            self.dir.set_inflight({"pid": pid, "step": step, "audit": a["id"],
                                   "audit_dir": str(d), "at": now_iso()})
            if step == "reference":
                fault("during-reference")

        r = proc.run(argv, log_prefix=d / step, timeout_s=min(timeout_s, remaining + 1),
                     env_names=env_names, secrets=self.cfg.secret_env, on_start=started,
                     on_end=self.dir.clear_inflight, watch=watch, should_stop=self._stop_reason,
                     cwd=str(d), extra_env=extra_env)
        commands = read_json(d / "commands.json", [])
        commands.append({"step": step, "attempt": a["attempt"], "at": now_iso(), "argv": r.argv,
                         "env": r.env_names, "result": r.summary, "seconds": round(r.seconds, 3),
                         "cpu_s": round(r.cpu_s, 3)})
        atomic_write_json(d / "commands.json", commands)
        return r

    @staticmethod
    def _rec(r, **extra):
        rec = {"exit": r.exit, "seconds": round(r.seconds, 3), "cpu_s": round(r.cpu_s, 3),
               "result": r.summary, "at": now_iso()}
        rec.update(extra)
        return rec

    def _step_collect(self, a):
        d = self.dir.audit_dir(a["id"])
        bundle = d / "bundle"
        # A bundle the collector published before a crash let the state
        # record it is the same evidence: adopt it.
        manifest = read_json(bundle / "manifest.json")
        if manifest is not None and manifest.get("selection", {}).get("checkpoint") == a["checkpoint"]:
            a["steps"]["collect"] = {"exit": 0, "adopted": True, "at": now_iso(), "seconds": 0, "cpu_s": 0}
            self._after_collect(a, manifest)
            return
        if bundle.exists() and not bundle.is_symlink():
            shutil.rmtree(bundle)
        self._clear_leftovers(d)
        prev = self._previous_completed(a["checkpoint"])
        lo = prev + 1
        a["start"] = prev
        if self.st["kind"] == "windowed":
            # An increment starts at the newest state the runner holds when it
            # is audited, which is usually this checkpoint's predecessor's,
            # held by then though not yet now: the bundle reaches back to the
            # newest held, so a failed increment can be spanned, by at most
            # REACH_BACK completed checkpoints, and with none held yet to the
            # job's start, for the seed. Each increment reads its base's own
            # interval evidence, the watermark recorded there.
            held = [c for c in self._held_states() if c < a["checkpoint"]]
            before = [c for c in self.completed if c < a["checkpoint"]]
            floor = before[-REACH_BACK] if len(before) >= REACH_BACK else 1
            base = max(held) if held else 0
            lo = max(1, base, floor) if held else 1
            lo = min(lo, prev) if prev else lo
            a["start"] = None  # decided by the scope, from what is held then
        lo = max(1, min(lo, a["checkpoint"]))
        a["lo"] = lo
        argv = [self.cfg.clink, "incident-collect", "--checkpoint-dir=%s" % self.cfg.checkpoint_dir,
                "--job=%d" % self.cfg.job_id, "--checkpoint=%d" % a["checkpoint"],
                "--capture-dir=%s" % self.cfg.capture_dir, "--include-state", "--include-captures",
                "--epochs=%d-%d" % (lo, a["checkpoint"]), "--out=%s" % bundle,
                "--max-bytes=%d" % self.cfg.limits["bundle_bytes"],
                "--max-files=%d" % self.cfg.limits["bundle_files"],
                "--timeout-s=%d" % max(1, int(self.cfg.limits["collect_timeout_s"])), "--json"]
        if self.cfg.ha_dir:
            argv.append("--ha-dir=%s" % self.cfg.ha_dir)
        if self.cfg.commit_confirmation:
            argv.append("--commit-confirmation=%s" % self.cfg.commit_confirmation)
        tries = 0
        while True:
            tries += 1
            r = self._run(a, "collect", argv, self.cfg.limits["collect_timeout_s"] + 30)
            if r.stopped.startswith("interrupted"):
                return
            out = _json_line(r.stdout)
            if r.exit == 0 and out and out.get("bundle"):
                manifest = read_json(bundle / "manifest.json") or {}
                # A windowed job's run that restored from a checkpoint before
                # the bundle reaches starts its next increment there: collect
                # again from it, when the reference's state there is held (the
                # restore point's own state the Workers keep).
                rest = (manifest.get("selection") or {}).get("restored") or {}
                back = rest.get("restore")
                newest = max((c for c in self._held_states() if c < a["checkpoint"]), default=0)
                # Only when no held state is of the restored run already: once
                # one is, the chain runs on from it.
                if (self.st["kind"] == "windowed" and isinstance(back, int) and 0 < back < lo
                        and int(rest.get("next") or 0) <= a["checkpoint"]
                        and newest < int(rest.get("next") or 0) and newest != back
                        and str(back) in self.st["window"]["states"] and not a.get("reached_back")):
                    a["reached_back"] = back
                    a["start"] = back
                    lo = back
                    a["lo"] = lo
                    shutil.rmtree(bundle)
                    argv = [x if not str(x).startswith("--epochs=") else
                            "--epochs=%d-%d" % (lo, a["checkpoint"]) for x in argv]
                    self.dir.event("collect-reach-back", audit=a["id"], checkpoint=a["checkpoint"],
                                   restored_from=back)
                    continue
                a["steps"]["collect"] = self._rec(r, bytes=out.get("bytes"), files=out.get("files"),
                                                  tries=tries)
                self.cycle_stats["collected"] += 1
                self.st["totals"]["bundle_bytes"] += int(out.get("bytes") or 0)
                self.save()
                fault("after-collect")
                self._after_collect(a, manifest)
                return
            kind = (out or {}).get("kind") if r.exit == 1 else None
            reason = (out or {}).get("reason") or r.stderr.strip()[-500:] or r.summary
            if kind == "changed" and tries <= self.cfg.limits["collect_retries"] and not self._out_of_time():
                time.sleep(min(1.0, 0.2 * tries))
                continue
            if kind == "missing":
                self._finish(a, EXPIRED, "collection: %s" % reason)
            elif kind == "bound":
                self._finish(a, PARTIAL, "collection: the bundle passed its bound: %s" % reason,
                             final=True)
            else:
                self._fail_attempt(a, ERROR, "collection: %s" % (reason if r.exit is not None else r.summary))
            return

    def _after_collect(self, a, manifest):
        sel = manifest.get("selection", {})
        job = manifest.get("job", {})
        if isinstance(job.get("commit_confirmation"), bool):
            if self.st["commit_confirmation"] != job["commit_confirmation"]:
                self.st["commit_confirmation"] = job["commit_confirmation"]
        a["identity"] = {"generation": sel.get("generation"), "restored": sel.get("restored"),
                         "engine_choice": sel.get("engine_choice"),
                         "collector": manifest.get("collector", {})}
        # A checkpoint whose state the Workers already purged: the bundle has
        # its marker and none of what its scope's positions are read from. Only
        # the checkpoints the scope reads count: the audited one and the one it
        # starts from; an older one a reach-back collected beside them may be
        # gone without consequence.
        # A windowed audit's start is decided when it is audited; only the
        # checkpoint's own state counts here.
        needed = {a["checkpoint"]} if a.get("start") is None else {
            a["checkpoint"], a.get("start", self._previous_completed(a["checkpoint"]))}
        gone = [x for x in manifest.get("absent", [])
                if x.get("role") in ("snapshot_meta", "interval_state")
                and "not present in the job's state directory" in x.get("reason", "")
                and _checkpoint_of(x.get("path", "")) in needed]
        if gone:
            self._finish(a, EXPIRED, "the checkpoint's state was purged before it was collected "
                         "(%d file(s) absent, such as %s); the Workers keep only their newest "
                         "checkpoints (--checkpoint-num-retained)" % (len(gone), gone[0].get("path")))
        self.save()

    def _audit(self, checkpoint):
        a = self.st["audits"].get(str(checkpoint))
        if a is None or a.get("outcome"):
            return
        self.cycle_stats["audits"] += 1
        if "collect" not in a["steps"]:
            if not self._disk_ok():
                return
            self._step_collect(a)
            if a.get("outcome") or "collect" not in a["steps"]:
                if a.get("outcome"):
                    self._publish(a)
                return
        self.st["last_attempted"] = {"audit": a["id"], "checkpoint": checkpoint, "at": now_iso(),
                                     "attempt": a["attempt"]}
        for step in ("scope", "reference", "verify", "reconcile"):
            if step in a["steps"]:
                continue
            if self._stop_reason():
                return
            getattr(self, "_step_" + step)(a)
            if a.get("outcome"):
                break
            if step not in a["steps"]:
                return  # interrupted: resumes at this step
            self.save()
            # Between steps too: a checkpoint completing now is collected
            # before the Workers' retention can purge what its scope reads.
            self._collect_new()
        if not a.get("outcome"):
            self._decide(a)
        if a.get("outcome"):
            self._publish(a)
        else:
            self.save()

    def _step_scope(self, a):
        d = self.dir.audit_dir(a["id"])
        out = d / "scope.json"
        out.unlink(missing_ok=True)
        base = ["--checkpoint=%d" % a["checkpoint"], "--job=%d" % self.cfg.job_id, "--out=%s" % out]
        if self.st["kind"] == "windowed" and any(c < a["checkpoint"] for c in self._held_states()):
            self._step_window_scope(a, base, None, None)
            return
        r = self._run(a, "scope", [self.cfg.clink, "incident-scope", str(d / "bundle")] + base,
                      self.cfg.limits["scope_timeout_s"])
        if r.stopped.startswith("interrupted"):
            return
        doc = read_json(out) if out.exists() else None
        windowed = isinstance(doc, dict) and doc.get("transform") is not None
        if r.exit == 0 and windowed or (r.exit == 3 and windowed):
            self._learn_kind("windowed")
            self._step_window_scope(a, base, r, doc)
            return
        if r.exit == 0 and isinstance(doc, dict):
            if doc.get("version") != 1:
                self._finish(a, UNSUPPORTED, "the job's scope is version %s, which this runner "
                             "does not read" % doc.get("version"))
                return
            self._learn_kind("record-wise")
            a["steps"]["scope"] = self._rec(r, scope_sha256=doc.get("scope_sha256"))
            self._note_graph(a, doc)
            return
        if r.exit == 3:
            if isinstance(doc, dict):
                self._finish(a, PARTIAL, "the scope's evidence or source positions are not whole: %s"
                             % _first_line(r.stderr or r.stdout), final=True)
            else:
                self._finish(a, UNSUPPORTED, "incident-scope: %s" % _first_line(r.stderr or r.stdout))
            return
        self._fail_attempt(a, ERROR, "incident-scope: %s: %s" % (r.summary, _first_line(r.stderr)))

    def _note_graph(self, a, doc):
        sha = doc.get("graph_sha256")
        if sha and sha != self.st["graph"]:
            if self.st["graph"] is not None:
                self.dir.event("job-changed", old=self.st["graph"], new=sha, checkpoint=a["checkpoint"])
            self.st["graph_history"].append({"graph_sha256": sha, "from_checkpoint": a["checkpoint"],
                                             "at": now_iso()})
            self.st["graph"] = sha

    def _learn_kind(self, kind):
        if self.st["kind"] != kind:
            if self.st["kind"] is not None:
                self.dir.event("job-kind-changed", old=self.st["kind"], new=kind)
            self.st["kind"] = kind

    def _held_states(self):
        return sorted(int(c) for c in self.st["window"]["states"])

    def _state_path(self, checkpoint):
        return self.dir.path / "states" / ("%d.json" % checkpoint)

    def _step_window_scope(self, a, args, seed_run, seed_doc):
        """A windowed job's scope: an increment from the newest state the
        runner holds on this checkpoint's lineage, or, with none held, the
        whole lineage from the job's start, which seeds the chain. A held
        state off the lineage (a restore went back past it) is refused by
        incident-scope, and the next older one is tried."""
        d = self.dir.audit_dir(a["id"])
        out = d / "scope.json"
        held = [c for c in self._held_states() if c < a["checkpoint"]]
        tried = []
        for m in sorted(held, reverse=True)[:8]:
            out.unlink(missing_ok=True)
            r = self._run(a, "scope", [self.cfg.clink, "incident-scope", str(d / "bundle"),
                                       "--since=%d" % m] + args, self.cfg.limits["scope_timeout_s"])
            if r.stopped.startswith("interrupted"):
                return
            doc = read_json(out) if out.exists() else None
            if r.exit == 0 and isinstance(doc, dict) and doc.get("version") == 3:
                a["since"] = m
                a["steps"]["scope"] = self._rec(r, scope_sha256=doc.get("scope_sha256"), since=m)
                self._note_graph(a, doc)
                return
            purged = [x.get("path") for x in (read_json(d / "bundle" / "manifest.json") or {}).get("absent", [])
                      if x.get("role") in ("snapshot_meta", "interval_state")
                      and _checkpoint_of(x.get("path", "")) == m]
            if r.exit == 3 and m < int(a.get("lo") or 1):
                tried.append("the state at checkpoint %d is further back than the bundle for checkpoint "
                             "%d reaches (from %d): the runner fell behind" % (m, a["checkpoint"], a["lo"]))
            elif r.exit == 3 and purged:
                tried.append("the Workers purged checkpoint %d, where that state is, before checkpoint "
                             "%d was collected (%s absent): the runner fell behind their retention"
                             % (m, a["checkpoint"], purged[0]))
            else:
                tried.append("from checkpoint %d: %s" % (m, _first_line(r.stderr) or r.summary))
            if r.exit != 2:
                break  # on the lineage, and the bundle cannot give it: no older one helps
        if not held:
            # The seed: the job's whole lineage, from a start with no state.
            if (seed_run is not None and seed_run.exit == 0 and isinstance(seed_doc, dict)
                    and seed_doc.get("version") == 2):
                a["since"] = None
                a["steps"]["scope"] = self._rec(seed_run, scope_sha256=seed_doc.get("scope_sha256"),
                                                since=None)
                self._note_graph(a, seed_doc)
                return
            why = ("no state of the reference's is held yet, and the job's whole lineage is not in "
                   "the bundle (%s): a windowed job is audited from its first checkpoint on, with "
                   "start_at earliest" % ((_first_line(seed_run.stderr) or seed_run.summary)
                                          if seed_run is not None else "not read"))
        else:
            why = ("no state the runner holds starts an increment to checkpoint %d (%s)"
                   % (a["checkpoint"], "; ".join(tried) or "none tried"))
        if self.st["window"].get("broken") is None:
            self.st["window"]["broken"] = {"at": a["checkpoint"], "why": why[:500], "since": now_iso()}
            self.dir.event("chain-broken", checkpoint=a["checkpoint"], reason=why[:500])
        self._finish(a, PARTIAL, "the reference's window state chain is broken: " + why)

    def _step_reference(self, a):
        d = self.dir.audit_dir(a["id"])
        scope = read_json(d / "scope.json") or {}
        final = d / "reference.ndjson"
        partial = d / "reference.ndjson.partial"
        final.unlink(missing_ok=True)
        partial.unlink(missing_ok=True)
        lim = self.cfg.limits
        values = {"scope": str(d / "scope.json"), "out": str(partial),
                  "max_records": str(int(lim["reference_records"])),
                  "max_bytes": str(int(lim["reference_bytes"])),
                  "deadline_s": str(int(lim["reference_timeout_s"]))}
        state_out = d / "state_out.json"
        state_out.unlink(missing_ok=True)
        windowed = scope.get("transform") is not None
        if windowed:
            values["state_out"] = str(state_out)
            if a.get("since"):
                values["state_in"] = str(self._state_path(a["since"]))
        argv = [_fill(arg, values) for arg in self.cfg.reference_command]
        if any(p.get("from") is None for s in scope.get("sources", []) for p in s.get("partitions", [])):
            argv += list(self.cfg.reference_when_run_start)
        if windowed and a.get("since"):
            argv += [_fill(arg, values) for arg in self.cfg.reference_with_state_in]
        if windowed and "{state_out}" not in " ".join(self.cfg.reference_command):
            self._finish(a, UNSUPPORTED, "the job is windowed, and the reference command takes no "
                         "{state_out}, so no increment could follow this audit")
            return
        r = self._run(a, "reference", argv, lim["reference_timeout_s"] + 30,
                      env_names=self.cfg.reference_env, watch=(partial, lim["reference_bytes"]))
        if r.stopped.startswith("interrupted"):
            return
        end = reference_end(partial) if partial.exists() else None
        rec = self._rec(r)
        if r.exit in (0, 3) and end is not None:
            records = sum(int(x.get("records") or 0) for x in end.get("read", []) if isinstance(x, dict))
            rec.update(rows=end.get("rows"), complete=bool(end.get("complete")), records=records,
                       artifact_bytes=partial.stat().st_size)
            bound = ""
            if int(end.get("rows") or 0) > lim["reference_rows"]:
                bound = "the reference holds %s rows, past its bound reference_rows (%d)" % (end.get("rows"), lim["reference_rows"])
            elif records > lim["reference_records"]:
                bound = "the reference read %d records, past its bound reference_records (%d)" % (records, lim["reference_records"])
            chain = self._check_chain(a, partial, end, state_out) if windowed else ""
            if bound:
                rec.update(usable=False, why=bound)
            elif chain:
                rec.update(usable=False, why=chain)
            else:
                os.replace(partial, final)
                rec.update(usable=True, why="" if end.get("complete") else
                           "the reference is incomplete by its own account: " + "; ".join(end.get("incomplete", [])))
            self.st["totals"]["reference_records"] += records
        else:
            why = r.stopped or ("exit %s: %s" % (r.exit, _first_line(r.stderr) or "no end line in its output")
                                if end is None or r.exit not in (0, 3) else r.summary)
            if r.exit in (0, 3) and end is None:
                why = "its output has no end line, so it is unfinished or malformed"
            rec.update(usable=False, why="the reference did not produce an artifact: " + why)
        a["steps"]["reference"] = rec

    def _check_chain(self, a, artifact, end, state_out):
        """A windowed reference must say it started from the state the runner
        gave it, and, when complete, leave one that hashes to what it says;
        that state is then held for the next increment."""
        header = _first_json_line(artifact) or {}
        if a.get("since"):
            want = self.st["window"]["states"].get(str(a["since"]))
            got = (header.get("state_in") or {}).get("sha256")
            if got != want:
                return ("the reference says it started from window state %s, and the runner gave "
                        "it %s (the state at checkpoint %d)" % (got, want, a["since"]))
        if not end.get("complete"):
            return ""
        out = end.get("state_out") or {}
        if out.get("checkpoint") != a["checkpoint"] or not state_out.is_file():
            return "the reference left no window state at checkpoint %d" % a["checkpoint"]
        import hashlib
        sha = hashlib.sha256(state_out.read_bytes()).hexdigest()
        if sha != out.get("sha256"):
            return ("the window state the reference left hashes to %s, not the %s its artifact "
                    "names" % (sha, out.get("sha256")))
        self._hold_state(a["checkpoint"], state_out, sha)
        return ""

    def _hold_state(self, checkpoint, path, sha):
        dst = self._state_path(checkpoint)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(".%s.tmp-%d" % (dst.name, os.getpid()))
        shutil.copyfile(path, tmp)
        os.replace(tmp, dst)
        states = self.st["window"]["states"]
        states[str(checkpoint)] = sha
        keep = int(self.cfg.limits.get("retain_states", 50))
        for c in sorted(int(x) for x in states)[:-keep] if len(states) > keep else []:
            states.pop(str(c), None)
            old = self._state_path(c)
            if old.is_file() and not old.is_symlink() and old.parent == dst.parent:
                old.unlink()
        if self.st["window"].get("broken"):
            self.dir.event("chain-resumed", checkpoint=checkpoint)
            self.st["window"]["broken"] = None
        self.save()

    def _step_verify(self, a):
        d = self.dir.audit_dir(a["id"])
        if self.isolation is None:
            self.isolation = Isolation()
        if not self.isolation.state:
            self.isolation.establish()
            self.dir.event("isolation", state=self.isolation.state, detail=self.isolation.detail)
        if not self.isolation.enforced:
            a["steps"]["verify"] = {"exit": None, "at": now_iso(), "seconds": 0, "cpu_s": 0,
                                    "isolation": self.isolation.state,
                                    "why": "verification did not run: network isolation is %s: %s"
                                           % (self.isolation.state, self.isolation.detail)}
            return
        report = d / "verify.json"
        report.unlink(missing_ok=True)
        argv = self.isolation.wrap([self.cfg.clink, "incident-verify", str(d / "bundle"),
                                    "--profile=%s" % self.cfg.verify_profile, "--json",
                                    "--report=%s" % report])
        r = self._run(a, "verify", argv, self.cfg.limits["verify_timeout_s"])
        if r.stopped.startswith("interrupted"):
            return
        a["steps"]["verify"] = self._rec(r, isolation=self.isolation.state,
                                         profile=self.cfg.verify_profile)

    def _step_reconcile(self, a):
        d = self.dir.audit_dir(a["id"])
        report = d / "reconcile.json"
        report.unlink(missing_ok=True)
        dest = self.cfg.destination
        lim = self.cfg.limits
        argv = [self.cfg.clink, "incident-reconcile", str(d / "bundle"),
                "--checkpoint=%d" % a["checkpoint"], "--job=%d" % self.cfg.job_id,
                "--host=%s" % dest["host"], "--key=%s" % ",".join(self.cfg.key),
                "--query-timeout-ms=%d" % lim["query_timeout_ms"],
                "--max-rows-per-query=%d" % lim["max_rows_per_query"],
                "--keys-per-query=%d" % lim["keys_per_query"], "--list=20",
                "--report=%s" % report]
        for k, flag in (("port", "--port=%s"), ("user", "--user=%s"), ("password_env", "--password-env=%s"),
                        ("tls_ca_file", "--tls-ca-file=%s"), ("database", "--database=%s"),
                        ("table", "--table=%s")):
            if dest.get(k) not in (None, ""):
                argv.append(flag % dest[k])
        if dest.get("secure"):
            argv.append("--secure")
        if "tls_verify" in dest:
            argv.append("--tls-verify=%s" % ("true" if dest["tls_verify"] else "false"))
        ref = a["steps"].get("reference", {})
        if ref.get("usable"):
            argv += ["--reference=%s" % (d / "reference.ndjson"),
                     "--max-reference-bytes=%d" % lim["reference_bytes"]]
        if a.get("since"):
            argv.append("--since=%d" % a["since"])
        env = [dest["password_env"]] if dest.get("password_env") else []
        r = self._run(a, "reconcile", argv, lim["reconcile_timeout_s"], env_names=env)
        if r.stopped.startswith("interrupted"):
            return
        rep = read_json(report) if report.exists() else None
        rec = self._rec(r, with_reference=bool(ref.get("usable")))
        if isinstance(rep, dict):
            queries = 0
            for c in rep.get("checks", []):
                queries += int((c.get("counts") or {}).get("queries") or 0)
            rec.update(queries=queries, expected_rows=(rep.get("scope") or {}).get("expected_rows"),
                       keys_selected=(rep.get("coverage") or {}).get("keys_selected"))
            self.st["totals"]["destination_queries"] += queries
        a["steps"]["reconcile"] = rec

    # -- deciding ------------------------------------------------------------------------------------
    def _decide(self, a):
        d = self.dir.audit_dir(a["id"])
        ref = a["steps"].get("reference", {})
        ver = a["steps"].get("verify", {})
        rc = a["steps"].get("reconcile", {})
        rep = read_json(d / "reconcile.json") if (d / "reconcile.json").exists() else None
        vrep = read_json(d / "verify.json") if (d / "verify.json").exists() else None
        findings = []
        if isinstance(rep, dict):
            findings += (rep.get("audit") or {}).get("findings", [])
            if not (rep.get("audit") or {}).get("findings") and rep.get("exit_code") == 1:
                for c in rep.get("checks", []):
                    if c.get("verdict") == "failed":
                        findings.append({"relationship": "delivery" if c["id"].startswith("reconcile")
                                         else c["id"], "summary": c.get("reason", "")})
        if ver.get("exit") == 1 and isinstance(vrep, dict):
            for c in vrep.get("checks", []):
                if c.get("verdict") == "failed":
                    findings.append({"relationship": "evidence", "summary": "%s: %s" % (c["id"], c.get("reason", ""))})
        a["findings"] = findings
        reasons = []
        if not ref.get("usable") or ref.get("why"):
            reasons.append(ref.get("why") or "the reference was not usable")
        if ver.get("exit") != 0:
            reasons.append(ver.get("why") or "verification: %s" % ver.get("result"))
        if rc.get("exit") == 1 or ver.get("exit") == 1:
            self._finish(a, DISCREPANCY, "; ".join(f["summary"] for f in findings) or "a check failed")
            return
        if rc.get("exit") == 2:
            self._fail_attempt(a, ERROR, "incident-reconcile refused: %s" % _first_line(self._err(a, "reconcile")))
            return
        if rc.get("exit") not in (0, 3):
            self._fail_attempt(a, ERROR, "incident-reconcile: %s" % rc.get("result"))
            return
        if rc.get("exit") == 0 and ver.get("exit") == 0 and ref.get("usable") and ref.get("complete"):
            self._finish(a, PASSED, "every required check passed, for the %s selected key(s)"
                         % rc.get("keys_selected"))
            return
        if rc.get("exit") == 3 and ver.get("exit") == 0 and _empty(rep, ref):
            self._finish(a, EMPTY, "the scope's source range held no record the job turns into a row: "
                         "the reference computed none and the sink received none")
            return
        if isinstance(rep, dict):
            for c in rep.get("checks", []):
                if c.get("verdict") in ("unavailable", "unsupported"):
                    reasons.append("%s %s: %s" % (c["id"], c["verdict"], c.get("reason", "")))
        final = ver.get("isolation") not in (None, "enforced")
        self._fail_attempt(a, PARTIAL, "; ".join(r for r in reasons if r) or "a required check did not complete",
                           final=final)

    def _err(self, a, step):
        try:
            return (self.dir.audit_dir(a["id"]) / ("%s.err" % step)).read_text(errors="replace")
        except OSError:
            return ""

    def _finish(self, a, outcome, reason, final=True):
        a["outcome"] = outcome
        a["reason"] = reason
        a["next_attempt_at"] = None

    def _fail_attempt(self, a, outcome, reason, final=False):
        """A result that may change on another attempt (a dependency down, a
        time limit) is retried up to max_attempts, after the backoff; the
        failed attempt's account is kept and published with the result."""
        lim = self.cfg.limits
        a["attempts"].append({"attempt": a["attempt"], "outcome": outcome, "reason": reason,
                              "at": now_iso(), "steps": {k: {"exit": v.get("exit"), "result": v.get("result")}
                                                         for k, v in a["steps"].items()}})
        if final or a["attempt"] >= lim["max_attempts"]:
            self._finish(a, outcome, reason)
            return
        backoff = lim["retry_backoff_s"][min(a["attempt"] - 1, len(lim["retry_backoff_s"]) - 1)]
        a["attempt"] += 1
        a["next_attempt_at"] = self.clock() + backoff
        # Run again every step that did not complete, and the reconciliation
        # whenever the reference is run again, since it reads the reference.
        keep = {}
        for k in ("collect", "scope"):
            if k in a["steps"]:
                keep[k] = a["steps"][k]
        if a["steps"].get("reference", {}).get("usable") and a["steps"]["reference"].get("complete"):
            keep["reference"] = a["steps"]["reference"]
            if a["steps"].get("verify", {}).get("exit") == 0:
                keep["verify"] = a["steps"]["verify"]
        a["steps"] = keep
        self.dir.event("retry", audit=a["id"], checkpoint=a["checkpoint"], attempt=a["attempt"],
                       outcome=outcome, reason=reason[:500], after_s=backoff)
        self.save()

    # -- publishing -----------------------------------------------------------------------------------
    def _publish(self, a):
        d = self.dir.audit_dir(a["id"])
        scope = read_json(d / "scope.json") if (d / "scope.json").exists() else None
        rep = read_json(d / "reconcile.json") if (d / "reconcile.json").exists() else None
        ref_header = _first_json_line(d / "reference.ndjson")
        steps = a["steps"]
        costs = {"cpu_s": round(sum(float(s.get("cpu_s") or 0) for s in steps.values()), 3),
                 "wall_s": round(sum(float(s.get("seconds") or 0) for s in steps.values()), 3),
                 "bundle_bytes": steps.get("collect", {}).get("bytes"),
                 "bundle_files": steps.get("collect", {}).get("files"),
                 "reference_records": steps.get("reference", {}).get("records"),
                 "reference_rows": steps.get("reference", {}).get("rows"),
                 "reference_bytes": steps.get("reference", {}).get("artifact_bytes"),
                 "destination_queries": steps.get("reconcile", {}).get("queries")}
        report = {
            "format": REPORT_FORMAT, "version": REPORT_VERSION, "id": a["id"],
            "checkpoint": a["checkpoint"], "outcome": a["outcome"], "reason": a["reason"],
            "findings": a.get("findings", []), "attempt": a["attempt"], "attempts": a["attempts"],
            "published_at": now_iso(),
            "identity": {
                "job": self.cfg.job_id, "checkpoint_dir": str(self.cfg.checkpoint_dir),
                "config_fingerprint": a["config"],
                "graph_sha256": (scope or {}).get("graph_sha256"),
                "scope_sha256": (scope or {}).get("scope_sha256"),
                "sink_op": (scope or {}).get("sink_op"),
                "sink_attempts": (scope or {}).get("attempts"),
                **a.get("identity", {}),
                "reference": {
                    "command_sha256": hashlib.sha256(json.dumps(self.cfg.reference_command).encode()).hexdigest(),
                    "implementation": (ref_header or {}).get("implementation"),
                },
                "runner": {"name": RUNNER_NAME, "version": RUNNER_VERSION},
                "window": None if not (scope or {}).get("transform") else {
                    "since": a.get("since"),
                    "state_in": (ref_header or {}).get("state_in"),
                    "state_out": self.st["window"]["states"].get(str(a["checkpoint"])),
                },
            },
            "scope": None if scope is None else {
                "starts_after": scope.get("starts_after"),
                "starts_at_run_start": scope.get("starts_at_run_start"),
                "sources": [{"topic": s.get("topic"), "cluster_id": s.get("cluster_id"),
                             "topic_id": s.get("topic_id"), "same_log": s.get("same_log"),
                             "partitions": s.get("partitions")} for s in scope.get("sources", [])],
                "expected_rows": ((rep or {}).get("scope") or {}).get("expected_rows"),
            },
            "coverage": {"mode": "selected-keys",
                         "keys_selected": ((rep or {}).get("coverage") or {}).get("keys_selected"),
                         "note": "the destination was read for the scope's keys; rows of other keys "
                                 "are not established"},
            "steps": steps, "costs": costs,
            "evidence": {"dir": "audits/%s" % a["id"] if d.is_dir() else None,
                         "reconcile_report": "audits/%s/reconcile.json" % a["id"] if rep else None,
                         "verify_report": "audits/%s/verify.json" % a["id"]
                         if (d / "verify.json").exists() else None},
        }
        fault("before-publish")
        atomic_write_json(self.dir.reports / ("%s.json" % a["id"]), report)
        fault("after-report")
        self._record_published(a, report)
        self.cycle_stats["published"] = self.cycle_stats.get("published", 0) + 1

    def _record_published(self, a, report):
        """Brings the state up to a published report; doing it twice changes
        nothing, which is what lets a restart adopt a report a crash left."""
        st = self.st
        cp = a["checkpoint"]
        st["audits"].pop(str(cp), None)
        if cp > st["handled_upto"] and cp not in st["handled_above"]:
            st["handled_above"].append(cp)
            st["handled_above"].sort()
        self._advance_handled()
        outcome = report["outcome"]
        summary = {"audit": report["id"], "checkpoint": cp, "outcome": outcome,
                   "reason": report["reason"][:300], "at": report["published_at"],
                   "report": "reports/%s.json" % report["id"]}
        if not any(r["audit"] == report["id"] for r in st["recent"]):
            st["recent"].append(summary)
            st["recent"] = st["recent"][-RECENT_KEPT:]
            st["totals"]["audits"] += 1
            st["totals"]["cpu_s"] = round(st["totals"]["cpu_s"] + report["costs"]["cpu_s"], 3)
            st["totals"]["wall_s"] = round(st["totals"]["wall_s"] + report["costs"]["wall_s"], 3)
        if outcome in VERIFIED:
            st["last_verified"] = summary
            self._cover(report)
            if report["id"] not in st["retained_passed"]:
                st["retained_passed"].append(report["id"])
            if report["id"] not in st["passed_reports"]:
                st["passed_reports"].append(report["id"])
        else:
            self._gap(cp, cp, outcome, report["reason"], audit=report["id"],
                      ranges=(report.get("scope") or {}).get("sources"))
        if outcome == DISCREPANCY and not any(x["audit"] == report["id"] for x in st["discrepancies"]):
            entry = dict(summary, findings=report.get("findings", [])[:10])
            st["discrepancies"].append(entry)
            st["last_discrepancy"] = entry
        if outcome != SKIPPED:
            st["last_attempted"] = summary
        self.save()
        self.dir.event("result", **summary)
        self._retain()
        self._write_status()

    def _write_status(self, every_s=2.0):
        """status.json, kept current during a long cycle (at most every
        every_s seconds), so a monitor reading it sees the backlog as it is."""
        now = time.monotonic()
        if now - getattr(self, "_status_at", -every_s) < every_s:
            return
        self._status_at = now
        from . import status
        try:
            status.write(self.dir.path)
        except (OSError, ValueError):
            pass

    def _advance_handled(self):
        st = self.st
        above = set(st["handled_above"])
        for c in self._eligible():
            if c <= st["handled_upto"]:
                continue
            if c in above:
                st["handled_upto"] = c
                above.discard(c)
            else:
                break
        st["handled_above"] = sorted(c for c in above if c > st["handled_upto"])

    def _cover(self, report):
        st = self.st
        for s in (report.get("scope") or {}).get("sources") or []:
            for p in s.get("partitions") or []:
                k = "%s|%s|%s|%d" % (s.get("topic"), s.get("cluster_id"), s.get("topic_id"), p["partition"])
                cov = st["coverage"].setdefault(k, {"topic": s.get("topic"), "cluster_id": s.get("cluster_id"),
                                                     "topic_id": s.get("topic_id"),
                                                     "partition": p["partition"], "verified": []})
                if p.get("to") is not None:
                    cov["verified"] = _merge(cov["verified"] + [[p.get("from"), p["to"]]])
        st["verified_checkpoints"] = self._add_checkpoint(st["verified_checkpoints"], report["checkpoint"])

    def _add_checkpoint(self, ranges, cp):
        """Adds a checkpoint to merged [first, last] ranges of completed
        checkpoints, joining a range that ends at the completed checkpoint
        before it or starts at the one after it."""
        if any(lo <= cp <= hi for lo, hi in ranges):
            return ranges
        prev = self._previous_completed(cp)
        later = [c for c in self.completed if c > cp]
        nxt = later[0] if later else None
        out = sorted(ranges + [[cp, cp]])
        merged = []
        for lo, hi in out:
            if merged and (merged[-1][1] == prev and lo == cp or
                           merged[-1][1] == cp and nxt is not None and lo == nxt):
                merged[-1][1] = max(merged[-1][1], hi)
            else:
                merged.append([lo, hi])
        return merged

    def _gap(self, first, last, outcome, reason, audit=None, ranges=None, count=1):
        gaps = self.st["gaps"]
        key = reason.split(":")[0][:80]
        if gaps:
            g = gaps[-1]
            if (g["outcome"] == outcome and g["kind"] == key and audit is not None and
                    g.get("last") == self._previous_completed(first) and not g.get("acknowledged")):
                g["last"] = last
                g["count"] += count
                g["audits"] = (g.get("audits") or [])[-20:] + [audit]
                return
        gaps.append({"first": first, "last": last, "count": count, "outcome": outcome, "kind": key,
                     "reason": reason[:500], "audits": [audit] if audit else [], "at": now_iso(),
                     "ranges": ranges})

    # -- retention ------------------------------------------------------------------------------------
    def _retain(self):
        st = self.st
        lim = self.cfg.limits
        removed = []
        while len(st["retained_passed"]) > lim["retain_passed"]:
            aid = st["retained_passed"].pop(0)
            if self.dir.remove_audit_dir(aid):
                removed.append(aid)
        while len(st["passed_reports"]) > lim["retain_passed_reports"]:
            aid = st["passed_reports"].pop(0)
            self.dir.remove_report(aid)
        if removed:
            self.dir.event("retention", removed=removed)
            self.save()


def _cpu():
    import resource
    s = resource.getrusage(resource.RUSAGE_SELF)
    c = resource.getrusage(resource.RUSAGE_CHILDREN)
    return s.ru_utime + s.ru_stime + c.ru_utime + c.ru_stime


def _fill(arg, values):
    return re.sub(r"\{([a-z_]+)\}", lambda m: values.get(m.group(1), m.group(0)), arg)


def _checkpoint_of(path):
    m = re.search(r"checkpoint-(\d+)\.snap", path)
    return int(m.group(1)) if m else None


def _json_line(text):
    for line in reversed((text or "").strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                return None
    return None


def _first_line(text):
    for line in (text or "").splitlines():
        if line.strip():
            return line.strip()[:500]
    return ""


def _first_json_line(path):
    try:
        with open(path) as f:
            return json.loads(f.readline())
    except (OSError, ValueError):
        return None


def _empty(rep, ref):
    """A scope that owed the destination nothing: the sink received no row,
    the reference computed none from the same complete source range, and the
    only checks not passed are the two that have no key to read."""
    if not isinstance(rep, dict) or rep.get("exit_code") != 3:
        return False
    if not ref.get("usable") or not ref.get("complete") or ref.get("rows") != 0:
        return False
    if ((rep.get("scope") or {}).get("expected_rows")) != 0:
        return False
    checks = {c.get("id"): c for c in rep.get("checks", [])}
    if (checks.get("reference.capture") or {}).get("verdict") != "passed":
        return False
    for cid, c in checks.items():
        if c.get("verdict") == "passed":
            continue
        if cid in ("reconcile.rows", "reference.destination") and c.get("verdict") == "unavailable":
            continue
        return False
    return True
