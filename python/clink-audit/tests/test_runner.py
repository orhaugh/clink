"""The runner against a scripted clink and reference (fake_clink.py).

Each test builds a job's checkpoint directory, a plan saying how every
command answers for each checkpoint and attempt, and a configuration, then
runs the runner in process or, where a crash or a second runner is the
point, as its own process. Nothing here needs a broker, a destination or
network isolation: verification runs under a stand-in that says isolation
is enforced, which only a test can construct.
"""
import io
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
PKG = HERE.parent
sys.path.insert(0, str(PKG))

from clink_audit import cli, config, runner, status  # noqa: E402
from clink_audit.state import StateDir  # noqa: E402


class Enforced:
    state = "enforced"
    detail = "a test's stand-in"
    mechanism = "test"
    enforced = True

    def establish(self):
        return self.state

    def wrap(self, argv):
        return list(argv)


class Unavailable(Enforced):
    state = "unavailable"
    detail = "no network isolation can run here (test)"
    enforced = False


LAUNCHER = """
import os, sys
sys.path.insert(0, %r)
from clink_audit import cli, runner
crash = os.environ.get("TEST_CRASH_AT", "")
def fault(point):
    if point == crash:
        os._exit(137)
runner.fault = fault
class Enforced:
    state = "enforced"; detail = "test"; mechanism = "test"; enforced = True
    def establish(self): return self.state
    def wrap(self, argv): return list(argv)
sys.exit(cli.main(sys.argv[1:], isolation=Enforced()))
""" % str(PKG)


class World:
    def __init__(self, tmp):
        self.root = Path(tmp)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        src = (HERE / "fake_clink.py").read_text()
        for name in ("clink", "reference"):
            p = self.bin / name
            p.write_text("#!%s\n%s" % (sys.executable, src))
            p.chmod(0o755)
        self.ckpt = self.root / "ckpt"
        (self.ckpt / "_jobs" / "1").mkdir(parents=True)
        self.capture = self.root / "capture"
        self.capture.mkdir()
        self.state = self.root / "state"
        self.plan = {"default": {}, "checkpoints": {}}
        self.launcher = self.root / "launch.py"
        self.launcher.write_text(LAUNCHER)
        self.write_plan()

    def write_plan(self):
        (self.bin / "plan.json").write_text(json.dumps(self.plan))

    def at(self, cp, **how):
        self.plan["checkpoints"].setdefault(str(cp), {}).update(how)
        self.write_plan()

    def complete(self, *ids):
        for i in ids:
            (self.ckpt / "_jobs" / "1" / ("COMPLETED-%d" % i)).write_text("{}")

    def config(self, **over):
        limits = {"retry_backoff_s": [0], "max_attempts": 2, "run_seconds": 120,
                  "poll_interval_s": 0, "collect_retries": 3}
        limits.update(over.pop("limits", {}))
        cfg = {"format": "clink-audit-config", "version": 1, "state_dir": str(self.state),
               "clink": str(self.bin / "clink"),
               "job": {"id": 1, "checkpoint_dir": str(self.ckpt), "capture_dir": str(self.capture),
                       "commit_confirmation": "no", "start_at": "earliest"},
               "key": ["event_id"],
               "destination": {"host": "127.0.0.1", "port": 9000, "user": "reconciler",
                               "password_env": "DEST_SECRET"},
               "reference": {"command": [str(self.bin / "reference"), "--scope={scope}", "--out={out}",
                                         "--max-records={max_records}"],
                             "env": ["REFERENCE_SECRET"], "when_run_start": ["--run-start-offset=ALL=0"]},
               "selection": "sequential", "limits": limits}
        for k, v in over.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
        p = self.root / "config.json"
        p.write_text(json.dumps(cfg))
        return p

    def run(self, *args, isolation=None, **cfg):
        p = self.config(**cfg)
        out = io.StringIO()
        env = {"DEST_SECRET": "dest-pass-1234", "REFERENCE_SECRET": "kafka-pass-5678"}
        old = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            with redirect_stdout(out):
                code = cli.main([args[0] if args else "run", "--config=%s" % p, *args[1:]],
                                isolation=isolation or Enforced())
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.last_output = out.getvalue()
        return code

    def settle(self, *args, cycles=6, **cfg):
        """Runs cycles until no audit waits to be tried again (each audit has
        one attempt per cycle), and returns the last exit status."""
        code = None
        for _ in range(cycles):
            code = self.run(*args, **cfg)
            st = json.loads((self.state / "state.json").read_text())
            if not any(a.get("attempts") for a in st["audits"].values()):
                break
        return code

    def spawn(self, *args, crash_at="", **cfg):
        p = self.config(**cfg)
        env = dict(os.environ, DEST_SECRET="dest-pass-1234", REFERENCE_SECRET="kafka-pass-5678",
                   TEST_CRASH_AT=crash_at)
        return subprocess.Popen([sys.executable, str(self.launcher), args[0] if args else "run",
                                 "--config=%s" % p, *args[1:]], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def status(self):
        return status.compute(self.state)

    def calls(self, kind=None):
        p = self.bin / "calls.ndjson"
        if not p.exists():
            return []
        out = [json.loads(line) for line in p.read_text().splitlines()]
        return [c for c in out if kind is None or c["kind"] == kind]

    def calls_for(self, kind, cp):
        flag = "--checkpoint=%d" % cp
        if kind == "reference":
            return [c for c in self.calls(kind) if '"checkpoint": %d,' % cp in
                    Path(next(a[8:] for a in c["argv"] if a.startswith("--scope="))).read_text()
                    or False]
        return [c for c in self.calls(kind) if flag in c["argv"]]

    def reports(self):
        d = self.state / "reports"
        return sorted((json.loads(p.read_text()) for p in d.glob("*.json")),
                      key=lambda r: r["checkpoint"]) if d.is_dir() else []

    def events(self, kind=None):
        p = self.state / "events.ndjson"
        out = [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []
        return [e for e in out if kind is None or e["kind"] == kind]


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="clink-audit-test-")
        self.w = World(self._tmp)

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def outcomes(self):
        return {r["checkpoint"]: r["outcome"] for r in self.w.reports()}


class Healthy(RunnerTest):
    def test_every_checkpoint_in_order_is_verified_once_with_contiguous_coverage(self):
        self.w.complete(1, 2, 3)
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "passed", 3: "passed"})
        s = self.w.status()
        self.assertEqual(s["state"], "healthy")
        self.assertEqual(s["verified_checkpoints"], [[1, 3]])
        self.assertEqual(s["coverage"][0]["verified"], [[0, 30]])
        self.assertEqual(s["coverage"][0]["holes"], [])
        # A second run with nothing new does nothing again.
        self.assertEqual(self.w.run(), 0)
        self.assertEqual(len(self.w.calls("incident-collect")), 3)
        self.w.complete(4)
        self.assertEqual(self.w.run(), 0)
        self.assertEqual(self.w.status()["verified_checkpoints"], [[1, 4]])
        report = self.w.reports()[0]
        self.assertEqual(report["format"], "clink-audit-report")
        for k in ("config_fingerprint", "graph_sha256", "scope_sha256", "generation", "reference"):
            self.assertIn(k, report["identity"])
        self.assertEqual(report["coverage"]["mode"], "selected-keys")

    def test_each_step_receives_only_its_own_credential(self):
        self.w.complete(1)
        self.assertEqual(self.w.run(), 0)
        for c in self.w.calls():
            env = set(c["env"])
            if c["kind"] == "incident-reconcile":
                self.assertIn("DEST_SECRET", env)
                self.assertNotIn("REFERENCE_SECRET", env)
            elif c["kind"] == "reference":
                self.assertIn("REFERENCE_SECRET", env)
                self.assertNotIn("DEST_SECRET", env)
            else:
                self.assertFalse({"DEST_SECRET", "REFERENCE_SECRET"} & env, c["kind"])
        # The reference printed its credential; no file the runner keeps holds it.
        for p in self.w.state.rglob("*"):
            if p.is_file():
                data = p.read_bytes()
                self.assertNotIn(b"kafka-pass-5678", data, p)
                self.assertNotIn(b"dest-pass-1234", data, p)
        argv = next(c for c in self.w.calls("incident-reconcile"))["argv"]
        self.assertIn("--password-env=DEST_SECRET", argv)
        self.assertNotIn("--coverage=complete-table", argv)

    def test_a_scope_with_no_record_is_covered_as_empty(self):
        self.w.complete(1, 2, 3)
        self.w.at(2, ranges=[[0, 10, 10]], rows=0)
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "empty", 3: "passed"})
        self.assertEqual(self.w.status()["verified_checkpoints"], [[1, 3]])

    def test_the_run_start_is_given_to_the_reference_only_when_the_scope_starts_there(self):
        self.w.complete(1, 2)
        self.w.at(1, ranges=[[0, None, 10]])
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        first = self.w.calls("reference")[0]["argv"]
        second = self.w.calls("reference")[1]["argv"]
        self.assertIn("--run-start-offset=ALL=0", first)
        self.assertNotIn("--run-start-offset=ALL=0", second)
        self.assertEqual(self.w.status()["coverage"][0]["verified"], [[None, 20]])


class Discrepancies(RunnerTest):
    def test_an_earlier_discrepancy_survives_later_healthy_checkpoints_until_acknowledged(self):
        self.w.complete(1, 2, 3)
        self.w.at(2, reconcile="delivery")
        self.assertEqual(self.w.run(), 1)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "discrepancy", 3: "passed"})
        s = self.w.status()
        self.assertEqual(s["state"], "discrepancy")
        self.assertEqual(s["last_verified"]["checkpoint"], 3)
        self.assertEqual(s["last_discrepancy"]["checkpoint"], 2)
        self.assertEqual(s["verified_checkpoints"], [[1, 1], [3, 3]])
        self.w.complete(4)
        self.assertEqual(self.w.run(), 1)
        # Its evidence is kept, whatever the retention of passed audits.
        disc = self.w.reports()[1]
        self.assertTrue((self.w.state / disc["evidence"]["dir"] / "bundle" / "manifest.json").exists())
        # Never retried: the discrepancy is the result.
        self.assertEqual(len(self.w.calls_for("incident-reconcile", 2)), 1)
        cfg = self.w.config()
        with redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["acknowledge", "--config=%s" % cfg, "--audit=%s" % disc["id"],
                                       "--note=fixed the row by hand"]), 0)
        s = self.w.status()
        self.assertEqual(s["state"], "healthy")
        self.assertEqual(s["discrepancies"][0]["acknowledged"]["note"], "fixed the row by hand")

    def test_a_transformation_defect_is_reported_as_one(self):
        self.w.complete(1)
        self.w.at(1, reconcile="transformation")
        self.assertEqual(self.w.run(), 1)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "discrepancy")
        self.assertEqual([f["relationship"] for f in r["findings"]], ["transformation"])

    def test_verification_that_fails_is_a_discrepancy_in_the_evidence(self):
        self.w.complete(1)
        self.w.at(1, verify=1)
        self.assertEqual(self.w.run(), 1)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "discrepancy")
        self.assertIn("evidence", [f["relationship"] for f in r["findings"]])


class Partial(RunnerTest):
    def test_a_destination_outage_is_retried_and_never_reported_as_passing(self):
        self.w.complete(1)
        self.w.at(1, reconcile=["destination-down", "pass"])
        self.assertEqual(self.w.run(), 3, self.w.last_output)
        self.assertEqual(self.w.reports(), [])
        self.assertEqual(self.w.status()["backlog"]["retrying"][0]["attempt"], 2)
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "passed")
        self.assertEqual(r["attempt"], 2)
        self.assertEqual(r["attempts"][0]["outcome"], "partial")
        self.assertIn("could not be read", r["attempts"][0]["reason"])
        # The reference completed on the first attempt and was not read again.
        self.assertEqual(len(self.w.calls("reference")), 1)

    def test_a_destination_that_stays_down_ends_partial_after_its_attempts(self):
        self.w.complete(1, 2)
        self.w.at(1, reconcile="destination-down")
        self.assertEqual(self.w.settle(limits={"max_attempts": 3}), 3)
        self.assertEqual(self.outcomes(), {1: "partial", 2: "passed"})
        self.assertEqual(len(self.w.calls_for("incident-reconcile", 1)), 3)
        s = self.w.status()
        self.assertEqual(s["state"], "incomplete")
        self.assertEqual(s["gaps"][0]["outcome"], "partial")

    def test_a_source_outage_still_checks_the_delivery_and_retries_the_reference(self):
        self.w.complete(1)
        self.w.at(1, reference=["fail", "complete"])
        self.assertEqual(self.w.settle(), 0, self.w.last_output)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "passed")
        first = self.w.calls_for("incident-reconcile", 1)[0]["argv"]
        self.assertFalse(any(a.startswith("--reference=") for a in first))
        second = self.w.calls_for("incident-reconcile", 1)[1]["argv"]
        self.assertTrue(any(a.startswith("--reference=") for a in second))

    def test_expired_source_history_leaves_the_scope_partial(self):
        self.w.complete(1)
        self.w.at(1, reference="incomplete")
        self.assertEqual(self.w.settle(), 3)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "partial")
        self.assertIn("expired or deleted", r["reason"])

    def test_a_reference_that_hangs_times_out_and_its_group_is_stopped(self):
        self.w.complete(1)
        self.w.at(1, reference="hang")
        self.assertEqual(self.w.run(limits={"reference_timeout_s": 1, "max_attempts": 1}), 3)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "partial")
        self.assertIn("time limit", r["steps"]["reference"]["why"] + r["reason"])
        pids = [c["pid"] for c in self.w.calls("reference")]
        for pid in pids:
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_malformed_reference_output_is_never_compared(self):
        self.w.complete(1)
        self.w.at(1, reference="malformed")
        self.assertEqual(self.w.run(limits={"max_attempts": 1}), 3)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "partial")
        self.assertIn("no end line", r["steps"]["reference"]["why"])
        argv = self.w.calls("incident-reconcile")[0]["argv"]
        self.assertFalse(any(a.startswith("--reference=") for a in argv))

    def test_a_reference_past_its_byte_bound_is_stopped(self):
        self.w.complete(1)
        self.w.at(1, reference="huge")
        self.assertEqual(self.w.run(limits={"reference_bytes": 100000, "max_attempts": 1}), 3)
        self.assertIn("passed its bound", self.w.reports()[0]["steps"]["reference"]["why"])

    def test_a_reference_past_its_row_and_record_bounds_is_not_used(self):
        self.w.complete(1, 2)
        self.w.at(1, rows=50)
        self.w.at(2, records=500)
        self.assertEqual(self.w.run(limits={"reference_rows": 20, "reference_records": 100,
                                            "max_attempts": 1}), 3)
        r1, r2 = self.w.reports()
        self.assertIn("reference_rows", r1["reason"])
        self.assertIn("reference_records", r2["reason"])
        self.assertEqual(self.w.calls("reference")[0]["argv"][-1], "--max-records=100")

    def test_a_destination_read_past_its_bound_is_partial(self):
        self.w.complete(1)
        self.w.at(1, reconcile="bound")
        self.assertEqual(self.w.run(limits={"max_rows_per_query": 5, "max_attempts": 1}), 3)
        self.assertEqual(self.w.reports()[0]["outcome"], "partial")
        argv = self.w.calls("incident-reconcile")[0]["argv"]
        self.assertIn("--max-rows-per-query=5", argv)

    def test_verification_without_isolation_is_partial_and_not_retried(self):
        self.w.complete(1)
        self.assertEqual(self.w.run(isolation=Unavailable()), 3)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "partial")
        self.assertIn("isolation is unavailable", r["reason"])
        self.assertEqual(self.w.calls("incident-verify"), [])
        self.assertEqual(len(self.w.calls("incident-reconcile")), 1)

    def test_a_windowed_job_is_unsupported_by_name(self):
        self.w.complete(1)
        self.w.at(1, scope="windowed")
        self.assertEqual(self.w.run(), 3)
        r = self.w.reports()[0]
        self.assertEqual(r["outcome"], "unsupported")
        self.assertIn("record-wise jobs only", r["reason"])
        self.assertEqual(self.w.calls("reference"), [])


class Collection(RunnerTest):
    def test_a_checkpoint_completing_during_collection_is_collected_again(self):
        self.w.complete(1)
        self.w.at(1, collect=["changed", "changed", "ok"])
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        self.assertEqual(len(self.w.calls("incident-collect")), 3)
        self.assertEqual(self.w.reports()[0]["steps"]["collect"]["tries"], 3)

    def test_a_checkpoint_whose_state_was_purged_is_a_gap(self):
        self.w.complete(1, 2, 3)
        self.w.at(1, collect="expired")
        self.w.at(2, collect="expired")
        self.assertEqual(self.w.run(), 3)
        self.assertEqual(self.outcomes(), {1: "expired", 2: "expired", 3: "passed"})
        self.assertIn("purged", self.w.reports()[0]["reason"])
        s = self.w.status()
        self.assertEqual(s["state"], "incomplete")
        gap = s["gaps"][0]
        self.assertEqual((gap["first"], gap["last"], gap["count"]), (1, 2, 2))
        self.assertEqual(s["verified_checkpoints"], [[3, 3]])
        # Acknowledging the range clears the condition, and keeps the gap listed.
        with redirect_stdout(io.StringIO()):
            cli.main(["acknowledge", "--state-dir=%s" % self.w.state, "--checkpoints=1-2",
                      "--note=retention was too short; raised"])
        s = self.w.status()
        self.assertEqual(s["state"], "healthy")
        self.assertTrue(s["gaps"][0]["acknowledged"])

    def test_a_checkpoint_without_its_marker_is_a_gap(self):
        self.w.complete(1, 2)
        self.w.at(1, collect="missing")
        self.assertEqual(self.w.run(), 3)
        self.assertEqual(self.outcomes(), {1: "expired", 2: "passed"})
        self.assertIn("no COMPLETED marker", self.w.reports()[0]["reason"])

    def test_a_bundle_past_its_bound_is_partial_and_not_retried(self):
        self.w.complete(1)
        self.w.at(1, collect="bound")
        self.assertEqual(self.w.run(), 3)
        self.assertEqual(self.outcomes(), {1: "partial"})
        self.assertEqual(len(self.w.calls("incident-collect")), 1)

    def test_a_collection_that_hangs_is_stopped_and_retried(self):
        self.w.complete(1)
        self.w.at(1, collect=["hang", "ok"])
        self.assertEqual(self.w.settle(limits={"collect_timeout_s": 0.5}), 0, self.w.last_output)
        self.assertEqual(self.outcomes(), {1: "passed"})
        # The collector's temporary directory from the stopped attempt is gone.
        d = self.w.state / "audits" / self.w.reports()[0]["id"]
        self.assertFalse([p for p in d.iterdir() if p.name.startswith(".bundle.incomplete-")])


class Selection(RunnerTest):
    def test_latest_only_records_every_scope_it_passes_over(self):
        self.w.complete(1, 2, 3, 4)
        self.assertEqual(self.w.run(selection="latest-only"), 3)
        self.assertEqual(self.outcomes(), {1: "skipped", 2: "skipped", 3: "skipped", 4: "passed"})
        s = self.w.status()
        self.assertEqual(s["gaps"][0]["first"], 1)
        self.assertEqual(s["gaps"][0]["last"], 3)
        self.assertEqual(s["coverage"][0]["verified"], [[30, 40]])
        self.assertEqual(len(self.w.calls("incident-collect")), 1)
        self.w.complete(5, 6)
        self.assertEqual(self.w.run(selection="latest-only"), 3)
        self.assertEqual(self.outcomes()[5], "skipped")
        self.assertEqual(self.outcomes()[6], "passed")

    def test_a_growing_backlog_is_shown_not_dropped(self):
        self.w.complete(*range(1, 9))
        self.w.at(1, reconcile_sleep=0.6)
        self.assertEqual(self.w.run(limits={"run_seconds": 1, "max_pending": 3, "backlog_warn": 2}), 3)
        s = self.w.status()
        self.assertIn("behind", s["conditions"])
        self.assertGreater(s["backlog"]["waiting"], 2)
        self.assertLessEqual(s["backlog"]["collected"], 3)
        self.assertEqual(s["gaps"], [])
        # Given time, every one is verified and none was skipped.
        for _ in range(6):
            self.w.run(limits={"max_pending": 3, "backlog_warn": 2})
        self.assertEqual(set(self.outcomes().values()), {"passed"})
        self.assertEqual(sorted(self.outcomes()), list(range(1, 9)))

    def test_a_cycle_collects_what_completes_during_it_and_leaves_its_audit_to_the_next(self):
        self.w.complete(1)
        self.w.at(1, reconcile_touch=[str(self.w.ckpt / "_jobs" / "1" / "COMPLETED-2")])
        self.assertEqual(self.w.run(), 0)
        self.assertEqual(self.outcomes(), {1: "passed"})
        self.assertEqual(len(self.w.calls_for("incident-collect", 2)), 1)
        self.assertEqual(self.w.calls_for("incident-scope", 2), [])
        self.assertEqual(self.w.status()["backlog"]["collected"], 1)
        self.assertEqual(self.w.run(), 0)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "passed"})
        self.assertEqual(len(self.w.calls_for("incident-collect", 2)), 1)

    def test_start_at_latest_records_what_came_before_as_a_gap(self):
        self.w.complete(1, 2, 3)
        self.assertEqual(self.w.run(job={"start_at": "latest"}), 3)
        s = self.w.status()
        self.assertEqual(self.outcomes(), {3: "passed"})
        self.assertEqual((s["gaps"][0]["first"], s["gaps"][0]["last"], s["gaps"][0]["outcome"]),
                         (1, 2, "before-start"))

    def test_no_eligible_scope_yet(self):
        self.assertEqual(self.w.run(), 5)
        self.assertEqual(self.w.status()["state"], "no-eligible-scope")


class Identity(RunnerTest):
    def test_a_recreated_topic_is_another_log_and_a_changed_job_is_recorded(self):
        self.w.complete(1, 2, 3)
        self.w.at(3, topic_id="T2", ranges=[[0, 0, 5]], graph="g2")
        self.assertEqual(self.w.run(), 0)
        s = self.w.status()
        self.assertEqual([(c["topic_id"], c["verified"]) for c in s["coverage"]],
                         [("T1", [[0, 20]]), ("T2", [[0, 5]])])
        self.assertEqual(len(self.w.events("job-changed")), 1)

    def test_a_changed_reference_starts_unfinished_work_again_under_a_new_identity(self):
        self.w.complete(1, 2)
        self.w.at(2, reconcile=["destination-down", "pass"])
        self.assertEqual(self.w.run(limits={"retry_backoff_s": [3600]}), 3)
        first_ids = {r["checkpoint"]: r["id"] for r in self.w.reports()}
        self.assertEqual(list(first_ids), [1])
        cfg = self.w.config()
        raw = json.loads(cfg.read_text())
        raw["reference"]["command"].append("--version-2")
        cfg.write_text(json.dumps(raw))
        with redirect_stdout(io.StringIO()):
            os.environ.update(DEST_SECRET="dest-pass-1234", REFERENCE_SECRET="kafka-pass-5678")
            code = cli.main(["run", "--config=%s" % cfg], isolation=Enforced())
        self.assertEqual(code, 0)
        self.assertEqual(len(self.w.events("config-changed")), 1)
        reports = {r["checkpoint"]: r for r in self.w.reports()}
        self.assertEqual(reports[1]["id"], first_ids[1])
        self.assertNotEqual(reports[2]["identity"]["config_fingerprint"],
                            reports[1]["identity"]["config_fingerprint"])

    def test_a_job_whose_checkpoints_were_replaced_stops_the_runner(self):
        self.w.complete(5, 6)
        self.assertEqual(self.w.run(), 0)
        for p in (self.w.ckpt / "_jobs" / "1").iterdir():
            p.unlink()
        self.w.complete(1)
        self.assertEqual(self.w.run(), 3)
        s = self.w.status()
        self.assertEqual(s["state"], "stopped")
        self.assertEqual(s["stopped"]["reason"], "job-reset")
        self.assertEqual(len(self.w.calls("incident-collect")), 2)

    def test_a_state_directory_belongs_to_one_job(self):
        self.w.complete(1)
        self.assertEqual(self.w.run(), 0)
        other = self.w.root / "other-ckpt"
        (other / "_jobs" / "1").mkdir(parents=True)
        code = self.w.run(job={"checkpoint_dir": str(other)})
        self.assertEqual(code, 2)


class Concurrency(RunnerTest):
    def test_a_second_invocation_while_one_runs_changes_nothing(self):
        self.w.complete(1)
        d = StateDir(self.w.state)
        d.lock()
        try:
            self.assertEqual(self.w.run(), cli.EXIT_BUSY)
        finally:
            d.unlock()
        self.assertEqual(self.w.calls(), [])
        self.assertEqual(self.w.run(), 0)

    def test_concurrent_runners_publish_each_result_once(self):
        self.w.complete(1, 2, 3)
        self.w.plan["default"]["reconcile_sleep"] = 0.3
        self.w.write_plan()
        procs = [self.w.spawn() for _ in range(3)]
        codes = sorted(p.wait(timeout=120) for p in procs)
        self.assertIn(0, codes)
        self.assertTrue(all(c in (0, cli.EXIT_BUSY) for c in codes), codes)
        while self.w.run() == cli.EXIT_BUSY:
            time.sleep(0.1)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "passed", 3: "passed"})
        for cp in (1, 2, 3):
            self.assertEqual(len(self.w.calls_for("incident-reconcile", cp)), 1)


class Crashes(RunnerTest):
    def crash_then_resume(self, point, **plan):
        self.w.complete(1, 2)
        for k, v in plan.items():
            self.w.at(1, **{k: v})
        p = self.w.spawn(crash_at=point)
        self.assertEqual(p.wait(timeout=120), 137, p.stderr.read())
        before = self.outcomes()
        self.assertNotIn(1, {cp for cp, o in before.items() if o == "passed" and point != "after-report"})
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "passed"})
        self.assertEqual(len(list((self.w.state / "reports").glob("c0000000001-*.json"))), 1)
        self.assertEqual(self.w.status()["verified_checkpoints"], [[1, 2]])
        return before

    def test_a_crash_after_collection_resumes_without_collecting_again(self):
        self.crash_then_resume("after-collect")
        self.assertEqual(len(self.w.calls_for("incident-collect", 1)), 1)

    def test_a_crash_while_the_reference_runs_stops_the_orphan_and_runs_it_again(self):
        self.crash_then_resume("during-reference", reference_sleep=30)
        refs = self.w.calls("reference")
        self.assertGreaterEqual(len(refs), 2)
        with self.assertRaises(ProcessLookupError):
            os.kill(refs[0]["pid"], 0)
        self.assertEqual(len(self.w.events("orphan-stopped")), 1)

    def test_a_crash_before_publication_publishes_once_without_reading_the_destination_again(self):
        self.crash_then_resume("before-publish")
        self.assertEqual(len(self.w.calls_for("incident-reconcile", 1)), 1)

    def test_a_crash_after_the_report_adopts_it(self):
        before = self.crash_then_resume("after-report")
        self.assertEqual(before, {1: "passed"})
        self.assertEqual(len(self.w.events("adopted")), 1)
        self.assertEqual(len(self.w.calls_for("incident-reconcile", 1)), 1)

    def test_a_discrepancy_found_before_a_crash_is_kept(self):
        self.w.complete(1)
        self.w.at(1, reconcile=["delivery", "pass"])
        p = self.w.spawn(crash_at="before-publish")
        self.assertEqual(p.wait(timeout=120), 137)
        self.assertEqual(self.w.run(), 1)
        self.assertEqual(self.outcomes(), {1: "discrepancy"})
        self.assertEqual(len(self.w.calls_for("incident-reconcile", 1)), 1)

    def test_a_poll_runner_stopped_by_a_signal_resumes(self):
        self.w.complete(1, 2)
        self.w.at(1, reference_sleep=30)
        p = self.w.spawn("poll")
        deadline = time.time() + 30
        while time.time() < deadline and not self.w.calls("reference"):
            time.sleep(0.05)
        p.send_signal(signal.SIGTERM)
        p.wait(timeout=60)
        self.assertEqual(self.outcomes(), {})
        self.w.at(1, reference_sleep=0)
        self.assertEqual(self.w.run(), 0, self.w.last_output)
        self.assertEqual(self.outcomes(), {1: "passed", 2: "passed"})


class Retention(RunnerTest):
    def test_cleanup_keeps_failure_evidence_and_never_touches_what_it_does_not_own(self):
        self.w.complete(1, 2, 3, 4, 5)
        self.w.at(2, reconcile="delivery")
        self.w.at(4, reference="incomplete")
        foreign = self.w.state / "audits" / "c0000000099-notmine"
        foreign.mkdir(parents=True)
        (foreign / "keep.txt").write_text("not the runner's")
        outside = self.w.root / "outside"
        outside.mkdir()
        (outside / "keep.txt").write_text("outside")
        (self.w.state / "audits" / "c0000000098-link").symlink_to(outside)
        (self.w.state / "notes.txt").write_text("the operator's")
        self.w.settle(limits={"retain_passed": 1, "retain_passed_reports": 2})
        o = self.outcomes()
        self.assertEqual(o[2], "discrepancy")
        self.assertEqual(o[4], "partial")
        kept = {p.name for p in (self.w.state / "audits").iterdir()}
        ids = {r["checkpoint"]: r["id"] for r in self.w.reports()}
        self.assertIn(ids[2], kept)
        self.assertIn(ids[4], kept)
        self.assertIn(ids[5], kept)
        self.assertNotIn(ids[3], kept)
        self.assertTrue((foreign / "keep.txt").exists())
        self.assertTrue((outside / "keep.txt").exists())
        self.assertTrue((self.w.state / "notes.txt").exists())
        # Passed reports past their bound are removed; the others never are.
        self.assertEqual(sorted(self.outcomes()), [2, 3, 4, 5])
        s = self.w.status()
        self.assertEqual(s["verified_checkpoints"], [[1, 1], [3, 3], [5, 5]])
        self.assertEqual(s["resources"]["failure_evidence"], sorted([ids[2], ids[4]]))
        text = status.render_text(s)
        self.assertIn("discrepancy: checkpoint 2", text)
        self.assertIn("gap: checkpoint 4, partial", text)

    def test_disk_pressure_stops_new_audits_visibly(self):
        self.w.complete(1, 2)
        self.assertEqual(self.w.run(limits={"disk_bytes": 1000, "bundle_bytes": 64}), 3)
        s = self.w.status()
        self.assertEqual(s["state"], "stopped")
        self.assertEqual(s["stopped"]["reason"], "disk")
        self.assertEqual(self.w.calls("incident-collect"), [])
        self.assertEqual(self.w.run(), 0)
        self.assertIsNone(self.w.status()["stopped"])


class Configuration(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="clink-audit-cfg-")
        self.w = World(self._tmp)

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def refused(self, why, **over):
        p = self.w.config(**over)
        with self.assertRaises(config.ConfigError) as cm:
            config.load(p)
        self.assertIn(why, str(cm.exception))

    def test_refusals(self):
        self.refused("{scope} and {out}", reference={"command": [str(self.w.bin / "reference"), "--out={out}"]})
        self.refused("not a placeholder", reference={"command": [str(self.w.bin / "reference"), "{scope}",
                                                                 "{out}", "{shell}"]})
        self.refused("absolute path", reference={"command": ["reference", "{scope}", "{out}"]})
        self.refused("must not contain one another", state_dir=str(self.w.ckpt / "audit"))
        self.refused("must not be passed to the reference",
                     reference={"env": ["DEST_SECRET"]})
        self.refused("unknown key", destination={"password": "inline"})
        self.refused("not an environment variable name", reference={"env": ["$(id)"]})

    def test_the_fingerprint_follows_what_changes_a_result(self):
        a = config.load(self.w.config()).fingerprint
        self.assertEqual(a, config.load(self.w.config(limits={"poll_interval_s": 99})).fingerprint)
        self.assertNotEqual(a, config.load(self.w.config(limits={"max_rows_per_query": 7})).fingerprint)
        self.assertNotEqual(a, config.load(self.w.config(key=["account"])).fingerprint)


if __name__ == "__main__":
    unittest.main()
