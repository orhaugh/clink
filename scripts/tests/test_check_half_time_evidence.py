"""scripts/check-half-time-evidence.py against bundles built here: one that
passes, and each way one must not.

    python3 -m unittest discover -s scripts/tests -v

Each bundle is written whole, its manifest last, so a test changes the one
property it is about and every other check still holds.
"""
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "check-half-time-evidence.py"
spec = importlib.util.spec_from_file_location("check_half_time_evidence", SCRIPT)
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)

CANDIDATE = "15a61500dbbc706ceeb983bfd7ca8b955ebcb168"
OTHER = "4345fcda" + "0" * 32
HALF_TIME = "3f47a85" + "1" * 33


def verdicts():
    rows = []
    for checker in evidence.CHECKERS:
        for g in ("exactly_once", "at_least_once", "upsert", "value"):
            rows.append({"checker": checker, "guarantee": g, "line": f"{checker} {g} store", "ok": True})
    return rows


def complete():
    """The files of a bundle that shows half-time passing against CANDIDATE."""
    binary = {"commit": CANDIDATE, "clean": True, "sha256": "a" * 64}
    ev = {
        "format": evidence.FORMAT,
        "candidate": CANDIDATE,
        "clink": {"source": {"path": "/src", "head": CANDIDATE, "clean": True},
                  "binaries": {"clink": dict(binary), "clink_node": dict(binary),
                               "clink_submit_sql": {"sha256": "b" * 64}}},
        "half_time": {"commit": HALF_TIME, "clean": True, "changes": []},
        "riders": {"built_against": "/ht/.clink/prefix/lib/cmake/clink", "commit": CANDIDATE},
        "invocation": {"env": {"SCENES": "1", "SPEED": "30"}},
        "run": {"exit_code": 0, "started_utc": "2026-11-05T12:00:00Z"},
        "checks": [{"checker": c, "exit_code": 0, "output": f"checks/{c}.out"} for c in evidence.CHECKERS],
        "verdicts": {"file": "verdicts.jsonl"},
        "scenes": [
            {"scene": "worker_loss", "status": "executed", "worker": "worker-1", "minute": "50"},
            {"scene": "rescale_up", "status": "executed", "minute": "67"},
            {"scene": "savepoint", "status": "executed", "minute": "75",
             "stop_reply": "stop: job_id=17 ok=1 savepoint_checkpoint_id=114",
             "savepoint_checkpoint_id": "114", "resume_reply": '{"job_id":19}'},
            {"scene": "coordinator_failover", "status": "executed", "minute": "95"},
            {"scene": "rescale_down", "status": "executed", "minute": "110"},
        ],
        "trace_check": {"ran": False},
        "logs": {"coordinator": "logs/coordinator.log", "standby": "logs/coordinator-standby.log"},
    }
    files = {
        "logs/coordinator.log": "[coordinator.register] [warning] job_id=7 awaiting_restart (attempt 1/10)\n",
        "logs/coordinator-standby.log": "coordinator became leader (epoch=2)\n"
                                        "[coordinator.ha] [info] recovered job_id=7 restore_from_ckpt=135\n",
        "verdicts.jsonl": "".join(json.dumps(v) + "\n" for v in verdicts()),
        "run.log": "check_run: PASS\n",
    }
    for c in evidence.CHECKERS:
        files[f"checks/{c}.out"] = f"{c}: PASS\n"
    return ev, files


def write(root, ev, files):
    root.mkdir(parents=True)
    for name, text in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    (root / "evidence.json").write_text(json.dumps(ev))
    listed = sorted(p for p in root.rglob("*") if p.is_file())
    (root / "MANIFEST.sha256").write_text("".join(
        f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(root).as_posix()}\n" for p in listed))


class HalfTimeEvidence(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.bundle = self.dir / "bundle"

    def tearDown(self):
        shutil.rmtree(self.dir)

    def judge(self, change=None, candidate=CANDIDATE):
        ev, files = complete()
        if change:
            change(ev, files)
        write(self.bundle, ev, files)
        return evidence.judge(self.bundle, candidate)

    def assertRefused(self, problems, *words):
        self.assertTrue(problems, "the bundle passed")
        self.assertTrue(any(all(w in p for w in words) for p in problems),
                        f"no reason names {words}: {problems}")

    def test_a_complete_bundle_for_the_candidate_passes(self):
        problems, summary = self.judge()
        self.assertEqual(problems, [])
        self.assertIn("coordinator_failover executed", summary)
        self.assertIn("upsert 4", summary)

    def test_the_command_line_passes_it_and_exits_0(self):
        write(self.bundle, *complete())
        run = subprocess.run([sys.executable, str(SCRIPT), str(self.bundle), "--candidate", CANDIDATE],
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn("PASS", run.stdout)

    def test_no_bundle_is_no_evidence(self):
        problems, _ = evidence.judge(self.dir / "absent", CANDIDATE)
        self.assertRefused(problems, "no evidence")
        run = subprocess.run([sys.executable, str(SCRIPT), str(self.dir / "absent"), "--candidate", CANDIDATE],
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 1)
        self.assertIn("FAIL", run.stdout)

    def test_a_bundle_for_another_candidate_is_refused(self):
        problems, _ = self.judge(candidate=OTHER)
        self.assertRefused(problems, "not the candidate", OTHER)

    def test_a_binary_built_from_another_commit_is_refused(self):
        def change(ev, files):
            ev["clink"]["binaries"]["clink_node"]["commit"] = OTHER
        self.assertRefused(self.judge(change)[0], "clink_node", OTHER)

    def test_a_riders_job_built_against_another_clink_is_refused(self):
        def change(ev, files):
            ev["riders"] = {"built_against": "/elsewhere/.clink/prefix/lib/cmake/clink", "commit": OTHER}
        self.assertRefused(self.judge(change)[0], "riders job", OTHER)

    def test_a_dirty_build_is_refused(self):
        def change(ev, files):
            ev["clink"]["binaries"]["clink"]["clean"] = False
        self.assertRefused(self.judge(change)[0], "clink reported", "not clean")

    def test_a_failed_checker_is_refused(self):
        def change(ev, files):
            ev["checks"][1]["exit_code"] = 1
            ev["run"]["exit_code"] = 1
        problems = self.judge(change)[0]
        self.assertRefused(problems, "check_round2 exited 1")
        self.assertRefused(problems, "run.sh exited 1")

    def test_a_verdict_that_did_not_hold_is_refused(self):
        def change(ev, files):
            rows = verdicts()
            rows[0]["ok"] = False
            files["verdicts.jsonl"] = "".join(json.dumps(v) + "\n" for v in rows)
        self.assertRefused(self.judge(change)[0], "did not hold", "exactly_once")

    def test_a_guarantee_no_store_was_held_to_is_refused(self):
        def change(ev, files):
            rows = [v for v in verdicts() if v["guarantee"] != "upsert"]
            files["verdicts.jsonl"] = "".join(json.dumps(v) + "\n" for v in rows)
        self.assertRefused(self.judge(change)[0], "no store was held to upsert")

    def test_a_missing_fault_scene_is_refused(self):
        def change(ev, files):
            ev["scenes"] = [s for s in ev["scenes"] if s["scene"] != "coordinator_failover"]
        self.assertRefused(self.judge(change)[0], "coordinator_failover")

    def test_a_skipped_scene_is_not_a_pass(self):
        def change(ev, files):
            for s in ev["scenes"]:
                if s["scene"] == "savepoint":
                    s.clear()
                    s.update(scene="savepoint", status="skipped", reason="SCENES=0")
        self.assertRefused(self.judge(change)[0], "savepoint", "skipped", "not a pass")

    def test_a_scene_whose_effect_the_logs_do_not_show_is_refused(self):
        def change(ev, files):
            files["logs/coordinator-standby.log"] = "standing by for leadership\n"
        self.assertRefused(self.judge(change)[0], "standby", "no takeover")

    def test_a_failed_optional_scene_is_refused(self):
        def change(ev, files):
            ev["scenes"][1]["status"] = "failed"
        self.assertRefused(self.judge(change)[0], "rescale_up", "failed")

    def test_half_time_with_local_changes_is_refused(self):
        def change(ev, files):
            ev["half_time"].update(clean=False, changes=[" M tools/check_run.py"], diff="half-time.diff")
            files["half-time.diff"] = "diff --git a/tools/check_run.py b/tools/check_run.py\n"
        self.assertRefused(self.judge(change)[0], "half-time had local changes")

    def test_a_file_changed_after_the_manifest_is_refused(self):
        write(self.bundle, *complete())
        (self.bundle / "checks" / "check_run.out").write_text("check_run: PASS, edited\n")
        self.assertRefused(evidence.judge(self.bundle, CANDIDATE)[0], "check_run.out", "hash")

    def test_a_file_added_after_the_manifest_is_refused(self):
        write(self.bundle, *complete())
        (self.bundle / "note.txt").write_text("added later\n")
        self.assertRefused(evidence.judge(self.bundle, CANDIDATE)[0], "note.txt", "not in the manifest")


if __name__ == "__main__":
    unittest.main()
