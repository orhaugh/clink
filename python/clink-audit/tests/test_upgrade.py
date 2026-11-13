"""Upgrade validation without a build: the definition, the reference, the
evidence's integrity, the comparisons, the verdict, the cluster's limits
against a scripted node, the isolation's states, and the command's refusals.
The workflow against real builds is examples/upgrade-check/."""
import copy
import hashlib
import json
import os
import shutil
import stat
import struct
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from clink_audit.upgrade import compare, definition, evidence, reference, workload  # noqa: E402
from clink_audit.upgrade import cli as upgrade_cli  # noqa: E402
from clink_audit.upgrade.builds import Build  # noqa: E402
from clink_audit.upgrade.cluster import Cluster, Limits, RunStopped  # noqa: E402
from clink_audit.isolation import _CONNECT_OUT, _LOOPBACK_PAIR  # noqa: E402
from clink_audit.upgrade import isolate  # noqa: E402
from clink_audit.upgrade.isolate import NETNS_ENV, OUTSIDE_ENV, RunIsolation  # noqa: E402
from clink_audit.upgrade.report import FAILED, PASSED, UNAVAILABLE, UNSUPPORTED, REQUIRED, Report  # noqa: E402

EXAMPLE = HERE.parent.parent.parent / "examples" / "upgrade-check" / "test.json"


def small_definition(**changes):
    raw = json.loads(EXAMPLE.read_text())
    raw["input"].update({"accounts": 6, "blocks": 12, "blocks_per_segment": 2,
                         "savepoint_after_segment": 3})
    for k, v in changes.items():
        raw[k] = v
    return definition.parse(json.dumps(raw).encode())


class TestDefinition(unittest.TestCase):
    def test_the_example_parses(self):
        d = definition.load(EXAMPLE)
        self.assertEqual(d.semantics["window_ms"], 10000)
        self.assertEqual(len(d.sha256), 64)

    def test_another_version_or_kind_is_unsupported_not_malformed(self):
        raw = json.loads(EXAMPLE.read_text())
        for field, value in (("version", 2), ("kind", "sessions")):
            r = copy.deepcopy(raw)
            r[field] = value
            with self.assertRaises(definition.Unsupported):
                definition.parse(json.dumps(r).encode())

    def test_a_malformed_definition_is_refused(self):
        raw = json.loads(EXAMPLE.read_text())
        for mutate in (lambda r: r["input"].pop("seed"), lambda r: r.update(extra=1),
                       lambda r: r["input"].update(savepoint_after_segment=99),
                       lambda r: r["uids"].pop("idle"), lambda r: r["input"].update(start_ms=5)):
            r = copy.deepcopy(raw)
            mutate(r)
            with self.assertRaises(definition.DefinitionError):
                definition.parse(json.dumps(r).encode())


class TestReference(unittest.TestCase):
    def test_it_is_deterministic_and_the_prefix_is_part_of_the_whole(self):
        d = small_definition()
        a, b = reference.compute(d), reference.compute(d)
        self.assertEqual(a, b)
        whole = [tuple(r) for r in a["rows"]]
        for r in a["rows_before_savepoint"]:
            self.assertIn(tuple(r), whole)
        self.assertEqual(a["state_at_end"]["windows"], {})
        self.assertEqual(a["state_at_end"]["timers"], [])
        self.assertEqual(a["state_at_savepoint"]["position"][:2], [4, 0])

    def test_the_savepoint_holds_open_windows_and_pending_timers(self):
        a = reference.compute(definition.load(EXAMPLE))
        self.assertTrue(a["state_at_savepoint"]["windows"])
        self.assertTrue(a["state_at_savepoint"]["timers"])
        self.assertGreater(len(a["rows"]), len(a["rows_before_savepoint"]))

    def test_an_event_behind_the_watermark_breaks_the_contract(self):
        d = small_definition()
        real = workload.segments
        try:
            workload.segments = lambda inp: [["W,1000", "E,999,1,5"], ["END"]]
            with self.assertRaises(reference.ContractError):
                reference.compute(d)
        finally:
            workload.segments = real


class TestEvidence(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="clink-upgrade-ev-"))
        self.ev = self.root / "evidence"
        (self.ev / "checkpoint").mkdir(parents=True)
        (self.ev / "input").mkdir()
        (self.ev / "checkpoint" / "dataset.json").write_text('{"a": 1}')
        (self.ev / "input" / "segment-000001.txt").write_text("W,1\nEND\n")
        ds = hashlib.sha256(b'{"a": 1}').hexdigest()
        self.digest = evidence.write_index(str(self.ev), {"checkpoint": {"dataset_digest": ds}})

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_a_whole_set_verifies(self):
        doc = evidence.verify(str(self.ev), self.digest)
        self.assertEqual(sorted(doc["files"]), ["checkpoint/dataset.json", "input/segment-000001.txt"])

    def test_a_wrong_digest_is_refused(self):
        with self.assertRaisesRegex(evidence.EvidenceInvalid, "not the evidence that was recorded"):
            evidence.verify(str(self.ev), "0" * 64)

    def test_a_truncated_file_is_refused(self):
        p = self.ev / "input" / "segment-000001.txt"
        p.write_text("W,1\n")
        with self.assertRaisesRegex(evidence.EvidenceInvalid, "truncated"):
            evidence.verify(str(self.ev), self.digest)

    def test_a_file_the_index_does_not_list_is_refused(self):
        (self.ev / "input" / "segment-000002.txt").write_text("END\n")
        with self.assertRaisesRegex(evidence.EvidenceInvalid, "does not list"):
            evidence.verify(str(self.ev), self.digest)

    def test_a_link_is_never_followed(self):
        os.symlink(self.ev / "input", self.ev / "more")
        with self.assertRaisesRegex(evidence.EvidenceInvalid, "link"):
            evidence.verify(str(self.ev), self.digest)

    def test_another_version_is_unsupported(self):
        idx = self.ev / evidence.INDEX
        doc = json.loads(idx.read_text())
        doc["version"] = 9
        text = json.dumps(doc).encode()
        idx.write_bytes(text)
        with self.assertRaises(evidence.EvidenceUnsupported):
            evidence.verify(str(self.ev), hashlib.sha256(text).hexdigest())

    def test_the_fingerprint_shows_a_change(self):
        before = evidence.fingerprint(str(self.ev))
        (self.ev / "input" / "segment-000001.txt").write_text("W,2\nEND\n")
        self.assertNotEqual(before, evidence.fingerprint(str(self.ev)))


def _i64(v):
    return struct.pack("<q", v).hex()


def _catalog():
    timers = struct.pack("<I", 0) + struct.pack("<I", 1) + struct.pack("<qI", 31000, 8) + struct.pack("<q", 2)
    position = bytes([1]) + struct.pack("<QQ", 4, 0) + bytes([1]) + struct.pack("<q", 2000)
    wkey = struct.pack("<I", 8) + struct.pack("<q", 0) + struct.pack("<q", 2)
    wval = bytes([0]) + struct.pack(">q", 0) + b"2,15"
    return {"complete": True, "checkpoint": 1, "slots": [
        {"op_id": "10", "slot": "<raw>", "scope": "operator", "entries": [
            {"participant": 0, "user_key_hex": b"uc.position".hex(), "value_hex": position.hex()},
            {"participant": 5, "user_key_hex": b"uc.position".hex(), "value_hex": position.hex()}]},
        {"op_id": "20", "slot": "windows", "scope": "keyed", "entries": [
            {"participant": 1, "user_key_hex": wkey.hex(), "value_hex": wval.hex()}]},
        {"op_id": "30", "slot": "last_seen", "scope": "keyed", "entries": [
            {"participant": 3, "user_key_hex": _i64(2), "value_hex": _i64(1000)}]},
        {"op_id": "30", "slot": "<raw>", "scope": "operator", "entries": [
            {"participant": 3, "user_key_hex": b"__clink_timers".hex(), "value_hex": timers.hex()}]},
    ]}


class TestCompare(unittest.TestCase):
    def test_rows_are_typed_and_compared_as_a_multiset(self):
        rows, bad = compare.parse_rows("window,1,0,10,2,5\nwindow,1,0,10,2,5\nidle,1,2,3,1\nnonsense\n", "t")
        self.assertEqual(len(rows), 3)
        self.assertEqual(len(bad), 1)
        d = compare.multiset_diff([("window", 1, 0, 10, 2, 5), ("idle", 1, 2, 3, 1)], rows)
        self.assertFalse(d["equal"])
        self.assertEqual((d["missing"], d["unexpected"]), (0, 1))

    def test_a_checkpoint_decodes_into_the_jobs_logical_state(self):
        st = compare.decode_state(_catalog())
        self.assertEqual(st["position"], [4, 0, 2000])
        self.assertEqual(st["windows"], {"2:0": [2, 15]})
        self.assertEqual(st["last_seen"], {"2": 1000})
        self.assertEqual(st["timers"], ["31000:2"])
        self.assertEqual(st["undecoded"], [])

    def test_operator_state_held_by_a_subtask_that_does_not_run_it_is_a_copy(self):
        cat = _catalog()
        stale = bytes([1]) + struct.pack("<QQ", 1, 0) + bytes([0]) + struct.pack("<q", 0)
        cat["slots"][0]["entries"][1]["value_hex"] = stale.hex()
        st = compare.decode_state(cat, compare.owners_of([{"op_id": "10", "subtasks": [0]}]))
        self.assertEqual(st["position"], [4, 0, 2000])
        self.assertEqual(st["copies"], 1)

    def test_a_row_outside_the_jobs_encodings_is_listed_never_dropped(self):
        cat = _catalog()
        cat["slots"][2]["entries"][0]["value_hex"] = "00"
        st = compare.decode_state(cat)
        self.assertEqual(len(st["undecoded"]), 1)
        d = compare.state_diff(compare.decode_state(_catalog()), st)
        self.assertFalse(d["equal"])
        self.assertIn("not in the job's encodings", compare.describe_state_diff(d))

    def test_a_lost_timer_is_a_difference(self):
        a = compare.decode_state(_catalog())
        b = dict(a, timers=[])
        d = compare.state_diff(a, b)
        self.assertFalse(d["equal"])
        self.assertEqual(d["sections"]["timers"]["missing_count"], 1)


class TestVerdict(unittest.TestCase):
    def _all(self, verdict=PASSED):
        r = Report("check")
        for cid, _ in REQUIRED:
            r.set(cid, verdict, "x")
        return r

    def test_every_required_check_passed_is_a_pass(self):
        self.assertEqual(self._all().result(), ("passed", 0))

    def test_a_failure_stays_a_failure_beside_what_could_not_run(self):
        r = self._all(UNAVAILABLE)
        r.set("behaviour.output", FAILED, "x")
        self.assertEqual(r.result(), ("failed", 1))

    def test_unavailable_or_unsupported_is_never_a_pass(self):
        for v in (UNAVAILABLE, UNSUPPORTED):
            r = self._all()
            r.set("recovery.candidate", v, "timeout")
            self.assertEqual(r.result(), ("not established", 3))

    def test_a_missing_required_check_is_not_established(self):
        r = Report("check")
        r.set("evidence.integrity", PASSED, "x")
        self.assertEqual(r.result()[1], 3)
        r.fill_unavailable("interrupted")
        self.assertEqual(r.result()[1], 3)
        self.assertEqual(r.verdict("restore.candidate"), UNAVAILABLE)

    def test_an_informational_failure_is_reported_and_never_decides(self):
        r = self._all()
        r.set("state.raw", FAILED, "raw bytes differ")
        self.assertEqual(r.result(), ("passed", 0))
        self.assertIn("state.raw", [c["id"] for c in r.document()["checks"]])


class TestClusterLimits(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="clink-upgrade-cl-"))
        node = self.root / "clink_node"
        node.write_text('#!/bin/sh\nexec "%s" "%s" "$@"\n' % (sys.executable, HERE / "fake_node.py"))
        node.chmod(node.stat().st_mode | stat.S_IXUSR)
        clink = self.root / "clink"
        clink.write_text("#!/bin/sh\nexit 0\n")
        clink.chmod(clink.stat().st_mode | stat.S_IXUSR)
        job = self.root / "job.so"
        job.write_text("")
        self.build = Build("candidate", str(clink), str(node), str(job))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def stopped(self, mode, limits=None, interrupted=lambda: "", **env):
        c = Cluster(self.build, list, str(self.root / ("run-" + mode)), limits or Limits(20, 4096, 256),
                    interrupted, env=dict(FAKE_NODE_MODE=mode, **env))
        try:
            c.start()
            with self.assertRaises(RunStopped) as cm:
                c.wait(lambda: False, "nothing")
            return cm.exception
        finally:
            c.stop()

    def test_a_process_that_exits_is_a_crash(self):
        e = self.stopped("exit")
        self.assertEqual(e.kind, "crash")
        self.assertIn("exit 3", e.detail)

    def test_a_process_killed_by_a_signal_names_it(self):
        e = self.stopped("killed")
        self.assertEqual(e.kind, "crash")
        self.assertIn("SIGKILL", e.detail)

    def test_the_time_limit_stops_the_run(self):
        e = self.stopped("run", Limits(1.5, 4096, 256))
        self.assertEqual(e.kind, "timeout")

    def test_the_memory_bound_stops_the_run(self):
        e = self.stopped("alloc", Limits(20, 64, 256), FAKE_NODE_MIB="200")
        self.assertEqual(e.kind, "memory")

    def test_the_output_bound_stops_the_run(self):
        e = self.stopped("write", Limits(20, 4096, 2))
        self.assertEqual(e.kind, "output")

    def test_an_interrupt_stops_the_run(self):
        t0 = time.monotonic()
        e = self.stopped("run", interrupted=lambda: "signal 2" if time.monotonic() - t0 > 1 else "")
        self.assertEqual(e.kind, "interrupted")


def namespace_answers(connect, loopback="ok"):
    """The two probes answered as a namespace would answer them, so a state
    does not depend on the network of the machine running the tests: the
    upgrade gate runs them in a container whose only interface is lo, where
    every real connection out is refused."""
    answers = {_CONNECT_OUT: connect, _LOOPBACK_PAIR: loopback}
    return mock.patch.object(isolate, "_run", lambda argv: answers[argv[-1]])


class TestIsolationStates(unittest.TestCase):
    def setUp(self):
        self.saved = {k: os.environ.get(k) for k in (NETNS_ENV, OUTSIDE_ENV)}
        self.net = Path(tempfile.mkdtemp(prefix="clink-upgrade-net-"))

    def tearDown(self):
        shutil.rmtree(self.net, ignore_errors=True)
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_linux_outside_the_namespace_must_reexecute(self):
        os.environ.pop(NETNS_ENV, None)
        (self.net / "lo").mkdir()
        (self.net / "eth0").mkdir()
        iso = RunIsolation(platform="Linux", sysnet=str(self.net))
        self.assertTrue(iso.needs_reexec())
        self.assertEqual(iso.establish(), "unavailable")

    def test_inside_a_namespace_that_still_reaches_out_is_not_enforced_or_inconclusive(self):
        os.environ[NETNS_ENV] = "1"
        os.environ[OUTSIDE_ENV] = "timeout"
        with namespace_answers(connect="timeout"):
            iso = RunIsolation(platform="Linux")
            self.assertFalse(iso.needs_reexec())
            self.assertEqual(iso.establish(), "not-enforced")
            self.assertFalse(iso.enforced)
            os.environ[OUTSIDE_ENV] = ""
            self.assertEqual(RunIsolation(platform="Linux").establish(), "inconclusive")

    def test_inside_a_namespace_that_refuses_with_loopback_working_is_enforced(self):
        os.environ[NETNS_ENV] = "1"
        os.environ[OUTSIDE_ENV] = "timeout"
        with namespace_answers(connect="ENETUNREACH"):
            iso = RunIsolation(platform="Linux")
            self.assertEqual(iso.establish(), "enforced")
            self.assertEqual(iso.wrap(["clink_node"]), ["clink_node"])
        with namespace_answers(connect="ENETUNREACH", loopback=""):
            self.assertEqual(RunIsolation(platform="Linux").establish(), "unavailable")


class TestAlreadyLoopbackOnly(unittest.TestCase):
    def setUp(self):
        self.net = Path(tempfile.mkdtemp(prefix="clink-upgrade-net-"))
        self.saved = os.environ.pop(NETNS_ENV, None)

    def tearDown(self):
        shutil.rmtree(self.net, ignore_errors=True)
        if self.saved is not None:
            os.environ[NETNS_ENV] = self.saved

    def test_a_namespace_with_only_lo_needs_no_reexecution_and_is_still_proven(self):
        (self.net / "lo").mkdir()
        iso = RunIsolation(platform="Linux", sysnet=str(self.net))
        self.assertTrue(iso.already_loopback_only())
        self.assertFalse(iso.needs_reexec())
        # The probes still decide: a connection that leaves is not enforced,
        # one refused before it leaves with loopback working is.
        with namespace_answers(connect="timeout"):
            self.assertEqual(iso.establish(), "not-enforced")
            self.assertFalse(iso.enforced)
        with namespace_answers(connect="ENETUNREACH"):
            iso = RunIsolation(platform="Linux", sysnet=str(self.net))
            self.assertEqual(iso.establish(), "enforced")
            self.assertEqual(iso.wrap(["clink_node"]), ["clink_node"])
        with namespace_answers(connect="ENETUNREACH", loopback=""):
            self.assertEqual(RunIsolation(platform="Linux", sysnet=str(self.net)).establish(),
                             "unavailable")

    def test_another_interface_means_reexecution(self):
        (self.net / "lo").mkdir()
        (self.net / "eth0").mkdir()
        iso = RunIsolation(platform="Linux", sysnet=str(self.net))
        self.assertFalse(iso.already_loopback_only())
        self.assertTrue(iso.needs_reexec())


class _Enforced:
    state, detail, mechanism, enforced = "enforced", "test", "test", True

    def wrap(self, argv):
        return list(argv)

    def needs_reexec(self):
        return False

    def establish(self):
        return self.state


class TestCommand(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="clink-upgrade-cmd-"))
        for name in ("clink", "clink_node"):
            p = self.root / name
            p.write_text("#!/bin/sh\nexit 0\n")
            p.chmod(p.stat().st_mode | stat.S_IXUSR)
        (self.root / "job.so").write_text("")
        self.ev = self.root / "evidence"
        (self.ev / "checkpoint").mkdir(parents=True)
        (self.ev / "checkpoint" / "dataset.json").write_text("{}")
        (self.ev / "test.json").write_bytes(EXAMPLE.read_bytes())
        self.digest = evidence.write_index(str(self.ev), {
            "checkpoint": {"dataset_digest": hashlib.sha256(b"{}").hexdigest()},
            "test": {"sha256": definition.load(EXAMPLE).sha256}})

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def argv(self, out, digest):
        r = str(self.root)
        a = ["check", "--test=%s" % EXAMPLE, "--evidence=%s" % self.ev, "--evidence-digest=%s" % digest,
             "--out=%s" % out, "--tools-clink=%s/clink" % r]
        for b in ("baseline", "candidate"):
            a += ["--%s-clink=%s/clink" % (b, r), "--%s-node=%s/clink_node" % (b, r), "--%s-job=%s/job.so" % (b, r)]
        return a

    def run_check(self, digest, out="out"):
        return upgrade_cli.main(self.argv(self.root / out, digest), isolation=_Enforced())

    def report(self, out="out"):
        return json.loads((self.root / out / "report.json").read_text())

    def test_a_wrong_digest_fails_before_any_build_runs(self):
        self.assertEqual(self.run_check("1" * 64), 1)
        r = self.report()
        checks = {c["id"]: c["verdict"] for c in r["checks"]}
        self.assertEqual(checks["evidence.integrity"], "failed")
        self.assertEqual(checks["restore.candidate"], "unavailable")
        self.assertNotIn("builds", r)

    def test_truncated_evidence_fails(self):
        (self.ev / "checkpoint" / "dataset.json").write_text("{")
        self.assertEqual(self.run_check(self.digest), 1)
        self.assertIn("truncated", self.report()["checks"][0]["reason"])

    def test_an_unsupported_evidence_version_is_not_established(self):
        idx = self.ev / evidence.INDEX
        doc = json.loads(idx.read_text())
        doc["version"] = 2
        text = json.dumps(doc).encode()
        idx.write_bytes(text)
        self.assertEqual(self.run_check(hashlib.sha256(text).hexdigest()), 3)
        self.assertEqual(self.report()["checks"][0]["verdict"], "unsupported")

    def test_an_existing_output_directory_is_bad_usage(self):
        (self.root / "taken").mkdir()
        self.assertEqual(self.run_check(self.digest, out="taken"), 2)

    def test_a_build_that_is_not_there_is_bad_usage(self):
        (self.root / "clink_node").unlink()
        self.assertEqual(self.run_check(self.digest), 2)


if __name__ == "__main__":
    unittest.main()
