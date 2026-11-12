"""The isolation proof the runner requires before any verification runs.

Each state is reached by giving the class a mechanism, a platform, a wrapper
or a control endpoint, which only a test can do; the platform's own
isolation is exercised where this machine allows it.
"""
import platform
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from clink_audit.isolation import Isolation, LoopbackIsolation  # noqa: E402


class IsolationProof(unittest.TestCase):
    def test_no_mechanism_on_another_platform_is_unavailable(self):
        iso = Isolation(platform="Plan9")
        self.assertEqual(iso.establish(), "unavailable")
        self.assertFalse(iso.enforced)
        with self.assertRaises(RuntimeError):
            iso.wrap(["true"])

    def test_a_wrapper_that_runs_nothing_isolates_nothing(self):
        iso = Isolation(mechanism="test", wrapper=["false"])
        self.assertEqual(iso.establish(), "unavailable")
        self.assertIn("does not run a command", iso.detail)

    def test_a_wrapper_that_lets_the_endpoint_answer_is_not_enforced(self):
        iso = Isolation(mechanism="test", wrapper=["env"])
        self.assertEqual(iso.establish(), "not-enforced")
        self.assertIn("still answered", iso.detail)
        self.assertFalse(iso.enforced)

    def test_an_endpoint_that_never_answers_is_inconclusive(self):
        iso = Isolation(mechanism="test", wrapper=["env"], probe_url="http://127.0.0.1:1/")
        self.assertEqual(iso.establish(), "inconclusive")
        self.assertFalse(iso.enforced)

    def test_the_platform_mechanism_blocks_the_endpoint_where_it_can_run(self):
        iso = Isolation()
        state = iso.establish()
        if state == "unavailable":
            self.skipTest("no isolation can run on this machine: %s" % iso.detail)
        self.assertEqual(state, "enforced", iso.detail)
        r = subprocess.run(iso.wrap([sys.executable, "-I", "-c", "print('ran')"]),
                           capture_output=True, text=True, timeout=30)
        self.assertEqual(r.stdout.strip(), "ran")
        expected = {"Darwin": "sandbox-exec", "Linux": "unshare"}.get(platform.system())
        self.assertEqual(iso.mechanism, expected)


class LoopbackIsolationProof(unittest.TestCase):
    def test_no_mechanism_on_another_platform_is_unavailable(self):
        iso = LoopbackIsolation(platform="Plan9")
        self.assertEqual(iso.establish(), "unavailable")
        with self.assertRaises(RuntimeError):
            iso.wrap(["true"])

    def test_a_wrapper_that_runs_nothing_isolates_nothing(self):
        iso = LoopbackIsolation(wrapper=["false"])
        self.assertEqual(iso.establish(), "unavailable")
        self.assertFalse(iso.enforced)

    def test_a_wrapper_that_lets_the_connection_through_is_not_enforced(self):
        iso = LoopbackIsolation(wrapper=["env"])
        state = iso.establish()
        if state == "inconclusive":
            self.skipTest("this machine cannot attempt a connection to 192.0.2.1: %s" % iso.detail)
        self.assertEqual(state, "not-enforced", iso.detail)
        self.assertFalse(iso.enforced)

    def test_the_platform_mechanism_denies_the_network_and_keeps_loopback(self):
        iso = LoopbackIsolation()
        state = iso.establish()
        if state in ("unavailable", "inconclusive"):
            self.skipTest("loopback isolation cannot be proven here: %s" % iso.detail)
        self.assertEqual(state, "enforced", iso.detail)
        pair = ("import socket\ns = socket.socket()\ns.bind(('127.0.0.1', 0))\ns.listen(1)\n"
                "c = socket.create_connection(s.getsockname(), timeout=2)\nprint('ok')")
        r = subprocess.run(iso.wrap([sys.executable, "-I", "-c", pair]), capture_output=True,
                           text=True, timeout=30)
        self.assertEqual(r.stdout.strip(), "ok")


if __name__ == "__main__":
    unittest.main()
