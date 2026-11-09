"""scripts/network-isolation.sh and the incident gate's use of it: isolation is
proven before verification relies on it, every way it can fail to be is told
apart, and nothing short of proven isolation qualifies.

    python3 -m unittest discover -s scripts/tests -v

Each case takes its path on any platform: the mechanism, the platform a
mechanism is chosen for, and the control endpoint are injected where the
platform does not offer them. The one case that needs the platform's own
isolation runs where it can (sandbox-exec on macOS, unshare -rn on Linux where
user namespaces are allowed) and is skipped, saying why, elsewhere.
"""
import http.server
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent
LIB = SCRIPTS / "network-isolation.sh"
GATE = SCRIPTS / "incident-gate.sh"
OVERRIDES = ("ALLOW_UNISOLATED", "CLINK_ISOLATION", "CLINK_ISOLATION_COMMAND",
             "CLINK_ISOLATION_OS", "CLINK_ISOLATION_PROBE_URL")


def env_with(**extra):
    env = {k: v for k, v in os.environ.items() if k not in OVERRIDES}
    env.update(extra)
    return env


def sourced(snippet, **extra):
    """Run `snippet` in bash with the library sourced; returns the result."""
    return subprocess.run(["bash", "-c", f'set -u; . "{LIB}"; {snippet}'],
                          env=env_with(**extra), capture_output=True, text=True, timeout=120)


def state(**extra):
    r = sourced('isolation_establish; echo "state=${ISOLATION_STATE}"; '
                'echo "detail=${ISOLATION_DETAIL}"', **extra)
    out = dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)
    return out.get("state"), out.get("detail", "")


def closed_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Endpoint:
    """A local HTTP endpoint that answers every request, for the duration."""

    def __enter__(self):
        class Answer(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"answered")

            def log_message(self, *args):
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Answer)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/"
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()


def native_mechanism_runs():
    r = sourced('isolation_wrapper "$(isolation_mechanism)" && echo yes')
    return r.stdout.strip() == "yes"


PROBE = "import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=3).read()"


class NetworkIsolation(unittest.TestCase):
    def test_each_platform_names_its_own_mechanism(self):
        for system, mechanism in (("Darwin", "sandbox-exec"), ("Linux", "unshare"),
                                  ("FreeBSD", "none")):
            r = sourced("isolation_mechanism", CLINK_ISOLATION_OS=system)
            self.assertEqual(r.stdout.strip(), mechanism, system)

    @unittest.skipUnless(native_mechanism_runs(),
                         "this machine cannot run its platform's isolation (sandbox-exec on "
                         "macOS, unshare -rn on Linux with user namespaces allowed)")
    def test_the_platforms_isolation_is_proven_and_blocks_a_local_endpoint(self):
        got, detail = state()
        self.assertEqual(got, "enforced", detail)
        with Endpoint() as endpoint:
            # The same request, without and then under offline().
            reach = f'python3 -I -c "{PROBE}" {endpoint.url}'
            r = sourced(f'{reach} && echo direct-ok; isolation_establish && '
                        f'{{ offline {reach} && echo isolated-ok || echo isolated-blocked; }}; '
                        'isolation_qualifies && echo qualifies')
        self.assertIn("direct-ok", r.stdout)
        self.assertIn("isolated-blocked", r.stdout, r.stderr)
        self.assertNotIn("isolated-ok", r.stdout)
        self.assertIn("qualifies", r.stdout)

    def test_a_mechanism_that_cannot_run_here_is_unavailable_and_refused(self):
        # "none", and whichever of the two platform mechanisms this machine
        # lacks (unshare on macOS, sandbox-exec on Linux), take the same path.
        candidates = ["none"] + [m for m in ("sandbox-exec", "unshare")
                                 if sourced(f"isolation_wrapper {m} && echo yes").stdout.strip()
                                 != "yes"]
        for mechanism in candidates:
            with tempfile.TemporaryDirectory() as d:
                marker = Path(d) / "ran"
                r = sourced(f'isolation_establish; echo "state=${{ISOLATION_STATE}}"; '
                            f'offline touch "{marker}"; echo "rc=$?"', CLINK_ISOLATION=mechanism)
                self.assertIn("state=unavailable", r.stdout, mechanism)
                self.assertIn("rc=2", r.stdout, mechanism)
                self.assertIn("refusing to verify without network isolation", r.stderr)
                self.assertFalse(marker.exists(), f"{mechanism}: the command ran unisolated")

    def test_a_wrapper_that_does_not_isolate_is_caught(self):
        got, detail = state(CLINK_ISOLATION="command", CLINK_ISOLATION_COMMAND="env")
        self.assertEqual(got, "not-enforced")
        self.assertIn("still answered", detail)
        r = sourced('isolation_establish; offline true; echo "rc=$?"',
                    CLINK_ISOLATION="command", CLINK_ISOLATION_COMMAND="env")
        self.assertIn("rc=2", r.stdout)

    def test_a_wrapper_that_runs_nothing_isolates_nothing(self):
        # It would block the probe too; without the check that it runs a
        # command, it would pass for isolation.
        got, detail = state(CLINK_ISOLATION="command", CLINK_ISOLATION_COMMAND="false")
        self.assertEqual(got, "unavailable")
        self.assertIn("does not run a command", detail)

    def test_an_endpoint_that_never_answered_is_inconclusive_not_isolation(self):
        url = f"http://127.0.0.1:{closed_port()}/"
        # With a wrapper that does not isolate, the unreachable endpoint would
        # otherwise pass for a block: the positive control tells them apart.
        got, detail = state(CLINK_ISOLATION="command", CLINK_ISOLATION_COMMAND="env",
                            CLINK_ISOLATION_PROBE_URL=url)
        self.assertEqual(got, "inconclusive")
        self.assertIn("did not answer even without isolation", detail)
        with Endpoint() as endpoint:
            got, _ = state(CLINK_ISOLATION="command", CLINK_ISOLATION_COMMAND="env",
                           CLINK_ISOLATION_PROBE_URL=endpoint.url)
        self.assertEqual(got, "not-enforced", "the same wrapper against a live endpoint")

    def test_the_development_override_runs_but_never_qualifies(self):
        with tempfile.TemporaryDirectory() as d:
            marker = Path(d) / "ran"
            r = sourced(f'isolation_establish; offline touch "{marker}"; echo "rc=$?"; '
                        'isolation_qualifies && echo qualifies || echo not-a-qualification',
                        CLINK_ISOLATION="none", ALLOW_UNISOLATED="1")
            self.assertTrue(marker.exists())
        self.assertIn("rc=0", r.stdout)
        self.assertIn("not-a-qualification", r.stdout)
        self.assertIn("WITHOUT network isolation", r.stderr)

    def test_the_gate_takes_no_override(self):
        for override, value in (("ALLOW_UNISOLATED", "1"), ("CLINK_ISOLATION", "none"),
                                ("CLINK_ISOLATION_COMMAND", "env"),
                                ("CLINK_ISOLATION_PROBE_URL", "http://127.0.0.1:1/")):
            r = subprocess.run(["bash", str(GATE), "/nonexistent"], env=env_with(**{override: value}),
                               capture_output=True, text=True, timeout=120)
            self.assertEqual(r.returncode, 2, override)
            self.assertIn(f"{override} is set", r.stderr)

    def test_the_gate_fails_where_isolation_cannot_be_proven(self):
        # A PATH with the tools the gate's first step needs and neither
        # isolation mechanism: the platform's own choice is then unavailable.
        with tempfile.TemporaryDirectory() as d:
            bin_dir = Path(d)
            for tool in ("bash", "python3", "uname", "mktemp", "cat", "seq", "sleep", "rm",
                         "dirname", "kill"):
                found = shutil.which(tool)
                if found:
                    (bin_dir / tool).symlink_to(found)
            r = subprocess.run([shutil.which("bash"), str(GATE), "/nonexistent"],
                               env=env_with(PATH=str(bin_dir)), capture_output=True, text=True,
                               timeout=120)
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIn("FAILED: network isolation is unavailable", r.stderr)


if __name__ == "__main__":
    unittest.main()
