"""Network isolation for offline verification, proven before it is relied on.

The same contract as scripts/network-isolation.sh, which the incident
example and gate use: the mechanism is `sandbox-exec` with a profile that
denies every network operation on macOS, and `unshare -rn` (a new user and
network namespace, its loopback down) on Linux. Before any verification runs
under it, it must run a command, and a control endpoint on 127.0.0.1 must
answer a request made without it and not answer the same request made under
it. The result is:

    enforced       the mechanism ran and blocked the control endpoint
    unavailable    no mechanism can run here
    not-enforced   the mechanism ran, and the endpoint still answered under it
    inconclusive   the endpoint did not answer even without isolation

Only `enforced` lets a verification run; anything else leaves the
verification unavailable, so the audit is partial. There is no override: the
mechanism, platform and control endpoint can be replaced only by a test that
constructs this class itself.
"""

import http.server
import platform as _platform
import shutil
import subprocess
import sys
import threading
import urllib.request

SANDBOX_PROFILE = "(version 1)(allow default)(deny network*)"

_PROBE = ("import sys, urllib.request\n"
          "urllib.request.urlopen(sys.argv[1], timeout=3).read()\n")


class _Answer(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"answered")

    def log_message(self, *args):
        pass


class Isolation:
    def __init__(self, mechanism=None, platform=None, probe_url=None, wrapper=None):
        self._mechanism = mechanism
        self._platform = platform
        self._probe_url = probe_url
        self._wrapper_override = wrapper
        self.state = ""
        self.detail = ""
        self.mechanism = ""
        self.wrapper = []

    def _choose(self):
        if self._mechanism:
            return self._mechanism
        system = self._platform or _platform.system()
        return {"Darwin": "sandbox-exec", "Linux": "unshare"}.get(system, "none")

    def _make_wrapper(self, mechanism):
        if self._wrapper_override is not None:
            return list(self._wrapper_override)
        if mechanism == "sandbox-exec":
            exe = shutil.which("sandbox-exec")
            if not exe:
                return None
            w = [exe, "-p", SANDBOX_PROFILE]
        elif mechanism == "unshare":
            exe = shutil.which("unshare")
            if not exe:
                return None
            w = [exe, "-rn"]
        else:
            return None
        try:
            ok = subprocess.run(w + ["true"], capture_output=True, timeout=20).returncode == 0
        except (OSError, subprocess.SubprocessError):
            ok = False
        return w if ok else None

    @staticmethod
    def _answers(url, wrapper=()):
        try:
            r = subprocess.run(list(wrapper) + [sys.executable, "-I", "-c", _PROBE, url],
                               capture_output=True, timeout=20)
        except (OSError, subprocess.SubprocessError):
            return False
        return r.returncode == 0

    def establish(self):
        self.mechanism = self._choose()
        wrapper = self._make_wrapper(self.mechanism)
        if not wrapper:
            self.state = "unavailable"
            self.detail = "no network isolation can run here (%s)" % self.mechanism
            return self.state
        # A wrapper that runs nothing would block the probe too.
        try:
            ran = subprocess.run(wrapper + [sys.executable, "-I", "-c", "print('ran')"],
                                 capture_output=True, text=True, timeout=20).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            ran = ""
        if ran != "ran":
            self.state = "unavailable"
            self.detail = "%s does not run a command here, so it isolates nothing" % self.mechanism
            return self.state
        server = None
        url = self._probe_url
        if url is None:
            server = http.server.HTTPServer(("127.0.0.1", 0), _Answer)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            url = "http://127.0.0.1:%d/" % server.server_address[1]
        try:
            if not self._answers(url):
                self.state = "inconclusive"
                self.detail = ("the control endpoint %s did not answer even without isolation, so a "
                               "probe under %s would prove nothing" % (url, self.mechanism))
            elif self._answers(url, wrapper):
                self.state = "not-enforced"
                self.detail = "%s ran, and the control endpoint %s still answered under it" % (
                    self.mechanism, url)
            else:
                self.state = "enforced"
                self.detail = "%s blocked a local control endpoint that answered without it" % (
                    self.mechanism)
        finally:
            if server is not None:
                server.shutdown()
                server.server_close()
        self.wrapper = wrapper if self.state == "enforced" else []
        return self.state

    @property
    def enforced(self):
        return self.state == "enforced"

    def wrap(self, argv):
        if not self.enforced:
            raise RuntimeError("isolation is %s: %s" % (self.state or "not established", self.detail))
        return list(self.wrapper) + list(argv)
