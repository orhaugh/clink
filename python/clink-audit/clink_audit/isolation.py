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

`LoopbackIsolation` is the variant for a command that needs loopback and
nothing else: `clink state-query`, whose embedded engine runs a Coordinator
and Workers that talk over 127.0.0.1. It denies every address but loopback:
a sandbox-exec profile that allows binding, accepting and connecting on
localhost alone on macOS, and on Linux a new user and network namespace with
only `lo` brought up. Its proof cannot use a loopback endpoint, which it must
leave reachable, so it uses one no network answers: a connection to
192.0.2.1 (TEST-NET-1, RFC 5737) is attempted without it, and must be denied
under it before any packet leaves (EPERM or EACCES from the sandbox,
ENETUNREACH in the empty namespace); and a connection between two sockets
on 127.0.0.1 must work under it. The states are the same four.
"""

import http.server
import platform as _platform
import shutil
import subprocess
import sys
import threading
import urllib.request

SANDBOX_PROFILE = "(version 1)(allow default)(deny network*)"
LOOPBACK_PROFILE = ('(version 1)(allow default)(deny network*)'
                    '(allow network-bind (local ip "localhost:*"))'
                    '(allow network-inbound (local ip "localhost:*"))'
                    '(allow network-outbound (remote ip "localhost:*"))'
                    '(allow network* (remote unix-socket))')

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


_CONNECT_OUT = """
import errno, socket
s = socket.socket()
s.settimeout(1.0)
try:
    s.connect(("192.0.2.1", 9))
    print("connected")
except socket.timeout:
    print("timeout")
except OSError as e:
    print(errno.errorcode.get(e.errno, str(e.errno)))
"""

_LOOPBACK_PAIR = """
import socket
srv = socket.socket()
srv.bind(("127.0.0.1", 0))
srv.listen(1)
c = socket.create_connection(srv.getsockname(), timeout=2)
a, _ = srv.accept()
c.sendall(b"ok")
print(a.recv(2).decode())
"""

_DENIED = {"EPERM", "EACCES", "ENETUNREACH"}


class LoopbackIsolation:
    def __init__(self, platform=None, wrapper=None, mechanism=None):
        self._platform = platform
        self._wrapper_override = wrapper
        self._mechanism = mechanism
        self.state = ""
        self.detail = ""
        self.mechanism = ""
        self.wrapper = []

    def _make_wrapper(self):
        if self._wrapper_override is not None:
            self.mechanism = self._mechanism or "test"
            return list(self._wrapper_override)
        system = self._platform or _platform.system()
        if system == "Darwin" and shutil.which("sandbox-exec"):
            self.mechanism = "sandbox-exec, loopback only"
            return [shutil.which("sandbox-exec"), "-p", LOOPBACK_PROFILE]
        if system == "Linux" and shutil.which("unshare") and shutil.which("ip"):
            self.mechanism = "unshare -rn, lo up"
            return [shutil.which("unshare"), "-rn", "sh", "-c", 'ip link set lo up && exec "$@"', "sh"]
        self.mechanism = "none"
        return None

    @staticmethod
    def _run(argv):
        try:
            return subprocess.run(argv, capture_output=True, text=True, timeout=20).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""

    def establish(self):
        wrapper = self._make_wrapper()
        if not wrapper:
            self.state = "unavailable"
            self.detail = "no loopback-only isolation can run here"
            return self.state
        if self._run(wrapper + [sys.executable, "-I", "-c", _LOOPBACK_PAIR]) != "ok":
            self.state = "unavailable"
            self.detail = "%s does not run a command with loopback working" % self.mechanism
            return self.state
        plain = self._run([sys.executable, "-I", "-c", _CONNECT_OUT])
        wrapped = self._run(wrapper + [sys.executable, "-I", "-c", _CONNECT_OUT])
        if plain == "" or plain in _DENIED:
            self.state = "inconclusive"
            self.detail = ("a connection to 192.0.2.1 is already %s without isolation, so its "
                           "denial under %s would prove nothing" % (plain or "not attempted",
                                                                   self.mechanism))
        elif wrapped not in _DENIED:
            self.state = "not-enforced"
            self.detail = "a connection to 192.0.2.1 was %s under %s" % (wrapped or "not attempted",
                                                                       self.mechanism)
        else:
            self.state = "enforced"
            self.detail = ("%s: a connection to 192.0.2.1 was %s without it and %s under it, and "
                           "loopback works under it" % (self.mechanism, plain, wrapped))
        self.wrapper = wrapper if self.state == "enforced" else []
        return self.state

    @property
    def enforced(self):
        return self.state == "enforced"

    def wrap(self, argv):
        if not self.enforced:
            raise RuntimeError("loopback isolation is %s: %s" % (self.state or "not established",
                                                                self.detail))
        return list(self.wrapper) + list(argv)
