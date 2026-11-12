"""Loopback-only isolation for every process a build runs, proven first.

A build's Coordinator, Worker and CLI talk to each other over 127.0.0.1 and
must reach nothing else. On macOS each process runs under sandbox-exec with
clink_audit.isolation's loopback profile, which the host's loopback serves for
all of them. On Linux a network namespace is per process, so separately
wrapped processes could not reach each other: there the gate re-executes
itself once inside a new user and network namespace with only `lo` up
(`unshare -rn`, then netns_exec.py brings `lo` up), and everything it starts
shares it. A process already in a namespace whose only interface is `lo`
(a container started with `--network none`) needs no re-execution: the
namespace is the mechanism, and the same probes prove it.

Proof, before any build runs: a connection to 192.0.2.1 (TEST-NET-1, which no
network answers) is attempted without the isolation and must not be denied,
and under it must be denied before a packet leaves (EPERM or EACCES from the
sandbox, ENETUNREACH in the namespace); and two sockets on 127.0.0.1 must
connect under it. The states are clink_audit.isolation's: enforced,
unavailable, not-enforced, inconclusive. Only enforced lets a build run.
"""

import os
import platform as _platform
import shutil
import subprocess
import sys

from ..isolation import _CONNECT_OUT, _DENIED, _LOOPBACK_PAIR, LoopbackIsolation

NETNS_ENV = "CLINK_UPGRADE_NETNS"
OUTSIDE_ENV = "CLINK_UPGRADE_OUTSIDE_CONNECT"


def _run(argv):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=20).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


class RunIsolation:
    def __init__(self, platform=None, inner=None, sysnet="/sys/class/net"):
        self._platform = platform or _platform.system()
        self._inner = inner  # a test passes its own (e.g. LoopbackIsolation(wrapper=[...]))
        self._sysnet = sysnet
        self.state = ""
        self.detail = ""
        self.mechanism = ""
        self._wrap = None

    def already_loopback_only(self):
        """Linux: this process's network namespace has no interface but lo,
        as in a container started with --network none."""
        try:
            return self._platform == "Linux" and sorted(os.listdir(self._sysnet)) == ["lo"]
        except OSError:
            return False

    def needs_reexec(self):
        """On Linux, outside a loopback-only namespace: the caller must
        re-execute itself under reexec_argv() before establishing anything."""
        return (self._inner is None and self._platform == "Linux"
                and os.environ.get(NETNS_ENV) != "1" and not self.already_loopback_only())

    def reexec_argv(self, argv):
        """The command that re-executes `argv` in a new user and network
        namespace with loopback up (netns_exec.py sets it up; no `ip` is
        needed)."""
        unshare = shutil.which("unshare")
        if not unshare:
            return None
        helper = os.path.join(os.path.dirname(os.path.abspath(__file__)), "netns_exec.py")
        return [unshare, "-rn", sys.executable, "-I", helper, "--"] + list(argv)

    def outside_probe(self):
        return _run([sys.executable, "-I", "-c", _CONNECT_OUT])

    def establish(self):
        if self._inner is not None or self._platform != "Linux":
            iso = self._inner or LoopbackIsolation(platform=self._platform)
            iso.establish()
            self.state, self.detail, self.mechanism = iso.state, iso.detail, iso.mechanism
            self._wrap = iso.wrap if iso.enforced else None
            return self.state
        if os.environ.get(NETNS_ENV) != "1" and self.already_loopback_only():
            # The environment is the mechanism: there is no interface to reach
            # anything but loopback through, which is itself what is checked,
            # and the connection must be refused before a packet leaves.
            self.mechanism = "this process's network namespace, whose only interface is lo"
            inside = _run([sys.executable, "-I", "-c", _CONNECT_OUT])
            pair = _run([sys.executable, "-I", "-c", _LOOPBACK_PAIR])
            if pair != "ok":
                self.state, self.detail = "unavailable", "loopback does not work in this namespace"
            elif inside not in _DENIED:
                self.state = "not-enforced"
                self.detail = "a connection to 192.0.2.1 was %s here" % (inside or "not attempted")
            else:
                self.state = "enforced"
                self.detail = ("%s: %s lists only lo, a connection to 192.0.2.1 was %s, and "
                               "loopback works" % (self.mechanism, self._sysnet, inside))
                self._wrap = list
            return self.state
        self.mechanism = "one user and network namespace for the whole run, lo up"
        if os.environ.get(NETNS_ENV) != "1":
            self.state = "unavailable"
            self.detail = "the gate is not running inside its network namespace"
            return self.state
        plain = os.environ.get(OUTSIDE_ENV, "")
        inside = _run([sys.executable, "-I", "-c", _CONNECT_OUT])
        pair = _run([sys.executable, "-I", "-c", _LOOPBACK_PAIR])
        if pair != "ok":
            self.state = "unavailable"
            self.detail = "loopback does not work inside the namespace"
        elif plain == "" or plain in _DENIED:
            self.state = "inconclusive"
            self.detail = ("a connection to 192.0.2.1 was already %s outside the namespace"
                           % (plain or "not attempted"))
        elif inside not in _DENIED:
            self.state = "not-enforced"
            self.detail = "a connection to 192.0.2.1 was %s inside the namespace" % (inside or "not attempted")
        else:
            self.state = "enforced"
            self.detail = ("%s: a connection to 192.0.2.1 was %s outside and %s inside, and "
                           "loopback works inside" % (self.mechanism, plain, inside))
            self._wrap = list
        return self.state

    @property
    def enforced(self):
        return self.state == "enforced"

    def wrap(self, argv):
        if not self.enforced:
            raise RuntimeError("isolation is %s: %s" % (self.state or "not established", self.detail))
        return self._wrap(list(argv))
