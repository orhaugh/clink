"""One build's local cluster: a Coordinator and a Worker from its clink_node,
driven with its own clink, every process under the run's isolation.

A run is bounded. A monitor samples it while it waits and stops the whole run
when its time limit passes, when the resident memory of its processes passes
the bound, when the bytes it has written (sink output, logs, checkpoints) pass
theirs, when a process exits that should still be running, or when the gate
is interrupted. Each is a RunStopped with its kind, so a crash, a timeout and a
limit are told apart in the report and none of them reads as success.

Every command is recorded with its exit status, so the report can say what was
run, and logs stay under the run's directory.
"""

import json
import os
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

POLL_S = 0.2


class RunStopped(Exception):
    def __init__(self, kind, detail):
        super().__init__("%s: %s" % (kind, detail))
        self.kind = kind      # timeout | memory | output | crash | interrupted
        self.detail = detail


class Limits:
    def __init__(self, run_timeout_s=180.0, max_rss_mib=4096, max_output_mib=256):
        self.run_timeout_s = float(run_timeout_s)
        self.max_rss_mib = int(max_rss_mib)
        self.max_output_mib = float(max_output_mib)

    def document(self):
        return {"run_timeout_s": self.run_timeout_s, "max_rss_mib": self.max_rss_mib,
                "max_output_mib": self.max_output_mib}


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _rss_kib(pids):
    """The summed resident memory of `pids` in KiB, from /proc where there is
    one and ps otherwise; None when neither can be read, so a bound that
    cannot be held is never taken as one that was."""
    if not pids:
        return 0
    if os.path.isdir("/proc/self"):
        total = 0
        for pid in pids:
            try:
                with open("/proc/%d/status" % pid) as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            total += int(line.split()[1])
                            break
            except (OSError, ValueError):
                pass  # exited since it was listed
        return total
    try:
        r = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(str(p) for p in pids)],
                           capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    values = [int(x) for x in r.stdout.split() if x.isdigit()]
    return sum(values) if values or r.returncode in (0, 1) else None


def _bytes_under(root):
    total = 0
    for d, _, files in os.walk(root):
        for f in files:
            try:
                total += os.lstat(os.path.join(d, f)).st_size
            except OSError:
                pass
    return total


def _signal_name(code):
    try:
        return signal.Signals(-code).name
    except (ValueError, TypeError):
        return "signal %d" % -code


class Cluster:
    def __init__(self, build, wrap, root, limits, interrupted, env=None, commands=None):
        self.build = build
        self.wrap = wrap
        self.root = os.path.abspath(root)
        self.limits = limits
        self.interrupted = interrupted
        self.env = dict(env or {})
        self.commands = commands if commands is not None else []
        for d in ("logs", "checkpoints", "ha", "out", "feed"):
            os.makedirs(os.path.join(self.root, d), exist_ok=True)
        self.procs = {}
        self.deadline = time.monotonic() + limits.run_timeout_s
        self.peak_rss_mib = 0.0
        self.expected_exit = False
        self.control = self.http = None

    # ---- paths ----------------------------------------------------------------------

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    @property
    def checkpoints(self):
        return self.path("checkpoints")

    @property
    def out(self):
        return self.path("out")

    @property
    def feed(self):
        return self.path("feed")

    # ---- bounds ---------------------------------------------------------------------

    def check(self):
        why = self.interrupted()
        if why:
            raise RunStopped("interrupted", why)
        # A process that has exited is a fact, whatever else is late.
        for name, p in self.procs.items():
            code = p.poll()
            if code is not None and not self.expected_exit:
                how = ("killed by %s" % _signal_name(code)) if code < 0 else ("exit %d" % code)
                raise RunStopped("crash", "%s %s exited unexpectedly (%s); its log is %s"
                                 % (self.build.name, name, how, self.path("logs", name + ".log")))
        if time.monotonic() > self.deadline:
            raise RunStopped("timeout", "the run passed its time limit of %g s"
                             % self.limits.run_timeout_s)
        kib = _rss_kib([p.pid for p in self.procs.values() if p.poll() is None])
        if kib is None:
            raise RunStopped("memory", "the run's resident memory could not be read, so its bound "
                             "of %d MiB cannot be held" % self.limits.max_rss_mib)
        rss = kib / 1024.0
        self.peak_rss_mib = max(self.peak_rss_mib, rss)
        if rss > self.limits.max_rss_mib:
            raise RunStopped("memory", "the run's processes held %.0f MiB, over the bound of %d MiB"
                             % (rss, self.limits.max_rss_mib))
        written = _bytes_under(self.root) / (1 << 20)
        if written > self.limits.max_output_mib:
            raise RunStopped("output", "the run wrote %.2f MiB, over the bound of %g MiB"
                             % (written, self.limits.max_output_mib))

    def wait(self, predicate, what):
        while True:
            self.check()
            got = predicate()
            if got:
                return got
            time.sleep(POLL_S)

    # ---- processes -------------------------------------------------------------------

    def _env(self):
        e = {k: os.environ[k] for k in ("PATH", "HOME", "LANG", "TMPDIR", "TZ") if k in os.environ}
        e.update(self.env)
        return e

    def _spawn(self, name, args):
        log = open(self.path("logs", name + ".log"), "wb")
        argv = self.wrap([self.build.clink_node] + args)
        self.commands.append({"build": self.build.name, "argv": argv, "process": name})
        self.procs[name] = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=log,
                                            stderr=subprocess.STDOUT, env=self._env(),
                                            start_new_session=True, close_fds=True)

    def _health(self, port):
        try:
            urllib.request.urlopen("http://127.0.0.1:%d/api/v1/health" % port, timeout=1).read()
            return True
        except (OSError, urllib.error.URLError):
            return False

    def start(self):
        self.control, self.http = free_port(), free_port()
        self._spawn("coordinator", ["--role=coordinator", "--bind-host=127.0.0.1",
                                    "--port=%d" % self.control, "--http-port=%d" % self.http,
                                    "--ha-dir=%s" % self.path("ha")])
        self.wait(lambda: self._health(self.http), "the Coordinator to answer")
        wh = free_port()
        self._spawn("worker-1", ["--role=worker", "--id=worker-1", "--coordinator-host=127.0.0.1",
                                 "--coordinator-port=%d" % self.control, "--port=%d" % free_port(),
                                 "--data-host=127.0.0.1", "--slots=16", "--http-port=%d" % wh])
        self.wait(lambda: self._health(wh), "the Worker to answer")

    def stop(self):
        self.expected_exit = True
        for p in self.procs.values():
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError):
                    pass
        for p in self.procs.values():
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                p.wait()

    # ---- the build's own CLI -------------------------------------------------------------

    def ctl(self, *args, timeout_s=60, env=None):
        """Runs the build's clink against this cluster's Coordinator."""
        argv = self.wrap([self.build.clink] + list(args))
        e = self._env()
        e.update(env or {})
        # The command runs while the cluster is watched: a Worker that dies
        # under a savepoint stops the run at once rather than when the
        # savepoint gives up.
        n = len(self.commands)
        log = self.path("logs", "cli-%d.log" % n)
        self.commands.append({"build": self.build.name, "argv": argv, "exit": None, "log": log})
        give_up = time.monotonic() + timeout_s
        with open(log, "wb") as f:
            p = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=f, stderr=subprocess.STDOUT,
                                 env=e, start_new_session=True)
        try:
            while p.poll() is None:
                if time.monotonic() > give_up:
                    break
                self.check()
                time.sleep(POLL_S)
        finally:
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                p.wait()
        code = p.returncode if time.monotonic() <= give_up else None
        self.commands[n]["exit"] = code
        with open(log, "rb") as f:
            out = f.read().decode(errors="replace")
        if code is None:
            out = "gave up after %g s: %s" % (timeout_s, out[-400:])
        self.check()
        return code, out

    def submit(self, restore=None):
        args = ["run", "--job=%s" % self.build.job, "--coordinator-host=127.0.0.1",
                "--coordinator-port=%d" % self.control, "--name=upgrade-check",
                "--checkpoint-dir=%s" % self.checkpoints, "--checkpoint-interval-ms=3600000",
                "--wait-timeout-s=0", "--max-restarts-on-worker-loss=0"]
        if restore is not None:
            args += ["--restore-from-dir=%s" % restore[0], "--restore-from-checkpoint-id=%d" % restore[1]]
        code, out = self.ctl(*args, timeout_s=90,
                             env={"CLINK_UC_FEED": self.feed, "CLINK_UC_OUT": self.out})
        m = re.search(r'"job_id":(\d+)', out)
        ok = code == 0 and '"ok":true' in out and m is not None
        return ok, (int(m.group(1)) if m else None), out.strip()

    def savepoint(self, job):
        code, out = self.ctl("savepoint", "--job-id=%d" % job, "--timeout-s=60",
                             "--coordinator-port=%d" % self.control, timeout_s=90)
        m = re.search(r"ok=1\b.*\bid=(\d+)", out)
        return (int(m.group(1)) if code == 0 and m else None), out.strip()

    def cancel(self, job):
        return self.ctl("cancel", "--job-id=%d" % job, "--coordinator-port=%d" % self.control)

    def _get(self, path):
        try:
            with urllib.request.urlopen("http://127.0.0.1:%d%s" % (self.http, path), timeout=5) as r:
                return json.load(r)
        except (OSError, urllib.error.URLError, ValueError):
            return None

    def job(self, job):
        return self._get("/api/v1/jobs/%d" % job)

    def operators(self, job):
        doc = self._get("/api/v1/jobs/%d/operators" % job)
        return (doc or {}).get("operators") or []

    def keyed_watermarks(self, job):
        ops = [o for o in self.operators(job) if o.get("keyed")]
        if not ops:
            return None
        return [o.get("watermark_ms") if o.get("has_watermark") else None for o in ops]

    def completed(self, job):
        d = os.path.join(self.checkpoints, "_jobs", str(job))
        ids = []
        if os.path.isdir(d):
            for n in os.listdir(d):
                if n.startswith("COMPLETED-") and n[10:].isdigit():
                    ids.append(int(n[10:]))
        return sorted(ids)

    def finished(self, job):
        """None while the job runs; else (succeeded, why)."""
        s = self.job(job)
        if not s:
            return None
        errors = list(s.get("errors") or []) + list(s.get("subtask_errors") or [])
        if errors:
            text = [json.dumps(e) if not isinstance(e, str) else e for e in errors]
            # An operator's own failure first: the transport failures beside it
            # are its consequence, its peers finding the channel gone.
            text.sort(key=lambda t: ("transport failure" in t, "operator failure" not in t))
            return False, "; ".join(text)[:2000]
        if s.get("completion_signalled") and s.get("completed_count") == s.get("expected_completion"):
            return True, "every subtask finished"
        return None
