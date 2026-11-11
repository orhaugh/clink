"""Running one step as a child process, bounded.

A child gets an argument array (never a shell), the base environment and the
variables its step names, its own process group, and a time limit. Its output
goes to files in the audit's directory, not to pipes, so a chatty child cannot
fill memory or block. When it passes its limit, or a file it writes passes its
size bound, the whole group is stopped. The runner records the child it is
waiting for, so that a runner restarted after a crash can stop an orphan
before running the step again.
"""

import os
import resource
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from .config import BASE_ENV

TAIL_BYTES = 64 << 10


@dataclass
class Result:
    argv: list
    exit: int | None
    seconds: float
    cpu_s: float
    timed_out: bool = False
    stopped: str = ""  # why the runner stopped it, when it did
    stdout: str = ""   # the tail of each stream, redacted
    stderr: str = ""
    env_names: list = field(default_factory=list)

    @property
    def summary(self):
        if self.stopped:
            return self.stopped
        if self.exit is None:
            return "did not finish"
        if self.exit < 0:
            return "killed by signal %d" % -self.exit
        return "exit %d" % self.exit


def child_env(names, source=None):
    """The base environment, then each named variable that is set."""
    source = os.environ if source is None else source
    env = {k: source[k] for k in BASE_ENV if k in source}
    for k in names:
        if k in source:
            env[k] = source[k]
    return env


def redact(text, secrets):
    for s in secrets:
        if s and len(s) >= 4:
            text = text.replace(s, "<redacted>")
    return text


def _tail(path, secrets):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - TAIL_BYTES))
            data = f.read().decode(errors="replace")
    except OSError:
        return ""
    return redact(data, secrets)


def _scrub(path, secrets):
    """A child should never print a credential; if it did, the copy kept in
    the audit's directory has it replaced."""
    live = [s for s in secrets if s and len(s) >= 4]
    if not live:
        return
    try:
        if os.path.getsize(path) > (16 << 20):
            return
        data = Path(path).read_text(errors="replace")
    except OSError:
        return
    clean = redact(data, live)
    if clean != data:
        Path(path).write_text(clean)


def kill_group(pgid, grace_s=2.0):
    try:
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except (ProcessLookupError, PermissionError):
            return
        time.sleep(0.05)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def run(argv, *, log_prefix, timeout_s, env_names=(), secrets=(), on_start=None, on_end=None,
        watch=None, should_stop=None, cwd=None, extra_env=None):
    """Runs argv to completion or to its bound.

    log_prefix: the child's stdout and stderr go to <prefix>.out and .err.
    watch: (path, max_bytes): stop the child when that file passes the bound.
    should_stop: called while waiting; a non-empty answer stops the child and
    is recorded as why (the runner's own deadline, or a signal to it).
    """
    argv = [str(a) for a in argv]
    env = child_env(env_names)
    if extra_env:
        env.update(extra_env)
    out_path = Path(str(log_prefix) + ".out")
    err_path = Path(str(log_prefix) + ".err")
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.monotonic()
    with open(out_path, "wb") as out, open(err_path, "wb") as err:
        p = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=err, env=env,
                             cwd=cwd, start_new_session=True, close_fds=True)
    if on_start:
        on_start(p.pid)
    timed_out = False
    stopped = ""
    try:
        while True:
            try:
                p.wait(timeout=0.1)
                break
            except subprocess.TimeoutExpired:
                pass
            elapsed = time.monotonic() - t0
            if elapsed > timeout_s:
                timed_out = True
                stopped = "stopped after its time limit (%g s)" % timeout_s
            elif watch is not None:
                try:
                    if os.path.getsize(watch[0]) > watch[1]:
                        stopped = "stopped: %s passed its bound of %d bytes" % (Path(watch[0]).name, watch[1])
                except OSError:
                    pass
            if not stopped and should_stop is not None:
                stopped = should_stop() or ""
            if stopped:
                kill_group(p.pid)
                p.wait()
                break
    finally:
        if p.poll() is None:
            kill_group(p.pid)
            p.wait()
        # Anything the child left running in its group goes with it.
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        if on_end:
            on_end()
    seconds = time.monotonic() - t0
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    secret_values = [os.environ.get(n, "") for n in secrets]
    _scrub(out_path, secret_values)
    _scrub(err_path, secret_values)
    return Result(argv=[redact(a, secret_values) for a in argv], exit=p.returncode, seconds=seconds,
                  cpu_s=cpu, timed_out=timed_out, stopped=stopped,
                  stdout=_tail(out_path, secret_values), stderr=_tail(err_path, secret_values),
                  env_names=sorted(env))


def process_matches(pid, needle):
    """Whether process `pid` is alive and its command line holds `needle`
    (an audit directory's path), so a restarted runner stops its own orphan
    and never an unrelated process that reused the id."""
    try:
        r = subprocess.run(["ps", "-o", "command=", "-p", str(int(pid))], capture_output=True,
                           text=True, timeout=10)
    except (OSError, subprocess.SubprocessError, ValueError):
        return False
    return r.returncode == 0 and needle in r.stdout
