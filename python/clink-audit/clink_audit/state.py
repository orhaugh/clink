"""The runner's state directory: the only place it writes, and the only files
it ever deletes.

    lock              held (flock) by the one runner using the directory
    state.json        progress: audits under way, coverage, gaps, discrepancies
    status.json       the last status, rewritten whole after every cycle
    events.ndjson     structured events, one JSON object per line (rotated)
    inflight.json     the child process running now, so a restart can stop it
    acks/<id>.json    an operator's acknowledgement of a result, written
                      without the lock and read by the runner
    audits/<id>/      one audit's evidence: its bundle, scope, reference,
                      verification and reconciliation reports, logs
    reports/<id>.json one audit's final report, published by rename

Every JSON file is replaced by writing a temporary file beside it, syncing it
and renaming it over the old one, so a crash leaves the old file or the new
one and never a mixture. An audit directory is created with a marker naming
its audit, and retention deletes a directory only when that marker matches.
"""

import errno
import fcntl
import json
import os
import re
import shutil
import time
from pathlib import Path

STATE_FORMAT = "clink-audit-state"
STATE_VERSION = 1
OWNED_MARKER = ".clink-audit-owned"
EVENTS_ROTATE_BYTES = 32 << 20


class Busy(Exception):
    """Another runner holds the state directory."""


def now_iso(t=None):
    t = time.time() if t is None else t
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dZ" % int((t % 1) * 1000)


def fsync_dir(path):
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path, data):
    path = Path(path)
    tmp = path.with_name(".%s.tmp-%d" % (path.name, os.getpid()))
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)


def atomic_write_json(path, obj):
    atomic_write_bytes(path, (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode())


def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


class StateDir:
    def __init__(self, path):
        self.path = Path(path)
        self.audits = self.path / "audits"
        self.reports = self.path / "reports"
        self.acks = self.path / "acks"
        self.state_file = self.path / "state.json"
        self.status_file = self.path / "status.json"
        self.events_file = self.path / "events.ndjson"
        self.inflight_file = self.path / "inflight.json"
        self._lock_fd = None

    def ensure(self):
        for d in (self.path, self.audits, self.reports, self.acks):
            d.mkdir(parents=True, exist_ok=True)

    # -- the one runner --------------------------------------------------------
    def lock(self):
        """Takes the directory for this process, or raises Busy. The lock is
        the kernel's: a runner that dies releases it with its last descriptor,
        so a crash never leaves the directory locked."""
        self.ensure()
        fd = os.open(str(self.path / "lock"), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            holder = ""
            try:
                holder = os.pread(fd, 512, 0).decode(errors="replace").strip()
            except OSError:
                pass
            os.close(fd)
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                raise Busy("another runner holds %s%s" % (self.path, (" (" + holder + ")") if holder else ""))
            raise
        os.ftruncate(fd, 0)
        os.pwrite(fd, ("pid %d since %s\n" % (os.getpid(), now_iso())).encode(), 0)
        self._lock_fd = fd

    def unlock(self):
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    # -- progress ------------------------------------------------------------------
    def load(self):
        st = read_json(self.state_file)
        if st is None:
            return None
        if st.get("format") != STATE_FORMAT or st.get("version") != STATE_VERSION:
            raise ValueError("%s is not a %s version %d file" % (self.state_file, STATE_FORMAT,
                                                                   STATE_VERSION))
        return st

    def save(self, st):
        atomic_write_json(self.state_file, st)

    # -- events --------------------------------------------------------------------
    def event(self, kind, **fields):
        rec = {"at": now_iso(), "kind": kind}
        rec.update(fields)
        line = json.dumps(rec, sort_keys=True) + "\n"
        try:
            if self.events_file.stat().st_size + len(line) > EVENTS_ROTATE_BYTES:
                os.replace(self.events_file, self.path / "events.1.ndjson")
        except FileNotFoundError:
            pass
        with open(self.events_file, "a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        return rec

    # -- the child running now ------------------------------------------------------------
    def set_inflight(self, rec):
        atomic_write_json(self.inflight_file, rec)

    def clear_inflight(self):
        try:
            self.inflight_file.unlink()
        except FileNotFoundError:
            pass

    def inflight(self):
        return read_json(self.inflight_file)

    # -- acknowledgements ---------------------------------------------------------------
    def acknowledgements(self):
        out = {}
        if not self.acks.is_dir():
            return out
        for p in sorted(self.acks.glob("*.json")):
            rec = read_json(p)
            if isinstance(rec, dict) and (isinstance(rec.get("audit"), str) or rec.get("checkpoints")):
                out[p.stem] = rec
        return out

    def acknowledge(self, note, audit_id=None, checkpoints=None):
        """Records an operator's acknowledgement of one audit's result, or of
        every result for a range of checkpoints. It never removes the result:
        the status lists it as acknowledged, with the note."""
        self.ensure()
        if (audit_id is None) == (checkpoints is None):
            raise ValueError("acknowledge one audit or one range of checkpoints")
        if audit_id is not None:
            if not re.match(r"^[A-Za-z0-9_.-]+$", audit_id):
                raise ValueError("%r is not an audit id" % audit_id)
            rec = {"audit": audit_id, "checkpoints": None, "note": note, "at": now_iso()}
            name = audit_id
        else:
            first, last = checkpoints
            rec = {"audit": None, "checkpoints": [first, last], "note": note, "at": now_iso()}
            name = "checkpoints-%d-%d" % (first, last)
        atomic_write_json(self.acks / ("%s.json" % name), rec)
        return rec

    # -- what it owns ---------------------------------------------------------------------
    def audit_dir(self, audit_id, create=False):
        d = self.audits / audit_id
        if create:
            d.mkdir(parents=True, exist_ok=True)
            marker = d / OWNED_MARKER
            if not marker.exists():
                atomic_write_bytes(marker, (audit_id + "\n").encode())
        return d

    def owns(self, path, audit_id):
        """Whether `path` is the audit directory this runner created for
        `audit_id`: a real directory directly under audits/, not a link,
        inside the state directory after resolving, holding the marker that
        names the audit."""
        p = Path(path)
        try:
            if p.is_symlink() or not p.is_dir():
                return False
            real = Path(os.path.realpath(p))
            if real.parent != Path(os.path.realpath(self.audits)) or real.name != audit_id:
                return False
            marker = real / OWNED_MARKER
            if marker.is_symlink() or not marker.is_file():
                return False
            return marker.read_text().strip() == audit_id
        except OSError:
            return False

    def remove_audit_dir(self, audit_id):
        d = self.audits / audit_id
        if not self.owns(d, audit_id):
            return False
        shutil.rmtree(d)
        return True

    def remove_report(self, audit_id):
        p = self.reports / ("%s.json" % audit_id)
        if p.is_symlink() or not p.is_file():
            return False
        if Path(os.path.realpath(p)).parent != Path(os.path.realpath(self.reports)):
            return False
        p.unlink()
        return True

    def usage(self):
        """Bytes the state directory holds, counted file by file (links are
        not followed)."""
        total = 0
        for root, dirs, files in os.walk(self.path):
            for name in files:
                try:
                    total += os.lstat(os.path.join(root, name)).st_size
                except OSError:
                    pass
        return total
