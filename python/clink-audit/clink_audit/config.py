"""The runner's configuration: one JSON file the operator owns and trusts.

It names the job's directories, the `clink` binary, the destination's
connection, the reference executable with its fixed argument array, and every
bound. Credentials are never in it: it names the environment variables that
hold them, and each child process receives only the ones its step needs.
Nothing a bundle, a scope, a reference artifact or the job's metadata says is
ever run, connected to or given a credential.
"""

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

FORMAT = "clink-audit-config"
VERSION = 1

# Every bound the runner applies, with its default. The limits that change
# what an audit establishes (the reference's, the destination's, the bundle's
# and the verification profile) are part of the configuration's fingerprint;
# the ones that only pace or bound the runner itself are not.
DEFAULT_LIMITS = {
    # One `run`, or one cycle of `poll`: no step starts after it, and a step
    # running at its end is stopped and left to resume.
    "run_seconds": 900,
    # Each child process, killed with its process group when it passes its time.
    "collect_timeout_s": 120,
    "scope_timeout_s": 120,
    "reference_timeout_s": 600,
    "verify_timeout_s": 600,
    "reconcile_timeout_s": 600,
    # The bundle (incident-collect --max-bytes, --max-files).
    "bundle_bytes": 1 << 30,
    "bundle_files": 10000,
    # The reference: the artifact's size, watched while it is written and
    # passed to incident-reconcile --max-reference-bytes; its rows; and the
    # source records it read, by its own account and through {max_records}.
    "reference_bytes": 1 << 30,
    "reference_rows": 10_000_000,
    "reference_records": 50_000_000,
    # The destination (incident-reconcile's own bounds).
    "query_timeout_ms": 30000,
    "max_rows_per_query": 1_000_000,
    "keys_per_query": 10000,
    # Local disk: the state directory's size, and the free space it leaves.
    "disk_bytes": 20 << 30,
    "min_free_bytes": 1 << 30,
    # Evidence kept for audits that passed; failure evidence is always kept.
    "retain_passed": 3,
    "retain_passed_reports": 1000,
    # Collected bundles waiting for their audit.
    "max_pending": 100,
    # Attempts for an audit whose result was partial or an error, and the
    # pause before each retry, in seconds (the last value repeats).
    "max_attempts": 3,
    "retry_backoff_s": [10, 60, 300],
    # Collection refused because the job moved on during the copy.
    "collect_retries": 10,
    "poll_interval_s": 10,
    # Status: verification older than this is stale; more eligible
    # checkpoints than this waiting is behind.
    "stale_after_s": 900,
    "backlog_warn": 10,
}

FINGERPRINTED_LIMITS = ("bundle_bytes", "bundle_files", "reference_bytes", "reference_rows",
                        "reference_records", "query_timeout_ms", "max_rows_per_query",
                        "keys_per_query")

# Placeholders a reference argument may hold; anything else in braces is a
# configuration error, so a typo never reaches the child as text.
PLACEHOLDERS = ("scope", "out", "max_records", "max_bytes", "deadline_s")
_PLACEHOLDER = re.compile(r"\{([^{}]*)\}")

# The environment every child starts from. Nothing else of the runner's own
# environment passes, so a credential meant for one step never reaches another.
BASE_ENV = ("PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TZ")

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(Exception):
    pass


@dataclass
class Config:
    path: Path
    state_dir: Path
    clink: Path
    job_id: int
    checkpoint_dir: Path
    ha_dir: Path | None
    capture_dir: Path
    commit_confirmation: str | None
    start_at: object  # "latest", "earliest" or a checkpoint id
    key: list
    destination: dict
    reference_command: list
    reference_env: list
    reference_when_run_start: list
    verify_profile: str
    selection: str
    limits: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)

    @property
    def fingerprint(self):
        """What an audit's result depends on, hashed: the same job, key,
        destination, reference, verification and verdict-changing bounds give
        the same fingerprint, and an audit's identity carries it."""
        basis = {
            "job": {"id": self.job_id, "checkpoint_dir": str(self.checkpoint_dir),
                    "commit_confirmation": self.commit_confirmation},
            "key": self.key,
            "destination": {k: self.destination.get(k) for k in
                            ("host", "port", "user", "password_env", "secure", "tls_ca_file",
                             "tls_verify", "database", "table")},
            "reference": {"command": self.reference_command, "env": self.reference_env,
                          "when_run_start": self.reference_when_run_start},
            "verify": self.verify_profile,
            "limits": {k: self.limits[k] for k in FINGERPRINTED_LIMITS},
        }
        text = json.dumps(basis, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(text.encode()).hexdigest()

    @property
    def secret_env(self):
        """The environment variables holding credentials, whose values are
        kept out of every log and report the runner writes."""
        names = list(self.reference_env)
        if self.destination.get("password_env"):
            names.append(self.destination["password_env"])
        return names


def _expect(cond, why):
    if not cond:
        raise ConfigError(why)


def _keys(obj, where, allowed, required=()):
    _expect(isinstance(obj, dict), "%s must be an object" % where)
    unknown = sorted(set(obj) - set(allowed))
    _expect(not unknown, "%s: unknown key(s) %s" % (where, ", ".join(unknown)))
    for k in required:
        _expect(k in obj, "%s: %s is required" % (where, k))


def _abs_dir(value, where, must_exist=True):
    _expect(isinstance(value, str) and value, "%s must be a path" % where)
    p = Path(value)
    _expect(p.is_absolute(), "%s must be an absolute path, not %r" % (where, value))
    if must_exist:
        _expect(p.is_dir(), "%s (%s) is not a directory" % (where, value))
    return Path(os.path.realpath(p))


def _within(a, b):
    """Whether a is b or inside it."""
    try:
        Path(a).relative_to(b)
        return True
    except ValueError:
        return False


def _env_names(value, where):
    _expect(isinstance(value, list) and all(isinstance(v, str) for v in value),
            "%s must be a list of environment variable names" % where)
    for v in value:
        _expect(_ENV_NAME.match(v), "%s: %r is not an environment variable name" % (where, v))
        _expect(v not in BASE_ENV, "%s: %s is passed to every step already" % (where, v))
    return list(value)


def load(path, state_dir_override=None):
    path = Path(path)
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        raise ConfigError("cannot read the configuration %s: %s" % (path, e))
    _keys(raw, "the configuration",
          ("format", "version", "state_dir", "clink", "job", "key", "destination", "reference",
           "verify", "selection", "limits"),
          ("format", "version", "clink", "job", "key", "destination", "reference"))
    _expect(raw["format"] == FORMAT, "format must be %s" % FORMAT)
    _expect(raw["version"] == VERSION, "version must be %d" % VERSION)

    state = state_dir_override or raw.get("state_dir")
    _expect(state, "state_dir is required (in the configuration or with --state-dir)")
    state_dir = _abs_dir(str(state), "state_dir", must_exist=False)

    clink = raw["clink"]
    _expect(isinstance(clink, str) and Path(clink).is_absolute(), "clink must be an absolute path")
    _expect(os.path.isfile(clink) and os.access(clink, os.X_OK), "clink (%s) is not an executable file" % clink)

    job = raw["job"]
    _keys(job, "job", ("id", "checkpoint_dir", "ha_dir", "capture_dir", "commit_confirmation",
                       "start_at"), ("id", "checkpoint_dir", "capture_dir"))
    _expect(isinstance(job["id"], int) and job["id"] >= 0, "job.id must be a whole number")
    checkpoint_dir = _abs_dir(job["checkpoint_dir"], "job.checkpoint_dir")
    capture_dir = _abs_dir(job["capture_dir"], "job.capture_dir")
    ha_dir = _abs_dir(job["ha_dir"], "job.ha_dir") if job.get("ha_dir") else None
    cc = job.get("commit_confirmation")
    _expect(cc in (None, "yes", "no"), "job.commit_confirmation must be yes, no or absent")
    _expect(ha_dir is not None or cc is not None,
            "job: give ha_dir (the restore rule comes from the job manifest) or commit_confirmation")
    start_at = job.get("start_at", "latest")
    _expect(start_at in ("latest", "earliest") or (isinstance(start_at, int) and start_at >= 1),
            "job.start_at must be latest, earliest or a checkpoint id")

    # The runner deletes only what it owns, under its state directory: that
    # directory must not hold, or sit inside, the job's own.
    for name, d in (("job.checkpoint_dir", checkpoint_dir), ("job.capture_dir", capture_dir),
                    ("job.ha_dir", ha_dir)):
        if d is None:
            continue
        _expect(not _within(state_dir, d) and not _within(d, state_dir),
                "state_dir (%s) and %s (%s) must not contain one another" % (state_dir, name, d))

    key = raw["key"]
    _expect(isinstance(key, list) and key and all(isinstance(k, str) and k for k in key),
            "key must be a non-empty list of column names")

    dest = raw["destination"]
    _keys(dest, "destination", ("host", "port", "user", "password_env", "secure", "tls_ca_file",
                                "tls_verify", "database", "table"), ("host",))
    _expect(isinstance(dest["host"], str) and dest["host"], "destination.host must be a host name")
    if "port" in dest:
        _expect(isinstance(dest["port"], int) and 0 < dest["port"] < 65536, "destination.port")
    if "password_env" in dest:
        _env_names([dest["password_env"]], "destination.password_env")
    for b in ("secure", "tls_verify"):
        if b in dest:
            _expect(isinstance(dest[b], bool), "destination.%s must be true or false" % b)

    ref = raw["reference"]
    _keys(ref, "reference", ("command", "env", "when_run_start"), ("command",))
    cmd = ref["command"]
    _expect(isinstance(cmd, list) and cmd and all(isinstance(a, str) for a in cmd),
            "reference.command must be a non-empty list of arguments (no shell)")
    _expect(Path(cmd[0]).is_absolute() and os.access(cmd[0], os.X_OK),
            "reference.command[0] must be an absolute path to an executable")
    used = set()
    for a in cmd:
        for name in _PLACEHOLDER.findall(a):
            _expect(name in PLACEHOLDERS, "reference.command: {%s} is not a placeholder (%s)"
                    % (name, ", ".join("{%s}" % p for p in PLACEHOLDERS)))
            used.add(name)
    _expect({"scope", "out"} <= used, "reference.command must take {scope} and {out}")
    ref_env = _env_names(ref.get("env", []), "reference.env")
    when = ref.get("when_run_start", [])
    _expect(isinstance(when, list) and all(isinstance(a, str) for a in when),
            "reference.when_run_start must be a list of arguments")
    _expect(not dest.get("password_env") or dest["password_env"] not in ref_env,
            "the destination's credential must not be passed to the reference")

    verify = raw.get("verify", {})
    _keys(verify, "verify", ("profile",))
    profile = verify.get("profile", "consistency")
    _expect(profile in ("integrity", "consistency", "replay", "full"),
            "verify.profile must be integrity, consistency, replay or full")

    selection = raw.get("selection", "sequential")
    _expect(selection in ("sequential", "latest-only"), "selection must be sequential or latest-only")

    limits = dict(DEFAULT_LIMITS)
    given = raw.get("limits", {})
    _keys(given, "limits", DEFAULT_LIMITS.keys())
    for k, v in given.items():
        if k == "retry_backoff_s":
            _expect(isinstance(v, list) and v and all(isinstance(x, (int, float)) and x >= 0 for x in v),
                    "limits.retry_backoff_s must be a non-empty list of seconds")
        else:
            _expect(isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0,
                    "limits.%s must be a non-negative number" % k)
        limits[k] = v
    _expect(limits["max_attempts"] >= 1, "limits.max_attempts must be at least 1")

    return Config(path=path, state_dir=state_dir, clink=Path(clink), job_id=job["id"],
                  checkpoint_dir=checkpoint_dir, ha_dir=ha_dir, capture_dir=capture_dir,
                  commit_confirmation=cc, start_at=start_at, key=list(key), destination=dict(dest),
                  reference_command=list(cmd), reference_env=ref_env,
                  reference_when_run_start=list(when), verify_profile=profile,
                  selection=selection, limits=limits, raw=raw)
