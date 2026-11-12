"""The test definition: which job, which semantics, which input, which builds
qualify. It is supplied explicitly to both `record` and `check`, and the
evidence binds the one it was recorded with by its SHA-256, so a check against
another definition is refused rather than compared.

The one supported kind is "keyed-windows-and-idle-timers", the job in
examples/upgrade-check/upgrade_job.cpp. Its semantics and the input's shape are
fields here, so the independent reference computes from this file and nothing
the builds report.
"""

import hashlib
import json

from . import TEST_FORMAT, TEST_VERSION

SUPPORTED_KIND = "keyed-windows-and-idle-timers"


class DefinitionError(ValueError):
    pass


class Unsupported(DefinitionError):
    """A well-formed definition of something this release does not validate."""


_INPUT = {"start_ms": int, "accounts": int, "blocks": int, "block_ms": int,
          "blocks_per_segment": int, "savepoint_after_segment": int, "seed": int}
_SEMANTICS = {"window_ms": int, "idle_ms": int, "keyed_parallelism": int}
_UIDS = ("source", "windows", "idle")


def _need(cond, msg):
    if not cond:
        raise DefinitionError(msg)


def _typed(d, spec, where):
    _need(isinstance(d, dict), "%s must be an object" % where)
    extra = sorted(set(d) - set(spec))
    _need(not extra, "%s: unknown field(s) %s" % (where, ", ".join(extra)))
    for k, t in spec.items():
        _need(k in d, "%s: %s is required" % (where, k))
        v = d[k]
        _need(isinstance(v, t) and not isinstance(v, bool), "%s.%s must be an integer" % (where, k))


class Definition:
    def __init__(self, raw, text):
        self.raw = raw
        self.text = text
        self.sha256 = hashlib.sha256(text).hexdigest()
        self.name = raw["name"]
        self.input = raw["input"]
        self.semantics = raw["semantics"]
        self.uids = raw["uids"]
        self.requires = raw.get("requires", {})

    @property
    def segments(self):
        n = self.input["blocks"]
        per = self.input["blocks_per_segment"]
        return (n + per - 1) // per


def parse(text):
    """Reads a definition from its bytes. Raises DefinitionError when it is not
    one, Unsupported when it is a definition this release does not validate."""
    try:
        raw = json.loads(text)
    except ValueError as e:
        raise DefinitionError("not JSON: %s" % e)
    _need(isinstance(raw, dict), "a test definition is a JSON object")
    if raw.get("format") != TEST_FORMAT:
        raise DefinitionError("format is %r, not %r" % (raw.get("format"), TEST_FORMAT))
    if raw.get("version") != TEST_VERSION:
        raise Unsupported("version %r of %s; this release reads version %d"
                          % (raw.get("version"), TEST_FORMAT, TEST_VERSION))
    if raw.get("kind") != SUPPORTED_KIND:
        raise Unsupported("kind %r; the supported kind is %r" % (raw.get("kind"), SUPPORTED_KIND))
    allowed = {"format", "version", "kind", "name", "description", "input", "semantics", "uids",
               "requires"}
    extra = sorted(set(raw) - allowed)
    _need(not extra, "unknown field(s) %s" % ", ".join(extra))
    _need(isinstance(raw.get("name"), str) and raw["name"], "name must be a non-empty string")
    _typed(raw.get("input"), _INPUT, "input")
    _typed(raw.get("semantics"), _SEMANTICS, "semantics")
    i, s = raw["input"], raw["semantics"]
    _need(i["block_ms"] > 0 and s["window_ms"] > 0 and s["idle_ms"] > 0, "times must be positive")
    _need(s["window_ms"] % i["block_ms"] == 0 or i["block_ms"] % s["window_ms"] == 0,
          "input.block_ms and semantics.window_ms must divide one another")
    _need(1 <= i["accounts"] <= 10000 and 1 <= i["blocks"] <= 100000, "input is out of range")
    _need(i["blocks_per_segment"] >= 1, "input.blocks_per_segment must be at least 1")
    segments = (i["blocks"] + i["blocks_per_segment"] - 1) // i["blocks_per_segment"]
    _need(1 <= i["savepoint_after_segment"] < segments,
          "input.savepoint_after_segment must leave at least one segment for the continuation")
    _need(i["start_ms"] % s["window_ms"] == 0, "input.start_ms must be a window boundary")
    _need(s["keyed_parallelism"] >= 1, "semantics.keyed_parallelism must be at least 1")
    uids = raw.get("uids")
    _need(isinstance(uids, dict) and sorted(uids) == sorted(_UIDS)
          and all(isinstance(v, str) and v for v in uids.values()),
          "uids must name the %s operators' uids" % ", ".join(_UIDS))
    req = raw.get("requires", {})
    _need(isinstance(req, dict), "requires must be an object of capability path to value")
    for k in req:
        _need(isinstance(k, str) and k, "requires: a capability path is a non-empty string")
    return Definition(raw, text if isinstance(text, bytes) else text.encode())


def load(path):
    with open(path, "rb") as f:
        return parse(f.read())


def capability(manifest, path):
    """The value at a dotted path of a `clink --capabilities-json` manifest, or
    the sentinel MISSING."""
    cur = manifest
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return MISSING
        cur = cur[part]
    return cur


class _Missing:
    def __repr__(self):
        return "missing"


MISSING = _Missing()
