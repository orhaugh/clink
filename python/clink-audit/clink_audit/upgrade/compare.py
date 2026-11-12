"""Comparisons that keep every difference visible.

Output rows are parsed into typed tuples and compared as multisets, so a
repeated row and a missing one both count, and two encodings of one value
cannot differ. Neither the job nor the engine documents an order across keys
or across windows that fire at one watermark, so order is not compared.

A checkpoint is compared through its dataset (`clink state-cat --dataset
--json`, from the tools build), decoded with the job's documented encodings
into its logical state:

    position     the source's "uc.position" row: (segment, line, watermark)
    windows      "windows": key [u32 length][i64 start][i64 account] (little-
                 endian, the pair codec), value [u8 fired][i64 pane, big-
                 endian][text "<count>,<sum>"] (the window entry codec)
    last_seen    "last_seen": i64 account to i64 time (little-endian)
    idle_count   "idle_count": i64 account to i64 count
    timers       the "__clink_timers" operator rows: [u32 n][i64 time][u32
                 length][key]... for processing time, then the same for event
                 time, each key an i64 account; the union over participants

and which operator holds each slot. A row it cannot decode is listed as
undecoded, never dropped. Raw bytes are compared separately, by `clink
state-diff`, and labelled so.
"""

import collections
import struct

MAX_SAMPLES = 10


def parse_rows(text, source):
    rows, bad = [], []
    for n, line in enumerate(text.split("\n"), 1):
        if not line:
            continue
        f = line.split(",")
        try:
            if f[0] == "window" and len(f) == 6:
                rows.append(("window",) + tuple(int(x) for x in f[1:]))
                continue
            if f[0] == "idle" and len(f) == 5:
                rows.append(("idle",) + tuple(int(x) for x in f[1:]))
                continue
        except ValueError:
            pass
        bad.append("%s line %d: %r" % (source, n, line[:200]))
    return rows, bad


def read_rows(directory, label):
    rows, bad = [], []
    for name in ("windows.txt", "idle.txt"):
        p = directory + "/" + name
        try:
            with open(p) as f:
                r, b = parse_rows(f.read(), "%s %s" % (label, name))
        except FileNotFoundError:
            r, b = [], []
        rows += r
        bad += b
    return rows, bad


def multiset_diff(expected, got, max_samples=MAX_SAMPLES):
    e = collections.Counter(tuple(r) for r in expected)
    g = collections.Counter(tuple(r) for r in got)
    missing = e - g
    unexpected = g - e
    return {
        "expected_rows": sum(e.values()),
        "rows": sum(g.values()),
        "missing": sum(missing.values()),
        "unexpected": sum(unexpected.values()),
        "missing_samples": [list(r) for r in sorted(missing)[:max_samples]],
        "unexpected_samples": [list(r) for r in sorted(unexpected)[:max_samples]],
        "equal": not missing and not unexpected,
    }


def _i64le(b):
    return struct.unpack("<q", b)[0]


def _timers(blob):
    """The TimerService blob: processing-time then event-time timers."""
    out, pos = {"processing": [], "event": []}, 0
    for kind in ("processing", "event"):
        (n,) = struct.unpack_from("<I", blob, pos)
        pos += 4
        for _ in range(n):
            (ts,) = struct.unpack_from("<q", blob, pos)
            (ln,) = struct.unpack_from("<I", blob, pos + 8)
            key = blob[pos + 12:pos + 12 + ln]
            pos += 12 + ln
            if len(key) != ln:
                raise ValueError("truncated timer")
            out[kind].append((ts, key))
    if pos != len(blob):
        raise ValueError("%d bytes after the timers" % (len(blob) - pos))
    return out


def owners_of(operators):
    """{op_id: set of job-global subtask indices} from the operators a running
    job reported (GET /api/v1/jobs/:id/operators)."""
    return {str(o["op_id"]): set(o.get("subtasks") or []) for o in operators
            if o.get("op_id") and o.get("subtasks")}


def decode_state(catalog, owners=None):
    """Decodes `state-cat --dataset --json --max-rows=0` output.

    An operator-state row is the state of the subtasks that run its operator.
    A row under an operator's id held by another subtask is a copy nothing
    reads (a release before 04963a61 handed every subtask every participant's
    operator state at a restore, and they wrote it on); given `owners`, such
    rows are counted under `copies` and left out of the logical state. Raw
    comparison (clink state-diff) still sees them."""
    st = {"position": None, "windows": {}, "last_seen": {}, "idle_count": {}, "timers": [],
          "slots": {}, "undecoded": [], "copies": 0, "complete": catalog.get("complete"),
          "checkpoint": catalog.get("checkpoint"), "digest": catalog.get("digest")}
    owners = owners or {}
    timers = set()
    for slot in catalog.get("slots", []):
        op, name, scope = slot.get("op_id"), slot.get("slot"), slot.get("scope")
        for e in slot.get("entries", []):
            if scope == "operator" and op in owners and e.get("participant") not in owners[op]:
                st["copies"] += 1
                continue
            st["slots"].setdefault(name if scope != "operator" else "<operator>", set()).add(op)
            k = bytes.fromhex(e.get("user_key_hex", ""))
            v = bytes.fromhex(e.get("value_hex", ""))
            try:
                if scope == "keyed" and name in ("last_seen", "idle_count"):
                    st[name][str(_i64le(k))] = _i64le(v)
                elif scope == "keyed" and name == "windows":
                    (ln,) = struct.unpack_from("<I", k, 0)
                    if ln != 8 or len(k) != 20 or len(v) < 9:
                        raise ValueError("not a window key and entry")
                    start, account = _i64le(k[4:12]), _i64le(k[12:20])
                    fired = v[0] != 0
                    pane = struct.unpack(">q", v[1:9])[0]
                    count, total = (int(x) for x in v[9:].decode().split(","))
                    st["windows"]["%d:%d" % (account, start)] = [count, total]
                    if fired or pane:
                        st["undecoded"].append("window %d:%d is fired=%s pane %d" % (
                            account, start, fired, pane))
                elif scope == "operator" and k == b"uc.position":
                    if len(v) != 26 or v[0] != 1:
                        raise ValueError("not a version 1 position")
                    seg, line = struct.unpack_from("<QQ", v, 1)
                    wm = _i64le(v[18:26]) if v[17] else None
                    st["position"] = [seg, line, wm]
                    st["slots"].setdefault("uc.position", set()).add(op)
                elif scope == "operator" and k == b"__clink_timers":
                    t = _timers(v)
                    if t["processing"]:
                        raise ValueError("processing-time timers this job never registers")
                    for ts, key in t["event"]:
                        if len(key) != 8:
                            raise ValueError("a timer key that is not an i64 account: %r" % key[:16])
                        timers.add((ts, _i64le(key)))
                    st["slots"].setdefault("timers", set()).add(op)
                else:
                    raise ValueError("a row this job does not write")
            except (ValueError, struct.error, UnicodeDecodeError) as ex:
                st["undecoded"].append("op %s slot %s %s key %s: %s" % (
                    op, name, scope, k.hex()[:40], ex))
    st["timers"] = ["%d:%d" % t for t in sorted(timers)]
    st["slots"] = {k: sorted(v) for k, v in sorted(st["slots"].items())}
    return st


SECTIONS = ("position", "windows", "last_seen", "idle_count", "timers")


def state_diff(expected, got, max_samples=MAX_SAMPLES):
    """Section by section; each difference sampled with both values."""
    out = {"equal": True, "sections": {}}
    for s in SECTIONS:
        a, b = expected.get(s), got.get(s)
        if s == "position":
            same = a == b
            out["sections"][s] = {"equal": same, "expected": a, "got": b}
        elif s == "timers":
            sa, sb = set(a or []), set(b or [])
            same = sa == sb
            out["sections"][s] = {"equal": same, "expected": len(sa), "got": len(sb),
                                  "missing": sorted(sa - sb)[:max_samples],
                                  "unexpected": sorted(sb - sa)[:max_samples],
                                  "missing_count": len(sa - sb), "unexpected_count": len(sb - sa)}
        else:
            a, b = a or {}, b or {}
            keys = sorted(set(a) | set(b), key=lambda k: [int(x) for x in k.split(":")])
            changed = [k for k in keys if a.get(k) != b.get(k)]
            same = not changed
            out["sections"][s] = {"equal": same, "expected": len(a), "got": len(b),
                                  "different": len(changed),
                                  "samples": [{"key": k, "expected": a.get(k), "got": b.get(k)}
                                              for k in changed[:max_samples]]}
        out["equal"] = out["equal"] and same
    undecoded = list(got.get("undecoded") or [])
    if undecoded:
        out["equal"] = False
        out["undecoded"] = undecoded[:max_samples]
        out["undecoded_count"] = len(undecoded)
    return out


def describe_state_diff(d):
    parts = []
    for s, v in d["sections"].items():
        if v["equal"]:
            continue
        if s == "position":
            parts.append("position %s, expected %s" % (v["got"], v["expected"]))
        elif s == "timers":
            parts.append("timers: %d missing, %d unexpected of %d" % (
                v["missing_count"], v["unexpected_count"], v["expected"]))
        else:
            parts.append("%s: %d of %d entries differ (%d present)" % (
                s, v["different"], v["expected"], v["got"]))
    if d.get("undecoded_count"):
        parts.append("%d row(s) not in the job's encodings" % d["undecoded_count"])
    return "; ".join(parts) or "equal"


def describe_rows(d):
    if d["equal"]:
        return "%d rows, equal as a multiset" % d["rows"]
    return "%d rows against %d: %d missing, %d unexpected" % (
        d["rows"], d["expected_rows"], d["missing"], d["unexpected"])
