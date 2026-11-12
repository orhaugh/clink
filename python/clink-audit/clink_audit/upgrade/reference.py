"""The independent reference: what the supported job must produce, computed
from the input and the test definition alone.

It implements the job's specification, not the job's code, and runs nothing
either build provides. Lines are read in order:

* E,<t>,<account>,<amount>: the event falls in the window [s, s + window_ms)
  with s = floor(t / window_ms) * window_ms, which counts it and adds its
  amount. When t is greater than the account's last_seen, last_seen becomes
  t and a timer is registered at t + idle_ms (timers are a set of (time,
  account)). An event behind the watermark already emitted breaks the
  supported contract and is refused, not guessed at.
* W,<w>: every open window whose end is at or before w emits
  ("window", account, start, end, count, sum) and is purged; then every timer
  at or before w fires, in (time, account) order, and when the account's
  last_seen plus idle_ms equals the timer's time, its idle count goes up by
  one and it emits ("idle", account, last_seen, time, count).
* END: the end of the input.

Rows are compared as a multiset: neither the job nor the engine documents an
order across keys or across windows that fire at the same watermark.

It also gives the job's logical state at the savepoint (after the segment the
definition names) and at the end, in the form compare.py decodes checkpoints
into: the source's position, the open windows, last_seen, idle_count and the
pending timers.
"""

import hashlib
import json

from . import workload

ARTIFACT_FORMAT = "clink-upgrade-reference"
ARTIFACT_VERSION = 1


class ContractError(ValueError):
    pass


def compute(defn):
    sem = defn.semantics
    size, idle = sem["window_ms"], sem["idle_ms"]
    segs = workload.segments(defn.input)
    cut = defn.input["savepoint_after_segment"]
    windows = {}
    last_seen = {}
    idle_count = {}
    timers = set()
    rows = []
    wm = None
    before = None
    at_cut = None
    end_state = None

    def state(position):
        return {
            "position": position,
            "windows": {"%d:%d" % k: list(v) for k, v in sorted(windows.items())},
            "last_seen": {str(a): t for a, t in sorted(last_seen.items())},
            "idle_count": {str(a): n for a, n in sorted(idle_count.items())},
            "timers": ["%d:%d" % t for t in sorted(timers)],
        }

    for si, lines in enumerate(segs, 1):
        for li, line in enumerate(lines):
            f = line.split(",")
            if f[0] == "E":
                t, a, amount = int(f[1]), int(f[2]), int(f[3])
                if wm is not None and t < wm:
                    raise ContractError("segment %d line %d: event at %d is behind the watermark %d"
                                        % (si, li + 1, t, wm))
                s = t - t % size
                c = windows.setdefault((a, s), [0, 0])
                c[0] += 1
                c[1] += amount
                if a not in last_seen or t > last_seen[a]:
                    last_seen[a] = t
                    timers.add((t + idle, a))
            elif f[0] == "W":
                w = int(f[1])
                if wm is not None and w <= wm:
                    raise ContractError("segment %d line %d: watermark %d does not advance past %d"
                                        % (si, li + 1, w, wm))
                wm = w
                for k in sorted(k for k in windows if k[1] + size <= w):
                    c = windows.pop(k)
                    rows.append(("window", k[0], k[1], k[1] + size, c[0], c[1]))
                for t, a in sorted(x for x in timers if x[0] <= w):
                    timers.discard((t, a))
                    if last_seen.get(a) is not None and last_seen[a] + idle == t:
                        idle_count[a] = idle_count.get(a, 0) + 1
                        rows.append(("idle", a, last_seen[a], t, idle_count[a]))
            elif line == "END":
                end_state = state([si, li, wm])
                break
            else:
                raise ContractError("segment %d line %d: %r is not an input line" % (si, li + 1, line))
        if si == cut:
            before = list(rows)
            at_cut = state([si + 1, 0, wm])
    if end_state is None:
        raise ContractError("the input has no END")
    return {
        "format": ARTIFACT_FORMAT,
        "version": ARTIFACT_VERSION,
        "test_sha256": defn.sha256,
        "semantics": {"window": "tumbling, event time, [start, end), fires when the watermark "
                                "reaches its end, allowed lateness 0",
                      "idle": "event-time timer at last_seen + idle_ms; emits when last_seen has "
                              "not moved", **sem},
        "rows": sorted(list(r) for r in rows),
        "rows_before_savepoint": sorted(list(r) for r in before),
        "state_at_savepoint": at_cut,
        "state_at_end": end_state,
        "last_watermark_before_savepoint": at_cut["position"][2],
    }


def save(artifact, path):
    text = json.dumps(artifact, indent=1, sort_keys=True).encode()
    with open(path, "wb") as f:
        f.write(text)
    return hashlib.sha256(text).hexdigest()
