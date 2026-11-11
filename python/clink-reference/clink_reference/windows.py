"""A windowed job's expected final output, computed from the source's records.

For a version 2 scope (a job of one keyed tumbling window over one Kafka
source), `TumblingWindows` takes every record the scope's ranges hold, from
the job's own start, and gives each window's final row, or says why a window
is left out. It implements the operator's specification (`WindowRules`), not
the job's: the audit compares the two, and a job configured otherwise than
its specification is a finding, not something to follow.

The rules it implements (docs/internals/incident-verification.md, "Auditing
windowed results"):

* A record at event time t (epoch milliseconds) falls in the window
  [s, e) with s = floor(t / size) * size and e = s + size.
* The watermark trails the least, over the source's partitions, of the
  greatest event time each has delivered, by the lag. Every record the
  source delivers advances it, those the transformation then filters out
  included. With no idleness, a partition that delivers nothing holds it.
* A window closes when the watermark reaches e + allowed lateness, and a
  record arriving once its window has closed is dropped.

Which records arrive after their window closed depends on the order the job
read its partitions in, which offsets do not record. So each record is
decided by what bounds the watermark when it arrived:

* from above, by the source alone (a source subtask's watermark is the least
  over the partitions it owns, each held until it delivers, which the audit
  checks the run recorded): at most the record's own partition's
  greatest earlier event time, less the lag, and at most the least partition
  maximum at the checkpoint after it (aligned barriers: nothing past a
  barrier reaches the window operator before the barrier does). A record
  whose window ends (plus lateness) above that bound was folded in.
* from below, by recorded evidence: at least the watermark the sink recorded
  at the checkpoint before it. A record whose window ends (plus lateness) at
  or below that was dropped.

A record between the two is undetermined, and so is its window, which is then
left out of the comparison rather than guessed. A window is final at the
scope's checkpoint when its end plus lateness is at or below the watermark
recorded there; open when it is above what the source allows there; otherwise
whether it had closed is undetermined.

An increment (a version 3 scope) is the same computation split at a
checkpoint M of the lineage. A window's row reaches the sink when the
watermark passes its end plus lateness, so the windows the sink received by M
are exactly those whose end plus lateness is at or below the watermark it
recorded at M, and the increment owes the rest that close by its own
checkpoint. Nothing else before M changes a decision after it: a record's
upper bound needs only its partition's greatest earlier event time, its
window's value only the records folded into it so far, and a record after M
whose window had closed by M is dropped. So the reference's state at M,
`WindowState`, holds each partition's position and greatest event time and
every window not final there. It is the reference's own, computed from the
records it read, never the job's: finish() returns the state at the scope's
checkpoint, and the next increment starts from it.
"""

import bisect
import hashlib
import json
import os
from dataclasses import dataclass, field
from decimal import Decimal

STATE_FORMAT = "clink-reference-window-state"
STATE_VERSION = 1


class StateError(ValueError):
    """A window state that is not the one an increment starts from."""

KIND = "tumbling-window"
WATERMARK_STRATEGY = "per-partition bounded out-of-orderness"


@dataclass(frozen=True)
class WindowRules:
    """The window rules a reference implements: the operator's own
    specification, declared here and carried into the artifact."""
    size_ms: int
    lag_ms: int
    time_column: str
    allowed_lateness_ms: int = 0

    def semantics(self):
        return {"kind": KIND, "size_ms": self.size_ms, "time_column": self.time_column,
                "watermark": {"strategy": WATERMARK_STRATEGY, "lag_ms": self.lag_ms,
                              "idleness": "none"},
                "lateness": {"allowed_lateness_ms": self.allowed_lateness_ms,
                             "late_records": "dropped"}}


@dataclass
class _Window:
    key: tuple
    start: int
    end: int
    rows: int = 0
    sums: dict = field(default_factory=dict)
    undetermined: str | None = None


@dataclass
class Result:
    rows: list            # (key, start, end, {output: value}) for final, decided windows
    left_out: list        # (key, start, end, status, why)
    source_bounds: list   # [{"checkpoint", "watermark_ms"}]
    records: dict         # the account of what became of every record
    state: "WindowState" = None  # the reference's state at the scope's checkpoint


def _key_cell(v):
    """A group key value, tagged by type so it comes back as it went in."""
    if v is None:
        return ["n"]
    if isinstance(v, bool):
        return ["b", v]
    if isinstance(v, int):
        return ["i", v]
    if isinstance(v, Decimal):
        return ["d", str(v)]
    if isinstance(v, str):
        return ["s", v]
    raise StateError("a group key value of type %s cannot be carried in a window state"
                     % type(v).__name__)


def _key_value(c):
    tag = c[0]
    if tag == "n":
        return None
    if tag in ("b", "i", "s"):
        return c[1]
    if tag == "d":
        return Decimal(c[1])
    raise StateError("a window state holds a key cell tagged %r" % tag)


def _exact_text(v):
    return ["i", v] if isinstance(v, int) else ["d", str(v)]


def _exact_value(c):
    return c[1] if c[0] == "i" else Decimal(c[1])


@dataclass
class WindowState:
    """The reference's own state at one checkpoint of a windowed job's
    lineage: where each partition stood and its greatest event time so far,
    and every window not final there, with what was folded into it. An
    increment starting at that checkpoint starts from it.

    `scope_sha256` names the scope whose computation produced it, and `logs`
    the Kafka logs it read; `semantics` and `aggregates` the rules it was
    computed under. A state is only the reference's word: its hash, kept by
    whoever chains the increments, shows it was not changed, not that it is
    right."""
    checkpoint: int
    watermark_ms: int | None          # the sink's recorded watermark at the checkpoint
    positions: dict                   # partition -> next offset
    greatest: dict                    # partition -> greatest event time so far, or None
    windows: list                     # [_Window], not final at the checkpoint
    logs: tuple
    semantics: dict
    aggregates: list
    scope_sha256: str

    def document(self):
        return {
            "format": STATE_FORMAT, "version": STATE_VERSION, "checkpoint": self.checkpoint,
            "watermark_ms": self.watermark_ms, "scope_sha256": self.scope_sha256,
            "logs": [list(x) for x in self.logs], "semantics": self.semantics,
            "aggregates": [list(a) for a in self.aggregates],
            "partitions": [{"partition": p, "next_offset": self.positions[p],
                            "greatest_ms": self.greatest.get(p)} for p in sorted(self.positions)],
            "windows": [{"key": [_key_cell(k) for k in w.key], "start": w.start, "end": w.end,
                         "rows": w.rows,
                         "sums": {n: _exact_text(v) for n, v in sorted(w.sums.items())},
                         "undetermined": w.undetermined}
                        for w in sorted(self.windows, key=lambda w: (w.end, repr(w.key)))],
        }

    def text(self):
        return json.dumps(self.document(), sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False) + "\n"

    @property
    def sha256(self):
        return hashlib.sha256(self.text().encode("utf-8")).hexdigest()

    def save(self, path):
        """Writes the state beside its name and renames it into place; returns
        its sha256."""
        data = self.text().encode("utf-8")
        tmp = "%s.tmp-%d" % (path, os.getpid())
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def load(path, expect_sha256=None):
        with open(path, "rb") as f:
            data = f.read()
        got = hashlib.sha256(data).hexdigest()
        if expect_sha256 is not None and got != expect_sha256:
            raise StateError("the window state %s hashes to %s, not the %s it was given as"
                             % (path, got, expect_sha256))
        try:
            d = json.loads(data.decode("utf-8"))
            if d.get("format") != STATE_FORMAT or d.get("version") != STATE_VERSION:
                raise StateError("%s is not a %s version %d document" % (path, STATE_FORMAT, STATE_VERSION))
            windows = []
            for w in d["windows"]:
                win = _Window(tuple(_key_value(c) for c in w["key"]), int(w["start"]), int(w["end"]),
                              int(w["rows"]))
                win.sums = {n: _exact_value(c) for n, c in w["sums"].items()}
                win.undetermined = w["undetermined"]
                windows.append(win)
            wm = d["watermark_ms"]
            return WindowState(
                checkpoint=int(d["checkpoint"]), watermark_ms=None if wm is None else int(wm),
                positions={int(p["partition"]): int(p["next_offset"]) for p in d["partitions"]},
                greatest={int(p["partition"]): (None if p["greatest_ms"] is None else int(p["greatest_ms"]))
                          for p in d["partitions"]},
                windows=windows, logs=tuple(tuple(x) for x in d["logs"]), semantics=d["semantics"],
                aggregates=[tuple(a) for a in d["aggregates"]], scope_sha256=d["scope_sha256"])
        except (KeyError, TypeError, ValueError, IndexError) as e:
            if isinstance(e, StateError):
                raise
            raise StateError("the window state %s is malformed: %s" % (path, e)) from e


class TumblingWindows:
    """Feed it every record of a version 2 scope's ranges, in offset order
    within each partition, then call finish().

    aggregates: [(output, function)], function "count" (rows) or "sum" (of
    the value given for output, None being SQL's NULL, which SUM skips).
    """

    def __init__(self, scope, rules, aggregates, state=None):
        if not scope.windowed or scope.window is None:
            raise ValueError("a windowed (version 2 or 3) scope is needed")
        for _, fn in aggregates:
            if fn not in ("count", "sum"):
                raise ValueError("aggregate function %r: COUNT and SUM are supported" % fn)
        self.scope = scope
        self.rules = rules
        self.aggregates = list(aggregates)
        self._parts = {}  # partition -> [(offset, ts, row)]
        self.partitions = sorted({p for lc in scope.lineage for p in lc.positions} |
                                 set(scope.base.positions if scope.base else ()))
        self.state = None
        if scope.base is not None:
            if state is None:
                raise StateError("an increment (a version 3 scope) starts from the reference's "
                                 "state at checkpoint %d, and none was given" % scope.base.checkpoint)
            self._check_state(state)
            self.state = state
        elif state is not None:
            raise StateError("a version 2 scope starts at the job's own start, with no state")

    def _check_state(self, st):
        base = self.scope.base
        differs = []
        if st.checkpoint != base.checkpoint:
            differs.append("it is the state at checkpoint %d, and the increment starts at %d"
                           % (st.checkpoint, base.checkpoint))
        if st.positions != base.positions:
            differs.append("its positions %s are not the increment's base %s"
                           % (st.positions, base.positions))
        if st.watermark_ms != base.watermark_ms:
            differs.append("it records the watermark %s at the base, and the increment %s"
                           % (st.watermark_ms, base.watermark_ms))
        if tuple(st.logs) != tuple(self.scope.logs):
            differs.append("it read the logs %s, and the increment reads %s"
                           % (list(st.logs), list(self.scope.logs)))
        if st.semantics != self.rules.semantics():
            differs.append("it was computed under other window rules")
        if [tuple(a) for a in st.aggregates] != [tuple(a) for a in self.aggregates]:
            differs.append("it was computed for other aggregates %s" % st.aggregates)
        if differs:
            raise StateError("the window state is not the increment's start: " + "; ".join(differs))

    def add(self, partition, offset, ts_ms, row):
        """One record the source delivered. ts_ms is its event time, an int;
        row is None when the transformation drops it, else (key tuple,
        {output: value}) with a value for each SUM."""
        if not isinstance(ts_ms, int) or isinstance(ts_ms, bool):
            raise ValueError("record %d@%d has no integer event time" % (partition, offset))
        recs = self._parts.setdefault(partition, [])
        if recs and offset <= recs[-1][0]:
            raise ValueError("partition %d's records came out of offset order" % partition)
        recs.append((offset, ts_ms, row))

    # -- the bounds -----------------------------------------------------------------

    def _carried(self, p):
        """A partition's greatest event time before the scope's records."""
        return self.state.greatest.get(p) if self.state is not None else None

    def _source_bounds(self):
        """The least partition maximum less the lag, at each lineage
        checkpoint's positions: None while a partition has delivered nothing."""
        base = self.scope.base.positions if self.scope.base else {}
        prefix = {}  # partition -> (offsets, greatest event time up to and including each)
        for p in self.partitions:
            recs = self._parts.get(p, [])
            offsets, greatest, mx = [], [], self._carried(p)
            for o, t, _ in recs:
                mx = t if mx is None else max(mx, t)
                offsets.append(o)
                greatest.append(mx)
            prefix[p] = (offsets, greatest)
        out = []
        for lc in self.scope.lineage:
            least = None
            for p in self.partitions:
                offsets, greatest = prefix[p]
                n = bisect.bisect_left(offsets, lc.positions.get(p, 0))
                g = greatest[n - 1] if n > 0 else self._carried(p)
                if g is None:
                    least = None
                    break
                least = g if least is None else min(least, g)
            out.append(None if least is None else least - self.rules.lag_ms)
        return out

    def finish(self):
        size, lag, late = self.rules.size_ms, self.rules.lag_ms, self.rules.allowed_lateness_ms
        lineage = self.scope.lineage
        bounds = self._source_bounds()
        counts = {"records": 0, "folded": 0, "dropped_late": 0, "undetermined": 0,
                  "filtered": 0}
        windows = {}
        base = self.scope.base
        if self.state is not None:
            for w in self.state.windows:
                carried = _Window(w.key, w.start, w.end, w.rows, dict(w.sums), w.undetermined)
                windows[(w.key, w.end)] = carried
        for p, recs in self._parts.items():
            if p not in self.partitions:
                raise ValueError("partition %d is not one the scope's lineage records" % p)
            positions = [lc.positions[p] for lc in lineage]
            if any(b < a for a, b in zip(positions, positions[1:])):
                raise ValueError("partition %d's positions go back along the lineage" % p)
            if base is not None and recs and recs[0][0] < base.positions.get(p, 0):
                raise ValueError("record %d@%d is before the increment's base" % (p, recs[0][0]))
            running = self._carried(p)  # the partition's greatest event time so far
            for offset, ts, row in recs:
                counts["records"] += 1
                # The interval it was read in: the first lineage checkpoint
                # past it, and the one before.
                after = bisect.bisect_right(positions, offset)
                after = after if after < len(positions) else None
                own = None if running is None else running - lag
                running = ts if running is None else max(running, ts)
                if row is None:
                    counts["filtered"] += 1
                    continue
                if after is None:
                    raise ValueError("record %d@%d is past the scope's last checkpoint"
                                     % (p, offset))
                key, values = row
                start = (ts // size) * size
                end = start + size
                upper = _least(own, bounds[after])
                lower = (lineage[after - 1].watermark_ms if after > 0 else
                         base.watermark_ms if base is not None else None)
                folded = upper is None or end + late > upper
                dropped = lower is not None and end + late <= lower
                w = windows.get((key, end))
                if w is None:
                    w = windows[(key, end)] = _Window(key, start, end)
                if folded and dropped:
                    counts["undetermined"] += 1
                    at = lineage[after - 1].checkpoint if after > 0 else base.checkpoint
                    w.undetermined = w.undetermined or (
                        "record %d@%d: the watermark recorded at checkpoint %d (%d ms) is past "
                        "what the source allowed when the record arrived (%s)"
                        % (p, offset, at, lower, _ms(upper)))
                elif folded:
                    counts["folded"] += 1
                    w.rows += 1
                    for out_name, fn in self.aggregates:
                        if fn == "sum":
                            v = values.get(out_name)
                            if v is not None:
                                w.sums[out_name] = w.sums.get(out_name, 0) + _exact(v, out_name)
                elif dropped:
                    counts["dropped_late"] += 1
                else:
                    counts["undetermined"] += 1
                    w.undetermined = w.undetermined or (
                        "record %d@%d (event time %d) is late by its own partition, and the "
                        "evidence does not say whether its window had closed when it arrived: the "
                        "watermark then was at least %s and at most %s, and the window closes at "
                        "%d" % (p, offset, ts, "%d ms" % lower if lower is not None else "none",
                                _ms(upper), end + late))
        final_wm = self.scope.final_watermark_ms
        final_bound = bounds[-1] if bounds else None
        base_wm = base.watermark_ms if base is not None else None
        rows, left_out, carry = [], [], []
        for (key, end), w in sorted(windows.items(), key=lambda kv: (kv[0][1], repr(kv[0][0]))):
            if w.rows == 0 and w.undetermined is None:
                continue  # every record of it was dropped: the job emits no row
            closes = end + late
            if final_wm is None or closes > final_wm:
                carry.append(w)  # not final here: the next increment owes it
            if base_wm is not None and closes <= base_wm and w.undetermined is None:
                continue  # final at the base: an earlier increment owed it
            if final_wm is not None and closes <= final_wm:
                if w.undetermined is not None:
                    left_out.append((key, w.start, end, "undetermined", w.undetermined))
                else:
                    aggs = {}
                    for out_name, fn in self.aggregates:
                        aggs[out_name] = w.rows if fn == "count" else w.sums.get(out_name)
                    rows.append((key, w.start, end, aggs))
            elif final_bound is None or closes > final_bound:
                left_out.append((key, w.start, end, "open",
                                 "it closes at %d, past the %s the source allows at checkpoint %d"
                                 % (closes, "%d ms" % final_bound if final_bound is not None
                                    else "no watermark", self.scope.checkpoint)))
            else:
                left_out.append((key, w.start, end, "undetermined",
                                 "it closes at %d: the source allows the watermark to %d ms at "
                                 "checkpoint %d and the sink recorded %s there, so whether it had "
                                 "closed is not decided"
                                 % (closes, final_bound, self.scope.checkpoint,
                                    "%d ms" % final_wm if final_wm is not None else "none")))
        source_bounds = [{"checkpoint": lc.checkpoint, "watermark_ms": b}
                         for lc, b in zip(lineage, bounds)]
        end_positions = lineage[-1].positions if lineage else (base.positions if base else {})
        greatest = {}
        for p in self.partitions:
            g = self._carried(p)
            for _, t, _ in self._parts.get(p, []):
                g = t if g is None else max(g, t)
            greatest[p] = g
        state = WindowState(checkpoint=self.scope.checkpoint, watermark_ms=final_wm,
                            positions=dict(end_positions), greatest=greatest, windows=carry,
                            logs=self.scope.logs, semantics=self.rules.semantics(),
                            aggregates=[tuple(a) for a in self.aggregates],
                            scope_sha256=self.scope.fingerprint)
        return Result(rows=rows, left_out=left_out, source_bounds=source_bounds, records=counts,
                      state=state)


def _ms(v):
    return "no watermark" if v is None else "%d ms" % v


def _least(a, b):
    """The lesser of two upper bounds on the watermark, None standing for no
    watermark at all (a partition that has delivered nothing holds it there)."""
    if a is None or b is None:
        return None
    return min(a, b)


def _exact(v, name):
    if isinstance(v, bool) or not isinstance(v, (int, Decimal)):
        raise ValueError("%s sums %r, which is not an exact integer or decimal" % (name, v))
    return v


def write_result(writer, result, output):
    """Writes a Result into a version 2 ReferenceWriter: each final window's row
    as `output(key, start, end, {aggregate: value})` gives it, and each window
    left out by its identity (the same row's identity columns; its aggregates,
    which are not decided, given as None)."""
    for key, start, end, aggs in result.rows:
        writer.add(output(key, start, end, aggs))
    for key, start, end, status, why in result.left_out:
        row = output(key, start, end, {name: None for name in _names(writer)})
        writer.add_window({c: row[c] for c in writer.scope.window.identity}, status, end, why)


def _names(writer):
    return [a.output for a in writer.scope.window.aggregates]
