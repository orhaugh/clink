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
"""

import bisect
from dataclasses import dataclass, field
from decimal import Decimal

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


class TumblingWindows:
    """Feed it every record of a version 2 scope's ranges, in offset order
    within each partition, then call finish().

    aggregates: [(output, function)], function "count" (rows) or "sum" (of
    the value given for output, None being SQL's NULL, which SUM skips).
    """

    def __init__(self, scope, rules, aggregates):
        if scope.version != 2 or scope.window is None:
            raise ValueError("a windowed (version 2) scope is needed")
        for _, fn in aggregates:
            if fn not in ("count", "sum"):
                raise ValueError("aggregate function %r: COUNT and SUM are supported" % fn)
        self.scope = scope
        self.rules = rules
        self.aggregates = list(aggregates)
        self._parts = {}  # partition -> [(offset, ts, row)]
        self.partitions = sorted({p for lc in scope.lineage for p in lc.positions})

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

    def _source_bounds(self):
        """The least partition maximum less the lag, at each lineage
        checkpoint's positions: None while a partition has delivered nothing."""
        prefix = {}  # partition -> (offsets, greatest event time up to and including each)
        for p in self.partitions:
            recs = self._parts.get(p, [])
            offsets, greatest, mx = [], [], None
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
                if n == 0:
                    least = None
                    break
                least = greatest[n - 1] if least is None else min(least, greatest[n - 1])
            out.append(None if least is None else least - self.rules.lag_ms)
        return out

    def finish(self):
        size, lag, late = self.rules.size_ms, self.rules.lag_ms, self.rules.allowed_lateness_ms
        lineage = self.scope.lineage
        bounds = self._source_bounds()
        counts = {"records": 0, "folded": 0, "dropped_late": 0, "undetermined": 0,
                  "filtered": 0}
        windows = {}
        for p, recs in self._parts.items():
            if p not in self.partitions:
                raise ValueError("partition %d is not one the scope's lineage records" % p)
            positions = [lc.positions[p] for lc in lineage]
            if any(b < a for a, b in zip(positions, positions[1:])):
                raise ValueError("partition %d's positions go back along the lineage" % p)
            running = None  # the partition's greatest event time so far
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
                lower = lineage[after - 1].watermark_ms if after > 0 else None
                folded = upper is None or end + late > upper
                dropped = lower is not None and end + late <= lower
                w = windows.get((key, end))
                if w is None:
                    w = windows[(key, end)] = _Window(key, start, end)
                if folded and dropped:
                    counts["undetermined"] += 1
                    w.undetermined = w.undetermined or (
                        "record %d@%d: the watermark recorded at checkpoint %d (%d ms) is past "
                        "what the source allowed when the record arrived (%d ms)"
                        % (p, offset, lineage[after - 1].checkpoint, lower, upper))
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
                        "watermark then was at least %s and at most %d, and the window closes at "
                        "%d" % (p, offset, ts, "%d ms" % lower if lower is not None else "none",
                                upper, end + late))
        final_wm = self.scope.final_watermark_ms
        final_bound = bounds[-1] if bounds else None
        rows, left_out = [], []
        for (key, end), w in sorted(windows.items(), key=lambda kv: (kv[0][1], repr(kv[0][0]))):
            if w.rows == 0 and w.undetermined is None:
                continue  # every record of it was dropped: the job emits no row
            closes = end + late
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
        return Result(rows=rows, left_out=left_out, source_bounds=source_bounds, records=counts)


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
