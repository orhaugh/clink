"""A windowed job's lineage computed whole and computed one increment at a
time, from the reference's own carried state, must agree exactly.

Random keyed streams over several partitions, with events out of order by up
to more than the lag and a few far behind, some filtered out; random
checkpoint cuts, a partition sometimes delivering nothing between two; and a
recorded watermark at each checkpoint at or below what the source allows,
sometimes exactly at it, never going back. The whole-lineage computation at
the last checkpoint and the chain (a whole-lineage seed at the first, then an
increment per checkpoint, each starting from the state the last one left)
must give the same final rows, the same windows left out, the same bounds,
and the same account of every record.
"""
import os
import random
import tempfile
import unittest
from collections import Counter
from decimal import Decimal

from clink_reference import Scope, StateError, TumblingWindows, WindowRules, WindowState

SIZE, LAG = 10_000, 2_000
AGGS = [("events", "count"), ("net", "sum")]


def _transform(lateness):
    return {"kind": "tumbling-window", "size_ms": SIZE, "time_column": "ts",
            "time_unit": "epoch milliseconds", "group_keys": ["account"],
            "identity": ["account", "window_start", "window_end"],
            "start_column": "window_start", "end_column": "window_end",
            "aggregates": [{"function": "count", "input": None, "output": "events"},
                           {"function": "sum", "input": "signed", "output": "net"}],
            "watermark": {"strategy": "per-partition bounded out-of-orderness", "lag_ms": LAG,
                          "idleness": "none", "assigned_on": "every record the source delivers"},
            "lateness": {"allowed_lateness_ms": lateness, "late_records": "dropped"},
            "source_parallelism": 2, "alignment": "aligned"}


def _checkpoint_doc(c, pos, wm, partitions):
    return {"checkpoint": c,
            "positions": [{"op": "7", "partition": p, "next_offset": pos[p]} for p in partitions],
            "watermark_ms": wm}


def scope(lineage, partitions, lateness, base=None):
    """A version 2 scope over `lineage` [(checkpoint, positions, watermark)],
    or with `base` (the same triple) the version 3 increment after it."""
    last = lineage[-1]
    start = base[1] if base else {p: None for p in partitions}
    doc = {
        "format": "clink-audit-scope", "version": 3 if base else 2, "job": "1", "sink_op": "9",
        "attempts": ["a"], "graph_sha256": None, "checkpoint": last[0],
        "starts_after": base[0] if base else 0, "from_run_start": base is None,
        "starts_at_run_start": base is None, "epochs": [c for c, _, _ in lineage],
        "sources": [{"op": "7", "topic": "t", "cluster_id": "c", "topic_id": "i",
                     "isolation_level": "read_committed", "isolation_set_by_job": False,
                     "same_log": True,
                     "partitions": [{"partition": p, "from": start[p], "to": last[1][p]}
                                    for p in partitions]}],
        "sources_complete": True,
        "output": {"columns": [{"name": "account", "type": "String"},
                               {"name": "window_start", "type": "Int64"},
                               {"name": "window_end", "type": "Int64"},
                               {"name": "events", "type": "Int64"},
                               {"name": "net", "type": "Nullable(Decimal(38, 2))"}],
                   "semantics": "clink-canonical-cells-v1"},
        "transform": _transform(lateness),
        "lineage": {"restores": [],
                    "checkpoints": [_checkpoint_doc(c, pos, wm, partitions) for c, pos, wm in lineage]},
        "finality": {"checkpoint": last[0], "watermark_ms": last[2]},
        "scope_sha256": "%064x" % (hash((tuple(c for c, _, _ in lineage), base and base[0])) & ((1 << 256) - 1)),
    }
    if base:
        doc["base"] = _checkpoint_doc(base[0], base[1], base[2], partitions)
    return Scope.parse(doc)


def random_case(rng):
    partitions = list(range(rng.choice([2, 3])))
    lateness = rng.choice([0, 0, 3_000])
    recs = {}
    for p in partitions:
        t = 1_700_000_000_000 + rng.randint(0, 5_000)
        out = []
        for _ in range(rng.randint(0, 120)):
            t += rng.randint(0, 600)
            ts = t
            if rng.random() < 0.25:
                ts -= rng.randint(0, 4_000)          # out of order, sometimes past the lag
            if rng.random() < 0.04:
                ts -= rng.randint(8_000, 40_000)     # far behind: its window has likely closed
            row = None if rng.random() < 0.1 else ((rng.choice("abc"),),
                                                   {"net": Decimal(rng.randint(-9999, 9999)) / 100})
            out.append(ts)
            recs.setdefault(p, []).append((len(out) - 1, ts, row))
    # Checkpoint cuts: per partition, positions that never go back.
    k = rng.randint(2, 8)
    lineage = []
    pos = {p: 0 for p in partitions}
    wm = None
    for i in range(k):
        for p in partitions:
            n = len(recs.get(p, []))
            pos[p] = min(n, pos[p] + rng.choice([0, rng.randint(0, max(1, n // k + 5))]))
        if i == k - 1:
            for p in partitions:
                pos[p] = len(recs.get(p, []))
        bound = None
        for p in partitions:
            before = [ts for o, ts, _ in recs.get(p, []) if o < pos[p]]
            if not before:
                bound = None
                break
            m = max(before) - LAG
            bound = m if bound is None else min(bound, m)
        if bound is not None:
            got = bound - rng.choice([0, 0, rng.randint(0, 3_000)])
            wm = got if wm is None else max(wm, got)
            wm = min(wm, bound)
        lineage.append((i + 1, dict(pos), wm))
    return partitions, lateness, recs, lineage


def feed(windows, recs, start, end):
    for p, rs in recs.items():
        for o, ts, row in rs:
            if start.get(p, 0) <= o < end[p]:
                windows.add(p, o, ts, row)


def finals(result):
    return Counter((k, s, e, tuple(sorted((n, v) for n, v in a.items()))) for k, s, e, a in result.rows)


class Increments(unittest.TestCase):
    def check(self, seed, tmp):
        rng = random.Random(seed)
        partitions, lateness, recs, lineage = random_case(rng)
        rules = WindowRules(size_ms=SIZE, lag_ms=LAG, time_column="ts", allowed_lateness_ms=lateness)
        whole = TumblingWindows(scope(lineage, partitions, lateness), rules, AGGS)
        feed(whole, recs, {}, lineage[-1][1])
        w = whole.finish()

        chain_rows = Counter()
        chain_undetermined = set()
        counts = Counter()
        bounds = []
        seed_scope = scope(lineage[:1], partitions, lateness)
        first = TumblingWindows(seed_scope, rules, AGGS)
        feed(first, recs, {}, lineage[0][1])
        r = first.finish()
        results = [r]
        for i in range(1, len(lineage)):
            # The state goes through a file and its hash, as a runner keeps it.
            path = os.path.join(tmp, "state-%d-%d.json" % (seed, lineage[i - 1][0]))
            sha = r.state.save(path)
            state = WindowState.load(path, expect_sha256=sha)
            inc = TumblingWindows(scope(lineage[i:i + 1], partitions, lateness, base=lineage[i - 1]),
                                  rules, AGGS, state=state)
            feed(inc, recs, lineage[i - 1][1], lineage[i][1])
            r = inc.finish()
            results.append(r)
        for i, r in enumerate(results):
            chain_rows += finals(r)
            fw = lineage[i][2]
            for k, s, e, status, _ in r.left_out:
                if status == "undetermined" and fw is not None and e + lateness <= fw:
                    chain_undetermined.add((k, s, e))
            counts += Counter(r.records)
            bounds += r.source_bounds
        last = results[-1]
        msg = "seed %d: partitions %s, lateness %d, lineage %s" % (
            seed, partitions, lateness, [(c, wm) for c, _, wm in lineage])
        self.assertEqual(chain_rows, finals(w), msg)
        final_wm = lineage[-1][2]
        whole_undetermined = {(k, s, e) for k, s, e, st, _ in w.left_out
                              if st == "undetermined" and final_wm is not None and e + lateness <= final_wm}
        self.assertEqual(chain_undetermined, whole_undetermined, msg)
        not_final = lambda res: {(k, s, e, st) for k, s, e, st, _ in res.left_out
                                 if final_wm is None or e + lateness > final_wm}
        self.assertEqual(not_final(last), not_final(w), msg)
        self.assertEqual(bounds, w.source_bounds, msg)
        self.assertEqual(+counts, +Counter(w.records), msg)
        # And the state each leaves at the last checkpoint is the same.
        self.assertEqual(last.state.document()["windows"], w.state.document()["windows"], msg)
        self.assertEqual(last.state.greatest, w.state.greatest, msg)

    def test_a_chain_of_increments_is_the_whole_lineage(self):
        with tempfile.TemporaryDirectory() as tmp:
            for seed in range(400):
                self.check(seed, tmp)


class Inconsistent(unittest.TestCase):
    def test_a_recorded_watermark_the_source_does_not_allow_is_reported_not_raised(self):
        # Partition 1 never delivers, so the source allows no watermark, and
        # the sink recorded one anyway: the record's fate is undetermined, and
        # the reason says so rather than failing to be written.
        rules = WindowRules(size_ms=SIZE, lag_ms=LAG, time_column="ts")
        lineage = [(1, {0: 1, 1: 0}, 50_000), (2, {0: 2, 1: 0}, 50_000)]
        t = TumblingWindows(scope(lineage, [0, 1], 0), rules, AGGS)
        t.add(0, 0, 1_000, (("a",), {"net": Decimal("1")}))
        t.add(0, 1, 2_000, (("a",), {"net": Decimal("2")}))
        r = t.finish()
        self.assertEqual(r.rows, [])
        self.assertEqual([x[3] for x in r.left_out], ["undetermined"])
        self.assertIn("no watermark", r.left_out[0][4])


class States(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.rules = WindowRules(size_ms=SIZE, lag_ms=LAG, time_column="ts")
        recs = {0: [(0, 1_000, (("a",), {"net": Decimal("1.50")})), (1, 9_000, (("b",), {"net": None}))],
                1: [(0, 2_000, (("a",), {"net": Decimal("-0.25")}))]}
        self.lineage = [(1, {0: 2, 1: 1}, 0), (2, {0: 2, 1: 1}, 0)]
        s = scope(self.lineage[:1], [0, 1], 0)
        t = TumblingWindows(s, self.rules, AGGS)
        feed(t, recs, {}, self.lineage[0][1])
        self.result = t.finish()
        self.path = os.path.join(self.tmp, "state.json")
        self.sha = self.result.state.save(self.path)

    def increment(self, state, base=None, aggregates=AGGS, rules=None):
        base = base or self.lineage[0]
        return TumblingWindows(scope(self.lineage[1:], [0, 1], 0, base=base), rules or self.rules,
                               aggregates, state=state)

    def test_a_state_round_trips_exactly(self):
        loaded = WindowState.load(self.path, expect_sha256=self.sha)
        self.assertEqual(loaded.document(), self.result.state.document())
        self.assertEqual(loaded.sha256, self.sha)
        sums = {w.key: w.sums for w in loaded.windows}
        self.assertEqual(sums[("a",)], {"net": Decimal("1.25")})

    def test_a_changed_state_is_refused_by_its_hash(self):
        with open(self.path, "a") as f:
            f.write(" ")
        with self.assertRaises(StateError):
            WindowState.load(self.path, expect_sha256=self.sha)

    def test_an_increment_refuses_a_state_that_is_not_its_start(self):
        state = WindowState.load(self.path, expect_sha256=self.sha)
        with self.assertRaises(StateError):
            self.increment(None)
        with self.assertRaisesRegex(StateError, "checkpoint"):
            self.increment(state, base=(7, self.lineage[0][1], 0))
        with self.assertRaisesRegex(StateError, "positions"):
            self.increment(state, base=(1, {0: 1, 1: 1}, 0))
        with self.assertRaisesRegex(StateError, "watermark"):
            self.increment(state, base=(1, self.lineage[0][1], 5))
        with self.assertRaisesRegex(StateError, "aggregates"):
            self.increment(state, aggregates=[("events", "count")])
        with self.assertRaisesRegex(StateError, "rules"):
            self.increment(state, rules=WindowRules(size_ms=SIZE, lag_ms=LAG + 1, time_column="ts"))
        with self.assertRaises(StateError):
            TumblingWindows(scope(self.lineage[:1], [0, 1], 0), self.rules, AGGS, state=state)


if __name__ == "__main__":
    unittest.main()
