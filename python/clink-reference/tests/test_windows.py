"""The window reference without a broker: window assignment, the watermark's
bounds, late records before and after their window closes, finality, and the
version 2 scope and artifact."""

import hashlib
import json
import os
import tempfile
import unittest
from decimal import Decimal

from clink_reference import (ReferenceWriter, Scope, ScopeError, TumblingWindows, WindowRules,
                             write_result)

SIZE, LAG = 10_000, 2_000
RULES = WindowRules(size_ms=SIZE, lag_ms=LAG, time_column="ts")
AGGS = [("events", "count"), ("net", "sum")]


def scope_doc(lineage, final=None, partitions=(0, 1), checkpoint=None, lateness=0):
    """A version 2 scope: `lineage` is [(checkpoint, {partition: next offset},
    recorded watermark)], the last being the scope's checkpoint."""
    ckpt = checkpoint if checkpoint is not None else lineage[-1][0]
    final_wm = final if final is not None else lineage[-1][2]
    return {
        "format": "clink-audit-scope", "version": 2, "job": "1", "sink_op": "9",
        "attempts": ["a"], "graph_sha256": None, "checkpoint": ckpt, "starts_after": 0,
        "from_run_start": True, "starts_at_run_start": True, "epochs": [c for c, _, _ in lineage],
        "sources": [{"op": "7", "topic": "t", "cluster_id": "c", "topic_id": "i",
                     "isolation_level": "read_committed", "isolation_set_by_job": False,
                     "same_log": True,
                     "partitions": [{"partition": p, "from": None, "to": lineage[-1][1][p]}
                                    for p in partitions]}],
        "sources_complete": True,
        "output": {"columns": [{"name": "account", "type": "String"},
                               {"name": "window_start", "type": "Int64"},
                               {"name": "window_end", "type": "Int64"},
                               {"name": "events", "type": "Int64"},
                               {"name": "net", "type": "Nullable(Decimal(38, 2))"}],
                   "semantics": "clink-canonical-cells-v1"},
        "transform": {"kind": "tumbling-window", "size_ms": SIZE, "time_column": "ts",
                      "time_unit": "epoch milliseconds", "group_keys": ["account"],
                      "identity": ["account", "window_start", "window_end"],
                      "start_column": "window_start", "end_column": "window_end",
                      "aggregates": [{"function": "count", "input": None, "output": "events"},
                                     {"function": "sum", "input": "signed", "output": "net"}],
                      "watermark": {"strategy": "per-partition bounded out-of-orderness",
                                    "lag_ms": LAG, "idleness": "none",
                                    "assigned_on": "every record the source delivers"},
                      "lateness": {"allowed_lateness_ms": lateness, "late_records": "dropped"},
                      "source_parallelism": len(partitions), "alignment": "aligned"},
        "lineage": {"restores": [],
                    "checkpoints": [{"checkpoint": c,
                                     "positions": [{"op": "7", "partition": p, "next_offset": pos[p]}
                                                   for p in partitions],
                                     "watermark_ms": wm} for c, pos, wm in lineage]},
        "finality": {"checkpoint": ckpt, "watermark_ms": final_wm},
        "scope_sha256": "f" * 64}


def feed(scope, records, rules=RULES):
    """records: [(partition, offset, ts, account or None, amount)]"""
    w = TumblingWindows(scope, rules, AGGS)
    for p, o, ts, account, amount in records:
        w.add(p, o, ts, None if account is None else ((account,), {"net": amount}))
    return w.finish()


def final_rows(result):
    return {(k[0], s, e): (a["events"], a["net"]) for k, s, e, a in result.rows}


def left_out(result):
    return {(k[0], s, e): status for k, s, e, status, _ in result.left_out}


class Assignment(unittest.TestCase):
    def test_boundaries_belong_to_the_window_they_start(self):
        # Both partitions run to 40000, so the watermark reaches 38000.
        sc = Scope.parse(scope_doc([(1, {0: 5, 1: 1}, 38_000)]))
        r = feed(sc, [(0, 0, -1, "a", Decimal("8.00")),
                      (0, 1, 0, "a", Decimal("1.00")),
                      (0, 2, 9_999, "a", Decimal("2.00")),
                      (0, 3, 10_000, "a", Decimal("4.00")),
                      (0, 4, 40_000, None, None),
                      (1, 0, 40_000, None, None)])
        self.assertEqual(final_rows(r), {("a", 0, 10_000): (2, Decimal("3.00")),
                                         ("a", 10_000, 20_000): (1, Decimal("4.00")),
                                         ("a", -10_000, 0): (1, Decimal("8.00"))})

    def test_sum_skips_null_and_stays_exact(self):
        sc = Scope.parse(scope_doc([(1, {0: 4, 1: 1}, 28_000)]))
        r = feed(sc, [(0, 0, 1, "a", Decimal("0.10")), (0, 1, 2, "a", Decimal("0.20")),
                      (0, 2, 3, "b", None), (0, 3, 30_000, None, None),
                      (1, 0, 30_000, None, None)])
        self.assertEqual(final_rows(r)[("a", 0, 10_000)], (2, Decimal("0.30")))
        self.assertEqual(final_rows(r)[("b", 0, 10_000)], (1, None))
        self.assertRaises(ValueError, feed, sc, [(0, 0, 1, "a", 0.1)])


class Lateness(unittest.TestCase):
    def test_a_record_late_within_the_lag_is_folded_whatever_the_order(self):
        # Partition 0 reaches 11000 and then delivers 9500: the watermark was
        # at most 9000 by its own partition, short of the window's end.
        sc = Scope.parse(scope_doc([(1, {0: 3, 1: 1}, 28_000)]))
        r = feed(sc, [(0, 0, 11_000, "a", 1), (0, 1, 9_500, "a", 2), (0, 2, 30_000, None, None),
                      (1, 0, 30_000, None, None)])
        self.assertEqual(final_rows(r)[("a", 0, 10_000)], (1, 2))
        self.assertEqual(r.records["folded"], 2)

    def test_a_record_after_its_window_closed_is_dropped_on_the_recorded_watermark(self):
        # By checkpoint 1 the sink had watermark 18000, so window [0, 10000)
        # had closed, and a record for it after checkpoint 1 was dropped.
        sc = Scope.parse(scope_doc([(1, {0: 2, 1: 1}, 18_000), (2, {0: 4, 1: 2}, 28_000)]))
        r = feed(sc, [(0, 0, 5_000, "a", 1), (0, 1, 20_000, None, None),
                      (0, 2, 9_000, "a", 100), (0, 3, 30_000, None, None),
                      (1, 0, 20_000, None, None), (1, 1, 30_000, None, None)])
        self.assertEqual(final_rows(r)[("a", 0, 10_000)], (1, 1))
        self.assertEqual(r.records["dropped_late"], 1)
        self.assertEqual(left_out(r), {})

    def test_a_record_the_evidence_cannot_place_leaves_its_window_undetermined(self):
        # Late by its own partition, but no checkpoint before it recorded a
        # watermark that had closed its window: it may or may not have been
        # folded, depending on how far the other partition had been read.
        sc = Scope.parse(scope_doc([(1, {0: 3, 1: 2}, 28_000)]))
        r = feed(sc, [(0, 0, 20_000, None, None), (0, 1, 5_000, "a", 1),
                      (0, 2, 30_000, None, None), (1, 0, 20_000, None, None),
                      (1, 1, 30_000, None, None)])
        self.assertEqual(left_out(r), {("a", 0, 10_000): "undetermined"})
        self.assertEqual(final_rows(r), {})
        self.assertEqual(r.records["undetermined"], 1)

    def test_evidence_past_the_sources_bound_decides_nothing(self):
        # The recorded watermark at checkpoint 1 (50000) is past anything the
        # source allows: a record after it is neither folded nor dropped.
        sc = Scope.parse(scope_doc([(1, {0: 1, 1: 1}, 50_000), (2, {0: 2, 1: 2}, 50_000)]))
        r = feed(sc, [(0, 0, 1_000, None, None), (0, 1, 3_000, "a", 1),
                      (1, 0, 1_000, None, None), (1, 1, 60_000, None, None)])
        self.assertIn("past what the source allowed", r.left_out[0][4])

    def test_filtered_records_still_advance_the_watermark(self):
        sc = Scope.parse(scope_doc([(1, {0: 2, 1: 1}, 18_000)]))
        r = feed(sc, [(0, 0, 1_000, "a", 1), (0, 1, 20_000, None, None),
                      (1, 0, 20_000, None, None)])
        self.assertEqual(r.source_bounds, [{"checkpoint": 1, "watermark_ms": 18_000}])
        self.assertEqual(r.records["filtered"], 2)


class Finality(unittest.TestCase):
    def test_a_watermark_exactly_at_the_end_closes_the_window(self):
        sc = Scope.parse(scope_doc([(1, {0: 2, 1: 1}, 10_000)]))
        r = feed(sc, [(0, 0, 1_000, "a", 1), (0, 1, 12_000, None, None),
                      (1, 0, 12_000, None, None)])
        self.assertEqual(final_rows(r), {("a", 0, 10_000): (1, 1)})
        sc = Scope.parse(scope_doc([(1, {0: 2, 1: 1}, 9_999)]))
        r = feed(sc, [(0, 0, 1_000, "a", 1), (0, 1, 11_999, None, None),
                      (1, 0, 11_999, None, None)])
        self.assertEqual(final_rows(r), {})
        self.assertEqual(left_out(r), {("a", 0, 10_000): "open"})

    def test_allowed_lateness_holds_the_window_and_folds_records_inside_it(self):
        rules = WindowRules(size_ms=SIZE, lag_ms=LAG, time_column="ts", allowed_lateness_ms=5_000)
        sc = Scope.parse(scope_doc([(1, {0: 3, 1: 1}, 15_000)], lateness=5_000))
        r = feed(sc, [(0, 0, 1_000, "a", 1), (0, 1, 14_000, None, None), (0, 2, 2_000, "a", 2),
                      (1, 0, 17_000, None, None)], rules=rules)
        self.assertEqual(final_rows(r), {("a", 0, 10_000): (2, 3)})

    def test_a_quiet_partition_holds_every_window_open(self):
        # Partition 1 has delivered nothing: no watermark, nothing closes.
        sc = Scope.parse(scope_doc([(1, {0: 2, 1: 0}, None)]))
        r = feed(sc, [(0, 0, 1_000, "a", 1), (0, 1, 50_000, None, None)])
        self.assertEqual(r.source_bounds, [{"checkpoint": 1, "watermark_ms": None}])
        self.assertEqual(left_out(r), {("a", 0, 10_000): "open"})

    def test_a_window_the_source_could_have_closed_but_the_sink_did_not_record_is_undetermined(self):
        # The source allows 18000 at checkpoint 1; the sink recorded 9000.
        sc = Scope.parse(scope_doc([(1, {0: 2, 1: 1}, 9_000)]))
        r = feed(sc, [(0, 0, 1_000, "a", 1), (0, 1, 20_000, None, None),
                      (1, 0, 20_000, None, None)])
        self.assertEqual(left_out(r), {("a", 0, 10_000): "undetermined"})


class Artifact(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "ref.ndjson")

    def test_a_windowed_artifact_hashes_its_rows_and_windows_apart(self):
        sc = Scope.parse(scope_doc([(1, {0: 3, 1: 1}, 18_000)]))
        r = feed(sc, [(0, 0, 1_000, "a", Decimal("1.50")), (0, 1, 15_000, "b", 1),
                      (0, 2, 20_000, None, None), (1, 0, 20_000, None, None)])
        w = ReferenceWriter(self.path, sc, {"name": "t", "version": "1"},
                            semantics=RULES.semantics())
        write_result(w, r, lambda key, s, e, a: {"account": key[0], "window_start": s,
                                                 "window_end": e, "events": a["events"],
                                                 "net": a["net"]})
        end = w.finish(True, source_bounds=r.source_bounds, records=r.records)
        with open(self.path, "rb") as f:
            lines = f.read().split(b"\n")[:-1]
        header = json.loads(lines[0])
        self.assertEqual(header["version"], 2)
        self.assertEqual(header["semantics"]["watermark"]["lag_ms"], LAG)
        rows = [l for l in lines[1:-1] if b'"row"' in l]
        wins = [l for l in lines[1:-1] if b'"window"' in l]
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(wins[0])["window"]["identity"],
                         {"account": "Sb", "window_start": "I10000", "window_end": "I20000"})
        self.assertEqual(end["windows_sha256"],
                         hashlib.sha256(b"".join(l + b"\n" for l in wins)).hexdigest())
        self.assertEqual(end["rows_sha256"],
                         hashlib.sha256(b"".join(l + b"\n" for l in rows)).hexdigest())
        self.assertEqual(end["source_bounds"], [{"checkpoint": 1, "watermark_ms": 18_000}])

    def test_rules_are_required_for_a_windowed_scope_and_refused_for_a_record_wise_one(self):
        sc = Scope.parse(scope_doc([(1, {0: 1, 1: 1}, None)]))
        self.assertRaises(ValueError, ReferenceWriter, self.path, sc, {"name": "t", "version": "1"})
        w = ReferenceWriter(self.path, sc, {"name": "t", "version": "1"}, semantics=RULES.semantics())
        self.assertRaises(ValueError, w.finish, True)  # no source bounds
        self.assertRaises(ValueError, w.add_window, {"account": "a"}, "open", 1, "x")
        w.abandon()


class ScopeParsing(unittest.TestCase):
    def test_a_version_2_scope_reads_its_window_and_lineage(self):
        sc = Scope.parse(scope_doc([(1, {0: 1, 1: 2}, 5), (2, {0: 3, 1: 4}, 9)]))
        self.assertEqual(sc.window.identity, ("account", "window_start", "window_end"))
        self.assertEqual([lc.checkpoint for lc in sc.lineage], [1, 2])
        self.assertEqual(sc.lineage[1].positions, {0: 3, 1: 4})
        self.assertEqual(sc.final_watermark_ms, 9)

    def test_another_transform_or_a_lineage_without_positions_is_refused(self):
        bad = scope_doc([(1, {0: 1, 1: 1}, None)])
        bad["transform"]["kind"] = "hopping-window"
        self.assertRaises(ScopeError, Scope.parse, bad)
        bad = scope_doc([(1, {0: 1, 1: 1}, None)])
        bad["lineage"]["checkpoints"][0]["positions"][0]["next_offset"] = None
        self.assertRaises(ScopeError, Scope.parse, bad)
        bad = scope_doc([(1, {0: 1, 1: 1}, None)])
        bad["version"] = 3
        self.assertRaises(ScopeError, Scope.parse, bad)


if __name__ == "__main__":
    unittest.main()
