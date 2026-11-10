"""The library without a broker: canonical cells against the vectors clink's
own canonical rules give (include/clink/incident/reconcile.hpp), and the
artifact's form, hashes and end line."""

import datetime as dt
import hashlib
import json
import math
import os
import tempfile
import unittest
from decimal import Decimal

from clink_reference import CellError, ReferenceWriter, Scope, ScopeError, cell, kafka_uuid_text


def scope_doc(columns=(("id", "Int64"), ("amount", "Decimal(18, 2)"), ("note", "Nullable(String)"))):
    return {"format": "clink-audit-scope", "version": 1, "job": "1", "sink_op": "9", "attempts": ["a"],
            "graph_sha256": None, "checkpoint": 6, "starts_after": 5, "from_run_start": False,
            "starts_at_run_start": False, "epochs": [6],
            "sources": [{"op": "7", "topic": "t", "cluster_id": "c", "topic_id": "i",
                         "isolation_level": "read_committed", "isolation_set_by_job": False,
                         "same_log": True, "partitions": [{"partition": 0, "from": 1, "to": 5}]}],
            "sources_complete": True,
            "output": {"columns": [{"name": n, "type": t} for n, t in columns],
                       "semantics": "clink-canonical-cells-v1"},
            "scope_sha256": "f" * 64}


class Cells(unittest.TestCase):
    def test_integers_floats_and_bools(self):
        self.assertEqual(cell(-12, "Int64"), "I-12")
        self.assertEqual(cell(0, "UInt8"), "I0")
        self.assertRaises(CellError, cell, 256, "UInt8")
        self.assertRaises(CellError, cell, -1, "UInt64")
        self.assertRaises(CellError, cell, True, "Int64")
        self.assertEqual(cell(1.0, "Float64"), "F3ff0000000000000")
        self.assertEqual(cell(-0.0, "Float64"), "F8000000000000000")
        self.assertEqual(cell(math.nan, "Float64"), "Fnan")
        self.assertEqual(cell(1.0, "Float32"), "f3f800000")
        self.assertEqual(cell(True, "Bool"), "B1")

    def test_decimals_are_exact_at_their_scale(self):
        self.assertEqual(cell(Decimal("10.50"), "Decimal(18, 2)"), "D1050@2")
        self.assertEqual(cell("-0.05", "Decimal(18, 2)"), "D-5@2")
        self.assertEqual(cell(7, "Decimal(18, 2)"), "D700@2")
        self.assertEqual(cell("-0.00", "Decimal(18, 2)"), "D0@2")
        self.assertEqual(cell("1.5", "Decimal64(3)"), "D1500@3")
        self.assertRaises(CellError, cell, "0.005", "Decimal(18, 2)")
        self.assertRaises(CellError, cell, 0.1, "Decimal(18, 2)")
        self.assertRaises(CellError, cell, "1" * 17, "Decimal(18, 2)")

    def test_strings_times_dates_and_null(self):
        self.assertEqual(cell("é \"q\"\\", "LowCardinality(String)"), "Sé \"q\"\\")
        self.assertEqual(cell(None, "Nullable(String)"), "N")
        utc = dt.timezone.utc
        self.assertEqual(cell(dt.datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=utc),
                              "DateTime64(3, 'UTC')"), "T1767225600123@3")
        self.assertEqual(cell(1767225600123, "DateTime64(3)"), "T1767225600123@3")
        self.assertRaises(CellError, cell, dt.datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=utc),
                          "DateTime64(3)")
        self.assertEqual(cell(dt.datetime(1970, 1, 1, 0, 0, 1, tzinfo=utc), "DateTime"), "T1@0")
        self.assertEqual(cell(dt.date(1970, 1, 2), "Date32"), "d1")

    def test_the_client_versions_read_as_whole_version_text(self):
        try:
            import confluent_kafka  # noqa: F401
        except ImportError:
            self.skipTest("confluent-kafka is not installed")
        from clink_reference import client_versions
        client, lib = client_versions()
        self.assertRegex(client, r"^\d+\.\d+")
        self.assertRegex(lib, r"^\d+\.\d+")

    def test_kafka_ids_read_as_kafkas_tools_print_them(self):
        # A topic id Apache Kafka 3.9.1 assigned, whose standard base64 has a '+'.
        self.assertEqual(kafka_uuid_text(1941277378982920319, -9032957890187645041),
                         "GvDNfajHQH-CpHyoVE2vjw")


class Artifact(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "ref.ndjson")
        self.scope = Scope.parse(scope_doc())

    def lines(self):
        with open(self.path, "rb") as f:
            return f.read().split(b"\n")[:-1]

    def test_a_finished_artifact_hashes_its_rows_and_header(self):
        w = ReferenceWriter(self.path, self.scope, {"name": "t", "version": "1"})
        w.add({"id": 1, "amount": Decimal("1.00"), "note": None})
        w.add({"id": 1, "amount": Decimal("1.00"), "note": None})  # copies are kept
        end = w.finish(True, verified=["x"])
        lines = self.lines()
        self.assertEqual(len(lines), 4)
        header = json.loads(lines[0])
        self.assertEqual(header["format"], "clink-reference-result")
        self.assertEqual(header["scope"], scope_doc())
        self.assertEqual(json.loads(lines[1]), {"row": {"amount": "D100@2", "id": "I1", "note": "N"}})
        self.assertEqual(end["rows"], 2)
        self.assertEqual(end["rows_sha256"],
                         hashlib.sha256(lines[1] + b"\n" + lines[2] + b"\n").hexdigest())
        self.assertEqual(end["header_sha256"], hashlib.sha256(lines[0] + b"\n").hexdigest())
        self.assertEqual(json.loads(lines[3])["end"]["complete"], True)

    def test_an_abandoned_artifact_has_no_end_line(self):
        with ReferenceWriter(self.path, self.scope, {"name": "t", "version": "1"}) as w:
            w.add({"id": 1, "amount": 1, "note": "a"})
        self.assertNotIn(b'"end"', b"\n".join(self.lines()))

    def test_rows_must_name_every_column_and_an_incomplete_end_says_why(self):
        w = ReferenceWriter(self.path, self.scope, {"name": "t", "version": "1"})
        self.assertRaises(ValueError, w.add, {"id": 1})
        self.assertRaises(ValueError, w.finish, False)
        self.assertRaises(ValueError, w.finish, True, ["why"])
        end = w.finish(False, ["partition 0 expired"])
        self.assertFalse(end["complete"])

    def test_a_document_that_is_not_a_scope_is_refused(self):
        self.assertRaises(ScopeError, Scope.parse, {"format": "other"})
        bad = scope_doc()
        bad["output"]["semantics"] = "other"
        self.assertRaises(ScopeError, Scope.parse, bad)


if __name__ == "__main__":
    unittest.main()
