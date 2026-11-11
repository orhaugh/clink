"""Compute an independent reference result for a clink checkpoint's scope.

    scope = Scope.load("scope.json")                     # clink incident-scope
    with ReferenceWriter("reference.ndjson", scope, {"name": "my-ref", "version": "1"}) as w:
        result = read_scope(scope, bootstrap, lambda rec: [w.add(r) for r in transform(rec)],
                            config={...credentials...})
        w.finish(result.complete, result.incomplete, result.verified, result.recorded, result.read)

`transform` is yours: it turns one Kafka record into the rows the job should
have written for it, as {column: value}. clink incident-reconcile --reference
then audits the job's output and its destination against the artifact.

A windowed job's scope (version 2) is computed with TumblingWindows: every
record of the job's lineage goes in with its event time, and the windows final
at the checkpoint come out, or why one is left out:

    rules = WindowRules(size_ms=10_000, lag_ms=2_000, time_column="ts")
    windows = TumblingWindows(scope, rules, [("events", "count"), ("net", "sum")])
    read_scope(scope, bootstrap, lambda rec: windows.add(rec.partition, rec.offset, ts(rec), row(rec)),
               run_start_offsets=...)
    with ReferenceWriter(path, scope, implementation, semantics=rules.semantics()) as w:
        result = windows.finish()
        write_result(w, result, output)
        w.finish(..., source_bounds=result.source_bounds, records=result.records)
"""

from .artifact import REFERENCE_FORMAT, REFERENCE_VERSION, WINDOW_REFERENCE_VERSION, ReferenceWriter
from .canonical import CellError, ColumnType, cell
from .kafka_range import ReadResult, Record, client_versions, kafka_uuid_text, read_scope
from .scope import Aggregate, Column, LineageCheckpoint, Range, Restore, Scope, ScopeError, Source, Window
from .windows import Result, TumblingWindows, WindowRules, write_result

__all__ = ["REFERENCE_FORMAT", "REFERENCE_VERSION", "WINDOW_REFERENCE_VERSION", "ReferenceWriter", "CellError", "ColumnType",
           "cell", "ReadResult", "Record", "client_versions", "kafka_uuid_text", "read_scope", "Column", "Range",
           "Scope", "ScopeError", "Source", "Aggregate", "LineageCheckpoint", "Restore", "Window",
           "Result", "TumblingWindows", "WindowRules", "write_result"]
