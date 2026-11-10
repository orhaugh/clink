"""Compute an independent reference result for a clink checkpoint's scope.

    scope = Scope.load("scope.json")                     # clink incident-scope
    with ReferenceWriter("reference.ndjson", scope, {"name": "my-ref", "version": "1"}) as w:
        result = read_scope(scope, bootstrap, lambda rec: [w.add(r) for r in transform(rec)],
                            config={...credentials...})
        w.finish(result.complete, result.incomplete, result.verified, result.recorded, result.read)

`transform` is yours: it turns one Kafka record into the rows the job should
have written for it, as {column: value}. clink incident-reconcile --reference
then audits the job's output and its destination against the artifact.
"""

from .artifact import REFERENCE_FORMAT, REFERENCE_VERSION, ReferenceWriter
from .canonical import CellError, ColumnType, cell
from .kafka_range import ReadResult, Record, client_versions, kafka_uuid_text, read_scope
from .scope import Column, Range, Scope, ScopeError, Source

__all__ = ["REFERENCE_FORMAT", "REFERENCE_VERSION", "ReferenceWriter", "CellError", "ColumnType",
           "cell", "ReadResult", "Record", "client_versions", "kafka_uuid_text", "read_scope", "Column", "Range",
           "Scope", "ScopeError", "Source"]
