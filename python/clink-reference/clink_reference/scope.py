"""The scope document `clink incident-scope` writes (format clink-audit-scope).

A reference implementation computes from it and nothing else: which Kafka
logs and offset ranges a checkpoint's scope consumed, under which isolation
level, and the columns the job's sink writes. The document is carried into the
artifact verbatim, and `clink incident-reconcile --reference` refuses an
artifact whose scope is not the one it audits.
"""

import json
from dataclasses import dataclass, field

SCOPE_FORMAT = "clink-audit-scope"
SCOPE_VERSION = 1
SEMANTICS = "clink-canonical-cells-v1"


class ScopeError(ValueError):
    """A document that is not a scope this library reads."""


@dataclass(frozen=True)
class Range:
    partition: int
    start: int | None  # None: the run's own start, which the checkpoints do not record
    end: int | None    # exclusive; None: not recorded


@dataclass(frozen=True)
class Source:
    op: str
    topic: str | None
    cluster_id: str | None
    topic_id: str | None
    isolation_level: str
    ranges: tuple[Range, ...]


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class Scope:
    document: dict = field(repr=False)
    fingerprint: str
    job: str
    checkpoint: int
    starts_after: int
    from_run_start: bool
    sources: tuple[Source, ...]
    sources_complete: bool
    columns: tuple[Column, ...]

    @staticmethod
    def load(path):
        with open(path, "rb") as f:
            return Scope.parse(json.loads(f.read().decode("utf-8")))

    @staticmethod
    def parse(doc):
        if not isinstance(doc, dict) or doc.get("format") != SCOPE_FORMAT:
            raise ScopeError("not a %s document" % SCOPE_FORMAT)
        if doc.get("version") != SCOPE_VERSION:
            raise ScopeError("version %r of the scope format; this library reads %d"
                             % (doc.get("version"), SCOPE_VERSION))
        output = doc.get("output") or {}
        if output.get("semantics") != SEMANTICS:
            raise ScopeError("the scope's cells are %r; this library writes %s"
                             % (output.get("semantics"), SEMANTICS))
        try:
            sources = tuple(
                Source(op=s["op"], topic=s.get("topic"), cluster_id=s.get("cluster_id"),
                       topic_id=s.get("topic_id"), isolation_level=s["isolation_level"],
                       ranges=tuple(Range(int(p["partition"]), p.get("from"), p.get("to"))
                                    for p in s["partitions"]))
                for s in doc["sources"])
            columns = tuple(Column(c["name"], c["type"]) for c in output["columns"])
            return Scope(document=doc, fingerprint=doc["scope_sha256"], job=doc["job"],
                         checkpoint=int(doc["checkpoint"]), starts_after=int(doc["starts_after"]),
                         from_run_start=bool(doc["from_run_start"]), sources=sources,
                         sources_complete=bool(doc["sources_complete"]), columns=columns)
        except (KeyError, TypeError, ValueError) as e:
            raise ScopeError("the scope document is malformed: %s" % e) from e
