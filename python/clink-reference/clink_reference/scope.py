"""The scope document `clink incident-scope` writes (format clink-audit-scope).

A reference implementation computes from it and nothing else: which Kafka
logs and offset ranges a checkpoint's scope consumed, under which isolation
level, and the columns the job's sink writes. The document is carried into the
artifact verbatim, and `clink incident-reconcile --reference` refuses an
artifact whose scope is not the one it audits.

Version 1 is a record-wise job's. Version 2 is a windowed job's: its ranges
start at the job's own start, and it adds the window's rules as the job
declares them (`transform`), the job's lineage (each completed checkpoint's
source positions and the watermark the sink recorded at its barrier, and the
restores it passes through) and the watermark windows are final by
(`finality`).
"""

import json
from dataclasses import dataclass, field

SCOPE_FORMAT = "clink-audit-scope"
SCOPE_VERSION = 1
WINDOW_SCOPE_VERSION = 2
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
class Aggregate:
    function: str      # "count" or "sum"
    input: str | None  # the column it reads; None for COUNT(*)
    output: str        # the sink column it lands in


@dataclass(frozen=True)
class Window:
    """The window's rules as the job's graph declares them. A reference
    implements its own specification (windows.WindowRules); the audit
    compares the two."""
    document: dict = field(repr=False)
    size_ms: int
    time_column: str
    group_keys: tuple[str, ...]
    identity: tuple[str, ...]  # the sink columns naming one window's row
    start_column: str
    end_column: str
    aggregates: tuple[Aggregate, ...]
    lag_ms: int
    allowed_lateness_ms: int
    source_parallelism: int


@dataclass(frozen=True)
class LineageCheckpoint:
    checkpoint: int
    positions: dict = field(repr=False)  # partition -> the next offset to read
    watermark_ms: int | None             # the least the sink's subtasks had at its barrier


@dataclass(frozen=True)
class Restore:
    restored_from: int
    first_epoch: int


@dataclass(frozen=True)
class Scope:
    document: dict = field(repr=False)
    fingerprint: str
    version: int
    job: str
    checkpoint: int
    starts_after: int
    from_run_start: bool
    sources: tuple[Source, ...]
    sources_complete: bool
    columns: tuple[Column, ...]
    window: Window | None = None
    lineage: tuple[LineageCheckpoint, ...] = ()
    restores: tuple[Restore, ...] = ()
    final_watermark_ms: int | None = None

    @staticmethod
    def load(path):
        with open(path, "rb") as f:
            return Scope.parse(json.loads(f.read().decode("utf-8")))

    @staticmethod
    def parse(doc):
        if not isinstance(doc, dict) or doc.get("format") != SCOPE_FORMAT:
            raise ScopeError("not a %s document" % SCOPE_FORMAT)
        version = doc.get("version")
        if version not in (SCOPE_VERSION, WINDOW_SCOPE_VERSION):
            raise ScopeError("version %r of the scope format; this library reads %d and %d"
                             % (version, SCOPE_VERSION, WINDOW_SCOPE_VERSION))
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
            extra = {}
            if version == WINDOW_SCOPE_VERSION:
                extra = _window_parts(doc)
            return Scope(document=doc, fingerprint=doc["scope_sha256"], version=version,
                         job=doc["job"], checkpoint=int(doc["checkpoint"]),
                         starts_after=int(doc["starts_after"]),
                         from_run_start=bool(doc["from_run_start"]), sources=sources,
                         sources_complete=bool(doc["sources_complete"]), columns=columns, **extra)
        except (KeyError, TypeError, ValueError) as e:
            raise ScopeError("the scope document is malformed: %s" % e) from e


def _window_parts(doc):
    t = doc["transform"]
    if t["kind"] != "tumbling-window":
        raise ScopeError("the scope's transform is %r; this library reads tumbling windows"
                         % t["kind"])
    window = Window(
        document=t, size_ms=int(t["size_ms"]), time_column=t["time_column"],
        group_keys=tuple(t["group_keys"]), identity=tuple(t["identity"]),
        start_column=t["start_column"], end_column=t["end_column"],
        aggregates=tuple(Aggregate(a["function"], a.get("input"), a["output"])
                         for a in t["aggregates"]),
        lag_ms=int(t["watermark"]["lag_ms"]),
        allowed_lateness_ms=int(t["lateness"]["allowed_lateness_ms"]),
        source_parallelism=int(t["source_parallelism"]))
    lineage = []
    for c in doc["lineage"]["checkpoints"]:
        positions = {}
        for p in c["positions"]:
            if p["next_offset"] is None:
                raise ValueError("checkpoint %s records no position for partition %s"
                                 % (c["checkpoint"], p["partition"]))
            positions[int(p["partition"])] = int(p["next_offset"])
        wm = c.get("watermark_ms")
        lineage.append(LineageCheckpoint(int(c["checkpoint"]), positions,
                                         None if wm is None else int(wm)))
    restores = tuple(Restore(int(r["restored_from"]), int(r["first_epoch"]))
                     for r in doc["lineage"]["restores"])
    final = doc["finality"]
    if int(final["checkpoint"]) != int(doc["checkpoint"]):
        raise ValueError("the finality names checkpoint %s and the scope %s"
                         % (final["checkpoint"], doc["checkpoint"]))
    fw = final.get("watermark_ms")
    return {"window": window, "lineage": tuple(lineage), "restores": restores,
            "final_watermark_ms": None if fw is None else int(fw)}
