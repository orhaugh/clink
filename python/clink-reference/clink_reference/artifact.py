"""The reference artifact (format clink-reference-result, version 1): NDJSON.

    {"format": ..., "version": 1, "scope": <the scope document>, "implementation": {...}, ...}
    {"row": {<column>: <canonical cell>, ...}}           once per expected row, copies kept
    {"end": {"rows": N, "rows_sha256": ..., "header_sha256": ..., "complete": ...,
             "incomplete": [...], "verified": [...], "recorded": [...], "read": [...]}}

rows_sha256 is over the bytes of every row line with its newline, and
header_sha256 over the header line's. A file without its end line was not
finished, and clink refuses it. The hashes establish that the file is whole as
its writer finished it, not who wrote it.
"""

import datetime as _dt
import hashlib
import json

from .canonical import ColumnType, cell

REFERENCE_FORMAT = "clink-reference-result"
REFERENCE_VERSION = 1


def _line(obj):
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


class ReferenceWriter:
    """Writes one artifact for one scope. Rows are given as {column: value};
    each is encoded by its column's type and must name every column."""

    def __init__(self, path, scope, implementation, reader=None):
        if not implementation.get("name") or not implementation.get("version"):
            raise ValueError("the implementation needs a name and a version")
        self.scope = scope
        self.path = path
        self.types = [(c.name, ColumnType(c.type)) for c in scope.columns]
        header = {"format": REFERENCE_FORMAT, "version": REFERENCE_VERSION,
                  "scope": scope.document, "implementation": implementation,
                  "created": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds")}
        if reader:
            header["reader"] = reader
        self._header = _line(header)
        self._rows_hash = hashlib.sha256()
        self.rows = 0
        self._out = open(path, "wb")
        self._out.write(self._header)
        self._finished = False

    def add(self, row):
        names = set(row)
        want = {n for n, _ in self.types}
        if names != want:
            raise ValueError("a row's columns are %s; the scope's are %s"
                             % (sorted(names), sorted(want)))
        line = _line({"row": {n: cell(row[n], t) for n, t in self.types}})
        self._rows_hash.update(line)
        self._out.write(line)
        self.rows += 1

    def finish(self, complete, incomplete=(), verified=(), recorded=(), read=()):
        """Ends the artifact. `complete` is the writer's claim, and comes with
        why not when false: an incomplete reference is never compared."""
        incomplete = list(incomplete)
        if complete and incomplete:
            raise ValueError("a complete reference has no reasons it is incomplete")
        if not complete and not incomplete:
            raise ValueError("an incomplete reference says why")
        end = {"rows": self.rows, "rows_sha256": self._rows_hash.hexdigest(),
               "header_sha256": hashlib.sha256(self._header).hexdigest(),
               "complete": bool(complete), "incomplete": incomplete,
               "verified": list(verified), "recorded": list(recorded), "read": list(read)}
        self._out.write(_line({"end": end}))
        self._out.close()
        self._finished = True
        return end

    def abandon(self):
        """Closes the file without an end line: an unfinished artifact, which
        clink refuses."""
        if not self._finished:
            self._out.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.abandon()
        return False
