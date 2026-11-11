"""The reference artifact (format clink-reference-result): NDJSON.

    {"format": ..., "version": 1, "scope": <the scope document>, "implementation": {...}, ...}
    {"row": {<column>: <canonical cell>, ...}}           once per expected row, copies kept
    {"end": {"rows": N, "rows_sha256": ..., "header_sha256": ..., "complete": ...,
             "incomplete": [...], "verified": [...], "recorded": [...], "read": [...]}}

rows_sha256 is over the bytes of every row line with its newline, and
header_sha256 over the header line's. A file without its end line was not
finished, and clink refuses it. The hashes establish that the file is whole as
its writer finished it, not who wrote it.

A windowed (version 2) scope's artifact is version 2: its header adds
"semantics", the window rules the reference implements; its body adds a line

    {"window": {"identity": {<identity column>: <cell>, ...}, "status": "open" | "undetermined",
                "end_ms": N, "why": ...}}

per window it leaves out of the comparison; and its end line adds "windows",
"windows_sha256" (over the window lines), "source_bounds" and "records".

An increment's (version 3) scope gives a version 3 artifact: its header adds
"state_in", the checkpoint and sha256 of the window state it started from,
and its end line "state_out", those of the state it left at the scope's
checkpoint. A windowed artifact of either version may give "state_out", so a
whole-lineage reference can start a chain of increments.
"""

import datetime as _dt
import hashlib
import json

from .canonical import ColumnType, cell

REFERENCE_FORMAT = "clink-reference-result"
REFERENCE_VERSION = 1
WINDOW_REFERENCE_VERSION = 2
WINDOW_INCREMENT_REFERENCE_VERSION = 3


def _line(obj):
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


class ReferenceWriter:
    """Writes one artifact for one scope. Rows are given as {column: value};
    each is encoded by its column's type and must name every column. A
    windowed scope's artifact also needs the rules the reference implements
    (`semantics`, windows.WindowRules.semantics())."""

    def __init__(self, path, scope, implementation, reader=None, semantics=None, state_in=None):
        if not implementation.get("name") or not implementation.get("version"):
            raise ValueError("the implementation needs a name and a version")
        version = getattr(scope, "version", 1)
        self.windowed = version in (2, 3)
        self.increment = version == 3
        if self.increment and not state_in:
            raise ValueError("an increment's reference names the state it started from (state_in)")
        if not self.increment and state_in:
            raise ValueError("only an increment's reference starts from a state")
        if self.windowed and not semantics:
            raise ValueError("a windowed scope's reference says which window rules it implements")
        if not self.windowed and semantics:
            raise ValueError("a record-wise scope's reference has no window rules")
        self.scope = scope
        self.path = path
        self.types = [(c.name, ColumnType(c.type)) for c in scope.columns]
        header = {"format": REFERENCE_FORMAT,
                  "version": (WINDOW_INCREMENT_REFERENCE_VERSION if self.increment else
                              WINDOW_REFERENCE_VERSION if self.windowed else REFERENCE_VERSION),
                  "scope": scope.document, "implementation": implementation,
                  "created": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds")}
        if reader:
            header["reader"] = reader
        if self.windowed:
            header["semantics"] = semantics
        if self.increment:
            header["state_in"] = {"checkpoint": int(state_in["checkpoint"]),
                                  "sha256": str(state_in["sha256"])}
        self._header = _line(header)
        self._rows_hash = hashlib.sha256()
        self._windows_hash = hashlib.sha256()
        self.rows = 0
        self.windows = 0
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

    def add_window(self, identity, status, end_ms, why):
        """A window left out of the comparison: open at the scope's checkpoint,
        or undetermined, and why. `identity` is {column: value} for the scope's
        identity columns."""
        if not self.windowed:
            raise ValueError("only a windowed scope's reference leaves windows out")
        if status not in ("open", "undetermined"):
            raise ValueError("a window is left out as open or undetermined, not %r" % status)
        want = list(self.scope.window.identity)
        if sorted(identity) != sorted(want):
            raise ValueError("a window's identity is %s; the scope's is %s"
                             % (sorted(identity), sorted(want)))
        types = dict(self.types)
        line = _line({"window": {"identity": {n: cell(identity[n], types[n]) for n in want},
                                 "status": status, "end_ms": int(end_ms), "why": why}})
        self._windows_hash.update(line)
        self._out.write(line)
        self.windows += 1

    def finish(self, complete, incomplete=(), verified=(), recorded=(), read=(),
               source_bounds=None, records=None, state_out=None):
        """Ends the artifact. `complete` is the writer's claim, and comes with
        why not when false: an incomplete reference is never compared. A
        windowed scope's needs `source_bounds`, the watermark the source's
        records allow at each checkpoint of the lineage."""
        incomplete = list(incomplete)
        if complete and incomplete:
            raise ValueError("a complete reference has no reasons it is incomplete")
        if not complete and not incomplete:
            raise ValueError("an incomplete reference says why")
        end = {"rows": self.rows, "rows_sha256": self._rows_hash.hexdigest(),
               "header_sha256": hashlib.sha256(self._header).hexdigest(),
               "complete": bool(complete), "incomplete": incomplete,
               "verified": list(verified), "recorded": list(recorded), "read": list(read)}
        if self.windowed:
            if source_bounds is None:
                raise ValueError("a windowed scope's reference gives the source's watermark bounds")
            end.update({"windows": self.windows, "windows_sha256": self._windows_hash.hexdigest(),
                        "source_bounds": list(source_bounds), "records": dict(records or {})})
            if self.increment and complete and not state_out:
                raise ValueError("a complete increment's reference names the state it leaves "
                                 "(state_out)")
            if state_out:
                end["state_out"] = {"checkpoint": int(state_out["checkpoint"]),
                                    "sha256": str(state_out["sha256"])}
        elif state_out:
            raise ValueError("a record-wise reference leaves no window state")
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
