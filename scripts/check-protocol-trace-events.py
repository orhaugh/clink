#!/usr/bin/env python3
"""Hold the protocol trace vocabulary, the engine and the trace module in agreement.

Three things must say the same:
  1. every protocol_trace::Event("X") in the source tree names an `event X`
     in formal/trace/events.txt (an event the specification does not know
     fails the build);
  2. every `event X` in the manifest is consumed by
     formal/trace/TraceExactlyOnce.tla (Is("X")), and every `unobserved A`
     appears in its Hidden step;
  3. every action in ExactlyOnce.tla's Next relation is either reached by
     the trace module or listed as unobserved (an action nothing in the
     engine ever emits is a hole in the trace, not a pass).
Exit 1 with the differences listed, else 0.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "formal" / "trace" / "events.txt"
MODULE = ROOT / "formal" / "trace" / "TraceExactlyOnce.tla"
SPEC = ROOT / "formal" / "ExactlyOnce.tla"
SOURCE_DIRS = [ROOT / "src", ROOT / "include", ROOT / "impls"]


def manifest() -> tuple[set[str], set[str]]:
    events, unobserved = set(), set()
    for line in MANIFEST.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        kind, _, name = line.partition(" ")
        if kind == "event":
            events.add(name.strip())
        elif kind == "unobserved":
            unobserved.add(name.strip())
        else:
            raise SystemExit(f"{MANIFEST}: unknown line: {line}")
    return events, unobserved


def emitted_in_code() -> dict[str, list[str]]:
    pat = re.compile(r'protocol_trace::Event\(\s*"([A-Za-z]+)"')
    dyn = re.compile(r'trace_walk\("([A-Za-z]+)"')
    found: dict[str, list[str]] = {}
    for d in SOURCE_DIRS:
        for f in d.rglob("*"):
            if f.suffix not in {".cpp", ".hpp", ".h"} or not f.is_file():
                continue
            text = f.read_text(errors="replace")
            for m in pat.finditer(text):
                found.setdefault(m.group(1), []).append(str(f.relative_to(ROOT)))
            for m in dyn.finditer(text):
                found.setdefault(m.group(1), []).append(str(f.relative_to(ROOT)))
            # The Kafka sink emits DeliverCommit / DeliverAbort through a
            # conditional name; the committing sink too.
            if 'Event(is_commit ? "DeliverCommit" : "DeliverAbort")' in text:
                found.setdefault("DeliverCommit", []).append(str(f.relative_to(ROOT)))
                found.setdefault("DeliverAbort", []).append(str(f.relative_to(ROOT)))
    return found


def spec_actions() -> set[str]:
    text = SPEC.read_text()
    m = re.search(r"^Next ==\n(.*?)\n\n", text, re.S | re.M)
    if not m:
        raise SystemExit("ExactlyOnce.tla: Next relation not found")
    body = m.group(1)
    names = set(re.findall(r"\b([A-Z][A-Za-z]+)(?:\([a-z]\))?", body))
    # Quantifier vocabulary, not actions.
    return names - {"Sinks", "Workers", "E"}


def main() -> int:
    events, unobserved = manifest()
    code = emitted_in_code()
    module = MODULE.read_text()
    problems: list[str] = []

    for name, where in sorted(code.items()):
        if name not in events:
            problems.append(f"emitted but not in {MANIFEST.name}: {name} ({where[0]})")
    for name in sorted(events):
        if name not in code:
            problems.append(f"in {MANIFEST.name} but nothing emits it: {name}")
        if f'Is("{name}")' not in module:
            problems.append(f"in {MANIFEST.name} but {MODULE.name} never consumes it: {name}")
    hidden = re.search(r"^Hidden ==\n(.*?)\n\n", module, re.S | re.M)
    hidden_body = hidden.group(1) if hidden else ""
    for name in sorted(unobserved):
        if name == "Done":
            continue  # the run's quiescent stutter; the trace has its own end
        if not re.search(rf"\b{name}\b", hidden_body):
            problems.append(f"unobserved in {MANIFEST.name} but not a hidden step in {MODULE.name}: {name}")

    reached = set(re.findall(r"\b([A-Z][A-Za-z]+)\(", module)) | set(re.findall(r"/\\ ([A-Z][A-Za-z]+)\b", module))
    for action in sorted(spec_actions()):
        if action in unobserved:
            continue
        if not re.search(rf"\b{action}\b", module):
            problems.append(f"action in ExactlyOnce.tla Next that no event reaches and is not unobserved: {action}")

    if problems:
        print("check-protocol-trace-events: FAILED", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print(f"check-protocol-trace-events: {len(events)} events, {len(unobserved)} unobserved actions, "
          f"{len(spec_actions())} spec actions agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
