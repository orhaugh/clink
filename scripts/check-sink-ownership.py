#!/usr/bin/env python3
"""Every shipped sink says whether it must own its chain's checkpoint.

A Dag's checkpoint is acknowledged by its owner: the sink when it is the only
one in the Dag, the operator in front of the sinks when there are two or more
(Dag::add_sink). A sink whose guarantee needs the acknowledgement to follow its
own barrier hook - an at-least-once writer that writes out at the barrier what
it holds (Sink::gates_checkpoint_ack), or a two-phase-commit sink that prepares
there (Sink::stages_state_at_barrier) - must declare it, and Dag::add_sink then
refuses another sink beside it. A sink that forgets loses that refusal
silently, so this check makes the decision explicit for every sink class:

  * every class in shipped code deriving from Sink<...> or CommittingSink<...>
    is listed in scripts/sink-ownership.txt, as `owns` or as `free <reason>`;
  * an `owns` class declares gates_checkpoint_ack() or stages_state_at_barrier()
    (or derives from CommittingSink, which declares the latter);
  * a `free` class declares neither;
  * every listed class still exists.

Exit 0 when the manifest and the code agree, 1 otherwise.
"""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "scripts" / "sink-ownership.txt"

# A class or struct definition with a base clause; the clause is inspected for
# a sink base below. Forward declarations end in ';' and do not match.
CLASS = re.compile(r"\b(?:class|struct)\s+(\w+)(?:\s+final)?\s*:([^{;]*)\{", re.S)
# A direct sink base: Sink<...> or CommittingSink<...>, possibly qualified.
SINK_BASE = re.compile(r"(?<![\w:])(?:\w+::)*(Sink|CommittingSink)\s*<")
DECLARES = re.compile(r"\b(gates_checkpoint_ack|stages_state_at_barrier)\s*\(\s*\)\s*const")


def shipped_sources():
    out = subprocess.run(
        ["git", "ls-files", "include", "src", "impls"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    for rel in out:
        if not rel.endswith((".hpp", ".h", ".cpp")):
            continue
        parts = pathlib.PurePosixPath(rel).parts
        if "tests" in parts or "plugin_examples" in parts:
            continue
        yield rel


def class_body(text, start):
    """The text from the class's opening brace to its matching closing brace."""
    brace = text.find("{", start)
    if brace < 0:
        return ""
    depth = 0
    for i in range(brace, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[brace : i + 1]
    return text[brace:]


def found_classes():
    """Every sink class in shipped code, keyed '<path>:<Class>'.

    A class is a sink when its base clause names Sink<...> or CommittingSink<...>,
    or names another sink class (so a sink derived from a shipped sink is listed
    too, and inherits what its base declares).
    """
    defs = []
    for rel in shipped_sources():
        text = (ROOT / rel).read_text(encoding="utf-8", errors="replace")
        for m in CLASS.finditer(text):
            body = class_body(text, m.end() - 1)
            defs.append((rel, m.group(1), m.group(2), bool(DECLARES.search(body))))
    classes = {}
    by_name = {}
    changed = True
    while changed:
        changed = False
        for rel, name, bases, declares in defs:
            key = f"{rel}:{name}"
            if key in classes:
                continue
            direct = SINK_BASE.findall(bases)
            parents = [
                by_name[b]
                for b in re.findall(r"(?<![\w:])(?:\w+::)*(\w+)\s*(?:<|,|$)", bases.strip())
                if b in by_name
            ]
            if not direct and not parents:
                continue
            classes[key] = {
                "committing": "CommittingSink" in direct or any(p["committing"] for p in parents),
                "declares": declares or any(p["declares"] for p in parents),
            }
            by_name[name] = classes[key]
            changed = True
    return classes


def read_manifest():
    entries = {}
    problems = []
    for n, raw in enumerate(MANIFEST.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split(None, 2)
        if len(fields) < 2 or fields[1] not in ("owns", "free"):
            problems.append(f"{MANIFEST.name}:{n}: expected '<path>:<Class> owns|free [reason]'")
            continue
        if fields[1] == "free" and len(fields) < 3:
            problems.append(f"{MANIFEST.name}:{n}: a free sink needs the reason it owns nothing")
            continue
        entries[fields[0]] = fields[1]
    return entries, problems


def main():
    classes = found_classes()
    entries, problems = read_manifest()
    for key, info in sorted(classes.items()):
        kind = entries.get(key)
        if kind is None:
            problems.append(
                f"{key}: a sink that is not classified. If its guarantee needs the checkpoint's "
                "acknowledgement to follow its barrier hook, declare gates_checkpoint_ack() (or "
                "stages_state_at_barrier() for a two-phase-commit sink) and list it as owns; "
                "otherwise list it as free with the reason it needs no ownership "
                f"({MANIFEST.relative_to(ROOT)})"
            )
            continue
        owns = info["committing"] or info["declares"]
        if kind == "owns" and not owns:
            problems.append(
                f"{key}: listed as owns, but declares neither gates_checkpoint_ack() nor "
                "stages_state_at_barrier()"
            )
        if kind == "free" and info["declares"]:
            problems.append(
                f"{key}: listed as free, but declares gates_checkpoint_ack() or "
                "stages_state_at_barrier(); list it as owns"
            )
        if kind == "free" and info["committing"]:
            problems.append(f"{key}: listed as free, but is a CommittingSink; list it as owns")
    for key in sorted(set(entries) - set(classes)):
        problems.append(f"{key}: listed, but no such sink class is in shipped code any more")

    owns = sum(1 for k in classes if entries.get(k) == "owns")
    print(
        f"check-sink-ownership: {len(classes)} sink class(es), {owns} owning their chain's "
        f"checkpoint, {len(classes) - owns} free"
    )
    for p in problems:
        print(f"  {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
