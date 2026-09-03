#!/usr/bin/env python3
"""Check the Stable C ABI symbol manifest (design record 011).

scripts/libclink-abi-symbols.txt names every function libclink exports and
the CLINK_EMBED_ABI_VERSION they belong to. Two checks, so the header, the
manifest and the built artefact can never quietly disagree:

  1. Header vs manifest (no build needed; CI and the pre-commit hook): the
     CLINK_EMBED_API declarations in include/clink/embed/clink.h must equal
     the manifest's [symbols], and the header's CLINK_EMBED_ABI_VERSION must
     equal its [abi-version].

  2. Library vs manifest (--library <path>, run as a test after the build):
     the clink_* symbols the shared library's dynamic table exports must
     equal the manifest's [symbols]. Catches a declared-but-undefined
     function and an extern "C" clink_* symbol that leaked without a
     declaration.

The manifest is hand-maintained and append-only within 1.x: a removed line
is a break of the Stable tier and is reviewed as one. --write regenerates
the [symbols] section from the header for convenience when adding.

Usage:
    scripts/check-c-abi-symbols.py                  header vs manifest
    scripts/check-c-abi-symbols.py --library PATH   plus library vs manifest
    scripts/check-c-abi-symbols.py --write          rewrite [symbols] from the header
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
HEADER_PATH = REPO_ROOT / "include" / "clink" / "embed" / "clink.h"
MANIFEST_PATH = REPO_ROOT / "scripts" / "libclink-abi-symbols.txt"

# `CLINK_EMBED_API <return type> <name>(` - declarations may wrap onto the
# next line after the name, so only the opening parenthesis is required.
DECL_RE = re.compile(r"^\s*CLINK_EMBED_API\s+[^;(]*?\b(clink_[A-Za-z0-9_]+)\s*\(", re.MULTILINE)
VERSION_RE = re.compile(r"^\s*#\s*define\s+CLINK_EMBED_ABI_VERSION\s+(\d+)", re.MULTILINE)


def header_symbols() -> tuple[set[str], int]:
    text = HEADER_PATH.read_text(encoding="utf-8")
    names = set(DECL_RE.findall(text))
    m = VERSION_RE.search(text)
    if m is None:
        sys.exit("check-c-abi-symbols: CLINK_EMBED_ABI_VERSION not found in clink.h")
    return names, int(m.group(1))


def parse_manifest() -> tuple[set[str], int, list[str]]:
    section = ""
    symbols: set[str] = set()
    version: int | None = None
    lines = MANIFEST_PATH.read_text(encoding="utf-8").splitlines()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1]
            continue
        if section == "abi-version":
            version = int(stripped)
        elif section == "symbols":
            symbols.add(stripped)
    if version is None:
        sys.exit("check-c-abi-symbols: manifest has no [abi-version]")
    return symbols, version, lines


def library_symbols(path: Path) -> set[str]:
    if sys.platform == "darwin":
        cmd = ["nm", "-gU", str(path)]
    else:
        cmd = ["nm", "-D", "--defined-only", str(path)]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    found: set[str] = set()
    for line in out.splitlines():
        parts = line.split()
        if not parts:
            continue
        name = parts[-1]
        if sys.platform == "darwin" and name.startswith("_"):
            name = name[1:]
        if name.startswith("clink_"):
            found.add(name)
    return found


def report(label: str, expected: set[str], actual: set[str]) -> bool:
    ok = True
    for missing in sorted(expected - actual):
        print(f"check-c-abi-symbols: {label}: missing {missing}", file=sys.stderr)
        ok = False
    for extra in sorted(actual - expected):
        print(f"check-c-abi-symbols: {label}: undeclared {extra}", file=sys.stderr)
        ok = False
    return ok


def write_manifest(lines: list[str], names: set[str]) -> None:
    out: list[str] = []
    in_symbols = False
    for line in lines:
        stripped = line.strip()
        if stripped == "[symbols]":
            in_symbols = True
            out.append(line)
            out.extend(sorted(names))
            continue
        if in_symbols:
            if stripped.startswith("[") and stripped.endswith("]"):
                in_symbols = False
                out.append("")
                out.append(line)
            continue
        out.append(line)
    MANIFEST_PATH.write_text("\n".join(out).rstrip("\n") + "\n", encoding="utf-8")


def main() -> int:
    args = sys.argv[1:]
    library: Path | None = None
    if "--library" in args:
        idx = args.index("--library")
        if idx + 1 >= len(args):
            sys.exit("check-c-abi-symbols: --library needs a path")
        library = Path(args[idx + 1])

    declared, header_version = header_symbols()
    manifest, manifest_version, lines = parse_manifest()

    if "--write" in args:
        write_manifest(lines, declared)
        print(f"check-c-abi-symbols: wrote {len(declared)} symbols to {MANIFEST_PATH.relative_to(REPO_ROOT)}")
        return 0

    ok = report("header vs manifest", manifest, declared)
    if header_version != manifest_version:
        print(
            f"check-c-abi-symbols: CLINK_EMBED_ABI_VERSION is {header_version} in clink.h but "
            f"{manifest_version} in the manifest",
            file=sys.stderr,
        )
        ok = False

    if library is not None:
        if not library.is_file():
            print(f"check-c-abi-symbols: library not found: {library}", file=sys.stderr)
            return 1
        ok = report("library vs manifest", manifest, library_symbols(library)) and ok

    if not ok:
        print(
            "check-c-abi-symbols: scripts/libclink-abi-symbols.txt, clink.h and the built library "
            "must agree. Adding a function: append it to the manifest. Removing one is a break of "
            "the Stable C ABI and lands only at a major release (design record 011).",
            file=sys.stderr,
        )
        return 1
    scope = "header" if library is None else "header and library"
    print(f"check-c-abi-symbols: {len(manifest)} symbols, ABI v{manifest_version}, {scope} agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
