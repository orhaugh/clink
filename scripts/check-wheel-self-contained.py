#!/usr/bin/env python3
"""Fail unless a repaired Linux pyclink wheel is self-contained.

auditwheel's own verdict is not enough: it passed a wheel whose libclink loaded
libarrow.so beside its static Arrow, because it vendored the shared Arrow into
pyclink.libs/ and whitelisted the libatomic.so.1 those libraries needed, which
slim images do not ship. A self-contained libclink links everything but the C and
C++ runtimes statically, so this gate asserts exactly that:

  * the wheel vendors nothing (no <pkg>.libs/ directory);
  * every shared object in it needs only glibc, libstdc++ and libgcc_s;
  * every dynamic symbol a shared object defines starts with clink_, so the C ABI
    is the whole export table and the static Arrow, OpenSSL and connector clients
    cannot interpose on, or be interposed by, a host's own copies;
  * the wheel carries at most one OpenSSL version string, so Kafka and ClickHouse
    share one OpenSSL rather than linking two.

With --link-map (the GNU ld map scripts/build-libclink-wheel.sh writes beside
libclink.so), it also asserts that every LZ4_ and LZ4F_ definition came from one
archive, and that the archive is not a ClickHouse client's (a path with a
clickhouse-cpp directory in it). Every lz4 reference, the client's and Arrow's
codecs alike, then resolves against one lz4 version, so frame code never runs
against another version's block functions. The archives that did supply lz4
definitions are printed.

    scripts/check-wheel-self-contained.py [--link-map libclink.so.map] wheelhouse/pyclink-*.whl

Exit 0 when the wheel passes, 1 with the offending entries named otherwise.
"""

import argparse
import re
import subprocess
import sys
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path

# The runtime every manylinux system provides and a self-contained libclink may
# need. libatomic and libz are deliberately absent: both are on auditwheel's
# whitelist, and libatomic is missing from slim images.
ALLOWED = {
    "libc.so.6",
    "libm.so.6",
    "libdl.so.2",
    "libpthread.so.0",
    "librt.so.1",
    "libstdc++.so.6",
    "libgcc_s.so.1",
}
LOADER = re.compile(r"^ld-linux-[a-z0-9_-]+\.so\.\d+$")
EXPORT_PREFIX = "clink_"
OPENSSL_VERSION = re.compile(rb"OpenSSL (\d+\.\d+\.\d+[a-z]?)")
LZ4_SYMBOL = re.compile(r"^LZ4F?_")
# Names in the export list printed per object, at most.
SHOW = 10


def needed(path: Path) -> list[str]:
    out = subprocess.run(
        ["readelf", "-d", str(path)], check=True, capture_output=True, text=True
    ).stdout
    return re.findall(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", out)


def defined_dynamic_symbols(path: Path) -> list[str]:
    """Every symbol the object's dynamic table defines and makes visible."""
    out = subprocess.run(
        ["readelf", "--dyn-syms", "-W", str(path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    names = []
    for line in out.splitlines():
        # Num: Value Size Type Bind Vis Ndx Name
        fields = line.split()
        if len(fields) < 8 or not fields[0].rstrip(":").isdigit():
            continue
        sym_type, bind, ndx, name = fields[3], fields[4], fields[6], fields[7]
        if ndx == "UND" or bind == "LOCAL" or sym_type in ("SECTION", "FILE"):
            continue
        names.append(name.split("@", 1)[0])
    return names


def openssl_versions(path: Path) -> set[str]:
    return {m.decode() for m in OPENSSL_VERSION.findall(path.read_bytes())}


def check(wheel: Path) -> list[str]:
    problems = []
    with zipfile.ZipFile(wheel) as zf:
        names = zf.namelist()
        vendored = sorted(n for n in names if re.search(r"\.libs/[^/]+$", n))
        problems += [f"vendored library: {n}" for n in vendored]
        objects = [n for n in names if re.search(r"\.so(\.\d+)*$", n)]
        if not objects:
            problems.append("no shared object in the wheel")
        versions: dict[str, set[str]] = defaultdict(set)
        with tempfile.TemporaryDirectory() as tmp:
            for name in objects:
                zf.extract(name, tmp)
                path = Path(tmp) / name
                for dep in needed(path):
                    if dep not in ALLOWED and not LOADER.match(dep):
                        problems.append(f"{name} needs {dep}")
                exported = defined_dynamic_symbols(path)
                foreign = sorted(s for s in exported if not s.startswith(EXPORT_PREFIX))
                if foreign:
                    shown = ", ".join(foreign[:SHOW])
                    more = f" and {len(foreign) - SHOW} more" if len(foreign) > SHOW else ""
                    problems.append(
                        f"{name} exports {len(foreign)} symbol(s) outside {EXPORT_PREFIX}*: "
                        f"{shown}{more}"
                    )
                elif not exported:
                    problems.append(f"{name} exports no {EXPORT_PREFIX}* symbol")
                else:
                    print(f"  {name}: {len(exported)} dynamic symbol(s), all {EXPORT_PREFIX}*")
                for v in openssl_versions(path):
                    versions[v].add(name)
        if len(versions) > 1:
            listed = "; ".join(
                f"OpenSSL {v} in {', '.join(sorted(objs))}" for v, objs in sorted(versions.items())
            )
            problems.append(f"more than one OpenSSL version string: {listed}")
        elif versions:
            print(f"  one OpenSSL version string: OpenSSL {next(iter(versions))}")
        else:
            print("  no OpenSSL version string")
    return problems


def lz4_definitions(link_map: Path) -> dict[str, list[str]]:
    """Input file -> the LZ4_ / LZ4F_ symbols the GNU ld map says it defines.

    Read from two sections of the map: "Archive member included to satisfy
    reference by file (symbol)", which names the member pulled in for each
    symbol, and the memory map, which lists each global symbol under the input
    section that defines it.
    """
    defs: dict[str, set[str]] = defaultdict(set)
    lines = link_map.read_text(errors="replace").splitlines()
    section = None
    member = None
    current = None
    input_line = re.compile(r"^\s*(?:\S+\s+)?0x[0-9a-fA-F]+\s+0x[0-9a-fA-F]+\s+(\S.*?)\s*$")
    symbol_line = re.compile(r"^\s+0x[0-9a-fA-F]+\s+([A-Za-z_][\w.$]*)\s*$")
    for line in lines:
        if line.startswith("Archive member included"):
            section = "archive"
            continue
        if line.startswith("Linker script and memory map"):
            section = "memory"
            continue
        if line and not line[0].isspace() and section == "archive":
            # "archive(member)" on its own line, or with the referencing file and
            # symbol on the same line.
            m = re.match(r"^(\S+\([^)]*\))(?:\s+\S+\s+\(([^)]*)\))?\s*$", line)
            if m:
                member = m.group(1)
                if m.group(2) and LZ4_SYMBOL.match(m.group(2)):
                    defs[member].add(m.group(2))
                continue
            if not line.startswith(("Discarded", "Allocating", "Memory")):
                continue
            section = None
        if section == "archive" and member:
            m = re.match(r"^\s+\S+\s+\(([^)]*)\)\s*$", line)
            if m and LZ4_SYMBOL.match(m.group(1)):
                defs[member].add(m.group(1))
        elif section == "memory":
            m = symbol_line.match(line)
            if m:
                if current and LZ4_SYMBOL.match(m.group(1)):
                    defs[current].add(m.group(1))
                continue
            m = input_line.match(line)
            if m:
                current = m.group(1)
    return {k: sorted(v) for k, v in defs.items()}


def from_clickhouse_client(input_file: str) -> bool:
    archive = input_file.split("(", 1)[0]
    return "clickhouse-cpp" in Path(archive).parts


def check_link_map(link_map: Path) -> list[str]:
    if not link_map.is_file():
        return [f"link map {link_map} does not exist"]
    defs = lz4_definitions(link_map)
    if not defs:
        return [
            f"{link_map} names no LZ4_ or LZ4F_ definition: libclink links lz4 through "
            "Arrow, so the map was not read"
        ]
    by_archive: dict[str, set[str]] = defaultdict(set)
    for input_file, symbols in defs.items():
        by_archive[input_file.split("(", 1)[0]].update(symbols)
    for archive, symbols in sorted(by_archive.items()):
        print(f"  lz4: {len(symbols)} LZ4_/LZ4F_ definition(s) from {archive}")
    problems = []
    if len(by_archive) > 1:
        problems.append(
            f"lz4 from {len(by_archive)} archives, not one: {', '.join(sorted(by_archive))}"
        )
    for input_file, symbols in sorted(defs.items()):
        if from_clickhouse_client(input_file):
            shown = ", ".join(symbols[:SHOW])
            problems.append(f"lz4 from the ClickHouse client: {input_file} defines {shown}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.strip().splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--link-map", type=Path, help="GNU ld map of libclink.so")
    parser.add_argument("wheels", nargs="+", type=Path, metavar="WHEEL")
    args = parser.parse_args()
    failed = False
    map_problems = check_link_map(args.link_map) if args.link_map else []
    for wheel in args.wheels:
        problems = check(wheel) + map_problems
        if problems:
            failed = True
            print(f"check-wheel-self-contained: {wheel.name} is NOT self-contained:")
            for p in problems:
                print(f"  {p}")
        else:
            print(
                f"check-wheel-self-contained: {wheel.name} needs only the C/C++ runtime"
                f" and exports only {EXPORT_PREFIX}*"
            )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
