#!/usr/bin/env python3
"""Fail unless a repaired Linux pyclink wheel is self-contained.

auditwheel's own verdict is not enough: it passed a wheel whose libclink loaded
libarrow.so beside its static Arrow, because it vendored the shared Arrow into
pyclink.libs/ and whitelisted the libatomic.so.1 those libraries needed, which
slim images do not ship. A self-contained libclink links everything but the C and
C++ runtimes statically, so this gate asserts exactly that:

  * the wheel vendors nothing (no <pkg>.libs/ directory), and
  * every shared object in it needs only glibc, libstdc++ and libgcc_s.

    scripts/check-wheel-self-contained.py wheelhouse/pyclink-*.whl

Exit 0 when the wheel passes, 1 with the offending entries named otherwise.
"""

import re
import subprocess
import sys
import tempfile
import zipfile
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


def needed(path: Path) -> list[str]:
    out = subprocess.run(
        ["readelf", "-d", str(path)], check=True, capture_output=True, text=True
    ).stdout
    return re.findall(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", out)


def check(wheel: Path) -> list[str]:
    problems = []
    with zipfile.ZipFile(wheel) as zf:
        names = zf.namelist()
        vendored = sorted(n for n in names if re.search(r"\.libs/[^/]+$", n))
        problems += [f"vendored library: {n}" for n in vendored]
        objects = [n for n in names if re.search(r"\.so(\.\d+)*$", n)]
        if not objects:
            problems.append("no shared object in the wheel")
        with tempfile.TemporaryDirectory() as tmp:
            for name in objects:
                zf.extract(name, tmp)
                for dep in needed(Path(tmp) / name):
                    if dep not in ALLOWED and not LOADER.match(dep):
                        problems.append(f"{name} needs {dep}")
    return problems


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__.strip().splitlines()[0], file=sys.stderr)
        print("usage: check-wheel-self-contained.py WHEEL...", file=sys.stderr)
        return 2
    failed = False
    for arg in sys.argv[1:]:
        wheel = Path(arg)
        problems = check(wheel)
        if problems:
            failed = True
            print(f"check-wheel-self-contained: {wheel.name} is NOT self-contained:")
            for p in problems:
                print(f"  {p}")
        else:
            print(f"check-wheel-self-contained: {wheel.name} needs only the C/C++ runtime")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
