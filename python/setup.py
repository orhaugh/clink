"""Build backend for pyclink wheels.

pyclink is pure Python (ctypes) over libclink's C ABI - there is no compiled
Python extension. A wheel therefore ships ONE artifact: the platform libclink
shared library, placed next to the package so pyclink's loader finds it with no
CLINK_LIB or separate build.

The library is heavy (a self-contained libclink statically links Arrow/Parquet),
so it is built ONCE, not per Python version:

  * If CLINK_LIB points at an existing libclink, it is copied verbatim. CI builds
    the library once (cibuildwheel CIBW_BEFORE_ALL) and sets CLINK_LIB, so each
    per-Python wheel build just copies it.
  * Otherwise cmake builds the `clink_shared` target from the repo that contains
    this package (the parent of python/), lean: SQL on, connector impls off, so
    the only external deps are Arrow's (curl / xml2 / aws-sdk / openssl / ...),
    which the wheel repair step (delocate / auditwheel) bundles.

The wheel is tagged py3-none-<platform>: it carries a native library but no
CPython ABI, so a single wheel serves every Python 3 on that platform.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.dist import Distribution

try:  # setuptools >= 70 vendors bdist_wheel; older needs the wheel package
    from setuptools.command.bdist_wheel import bdist_wheel
except ImportError:  # pragma: no cover
    from wheel.bdist_wheel import bdist_wheel

HERE = Path(__file__).resolve().parent
REPO = HERE.parent


def _lib_filename() -> str:
    if sys.platform == "darwin":
        return "libclink.dylib"
    if sys.platform == "win32":
        return "clink.dll"
    return "libclink.so"


def _find_built_lib(build_dir: Path) -> Path:
    name = _lib_filename()
    for p in build_dir.rglob(name):
        return p
    raise FileNotFoundError(f"{name} not found under {build_dir} after the cmake build")


def _build_libclink() -> Path:
    """Return a path to a libclink to bundle: a prebuilt one via CLINK_LIB, or a
    fresh lean cmake build of clink_shared from the enclosing repo."""
    env_lib = os.environ.get("CLINK_LIB")
    if env_lib and Path(env_lib).exists():
        return Path(env_lib)

    build_dir = Path(os.environ.get("CLINK_WHEEL_BUILD_DIR", REPO / "build-pyclink"))
    jobs = str(min(10, (os.cpu_count() or 4)))
    subprocess.check_call(
        [
            "cmake",
            "-S",
            str(REPO),
            "-B",
            str(build_dir),
            "-DCMAKE_BUILD_TYPE=Release",
            "-DCLINK_BUILD_SQL=ON",
            "-DCLINK_BUILD_IMPLS=OFF",
            "-DCLINK_BUILD_TESTS=OFF",
            "-DCLINK_BUILD_EXAMPLES=OFF",
        ]
    )
    subprocess.check_call(
        ["cmake", "--build", str(build_dir), "--target", "clink_shared", "--parallel", jobs]
    )
    return _find_built_lib(build_dir)


class build_py_with_lib(build_py):
    """Copy the platform libclink into the built package (not the source tree)."""

    def run(self) -> None:
        super().run()
        lib = _build_libclink()
        dst_dir = Path(self.build_lib) / "pyclink"
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / _lib_filename()
        shutil.copy2(lib, dst)
        self.announce(f"pyclink: bundled {lib} -> {dst}", level=2)


class BinaryDistribution(Distribution):
    """Force a platform (platlib) wheel with the package at the wheel root.

    pyclink has a native library but no ext module; declaring has_ext_modules
    makes setuptools build a platlib wheel (pyclink/ at the root), NOT route the
    package through pyclink-*.data/purelib/. That placement matters: delocate /
    auditwheel compute @loader_path/@rpath depth from the wheel layout, and the
    extra .data/purelib/ levels would make the repaired dep paths wrong once the
    install flattens them into site-packages.
    """

    def has_ext_modules(self) -> bool:  # noqa: D102
        return True


class platform_wheel(bdist_wheel):
    """A native library but no CPython ABI: tag py3-none-<platform> so one wheel
    serves every Python 3 on the platform."""

    def get_tag(self):
        _python, _abi, plat = super().get_tag()
        # libclink is single-arch (the pinned deps are not universal), but a
        # universal2 interpreter tags the wheel "universal2". Replace that with
        # the real machine arch so the tag matches the binary and delocate /
        # auditwheel can repair and retag it. (cibuildwheel sets the arch and
        # MACOSX_DEPLOYMENT_TARGET per build, so this only bites host builds.)
        if plat.endswith("universal2"):
            import platform

            plat = plat[: -len("universal2")] + platform.machine()
        return "py3", "none", plat


setup(
    distclass=BinaryDistribution,
    cmdclass={"build_py": build_py_with_lib, "bdist_wheel": platform_wheel},
    package_data={"pyclink": ["libclink.dylib", "libclink.so", "clink.dll"]},
)
