"""pyclink - embed the clink engine in Python.

Pure Python over libclink's C ABI (ctypes, no compiled extension): open an
engine (the whole clink runtime in this process, no daemons), run SQL, and
read a connector='collect' table's rows as Arrow record batches, zero-copy
through the Arrow C stream interface.

    import pyclink

    with pyclink.Engine() as e:
        e.execute('''
            CREATE TABLE orders (user_id BIGINT, amount BIGINT)
              WITH (connector='file', format='json', path='/tmp/orders.ndjson');
            CREATE TABLE results (user_id BIGINT, amount BIGINT)
              WITH (connector='collect');
            INSERT INTO results SELECT user_id, amount FROM orders
        ''')
        table = e.collect("results").read_all()   # pyarrow.Table
        e.await_all()

The library is located via the `lib_path` argument, the CLINK_LIB
environment variable, or the system loader path, in that order. Reads on a
collect stream block inside C and therefore do not respond to Ctrl-C until
a batch arrives; `await_all` polls in slices so it stays interruptible and
cancels the running jobs on KeyboardInterrupt.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import time

# pyarrow is a hard dependency (collect streams import into
# pyarrow.RecordBatchReader). Load order against libclink no longer
# matters: libclink is self-contained (its Arrow is statically linked
# with only the clink_* C API exported, and its private Arrow uses the
# system allocator), so either library can load first.
import pyarrow as _pa  # noqa: F401  (used in collect)

__all__ = ["Engine", "ClinkError"]

_ABI_VERSION = 1


class ClinkError(RuntimeError):
    """An error reported by libclink."""


class _EngineOptions(ctypes.Structure):
    _fields_ = [
        ("parallelism", ctypes.c_uint32),
        ("state_backend_uri", ctypes.c_char_p),
        ("checkpoint_dir", ctypes.c_char_p),
        ("checkpoint_interval_ms", ctypes.c_int64),
        ("catalog_dir", ctypes.c_char_p),
    ]


class _ArrowArrayStream(ctypes.Structure):
    # Callback signatures never invoked from Python: opaque pointers suffice.
    _fields_ = [
        ("get_schema", ctypes.c_void_p),
        ("get_next", ctypes.c_void_p),
        ("get_last_error", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


def _load_library(lib_path: str | None) -> ctypes.CDLL:
    candidates = []
    if lib_path:
        candidates.append(lib_path)
    env = os.environ.get("CLINK_LIB")
    if env:
        candidates.append(env)
    found = ctypes.util.find_library("clink")
    if found:
        candidates.append(found)
    errors = []
    for c in candidates:
        try:
            return ctypes.CDLL(c)
        except OSError as e:  # noqa: PERF203 - tiny loop, per-candidate diagnostics
            errors.append(f"{c}: {e}")
    raise ClinkError(
        "libclink not found. Pass Engine(lib_path=...), set CLINK_LIB, or put "
        "libclink on the loader path. Tried: " + ("; ".join(errors) or "(no candidates)")
    )


def _bind(lib: ctypes.CDLL) -> ctypes.CDLL:
    h = ctypes.c_void_p
    lib.clink_abi_version.restype = ctypes.c_int32
    lib.clink_abi_version.argtypes = []
    lib.clink_engine_open.restype = h
    lib.clink_engine_open.argtypes = [ctypes.POINTER(_EngineOptions)]
    lib.clink_open_error.restype = ctypes.c_char_p
    lib.clink_open_error.argtypes = []
    lib.clink_engine_close.restype = None
    lib.clink_engine_close.argtypes = [h]
    lib.clink_exec.restype = ctypes.c_int
    lib.clink_exec.argtypes = [h, ctypes.c_char_p]
    lib.clink_last_error.restype = ctypes.c_char_p
    lib.clink_last_error.argtypes = [h]
    lib.clink_job_count.restype = ctypes.c_size_t
    lib.clink_job_count.argtypes = [h]
    lib.clink_job_id_at.restype = ctypes.c_uint64
    lib.clink_job_id_at.argtypes = [h, ctypes.c_size_t]
    lib.clink_job_wait.restype = ctypes.c_int
    lib.clink_job_wait.argtypes = [h, ctypes.c_uint64, ctypes.c_int64]
    lib.clink_job_cancel.restype = ctypes.c_int
    lib.clink_job_cancel.argtypes = [h, ctypes.c_uint64]
    lib.clink_await_all.restype = ctypes.c_int
    lib.clink_await_all.argtypes = [h, ctypes.c_int64]
    lib.clink_cancel_all.restype = None
    lib.clink_cancel_all.argtypes = [h]
    lib.clink_collect_stream.restype = ctypes.c_int
    lib.clink_collect_stream.argtypes = [h, ctypes.c_char_p, ctypes.POINTER(_ArrowArrayStream)]
    return lib


def _enc(s: str | None) -> bytes | None:
    return None if s is None else s.encode()


class Engine:
    """One embedded clink engine (in-process JobManager + TaskManager)."""

    def __init__(
        self,
        *,
        parallelism: int = 1,
        state_backend_uri: str | None = None,
        checkpoint_dir: str | None = None,
        checkpoint_interval_ms: int = 0,
        catalog_dir: str | None = None,
        lib_path: str | None = None,
    ):
        self._lib = _bind(_load_library(lib_path))
        abi = self._lib.clink_abi_version()
        if abi != _ABI_VERSION:
            raise ClinkError(f"libclink speaks ABI v{abi}; this pyclink expects v{_ABI_VERSION}")
        opts = _EngineOptions(
            parallelism,
            _enc(state_backend_uri),
            _enc(checkpoint_dir),
            checkpoint_interval_ms,
            _enc(catalog_dir),
        )
        self._h = self._lib.clink_engine_open(ctypes.byref(opts))
        if not self._h:
            raise ClinkError(self._lib.clink_open_error().decode() or "engine open failed")

    # -- lifecycle -----------------------------------------------------

    def close(self) -> None:
        """Cancel jobs, wake collect readers (they see a cancelled status),
        tear the engine down. Idempotent."""
        if self._h:
            self._lib.clink_engine_close(self._h)
            self._h = None

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):  # noqa: D105 - best-effort safety net
        try:
            self.close()
        except Exception:
            pass

    # -- execution -----------------------------------------------------

    def _check_open(self) -> None:
        if not self._h:
            raise ClinkError("engine is closed")

    def _last_error(self) -> str:
        return self._lib.clink_last_error(self._h).decode()

    def execute(self, sql: str) -> None:
        """Run a SQL script: DDL folds into the engine catalog, each INSERT /
        materialized-view statement starts a job immediately."""
        self._check_open()
        if self._lib.clink_exec(self._h, sql.encode()) != 0:
            raise ClinkError(self._last_error() or "execute failed")

    @property
    def job_count(self) -> int:
        self._check_open()
        return self._lib.clink_job_count(self._h)

    def await_all(self, timeout_ms: int = -1) -> None:
        """Block until every submitted job reaches a terminal state.

        Polls in slices so Ctrl-C works: on KeyboardInterrupt the running
        jobs are cancelled, given a short drain, and the interrupt is
        re-raised. Raises TimeoutError on timeout and ClinkError when any
        job reported errors."""
        self._check_open()
        deadline = None if timeout_ms < 0 else time.monotonic() + timeout_ms / 1000.0
        try:
            while True:
                slice_ms = 200
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("jobs still running after timeout")
                    slice_ms = min(slice_ms, max(1, int(remaining * 1000)))
                rc = self._lib.clink_await_all(self._h, slice_ms)
                if rc == 0:
                    return
                if rc == -1:
                    raise ClinkError(self._last_error() or "job(s) failed")
                # rc == 1: still running - loop (and give Python a chance to
                # deliver KeyboardInterrupt between slices).
        except KeyboardInterrupt:
            self.cancel_all()
            self._lib.clink_await_all(self._h, 5000)
            raise

    def cancel_all(self) -> None:
        self._check_open()
        self._lib.clink_cancel_all(self._h)

    # -- results -------------------------------------------------------

    def collect(self, table: str):
        """The typed Arrow stream of a connector='collect' table, as a
        pyarrow.RecordBatchReader (zero-copy across the C ABI). One reader
        per table. Reads block until data; the stream ends when the
        producing job finishes; it errors if the engine is closed first."""
        self._check_open()
        stream = _ArrowArrayStream()
        if self._lib.clink_collect_stream(self._h, table.encode(), ctypes.byref(stream)) != 0:
            raise ClinkError(self._last_error() or f"collect stream for '{table}' failed")
        # pyarrow takes ownership of the C stream (it is moved out of
        # `stream`); the reader stays valid independently of this Engine.
        return _pa.RecordBatchReader._import_from_c(ctypes.addressof(stream))
