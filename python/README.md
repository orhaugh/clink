# pyclink

Embed the clink stream engine in Python. Pure Python over libclink's C ABI
(ctypes, no compiled extension): the whole runtime starts inside your
process - no daemons, no cluster - and results stream back as Arrow record
batches, zero-copy, straight into pyarrow (and from there pandas or polars).

## Install

A prebuilt wheel bundles a self-contained libclink (statically linked
Arrow/Parquet) next to the package, so nothing else is needed - no separate
build, no `CLINK_LIB`:

```bash
pip install pyclink-<version>-py3-none-macosx_14_0_arm64.whl
```

pyclink is not yet published to PyPI. Wheels are produced by
`.github/workflows/wheels.yml` and attached to CI runs as artifacts: macOS
arm64 wheels are built and smoke-tested on every release tag. Linux wheels
are opt-in and currently carry a high glibc floor (built on the project's
toolchain image), so they are not broadly installable yet; on Linux, and on
any platform without a wheel, use the build-from-source path below. There is
no Windows wheel.

## Build from source

The supported path for local development, editable installs, and platforms
without a wheel. Build libclink from the repo root (needs the SQL frontend):

```bash
cmake -S . -B build -DCLINK_BUILD_SQL=ON
cmake --build build --target clink_shared --parallel 10
```

Install pyclink and point it at the library:

```bash
pip install ./python           # or: pip install -e ./python
export CLINK_LIB=$PWD/build/libclink.dylib    # .so on Linux
```

Run streaming SQL and read Arrow:

```python
import pyclink

with pyclink.Engine() as e:
    e.execute("""
        CREATE TABLE orders (user_id BIGINT, amount BIGINT)
          WITH (connector='file', format='json', path='/tmp/orders.ndjson');
        CREATE TABLE results (user_id BIGINT, amount BIGINT)
          WITH (connector='collect');
        INSERT INTO results SELECT user_id, amount FROM orders
    """)
    table = e.collect("results").read_all()   # pyarrow.Table
    e.await_all()

print(table.to_pandas())
```

Any connector compiled into libclink works as a source or sink (Kafka,
Postgres CDC, Iceberg, S3, ...). `connector='collect'` is the results
surface: one reader per collect table, reads block until data arrives, the
stream ends when the producing job finishes, and closing the engine wakes a
blocked reader with an error. Collect is append-only in v1 - a retracting
(changelog) query is rejected at bind time.

`Engine(...)` accepts `parallelism`, `state_backend_uri`, `checkpoint_dir`,
`checkpoint_interval_ms`, `catalog_dir`, and `lib_path` (which beats the
`CLINK_LIB` environment variable). `await_all()` polls in slices, so Ctrl-C
cancels the running jobs and drains before re-raising.

See `docs/internals/embedded.md` in the repo for the semantics underneath and
for how the wheel bundles libclink.

## Tests

```bash
CLINK_LIB=$PWD/build/libclink.dylib python3 python/tests/test_pyclink.py
```

(Also runnable under pytest.)
