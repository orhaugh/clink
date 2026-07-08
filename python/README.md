# pyclink

Embed the clink stream engine in Python. Pure Python over libclink's C ABI
(ctypes, no compiled extension): the whole runtime starts inside your
process - no daemons, no cluster - and results stream back as Arrow record
batches, zero-copy, straight into pyarrow (and from there pandas or polars).

## Quickstart

Build libclink from the repo root (needs the SQL frontend):

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

Wheels that bundle the library are not built yet; the supported path is
build-from-source plus `CLINK_LIB`. See `docs/internals/embedded.md` in the
repo for the semantics underneath.

## Tests

```bash
CLINK_LIB=$PWD/build/libclink.dylib python3 python/tests/test_pyclink.py
```

(Also runnable under pytest.)
