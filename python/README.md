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

Release tags publish pyclink to PyPI (`pip install pyclink`) through trusted
publishing, from v0.9.1. The wheels are built by
`.github/workflows/wheels.yml`:

- macOS arm64 (`macosx_14_0_arm64`), without connectors.
- Linux x86_64 and aarch64 (`manylinux_2_28`), with the Kafka connector.
  From the release after v0.10.0 they also carry the ClickHouse connector,
  its native sink (`insert_format='native'`) and that sink's TLS; v0.10.0
  and earlier have no ClickHouse connector. They install on any glibc 2.28
  or later distribution (AlmaLinux 8, Debian 10+, Ubuntu 20.04+) and need
  nothing beyond the C and C++ runtime.

None of the wheels has the object-store filesystems, and none serves or
fetches HTTPS through clink's HTTP subsystem (the REST and diagnostic
endpoints). Kafka TLS and SASL and ClickHouse TLS are separate: both use one
OpenSSL linked into the Linux wheels' library. The Linux Kafka connector leaves
out GSSAPI and the Schema Registry formats. Build from source (below) for
those, or on a platform without a wheel. There is no Windows wheel.

In the wheels that carry it, the ClickHouse sink with `secure='true'` and no
`tls_ca_file` or `tls_ca_dir` trusts `SSL_CERT_FILE` and `SSL_CERT_DIR` when
they are set, and otherwise the distribution's CA bundle: the wheel's OpenSSL looks in `/usr/lib/ssl`, and
where that is missing (AlmaLinux 8, or Debian and Ubuntu without the `openssl`
package) the sink takes the first bundle it finds among the standard locations,
`/etc/pki/tls/certs/ca-bundle.crt` and `/etc/ssl/certs/ca-certificates.crt`
among them, and names it in its open log line. A slim image may need the
`ca-certificates` package for a publicly signed server. See the
[ClickHouse connector](https://orhaugh.github.io/clink/connectors/clickhouse/#tls-and-failover).

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
`checkpoint_interval_ms`, `catalog_dir`, `fresh`, and `lib_path` (which beats
the `CLINK_LIB` environment variable). With `checkpoint_dir` set, a job whose
last run there did not finish resumes from its checkpoints; `fresh=True`
starts it from empty state instead. Jobs are matched by their order within a
session (the first job submitted is job 1 in every session), so an
interactive session that reuses a checkpoint directory resumes its first
query from the previous session's first query, if that one was still running
when the engine closed; a different first query is refused. Give each
notebook its own directory, or pass `fresh=True`. `await_all()` polls in slices, so Ctrl-C
cancels the running jobs and drains before re-raising.

See `docs/internals/embedded.md` in the repo for the semantics underneath and
for how the wheel bundles libclink.

## Tests

```bash
CLINK_LIB=$PWD/build/libclink.dylib python3 python/tests/test_pyclink.py
```

(Also runnable under pytest.)
