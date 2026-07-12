# Embedded execution

> How `clink run <file>.sql` runs the whole engine in one process with no
> daemons; the EmbeddedEngine class behind it; and libclink, the pure-C
> embedding ABI with collect-to-Arrow results.

## Overview

Embedded execution packages the full runtime - JobManager, TaskManager, the SQL
frontend, and every linked connector - inside a single process. The connector
catalogue is installed through `clink::plugin::install_defaults` at the front
doors (`clink run`'s tool TU and libclink's `clink_engine_open`), whose
`CLINK_HAS_*` guards resolve in a TU that links the impls - so embedded SQL
reaches the same connectors a cluster node does (Kafka, Iceberg, Postgres, ...),
not just the built-ins. `clink run
pipeline.sql` starts an in-process JobManager and TaskManager pair connected
over an ephemeral loopback port, folds the script's DDL into a session catalog,
compiles each `INSERT INTO ... SELECT` (or materialized-view statement) to a
`JobGraphSpec`, submits it straight to the in-process JobManager, and blocks
until the jobs finish. Nothing about the job differs from cluster execution:
the same planner, the same chain materialisation on the TaskManager, the same
checkpointing machinery. Adding `--jm-host`/`--jm-port` to the same command
compiles the same file and submits it to a running cluster instead - one verb
from laptop to cluster.

Measured on a Debug build (2026-07-11, 12-core Apple Silicon): ~0.4 s from
process start to the first result row on stdout for a file-source query,
identical for a plain projection and a GROUP BY - engine bring-up
dominates, the operator chain barely registers - with total process
lifetime dominated by post-completion teardown, not startup. (An earlier
~100 ms figure predated the full connector registry and no longer
reproduces; re-measure on a Release build before quoting a number.)

## Where it lives

- `include/clink/embed/embedded_engine.hpp`, `src/embed/embedded_engine.cpp` -
  `EmbeddedEngine`: the in-process JM + TM pair plus the script-execution and
  await/cancel surface. The `clink::embed` CMake target.
- `include/clink/embed/clink.h`, `src/embed/clink_c.cpp` - libclink: the
  pure-C ABI over EmbeddedEngine (`clink_shared` CMake target, artifact
  `libclink.so` / `.dylib` with the engine and every built connector linked
  in statically).
- `include/clink/embed/collect_hub.hpp`, `src/embed/collect_hub.cpp` - the
  `connector='collect'` plumbing: per-engine queues of typed Arrow batches,
  the process-wide scope registry that lets process-wide sink factories find
  per-engine queues, and the `collect_sink_row` factory.
- `include/clink/sql/script_runner.hpp`, `src/sql/script_runner.cpp` -
  `run_script`: the statement-processing loop shared by `clink_submit_sql`,
  the embedded runner, and any future front door. DDL folds into the caller's
  catalog; every compiled job goes to a caller-supplied `SubmitFn` (HTTP POST
  for the tool, `JobManager::submit_job` for the embedded engine).
- `tools/clink_run_sql.cpp` - the `clink run <file>.sql` front end: flag
  parsing, the embedded/remote fork, and Ctrl-C handling.
  `tools/clink_run_sql_stub.cpp` links in its place when the build has no SQL
  frontend (`CLINK_BUILD_SQL=OFF`) so the CLI still links and errors usefully.
- `tools/clink_submit_job.cpp` - `clink run` dispatches to the SQL front end
  on the argument shape (a positional ending `.sql`, or `-e`/`--file`); the
  compiled-job `.so` path is untouched.

## How it works

### The in-process cluster

`EmbeddedEngine`'s constructor installs the built-in and SQL operator
factories once per process, then builds the same topology the SqlRuntime
end-to-end suite proves: `JobManager::start(0)` binds an ephemeral loopback
port, `expect_tms` + `TaskManager::connect_to_jm` + `await_registrations`
make registration deterministic (no settle sleeps). Slots default to 64 -
slots are placement bookkeeping, not threads, so a generous default keeps
deep plans deployable at no cost.

Each compiled job is submitted with a per-job `CheckpointConfig` assembled
from the engine options: `--checkpoint-dir` enables checkpointing (periodic
trigger cadence from `--checkpoint-interval-ms`, default 10 s), and
`--state-backend` carries a state-backend URI exactly as a cluster submit
would (`rocksdb:///path`, `remote-read://bucket`, ...). Control and data
planes ride loopback TCP; short-circuiting same-process edges is deliberate
future work, not a correctness gap.

### Await and cancellation

`await_all` polls every submitted job in 200 ms slices. A caller-supplied
`cancel_requested` hook (the CLI wires it to a SIGINT flag) flips the run
into cancellation: every job is cancelled, and the drain continues under a
30 s cap so a wedged cancel cannot hang the caller. A user stop is treated
as a normal outcome for an unbounded pipeline - the CLI exits 0, reporting
any teardown errors as notes. A second Ctrl-C force-quits (exit 130).

### Bare SELECT and the print sink

A bare top-level `SELECT` has no sink, so the script runner (when the
embedded front door enables `bare_select_to_print`) binds the SELECT for its
output schema, registers a synthesised `connector='print'` table carrying
that schema (Arrow types verbatim - `ColumnSpec` holds `arrow::DataType`),
and compiles the statement as `INSERT INTO` it. The print sink
(`print_sink_row`, registered with the other SQL built-ins in
`src/sql/install.cpp`) writes one JSON line per record to stdout; changelog
rows print with their kind prefixed (`-U` / `+U` / `-D`) and the
`__row_kind` marker stripped, so a retracting TOP-N reads naturally. Remote
submission rejects a bare SELECT: its print sink would write to a
TaskManager's stdout, not the user's terminal.

### libclink and the collect sink

libclink is one shared library plus one pure-C header (compiles as C99; the
Arrow C data/stream interface structs are reproduced verbatim so the header
stands alone). The surface is handle-based: `clink_engine_open` starts an
EmbeddedEngine, `clink_exec` runs a script, `clink_job_wait` /
`clink_await_all` / `clink_cancel_all` control the submitted jobs, and
`clink_last_error` carries diagnostics - in library mode the engine's err
stream is captured into the handle rather than written to stderr. ABI
compatibility is guarded by `clink_abi_version()`.

Rows reach the host through `connector='collect'`: the sink converts each
Row batch to a typed Arrow RecordBatch with the same schema-driven batcher
the wire uses (`make_row_columnar_arrow_batcher`; declared column types,
utf8 fallback for shapes outside its set, and the batcher's prepended
engine event-time column stripped so the host sees exactly the declared
SELECT columns). Batches land in a per-table queue owned by the engine's
CollectHub, and `clink_collect_stream` exports the queue as an
`ArrowArrayStream` via `arrow::ExportRecordBatchReader` - zero-copy into
pyarrow, DuckDB, polars, or Arrow C++.

The factory-to-engine binding works by scope stamping: sink factories are
process-wide, queues are per-engine, so each engine registers its
CollectHub under a fresh scope token and stamps that token onto every
`collect_sink_row` op at submit time; the sink resolves its hub through the
registry at open(). This is also what makes collect embedded-only: a
cluster submit carries no scope (and cluster TaskManagers have no factory),
so it fails loudly rather than silently writing nowhere.

Stream semantics: one consumer per table; `get_next` blocks until a batch
arrives; end-of-stream fires when the producing job's sink subtasks have
all closed (completion, failure and cancellation all close, so a reader
never waits on a dead job); closing the engine wakes blocked readers with a
cancelled status, and an exported stream stays safe to drain and release
after the engine is gone (it keeps its queue alive).

Changelog semantics: a plain collect table is append-only - a retracting
(changelog) SELECT is rejected at bind so retractions are never silently
flattened into inserts. Declaring the table with `changelog='true'` opts
in: the Arrow stream then carries a leading `row_kind` utf8 column
(`insert` / `delete` / `update_before` / `update_after`) ahead of the
declared columns, populated by the sink from each row's changelog kind
(unmarked rows are inserts), and the host applies the changelog itself -
add on insert/update_after, remove on delete/update_before. The reader
prepends the identical field to its schema, so both sides stay
byte-identical; a declared column named `row_kind` is rejected as
reserved in this mode.

pyclink (`python/pyclink/`) is pure Python over this ABI: ctypes bindings,
no compiled extension, with collect streams imported straight into
`pyarrow.RecordBatchReader`.

libclink is SELF-CONTAINED: Arrow and Parquet link statically into the
artifact (the then-unreferenced dylib load commands are stripped), and the
dynamic symbol table exports ONLY the clink_* C API - macOS uses an
exported-symbols list, Linux a version script - so ~20k engine and
third-party symbols no longer leak into the host process. Two consequences:
the artifact needs no Arrow install on the host, and load order against a
host Arrow (pyarrow) no longer matters. The last co-residence hazard was
allocator-level, not symbol-level: Arrow's default mimalloc pool corrupts
when a second Arrow (pyarrow's) runs its own mimalloc in the same process,
so `clink_engine_open` binds libclink's PRIVATE Arrow copy to the system
memory pool once, before any Arrow allocation, restoring the environment so
a host Arrow's allocator choice is untouched (an explicit
`ARROW_DEFAULT_MEMORY_POOL` is always respected).

### Packaging: pyclink wheels

A pyclink wheel bundles one platform `libclink` next to the package
(`pyclink/libclink.dylib` / `.so`); the loader (`_bundled_library`) finds it, so
`pip install pyclink` needs no separate build and no `CLINK_LIB`. Because pyclink
is ctypes with no CPython extension, the library carries no CPython ABI, and the
wheel is tagged `py3-none-<arch>` - one wheel serves every Python 3 on a
platform (`python/setup.py`, `platform_wheel.get_tag`). The build is lean:
`clink_shared` with `CLINK_BUILD_IMPLS=OFF`, so libclink carries the SQL engine
and the built-in file / collect / Parquet surfaces but no external-SDK
connectors, keeping the only shared deps Arrow's own (curl, xml2, aws-sdk,
openssl, lz4, zstd), which the repair step (delocate on macOS, auditwheel on
Linux) vendors into the wheel. libclink is built once per platform outside the
wheel build and handed to `setup.py` via `CLINK_LIB`
(`scripts/build-libclink-wheel.sh`); the wheel then simply copies it. The CI
that produces the wheels and their platform scope is `.github/workflows/wheels.yml`.

### The Flight SQL endpoint

`clink flight-sql` (or `ClinkFlightSqlServer` from
`include/clink/embed/flight_sql_server.hpp`, built when the linked Arrow
ships Flight SQL) serves the embedded engine over Arrow Flight SQL, so any
Flight SQL / ADBC / JDBC client - DBeaver, pandas via
`adbc_driver_flightsql`, dbt adapters - speaks to clink over one wire
protocol. Statement QUERIES compile a bare SELECT through the same
synthesis the collect path uses (`ScriptRunOptions::bare_select_to_collect`
-> `EmbeddedEngine::submit_select_to_collect`) and stream the typed Arrow
batches back; a retracting plan's stream carries the leading `row_kind`
column. Statement UPDATES run DDL / INSERT scripts synchronously
(`EmbeddedEngine::execute_update` - submitted jobs complete before the call
returns; streaming pipelines belong in queries). `GetTables` serves the
engine catalog, with real per-table Arrow schemas when the client asks.
v1 has no authentication (bind loopback, the default, or front with a
proxy), no prepared statements and no transactions.

### What embedded mode does not change

The session catalog behaves exactly like `clink_submit_sql`'s
(`--catalog-dir` opts into persistence). Full-refresh materialized views run
their initial population, but scheduler re-arming across engine restarts
needs a catalog dir, same as a cluster. Parallelism above 1 fans plans out
identically to a cluster submit; everything runs on the one in-process
TaskManager's slots.

## Verification

`tests/test_embedded_engine.cpp`: a bounded file-to-file GROUP BY end to end,
bare-SELECT print output captured at the fd level, a retracting TOP-N
printing kind prefixes, cancel-while-running, pure-DDL scripts, the
collect reader (typed batches, end-of-stream, single-consumer, changelog
rejection), and the script-runner's synthesis and rejection paths.
`tests/test_clink_c_abi.cpp` drives the C ABI end to end while linking ONLY
the shared library (the registries live inside libclink, mirroring a real
embedding), importing the collect stream through Arrow C++ and asserting
values; `tests/test_clink_c_smoke.c` is a pure-C consumer (compiled as C)
that drains the stream through the raw callbacks. The full SQL suite
exercises the shared script runner through `clink_submit_sql`'s CLI tests.
