# Embedded execution

> How `clink run <file>.sql` runs the whole engine in one process with no
> daemons, and the EmbeddedEngine class behind it.

## Overview

Embedded execution packages the full runtime - JobManager, TaskManager, the SQL
frontend, and every linked connector - inside a single process. `clink run
pipeline.sql` starts an in-process JobManager and TaskManager pair connected
over an ephemeral loopback port, folds the script's DDL into a session catalog,
compiles each `INSERT INTO ... SELECT` (or materialized-view statement) to a
`JobGraphSpec`, submits it straight to the in-process JobManager, and blocks
until the jobs finish. Nothing about the job differs from cluster execution:
the same planner, the same chain materialisation on the TaskManager, the same
checkpointing machinery. Adding `--jm-host`/`--jm-port` to the same command
compiles the same file and submits it to a running cluster instead - one verb
from laptop to cluster.

Measured on a Debug build: under ~100 ms from process start to the first
result row on stdout for a file-source query, with total process lifetime
dominated by post-completion teardown, not startup.

## Where it lives

- `include/clink/embed/embedded_engine.hpp`, `src/embed/embedded_engine.cpp` -
  `EmbeddedEngine`: the in-process JM + TM pair plus the script-execution and
  await/cancel surface. The `clink::embed` CMake target.
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
printing kind prefixes, cancel-while-running, pure-DDL scripts, and the
script-runner's synthesis and rejection paths. The full SQL suite exercises
the shared script runner through `clink_submit_sql`'s CLI tests.
