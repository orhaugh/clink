# tools/

The two installed binaries and the sources behind them.

## `clink`

The client CLI (`clink_cli.cpp`). One binary, one subcommand per verb; every
subcommand accepts `--help` for its own flag list, and `clink --capabilities`
(or `--capabilities-json`) prints what this particular binary was built with:
connectors compiled in, their declared delivery guarantees, and the build
surfaces (SQL, HTTP, TLS, WASM, ONNX).

| Subcommand | Source | What it does |
| --- | --- | --- |
| `run` | `clink_submit_job.cpp`, `clink_run_sql.cpp` | `clink run pipeline.sql` (or `-e "<sql>"`) runs a SQL script embedded in this process; with `--coordinator-host`/`--coordinator-port` (the coordinator's HTTP port) the same script submits to a cluster. `clink run --job=<path.so> --coordinator-host=H --coordinator-port=P` (the RPC port, default 6123) submits a compiled job plugin. |
| `run-application` | `clink_app.cpp` | Start an in-process coordinator and run a job. |
| `cancel`, `stop` | `clink_cancel_job.cpp`, `clink_stop_job.cpp` | Cancel a job abruptly (everything since the last checkpoint replays on restart) or stop it gracefully (drain, commit the tail at a final checkpoint, print the id to resume from). |
| `savepoint` | `clink_savepoint.cpp` | Trigger a synchronous savepoint. |
| `rescale`, `rescale-op` | `clink_rescale_job.cpp`, `clink_rescale_op.cpp` | Change a running job's per-role parallelism, or rescale one operator. |
| `list` | `clink_list.cpp` | List active and recently completed jobs. |
| `lint` | `clink_lint.cpp` | Check a job or cluster configuration for settings that would be ignored or contradict each other; no cluster needed; exits 1 on anything submission would refuse. |
| `checkpoint-verify` | `clink_checkpoint_verify.cpp` | Verify a checkpoint directory's integrity; `--repair` mints sidecars for a directory written before they existed. |
| `check-savepoint` | `clink_check_savepoint.cpp` | Print a savepoint's state-schema version and shape-fingerprint stamps; with `--expected=<job.so>` run the pre-deploy compatibility check. |
| `state-cat`, `state-diff`, `state-export`, `state-query`, `state-sweep` | `clink_state_diff.cpp`, `clink_state_query.cpp`, `clink_state_sweep.cpp`, `state_tool_io.hpp` | Dump, diff, export (Arrow IPC) and query (SQL) a checkpoint or savepoint's keyed state; reclaim unreferenced objects from a disaggregated store. |
| `capture-cat`, `capture-push`, `capture-fetch` | `clink_capture_sync.cpp` | Inspect the flight recorder's epochs; ship a capture tree to object storage and fetch it back. |
| `replay`, `replay-diff` | `clink_replay.cpp` | Replay a captured epoch offline (one operator or the whole job) and diff two emission dumps. |
| `flight-sql` | `clink_flight.cpp` | Serve the embedded engine over Arrow Flight SQL. |

`cli_config_args.hpp` holds the flag parsing and checkpoint-config assembly the
client verbs share, so `clink lint` and `clink run` cannot disagree;
`cli_tls_args.hpp` validates control-plane TLS arguments. The `*_stub.cpp`
files keep every subcommand present in builds without the SQL frontend or
Arrow Flight, answering with a clear "not built" instead of an unknown
command. The `*_shim.cpp` files keep the pre-CLI binary names
(`clink_submit_job`, `clink_cancel_job`, `clink_savepoint`,
`clink_rescale_job`, `clink_app`) working as aliases of the subcommands.

## `clink_node`

The cluster daemon (`clink_node.cpp`): one binary that runs as either a
Coordinator (`--role=coordinator --port=6123`, plus `--http-port=<n>` to
enable the HTTP API and console) or a Worker (`--role=worker
--coordinator-host=<h> --coordinator-port=6123 --slots=<n>`). `--help` lists
every flag; `--version` prints the build, including the allocator in use. The
control plane it implements is described in
[docs/internals/distributed-runtime.md](../docs/internals/distributed-runtime.md).

## `clink_submit_sql`

The SQL-to-`JobGraphSpec` compiler (`clink_submit_sql.cpp`), built when the
SQL frontend is. It prints a plan or POSTs it to a running coordinator, and is
what the benchmark harnesses submit with; `clink run` covers the same path for
everyday use.
