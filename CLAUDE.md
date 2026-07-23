# clink - working guide

Conventions and orientation for working in this repository, whether by hand or
with an AI coding assistant. It supplements the root `README.md` and the
references under `docs/`; it does not replace them.

## Working conventions

Standing preferences for this repository. Follow them by default.

- Voice: British English. No em dashes; use a spaced hyphen or restructure the sentence. No hype or filler.
- Framing: describe clink in its own terms in engine code, tests, and docs, and do not explain features by analogy to other engines. The exceptions are the "inspired by Apache Flink" note in the root README, one sentence in `docs/internals/architecture.md`, and the cross-engine benchmark harnesses under `benchmarks/`, where naming the compared engine is unavoidable. Keep those harnesses premise-pinned and correctness-gated, and make no absolute claims about the other engine's capabilities.
- Git: work directly on `main`; no feature branches. Commit once a unit of work is done and verified, and push only when asked. Do not add a `Co-Authored-By` trailer; the commit history is kept single-author.
- Effort: work inline by default. Reserve multi-agent workflows for genuine large fan-out, such as many independent units, parallel edits that would otherwise collide, or scope beyond a single context. Consult the codebase map and `docs/` below before scanning the tree, so work starts informed.
- Comments: do not add internal roadmap or milestone tags (for example "Phase 12") to comments or commit messages. They carry no meaning outside the work plan and were removed deliberately.
- Stateful operators need a uid: give every stateful operator a stable uid, via `.uid("...")` on the fluent `DataStream` / `KeyedDataStream` or `set_uid("...")` on a Dag-direct operator. The uid derives the `OperatorId` (`operator_id_from_uid` in `include/clink/core/types.hpp`) that state restore, rescale, and schema evolution key on. An operator without one cannot have its state restored, so a missing uid is a correctness bug rather than a style nit.
- Build parallelism: use `cmake --build <dir> --parallel 10` and `ctest --test-dir <dir> --parallel 8` (or `-j8`). Never use a bare `-j`, as unbounded parallelism can freeze a workstation.
- clang-format: the pre-commit hook uses Apple clang-format at `/Library/Developer/CommandLineTools/usr/bin/clang-format`. Format with that binary, not Homebrew's newer clang-format, or the version skew reformats lines differently and blocks the commit.
- Docs are part of "done": at the end of a feature round, update the affected documentation in the same change rather than as a follow-up. That means the relevant `docs/connectors/<name>.md` or `docs/internals/<page>.md` (and its index `README.md`), the root `README.md` if a capability or section changed, and the codebase map below if a subsystem or key file moved or was added. If a shipped feature has no doc page, create one. A feature is not complete until its docs match the code.
- Docs location: `/docs/*` is gitignored except `consumer-examples/`, `connectors/`, and `internals/`. Do not commit other `docs/*.md`, which are internal work notes; to publish a new docs subdirectory, add a matching `!/docs/<dir>/` exception.
- Diagrams: use a fenced ` ```mermaid ` block, which GitHub renders, not ASCII art. Keep node labels in double quotes; escape `<`, `>`, and `&` as `&lt;`, `&gt;`, and `&amp;`; use `<br/>` for line breaks; and avoid `[`, `]`, `|`, `{`, and `}` inside labels. For data layouts such as struct fields or byte formats, a Markdown table is fine.

## Codebase map

Orientation so work starts informed instead of re-deriving the layout each time.
Read this before scanning the tree. For any "how does X work" or "where does X
live" question, read the relevant `docs/internals/` page first, then follow it to
the source it cites. Keep these pages current when a subsystem changes.

- Deep per-subsystem references: `docs/internals/` (index: `docs/internals/README.md`).
- Per-connector references (dependencies, factory names, options, SQL usage): `docs/connectors/`.
- Runnable API examples: `docs/consumer-examples/`.

| Subsystem | Source | Internals page |
|-----------|--------|----------------|
| Engine core (types, StreamElement, batches) | `include/clink/core/` | `docs/internals/architecture.md` |
| Operators + DAG + fluent API | `include/clink/operators/`, `include/clink/runtime/dag.hpp`, `include/clink/api/` | `docs/internals/operator-model.md` |
| Local runtime / task lifecycle | `src/runtime/local_executor.cpp`, `include/clink/runtime/` | `docs/internals/task-lifecycle.md` |
| Jobs, parallelism, scheduling, key groups | `include/clink/cluster/job_graph.hpp`, `include/clink/job/` | `docs/internals/jobs-and-scheduling.md` |
| Cluster control plane (coordinator/worker, protocol, HA, plugin model) | `src/cluster/`, `include/clink/cluster/`, `include/clink/plugin/`, `tools/clink_node.cpp` | `docs/internals/distributed-runtime.md` |
| Network / channels / Arrow wire / backpressure | `include/clink/runtime/network/`, `include/clink/runtime/multi_input_alignment.hpp` | `docs/internals/network-stack.md` |
| Time, watermarks, windows, CEP | `include/clink/time/`, `include/clink/operators/`, `include/clink/cep/` | `docs/internals/time-and-windowing.md` |
| Keyed/broadcast state + backends + queryable state | `include/clink/state/`, `src/state/`, `include/clink/queryable_state/` | `docs/internals/state-and-backends.md`, format contract: `docs/internals/state-snapshot-format.md` |
| Checkpointing, barriers, 2PC sinks | `include/clink/checkpoint/`, `src/checkpoint/`, `include/clink/connectors/file_2pc_sink.hpp` | `docs/internals/checkpointing.md` |
| Sink committer framework (exactly-once / upsert sinks) | `include/clink/connectors/committing_sink.hpp`, `include/clink/connectors/sql_json_builder.hpp`, per-connector `*_2pc_sink`/`*_upsert_sink` in `impls/<name>/` | `docs/internals/sink-committer-framework.md` |
| Failover, rescale, schema evolution, savepoints | `include/clink/cluster/rescale_*`, `include/clink/state/schema_version.hpp`, `include/clink/state_processor/` | `docs/internals/fault-tolerance-and-rescale.md` |
| Arrow columnar execution | `include/clink/core/arrow_batcher.hpp`, columnar hooks in `include/clink/operators/` | `docs/internals/columnar-execution.md` |
| Async substrate + disaggregated state | `include/clink/async/`, `src/async/`, RemoteReadBackend in `include/clink/state/` | `docs/internals/async-state-execution.md` |
| SQL frontend (parse -> bind -> plan -> ops) | `include/clink/sql/`, `src/sql/` | `docs/internals/sql-frontend.md` |
| Embedded execution (`clink run <file>.sql`, EmbeddedEngine) | `include/clink/embed/`, `src/embed/`, `include/clink/sql/script_runner.hpp`, `tools/clink_run_sql.cpp` | `docs/internals/embedded.md` |
| Data lineage (capture + pluggable export) | `include/clink/lineage/`, `src/lineage/` | `docs/internals/data-lineage.md` |
| Connectors (sources/sinks/backends) | `impls/<name>/` | `docs/connectors/<name>.md` |
| Testing framework (public, for library consumers) | `include/clink/test/` | `docs/internals/testing-framework.md` |

## Pinned toolchain (one-time bootstrap)

Arrow, Parquet, and iceberg-cpp are pinned at the exact versions in
`scripts/versions.env` and installed into `CLINK_DEPS_PREFIX` (host default
`~/.clink-deps`), so the host and the Debian image link byte-for-byte identical
libraries. CMake auto-prepends that prefix and asserts the Arrow version, so any
`cmake` invocation finds it once it exists. Bootstrap it once on a fresh checkout;
idempotent afterwards:

```bash
scripts/build-arrow.sh && scripts/build-iceberg-cpp.sh   # -> ~/.clink-deps
```

`build-arrow.sh` first tries `scripts/fetch-deps.sh`, which restores a prebuilt
archive from the repo's `deps` GitHub release (about a minute) when one exists
for the platform and pin; otherwise it compiles from source (slow). Archives are
verified against the sha256 pins in `scripts/deps-checksums.txt`, and on macOS
refused unless the linked Homebrew `aws-sdk-cpp` keg matches the one the archive
was compiled against (the ABI-drift SIGBUS gotcha). `CLINK_DEPS_FROM_SOURCE=1`
forces the source build. After bumping `scripts/versions.env`: rebuild + repackage
(`scripts/package-deps.sh` on macOS, the `deps-artifacts` workflow for Linux),
upload to the `deps` release, and update `scripts/deps-checksums.txt`.

`./build_and_test.sh` does this automatically. In Docker it is baked into the
image at `/usr/local` (`scripts/setup-build-env.sh`). To change a version, bump it
in `scripts/versions.env`, delete the prefix, and re-bootstrap. Arrow's S3 support
uses the system aws-sdk (Homebrew on the host, built into the image) rather than
Arrow 24's bundled CRT, which does not build cleanly here; the data-path
dependencies stay bundled and pinned.

## Build & test

The reproducible path is `./build_and_test.sh`, optionally inside the project's
Docker image:

```bash
./build_and_test.sh                                            # normal build + ctest
./build_and_test.sh --sanitizer asan                           # AddressSanitizer
./build_and_test.sh --sanitizer tsan                           # ThreadSanitizer
./build_and_test.sh --sanitizer ubsan                          # UBSanitizer
./build_and_test.sh --sanitizer all                            # normal + ASan + TSan + UBSan
./build_and_test.sh --image clink-build:latest --sanitizer all
```

The local Docker image is `clink-build:latest`. When running sanitizers or
coverage, prefer the `--image clink-build:latest` form, since that is where the
toolchain (clang, lcov, gcovr, the right librdkafka and libpq versions, and so
on) is pinned; the host machine can be missing pieces.

Build artifacts go into `build/`, `build-asan/`, `build-tsan/`, `build-ubsan/`,
and `build-coverage/`, all of which are gitignored.

## Fast path

For everyday local iteration without sanitizers:

```bash
cmake -S . -B build && cmake --build build --parallel 10 && ctest --test-dir build -j8
```

`ctest --test-dir build -L core` runs just `clink_core_tests`; `-L kafka`,
`-L postgres`, `-L clickhouse`, `-L s3`, `-L rocksdb`, `-L tls`, and
`-L integration` hit the per-impl test executables.

## Nexmark benchmark ("run a nexmark run")

The harness lives in `benchmarks/nexmark_compare/`. Two scripts do different jobs:

- `throughput_sampled.sh` is the canonical throughput run, and what "run a nexmark run" means by default. It measures sustained engine-side records per second by polling each engine's own counter and taking the maximum slope, so the figure excludes deploy time, JVM warm-up, and the end taper. Defaults: `q0 q12`, parallelism 4, 5M events. Both engines run containerised.
- `run.sh` is the correctness-gated comparison. It times broker append and gates on identical output-row counts. Defaults: `q0 q12`, parallelism 1, 500k events. It runs the host build (`BUILD_DIR`, default `build/`).

Canonical command for a before/after throughput check:

```bash
cd benchmarks/nexmark_compare
SINK=blackhole EVENTS=10000000 QUERIES="q0 q12" PARALLELISM=4 ./throughput_sampled.sh
```

`SINK=blackhole` discards output (using the `q*_bh.tmpl.sql` variants), removing
the Kafka write ceiling so the measurement reflects engine read and process rate
only. `SINK=kafka` (the default) writes to a topic and gates on row count.
`KEEP_UP=1` leaves the cluster running. `STATE_BACKEND=<uri>` (for example
`rocksdb:///tmp/nx-state` or `forst:///tmp/nx-state`; paths are inside the
worker containers) passes a per-job `--state-backend` to clink's submit; unset
keeps the canonical in-memory premise, and a set value makes the run
clink-vs-clink tracking for that backend (Flink stays on its compose-pinned
hashmap, so the cross-engine ratio no longer holds the matched premise).
`RUN_TAG=<tag>` suffixes the clink result filenames (`q12-clink-<tag>.json`)
and scopes the startup wipe to that tag, so backend variants sit side by side.
A `forst://` run needs the runtime image built with the opt-in engine:
`docker build --build-arg CLINK_WITH_FORST=ON -t clink-runtime:latest -f
docker/Dockerfile.runtime .`

Four things to get right for a valid before/after:

1. `throughput_sampled.sh` runs the `clink-runtime:latest` Docker image, not the host `build/`. The host build only compiles `nexmark_dump` (data generation) and `clink_submit_sql` (submission), so a code change is not measured until the image is rebuilt at the new commit. Rebuild it first (`verify_distributed.sh` builds or refreshes `clink-runtime:latest`), then run. Check freshness with `docker image inspect clink-runtime:latest --format '{{.Created}}'` against the commit under test.
2. The columnar fast paths (`process_columnar`: WS1 filter/project programs, WS3 within-batch group-by) do not fire on a Kafka-JSON-sourced nexmark. The Kafka JSON source decodes to row batches with no Arrow sidecar, and the inter-operator wire batcher (`make_row_wire_batcher`) only passes an existing sidecar through; it never manufactures columnar data from rows, so the receiver materialises rows and the window or aggregation operator takes the row path. Columnar data is only born from a columnar-native source such as `ParquetSource` with `schema_columns`, which is what the in-tree `ColumnarParquet*` tests use. To benchmark WS1 or WS3, feed the query from Parquet, not Kafka JSON. As wired today, the only columnar lever nexmark touches is WS4 (the `FlatMap` window-state map on q12), and only when the image is built with `-DCLINK_USE_FLAT_HASH_MAP=ON` (off by default).
3. Only `q0` (projection-style passthrough) and `q12` (windowed GROUP BY) are sampled for throughput. `q6` and `q8` have tiny join inputs that drain in under a second, so they live in the gate harness rather than here.
4. A `STATE_BACKEND` run on a synchronous backend (`rocksdb://`, plain `forst://`) is an integration check, not a state-backend benchmark. The SQL window and aggregate operators keep hot-path state in in-memory maps unless the backend defers reads (`supports_async_get()`); a synchronous backend carries checkpoint and restore durability only, and q12's window operator does not even flush to it. Verified 2026-07-22: q0/q12 throughput deltas across memory, rocksdb and forst runs sit inside the harness's run-to-run variance (stateless q0 alone spread 1.04M-1.82M drain rec/s over three runs). The deferring backends are `remote-read://`, `forst://...?defer_reads=1`, and `s3sst+forst://` (deferring by default) - with those, per-record state genuinely rides the backend and the operators take their async KeyedState paths. For a per-record backend A/B on the engine side, use `benchmarks/inproc_compare` with `CLINK_STATE_BACKEND=rocksdb://<dir>` vs `forst://<dir>` and `CLINK_WB_STATE_CACHE=0` (strict writes); measured there at parity, with a one-off ~5s cold-start on the first-ever forst run.

Results are gitignored: `results-sampled/` (sampled runs), `results/` (`run.sh`),
and `results-containers/`. Each is a per-query JSON with `sustained_slope` (the
headline records per second), `drain_rate`, `reached_target`, and CPU and wall
seconds. Compare `sustained_slope` before and after for the same query,
parallelism, event count, sink, and state backend.
