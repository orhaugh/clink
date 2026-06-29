# clink - agent notes
No emdashes!!!

## Working conventions

How to work in this repo. These reflect standing preferences; honour them by default.

- Voice: British English. No em dashes anywhere (use a spaced hyphen or restructure). No hype or filler.
- Framing: describe clink in its own terms. Do NOT frame it against Flink in code, tests, or docs. The only exceptions are the root README's existing "inspired by Apache Flink" note and the single sentence in `docs/internals/architecture.md`.
- Git: work directly on `main`; no feature branches. Commit when a unit of work is done and verified; push only when asked. Do NOT add a `Co-Authored-By: Claude` trailer (this is a public repo).
- Effort: default to working INLINE. Reserve multi-agent workflows for genuine large parallel fan-out (many independent units, parallel edits that would collide, scope beyond one context), not routine tasks. Consult the Codebase map and `docs/` below before scanning the tree, so work starts informed.
- Comments: do not add internal roadmap or milestone tags (e.g. "Phase 12", "Phase 29d") to comments or messages; they carry no meaning outside the work plan and were deliberately removed.
- Stateful operators MUST be given a stable uid: `.uid("...")` on the fluent `DataStream`/`KeyedDataStream`, or `set_uid("...")` on a Dag-direct operator. The uid derives the `OperatorId` (`operator_id_from_uid`, `include/clink/core/types.hpp`) that state restore, rescale, and schema evolution key on. A missing uid on a stateful op is a correctness bug, not a style nit.
- Build parallelism: use `cmake --build <dir> --parallel 10` and `ctest --test-dir <dir> --parallel 8` (or `-j8`). NEVER a bare `-j` (it can freeze this 12-core machine).
- clang-format: the pre-commit hook uses Apple clang-format at `/Library/Developer/CommandLineTools/usr/bin/clang-format`. Format with THAT binary, not Homebrew's newer clang-format, or a version skew reformats lines differently and blocks the commit.
- Docs: `/docs/*` is gitignored except `consumer-examples/`, `connectors/`, and `internals/`. Never commit other `docs/*.md` (internal work notes); to publish a new docs subdir, add a matching `!/docs/<dir>/` exception.
- Diagrams in docs: use a fenced ` ```mermaid ` block (GitHub renders it), not ASCII art. Keep node labels in double quotes, escape `<`/`>`/`&` as `&lt;`/`&gt;`/`&amp;`, use `<br/>` for line breaks, and avoid `[`/`]`/`|`/`{`/`}` inside labels. For data layouts (struct fields, byte formats) a Markdown table is fine.

## Codebase map (read this before scanning the tree)

Persisted orientation so work starts informed instead of re-deriving the layout
each time. For any "how does X work" or "where does X live" question, consult the
relevant `docs/internals/` page first, then go to the source it cites. Keep these
docs current when a subsystem changes.

- Deep per-subsystem references: `docs/internals/` (index: `docs/internals/README.md`).
- Per-connector references (deps, factory names, options, SQL usage): `docs/connectors/`.
- Runnable API examples: `docs/consumer-examples/`.

| Subsystem | Source | Internals page |
|-----------|--------|----------------|
| Engine core (types, StreamElement, batches) | `include/clink/core/` | `docs/internals/architecture.md` |
| Operators + DAG + fluent API | `include/clink/operators/`, `include/clink/runtime/dag.hpp`, `include/clink/api/` | `docs/internals/operator-model.md` |
| Local runtime / task lifecycle | `src/runtime/local_executor.cpp`, `include/clink/runtime/` | `docs/internals/task-lifecycle.md` |
| Jobs, parallelism, scheduling, key groups | `include/clink/cluster/job_graph.hpp`, `include/clink/job/` | `docs/internals/jobs-and-scheduling.md` |
| Cluster control plane (JM/TM, protocol, HA, plugin model) | `src/cluster/`, `include/clink/cluster/`, `include/clink/plugin/`, `tools/clink_node.cpp` | `docs/internals/distributed-runtime.md` |
| Network / channels / Arrow wire / backpressure | `include/clink/runtime/network/`, `include/clink/runtime/multi_input_alignment.hpp` | `docs/internals/network-stack.md` |
| Time, watermarks, windows, CEP | `include/clink/time/`, `include/clink/operators/`, `include/clink/cep/` | `docs/internals/time-and-windowing.md` |
| Keyed/broadcast state + backends + queryable state | `include/clink/state/`, `src/state/`, `include/clink/queryable_state/` | `docs/internals/state-and-backends.md` |
| Checkpointing, barriers, 2PC sinks | `include/clink/checkpoint/`, `src/checkpoint/`, `include/clink/connectors/file_2pc_sink.hpp` | `docs/internals/checkpointing.md` |
| Failover, rescale, schema evolution, savepoints | `include/clink/cluster/rescale_*`, `include/clink/state/schema_version.hpp`, `include/clink/state_processor/` | `docs/internals/fault-tolerance-and-rescale.md` |
| Arrow columnar execution | `include/clink/core/arrow_batcher.hpp`, columnar hooks in `include/clink/operators/` | `docs/internals/columnar-execution.md` |
| Async substrate + disaggregated state | `include/clink/async/`, `src/async/`, RemoteReadBackend in `include/clink/state/` | `docs/internals/async-state-execution.md` |
| SQL frontend (parse -> bind -> plan -> ops) | `include/clink/sql/`, `src/sql/` | `docs/internals/sql-frontend.md` |
| Connectors (sources/sinks/backends) | `impls/<name>/` | `docs/connectors/<name>.md` |

## Pinned toolchain (one-time bootstrap)

Arrow/Parquet + iceberg-cpp are COMPILED FROM SOURCE at exact versions
(`scripts/versions.env`) into `CLINK_DEPS_PREFIX` (host default `~/.clink-deps`)
so the host and the Debian image link byte-for-byte the same libraries. CMake
auto-prepends that prefix and asserts the Arrow version, so any `cmake` invocation
finds it once it exists. Bootstrap it once on a fresh checkout (slow - builds Arrow
from source; idempotent after):

```bash
scripts/build-arrow.sh && scripts/build-iceberg-cpp.sh   # -> ~/.clink-deps
```

`./build_and_test.sh` does this automatically. In Docker it is baked into the
image at `/usr/local` (`scripts/setup-build-env.sh`). Bump a version in
`scripts/versions.env`, delete the prefix, and re-bootstrap to change it. Arrow's
S3 uses the SYSTEM aws-sdk (Homebrew on host / built in the image), not Arrow 24's
broken bundled CRT; the data-path deps stay bundled + pinned.

## Build & test

The reproducible path is `./build_and_test.sh`, optionally inside the
project's Docker image:

```bash
./build_and_test.sh                                            # normal build + ctest
./build_and_test.sh --sanitizer asan                           # AddressSanitizer
./build_and_test.sh --sanitizer tsan                           # ThreadSanitizer
./build_and_test.sh --sanitizer ubsan                          # UBSanitizer
./build_and_test.sh --sanitizer all                            # normal + ASan + TSan + UBSan
./build_and_test.sh --image clink-build:latest --sanitizer all
```

**The local Docker image is `clink-build:latest`.** When running
sanitizers or coverage, prefer the `--image clink-build:latest`
form - that's where the toolchain (clang, lcov, gcovr, the right
librdkafka/libpq versions, etc.) is pinned. The host machine can be
missing pieces.

Build artifacts go into `build/`, `build-asan/`, `build-tsan/`,
`build-ubsan/`, `build-coverage/`. All are gitignored.

## Quick fast-path

For everyday local iteration without sanitizers:

```bash
cmake -S . -B build && cmake --build build --parallel 10 && ctest --test-dir build -j8
```

`ctest --test-dir build -L core` runs just `clink_core_tests`;
`-L kafka`, `-L postgres`, `-L clickhouse`, `-L s3`, `-L rocksdb`,
`-L tls`, `-L integration` hit the per-impl test exes.

## Nexmark benchmark ("run a nexmark run")

Harness lives in `benchmarks/nexmark_compare/`. Two scripts, different jobs:

- `throughput_sampled.sh` is the CANONICAL throughput run and what "run a
  nexmark run" means by default. It measures sustained engine-side
  records/sec (polls each engine's own counter, takes the max slope, so the
  number excludes deploy/JVM-warmup and end taper). Default `q0 q12`, par 4,
  5M events. Both engines run CONTAINERIZED.
- `run.sh` is the correctness-gated clink-vs-Flink comparison (broker-append
  timing, gates on identical output-row counts). Default `q0 q12`, par 1, 500k
  events. Runs the HOST build (`BUILD_DIR`, default `build/`).

Canonical command for a before/after engine-throughput check:

```bash
cd benchmarks/nexmark_compare
SINK=blackhole EVENTS=10000000 QUERIES="q0 q12" PARALLELISM=4 ./throughput_sampled.sh
```

`SINK=blackhole` discards output (uses the `q*_bh.tmpl.sql` variants) so the
Kafka write ceiling is removed and we measure engine read+process rate only;
`SINK=kafka` (default) writes to a topic and gates on row count. `KEEP_UP=1`
leaves the cluster up.

CRITICAL caveats for a valid before/after (do NOT skip):

1. `throughput_sampled.sh` runs the `clink-runtime:latest` DOCKER IMAGE for the
   engine, not the host `build/`. The host build only compiles `nexmark_dump`
   (data gen) and `clink_submit_sql` (submission). So a code change is NOT
   measured until the image is rebuilt at the new commit. Rebuild it first
   (`verify_distributed.sh` builds/refreshes `clink-runtime:latest`), then run.
   Check freshness: `docker image inspect clink-runtime:latest --format '{{.Created}}'`
   against the commit under test.
2. The columnar wave's `process_columnar` fast paths (WS1 filter/project
   programs, WS3 within-batch group-by) do NOT fire on a Kafka-JSON-sourced
   nexmark. Verified by tracing: the Kafka JSON source decodes to row `Batch`es
   with no Arrow sidecar; the inter-operator wire batcher (`make_row_wire_batcher`)
   is columnar PASS-THROUGH (ships an existing sidecar verbatim, but a row-form
   batch falls back to the JSON binary layout) so it never MANUFACTURES columnar
   from rows; the receiver materialises rows and the window/agg op takes the row
   path. Columnar is only born from a columnar-native source (e.g. `ParquetSource`
   with `schema_columns`) - which is what the in-tree `ColumnarParquet*` tests
   use. So to benchmark WS1/WS3 you must feed the query from Parquet, not Kafka
   JSON. As wired today the ONLY wave lever nexmark touches is WS4 (the `FlatMap`
   window-state map on q12), and only when the image is built with
   `-DCLINK_USE_FLAT_HASH_MAP=ON` (OFF by default).
3. Only `q0` (projection-ish passthrough) and `q12` (windowed GROUP BY) are
   sampled for throughput; `q8`/`q6` have tiny join inputs that drain in under a
   second so they live in the gate harness, not here.

Results (gitignored): `results-sampled/` (sampled), `results/` (run.sh),
`results-containers/`. Each is a per-query JSON with `sustained_slope`
(headline rec/s), `drain_rate`, `reached_target`, cpu/wall seconds. Compare
`sustained_slope` before vs after for the same query/par/events/sink.
