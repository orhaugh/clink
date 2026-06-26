# clink - agent notes
No emdashes!!!
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
cmake -S . -B build && cmake --build build -j && ctest --test-dir build -j8
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
