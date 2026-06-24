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
