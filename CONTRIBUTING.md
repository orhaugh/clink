# Contributing to clink

Thank you for your interest. clink is young and moves quickly; small,
focused changes are the easiest to review and land.

## Before you start

- For anything beyond a small fix, open an issue first so the approach can
  be agreed before you invest time.
- Read the relevant page of the [internals reference](docs/internals/README.md)
  rather than reading the tree cold; each page names the source files
  behind a subsystem.

## Building and testing

The fast path:

```bash
cmake -S . -B build
cmake --build build --parallel 10
ctest --test-dir build --parallel 8
```

`./build_and_test.sh` is the reproducible path CI runs, optionally inside
the pinned Docker image (see the README's "Build & test" section). Useful
ctest labels: `-L core`, `-L kafka`, `-L postgres`, `-L clickhouse`,
`-L s3`, `-L rocksdb`, `-L tls`. The multi-process integration suite is
opt-in: configure with `-DCLINK_INTEGRATION_TESTS=ON` and run
`-L integration` serially.

## What a change needs

- Tests. Unit suites live in `tests/`; operator-level tests should use the
  public testing framework (`clink::test`, see
  [`docs/internals/testing-framework.md`](docs/internals/testing-framework.md)).
- A stable uid on every stateful operator (`.uid("...")` on the fluent
  API, `set_uid("...")` on a Dag-direct operator). State restore, rescale,
  and schema evolution key on it; a missing uid is a correctness bug.
- Docs updated in the same change: the affected `docs/connectors/<name>.md`
  or `docs/internals/<page>.md`, and the README if a capability changed.
- Formatting: a pre-commit hook (installed at configure time) runs
  clang-format; `cmake --build build --target format` fixes everything in
  place.
- British English in comments and docs. Plain commit messages in the
  existing style: `area: what changed` (e.g. `fix(net): ...`,
  `docs(internals): ...`).

## Pull requests

- Target `main`. The CI gate is the full build, the unit + SQL ctest
  suite, and the install/consume smoke.
- PRs from forks reuse the pinned toolchain image from the latest `main`
  build, so a PR that changes `docker/Dockerfile` or
  `scripts/versions.env` will not see its toolchain change take effect in
  CI until a maintainer lands it; say so in the PR description if your
  change touches those files.
- Contributions are accepted under the repository's licence (Apache-2.0);
  submitting a PR indicates agreement.
