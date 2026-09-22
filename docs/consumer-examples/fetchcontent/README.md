# Consuming clink from source with FetchContent

The examples one directory up consume an installed clink through
`find_package(clink)`. This project is the other route: it pulls clink's source
from GitHub at a pinned tag with CMake's `FetchContent`, builds the engine
inside its own build tree and links it as `clink::core`, the same target the
installed package exports. Nothing needs installing first, and the consumer
chooses clink's build options.

It is a complete project to copy from: [`CMakeLists.txt`](CMakeLists.txt) is
the integration, and [`fetched_pipeline.cpp`](fetched_pipeline.cpp) is an
ordinary self-checking pipeline (bounded source, key by customer, one-second
tumbling window summing amounts, collecting sink) that proves the linked
engine runs. It registers with CTest and exits non-zero if the window totals
differ from the input.

## Prerequisites

The same as building clink itself: a C++23 compiler, CMake 3.24 or newer, and
the pinned Arrow and Parquet under `CLINK_DEPS_PREFIX` (host default
`~/.clink-deps`, bootstrapped once with `scripts/build-arrow.sh` from a clink
checkout). clink's CMake finds that prefix on its own. No connector SDKs are
needed, because this project builds clink with `CLINK_BUILD_IMPLS=OFF`.

## Build and run

```bash
cd docs/consumer-examples/fetchcontent
cmake -S . -B build                      # clones clink at the pinned tag
cmake --build build --parallel 10        # builds clink::core, then the program
ctest --test-dir build --output-on-failure
```

The first configure clones clink and the third-party sources clink fetches
itself. The build compiles the engine, so expect it to take as long as a
core-only clink build.

## Choosing what to build

Two cache variables select the source, and two ordinary variables set before
the fetch shape clink's build:

| Setting | Default | Meaning |
|---------|---------|---------|
| `CLINK_GIT_REPOSITORY` | `https://github.com/orhaugh/clink.git` | Repository FetchContent clones |
| `CLINK_GIT_TAG` | `main` | Tag or branch to build. Pin a release tag once one carries build-tree consumption; v0.8.0 predates it. A commit hash works once `GIT_SHALLOW` is dropped from the declaration |
| `CLINK_BUILD_IMPLS` | `OFF` | Connector and backend impls. Set it on and link `clink::kafka` and friends to use one |
| `CLINK_BUILD_SQL` | `OFF` | The SQL frontend |

clink's own tests and in-tree examples are off by default when clink is not
the top-level project, so this project does not mention them. Set
`CLINK_BUILD_TESTS` to `ON` before the fetch to build them too.

To build against a clink checkout you already have, rather than cloning, point
FetchContent at it. clink's CI does exactly this with the commit under test:

```bash
cmake -S . -B build -DFETCHCONTENT_SOURCE_DIR_CLINK=/path/to/clink
```

## When to prefer find_package

Fetching from source suits a single application that wants one build, one
toolchain and a pinned clink. Several projects on one machine, or a runtime
image that ships the `clink` CLI and `clink_node`, are better served by
installing clink once and consuming it with `find_package(clink)`, as the
[consumer examples](../README.md) do.
