cmake_minimum_required(VERSION 3.24)
project(clink-fetchcontent-consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Where clink comes from. Pin a release tag once one carries build-tree
# consumption: v0.8.0 predates it (its ankerl header is not a usage requirement
# of clink::core in the build tree, and its tests default on as a subproject),
# so until the next release this follows main. A commit hash also works, but
# needs GIT_SHALLOW dropped from the declaration below.
#
# To build this project against a checkout you already have (clink's own CI
# does this with the commit under test), point FetchContent at it:
#
#   cmake -S . -B build -DFETCHCONTENT_SOURCE_DIR_CLINK=/path/to/clink
#
# That skips the clone and uses the directory as the source. Nothing else in
# this file changes.
set(CLINK_GIT_REPOSITORY "https://github.com/orhaugh/clink.git"
    CACHE STRING "Git repository FetchContent clones clink from")
set(CLINK_GIT_TAG "main"
    CACHE STRING "clink tag or branch to build")

# clink's build options, set before the fetch so its option() calls take
# these values (CMP0077). This project builds the engine it links and nothing
# else:
#   - no connector impls, so configuring needs only Arrow, Parquet and
#     Threads. Set CLINK_BUILD_IMPLS back on and link clink::kafka and friends
#     to use a connector.
#   - no SQL frontend, which this program does not use.
# clink's own tests and in-tree examples are not listed: they default off when
# clink is not the top-level project. Set CLINK_BUILD_TESTS ON to build them.
set(CLINK_BUILD_IMPLS OFF)
set(CLINK_BUILD_SQL OFF)

include(FetchContent)
FetchContent_Declare(clink
    GIT_REPOSITORY "${CLINK_GIT_REPOSITORY}"
    GIT_TAG        "${CLINK_GIT_TAG}"
    GIT_SHALLOW    TRUE)
FetchContent_MakeAvailable(clink)

# From here on this is an ordinary project. clink::core is the same target
# the installed package exports, resolved from the build tree instead.
add_executable(fetched_pipeline fetched_pipeline.cpp)
target_link_libraries(fetched_pipeline PRIVATE clink::core)

# The program checks its own output and exits non-zero on a mismatch.
enable_testing()
add_test(NAME fetched_pipeline COMMAND fetched_pipeline)
