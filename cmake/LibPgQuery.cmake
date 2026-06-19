# LibPgQuery.cmake
#
# Vendors pganalyze/libpg_query (the Postgres SQL parser carved out as a
# standalone C library). Upstream ships a Makefile, not a CMakeLists, so
# we drive the upstream build via ExternalProject_Add and re-expose the
# resulting libpg_query.a + headers as an IMPORTED static-library target
# named libpg_query::libpg_query.
#
# Pinned to a stable PG16-line tag for reproducibility. Updating requires
# revalidating that our parse-tree translation layer (include/clink/sql/
# parser.hpp) still compiles against the new release's struct layouts.

include(ExternalProject)
include(GNUInstallDirs)

set(_LIBPG_QUERY_TAG "16-5.2.0")
set(_LIBPG_QUERY_PREFIX "${CMAKE_BINARY_DIR}/_deps/libpg_query")
set(_LIBPG_QUERY_SOURCE_DIR "${_LIBPG_QUERY_PREFIX}/src/libpg_query_ext")
set(_LIBPG_QUERY_LIB "${_LIBPG_QUERY_SOURCE_DIR}/libpg_query.a")

# The IMPORTED target's INTERFACE_INCLUDE_DIRECTORIES must exist at
# configure time even though it's only populated post-build.
file(MAKE_DIRECTORY "${_LIBPG_QUERY_SOURCE_DIR}")

# macOS SDK 14+ (and glibc with _GNU_SOURCE) declares strchrnul in
# <string.h>; libpg_query's vendored snprintf.c provides its own static
# strchrnul guarded by #ifndef HAVE_STRCHRNUL. Define HAVE_STRCHRNUL so
# the parser picks up the platform symbol and skips its own copy.
# Without this the macOS build fails with "static declaration of
# 'strchrnul' follows non-static declaration" on snprintf.c. Same flag
# is harmless on Linux where glibc already provides strchrnul.
set(_LIBPG_QUERY_EXTRA_CFLAGS "-DHAVE_STRCHRNUL")

ExternalProject_Add(libpg_query_ext
    PREFIX "${_LIBPG_QUERY_PREFIX}"
    GIT_REPOSITORY https://github.com/pganalyze/libpg_query.git
    GIT_TAG ${_LIBPG_QUERY_TAG}
    GIT_SHALLOW TRUE
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make -j build CFLAGS=${_LIBPG_QUERY_EXTRA_CFLAGS}
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS "${_LIBPG_QUERY_LIB}"
    LOG_DOWNLOAD ON
    LOG_BUILD ON
)

add_library(libpg_query STATIC IMPORTED GLOBAL)
add_dependencies(libpg_query libpg_query_ext)
set_target_properties(libpg_query PROPERTIES
    IMPORTED_LOCATION "${_LIBPG_QUERY_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${_LIBPG_QUERY_SOURCE_DIR}"
)
add_library(libpg_query::libpg_query ALIAS libpg_query)

message(STATUS "clink: libpg_query vendored at tag ${_LIBPG_QUERY_TAG}")
