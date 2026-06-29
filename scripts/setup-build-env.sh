#!/usr/bin/env bash
# setup-build-env.sh
# One-shot, full clink build environment (back-compat entrypoint). Composes the two
# halves that docker/Dockerfile layers separately for build-cache efficiency:
#
#   install-system-deps.sh    - base toolchain + from-source toolchain (aws-sdk, Arrow,
#                               clickhouse-cpp, iceberg-cpp, cassandra). EXPENSIVE, but
#                               rarely invalidated.
#   install-connector-deps.sh - optional connector client apt packages + Pulsar .deb.
#                               CHEAP, changes whenever a connector is added.
#
# The Dockerfile calls the two halves directly (so adding a connector apt dep re-runs
# only the cheap layer). This wrapper preserves the historical single-command setup.
#
# Usage: ./setup-build-env.sh [BUILD_TYPE]   (BUILD_TYPE defaults to "Release")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"${SCRIPT_DIR}/install-system-deps.sh" "${1:-Release}"
"${SCRIPT_DIR}/install-connector-deps.sh"

echo "▶ Build env ready."
