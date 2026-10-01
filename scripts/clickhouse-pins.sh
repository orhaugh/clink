#!/usr/bin/env bash
# Run the native ClickHouse sink's server pins against every supported line.
#
#   scripts/clickhouse-pins.sh                # every line, build/ binaries
#   CLICKHOUSE_LINES="26.8" scripts/clickhouse-pins.sh   # one line
#
# For each line it brings up the line's two services from
# docker/integration-services.yml (one with an embedded Keeper, one with a
# server-wide <merge_tree> async_insert default), runs ClickHousePins.* against
# each, and tears them down. A pin that fails fails the run: the design takes
# that behaviour as given (docs/connectors/clickhouse.md lists what each pin is
# answerable to). BUILD_DIR selects the build (default build/). PIN_RUNNER, when
# set, prefixes the test binary's command (CI runs it inside the toolchain image
# with `docker run ... -e CLINK_CLICKHOUSE_TEST_HOST ...`, so the variables this
# script sets on the command reach the container).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/docker/integration-services.yml")
BUILD_DIR="${BUILD_DIR:-${ROOT}/build}"
CLICKHOUSE_LINES="${CLICKHOUSE_LINES:-26.3 26.8}"
BIN="$(find "${BUILD_DIR}" -name clink_clickhouse_tests -type f -perm -u+x | head -1)"
if [ -z "${BIN}" ]; then
    echo "clickhouse-pins: clink_clickhouse_tests is not built under ${BUILD_DIR}" >&2
    exit 1
fi

status=0
for line in ${CLICKHOUSE_LINES}; do
    tag="${line//./-}"
    minor="${line#*.}"
    svc="clickhouse-${tag}"
    async_svc="clickhouse-${tag}-async-default"
    port="191$(printf '%02d' "${minor}")"
    async_port="191$((10 + minor))"
    echo "clickhouse-pins: ${line}"
    "${COMPOSE[@]}" up -d --wait "${svc}" "${async_svc}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    if ! CLINK_CLICKHOUSE_TEST_HOST=localhost CLINK_CLICKHOUSE_TEST_PORT="${port}" \
        ${PIN_RUNNER:-} "${BIN}" --gtest_filter='ClickHousePins.*:-ClickHousePins.P6AServerWide*'; then
        status=1
    fi
    # shellcheck disable=SC2086
    if ! CLINK_CLICKHOUSE_TEST_HOST=localhost CLINK_CLICKHOUSE_TEST_PORT="${async_port}" \
        CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT=1 \
        ${PIN_RUNNER:-} "${BIN}" --gtest_filter='ClickHousePins.P6AServerWide*'; then
        status=1
    fi
    "${COMPOSE[@]}" rm -sf "${svc}" "${async_svc}" >/dev/null
done
exit "${status}"
