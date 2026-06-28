#!/usr/bin/env bash
# sanitize_connectors_in_image.sh
# Runs INSIDE the clink-build:latest toolchain image (invoked by
# sanitize_connectors.sh). Builds the connector test executables under each
# sanitizer and runs them directly - so the endpoint-gated LIVE tests (pointed at
# the clink-san-* services on the shared docker network) execute UNDER the
# sanitizer, which the CI matrix cannot do without live endpoints.
#
# Canonical flags + suppression files come from build_and_test.sh / the repo root.
# Build dirs are container-local (/tmp) to avoid the macOS bind-mount penalty.
#
# Env:
#   CONNECTORS   space-separated connector names (default below). Each maps to the
#                target/exe clink_<name>_tests under impls/<name>/tests/.
#   SANITIZERS   space-separated subset of "asan ubsan tsan" (default all).
#
# NB: clink::aws is intentionally absent - clink-build:latest ships only the AWS
# SDK s3 component, not kinesis/firehose/dynamodb, so clink_aws_tests is not built
# there (its kinesis hardening is validated on the host).
set -uo pipefail
SRC=/workspace
JOBS=$(( $(nproc) / 2 )); [ "$JOBS" -lt 1 ] && JOBS=1
echo "nproc=$(nproc) -> jobs=$JOBS"

CONNECTORS="${CONNECTORS:-http_connector mysql redis postgres}"
SANITIZERS="${SANITIZERS:-asan ubsan tsan}"

# Live-service endpoints (the services are stood up by sanitize_connectors.sh on
# the clink-san-net network). Setting these makes the gated live tests run.
export CLINK_MYSQL_TEST_DSN="host=clink-san-mysql port=3306 user=root password=mysql database=test"
export CLINK_REDIS_TEST_URL="redis://clink-san-redis:6380"
export CLINK_REDIS_TLS_TEST_URL="clink-san-redis:6379"
export CLINK_POSTGRES_CDC_TEST_DSN="host=clink-san-pg port=5432 user=postgres password=postgres dbname=postgres"

TARGETS=""; EXES=""
for c in $CONNECTORS; do
    TARGETS="$TARGETS clink_${c}_tests"
    EXES="$EXES impls/${c}/tests/clink_${c}_tests"
done

wait_port() {
    for _ in $(seq 1 90); do
        (echo > "/dev/tcp/$1/$2") 2>/dev/null && { echo "$1:$2 open"; return 0; }
        sleep 2
    done
    echo "$1:$2 NOT open"; return 1
}
wait_port clink-san-mysql 3306 || true
wait_port clink-san-redis 6380 || true
wait_port clink-san-redis 6379 || true
wait_port clink-san-pg 5432 || true
sleep 8  # service first-boot grace (mysql especially)

declare -A RESULT
run_one() {
    local san=$1
    local bd="/tmp/build-$san"
    local -a F
    local E
    case $san in
        asan) F=(-DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer"
                 -DCMAKE_C_FLAGS="-fsanitize=address -fno-omit-frame-pointer"
                 -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address")
              E="ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 LSAN_OPTIONS=suppressions=$SRC/lsan-suppressions.txt" ;;
        ubsan) F=(-DCMAKE_CXX_FLAGS="-fsanitize=undefined -fno-omit-frame-pointer"
                  -DCMAKE_C_FLAGS="-fsanitize=undefined -fno-omit-frame-pointer"
                  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=undefined")
              E="UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1:suppressions=$SRC/ubsan-suppressions.txt" ;;
        tsan) F=(-DCMAKE_CXX_FLAGS="-fsanitize=thread"
                 -DCMAKE_C_FLAGS="-fsanitize=thread"
                 -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread")
              E="TSAN_OPTIONS=halt_on_error=1:suppressions=$SRC/tsan-suppressions.txt" ;;
        *) echo "unknown sanitizer '$san'"; RESULT[$san]="BAD_SAN"; return ;;
    esac
    echo; echo "########## $san : configure ##########"
    if ! cmake -S "$SRC" -B "$bd" -DCMAKE_BUILD_TYPE=Debug -DCLINK_BUILD_TESTS=ON \
            -DCLINK_BUILD_SQL=ON "${F[@]}" >/tmp/$san-cfg.log 2>&1; then
        echo "$san CONFIG FAILED"; tail -25 /tmp/$san-cfg.log; RESULT[$san]="CONFIG_FAIL"; return
    fi
    echo "########## $san : build ($TARGETS, j=$JOBS) ##########"
    if ! cmake --build "$bd" --target $TARGETS -- -j"$JOBS" >/tmp/$san-build.log 2>&1; then
        echo "$san BUILD FAILED"; tail -60 /tmp/$san-build.log; RESULT[$san]="BUILD_FAIL"; return
    fi
    echo "########## $san : run exes (live tests included) ##########"
    local ok=1
    for e in $EXES; do
        local log="/tmp/$san-$(basename "$e").log"
        if ( cd "$bd" && timeout 900 env $E "./$e" >"$log" 2>&1 ); then
            echo "  PASS $e :: $(grep -E '\[==========\] .* ran|\[  PASSED|\[  SKIPPED \] [0-9]' "$log" | tail -2 | tr '\n' '|')"
        else
            ok=0
            echo "  FAIL $e (sanitizer/test failure):"
            grep -iE "runtime error|ERROR: AddressSanitizer|ERROR: ThreadSanitizer|SUMMARY: |data race|detected memory leaks|direct leak|\[  FAILED" "$log" | head -25
            echo "  --- tail ---"; tail -15 "$log"
        fi
    done
    RESULT[$san]=$([ $ok -eq 1 ] && echo PASS || echo "FAIL")
    rm -rf "$bd"
}

for s in $SANITIZERS; do run_one "$s"; done

echo; echo "============ SANITIZER SUMMARY (connectors) ============"
rc=0
for s in $SANITIZERS; do
    printf "  %-6s %s\n" "$s" "${RESULT[$s]:-SKIPPED}"
    [ "${RESULT[$s]:-}" = "PASS" ] || rc=1
done
echo "========================================================"
exit $rc
