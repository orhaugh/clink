#!/usr/bin/env bash
# The live gate for the brokers clink reads and writes: Redis, Pub/Sub,
# RabbitMQ, NATS JetStream, Pulsar and MQTT. Each broker's phases run against
# its service of docker/integration-services.yml, one broker at a time, then a
# check that nothing was skipped and that every required suite ran in full.
#
#   scripts/brokers-live.sh [build-dir]
#
# Each service runs as a one-off container of its compose service, published on
# free loopback ports, so a broker already on the machine is left alone; the
# container goes, with its volumes, when its phases end or the script exits.
# BROKERS_LIVE_PHASES names the phases each broker runs:
#
#   sources: the source's round trip (RedisLive, PubSubLive, RabbitMqLive,
#     NatsLive, PulsarLive, MqttLive) and its acknowledgement contract
#     (<Connector>SourceAckLive): an aborted checkpoint, crashes before and
#     after a completion, notifications out of order, close after a cancel, the
#     bound on what is held, and per connector the leases, a failed
#     acknowledgement and a scale-down. For Redis also the source's offline
#     suites. The Redis and MQTT TLS round trips need a TLS server this gate
#     does not start and are left out.
#   sinks: the sinks' suites, offline and live, where a broker's sinks have
#     them, today Redis and RabbitMQ: the failure state against a scripted
#     server, the contract against the server, the factories (and for Redis
#     the key rules and the upsert contract suite); then the recovery cells in
#     clink_integration_tests (RedisSinkRecovery, RabbitMqSinkRecovery): a
#     coordinator and two workers polling a feed the test serves into each
#     sink, one of them a SQL job at parallelism 2, with a worker killed before
#     a flush (or its confirms) and after it, before its checkpoint completes.
#     A broker without a sinks phase says so in provenance.txt.
#
# A live case skips when its service is not named and googletest counts that
# as a pass, so scripts/check-gtest-reports.py fails the run on any skip and on
# a suite that ran fewer cases than it should. When both phases run for a
# broker that has both, the cases its binary ran between them must be every
# case it lists apart from those left out on purpose (the Redis TLS cases), so
# a suite outside both phases fails the gate. A missing binary, a service that
# never answers and a failed case fail it too.
#
# REPORT_DIR keeps each broker's reports and output under <broker>/<phase>,
# provenance.txt (the commit, whether the tree was clean, the build's
# configuration, each broker's image and each phase's result) and, on failure,
# the broker's log. Nothing records the environment, so no credential reaches
# it; the test brokers' are fixed test values anyway. This is what the brokers
# CI workflow runs.
#
# Knobs:
#   BUILD_DIR              the build to take the binaries from (argument or
#                          default build/)
#   REPORT_DIR             where the reports and, on failure, the service logs
#                          go (default <build-dir>/brokers-live-reports)
#   BROKERS_LIVE_ONLY      the brokers to run, of "redis pubsub rabbitmq nats
#                          pulsar mqtt" (default all)
#   BROKERS_LIVE_PHASES    the phases to run, of "sources sinks" (default both)
#   BROKERS_LIVE_RECOVERY  1 (default) or 0, which leaves the recovery cells out
#   PIN_RUNNER             a prefix for every test command (CI runs the
#                          binaries inside the toolchain image with `docker run
#                          --network host ... -e <each variable below>`)
#
# What the tests read, all set here: CLINK_REDIS_TEST_URL,
# CLINK_REDIS_RECOVERY_LOG_DIR, CLINK_PUBSUB_EMULATOR_HOST,
# CLINK_RABBITMQ_TEST_ENDPOINT, CLINK_RABBITMQ_TEST_PORT,
# CLINK_RABBITMQ_TEST_USER, CLINK_RABBITMQ_TEST_PASSWORD,
# CLINK_RABBITMQ_TEST_MANAGEMENT_PORT, CLINK_RABBITMQ_RECOVERY_LOG_DIR,
# CLINK_NATS_TEST_ENDPOINT, CLINK_PULSAR_TEST_ENDPOINT, CLINK_MQTT_TEST_URL.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/brokers-live-reports}"
ALL_BROKERS="redis pubsub rabbitmq nats pulsar mqtt"
BROKERS_LIVE_ONLY="${BROKERS_LIVE_ONLY:-${ALL_BROKERS}}"
BROKERS_LIVE_PHASES="${BROKERS_LIVE_PHASES:-sources sinks}"
BROKERS_LIVE_RECOVERY="${BROKERS_LIVE_RECOVERY:-1}"
PROJECT=clink-brokers-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")

for b in ${BROKERS_LIVE_ONLY}; do
    if [[ " ${ALL_BROKERS} " != *" ${b} "* ]]; then
        echo "brokers-live: no broker '${b}' (choose from ${ALL_BROKERS})" >&2
        exit 1
    fi
done
for p in ${BROKERS_LIVE_PHASES}; do
    if [ "${p}" != sources ] && [ "${p}" != sinks ]; then
        echo "brokers-live: no phase '${p}' (choose from sources sinks)" >&2
        exit 1
    fi
done
runs() {  # <broker>
    [[ " ${BROKERS_LIVE_ONLY} " == *" $1 "* ]]
}
phase_runs() {  # <phase>
    [[ " ${BROKERS_LIVE_PHASES} " == *" $1 "* ]]
}

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
binary_of() {  # <broker>
    case "$1" in
        redis) find_bin clink_redis_tests ;;
        pubsub) find_bin clink_http_connector_tests ;;
        rabbitmq) find_bin clink_rabbitmq_tests ;;
        nats) find_bin clink_nats_tests ;;
        pulsar) find_bin clink_pulsar_tests ;;
        mqtt) find_bin clink_mqtt_tests ;;
    esac
}

# Each broker's cases in its test binary, per phase, as a googletest filter,
# and the passing cases each suite must count. A broker with no filter for a
# phase has no such phase yet.
filter_of() {  # <broker> <phase>
    case "$1/$2" in
        redis/sources) echo 'RedisLive.*:RedisSourceAckLive.*:RedisSourceLogic.*:SourceCapabilityRecord.*:-RedisLive.TlsConnectionRoundTrip' ;;
        redis/sinks) echo 'RedisSink*:RedisUpsert*:RedisFactoryRegistration.*' ;;
        pubsub/sources) echo 'PubSubLive.*:PubSubSourceAckLive.*' ;;
        rabbitmq/sources) echo 'RabbitMqLive.*:RabbitMqSourceAckLive.*:RabbitMqSource.*' ;;
        rabbitmq/sinks) echo 'RabbitMqSink*:RabbitMqFactoryRegistration.*' ;;
        nats/sources) echo 'NatsLive.*:NatsSourceAckLive.*' ;;
        pulsar/sources) echo 'PulsarLive.*:PulsarSourceAckLive.*' ;;
        mqtt/sources) echo 'MqttLive.*:MqttSourceAckLive.*:-MqttLive.TlsConnectionRoundTrip' ;;
    esac
}
expectations_of() {  # <broker> <phase>
    case "$1/$2" in
        redis/sources)
            echo 'RedisLive.*=5'
            echo 'RedisSourceAckLive.*=9'
            echo 'RedisSourceLogic.*=7'
            echo 'SourceCapabilityRecord.*=2'
            ;;
        redis/sinks)
            echo 'RedisSinkRules.*=7'
            echo 'RedisSinkLogic.*=4'
            echo 'RedisSinkFailures.*=11'
            echo 'RedisUpsertSinkFailures.*=6'
            echo 'RedisSinkContractLive.*=10'
            echo 'RedisUpsertSink.*=1'
            echo 'RedisUpsertSinkLive.*=4'
            echo 'RedisUpsert/UpsertContractSuite/0.*=5'
            echo 'RedisFactoryRegistration.*=6'
            ;;
        pubsub/sources)
            echo 'PubSubLive.*=1'
            echo 'PubSubSourceAckLive.*=3'
            ;;
        rabbitmq/sources)
            echo 'RabbitMqLive.*=3'
            echo 'RabbitMqSourceAckLive.*=7'
            echo 'RabbitMqSource.*=2'
            ;;
        rabbitmq/sinks)
            echo 'RabbitMqSink.*=2'
            echo 'RabbitMqSinkFailures.*=13'
            echo 'RabbitMqSinkContractLive.*=11'
            echo 'RabbitMqFactoryRegistration.*=3'
            ;;
        nats/sources)
            echo 'NatsLive.*=2'
            echo 'NatsSourceAckLive.*=6'
            ;;
        pulsar/sources)
            echo 'PulsarLive.*=2'
            echo 'PulsarSourceAckLive.*=4'
            ;;
        mqtt/sources)
            echo 'MqttLive.*=5'
            echo 'MqttSourceAckLive.*=8'
            ;;
    esac
}
recovery_filter_of() {  # <broker> <phase>
    case "$1/$2" in
        redis/sinks) echo 'RedisSinkRecovery.*' ;;
        rabbitmq/sinks) echo 'RabbitMqSinkRecovery.*' ;;
    esac
}
recovery_expectations_of() {  # <broker> <phase>
    case "$1/$2" in
        redis/sinks) echo 'RedisSinkRecovery.*=5' ;;
        rabbitmq/sinks) echo 'RabbitMqSinkRecovery.*=3' ;;
    esac
}
# Where each broker's recovery cells keep the cluster's logs.
recovery_log_var_of() {  # <broker>
    case "$1" in
        redis) echo CLINK_REDIS_RECOVERY_LOG_DIR ;;
        rabbitmq) echo CLINK_RABBITMQ_RECOVERY_LOG_DIR ;;
    esac
}
# The cases the phases leave out on purpose, which the coverage check allows:
# the TLS cases, which need a TLS-enabled build and server (ctest runs the
# certificate cases in every TLS-enabled build).
excluded_of() {  # <broker>
    case "$1" in
        redis) echo 'RedisTls.*:RedisLive.TlsConnectionRoundTrip' ;;
    esac
}

IT_BIN="$(find_bin clink_integration_tests)"
missing=()
for b in ${BROKERS_LIVE_ONLY}; do
    if [ -z "$(binary_of "${b}")" ]; then
        missing+=("${b}")
    fi
done
if [ "${#missing[@]}" -gt 0 ]; then
    echo "brokers-live: the test binaries of ${missing[*]} are not built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${BROKERS_LIVE_RECOVERY}" = 1 ] && phase_runs sinks && { runs redis || runs rabbitmq; } &&
    [ -z "${IT_BIN}" ]; then
    echo "brokers-live: the recovery cells need clink_integration_tests (and clink_node and" \
        "clink_submit_sql) under ${BUILD_DIR}; BROKERS_LIVE_RECOVERY=0 leaves them out" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
for b in ${ALL_BROKERS}; do
    rm -rf "${REPORT_DIR:?}/${b}"
done
rm -f "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/provenance.txt"
# The toolchain image's user is not the runner's; let either write here.
chmod a+rwx "${REPORT_DIR}"
{
    # CI runs this as another user than the checkout's owner; safe.directory
    # lets git read it there.
    echo "commit: $(git -c safe.directory='*' -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -c safe.directory='*' -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_REDIS CLINK_WITH_HTTP CLINK_WITH_RABBITMQ \
        CLINK_WITH_NATS CLINK_WITH_PULSAR CLINK_WITH_MQTT CLINK_BUILD_SQL \
        CLINK_INTEGRATION_TESTS CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "brokers: ${BROKERS_LIVE_ONLY}"
    echo "phases: ${BROKERS_LIVE_PHASES}"
    echo "recovery cells: $( [ "${BROKERS_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

status=0
container=""
stop() {
    if [ -n "${container}" ]; then
        if [ "${status}" -ne 0 ]; then
            docker logs "${container}" >"${REPORT_DIR}/${container}.log" 2>&1 || true
        fi
        docker rm -fv "${container}" >/dev/null 2>&1 || true
        container=""
    fi
}
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    stop
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

start() {  # <compose service> <container port>...
    local service="$1"
    shift
    container="${PROJECT}-${service}"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    local publish=()
    for p in "$@"; do
        publish+=(-p "127.0.0.1::${p}")
    done
    "${COMPOSE[@]}" run -d --name "${container}" "${publish[@]}" "${service}" >/dev/null
    echo "${service}: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")" \
        >>"${REPORT_DIR}/provenance.txt"
}

port_of() {  # <container port>
    docker port "${container}" "$1/tcp" | head -1 | sed 's/.*://'
}

await() {  # <what> <seconds> <command...>
    local what="$1" seconds="$2"
    shift 2
    for _ in $(seq 1 "${seconds}"); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "brokers-live: ${what} never became ready" >&2
    status=1
    exit 1
}

# shellcheck disable=SC2329 # called through await
http_ok() {  # <url>
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$1" || true)" = "200" ]
}

# shellcheck disable=SC2329 # called through await
http_any() {  # <url>
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' "$1" || true)"
    [ -n "${code}" ] && [ "${code}" != "000" ]
}

failed=0
run() {  # <dir> <report name> <binary> [gtest args...]
    local dir="$1" name="$2" bin="$3"
    shift 3
    echo "==> ${dir#"${REPORT_DIR}"/} ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_brief=1 --gtest_output="json:${dir}/${name}.json" "$@" \
        2>&1 | tee "${dir}/${name}.log" || failed=1
}

# The cases a report holds.
cases_in() {
    python3 -c 'import json, sys
print(sum(len(s.get("testsuite", [])) for s in json.load(open(sys.argv[1])).get("testsuites", [])))' "$1"
}

# Runs the phases of the broker whose service is up.
phases() {  # <broker>
    local broker="$1" bin
    bin="$(binary_of "${broker}")"
    for phase in ${BROKERS_LIVE_PHASES}; do
        local filter dir
        filter="$(filter_of "${broker}" "${phase}")"
        if [ -z "${filter}" ]; then
            echo "  ${broker} ${phase}: no such phase yet" >>"${REPORT_DIR}/provenance.txt"
            continue
        fi
        dir="${REPORT_DIR}/${broker}/${phase}"
        mkdir -p "${dir}"
        chmod a+rwx "${REPORT_DIR}/${broker}" "${dir}"
        failed=0
        run "${dir}" tests "${bin}" --gtest_filter="${filter}"
        local reports=("${dir}/tests.json") expect=()
        while read -r e; do
            expect+=(--expect "${e}")
        done < <(expectations_of "${broker}" "${phase}")
        local cells
        cells="$(recovery_filter_of "${broker}" "${phase}")"
        if [ -n "${cells}" ] && [ "${BROKERS_LIVE_RECOVERY}" = 1 ]; then
            local logs="${dir}/recovery-logs"
            mkdir -p "${logs}"
            chmod a+rwx "${logs}"
            export "$(recovery_log_var_of "${broker}")=${logs}"
            run "${dir}" recovery "${IT_BIN}" --gtest_filter="${cells}"
            reports+=("${dir}/recovery.json")
            while read -r e; do
                expect+=(--expect "${e}")
            done < <(recovery_expectations_of "${broker}" "${phase}")
        fi
        python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || failed=1
        echo "  ${broker} ${phase}: $( [ "${failed}" = 0 ] && echo pass || echo fail)" \
            >>"${REPORT_DIR}/provenance.txt"
        if [ "${failed}" != 0 ]; then
            status=1
        fi
    done
    # Between them the phases ran every case the binary has, bar the
    # exclusions.
    if [ -n "$(filter_of "${broker}" sources)" ] && [ -n "$(filter_of "${broker}" sinks)" ] &&
        phase_runs sources && phase_runs sinks; then
        local excluded listed left=0 ran
        excluded="$(excluded_of "${broker}")"
        # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
        listed="$(${PIN_RUNNER:-} "${bin}" --gtest_list_tests | grep -c '^  ' || true)"
        if [ -n "${excluded}" ]; then
            # shellcheck disable=SC2086
            left="$(${PIN_RUNNER:-} "${bin}" --gtest_list_tests --gtest_filter="${excluded}" |
                grep -c '^  ' || true)"
        fi
        ran=$(( $(cases_in "${REPORT_DIR}/${broker}/sources/tests.json" || echo 0) + \
                $(cases_in "${REPORT_DIR}/${broker}/sinks/tests.json" || echo 0) ))
        echo "  ${broker} cases: ${listed} listed, ${left} left out on purpose, ${ran} run by" \
            "the phases" >>"${REPORT_DIR}/provenance.txt"
        if [ "$(( listed - left ))" != "${ran}" ]; then
            echo "brokers-live: ${bin##*/} lists ${listed} cases, ${left} left out on purpose," \
                "and the phases ran ${ran}: a suite is outside both phases' filters" >&2
            status=1
        fi
    fi
}

if runs redis; then
    start redis 6379
    await redis 60 sh -c "docker exec '${container}' redis-cli ping | grep -q PONG"
    port="$(port_of 6379)"
    export CLINK_REDIS_TEST_URL="redis://127.0.0.1:${port}"
    phases redis
    stop
fi

if runs pubsub; then
    start pubsub 8085
    await "the Pub/Sub emulator" 180 http_any "http://127.0.0.1:$(port_of 8085)/"
    port="$(port_of 8085)"
    export CLINK_PUBSUB_EMULATOR_HOST="127.0.0.1:${port}"
    phases pubsub
    stop
fi

if runs rabbitmq; then
    start rabbitmq 5672 15672
    # Asked only once the server has written its Erlang cookie: a CLI that finds
    # none writes one as root, which the server cannot read, and it stops. The
    # listener opens after the node answers a ping, so its port is asked too.
    await rabbitmq 120 docker exec "${container}" sh -c \
        '[ -f /var/lib/rabbitmq/.erlang.cookie ] && rabbitmq-diagnostics -q check_port_connectivity'
    port="$(port_of 5672)"
    export CLINK_RABBITMQ_TEST_ENDPOINT=127.0.0.1 CLINK_RABBITMQ_TEST_PORT="${port}"
    export CLINK_RABBITMQ_TEST_USER=clink CLINK_RABBITMQ_TEST_PASSWORD=clink
    port="$(port_of 15672)"
    export CLINK_RABBITMQ_TEST_MANAGEMENT_PORT="${port}"
    phases rabbitmq
    stop
fi

if runs nats; then
    start nats 4222 8222
    await nats 60 http_ok "http://127.0.0.1:$(port_of 8222)/healthz?js-enabled-only=true"
    port="$(port_of 4222)"
    export CLINK_NATS_TEST_ENDPOINT="nats://127.0.0.1:${port}"
    phases nats
    stop
fi

if runs pulsar; then
    start pulsar 6650 8080
    await pulsar 240 http_ok "http://127.0.0.1:$(port_of 8080)/admin/v2/brokers/health"
    port="$(port_of 6650)"
    export CLINK_PULSAR_TEST_ENDPOINT="pulsar://127.0.0.1:${port}"
    phases pulsar
    stop
fi

if runs mqtt; then
    start mqtt 1883
    await mqtt 60 docker exec "${container}" mosquitto_sub -h 127.0.0.1 -t clink/ready -E -W 2
    port="$(port_of 1883)"
    export CLINK_MQTT_TEST_URL="mqtt://127.0.0.1:${port}"
    phases mqtt
    stop
fi

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
