#!/usr/bin/env bash
# The reference library's live suite: python/clink-reference's tests against
# one Apache Kafka broker (apache/kafka:3.9.1, which reports topic ids) in
# Docker, on a free loopback port, removed when the script exits. The suite
# reads ranges holding committed and aborted transactions, expired history, a
# topic created again, a compacting topic and a bound, and requires each read
# to be complete or incomplete as it should.
#
#   scripts/reference-live.sh
#
# Needs Docker and Python 3.10+; installs the package and its client
# (confluent-kafka) into a virtual environment under WORK (default a new
# temporary directory). Exits 0 only when every case ran and passed.
set -euo pipefail
cd "$(dirname "$0")/.."
WORK="${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/clink-reference-live.XXXXXX")}"
name="clink-reference-kafka-$$"
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
cleanup() { docker rm -f "${name}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --rm --name "${name}" -p "127.0.0.1:${port}:${port}" \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e "KAFKA_LISTENERS=PLAINTEXT://:9092,HOST://:${port},CONTROLLER://:9093" \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,HOST://127.0.0.1:${port}" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    apache/kafka:3.9.1 >/dev/null
for _ in $(seq 1 90); do
    docker exec "${name}" /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
    sleep 1
done
python3 -m venv "${WORK}/venv"
"${WORK}/venv/bin/pip" install -q ./python/clink-reference "confluent-kafka==2.16.1"
export PYTHONDONTWRITEBYTECODE=1
CLINK_REFERENCE_KAFKA="127.0.0.1:${port}" "${WORK}/venv/bin/python" -m unittest discover \
    -s python/clink-reference/tests -v 2>&1 | tee "${WORK}/results.txt"
grep -q "^OK" "${WORK}/results.txt" && ! grep -q "skipped" "${WORK}/results.txt"
