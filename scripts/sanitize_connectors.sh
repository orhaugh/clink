#!/usr/bin/env bash
# sanitize_connectors.sh
# Full ASan/UBSan/TSan suite over the live-service connectors, run inside the
# clink-build:latest toolchain image with real MySQL/Redis/Postgres networked in
# so the endpoint-gated LIVE tests run UNDER each sanitizer - coverage the CI
# matrix lacks (it has no live endpoints). Builds the host source (mounted at
# /workspace), so no image rebuild is needed for a code-only run.
#
# Stands up: binlog+TLS mysql:8.0, TLS+plain redis:7 (self-signed cert), and a
# logical-replication postgres:16, on a private docker network; tears them down
# on exit. The default connector set is http_connector/mysql/redis/postgres
# (clink::aws is excluded - the image ships only the AWS SDK s3 component).
#
# Usage:  scripts/sanitize_connectors.sh
# Env:
#   CLINK_IMAGE     toolchain image (default clink-build:latest)
#   SKIP_SERVICES=1 reuse already-running clink-san-* services (no standup/teardown)
#   CONNECTORS / SANITIZERS   forwarded to the in-image runner (see that script)
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${CLINK_IMAGE:-clink-build:latest}"
NET=clink-san-net
CERTS=""

teardown() {
    if [ "${SKIP_SERVICES:-0}" != "1" ]; then
        docker rm -f clink-san-mysql clink-san-redis clink-san-pg >/dev/null 2>&1 || true
        docker network rm "$NET" >/dev/null 2>&1 || true
    fi
    [ -n "$CERTS" ] && rm -rf "$CERTS"
}
trap teardown EXIT

if [ "${SKIP_SERVICES:-0}" != "1" ]; then
    CERTS="$(mktemp -d)"
    openssl req -x509 -newkey rsa:2048 -keyout "$CERTS/redis.key" -out "$CERTS/redis.crt" \
        -days 2 -nodes -subj "/CN=clink-san-redis" >/dev/null 2>&1
    chmod 644 "$CERTS/redis.key" "$CERTS/redis.crt"

    docker network create "$NET" >/dev/null 2>&1 || true
    docker rm -f clink-san-mysql clink-san-redis clink-san-pg >/dev/null 2>&1 || true

    docker run -d --name clink-san-mysql --network "$NET" \
        -e MYSQL_ROOT_PASSWORD=mysql -e MYSQL_DATABASE=test mysql:8.0 \
        --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL >/dev/null
    docker run -d --name clink-san-redis --network "$NET" -v "$CERTS":/certs:ro redis:7 \
        redis-server --tls-port 6379 --port 6380 \
        --tls-cert-file /certs/redis.crt --tls-key-file /certs/redis.key \
        --tls-ca-cert-file /certs/redis.crt --tls-auth-clients no >/dev/null
    docker run -d --name clink-san-pg --network "$NET" -e POSTGRES_PASSWORD=postgres postgres:16 \
        -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 >/dev/null

    echo "waiting for mysql init..."
    for i in $(seq 1 60); do
        if docker exec clink-san-mysql mysqladmin ping -h127.0.0.1 -uroot -pmysql >/dev/null 2>&1; then
            echo "mysql up (~$((i * 3))s)"; break
        fi
        sleep 3
    done
fi

echo "=== running sanitizer suite in $IMAGE ==="
docker run --rm --network "$NET" -v "$REPO":/workspace \
    -e "CONNECTORS=${CONNECTORS:-}" -e "SANITIZERS=${SANITIZERS:-}" \
    "$IMAGE" bash /workspace/scripts/sanitize_connectors_in_image.sh
