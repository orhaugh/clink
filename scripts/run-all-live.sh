#!/usr/bin/env bash
#
# run-all-live.sh - the definitive connector LIVE integration run.
#
# Brings up the dockerised dependency services, builds clink with every
# connector + the multi-process integration suite in the pinned toolchain image,
# and runs the test suite with the LIVE tests ACTIVE - pointed at the services
# over the compose network - then tears the services down.
#
# The normal build/test run skips every live test (their endpoint env vars are
# unset); this script is how you get end-to-end confidence that the connectors
# actually work against real servers, not just that their logic compiles.
#
# Services wired so far (docker/integration-services.yml):
#   postgres, mysql, redis, cassandra, elasticsearch, aws/kinesis + s3
#   (localstack), pubsub. Kafka needs no service - it is tested against an
#   in-process librdkafka mock. Connectors without a service yet (mongodb, nats,
#   mqtt, pulsar, influxdb, gcs, azure, webhdfs, iceberg-REST, etcd) self-skip;
#   they will activate as their services are added here.
#
# Everything runs inside IMAGE (the pinned toolchain image) on the compose
# network, so it needs no host toolchain and reaches services by DNS name.
#
# Env knobs:
#   IMAGE=clink-build:latest   toolchain image to build + run in
#   BUILD_DIR=build-live       build dir (under the repo, which is mounted)
#   FULL=1                     run the WHOLE suite; default runs the connector live tests
#   WITH_SQL=1                 also build + test the SQL frontend
#   KEEP_SERVICES=1            leave the services up at the end
#   CTEST_TIMEOUT=300          per-test timeout (seconds)
#   LIVE_EXCLUDE=<regex>       override the default ctest -E exclusion (below)
#
# Default exclusions (LIVE_EXCLUDE): GatewayParity is a multi-process integration
# test falsely matched by the "Live" filter (its name contains "Liveness"), not a
# connector test; IcebergS3Live is under-gated (it activates on the S3 endpoint
# alone but needs an Iceberg REST catalog that is not wired here, and crashes
# rather than skipping) - drop both until Iceberg's catalog is added.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

COMPOSE="docker/integration-services.yml"
PROJECT="clink-live"
NETWORK="${PROJECT}_default"
IMAGE="${IMAGE:-clink-build:latest}"
BUILD_DIR="${BUILD_DIR:-build-live}"
CTEST_TIMEOUT="${CTEST_TIMEOUT:-300}"

if ! docker info >/dev/null 2>&1; then
  echo "▶ Docker is not running." >&2
  exit 1
fi

cleanup() {
  if [[ -z "${KEEP_SERVICES:-}" ]]; then
    echo "▶ Tearing down services..."
    docker compose -f "$COMPOSE" -p "$PROJECT" down -v >/dev/null 2>&1 || true
  else
    echo "▶ KEEP_SERVICES set - services left up."
    echo "  Stop them with: docker compose -f $COMPOSE -p $PROJECT down -v"
  fi
}
trap cleanup EXIT

echo "▶ Bringing up dependency services and waiting for healthchecks"
echo "  (Cassandra + Elasticsearch take ~30-60s to become healthy)..."
docker compose -f "$COMPOSE" -p "$PROJECT" up -d --wait

# Cassandra reports "healthy" (its native transport answers a SELECT on
# system.local) well before it can reliably serve DDL. The connector tests
# create a keyspace + table, so wait until a create/drop round-trip actually
# succeeds - otherwise a fast (cached) build races Cassandra's warm-up.
if docker compose -f "$COMPOSE" -p "$PROJECT" ps --services --filter status=running 2>/dev/null | grep -qx cassandra; then
  echo "▶ Waiting for Cassandra to accept DDL (healthy != ready)..."
  cass_ready=""
  for i in $(seq 1 40); do
    if docker compose -f "$COMPOSE" -p "$PROJECT" exec -T cassandra cqlsh -e \
         "CREATE KEYSPACE IF NOT EXISTS clink_probe WITH replication = {'class':'SimpleStrategy','replication_factor':1}; DROP KEYSPACE clink_probe" \
         >/dev/null 2>&1; then
      echo "  Cassandra ready (DDL round-trip ok after ${i} attempt(s))."
      cass_ready=1
      break
    fi
    sleep 3
  done
  [[ -z "$cass_ready" ]] && echo "  WARNING: Cassandra never accepted DDL in ~120s; its tests may fail."
fi

# Live-test env, pointing at the services by their compose DNS names (the test
# container joins the same network). LocalStack accepts the dummy AWS creds.
declare -a env_args=(
  -e "CLINK_POSTGRES_CDC_TEST_DSN=host=postgres-cdc port=5432 user=postgres password=postgres dbname=postgres"
  -e "CLINK_MYSQL_TEST_DSN=host=mysql port=3306 user=root password=mysql database=test"
  -e "CLINK_REDIS_TEST_URL=redis://redis:6379"
  -e "CLINK_CASSANDRA_TEST_CONTACT_POINTS=cassandra"
  -e "CLINK_ELASTICSEARCH_TEST_ENDPOINT=http://elasticsearch:9200"
  -e "CLINK_KINESIS_TEST_ENDPOINT=http://localstack:4566"
  -e "CLINK_S3_TEST_ENDPOINT=http://localstack:4566"
  -e "CLINK_S3_TEST_BUCKET=clink-live-test"
  -e "CLINK_PUBSUB_EMULATOR_HOST=pubsub:8085"
  -e "AWS_ACCESS_KEY_ID=test"
  -e "AWS_SECRET_ACCESS_KEY=test"
  -e "AWS_DEFAULT_REGION=us-east-1"
  -e "AWS_EC2_METADATA_DISABLED=true"
)

sql_flag="-DCLINK_BUILD_SQL=OFF"
[[ -n "${WITH_SQL:-}" ]] && sql_flag="-DCLINK_BUILD_SQL=ON"
# Default: the connector live tests. FULL=1 runs the whole suite.
ctest_filter="-R Live"
[[ -n "${FULL:-}" ]] && ctest_filter=""
exclude_re="${LIVE_EXCLUDE:-GatewayParity|IcebergS3Live}"

echo "▶ Building + running in $IMAGE on network $NETWORK (BUILD_DIR=$BUILD_DIR)"
rc=0
docker run --rm \
  --network "$NETWORK" \
  -v "$(pwd)":/workspace -w /workspace \
  -e IN_DOCKER=1 -e CLINK_DEPS_PREFIX=/usr/local \
  "${env_args[@]}" \
  "$IMAGE" bash -c "
    set -e
    cmake -S . -B '$BUILD_DIR' -DCLINK_BUILD_TESTS=ON -DCLINK_INTEGRATION_TESTS=ON $sql_flag
    cmake --build '$BUILD_DIR' -j\$(( \$(nproc) / 2 ))
    cd '$BUILD_DIR'
    ctest --output-on-failure --timeout $CTEST_TIMEOUT $ctest_filter -E '$exclude_re'
  " || rc=$?

echo ""
if [[ $rc -eq 0 ]]; then
  echo "✅ Live suite passed."
else
  echo "❌ Live suite reported failures (exit $rc). See the ctest output above."
fi
exit $rc
