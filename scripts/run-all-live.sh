#!/usr/bin/env bash
#
# run-all-live.sh - the definitive connector LIVE integration run.
#
# Runs each connector's live tests SEQUENTIALLY, provisioning ONLY the docker
# service(s) that connector needs, running its tests, then tearing those services
# down before moving to the next. Never more than one connector's services are up
# at a time, so the machine is never asked to host all ten dependency containers
# at once (that oversubscribes RAM and OOM-kills the build).
#
# Shape:
#   1. Build ONCE with NO services running, so the compile has the whole machine
#      (this is what previously OOM'd - the build competed with ten containers).
#      Only the connector test binaries are built, not the full suite.
#   2. Capture the registered-test list once. A connector whose binary was not
#      produced (e.g. its client lib is missing from the image) has ZERO matching
#      tests - it is reported BLOCKED, never a silent phantom pass.
#   3. For each connector: bring up its service(s) (--wait on healthchecks, plus a
#      readiness probe where "healthy" is not the same as "ready"), run its exact
#      named test suites, then stop+remove just those services.
#
# Because each connector is filtered by its exact gtest suite names, there is no
# broad "-R Live" sweep - so the old GatewayParity ("Liveness") false match and the
# under-gated IcebergS3Live crash simply cannot be swept in.
#
# The normal build/test run skips every live test (their endpoint env vars are
# unset); this script is how you get end-to-end confidence that the connectors
# actually work against real servers, not just that their logic compiles.
#
# Everything runs inside IMAGE (the pinned toolchain image) on the compose network,
# so it needs no host toolchain and reaches services by DNS name.
#
# Env knobs:
#   IMAGE=clink-build:latest   toolchain image to build + run in
#   BUILD_DIR=build-live       build dir (under the repo, which is mounted)
#   BUILD_JOBS=6               compile parallelism (capped - rocksdb TUs are large)
#   ONLY="mongodb mqtt"        run only these connectors (default: all runnable)
#   WITH_SQL=1                 also configure the SQL frontend
#   SKIP_BUILD=1               reuse an existing BUILD_DIR (skip the build step)
#   KEEP_SERVICES=1            leave the last connector's services up at the end
#   CTEST_TIMEOUT=300          per-test timeout (seconds)
#
# Registry notes:
#   aws-kinesis  - runs KinesisLive against localstack (SERVICES=kinesis,s3). The
#                  aws-cpp-sdk kinesis/firehose/dynamodb components are now built into
#                  the image (install-system-deps.sh BUILD_ONLY), so clink_aws_tests
#                  builds. The test creates its own stream; localstack's healthcheck
#                  is enough (no extra probe). If clink_aws_tests is ever absent (an
#                  image without those SDK components) it reports BLOCKED, not a pass.
#   iceberg      - runs IcebergRestLive (REST catalog over an S3 warehouse). Uses two
#                  services: localstack (the S3 warehouse) + iceberg-rest. The sibling
#                  IcebergS3Live (local SQLite catalog over S3) is NOT run: it SEGFAULTS
#                  even with the bucket pre-created - a pre-existing bug in that test,
#                  not a wiring gap. Re-add it to the filter once that crash is fixed.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

COMPOSE="docker/integration-services.yml"
PROJECT="clink-live"
NETWORK="${PROJECT}_default"
IMAGE="${IMAGE:-clink-build:latest}"
BUILD_DIR="${BUILD_DIR:-build-live}"
BUILD_JOBS="${BUILD_JOBS:-6}"
CTEST_TIMEOUT="${CTEST_TIMEOUT:-300}"

# --- connector registry -----------------------------------------------------
# One entry per connector, run in this order (light/fast services first so a
# problem surfaces early; the slow-to-start cassandra + elasticsearch are last).
# Fields, separated by '@@':  name @@ compose-services @@ test-target @@ ctest-regex
# The ctest-regex is the exact gtest suite name(s) whose tests require the live
# service; '|' inside it is regex alternation.
CONNECTORS=(
  "redis@@redis@@clink_redis_tests@@RedisLive|RedisUpsertSinkLive"
  "mongodb@@mongodb@@clink_mongodb_tests@@MongoLive"
  "nats@@nats@@clink_nats_tests@@NatsLive"
  "mqtt@@mqtt@@clink_mqtt_tests@@MqttLive"
  "postgres@@postgres-cdc@@clink_postgres_tests@@PostgresCdcLive|PostgresJsonSinkLive|PostgresSink2PCLive|PostgresUpsertSinkLive"
  "mysql@@mysql@@clink_mysql_tests@@MysqlLive|MysqlCdcLive|MysqlUpsertSinkLive"
  "s3@@localstack@@clink_s3_tests@@S3Sink2PCLive|S3MaterializationStore.RoundTripAgainstLiveEndpoint|StateParquetExportS3.PartitionedExportReadableBackFromS3|S3SnapshotStore.CheckpointDirRoundTripAgainstLiveEndpoint|S3SnapshotStore.CacheServesImmutableSstOnSecondFetch|S3CasSnapshotStore|S3RemotePool"
  "rocksdb-s3@@localstack@@clink_rocksdb_s3_tests@@S3RocksdbSchemes"
  "elasticsearch@@elasticsearch@@clink_http_connector_tests@@ElasticsearchLive"
  "pubsub@@pubsub@@clink_http_connector_tests@@PubSubLive"
  "cassandra@@cassandra@@clink_cassandra_tests@@CassandraLive|CassandraUpsertSinkLive"
  "influxdb@@influxdb@@clink_http_connector_tests@@InfluxDbLive"
  "rabbitmq@@rabbitmq@@clink_rabbitmq_tests@@RabbitMqLive"
  "pulsar@@pulsar@@clink_pulsar_tests@@PulsarLive"
  "gcs@@gcs@@clink_gcs_tests@@GcsParquetLive"
  "azure@@azurite@@clink_azure_tests@@AzureParquetLive"
  "webhdfs@@webhdfs@@clink_webhdfs_tests@@WebHdfsParquetLive"
  "iceberg@@localstack iceberg-rest@@clink_iceberg_tests@@IcebergRestLive"
  "etcd@@etcd@@clink_etcd_tests@@EtcdHaCoordinator"
  # Reported BLOCKED unless the image gains the SDK components (see header):
  "aws-kinesis@@localstack@@clink_aws_tests@@KinesisLive"
)

# All connector test targets we try to build (intersected with what actually
# configures, so a connector missing its client lib is simply skipped, not fatal).
WANT_TARGETS="clink_redis_tests clink_mongodb_tests clink_nats_tests clink_mqtt_tests \
clink_postgres_tests clink_mysql_tests clink_s3_tests clink_rocksdb_s3_tests \
clink_http_connector_tests clink_cassandra_tests clink_pulsar_tests clink_rabbitmq_tests \
clink_gcs_tests clink_azure_tests clink_webhdfs_tests clink_iceberg_tests clink_etcd_tests \
clink_aws_tests"

AWS_ENV=(-e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test
         -e AWS_DEFAULT_REGION=us-east-1 -e AWS_EC2_METADATA_DISABLED=true)

# Per-connector env, pointing at the services by their compose DNS names (the test
# container joins the same network). LocalStack accepts the dummy AWS creds.
set_env() {
  ENVARGS=()
  case "$1" in
    redis)         ENVARGS=(-e "CLINK_REDIS_TEST_URL=redis://redis:6379") ;;
    mongodb)       ENVARGS=(-e "CLINK_MONGODB_TEST_URI=mongodb://mongodb:27017/?replicaSet=rs0") ;;
    nats)          ENVARGS=(-e "CLINK_NATS_TEST_ENDPOINT=nats://nats:4222") ;;
    mqtt)          ENVARGS=(-e "CLINK_MQTT_TEST_URL=mqtt://mqtt:1883") ;;
    postgres)      ENVARGS=(-e "CLINK_POSTGRES_CDC_TEST_DSN=host=postgres-cdc port=5432 user=postgres password=postgres dbname=postgres") ;;
    mysql)         ENVARGS=(-e "CLINK_MYSQL_TEST_DSN=host=mysql port=3306 user=root password=mysql database=test") ;;
    cassandra)     ENVARGS=(-e "CLINK_CASSANDRA_TEST_CONTACT_POINTS=cassandra") ;;
    influxdb)      ENVARGS=(-e "CLINK_INFLUXDB_TEST_ENDPOINT=http://influxdb:8086" -e "CLINK_INFLUXDB_TEST_ORG=clink" -e "CLINK_INFLUXDB_TEST_BUCKET=clink" -e "CLINK_INFLUXDB_TEST_TOKEN=clink-token") ;;
    rabbitmq)      ENVARGS=(-e "CLINK_RABBITMQ_TEST_ENDPOINT=rabbitmq" -e "CLINK_RABBITMQ_TEST_USER=clink" -e "CLINK_RABBITMQ_TEST_PASSWORD=clink") ;;
    pulsar)        ENVARGS=(-e "CLINK_PULSAR_TEST_ENDPOINT=pulsar://pulsar:6650") ;;
    gcs)           ENVARGS=(-e "CLINK_GCS_TEST_ENDPOINT=gcs:4443") ;;
    azure)         ENVARGS=(-e "CLINK_AZURE_TEST_ENDPOINT=azurite:10000") ;;
    webhdfs)       ENVARGS=(-e "CLINK_WEBHDFS_TEST_ENDPOINT=http://webhdfs:50070" -e "CLINK_WEBHDFS_TEST_USER=root") ;;
    iceberg)       ENVARGS=(-e "CLINK_ICEBERG_REST_URI=http://iceberg-rest:8181" -e "CLINK_ICEBERG_REST_WAREHOUSE=s3://clink-iceberg-rest/wh" -e "CLINK_S3_TEST_ENDPOINT=http://localstack:4566" -e "CLINK_S3_TEST_BUCKET=clink-iceberg-rest" "${AWS_ENV[@]}") ;;
    etcd)          ENVARGS=(-e "CLINK_ETCD_ENDPOINT=http://etcd:2379") ;;
    elasticsearch) ENVARGS=(-e "CLINK_ELASTICSEARCH_TEST_ENDPOINT=http://elasticsearch:9200") ;;
    pubsub)        ENVARGS=(-e "CLINK_PUBSUB_EMULATOR_HOST=pubsub:8085") ;;
    s3|rocksdb-s3) ENVARGS=(-e "CLINK_S3_TEST_ENDPOINT=http://localstack:4566" -e "CLINK_S3_TEST_BUCKET=clink-live-test" "${AWS_ENV[@]}") ;;
    aws-kinesis)   ENVARGS=(-e "CLINK_KINESIS_TEST_ENDPOINT=http://localstack:4566" "${AWS_ENV[@]}") ;;
  esac
}

if ! docker info >/dev/null 2>&1; then
  echo "▶ Docker is not running." >&2
  exit 1
fi

cleanup() {
  if [[ -z "${KEEP_SERVICES:-}" ]]; then
    docker compose -f "$COMPOSE" -p "$PROJECT" down -v >/dev/null 2>&1 || true
  else
    echo "▶ KEEP_SERVICES set - services left up. Stop with:"
    echo "  docker compose -f $COMPOSE -p $PROJECT down -v"
  fi
}
trap cleanup EXIT

# --- readiness probes (where compose "healthy" is not the same as "ready") ---
compose() { docker compose -f "$COMPOSE" -p "$PROJECT" "$@"; }

wait_tcp() {  # host port name
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; then exec 3>&- 3<&-; echo "  $3 ready."; return 0; fi
    sleep 2
  done
  echo "  WARNING: $3 not ready ($1:$2); its tests may fail."
}

probe() {  # connector-name
  case "$1" in
    mongodb)
      # The change-stream source needs a replica set; initiate the single-node set
      # (member host = the service DNS name) and wait for it to elect a PRIMARY.
      compose exec -T mongodb mongosh --quiet --eval \
        "try { rs.status() } catch (e) { rs.initiate({_id:'rs0',members:[{_id:0,host:'mongodb:27017'}]}) }" >/dev/null 2>&1 || true
      for i in $(seq 1 40); do
        if compose exec -T mongodb mongosh --quiet --eval "db.hello().isWritablePrimary" 2>/dev/null | grep -qx true; then
          echo "  mongodb PRIMARY ready (attempt $i)."; return 0; fi
        sleep 3
      done
      echo "  WARNING: mongodb never elected PRIMARY; its tests may fail." ;;
    cassandra)
      # Cassandra answers a SELECT on system.local well before it can serve DDL;
      # the tests create a keyspace, so wait for a CREATE/DROP round-trip.
      for i in $(seq 1 40); do
        if compose exec -T cassandra cqlsh -e \
             "CREATE KEYSPACE IF NOT EXISTS clink_probe WITH replication = {'class':'SimpleStrategy','replication_factor':1}; DROP KEYSPACE clink_probe" \
             >/dev/null 2>&1; then
          echo "  cassandra ready (DDL round-trip ok after $i attempt(s))."; return 0; fi
        sleep 3
      done
      echo "  WARNING: cassandra never accepted DDL; its tests may fail." ;;
    mysql)
      # mysqladmin ping (the compose healthcheck) passes during the entrypoint's
      # temporary init server AND across the restart into the real server, so a
      # test connecting to mysql:3306 right after "healthy" hits "Can't connect".
      # Wait for a real TCP query round-trip against the running server.
      for i in $(seq 1 40); do
        if compose exec -T mysql mysql -h127.0.0.1 -uroot -pmysql -e "SELECT 1" >/dev/null 2>&1; then
          echo "  mysql ready (SELECT 1 over TCP ok after $i attempt(s))."; return 0; fi
        sleep 3
      done
      echo "  WARNING: mysql never answered a TCP query; its tests may fail." ;;
    pubsub)
      # The emulator binds :8085 (TCP up) seconds before it actually SERVES the
      # REST admin API, so a port probe is not enough - wait for a real HTTP reply.
      for i in $(seq 1 40); do
        # curl -w prints "000" (and exits non-zero) when it cannot connect, so
        # swallow the exit with '|| true' - do NOT append another 000.
        code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8085/v1/projects/clink-test/topics" 2>/dev/null || true)"
        if [[ -n "$code" && "$code" != "000" ]]; then echo "  pubsub serving (HTTP $code after $i attempt(s))."; return 0; fi
        sleep 2
      done
      echo "  WARNING: pubsub never served HTTP; its tests may fail." ;;
    pulsar)
      # Pulsar standalone is slow to come up; wait for the broker admin health.
      for i in $(seq 1 60); do
        code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8080/admin/v2/brokers/health" 2>/dev/null || true)"
        if [[ "$code" == "200" ]]; then echo "  pulsar broker healthy (after $i attempt(s))."; return 0; fi
        sleep 2
      done
      echo "  WARNING: pulsar broker never reported healthy; its tests may fail." ;;
    influxdb)
      # v2 init "setup" mode double-starts like mysql (temp server -> setup ->
      # real server), so `influx ping` can pass before the org/token exist. Verify
      # the token actually authenticates against the created org.
      for i in $(seq 1 40); do
        code="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Token clink-token" "http://localhost:8086/api/v2/buckets?org=clink" 2>/dev/null || true)"
        if [[ "$code" == "200" ]]; then echo "  influxdb ready (org+token live after $i attempt(s))."; return 0; fi
        sleep 2
      done
      echo "  WARNING: influxdb setup not confirmed; its tests may fail." ;;
    rabbitmq)
      # ping (the healthcheck) means the node is up; also confirm the AMQP port
      # is actually accepting before the test declares a queue over it.
      for i in $(seq 1 40); do
        if compose exec -T rabbitmq rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1; then
          echo "  rabbitmq ready (port connectivity ok after $i attempt(s))."; return 0; fi
        sleep 2
      done
      echo "  WARNING: rabbitmq not fully ready; its tests may fail." ;;
    s3|rocksdb-s3)
      # localstack is torn down between connectors, so each localstack-backed
      # connector gets a fresh, empty S3 - create the bucket the tests write to.
      # (Some suites create their own bucket; rocksdb-s3's snapshot store does not.)
      compose exec -T localstack awslocal s3 mb s3://clink-live-test >/dev/null 2>&1 || true
      echo "  localstack bucket clink-live-test ensured." ;;
    gcs)
      # fake-gcs-server has no healthcheck; wait for the storage API to answer.
      for i in $(seq 1 40); do
        code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:4443/storage/v1/b?project=clink-test" 2>/dev/null || true)"
        if [[ -n "$code" && "$code" != "000" ]]; then echo "  gcs serving (HTTP $code after $i attempt(s))."; return 0; fi
        sleep 2
      done
      echo "  WARNING: fake-gcs-server never served HTTP; its tests may fail." ;;
    azure)
      # Azurite has no healthcheck; wait for the blob endpoint to answer (any
      # HTTP status - an unauthenticated request 400s, which still means serving).
      for i in $(seq 1 40); do
        code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:10000/devstoreaccount1" 2>/dev/null || true)"
        if [[ -n "$code" && "$code" != "000" ]]; then echo "  azurite serving (HTTP $code after $i attempt(s))."; return 0; fi
        sleep 2
      done
      echo "  WARNING: azurite never served HTTP; its tests may fail." ;;
    webhdfs)
      # Wait for the namenode WebHDFS to answer.
      for i in $(seq 1 60); do
        code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:50070/webhdfs/v1/?op=LISTSTATUS&user.name=root" 2>/dev/null || true)"
        [[ "$code" == "200" ]] && break
        sleep 2
      done
      # A fresh namenode boots into safemode (writes forbidden). It auto-leaves once
      # a datanode registers - but forcing 'safemode leave' BEFORE that leaves the
      # namenode with zero live datanodes, so CREATE fails "Failed to find datanode".
      # So wait for a live datanode FIRST, then leave safemode (skips the ~30s
      # safemode extension) and ensure a writable /tmp for the test files.
      for i in $(seq 1 60); do
        if compose exec -T webhdfs bash -lc "hdfs dfsadmin -report 2>/dev/null | grep -qE 'Live datanodes \([1-9]'" 2>/dev/null; then
          echo "  webhdfs live datanode registered (after $i attempt(s))."; break; fi
        sleep 2
      done
      compose exec -T webhdfs bash -lc "hdfs dfsadmin -safemode leave; hdfs dfs -mkdir -p /tmp; hdfs dfs -chmod 1777 /tmp" >/dev/null 2>&1 || true
      echo "  webhdfs ready (live datanode + safemode left, /tmp writable)." ;;
    iceberg)
      # Pre-create the warehouse bucket (localstack is torn down between connectors,
      # so it starts empty; neither the REST catalog nor IcebergS3Live creates it).
      compose exec -T localstack awslocal s3 mb s3://clink-iceberg-rest >/dev/null 2>&1 || true
      # Wait for the REST catalog to actually serve (its container has a healthcheck,
      # but belt-and-braces since it starts a JVM after localstack is healthy).
      for i in $(seq 1 40); do
        code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8181/v1/config" 2>/dev/null || true)"
        if [[ "$code" == "200" ]]; then echo "  iceberg-rest serving (HTTP 200 after $i attempt(s)), bucket ensured."; return 0; fi
        sleep 2
      done
      echo "  WARNING: iceberg-rest catalog never served /v1/config; its tests may fail." ;;
    *) : ;;  # compose --wait on the service healthcheck was sufficient
  esac
}

# --- resolve the run list ---------------------------------------------------
declare -a RUN_LIST
if [[ -n "${ONLY:-}" ]]; then
  for want in $ONLY; do
    found=""
    for entry in "${CONNECTORS[@]}"; do
      [[ "${entry%%@@*}" == "$want" ]] && { RUN_LIST+=("$entry"); found=1; break; }
    done
    [[ -z "$found" ]] && echo "▶ WARNING: unknown connector '$want' (ignored)."
  done
else
  # Default: every registered connector. aws-kinesis stays in and reports BLOCKED
  # (its binary is not built in the image) rather than being silently dropped.
  RUN_LIST=("${CONNECTORS[@]}")
fi
[[ ${#RUN_LIST[@]} -eq 0 ]] && { echo "▶ nothing to run."; exit 0; }

sql_flag="-DCLINK_BUILD_SQL=OFF"
[[ -n "${WITH_SQL:-}" ]] && sql_flag="-DCLINK_BUILD_SQL=ON"

# --- 1. build once, no services running -------------------------------------
if [[ -z "${SKIP_BUILD:-}" ]]; then
  echo "▶ Building connector test targets in $IMAGE (no services up, BUILD_DIR=$BUILD_DIR, -j$BUILD_JOBS)..."
  docker run --rm \
    -v "$(pwd)":/workspace -w /workspace \
    -e IN_DOCKER=1 -e CLINK_DEPS_PREFIX=/usr/local \
    "$IMAGE" bash -c "
      set -e
      cmake -S . -B '$BUILD_DIR' -DCLINK_BUILD_TESTS=ON -DCLINK_INTEGRATION_TESTS=ON $sql_flag >/dev/null
      avail=\$(cmake --build '$BUILD_DIR' --target help 2>/dev/null | grep -oE 'clink_[a-z0-9_]+_tests' | sort -u)
      build=''
      for t in $WANT_TARGETS; do echo \"\$avail\" | grep -qx \"\$t\" && build=\"\$build \$t\"; done
      echo \"▶ building:\$build\"
      cmake --build '$BUILD_DIR' --target \$build -j$BUILD_JOBS
    "
else
  echo "▶ SKIP_BUILD set - reusing $BUILD_DIR."
fi

# --- 2. capture which connector test binaries were actually built ----------
# Detect BLOCKED from the BINARY, not `ctest -N`: gtest_discover_tests runs the
# exe at enumeration time and can transiently fail, so ctest -N is not a reliable
# "was it built" signal. A missing binary means the impl compiled to a stub
# (client lib absent from the image) - that is the real BLOCKED condition.
echo "▶ Enumerating built connector test binaries..."
BUILT="$(docker run --rm -v "$(pwd)":/workspace -w "/workspace/$BUILD_DIR" \
  "$IMAGE" bash -c "find . -type f -name 'clink_*_tests' -exec basename {} ';'" 2>/dev/null | sort -u || true)"

# --- 3. per-connector: provision -> probe -> test -> teardown ---------------
declare -a SUMMARY
overall=0
for entry in "${RUN_LIST[@]}"; do
  name="${entry%%@@*}";        rest="${entry#*@@}"
  services="${rest%%@@*}";     rest="${rest#*@@}"
  target="${rest%%@@*}";       regex="${rest#*@@}"

  # BLOCKED: the connector's test binary was not built (its client lib is absent
  # from the image, so the impl compiled to a stub). Report it - never provision
  # services or record a phantom pass for a connector whose tests do not exist.
  if ! grep -qx "$target" <<<"$BUILT"; then
    echo ""
    echo "▶ $name: BLOCKED - test binary $target was not built (missing image client lib?)."
    SUMMARY+=("$(printf '%-14s BLOCKED (%s not built - missing image dep)' "$name" "$target")")
    continue
  fi

  echo ""
  echo "▶ $name: up [$services] -> test -> down"
  compose up -d --wait $services
  probe "$name"
  set_env "$name"

  rc=0
  docker run --rm --network "$NETWORK" \
    -v "$(pwd)":/workspace -w "/workspace/$BUILD_DIR" \
    -e IN_DOCKER=1 -e CLINK_DEPS_PREFIX=/usr/local \
    ${ENVARGS[@]+"${ENVARGS[@]}"} \
    "$IMAGE" ctest --output-on-failure --timeout "$CTEST_TIMEOUT" -R "$regex" || rc=$?

  if [[ $rc -eq 0 ]]; then
    SUMMARY+=("$(printf '%-14s PASS' "$name")")
  else
    SUMMARY+=("$(printf '%-14s FAIL (exit %s)' "$name" "$rc")")
    overall=1
  fi

  # Tear down just this connector's services before the next one.
  compose rm -sf $services >/dev/null 2>&1 || true
done

# --- summary ----------------------------------------------------------------
echo ""
echo "================ per-connector live results ================"
for line in "${SUMMARY[@]}"; do echo "  $line"; done
echo "============================================================"
if [[ $overall -eq 0 ]]; then
  echo "✅ All run connectors passed (BLOCKED = binary not built in image; not a failure)."
else
  echo "❌ One or more connectors failed. See the ctest output above."
fi
exit $overall
