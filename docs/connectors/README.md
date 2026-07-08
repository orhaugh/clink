# Connectors

Reference documentation for clink's source and sink connectors. Each connector
has its own page covering the dependency and pinned version, the CMake build
knob, the exact factory names, every configuration option, SQL usage where
available, an example, delivery semantics, and limitations.

Every connector is an optional module. It is gated by a `CLINK_WITH_<NAME>`
CMake option (default `AUTO`: built when its client library is found, skipped
otherwise; set `ON` to require it or `OFF` to exclude it). Most connectors link
a system client library obtained via apt (Debian) or brew (macOS); a few ride
the from-source toolchain (Apache Arrow/Parquet `24.0.0`, iceberg-cpp `v0.3.0`,
aws-sdk-cpp `1.11.795`, Pulsar client `4.2.0`, DataStax cpp-driver `2.17.1`),
which is compiled at exact versions into `CLINK_DEPS_PREFIX` on both the host
and the Debian image. Versions are recorded per connector and in
[`scripts/versions.env`](../../scripts/versions.env).

The `SQL connector=` column lists the string to use in a SQL
`CREATE TABLE ... WITH (connector='...')` statement. A dash means the connector
is reachable through the programmatic API only.

## Messaging and streaming

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [Apache Kafka](kafka.md) | source + sink | librdkafka | system pkg | `kafka` |
| [Apache Pulsar](pulsar.md) | source + sink | Pulsar C++ client | `4.2.0` | `pulsar` |
| [RabbitMQ (AMQP 0-9-1)](rabbitmq.md) | source + sink | rabbitmq-c | system pkg | `rabbitmq` |
| [NATS JetStream](nats.md) | source + sink | nats.c | system pkg | `nats` |
| [MQTT](mqtt.md) | source + sink | libmosquitto | system pkg | - |

## Object storage and table formats (Parquet)

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [Amazon S3 (Parquet)](s3-parquet.md) | source + sink | Arrow S3FileSystem + aws-sdk-cpp | Arrow `24.0.0`, aws-sdk `1.11.795` | `s3_parquet` |
| [Amazon S3 (raw objects)](s3.md) | sink | aws-sdk-cpp | `1.11.795` | `s3` |
| [Google Cloud Storage](gcs-parquet.md) | source + sink | Arrow GcsFileSystem (`ARROW_GCS`) | Arrow `24.0.0` | `gcs_parquet` |
| [Azure Blob Storage](azure-parquet.md) | source + sink | Arrow AzureFileSystem (`ARROW_AZURE`) | Arrow `24.0.0` | `azure_parquet` |
| [WebHDFS / HttpFS](webhdfs-parquet.md) | source + sink | clink::http_connector (vendored httplib) | Arrow `24.0.0` | `webhdfs_parquet` |
| [Apache Iceberg](iceberg.md) | sink | iceberg-cpp + Arrow | iceberg-cpp `v0.3.0`, Arrow `24.0.0` | `iceberg` |
| [Local files and Parquet](local.md) | source + sink | core (Arrow for Parquet) | built in | `file`, `filesystem`, `parquet` |

## Databases and key-value stores

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [PostgreSQL](postgres.md) | source + sink | libpq | system pkg | `postgres` |
| [MySQL / MariaDB](mysql.md) | source + sink | mariadb-connector-c | system pkg | `mysql` |
| [ClickHouse](clickhouse.md) | source + sink | clickhouse-cpp | `2.5.1` (from source) | `clickhouse` |
| [Cassandra / ScyllaDB](cassandra.md) | sink | DataStax cpp-driver | `2.17.1` | `cassandra` |
| [MongoDB](mongodb.md) | source + sink | mongo-cxx-driver | system pkg | - |
| [Redis](redis.md) | source + sink | hiredis | system pkg | `redis` |

## Cloud services and HTTP

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [AWS (Kinesis / Firehose / DynamoDB)](aws.md) | source + sink | aws-sdk-cpp | `1.11.795` | `kinesis`, `firehose`, `dynamodb` |
| [HTTP (Elasticsearch, OpenSearch, Splunk, InfluxDB, Prometheus, poll, Pub/Sub)](http.md) | source + sink | cpp-httplib (vendored) | vendored | `http`, `elasticsearch`, `opensearch`, `splunk`, `influxdb`, `prometheus`, `http_poll`, `pubsub` |

## Serialization

| Format | Role | Dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [Apache Avro](avro.md) | encoding (codecs) | Avro C++ | system pkg | - |

## Built-in sinks (no dependency)

Compiled into the SQL frontend itself; always available when
`CLINK_BUILD_SQL=ON`. One shared page: [built-in sinks](builtin.md).

| Sink | I/O | SQL `connector=` |
| --- | --- | --- |
| [Blackhole (discard)](builtin.md#blackhole) | sink | `blackhole` |
| [Changelog netting](builtin.md#changelog) | sink | `changelog` |
| [Print (stdout)](builtin.md#print) | sink | `print` |
| [Collect (Arrow to host, embedded only)](builtin.md#collect-embedded-only) | sink | `collect` |

## Notes on delivery semantics

Guarantees vary by connector and are stated on each page. In summary:

- Exactly-once sinks require the `on_barrier` / `on_commit` two-phase-commit
  contract. The Kafka transactional sink (`kafka_2pc_sink_string`) implements it.
  The object-store and WebHDFS Parquet connectors offer both: the default
  single-object sink is at-least-once, and a 2PC variant
  (`<connector>_2pc_*_sink`, or `delivery_guarantee='exactly_once'` in SQL) stages
  one file per checkpoint under `<prefix>/staging` and atomically promotes it to
  `<prefix>/committed` only when the checkpoint completes globally.
- Sources that record their position as operator state replay from the last
  checkpoint on recovery; the exact mechanism (Kafka offsets, Postgres LSN,
  object index, row index) and any caveats are documented per connector.
- Messaging sources (RabbitMQ, NATS, Pulsar) acknowledge at the checkpoint
  barrier rather than post-commit; unacknowledged messages are redelivered.
