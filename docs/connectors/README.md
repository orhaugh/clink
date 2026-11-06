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
aws-sdk-cpp `1.11.795`, Pulsar client `4.2.0`, DataStax cpp-driver `2.17.1`,
clickhouse-cpp `2.6.2`, Avro C++ `1.12.1`),
which is compiled at exact versions into `CLINK_DEPS_PREFIX` on both the host
and the Debian image. Versions are recorded per connector and in
[`scripts/versions.env`](https://github.com/orhaugh/clink/blob/main/scripts/versions.env).

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
| [MQTT](mqtt.md) | source + sink | libmosquitto (sink); in-tree MQTT 5 / 3.1.1 client, OpenSSL for TLS (source) | system pkg | - |
| [WebSocket](websocket.md) | source | none (OpenSSL for `wss://`) | - | `websocket` |

## Object storage and table formats (Parquet)

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [Amazon S3 (Parquet)](s3-parquet.md) | source + sink | Arrow S3FileSystem + aws-sdk-cpp | Arrow `24.0.0`, aws-sdk `1.11.795` | `s3_parquet` |
| [Amazon S3 (raw objects)](s3.md) | sink (+ programmatic line source) | aws-sdk-cpp | `1.11.795` | `s3` |
| [Google Cloud Storage](gcs-parquet.md) | source + sink | Arrow GcsFileSystem (`ARROW_GCS`) | Arrow `24.0.0` | `gcs_parquet` |
| [Azure Blob Storage](azure-parquet.md) | source + sink | Arrow AzureFileSystem (`ARROW_AZURE`) | Arrow `24.0.0` | `azure_parquet` |
| [WebHDFS / HttpFS](webhdfs-parquet.md) | source + sink | clink::http_connector (vendored httplib) | Arrow `24.0.0` | `webhdfs_parquet` |
| [Apache Iceberg](iceberg.md) | source + sink | iceberg-cpp + Arrow | iceberg-cpp `v0.3.0`, Arrow `24.0.0` | `iceberg` |
| [Delta Lake](delta.md) | sink | SQL frontend + Arrow (aws-sdk-cpp for `s3://` roots) | Arrow `24.0.0` | `delta` |
| [Local files and Parquet](local.md) | source + sink | core (Arrow for Parquet) | built in | `file`, `filesystem`, `parquet` |

## Databases and key-value stores

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [PostgreSQL](postgres.md) | source + sink | libpq | system pkg | `postgres` |
| [MySQL / MariaDB](mysql.md) | source + sink | mariadb-connector-c | system pkg | `mysql` |
| [ClickHouse](clickhouse.md) | source + sink (`clickhouse_sink`, the SQL default; `clickhouse_native_sink` via `insert_format='native'`, recommended for a new table) | clickhouse-cpp | `2.6.2` (from source, Release) | `clickhouse` |
| [Cassandra / ScyllaDB](cassandra.md) | sink | DataStax cpp-driver | `2.17.1` | `cassandra` |
| [Neo4j](neo4j.md) | sink (Cypher statement per batch, Query API); read-only lookup (`connector='lookup'`, `function='neo4j'`) | clink::http_connector (vendored httplib) | Neo4j 2026.09.0 tested | `neo4j` |
| [MongoDB](mongodb.md) | source (change streams, changes only) + sink (insert, or replace by key) | mongo-cxx-driver | `4.4.0` | `mongodb` |
| [Redis](redis.md) | source + sink | hiredis | system pkg | `redis` |

## Cloud services and HTTP

| Connector | I/O | Client dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [AWS (Kinesis / Firehose / DynamoDB)](aws.md) | source + sink | aws-sdk-cpp | `1.11.795` | `kinesis`, `firehose`, `dynamodb` |
| [HTTP (Elasticsearch, OpenSearch, Splunk, InfluxDB, Prometheus, poll, Pub/Sub)](http.md) | source + sink | cpp-httplib (vendored) | vendored | `http`, `elasticsearch`, `opensearch`, `splunk`, `influxdb`, `prometheus`, `http_poll`, `pubsub` |

## Serialization

| Format | Role | Dependency | Version | SQL `connector=` |
| --- | --- | --- | --- | --- |
| [Apache Avro](avro.md) | encoding (codecs) | Avro C++ | `1.12.1` (from-source pin) | - |
| [Schema Registry formats](schema-registry.md) | value formats on Kafka (`format='avro'`, `'protobuf'`, `'json-schema'`) | Avro C++, libprotobuf + libprotoc, cpp-httplib (vendored) | Avro `1.12.1`; protobuf system pkg | `kafka` |

## Built-in sinks (no dependency)

Compiled into the SQL frontend itself; always available when
`CLINK_BUILD_SQL=ON`. One shared page: [built-in connectors](builtin.md).

| Connector | I/O | SQL `connector=` |
| --- | --- | --- |
| [Blackhole (discard)](builtin.md#blackhole) | sink | `blackhole` |
| [Changelog netting](builtin.md#changelog) | sink | `changelog` |
| [Print (stdout)](builtin.md#print) | sink | `print` |
| [Collect (Arrow to host, embedded only)](builtin.md#collect-embedded-only) | sink | `collect` |
| [Queryable state (another job's live state)](builtin.md#queryable_state-source) | source | `queryable_state` |
| [Generator (`GeneratorSource<T>`, tests and benchmarks)](builtin.md#generator-programmatic-only) | source | - |

## The capability manifest

Every compiled connector declares a machine-readable capability record next
to its factory registration: identity, formats, boundedness, recovery model
(replayable offset or broker redelivery), delivery guarantee as implemented
(not as the external system could theoretically provide), transactionality,
idempotency-key requirements, auth/TLS surface, and limitations. `clink
--capabilities` prints the manifest for the binary at hand, the delivery
analyser computes end-to-end guarantees from it at submission, and each
record's internal coherence is checked by `self_check()`. Coverage is
enforced mechanically for the `impls/` modules: a build-generated list of
enabled connector modules feeds a gate test
(`tests/test_connector_manifest_gate.cpp`), so a new connector module cannot
register factories without either declaring its record or being explicitly
classified as a non-connector module. The connectors the SQL frontend
registers itself, the [Delta Lake sink](delta.md) and the `print`,
`changelog`, `collect` and `queryable_state` built-ins, declare their records
from `clink::sql::install()`, and the same gate lists them by hand.

## Notes on delivery semantics

Guarantees vary by connector and are stated on each page. In summary:

- Exactly-once sinks require the `on_barrier` / `on_commit` two-phase-commit
  contract. The Kafka transactional sink (`kafka_2pc_sink_string`) implements it.
  The Parquet connectors offer both: the default sink is at-least-once, a
  directory of complete part files, one per subtask per checkpoint interval,
  and a 2PC variant
  (`<connector>_2pc_*_sink`, or `delivery_guarantee='exactly_once'` in SQL) stages
  one file for each checkpoint interval records reached under `<prefix>/staging`
  and atomically promotes it to `<prefix>/committed` only when the checkpoint
  completes globally; an interval no records reached publishes nothing.
- Sources that record their position as operator state replay from the last
  checkpoint on recovery; the exact mechanism (Kafka offsets, Postgres LSN,
  object index, row index) and any caveats are documented per connector.
- Messaging sources whose only position is the broker's (RabbitMQ, NATS,
  Pulsar, Redis Streams, Pub/Sub) acknowledge once the checkpoint that
  captured a message has completed, never at the barrier, so an aborted
  checkpoint leaves its messages to be redelivered. Their records declare
  `OffsetModel::Acknowledgement`, and the delivery analyser reports a job
  over them as `AT_LEAST_ONCE_SOURCE`: nothing in flight is lost, a
  checkpoint acknowledged late is delivered again, and a sink in the job may
  declare `at_least_once` but not `exactly_once`. A restore from a checkpoint
  older than one such a source acknowledged (the commit-confirmed protocol, a
  savepoint) does not see that checkpoint's messages again. The Redis Streams
  and Pub/Sub sources bound what they hold and stop reading at the bound
  until a checkpoint completes, and the Redis Streams source cannot be
  rescaled to fewer subtasks; their pages give the detail.

## Maturity

What each source and sink factory, or materially different mode, is shown to
do, kept apart from what its capability record declares. The columns say
whether a test exists, whether it ran and where, and whether a recovery was
demonstrated: a round trip shows a connector works, not that it recovers. A
row marked not reviewed repeats the record and the test entry points, nothing
more, and claims no result.

In-process tests run in the `ci` workflow's full ctest, which builds every
connector module and ran each one's in-process cases on commit 6ad5168a
(2026-11-05), all passing; live cases skip there. *run-all-live* is
`scripts/run-all-live.sh`, run by hand, which provisions each connector's
services in turn. A gate is a script that fails on a skipped case, a short
suite or a missing binary. A qualification campaign is retained evidence for
the engine version it ran, not a current result.

| Factory or mode | Surface | As implemented | Material limitations | Tests | Live run | Recovery demonstrated | Reviewed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `redis_source` | SQL `redis`, C++ | Acknowledges at checkpoint completion; the consumer's pending entries are redelivered. At-least-once. | Duplicates after a crash between completion and `XACK`; a restore from an older checkpoint misses that checkpoint's entries; a job with no completed checkpoint stops at `max_pending`; no scale-down | `RedisSourceLogic`, `SourceCapabilityRecord`, `CheckpointAckLedger`, `SourceAckHooks`; live `RedisSourceAckLive`, `RedisLive` | Gate `scripts/ack-sources-live.sh`, run by the `ack-sources` CI workflow: passed on commit 1dfa459a, [run 42084431285](https://github.com/orhaugh/clink/actions/runs/42084431285), 55 of 55 across the six brokers. The run's log carries each suite's count; its report artifact (`ack-sources-gate`) is kept until 2026-11-19 | Yes, against `redis:7`: abort, crash after the barrier, crash after completion, a killed and a paused `XACK`, scale-down refused. None retained. | 2026-11-05 |
| `pubsub_source` | SQL `pubsub`, C++ | Acknowledges at checkpoint completion, holding each message's lease up to `max_lease_s`; the server redelivers. At-least-once. | As Redis, plus duplicates past `max_lease_s`; REST pull only; `on_decode_error='drop'` loses undecodable messages by choice | `PubSubSourceAck` (a stub of the REST API), `SourceCapabilityRecord`; live `PubSubSourceAckLive`, `PubSubLive` | Same gate and run | Yes, against the stub and the emulator: abort, crashes, failed acknowledges, a checkpoint delayed past the deadline. None retained. | 2026-11-05 |
| `rabbitmq_source_string` | SQL `rabbitmq` | Acknowledges at checkpoint completion through the ledger, cumulatively only where no aborted interval lies below; the queue redelivers. At-least-once. | Duplicates after a crash between completion and ack; older-checkpoint restore as Redis; `prefetch` (default 10,000) is the most a subtask reads per checkpoint | In-process; live `RabbitMqSourceAckLive`, `RabbitMqLive` | Same gate and run | Yes, against `rabbitmq:3`: abort, crash after completion, an aborted interval below a completed one, close after a cancel, the prefetch bound. None retained. | 2026-11-05 |
| `nats_source_string` | SQL `nats` | Acknowledges at checkpoint completion, keeping held messages in progress up to `max_lease_s`; JetStream redelivers. At-least-once. | Duplicates after a crash between completion and ack, and past `max_lease_s`; older-checkpoint restore as Redis; `max_ack_pending` is shared by the durable's subtasks | In-process; live `NatsSourceAckLive`, `NatsLive` | Same gate and run | Yes, against `nats:2.10`: abort, crash after completion, close after a cancel, a checkpoint three times AckWait, the bound. None retained. | 2026-11-05 |
| `pulsar_source_*`, Shared and Exclusive | SQL `pulsar` | Acknowledges at checkpoint completion; the broker redelivers a closed consumer's unacknowledged messages. Exclusive also seeks back to the recorded message. At-least-once. | Duplicates after a crash between completion and ack; older-checkpoint restore as Redis; the broker's per-consumer limit bounds what is held | In-process; live `PulsarSourceAckLive`, `PulsarLive` | Same gate and run | Yes, against `apachepulsar/pulsar:3.3.1`, Shared: abort, crash after completion, close after a cancel; Exclusive replay by `PulsarLive`. None retained. | 2026-11-05 |
| `mqtt_source` | C++ only | Acknowledges at checkpoint completion (PUBACK, or PUBREC at QoS 2) through its own client, on the connection the message arrived on; an aborted checkpoint reconnects, and the persistent session redelivers. At-least-once at QoS 1 and 2. The sink keeps its own record (`mqtt`). | Duplicates after a crash between completion and acknowledgement, and after an abort or a lost connection; older-checkpoint restore as Redis; one interval reads at most the in-flight window (`receive_maximum`, or the broker's own limit under MQTT 3.1.1); a broker that bounds its queue drops what overflows while acknowledgements wait; no scale-down with `shared_group` set | `MqttProtocol`, `SourceAckTest` (an in-process broker), `MqttCapabilityRecords`; live `MqttSourceAckLive`, `MqttLive` | Same gate and run | Yes, against `eclipse-mosquitto:2` (2.1.2): crash before and after completion, abort, close, QoS 2, the window under MQTT 5 and 3.1.1, a resumed session's retained message. The previous source lost every message in the crash, abort, QoS 2 and close cases. None retained. | 2026-11-05 |
| `kafka_source_*` | SQL `kafka` | Record: replays from log offsets | See its page | In-process; live suites | `docker-suites` gate in CI | Suites exist (exactly-once, window recovery, source ownership); [QUAL-01](../qualification/qual-01-kafka-exactly-once.md) | Not reviewed |
| PostgreSQL CDC, `changelog_mode` | SQL `postgres` | Record: replays from the LSN; slot confirmed at completion | See its page | In-process; `PostgresCdcContractLive`, recovery cells | `postgres-cdc` gate in CI | Suites exist | Not reviewed |
| MySQL CDC | SQL `mysql` | Record: replays from a log position | See its page | In-process; live `MysqlCdcLive` | run-all-live | Not assessed | Not reviewed |
| `kinesis_source` | SQL `kinesis` | Record: replays from a cursor it keeps (opaque token) | Resharding order caveat | In-process; live `KinesisLive` | run-all-live | Not assessed | Not reviewed |
| `mongo_cdc_source` | SQL `mongodb`, C++ | Change streams, changes only: resumes after its checkpointed resume token, which covers only events a pass handed over; records where each run's stream started, for a restart before the first checkpoint; never falls back to the current time. At-least-once. In SQL, one appended row per event, each value converted strictly to its declared type. | No snapshot; a single reader; a token the oplog no longer covers fails the job; drop, rename, dropDatabase and invalidate stop the source; `updateLookup` images are read after the event; in SQL no changelog form, and a value its column cannot hold stops the job | `MongoEvent`, `MongoChangePass`, `MongoSourceRestore`, `MongoSqlRow`, `MongoSqlPlan`; live `MongoSourceRecoveryLive`, `MongoLive`, `MongoSqlLive`; cluster `MongoRecovery` | Gate `scripts/mongodb-live.sh`: passed locally on commit 5e29b318, 86 of 86 with the seven recovery cells, 2026-11-06 (reports not published); run by the `mongodb` CI workflow, which passed it on commit ebb9355f, 86 of 86 (run 42174656404) | Yes, against `mongo:7` with two workers: kills before a write, after a write, with events in flight and before any checkpoint; a server restart. The previous source lost an event at the per-pass cap and every change after a checkpoint taken before its first pass. | 2026-11-06 |
| `mongo_sink`, `mongo_upsert_sink` | SQL `mongodb` (`mode='upsert'` for the second), C++ | `w=majority` writes, flushed at every barrier. Inserts: at-least-once, a replay of a record carrying its `_id` recognised. Replacements by key: duplicates collapse on the key; key type and, for a key other than `_id`, a unique index checked. In SQL, typed BSON per declared column, and deletes applied by key. | A replay rewrites a key to earlier records before its latest; convergence per key only within one subtask or a stream partitioned by key; a record without `_id` is inserted again on replay; in SQL a single-column key | `MongoSinkRules`, `MongoFactoryRegistration`, `MongoSqlSink`, `MongoSqlPlan`; live `MongoSinkLive`, `MongoLive`, `MongoSqlLive`; cluster `MongoRecovery` | Same gate | Yes, with the server's `failCommand`: a write concern error, a dropped connection, a batch stopped part way; in the cluster, kills before and after a write and a mirror by key, as a C++ job and as a SQL job | 2026-11-06 |
| Parquet and file sources (`file`, `parquet`, `s3_parquet`, `gcs_parquet`, `azure_parquet`, `webhdfs_parquet`) | SQL | Record: replays from file offsets | See their pages | In-process; live suites for each store | run-all-live | Not assessed | Not reviewed |
| `iceberg` source | SQL `iceberg` | Record: replays from a cursor it keeps (opaque token) | See its page | In-process; live `IcebergRestLive`, `IcebergS3Live` | run-all-live | Not assessed | Not reviewed |
| `http_poll_source` | SQL `http_poll` | Record: replays from a cursor | Needs an exclusive, ascending cursor server-side | In-process only | None | Not assessed | Not reviewed |
| `websocket_source` | SQL `websocket` | Record: no replay, at-most-once | See its page | In-process | None | Not assessed | Not reviewed |
| `kafka_2pc_sink_string` | SQL `exactly_once` | Record: two-phase commit on the commit-confirmed protocol | See its page | In-process; exactly-once suites, fault matrix | `docker-suites` gate in CI | Suites exist; [QUAL-01](../qualification/qual-01-kafka-exactly-once.md) | Not reviewed |
| `postgres_2pc_sink` | SQL `exactly_once` | Record: two-phase commit | See its page | In-process; `PostgresExactlyOnceTest` | `docker-suites` and `postgres-cdc` gates in CI | Suites exist; [QUAL-02](../qualification/qual-02-postgres-two-phase-commit.md) | Not reviewed |
| Object-store `_2pc` Parquet sinks | SQL `exactly_once` | Record: atomic publish at completion | See their pages | In-process; `S3ExactlyOnceTest`, live suites | `docker-suites` gate in CI (S3); run-all-live for the others | Suites exist; [QUAL-03](../qualification/qual-03-s3-staged-commits.md) (S3) | Not reviewed |
| ClickHouse sinks (`clickhouse_sink`, `clickhouse_native_sink`) | SQL `clickhouse` | Record: at-least-once | See its page | In-process; live and recovery suites | ClickHouse live suite in `ci`; recovery matrix nightly | Suites exist | Not reviewed |
| `neo4j` sink and lookup | SQL `neo4j`, lookup | Record: at-least-once | See its page | In-process; live and recovery suites | `neo4j` gate in CI | Suites exist | Not reviewed |
| Upsert sinks (`postgres_upsert_sink`, `mysql_upsert_sink`, `redis_upsert_sink`, `cassandra` upsert, `dynamodb_sink`) | SQL `mode='upsert'` | Record: effectively-once on a stable key | See their pages | In-process; `UpsertContractSuite` per store | `postgres-cdc` gate in CI (PostgreSQL); run-all-live for the others | Not assessed | Not reviewed |
| `elasticsearch_sink`, `opensearch_sink` | SQL `elasticsearch`, `opensearch`, C++ | At-least-once. A batch counts as written only when the bulk response confirms every operation; anything else is resent, then fails the job, never dead-lettered. With `document_id` each record replaces its id's document and a resend carries the later records of its id, so a replay converges on each id's latest record. | Records of one id ordered only within one sink subtask, not against other writers; a replay can show a document at an older record until it reaches the latest; ids outside the [id rules](http.md#document-ids) refused; no update, delete or external versioning | `ElasticsearchSink`, `EsBulkResult`, `EsDocumentId`; live `Elasticsearch/SearchSinkLive`, `OpenSearch/SearchSinkLive`; cluster `Elasticsearch/SearchRecovery`, `OpenSearch/SearchRecovery` | Gate `scripts/search-live.sh` for Elasticsearch 8.13.4 and 9.2.0 and OpenSearch 2.17.1 and 3.3.0, each with a throttled, a secured and a three-node cluster phase; run by the `search` CI workflow, which passed the first two servers' gates on commit bf6cae33 (run 42184675107) | Yes, against each server with two workers: kills before a write and after a confirmed flush, the second also as a SQL job; replies that confirm nothing, during which no checkpoint completed; a reply lost after the server applied a flush, the worker killed while it was held. Also a node of a three-node cluster killed while the sink writes. Some faults come from a proxy, as [Testing](http.md#elasticsearch-and-opensearch) names. Before: a 200 that confirmed nothing counted as written, and a resend could leave a document at an older record. | 2026-11-06 |
| Append sinks, at-least-once (Kafka, RabbitMQ, NATS, Pulsar, MQTT, Redis Streams, Pub/Sub, the other HTTP sinks, Kinesis, Firehose, Cassandra, MySQL, PostgreSQL, Iceberg, Delta, rolling Parquet) | SQL, except MQTT | Record: at-least-once; a replay duplicates | See their pages | In-process; live suites where a page lists them | Mostly run-all-live | Not assessed | Not reviewed |
| `s3` raw objects | SQL `s3` | Record: at-most-once | See its page | In-process; live suites | run-all-live | Not assessed | Not reviewed |

## NaN and infinity in JSON output

JSON has no token for NaN or an infinity, so every sink that writes JSON
renders a non-finite `DOUBLE` or `REAL` value deliberately. That covers the
sinks that write the JSON themselves (`file` with `format='json'`, including
its upsert, exactly-once and `partition_by` forms, `changelog` and `print`) and
every string sink the planner feeds through the `row_to_json_string` bridge:
Kafka (`format='json'`, and the registry format `'json-schema'`), RabbitMQ,
NATS, Pulsar, Pub/Sub, the HTTP family (`http`,
`elasticsearch`, `opensearch`, `splunk_hec`, `influxdb`, `prometheus`), Redis,
MySQL, Postgres, Cassandra, ClickHouse with `insert_format='jsoneachrow'`,
Kinesis, Firehose and DynamoDB. The table option `json_non_finite` chooses the
form:

| `json_non_finite` | NaN | +Infinity | -Infinity |
| --- | --- | --- | --- |
| `'null'` (default) | `null` | `null` | `null` |
| `'string'` | `"NaN"` | `"Infinity"` | `"-Infinity"` |

`'string'` is for a consumer that must tell NaN from a missing value. Any other
value is refused at `CREATE TABLE`, and again when the job is planned, naming
the accepted values; a sink factory built outside SQL refuses it too. The
option is refused on a sink that writes no JSON (`parquet` and the object-store
Parquet connectors, `delta`, `iceberg`, ClickHouse `insert_format='native'`,
`neo4j`, `collect`, `blackhole`, or a single-TEXT column table, which writes each
record as it is): a typed sink holds NaN and
the infinities as they are. So does Kafka with the registry format
`format='avro'` or `'protobuf'`, which refuses the option too: its encoder
writes the IEEE value itself, in a nullable field as in a required one (see
[Schema Registry formats](schema-registry.md#sql-usage)). A sink that turns the
bridge's JSON into its own encoding (the database sinks, InfluxDB, Prometheus,
DynamoDB) receives the `null` or the string, and treats it as it treats any
null or string value in that column.

Reading such output back with a JSON source: `null` in a `DOUBLE` column is
SQL `NULL`. The strings `"NaN"`, `"Infinity"` and `"-Infinity"` are not turned
back into doubles. The column holds the string: a JSON sink writes it out again
unchanged, arithmetic on it gives `NULL`, and a typed sink such as Parquet
stores `NULL`.

Other surfaces follow the same rule with no option: the HTTP API's JSON
responses (the queryable-state routes) write `null`, and so do dead-letter
payloads and `json_object`. The queryable-state Arrow scan is typed and carries
the float64 value itself. Text clink writes and reads back itself (operator
state, the row codec between operators, the JSON in front of an Avro or
Protobuf encoder) keeps the value exactly; see
[the SQL frontend internals](../internals/sql-frontend.md).
