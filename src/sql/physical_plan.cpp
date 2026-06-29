#include "clink/sql/physical_plan.hpp"

#include <chrono>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/cluster/job_graph.hpp"
#include "clink/config/json.hpp"
#include "clink/metrics/sql_metrics.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/row_columnar_batcher.hpp"
#include "clink/sql/type.hpp"

namespace clink::sql {

namespace {

[[noreturn]] void unsupported(const std::string& msg, int pos = 0) {
    throw TranslationError(msg, pos);
}

constexpr const char* kChannelString = "string";
constexpr const char* kChannelRow = "row";

// A plan's channel type: every op on the chain must speak the same
// channel. Phase 3.2 picks the channel at the source table based on
// column count + format='json' property.
enum class Channel { String, Row };

const char* channel_name(Channel c) {
    return c == Channel::String ? kChannelString : kChannelRow;
}

const std::string& require_property(const TableDef& table, const std::string& key) {
    auto it = table.properties.find(key);
    if (it == table.properties.end()) {
        unsupported("table " + table.name + " missing required property: " + key);
    }
    return it->second;
}

Channel channel_for_table(const TableDef& table) {
    auto fmt = table.properties.find("format");
    const bool json = fmt != table.properties.end() && fmt->second == "json";
    if (json)
        return Channel::Row;
    if (table.columns.size() > 1)
        return Channel::Row;
    return Channel::String;
}

void check_string_channel_table(const TableDef& table) {
    if (table.columns.size() != 1) {
        unsupported("table " + table.name +
                    " has multiple columns but no format='json' - declare "
                    "format='json' for multi-column tables");
    }
    if (!table.columns[0].type->Equals(*arrow::utf8())) {
        unsupported("string-channel table " + table.name + " column " + table.columns[0].name +
                    " must be TEXT/VARCHAR (got " +
                    arrow_to_sql_type_string(*table.columns[0].type) + ")");
    }
}

// --- Channel::String factory dispatch (Phase 2 logic) ---

std::string string_source_factory_for(const TableDef& table) {
    const auto& connector = require_property(table, "connector");
    if (connector == "file" || connector == "filesystem") {
        return "file_text_source";
    }
    if (connector == "kafka") {
        return "kafka_source_string";
    }
    if (connector == "rabbitmq") {
        return "rabbitmq_source_string";
    }
    if (connector == "nats") {
        return "nats_source_string";
    }
    if (connector == "pulsar") {
        return "pulsar_source_string";
    }
    if (connector == "postgres") {
        auto it = table.properties.find("mode");
        if (it != table.properties.end() && it->second == "cdc") {
            return "postgres_cdc_text_source";
        }
        return "postgres_text_source";
    }
    if (connector == "clickhouse") {
        return "clickhouse_text_source";
    }
    if (connector == "parquet") {
        return "parquet_string_source";
    }
    if (connector == "s3_parquet") {
        return "s3_parquet_string_source";
    }
    if (connector == "kinesis") {
        return "kinesis_source";
    }
    if (connector == "http_poll") {
        return "http_poll_source";
    }
    if (connector == "pubsub") {
        return "pubsub_source";
    }
    if (connector == "redis") {
        return "redis_source";
    }
    if (connector == "mysql") {
        auto it = table.properties.find("mode");
        if (it != table.properties.end() && it->second == "cdc") {
            return "mysql_cdc_source";
        }
        return "mysql_source";
    }
    unsupported("unsupported source connector '" + connector + "' for table " + table.name);
}

std::string string_sink_factory_for(const TableDef& table) {
    const auto& connector = require_property(table, "connector");
    if (connector == "file" || connector == "filesystem") {
        return "file_text_sink";
    }
    if (connector == "kafka") {
        return "kafka_sink_string";
    }
    if (connector == "rabbitmq") {
        return "rabbitmq_sink_string";
    }
    if (connector == "nats") {
        return "nats_sink_string";
    }
    if (connector == "pulsar") {
        return "pulsar_sink_string";
    }
    if (connector == "clickhouse") {
        return "clickhouse_sink";
    }
    if (connector == "s3") {
        return "s3_text_sink";
    }
    if (connector == "parquet") {
        return "parquet_string_sink";
    }
    if (connector == "s3_parquet") {
        return "s3_parquet_string_sink";
    }
    if (connector == "redis") {
        return "redis_sink";
    }
    if (connector == "mysql") {
        return "mysql_sink";
    }
    if (connector == "postgres") {
        return "postgres_sink";
    }
    if (connector == "pubsub") {
        return "pubsub_sink";
    }
    unsupported("unsupported sink connector '" + connector + "' for table " + table.name);
}

// --- Channel::Row factory dispatch (Phase 3.2 / Phase 6.4) ---

// Describes how a Row-typed scan / sink lowers to one or more ops.
// 'bridge' is empty when the connector natively speaks the Row channel
// (file_json_source / file_json_sink). For string-backed connectors
// (kafka) we emit the native string-channel source / sink, then bridge
// to / from Row via json_string_to_row or row_to_json_string.
struct RowConnectorBinding {
    std::string source_or_sink_op;       // factory name for the connector op
    std::string source_or_sink_channel;  // out_channel of that op (string or row)
    std::string bridge_op;               // optional Map op converting between string and Row
    // Connector params the binding forces onto the op, merged AFTER build_params
    // (so they override / supply keys the channel selector stripped). E.g. the
    // clickhouse Row sink forces format=jsoneachrow because the SQL format='json'
    // channel selector is consumed+stripped before it reaches the sink factory.
    std::map<std::string, std::string> extra_params{};
};

// Convert a table's declared columns to the RowColumn list the
// schema-driven Row batcher (and its param serialiser) consume.
std::vector<RowColumn> row_columns_of(const TableDef& table) {
    std::vector<RowColumn> rc;
    rc.reserve(table.columns.size());
    for (const auto& c : table.columns) {
        rc.push_back(RowColumn{c.name, c.type});
    }
    return rc;
}

RowConnectorBinding row_source_binding_for(const TableDef& table) {
    const auto& connector = require_property(table, "connector");
    if (connector == "file" || connector == "filesystem") {
        return RowConnectorBinding{"file_json_source", kChannelRow, {}};
    }
    if (connector == "kafka") {
        // Wave 2 inc1: an opt-in WITH-option swaps the row-form JSON bridge for
        // the columnar one, which attaches an Arrow sidecar so the downstream
        // columnar fast paths fire on the Kafka path. Default stays row-form.
        const auto it = table.properties.find("columnar_decode");
        const bool columnar =
            it != table.properties.end() && (it->second == "true" || it->second == "1");
        return RowConnectorBinding{"kafka_source_string",
                                   kChannelString,
                                   columnar ? "json_string_to_row_columnar" : "json_string_to_row"};
    }
    if (connector == "rabbitmq") {
        // RabbitMQ / AMQP source: each message body is a JSON object string (string
        // channel) bridged to Row. At-least-once (manual ack at the checkpoint barrier).
        return RowConnectorBinding{"rabbitmq_source_string", kChannelString, "json_string_to_row"};
    }
    if (connector == "nats") {
        // NATS JetStream source: each message body is a JSON object string (string channel)
        // bridged to Row. At-least-once (durable pull consumer, ack at the checkpoint barrier).
        return RowConnectorBinding{"nats_source_string", kChannelString, "json_string_to_row"};
    }
    if (connector == "pulsar") {
        // Apache Pulsar source: each message body is a JSON object string (string channel)
        // bridged to Row. At-least-once (Shared subscription, ack at the checkpoint barrier).
        return RowConnectorBinding{"pulsar_source_string", kChannelString, "json_string_to_row"};
    }
    if (connector == "parquet") {
        // Typed-columnar Parquet: each declared column is its own Arrow
        // column. The Row batcher is built from the schema_columns param.
        return RowConnectorBinding{"parquet_row_source", kChannelRow, {}};
    }
    if (connector == "nexmark") {
        // Synthetic Nexmark event generator (benchmark). The factory is
        // registered out-of-tree by the Nexmark harness; here we only map the
        // connector name to the Row-channel source op the planner emits.
        return RowConnectorBinding{"nexmark_source", kChannelRow, {}};
    }
    if (connector == "kinesis") {
        // Kinesis Data Streams source: each record's Data is a JSON object string
        // (string channel), bridged to Row. At-least-once (per-shard sequence
        // number checkpoint).
        return RowConnectorBinding{"kinesis_source", kChannelString, "json_string_to_row"};
    }
    if (connector == "http_poll") {
        // HTTP polling source: GET a JSON endpoint on an interval, each array
        // element is a JSON object string (string channel) bridged to Row.
        // At-least-once (cursor checkpoint).
        return RowConnectorBinding{"http_poll_source", kChannelString, "json_string_to_row"};
    }
    if (connector == "pubsub") {
        // Google Cloud Pub/Sub source (REST Pull): each message's base64-decoded
        // data is a JSON object string (string channel) bridged to Row.
        // At-least-once (ack-on-checkpoint + server redelivery after ackDeadline).
        return RowConnectorBinding{"pubsub_source", kChannelString, "json_string_to_row"};
    }
    if (connector == "redis") {
        // Redis Streams source (XREADGROUP consumer group): each entry is a JSON
        // object string (string channel) bridged to Row. At-least-once (PEL
        // replay + XACK-on-checkpoint).
        return RowConnectorBinding{"redis_source", kChannelString, "json_string_to_row"};
    }
    if (connector == "mysql") {
        // MySQL source on the Row path. mode='cdc': binlog CDC change events as
        // flat JSON (changed columns + __op/__table/__lsn/__xid) bridged to Row.
        // Otherwise: an incremental cursor source (SELECT WHERE cursor_col >
        // <cursor>). Both at-least-once.
        auto it = table.properties.find("mode");
        if (it != table.properties.end() && it->second == "cdc") {
            return RowConnectorBinding{"mysql_cdc_source", kChannelString, "json_string_to_row"};
        }
        return RowConnectorBinding{"mysql_source", kChannelString, "json_string_to_row"};
    }
    if (connector == "clickhouse") {
        // ClickHouse source (M2): SELECT rows as JSON objects keyed by column name
        // (string channel) bridged to Row. Bounded query (no cursor checkpoint).
        return RowConnectorBinding{"clickhouse_source", kChannelString, "json_string_to_row"};
    }
    if (connector == "postgres") {
        // Postgres source on the Row path. mode='cdc' (M5): logical-replication
        // change events as flat JSON objects (changed columns + __op/__table/__lsn
        // metadata) bridged to Row. Otherwise (M3): a plain SELECT cursor source,
        // each row a JSON object keyed by column name.
        auto it = table.properties.find("mode");
        if (it != table.properties.end() && it->second == "cdc") {
            return RowConnectorBinding{"postgres_cdc_source", kChannelString, "json_string_to_row"};
        }
        return RowConnectorBinding{"postgres_source", kChannelString, "json_string_to_row"};
    }
    unsupported(
        "format='json' source requires connector='file', 'kafka', 'parquet', 'nexmark', "
        "'kinesis', 'http_poll', 'pubsub', 'redis', 'mysql', 'clickhouse' or 'postgres' (got '" +
        connector + "')");
}

RowConnectorBinding row_sink_binding_for(const TableDef& table) {
    const auto& connector = require_property(table, "connector");
    const bool upsert = table.is_upsert();
    const bool exactly_once = table.is_exactly_once();
    if (connector == "file" || connector == "filesystem") {
        // A partition_by WITH-option routes to the partitioning sink (one file
        // per distinct partition-key value). It is append-only, so reject a
        // combination with upsert / exactly-once rather than silently ignoring
        // those modes.
        if (table.properties.find("partition_by") != table.properties.end()) {
            if (upsert || exactly_once) {
                unsupported(
                    "connector='file' partition_by is append-only and cannot be combined "
                    "with mode='upsert' or exactly-once delivery");
            }
            return RowConnectorBinding{"partition_file_sink", kChannelRow, {}};
        }
        if (exactly_once) {
            // 2PC + upsert is rejected at bind time, so we know
            // we're routing an append stream into the 2PC sink.
            return RowConnectorBinding{"file_2pc_sink_row", kChannelRow, {}};
        }
        return RowConnectorBinding{
            upsert ? "file_json_upsert_sink" : "file_json_sink", kChannelRow, {}};
    }
    if (connector == "kafka") {
        if (exactly_once) {
            // Row -> row_to_json_string -> kafka_2pc_sink_string.
            // The transactional producer wraps each barrier-bounded
            // run of records in a librdkafka transaction.
            return RowConnectorBinding{
                "kafka_2pc_sink_string", kChannelString, "row_to_json_string"};
        }
        if (upsert) {
            // Row -> row_to_json_string -> kafka_upsert_sink_string
            // (the sink parses the row JSON to extract the PK and
            // tomb-stones on __row_kind=delete).
            return RowConnectorBinding{
                "kafka_upsert_sink_string", kChannelString, "row_to_json_string"};
        }
        return RowConnectorBinding{"kafka_sink_string", kChannelString, "row_to_json_string"};
    }
    if (connector == "rabbitmq") {
        // RabbitMQ / AMQP sink. Each row -> JSON object string -> basic.publish (persistent +
        // publisher confirms). At-least-once; RabbitMQ has no producer dedup key, so
        // exactly-once and upsert are not supported.
        if (exactly_once) {
            unsupported(
                "connector='rabbitmq' sink is at-least-once (publisher confirms; no producer "
                "dedup key); exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='rabbitmq' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"rabbitmq_sink_string", kChannelString, "row_to_json_string"};
    }
    if (connector == "nats") {
        // NATS JetStream sink. Each row -> JSON object string -> async publish (acks awaited at
        // each barrier). At-least-once; no producer dedup key set, so exactly-once and upsert
        // are not supported.
        if (exactly_once) {
            unsupported(
                "connector='nats' sink is at-least-once (publisher acks; no producer dedup key); "
                "exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='nats' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"nats_sink_string", kChannelString, "row_to_json_string"};
    }
    if (connector == "pulsar") {
        // Apache Pulsar sink. Each row -> JSON object string -> async publish (acks awaited at
        // each barrier). At-least-once; no producer dedup wired, so exactly-once and upsert are
        // not supported.
        if (exactly_once) {
            unsupported(
                "connector='pulsar' sink is at-least-once (publisher acks; producer dedup not "
                "wired); exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='pulsar' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"pulsar_sink_string", kChannelString, "row_to_json_string"};
    }
    if (connector == "clickhouse") {
        // ClickHouse sink (M1). Each row -> JSON object string -> the sink's
        // FORMAT JSONEachRow body. At-least-once (INSERT replay re-inserts;
        // ClickHouse has no row dedup). format=jsoneachrow is FORCED via the
        // binding because the SQL format='json' channel selector is stripped
        // before it reaches the sink factory (which would otherwise default to
        // TSV). exactly_once / upsert are not supported.
        if (exactly_once) {
            unsupported(
                "connector='clickhouse' sink is at-least-once; exactly-once delivery is not "
                "supported");
        }
        if (upsert) {
            unsupported("connector='clickhouse' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{
            "clickhouse_sink", kChannelString, "row_to_json_string", {{"format", "jsoneachrow"}}};
    }
    if (connector == "postgres") {
        // Postgres sink (M4). Each row -> JSON object string -> batched INSERT.
        // At-least-once. exactly_once (2PC) not supported. clink mode='upsert' is a
        // changelog contract (delete tombstones) this append sink does not
        // implement; use the WITH-option on_conflict='update' (+ conflict_columns)
        // for idempotent insert-or-update by key.
        if (exactly_once) {
            unsupported(
                "connector='postgres' sink is at-least-once; exactly-once delivery is not "
                "supported");
        }
        if (upsert) {
            unsupported(
                "connector='postgres' sink does not implement mode='upsert' (changelog deletes); "
                "use on_conflict='update' with conflict_columns for idempotent upsert by key");
        }
        return RowConnectorBinding{"postgres_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "http") {
        // HTTP(S) bulk / webhook sink (at-least-once; no 2PC). Each row is
        // rendered to a JSON object string then POSTed in batches by http_sink.
        if (exactly_once) {
            unsupported(
                "connector='http' sink is at-least-once; exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='http' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"http_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "pubsub") {
        // Google Cloud Pub/Sub publish sink (REST :publish). At-least-once;
        // Pub/Sub publish has no producer dedup key (a replay re-publishes). Each
        // row -> JSON object string -> base64 in a {"data":...} message.
        if (exactly_once) {
            unsupported(
                "connector='pubsub' sink is at-least-once (Pub/Sub publish has no producer "
                "dedup key); exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='pubsub' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"pubsub_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "elasticsearch" || connector == "opensearch") {
        // Bulk-index into Elasticsearch / OpenSearch (identical _bulk API).
        // At-least-once; set document_id for idempotent (effectively-once)
        // writes. Each row -> JSON object string -> NDJSON action+doc bulk body.
        if (exactly_once) {
            unsupported("connector='" + connector +
                        "' sink is at-least-once (use document_id for idempotent writes); "
                        "exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='" + connector + "' sink does not support mode='upsert'");
        }
        const std::string sink =
            connector == "opensearch" ? "opensearch_sink" : "elasticsearch_sink";
        return RowConnectorBinding{sink, kChannelString, "row_to_json_string"};
    }
    if (connector == "splunk_hec" || connector == "splunk") {
        // Splunk HTTP Event Collector. At-least-once; HEC ingestion is append
        // (re-delivery on replay indexes the event again). Each row -> JSON
        // object string -> wrapped in the HEC {"event":...} envelope.
        if (exactly_once) {
            unsupported("connector='" + connector +
                        "' (Splunk HEC) sink is at-least-once; exactly-once delivery is not "
                        "supported");
        }
        if (upsert) {
            unsupported("connector='" + connector + "' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"splunk_hec_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "prometheus") {
        // Prometheus Pushgateway. At-least-once; the gateway stores the latest
        // value per series. Each row -> JSON object string; the sink extracts
        // value_field as the gauge value and the other scalar fields as labels.
        if (exactly_once) {
            unsupported(
                "connector='prometheus' (Pushgateway) sink is at-least-once; exactly-once "
                "delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='prometheus' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"prometheus_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "influxdb") {
        // InfluxDB v2 line-protocol write (/api/v2/write). At-least-once;
        // effectively-once when a timestamp_field is set (an identical point is
        // idempotent). Each row -> JSON object string -> one line-protocol point
        // (every number a float field; `tags` names the tag columns).
        if (exactly_once) {
            unsupported(
                "connector='influxdb' sink is at-least-once (set timestamp_field for idempotent "
                "writes); exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='influxdb' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"influxdb_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "kinesis") {
        // Kinesis Data Streams sink (PutRecords). At-least-once; no producer
        // dedup key (a retry of throttled records may duplicate). Each row -> JSON
        // object string -> record Data; partition_key names the field used as the
        // Kinesis PartitionKey (shard routing).
        if (exactly_once) {
            unsupported(
                "connector='kinesis' sink is at-least-once (Kinesis has no producer dedup "
                "key); exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='kinesis' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"kinesis_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "redis") {
        // Redis Streams sink (XADD). At-least-once; XADD is append-only with no
        // producer dedup key (a replay re-appends). Each row -> JSON object string
        // -> stored under one stream field (default "v").
        if (exactly_once) {
            unsupported(
                "connector='redis' sink is at-least-once (Redis Streams XADD has no producer "
                "dedup key); exactly-once delivery is not supported");
        }
        if (upsert) {
            unsupported("connector='redis' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"redis_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "mysql") {
        // MySQL sink (batched INSERT). At-least-once. Each row -> JSON object
        // string -> columns by name. exactly_once (XA 2PC) is not supported.
        if (exactly_once) {
            unsupported(
                "connector='mysql' sink is at-least-once; exactly-once delivery is not supported");
        }
        if (upsert) {
            // clink mode='upsert' is a changelog contract (delete tombstones via
            // __row_kind) this append sink does not implement. For idempotent
            // insert-or-update by primary key on replay, use the WITH-option
            // on_duplicate='update' instead (INSERT ... ON DUPLICATE KEY UPDATE).
            unsupported(
                "connector='mysql' sink does not implement mode='upsert' (changelog deletes); "
                "use on_duplicate='update' for idempotent insert-or-update by primary key");
        }
        return RowConnectorBinding{"mysql_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "firehose") {
        // Amazon Data Firehose sink (PutRecordBatch). At-least-once; no partition
        // key and no ordering guarantee. Each row -> JSON object string -> record
        // Data (a newline is appended so the destination can split records).
        if (exactly_once) {
            unsupported(
                "connector='firehose' sink is at-least-once; exactly-once delivery is not "
                "supported");
        }
        if (upsert) {
            unsupported("connector='firehose' sink does not support mode='upsert'");
        }
        return RowConnectorBinding{"firehose_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "dynamodb") {
        // DynamoDB sink (BatchWriteItem). At-least-once; EFFECTIVELY-once when the
        // primary key is stable (PutItem upserts). Each row -> JSON object string
        // -> a DynamoDB item; partition_key (+ optional sort_key) name the key
        // attributes. exactly_once (2PC) is not offered, but the idempotent
        // overwrite is the effectively-once path - so reject exactly_once loudly.
        if (exactly_once) {
            unsupported(
                "connector='dynamodb' sink is at-least-once with idempotent overwrite "
                "(effectively-once via a stable primary key); 2PC exactly-once is not supported");
        }
        if (upsert) {
            // A DynamoDB put IS an upsert; mode='upsert' would be redundant, and
            // the engine's upsert path expects a primary_key/changelog contract
            // this sink does not implement. Reject rather than half-honour it.
            unsupported(
                "connector='dynamodb' sink already upserts by primary key; do not set "
                "mode='upsert' (set partition_key/sort_key instead)");
        }
        return RowConnectorBinding{"dynamodb_sink", kChannelString, "row_to_json_string"};
    }
    if (connector == "parquet") {
        // Typed-columnar Parquet. exactly_once routes to the 2PC variant
        // (staging/ + atomic commit on checkpoint); else one file/subtask.
        // upsert is not supported for the Parquet sink (append-only).
        if (upsert) {
            unsupported("connector='parquet' sink does not support mode='upsert'");
        }
        if (exactly_once) {
            return RowConnectorBinding{"parquet_row_2pc_sink", kChannelRow, {}};
        }
        return RowConnectorBinding{"parquet_row_sink", kChannelRow, {}};
    }
    if (connector == "delta") {
        // Delta Lake table sink: typed Parquet data files + the _delta_log
        // transaction log (clink::delta). Append-only, single-writer,
        // at-least-once - upsert and exactly-once are not supported in v1.
        if (upsert) {
            unsupported("connector='delta' sink does not support mode='upsert' (append-only v1)");
        }
        if (exactly_once) {
            unsupported(
                "connector='delta' sink is at-least-once in v1; exactly-once is not yet supported");
        }
        return RowConnectorBinding{"delta_row_sink", kChannelRow, {}};
    }
    if (connector == "iceberg") {
        // Apache Iceberg table sink (typed Parquet data files + Iceberg snapshots via
        // iceberg-cpp + a SQLite/REST catalog). Single-writer. Exactly-once is provided
        // automatically via two-phase commit when the job checkpoints; the explicit
        // exactly_once DDL flag (its source-replay coordination contract) is a follow-on,
        // so it is still rejected here.
        if (exactly_once) {
            unsupported(
                "connector='iceberg' provides exactly-once via 2PC when the job checkpoints; "
                "the explicit exactly_once flag is not yet wired");
        }
        if (upsert) {
            // mode='upsert': consume the changelog and maintain the table by primary key via
            // Iceberg v2 equality deletes. Needs a PRIMARY KEY (or primary_key=) -> equality_key.
            if (table.primary_key.empty()) {
                unsupported(
                    "connector='iceberg' mode='upsert' requires a PRIMARY KEY (or primary_key=)");
            }
            std::string keys;
            for (std::size_t i = 0; i < table.primary_key.size(); ++i) {
                keys += (i ? "," : "") + table.primary_key[i];
            }
            return RowConnectorBinding{
                "iceberg_row_sink", kChannelRow, "", {{"equality_key", keys}}};
        }
        return RowConnectorBinding{"iceberg_row_sink", kChannelRow, {}};
    }
    if (connector == "blackhole") {
        // Discard sink: counts + drops every row (the runner's records_in
        // metric still tallies them). For benchmarks where sink I/O must not
        // distort throughput. Factory registered out-of-tree (e.g. the Nexmark
        // harness); here we map the connector name to the Row-channel sink op.
        return RowConnectorBinding{"blackhole_sink_row", kChannelRow, {}};
    }
    if (connector == "changelog") {
        // Nets a changelog stream (insert/delete/update_*) into its final
        // relation by full-row multiplicity (no primary key), writing survivors
        // on flush. For keyless changelog outputs (e.g. a retracting aggregate
        // joined to another stream).
        return RowConnectorBinding{"changelog_net_sink", kChannelRow, {}};
    }
    unsupported(
        "format='json' sink requires connector='file', 'kafka', 'clickhouse', 'postgres', "
        "'parquet', 'http', 'pubsub', 'elasticsearch', 'opensearch', 'splunk_hec', 'prometheus', "
        "'kinesis', 'redis', 'mysql', 'firehose', 'dynamodb', 'blackhole' or 'changelog' (got '" +
        connector + "')");
}

// Copy properties from a TableDef to an OperatorSpec's params,
// excluding the connector= and format= keys (which selected the
// factory and channel respectively) and event-time keys (which the
// planner consumes to emit a separate assign_timestamps op).
std::map<std::string, std::string> build_params(const TableDef& table) {
    std::map<std::string, std::string> out;
    for (const auto& [k, v] : table.properties) {
        if (k == "connector" || k == "format")
            continue;
        if (k == "event_time_column" || k == "watermark_lag_ms")
            continue;
        out[k] = v;
    }
    return out;
}

// Source-side watermark wiring (Phase 4.1). When the source table
// declares an event_time_column, the planner emits an
// assign_timestamps_row op right after the scan so the rest of the
// chain has event-time + watermarks to align on.
std::optional<std::string> maybe_emit_assign_timestamps(const TableDef& table,
                                                        Channel ch,
                                                        const std::string& input_id,
                                                        cluster::JobGraphSpec& spec,
                                                        int& next_id) {
    if (ch != Channel::Row)
        return std::nullopt;  // String channel: no event-time today
    auto col_it = table.properties.find("event_time_column");
    if (col_it == table.properties.end() || col_it->second.empty())
        return std::nullopt;

    cluster::OperatorSpec op;
    op.id = "ts_" + std::to_string(next_id++);
    op.type = "assign_timestamps_row";
    op.inputs = {input_id};
    op.out_channel = std::string{kChannelRow};
    op.params["column"] = col_it->second;
    auto lag_it = table.properties.find("watermark_lag_ms");
    if (lag_it != table.properties.end() && !lag_it->second.empty()) {
        op.params["out_of_order_ms"] = lag_it->second;
    }
    std::string id = op.id;
    spec.ops.push_back(std::move(op));
    return id;
}

// Backstop: reject a plan that carries a null child anywhere in its tree before
// compile_node walks it. compile_node dereferences children unconditionally (e.g.
// EquiJoin::left() returns *left_), so a null child would be a null-this virtual
// call (a crash), not a clean error. A well-formed bound + optimized plan never
// has null children; this guards against an optimizer pass that throws mid-rewrite
// and leaves a moved-out (null) slot (see join_reorder reorder_subtree + the
// optimize() guard) - that path then surfaces here as a clean TranslationError
// instead of a downstream crash. inputs() exposes children via unique_ptr::get(),
// so inspecting it never dereferences a null.
void require_no_null_children(const LogicalPlan& node) {
    for (const auto* in : node.inputs()) {
        if (in == nullptr) {
            unsupported("internal planner error: '" + node.kind() +
                        "' node has a null child (an optimizer pass likely failed mid-rewrite); "
                        "the query cannot be compiled");
        }
        require_no_null_children(*in);
    }
}

// Compile-node carries the active channel down the tree. Every node
// in the chain emits ops on the same channel; root (Sink) decides it
// when it first compiles the source-side scan.
std::string compile_node(const LogicalPlan& node,
                         Channel ch,
                         cluster::JobGraphSpec& spec,
                         int& next_id,
                         bool async_agg) {
    if (node.kind() == "Scan") {
        const auto& scan = static_cast<const LogicalScan&>(node);
        const auto& table = scan.table();
        if (ch == Channel::String)
            check_string_channel_table(table);

        cluster::OperatorSpec op;
        op.id = "src_" + std::to_string(next_id++);
        op.params = build_params(table);
        // #56: tell a Row file source which columns are DECIMAL(p,s) so it tags
        // them exact at ingestion ("name:scale,..."). String/Kafka sources
        // (which route through a bridge) don't carry this yet.
        if (ch == Channel::Row) {
            std::string dec_csv;
            for (const auto& c : table.columns) {
                if (c.type && c.type->id() == arrow::Type::DECIMAL128) {
                    const auto& d = static_cast<const arrow::Decimal128Type&>(*c.type);
                    if (!dec_csv.empty())
                        dec_csv += ',';
                    dec_csv += c.name + ':' + std::to_string(d.scale());
                }
            }
            if (!dec_csv.empty())
                op.params["decimal_columns"] = std::move(dec_csv);
            // Typed-columnar connectors (parquet) read the full column
            // schema from here to build their Arrow batcher. Other Row
            // connectors ignore it.
            op.params["schema_columns"] = serialize_row_schema(row_columns_of(table));
        }
        if (!scan.projected_columns().empty()) {
            std::string csv;
            for (std::size_t i = 0; i < scan.projected_columns().size(); ++i) {
                if (i > 0)
                    csv += ',';
                csv += scan.projected_columns()[i];
            }
            op.params["projected_columns"] = std::move(csv);
        }

        std::string after_src;
        if (ch == Channel::Row) {
            auto binding = row_source_binding_for(table);
            op.type = binding.source_or_sink_op;
            op.out_channel = binding.source_or_sink_channel;
            std::string src_id = op.id;
            spec.ops.push_back(std::move(op));
            after_src = src_id;
            if (!binding.bridge_op.empty()) {
                cluster::OperatorSpec bridge;
                bridge.id = "bridge_" + std::to_string(next_id++);
                bridge.type = binding.bridge_op;
                bridge.inputs = {src_id};
                bridge.out_channel = std::string{kChannelRow};
                // The columnar JSON bridge builds its Arrow sidecar from the
                // declared column schema; the plain row bridge ignores it.
                if (binding.bridge_op == "json_string_to_row_columnar") {
                    bridge.params["schema_columns"] = serialize_row_schema(row_columns_of(table));
                }
                after_src = bridge.id;
                spec.ops.push_back(std::move(bridge));
            }
        } else {
            op.type = string_source_factory_for(table);
            op.out_channel = std::string{kChannelString};
            std::string src_id = op.id;
            spec.ops.push_back(std::move(op));
            after_src = src_id;
        }
        if (auto ts_id = maybe_emit_assign_timestamps(table, ch, after_src, spec, next_id)) {
            return *ts_id;
        }
        return after_src;
    }
    if (node.kind() == "EquiJoin") {
        // Phase 18: stream-stream equi-join. Mirrors the IntervalJoin
        // wiring: row_compute_key on each side keys by the join
        // column, then a co-op merges the keyed streams.
        const auto& jn = static_cast<const LogicalEquiJoin&>(node);
        if (ch != Channel::Row) {
            unsupported("equi-join requires format='json' Row channel on both sides");
        }
        std::string left_id = compile_node(jn.left(), ch, spec, next_id, async_agg);
        std::string right_id = compile_node(jn.right(), ch, spec, next_id, async_agg);

        auto emit_key = [&](const std::string& input, const std::string& column) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {input};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = column;
            std::string id = keyer.id;
            spec.ops.push_back(std::move(keyer));
            return id;
        };
        left_id = emit_key(left_id, jn.left_key_column());
        right_id = emit_key(right_id, jn.right_key_column());

        cluster::OperatorSpec op;
        op.id = "ejoin_" + std::to_string(next_id++);
        op.type = "equi_join_row";
        op.inputs = {std::move(left_id), std::move(right_id)};
        op.out_channel = std::string{kChannelRow};
        op.key_by = "row_key";
        op.params["left_key_column"] = jn.left_key_column();
        op.params["right_key_column"] = jn.right_key_column();
        op.params["left_alias"] = jn.left_alias();
        op.params["right_alias"] = jn.right_alias();
        switch (jn.join_type()) {
            case JoinType::Inner:
                op.params["join_type"] = "inner";
                break;
            case JoinType::LeftOuter:
                op.params["join_type"] = "left_outer";
                break;
            case JoinType::RightOuter:
                op.params["join_type"] = "right_outer";
                break;
            case JoinType::FullOuter:
                op.params["join_type"] = "full_outer";
                break;
        }
        // Outer joins null-pad the absent side, so the op needs each
        // side's column names (the scan schemas, unprefixed).
        auto columns_csv = [](const std::shared_ptr<arrow::Schema>& s) {
            std::string out;
            for (int i = 0; i < s->num_fields(); ++i) {
                if (i > 0)
                    out += ',';
                out += s->field(i)->name();
            }
            return out;
        };
        op.params["left_columns"] = columns_csv(jn.left().schema());
        op.params["right_columns"] = columns_csv(jn.right().schema());
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "LookupJoin") {
        // Lookup (enrichment) join: drive the probe stream through the
        // async_lookup_join_row operator, which calls the dim table's
        // registered Row -> Task<Row> function and merges aliased probe
        // + dim columns. INNER drops misses (null dim columns) with a
        // trailing IS NOT NULL filter on the dim key output column;
        // LEFT keeps every probe row.
        const auto& lj = static_cast<const LogicalLookupJoin&>(node);
        if (ch != Channel::Row) {
            unsupported("lookup join requires format='json' Row channel");
        }
        std::string input_id = compile_node(lj.input(), ch, spec, next_id, async_agg);

        auto join_csv = [](const std::vector<std::string>& cols) {
            std::string out;
            for (std::size_t i = 0; i < cols.size(); ++i) {
                if (i > 0)
                    out += ',';
                out += cols[i];
            }
            return out;
        };

        cluster::OperatorSpec op;
        op.id = "lookupjoin_" + std::to_string(next_id++);
        op.type = "async_lookup_join_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        op.params["function_name"] = lj.function_name();
        op.params["probe_alias"] = lj.probe_alias();
        op.params["dim_alias"] = lj.dim_alias();
        op.params["probe_columns"] = join_csv(lj.probe_columns());
        op.params["dim_columns"] = join_csv(lj.dim_columns());
        op.params["join_type"] = lj.outer() ? "left_outer" : "inner";
        std::string last_id = op.id;
        spec.ops.push_back(std::move(op));

        if (!lj.outer()) {
            cluster::OperatorSpec filt;
            filt.id = "lookupfilt_" + std::to_string(next_id++);
            filt.type = "filter_row_predicate";
            filt.inputs = {last_id};
            filt.out_channel = std::string{kChannelRow};
            const std::string col = lj.dim_alias() + "_" + lj.dim_key_column();
            filt.params["predicate"] = R"({"op":"is_not_null","col":")" + col + "\"}";
            last_id = filt.id;
            spec.ops.push_back(std::move(filt));
        }
        return last_id;
    }
    if (node.kind() == "SemiJoin") {
        // Inc 4: IN / NOT IN / EXISTS. Key both sides on the join column
        // then a semi_join_row co-op emits the left rows that (do not)
        // match, as changelog.
        const auto& jn = static_cast<const LogicalSemiJoin&>(node);
        if (ch != Channel::Row) {
            unsupported("semi/anti join requires format='json' Row channel on both sides");
        }
        std::string left_id = compile_node(jn.left(), ch, spec, next_id, async_agg);
        std::string right_id = compile_node(jn.right(), ch, spec, next_id, async_agg);
        auto emit_key = [&](const std::string& input, const std::string& column) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {input};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = column;
            std::string id = keyer.id;
            spec.ops.push_back(std::move(keyer));
            return id;
        };
        auto join_csv = [](const std::vector<std::string>& cols) {
            std::string out;
            for (std::size_t i = 0; i < cols.size(); ++i) {
                if (i > 0)
                    out += ',';
                out += cols[i];
            }
            return out;
        };
        const std::string left_cols = join_csv(jn.left_key_columns());
        const std::string right_cols = join_csv(jn.right_key_columns());
        // Composite keys co-partition by hashing the column tuple in order;
        // row_compute_key takes a CSV column list.
        left_id = emit_key(left_id, left_cols);
        right_id = emit_key(right_id, right_cols);
        cluster::OperatorSpec op;
        op.id = "semijoin_" + std::to_string(next_id++);
        op.type = "semi_join_row";
        op.inputs = {std::move(left_id), std::move(right_id)};
        op.out_channel = std::string{kChannelRow};
        op.key_by = "row_key";
        op.params["left_key_column"] = left_cols;
        op.params["right_key_column"] = right_cols;
        op.params["anti"] = jn.anti() ? "1" : "0";
        op.params["null_aware"] = jn.null_aware() ? "1" : "0";
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "ScalarBroadcast") {
        // Inc 4: uncorrelated scalar subquery. Runs at parallelism 1 (the
        // OperatorSpec default) so the single scalar value and every main
        // row reach the one instance; the op compares at EOS.
        const auto& sb = static_cast<const LogicalScalarBroadcast&>(node);
        if (ch != Channel::Row) {
            unsupported("scalar subquery requires format='json' Row channel");
        }
        std::string main_id = compile_node(sb.main(), ch, spec, next_id, async_agg);
        std::string scalar_id = compile_node(sb.scalar(), ch, spec, next_id, async_agg);
        cluster::OperatorSpec op;
        op.id = "scalarbcast_" + std::to_string(next_id++);
        op.type = "scalar_broadcast_filter_row";
        op.inputs = {std::move(main_id), std::move(scalar_id)};
        op.out_channel = std::string{kChannelRow};
        op.params["test_column"] = sb.test_column();
        op.params["comparison_op"] = sb.comparison_op();
        op.params["scalar_column"] = sb.scalar_column();
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "ScalarProject") {
        // Scalar subquery in the SELECT list. Parallelism 1 (default) so the
        // single scalar and every main row reach the one instance; the op
        // appends the scalar at EOS.
        const auto& sp = static_cast<const LogicalScalarProject&>(node);
        if (ch != Channel::Row) {
            unsupported("scalar subquery requires format='json' Row channel");
        }
        std::string main_id = compile_node(sp.main(), ch, spec, next_id, async_agg);
        std::string scalar_id = compile_node(sp.scalar(), ch, spec, next_id, async_agg);
        cluster::OperatorSpec op;
        op.id = "scalarproj_" + std::to_string(next_id++);
        op.type = "scalar_project_row";
        op.inputs = {std::move(main_id), std::move(scalar_id)};  // [0]=main, [1]=scalar
        op.out_channel = std::string{kChannelRow};
        op.params["output_column"] = sp.output_column();
        op.params["scalar_column"] = sp.scalar_column();
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "IntervalJoin") {
        const auto& jn = static_cast<const LogicalIntervalJoin&>(node);
        if (ch != Channel::Row) {
            unsupported("interval join requires format='json' Row channel on both sides");
        }
        std::string left_id = compile_node(jn.left(), ch, spec, next_id, async_agg);
        std::string right_id = compile_node(jn.right(), ch, spec, next_id, async_agg);

        // Phase 7: insert row_compute_key on each side so the
        // upstream routing layer hash-partitions records by the
        // join key. Without this, parallelism > 1 would route
        // same-key records to different subtasks and miss matches.
        auto emit_key = [&](const std::string& input, const std::string& column) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {input};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = column;
            std::string id = keyer.id;
            spec.ops.push_back(std::move(keyer));
            return id;
        };
        left_id = emit_key(left_id, jn.left_key_column());
        right_id = emit_key(right_id, jn.right_key_column());

        cluster::OperatorSpec op;
        op.id = "ijoin_" + std::to_string(next_id++);
        op.type = "interval_join_row";
        op.inputs = {std::move(left_id), std::move(right_id)};
        op.out_channel = std::string{kChannelRow};
        op.key_by = "row_key";
        op.params["left_key_column"] = jn.left_key_column();
        op.params["right_key_column"] = jn.right_key_column();
        op.params["left_ts_column"] = jn.left_ts_column();
        op.params["right_ts_column"] = jn.right_ts_column();
        op.params["left_alias"] = jn.left_alias();
        op.params["right_alias"] = jn.right_alias();
        op.params["lower_offset_ms"] = std::to_string(jn.lower_offset_ms());
        op.params["upper_offset_ms"] = std::to_string(jn.upper_offset_ms());
        switch (jn.join_type()) {
            case JoinType::Inner:
                op.params["join_type"] = "inner";
                break;
            case JoinType::LeftOuter:
                op.params["join_type"] = "left_outer";
                break;
            case JoinType::RightOuter:
                op.params["join_type"] = "right_outer";
                break;
            case JoinType::FullOuter:
                op.params["join_type"] = "full_outer";
                break;
        }
        // Outer interval joins null-pad the absent side, so the op needs each
        // side's (unprefixed) column names to fill nulls. Mirrors equi-join.
        auto ijoin_columns_csv = [](const std::shared_ptr<arrow::Schema>& s) {
            std::string out;
            for (int i = 0; i < s->num_fields(); ++i) {
                if (i > 0)
                    out += ',';
                out += s->field(i)->name();
            }
            return out;
        };
        op.params["left_columns"] = ijoin_columns_csv(jn.left().schema());
        op.params["right_columns"] = ijoin_columns_csv(jn.right().schema());
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Aggregate") {
        // Phase 8: unbounded GROUP BY. Same Row-channel constraint as
        // the windowed path; key-by routes by group columns so par>1
        // keeps each group on one subtask. Emits an upsert-style Row
        // per input record carrying the latest aggregate values.
        const auto& agg = static_cast<const LogicalAggregate&>(node);
        std::string input_id = compile_node(agg.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("unbounded GROUP BY requires format='json' Row channel");
        }

        if (!agg.group_keys().empty()) {
            std::string keys_csv;
            for (std::size_t i = 0; i < agg.group_keys().size(); ++i) {
                if (i > 0)
                    keys_csv += ',';
                keys_csv += agg.group_keys()[i];
            }
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = std::move(keys_csv);
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }

        cluster::OperatorSpec op;
        op.id = "agg_" + std::to_string(next_id++);
        op.type = "aggregate_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (async_agg)
            op.params["async_state"] = "true";
        if (!agg.group_keys().empty())
            op.key_by = "row_key";

        std::string keys_csv;
        for (std::size_t i = 0; i < agg.group_keys().size(); ++i) {
            if (i > 0)
                keys_csv += ',';
            keys_csv += agg.group_keys()[i];
        }
        op.params["group_keys"] = std::move(keys_csv);

        // group_key_outputs: the output column name per group key (parallel to
        // group_keys), so the aggregate emits the key under its SELECT alias.
        // Absent => the runtime falls back to the raw group_keys names.
        if (!agg.key_output_names().empty()) {
            std::string key_outs_csv;
            for (std::size_t i = 0; i < agg.key_output_names().size(); ++i) {
                if (i > 0)
                    key_outs_csv += ',';
                key_outs_csv += agg.key_output_names()[i];
            }
            op.params["group_key_outputs"] = std::move(key_outs_csv);
        }

        clink::config::JsonArray arr;
        for (const auto& a : agg.aggregates()) {
            clink::config::JsonObject obj;
            obj["name"] = clink::config::JsonValue{a.output_name};
            obj["fn"] = clink::config::JsonValue{a.agg_fn};
            obj["input_column"] = clink::config::JsonValue{a.input_column};
            obj["distinct"] = clink::config::JsonValue{a.distinct};
            obj["separator"] = clink::config::JsonValue{a.separator};
            obj["percentile"] = clink::config::JsonValue{a.percentile};
            arr.emplace_back(std::move(obj));
        }
        op.params["aggregates"] = clink::config::JsonValue{std::move(arr)}.serialize(0);

        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "MatchRecognize") {
        // #61 phase 2: PARTITION BY -> row_compute_key (hash routing); the
        // match_recognize_row op rebuilds a Pattern<Row> from the params and
        // drives CepOperator<Row,Row>. ONE ROW PER MATCH, append-only.
        const auto& mr = static_cast<const LogicalMatchRecognize&>(node);
        std::string input_id = compile_node(mr.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("MATCH_RECOGNIZE requires format='json' Row channel");
        }

        std::string keys_csv;
        for (std::size_t i = 0; i < mr.partition_columns().size(); ++i) {
            if (i > 0) {
                keys_csv += ',';
            }
            keys_csv += mr.partition_columns()[i];
        }
        if (!mr.partition_columns().empty()) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = keys_csv;
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }

        cluster::OperatorSpec op;
        op.id = "match_recognize_" + std::to_string(next_id++);
        op.type = "match_recognize_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (!mr.partition_columns().empty()) {
            op.key_by = "row_key";
        }
        op.params["partition_keys"] = keys_csv;
        op.params["order_column"] = mr.order_column();

        clink::config::JsonArray pat;
        for (const auto& s : mr.pattern()) {
            clink::config::JsonObject o;
            o["name"] = clink::config::JsonValue{s.name};
            o["min"] = clink::config::JsonValue{static_cast<double>(s.min_count)};
            o["max"] = clink::config::JsonValue{static_cast<double>(s.max_count)};
            pat.emplace_back(std::move(o));
        }
        op.params["pattern"] = clink::config::JsonValue{std::move(pat)}.serialize(0);

        clink::config::JsonArray defs;
        for (const auto& d : mr.defines()) {
            clink::config::JsonObject o;
            o["var"] = clink::config::JsonValue{d.var};
            o["predicate"] = clink::config::parse(d.predicate_json);
            defs.emplace_back(std::move(o));
        }
        op.params["defines"] = clink::config::JsonValue{std::move(defs)}.serialize(0);

        clink::config::JsonArray meas;
        for (const auto& m : mr.measures()) {
            clink::config::JsonObject o;
            o["name"] = clink::config::JsonValue{m.output_name};
            o["fn"] = clink::config::JsonValue{m.fn};
            o["var"] = clink::config::JsonValue{m.var};
            o["column"] = clink::config::JsonValue{m.column};
            meas.emplace_back(std::move(o));
        }
        op.params["measures"] = clink::config::JsonValue{std::move(meas)}.serialize(0);

        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "ProcessTableFunction") {
        // SQLOPT PTF: PARTITION BY -> row_compute_key (hash routing); the
        // process_table_function_row op resolves the registered
        // KeyedProcessFunction<string,Row,Row> by name and drives it.
        const auto& ptf = static_cast<const LogicalProcessTableFunction&>(node);
        std::string input_id = compile_node(ptf.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("process table function requires format='json' Row channel");
        }
        std::string keys_csv;
        for (std::size_t i = 0; i < ptf.partition_columns().size(); ++i) {
            if (i > 0) {
                keys_csv += ',';
            }
            keys_csv += ptf.partition_columns()[i];
        }
        // PARTITION BY -> keyer + key_by together (or neither = per-subtask).
        if (!ptf.partition_columns().empty()) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = keys_csv;
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }
        cluster::OperatorSpec op;
        op.id = "ptf_" + std::to_string(next_id++);
        op.type = "process_table_function_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (!ptf.partition_columns().empty()) {
            op.key_by = "row_key";
        }
        op.params["function_name"] = ptf.fn_name();
        op.params["partition_keys"] = keys_csv;
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "WindowAggregate") {
        const auto& agg = static_cast<const LogicalWindowAggregate&>(node);
        std::string input_id = compile_node(agg.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("windowed aggregation requires format='json' Row channel");
        }
        const auto& w = agg.window();

        // Phase 7: insert row_compute_key upstream so the routing
        // layer hash-partitions by the group keys. Phase 7.x makes
        // this a multi-column hash so GROUP BY a, b lands every
        // (a, b) tuple on the same subtask at par > 1.
        if (!agg.group_keys().empty()) {
            std::string keys_csv;
            for (std::size_t i = 0; i < agg.group_keys().size(); ++i) {
                if (i > 0)
                    keys_csv += ',';
                keys_csv += agg.group_keys()[i];
            }
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = std::move(keys_csv);
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }

        cluster::OperatorSpec op;
        op.id = "agg_" + std::to_string(next_id++);
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (!agg.group_keys().empty())
            op.key_by = "row_key";
        op.params["time_column"] = w.time_column;
        switch (w.kind) {
            case WindowSpec::Kind::Tumble:
                op.type = "tumbling_window_row";
                op.params["size_ms"] = std::to_string(w.size_ms);
                break;
            case WindowSpec::Kind::Hop:
                op.type = "hopping_window_row";
                op.params["size_ms"] = std::to_string(w.size_ms);
                op.params["slide_ms"] = std::to_string(w.slide_ms);
                break;
            case WindowSpec::Kind::Session:
                op.type = "session_window_row";
                op.params["gap_ms"] = std::to_string(w.gap_ms);
                break;
            case WindowSpec::Kind::Cumulate:
                op.type = "cumulate_window_row";
                op.params["size_ms"] = std::to_string(w.size_ms);
                op.params["step_ms"] = std::to_string(w.step_ms);
                break;
        }

        // group_keys: comma-separated list.
        std::string keys_csv;
        for (std::size_t i = 0; i < agg.group_keys().size(); ++i) {
            if (i > 0)
                keys_csv += ',';
            keys_csv += agg.group_keys()[i];
        }
        op.params["group_keys"] = std::move(keys_csv);

        // group_key_outputs: the output column name per group key (parallel to
        // group_keys), so the aggregate emits the key under its SELECT alias.
        // Absent => the runtime falls back to the raw group_keys names.
        if (!agg.key_output_names().empty()) {
            std::string key_outs_csv;
            for (std::size_t i = 0; i < agg.key_output_names().size(); ++i) {
                if (i > 0)
                    key_outs_csv += ',';
                key_outs_csv += agg.key_output_names()[i];
            }
            op.params["group_key_outputs"] = std::move(key_outs_csv);
        }

        // window_start_output / window_end_output: the output column name for
        // each projected window bound (so SELECT window_start AS st emits "st").
        // Absent => the runtime emits the bound under its literal name.
        if (!agg.window_start_output().empty())
            op.params["window_start_output"] = agg.window_start_output();
        if (!agg.window_end_output().empty())
            op.params["window_end_output"] = agg.window_end_output();

        // aggregates: JSON array of {name, fn, input_column}.
        clink::config::JsonArray arr;
        for (const auto& a : agg.aggregates()) {
            clink::config::JsonObject obj;
            obj["name"] = clink::config::JsonValue{a.output_name};
            obj["fn"] = clink::config::JsonValue{a.agg_fn};
            obj["input_column"] = clink::config::JsonValue{a.input_column};
            obj["distinct"] = clink::config::JsonValue{a.distinct};
            obj["separator"] = clink::config::JsonValue{a.separator};
            obj["percentile"] = clink::config::JsonValue{a.percentile};
            arr.emplace_back(std::move(obj));
        }
        op.params["aggregates"] = clink::config::JsonValue{std::move(arr)}.serialize(0);

        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Union") {
        // Phase 13: UNION ALL. Compile both sides on the same Row
        // channel and feed both into the union_row co-op. Schema is
        // identical to either side (binder validates).
        const auto& un = static_cast<const LogicalUnion&>(node);
        if (ch != Channel::Row) {
            unsupported("UNION ALL requires format='json' Row channel");
        }
        std::string left_id = compile_node(un.left(), ch, spec, next_id, async_agg);
        std::string right_id = compile_node(un.right(), ch, spec, next_id, async_agg);
        cluster::OperatorSpec op;
        op.id = "uni_" + std::to_string(next_id++);
        op.type = "union_row";
        op.inputs = {std::move(left_id), std::move(right_id)};
        op.out_channel = std::string{kChannelRow};
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "SetOp") {
        // INTERSECT / EXCEPT (distinct). Key both sides by their own
        // columns (positional: set ops match by position, not name, so
        // the keyer hashes values in column order) so equal rows
        // co-partition, then a set_op_row co-op tracks per-key presence
        // and emits the distinct result. INTERSECT is insert-only;
        // EXCEPT is changelog (emit a left row, retract when the right
        // later produces the same row).
        const auto& so = static_cast<const LogicalSetOp&>(node);
        if (ch != Channel::Row) {
            unsupported("set operation requires format='json' Row channel on both sides");
        }
        std::string left_id = compile_node(so.left(), ch, spec, next_id, async_agg);
        std::string right_id = compile_node(so.right(), ch, spec, next_id, async_agg);

        auto all_columns_csv = [](const std::shared_ptr<arrow::Schema>& s) {
            std::string out;
            for (int i = 0; s && i < s->num_fields(); ++i) {
                if (i > 0)
                    out += ',';
                out += s->field(i)->name();
            }
            return out;
        };
        const std::string left_cols = all_columns_csv(so.left().schema());
        const std::string right_cols = all_columns_csv(so.right().schema());
        auto emit_key = [&](const std::string& input, const std::string& cols) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {input};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = cols;
            std::string id = keyer.id;
            spec.ops.push_back(std::move(keyer));
            return id;
        };
        left_id = emit_key(left_id, left_cols);
        right_id = emit_key(right_id, right_cols);

        cluster::OperatorSpec op;
        op.id = "setop_" + std::to_string(next_id++);
        op.type = "set_op_row";
        op.inputs = {std::move(left_id), std::move(right_id)};
        op.out_channel = std::string{kChannelRow};
        op.key_by = "row_key";
        op.params["mode"] = so.is_except() ? "except" : "intersect";
        op.params["all"] = so.is_all() ? "true" : "false";
        op.params["left_columns"] = left_cols;
        op.params["right_columns"] = right_cols;
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "TopN") {
        const auto& topn = static_cast<const LogicalTopN&>(node);
        std::string input_id = compile_node(topn.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("ORDER BY requires format='json' Row channel");
        }
        cluster::OperatorSpec op;
        op.id = "tpn_" + std::to_string(next_id++);
        op.type = "top_n_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        std::string cols_csv;
        std::string descs_csv;
        for (std::size_t i = 0; i < topn.sort_columns().size(); ++i) {
            if (i > 0) {
                cols_csv += ',';
                descs_csv += ',';
            }
            cols_csv += topn.sort_columns()[i];
            descs_csv += topn.sort_descending()[i] ? "1" : "0";
        }
        op.params["sort_columns"] = std::move(cols_csv);
        op.params["sort_descending"] = std::move(descs_csv);
        op.params["count"] = std::to_string(topn.count());
        if (topn.offset() > 0) {
            op.params["offset"] = std::to_string(topn.offset());
        }
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Limit") {
        // Phase 11: LIMIT n. Pass-through schema; runtime emits the
        // first n records and drops the rest. Per-subtask semantics
        // at parallelism > 1 - same caveat as the runtime op.
        const auto& lim = static_cast<const LogicalLimit&>(node);
        std::string input_id = compile_node(lim.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("LIMIT requires format='json' Row channel");
        }
        cluster::OperatorSpec op;
        op.id = "lim_" + std::to_string(next_id++);
        op.type = "limit_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        op.params["count"] = std::to_string(lim.count());
        if (lim.offset() > 0) {
            op.params["offset"] = std::to_string(lim.offset());
        }
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Distinct") {
        // Phase 10: dedupe by every input column. For parallelism > 1
        // we must keyBy all columns so each unique row lands on a
        // deterministic subtask and the per-subtask seen-set is
        // sufficient. String-channel DISTINCT is not yet supported.
        const auto& d = static_cast<const LogicalDistinct&>(node);
        std::string input_id = compile_node(d.input(), ch, spec, next_id, async_agg);
        if (ch != Channel::Row) {
            unsupported("SELECT DISTINCT requires format='json' Row channel");
        }
        auto in_schema = d.input().schema();
        std::string columns_csv;
        for (int i = 0; in_schema && i < in_schema->num_fields(); ++i) {
            if (i > 0)
                columns_csv += ',';
            columns_csv += in_schema->field(i)->name();
        }
        if (!columns_csv.empty()) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = std::move(columns_csv);
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }
        cluster::OperatorSpec op;
        op.id = "dst_" + std::to_string(next_id++);
        op.type = "distinct_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        op.key_by = "row_key";
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Filter") {
        const auto& filter = static_cast<const LogicalFilter&>(node);
        std::string input_id = compile_node(filter.input(), ch, spec, next_id, async_agg);
        cluster::OperatorSpec op;
        op.id = "filt_" + std::to_string(next_id++);
        op.type = ch == Channel::Row ? "filter_row_predicate" : "filter_string_predicate";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{channel_name(ch)};
        op.params["predicate"] = filter.predicate_json();
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "AsyncMap") {
        // Phase 28c-frontend: lower a registered async lookup to the
        // async_lookup_row runtime operator. The operator drives the
        // Row -> async::Task<Row> coroutine looked up by function_name
        // in AsyncFunctionRegistry::global(). Row channel only (the
        // enrichment is whole-row).
        const auto& amap = static_cast<const LogicalAsyncMap&>(node);
        std::string input_id = compile_node(amap.input(), ch, spec, next_id, async_agg);
        cluster::OperatorSpec op;
        op.id = "async_" + std::to_string(next_id++);
        op.type = "async_lookup_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{channel_name(ch)};
        op.params["function_name"] = amap.function_name();
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Project") {
        const auto& proj = static_cast<const LogicalProject&>(node);
        std::string input_id = compile_node(proj.input(), ch, spec, next_id, async_agg);
        cluster::OperatorSpec op;
        op.id = "proj_" + std::to_string(next_id++);
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{channel_name(ch)};

        const auto& outputs = proj.outputs();
        // identity_string fast path: single output that's a bare
        // {"col": "<source-name>"} reference into a single-column
        // string-channel scan. Anything else takes the project_row
        // path even on the string channel - we promote to Row.
        bool is_string_identity = ch == Channel::String && outputs.size() == 1 &&
                                  outputs[0].expr_json.find("\"col\"") != std::string::npos &&
                                  outputs[0].expr_json.find("\"op\"") == std::string::npos;
        if (is_string_identity) {
            op.type = "identity_string";
        } else if (ch == Channel::String) {
            unsupported(
                "string-channel projection must be the single source column; declare "
                "format='json' to use multi-column / expression projections");
        } else {
            // Row channel: emit project_row with the outputs JSON.
            clink::config::JsonArray arr;
            arr.reserve(outputs.size());
            for (const auto& o : outputs) {
                clink::config::JsonObject obj;
                obj["name"] = clink::config::JsonValue{o.name};
                obj["expr"] = clink::config::parse(o.expr_json);
                arr.emplace_back(std::move(obj));
            }
            op.type = "project_row";
            op.params["outputs"] = clink::config::JsonValue{std::move(arr)}.serialize(0);
        }
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "Sink") {
        const auto& sink = static_cast<const LogicalSink&>(node);
        const auto& table = sink.table();
        if (ch == Channel::String)
            check_string_channel_table(table);
        std::string input_id = compile_node(sink.input(), ch, spec, next_id, async_agg);

        if (ch == Channel::Row) {
            auto binding = row_sink_binding_for(table);
            // If the connector consumes std::string (kafka_sink_string),
            // emit a bridge first so the Row chain ends with a JSON
            // serialiser before reaching the sink.
            std::string before_sink = std::move(input_id);
            if (!binding.bridge_op.empty()) {
                cluster::OperatorSpec bridge;
                bridge.id = "bridge_" + std::to_string(next_id++);
                bridge.type = binding.bridge_op;
                bridge.inputs = {std::move(before_sink)};
                bridge.out_channel = binding.source_or_sink_channel;
                before_sink = bridge.id;
                spec.ops.push_back(std::move(bridge));
            }
            cluster::OperatorSpec op;
            op.id = "snk_" + std::to_string(next_id++);
            op.type = binding.source_or_sink_op;
            op.inputs = {std::move(before_sink)};
            op.out_channel = binding.source_or_sink_channel;
            op.params = build_params(table);
            for (const auto& [k, v] : binding.extra_params) {
                op.params[k] = v;  // binding-forced params override passed-through WITH-options
            }
            // #56: tell the sink which columns are DECIMAL(p,s) so it quantises
            // the value to the column scale on assignment and renders clean.
            std::string dec_csv;
            for (const auto& c : table.columns) {
                if (c.type && c.type->id() == arrow::Type::DECIMAL128) {
                    const auto& d = static_cast<const arrow::Decimal128Type&>(*c.type);
                    if (!dec_csv.empty())
                        dec_csv += ',';
                    dec_csv += c.name + ':' + std::to_string(d.scale());
                }
            }
            if (!dec_csv.empty())
                op.params["decimal_columns"] = std::move(dec_csv);
            // Typed-columnar sinks (parquet) build their Arrow batcher from
            // the full column schema; other Row sinks ignore it.
            op.params["schema_columns"] = serialize_row_schema(row_columns_of(table));
            std::string id = op.id;
            spec.ops.push_back(std::move(op));
            return id;
        }
        cluster::OperatorSpec op;
        op.id = "snk_" + std::to_string(next_id++);
        op.type = string_sink_factory_for(table);
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelString};
        op.params = build_params(table);
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "OverAggregate") {
        // OVER (running) aggregate: key by the partition columns (so
        // par>1 keeps a partition on one subtask) then an
        // over_aggregate_row op that emits one append-only Row per input
        // once the watermark passes its event time.
        const auto& ov = static_cast<const LogicalOverAggregate&>(node);
        if (ch != Channel::Row) {
            unsupported("OVER aggregates require format='json' Row channel");
        }
        std::string input_id = compile_node(ov.input(), ch, spec, next_id, async_agg);
        std::string part_csv;
        for (std::size_t i = 0; i < ov.partition_columns().size(); ++i) {
            if (i > 0)
                part_csv += ',';
            part_csv += ov.partition_columns()[i];
        }
        if (!ov.partition_columns().empty()) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = part_csv;
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }
        cluster::OperatorSpec op;
        op.id = "over_" + std::to_string(next_id++);
        op.type = "over_aggregate_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (!ov.partition_columns().empty())
            op.key_by = "row_key";
        op.params["partition_columns"] = part_csv;
        op.params["time_column"] = ov.order_time_column();
        clink::config::JsonArray arr;
        for (const auto& o : ov.outputs()) {
            clink::config::JsonObject obj;
            obj["name"] = clink::config::JsonValue{o.output_name};
            obj["fn"] = clink::config::JsonValue{o.fn};
            obj["input_column"] = clink::config::JsonValue{o.input_column};
            obj["lag_offset"] = clink::config::JsonValue{static_cast<std::int64_t>(o.lag_offset)};
            obj["frame_mode"] = clink::config::JsonValue{static_cast<std::int64_t>(o.frame_mode)};
            obj["frame_start"] = clink::config::JsonValue{static_cast<std::int64_t>(o.frame_start)};
            arr.emplace_back(std::move(obj));
        }
        op.params["outputs"] = clink::config::JsonValue{std::move(arr)}.serialize(0);
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "LastNAgg") {
        // Last-N-per-key rolling aggregate: key by the partition columns (so
        // par>1 keeps a key on one subtask) then a last_n_agg_row op that
        // emits a per-key changelog over the most-recent N elements.
        const auto& ln = static_cast<const LogicalLastNAgg&>(node);
        if (ch != Channel::Row) {
            unsupported("last-N window aggregates require format='json' Row channel");
        }
        std::string input_id = compile_node(ln.input(), ch, spec, next_id, async_agg);
        std::string part_csv;
        for (std::size_t i = 0; i < ln.partition_columns().size(); ++i) {
            if (i > 0)
                part_csv += ',';
            part_csv += ln.partition_columns()[i];
        }
        if (!ln.partition_columns().empty()) {
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = part_csv;
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }
        cluster::OperatorSpec op;
        op.id = "lastn_" + std::to_string(next_id++);
        op.type = "last_n_agg_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (!ln.partition_columns().empty())
            op.key_by = "row_key";
        op.params["partition_columns"] = part_csv;
        op.params["order_column"] = ln.order_column();
        clink::config::JsonArray arr;
        for (const auto& o : ln.outputs()) {
            clink::config::JsonObject obj;
            obj["name"] = clink::config::JsonValue{o.output_name};
            obj["fn"] = clink::config::JsonValue{o.fn};
            obj["input_column"] = clink::config::JsonValue{o.input_column};
            obj["frame_start"] = clink::config::JsonValue{static_cast<std::int64_t>(o.frame_start)};
            arr.emplace_back(std::move(obj));
        }
        op.params["outputs"] = clink::config::JsonValue{std::move(arr)}.serialize(0);
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "TopNPerKey") {
        // Phase 21c: row_compute_key over the partition columns
        // (Hash routing keeps a partition's records on one subtask),
        // then top_n_per_key_row co-... single-input op emitting
        // changelog rows tagged via __row_kind.
        const auto& tn = static_cast<const LogicalTopNPerKey&>(node);
        if (ch != Channel::Row) {
            unsupported("ROW_NUMBER TOP-N requires format='json' Row channel");
        }
        std::string input_id = compile_node(tn.input(), ch, spec, next_id, async_agg);
        if (!tn.partition_columns().empty()) {
            std::string keys_csv;
            for (std::size_t i = 0; i < tn.partition_columns().size(); ++i) {
                if (i > 0)
                    keys_csv += ',';
                keys_csv += tn.partition_columns()[i];
            }
            cluster::OperatorSpec keyer;
            keyer.id = "key_" + std::to_string(next_id++);
            keyer.type = "row_compute_key";
            keyer.inputs = {std::move(input_id)};
            keyer.out_channel = std::string{kChannelRow};
            keyer.params["columns"] = std::move(keys_csv);
            input_id = keyer.id;
            spec.ops.push_back(std::move(keyer));
        }
        cluster::OperatorSpec op;
        op.id = "tnpk_" + std::to_string(next_id++);
        op.type = "top_n_per_key_row";
        op.inputs = {std::move(input_id)};
        op.out_channel = std::string{kChannelRow};
        if (!tn.partition_columns().empty())
            op.key_by = "row_key";
        std::string part_csv;
        for (std::size_t i = 0; i < tn.partition_columns().size(); ++i) {
            if (i > 0)
                part_csv += ',';
            part_csv += tn.partition_columns()[i];
        }
        std::string cols_csv;
        std::string descs_csv;
        for (std::size_t i = 0; i < tn.sort_columns().size(); ++i) {
            if (i > 0) {
                cols_csv += ',';
                descs_csv += ',';
            }
            cols_csv += tn.sort_columns()[i];
            descs_csv += tn.sort_descending()[i] ? "1" : "0";
        }
        op.params["partition_columns"] = std::move(part_csv);
        op.params["sort_columns"] = std::move(cols_csv);
        op.params["sort_descending"] = std::move(descs_csv);
        op.params["count"] = std::to_string(tn.count());
        switch (tn.rank_kind()) {
            case RankKind::RowNumber:
                op.params["rank_kind"] = "row_number";
                break;
            case RankKind::Rank:
                op.params["rank_kind"] = "rank";
                break;
            case RankKind::DenseRank:
                op.params["rank_kind"] = "dense_rank";
                break;
        }
        std::string id = op.id;
        spec.ops.push_back(std::move(op));
        return id;
    }
    if (node.kind() == "RowNumber") {
        // Phase 21b: ROW_NUMBER() at the top level isn't a valid
        // runtime shape - emitting per-record ranks over an unbounded
        // stream needs unbounded state. Phase 21c rewrites the
        // bounded `WHERE rn <= N` pattern into LogicalTopNPerKey;
        // anything else is rejected here with a clear message.
        unsupported(
            "ROW_NUMBER() OVER must be paired with a 'WHERE rn <= N' filter in an enclosing "
            "SELECT (Phase 21c)");
    }
    unsupported("PhysicalPlanner: unsupported LogicalPlan kind '" + node.kind() + "'");
}

// Find the root scan of a plan tree and decide the channel from its
// table. The whole chain runs on that one channel; the sink table
// must agree (no implicit conversions in Phase 3.2).
Channel decide_channel(const LogicalPlan& node) {
    if (node.kind() == "Scan") {
        return channel_for_table(static_cast<const LogicalScan&>(node).table());
    }
    auto inputs = node.inputs();
    if (inputs.empty()) {
        unsupported("PhysicalPlanner: cannot determine channel - no scan in plan");
    }
    return decide_channel(*inputs[0]);
}

// Post-pass over the built operator graph: mark every aggregate_row that feeds a
// changelog-CONSUMING op (a netting/upsert sink, or a retraction-aware join) so
// it emits update_before/update_after instead of an append snapshot. Walks back
// from each consumer through changelog-preserving pass-through ops to the first
// aggregate_row on that input. Opt-in by construction: an aggregate whose output
// only reaches append sinks is never marked, so append pipelines are unchanged.
void mark_changelog_producers(cluster::JobGraphSpec& spec) {
    std::unordered_map<std::string, std::size_t> by_id;
    for (std::size_t i = 0; i < spec.ops.size(); ++i) {
        by_id[spec.ops[i].id] = i;
    }
    auto is_consumer = [](const std::string& t) {
        // aggregate_row consumes a changelog (fold_into_ is retraction-aware), so a
        // STACKED aggregate (an aggregate over another aggregate, e.g. AVG over a
        // per-key MAX) needs the inner one to emit a changelog - else the outer
        // double-counts the inner's intermediate upsert values.
        return t == "changelog_net_sink" || t == "equi_join_row" || t == "aggregate_row" ||
               t == "last_n_agg_row";
    };
    auto is_passthrough = [](const std::string& t) {
        // Ops that forward __row_kind unchanged. union_row is multi-input, so the
        // walk below follows ALL inputs of a pass-through, not just the first.
        return t == "row_compute_key" || t == "filter_row_predicate" || t == "project_row" ||
               t == "identity_row" || t == "assign_timestamps_row" || t == "union_row" ||
               t == "async_lookup_join_row";
    };
    // Walk back (breadth-first over all inputs) from each `start` through
    // pass-throughs, marking every aggregate_row reached. The graph is a DAG;
    // marking is idempotent, so a diamond merely re-marks. The iteration guard is
    // a runaway backstop.
    auto mark_from = [&](const std::string& start) {
        std::vector<std::string> work{start};
        std::size_t guard = 0;
        const std::size_t cap = spec.ops.size() * 4 + 16;
        while (!work.empty() && guard++ < cap) {
            const std::string cur = work.back();
            work.pop_back();
            auto it = by_id.find(cur);
            if (it == by_id.end()) {
                continue;
            }
            auto& up = spec.ops[it->second];
            if (up.type == "aggregate_row") {
                up.params["emit_changelog"] = "true";
                continue;
            }
            if (is_passthrough(up.type)) {
                for (const auto& in : up.inputs) {
                    work.push_back(in);
                }
            }
            // else: not a pass-through and not an aggregate: stop this branch.
        }
    };
    for (const auto& op : spec.ops) {
        if (!is_consumer(op.type)) {
            continue;
        }
        for (const auto& in_id : op.inputs) {
            mark_from(in_id);
        }
    }
}

}  // namespace

cluster::JobGraphSpec PhysicalPlanner::compile(const LogicalSink& root) const {
    const auto t0 = std::chrono::steady_clock::now();
    cluster::JobGraphSpec spec;
    int next_id = 0;
    // Reject a malformed plan (a null child anywhere) before anything walks the
    // tree - decide_channel/compile_node both dereference children unconditionally,
    // so this must run first. A well-formed plan never trips it; it converts a
    // mid-rewrite optimizer failure (null slot) into a clean error, not a crash.
    require_no_null_children(root);
    // Determine the channel from the source side first; cross-check
    // against the sink so users get a clear error before deploy time.
    Channel ch = decide_channel(root);
    Channel sink_ch = channel_for_table(root.table());
    if (ch != sink_ch) {
        unsupported(
            "source and sink tables must use the same channel (string vs row): "
            "either both single-TEXT-column or both format='json'");
    }
    compile_node(root, ch, spec, next_id, async_state_for_aggregation_);
    mark_changelog_producers(spec);
    spec.validate();
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::sql::physical_plan_completed(static_cast<std::uint64_t>(dt));
    return spec;
}

ScanSourceSpec row_scan_source_spec(const TableDef& table) {
    auto binding = row_source_binding_for(table);  // throws for unsupported connectors
    if (!binding.bridge_op.empty()) {
        unsupported("ANALYZE: table " + table.name + " uses connector '" +
                    require_property(table, "connector") +
                    "', a string-channel source that needs a string->row bridge and is not a "
                    "bounded scan; ANALYZE supports bounded Row sources (file/json, parquet)");
    }
    ScanSourceSpec spec;
    spec.type = binding.source_or_sink_op;
    spec.params = build_params(table);
    std::string dec_csv;
    for (const auto& c : table.columns) {
        if (c.type && c.type->id() == arrow::Type::DECIMAL128) {
            const auto& d = static_cast<const arrow::Decimal128Type&>(*c.type);
            if (!dec_csv.empty()) {
                dec_csv += ',';
            }
            dec_csv += c.name + ':' + std::to_string(d.scale());
        }
    }
    if (!dec_csv.empty()) {
        spec.params["decimal_columns"] = std::move(dec_csv);
    }
    spec.params["schema_columns"] = serialize_row_schema(row_columns_of(table));
    return spec;
}

}  // namespace clink::sql
