#include "clink/cluster/connector_availability.hpp"

#include <algorithm>
#include <array>
#include <vector>

#include "clink/connectors/capability.hpp"

namespace clink::cluster {

namespace {

// Longest prefix first, so s3_parquet wins over s3 and http_poll over
// http. Read off impls/*/register_factories.cpp (capability names) and
// the CLINK_WITH_* options in the root CMakeLists; the always-built
// connectors (file, parquet, generator, blackhole, changelog) are
// deliberately absent - they cannot be missing, so they never need a
// diagnostic. test_connector_availability pins that every DECLARED
// capability in a full build resolves through this table or is
// always-built, so a new connector cannot land without its entry.
constexpr std::array<ConnectorVocabularyEntry, 32> kVocabulary{{
    {"webhdfs_parquet", "webhdfs_parquet", "CLINK_WITH_WEBHDFS"},
    {"azure_parquet", "azure_parquet", "CLINK_WITH_AZURE"},
    {"gcs_parquet", "gcs_parquet", "CLINK_WITH_GCS"},
    {"s3_parquet", "s3_parquet", "CLINK_WITH_AWS_S3"},
    {"elasticsearch", "elasticsearch", "CLINK_WITH_HTTP"},
    {"splunk_hec", "splunk_hec", "CLINK_WITH_HTTP"},
    {"opensearch", "opensearch", "CLINK_WITH_HTTP"},
    {"prometheus", "prometheus", "CLINK_WITH_HTTP"},
    {"influxdb", "influxdb", "CLINK_WITH_HTTP"},
    {"http_poll", "http_poll", "CLINK_WITH_HTTP"},
    {"clickhouse", "clickhouse", "CLINK_WITH_CLICKHOUSE"},
    {"cassandra", "cassandra", "CLINK_WITH_CASSANDRA"},
    {"websocket", "websocket", "CLINK_WITH_WEBSOCKET"},
    {"rabbitmq", "rabbitmq", "CLINK_WITH_RABBITMQ"},
    {"dynamodb", "dynamodb", "CLINK_WITH_AWS"},
    {"firehose", "firehose", "CLINK_WITH_AWS"},
    {"postgres", "postgres", "CLINK_WITH_POSTGRES"},
    {"kinesis", "kinesis", "CLINK_WITH_AWS"},
    {"iceberg", "iceberg", "CLINK_WITH_ICEBERG"},
    {"mongodb", "mongodb", "CLINK_WITH_MONGODB"},
    {"pulsar", "pulsar", "CLINK_WITH_PULSAR"},
    {"pubsub", "pubsub", "CLINK_WITH_HTTP"},
    {"mongo", "mongodb", "CLINK_WITH_MONGODB"},
    {"kafka", "kafka", "CLINK_WITH_KAFKA"},
    {"mysql", "mysql", "CLINK_WITH_MYSQL"},
    {"redis", "redis", "CLINK_WITH_REDIS"},
    {"http", "http", "CLINK_WITH_HTTP"},
    {"nats", "nats", "CLINK_WITH_NATS"},
    {"mqtt", "mqtt", "CLINK_WITH_MQTT"},
    {"avro", "avro", "CLINK_WITH_AVRO"},
    {"etcd", "etcd", "CLINK_WITH_ETCD"},
    {"s3", "s3", "CLINK_WITH_AWS_S3"},
}};

bool prefix_matches(std::string_view op_type, std::string_view prefix) {
    if (op_type == prefix) {
        return true;
    }
    return op_type.size() > prefix.size() && op_type.substr(0, prefix.size()) == prefix &&
           op_type[prefix.size()] == '_';
}

// The connectors this binary genuinely has, split by role. Read from
// the capability registry, never a literal list, so the message always
// reflects the running build.
std::string available_connectors_by_role() {
    std::vector<std::string> sources;
    std::vector<std::string> sinks;
    for (const auto& c : connectors::CapabilityRegistry::instance().all()) {
        if (c.is_source) {
            sources.push_back(c.name);
        }
        if (c.is_sink) {
            sinks.push_back(c.name);
        }
    }
    auto join = [](const std::vector<std::string>& names) {
        std::string out;
        for (const auto& n : names) {
            out += (out.empty() ? "" : ", ") + n;
        }
        return out.empty() ? std::string{"(none declared)"} : out;
    };
    return "Available source connectors: " + join(sources) +
           "\nAvailable sink connectors: " + join(sinks);
}

}  // namespace

std::optional<ConnectorVocabularyEntry> connector_vocabulary_lookup(std::string_view op_type) {
    for (const auto& entry : kVocabulary) {
        if (prefix_matches(op_type, entry.prefix)) {
            return entry;
        }
    }
    return std::nullopt;
}

bool connector_declared_available(std::string_view connector) {
    // In-tree connectors compiled into every SQL-linked build: no
    // capability toggle can remove them, so a registry miss for one of
    // these would be a false "unavailable".
    static constexpr std::array<std::string_view, 9> kAlwaysBuilt{
        "file",
        "filesystem",
        "parquet",
        "blackhole",
        "generator",
        "changelog",
        "nexmark",
        "delta",
        "print",
    };
    for (const auto a : kAlwaysBuilt) {
        if (connector == a) {
            return true;
        }
    }
    const std::string with_sep = std::string(connector) + "_";
    for (const auto& cap : connectors::CapabilityRegistry::instance().all()) {
        if (cap.name == connector) {
            return true;
        }
        // kafka -> kafka_2pc: the family's variant records imply the base.
        if (cap.name.size() > with_sep.size() &&
            cap.name.compare(0, with_sep.size(), with_sep) == 0) {
            return true;
        }
        // s3_parquet -> s3: the providing impl's base record implies the
        // longer SQL-vocabulary name it also registers factories for.
        if (connector.size() > cap.name.size() + 1 &&
            connector.substr(0, cap.name.size()) == cap.name && connector[cap.name.size()] == '_') {
            return true;
        }
    }
    return false;
}

std::string check_connector_availability(const JobGraphSpec& graph,
                                         const OperatorRegistry& ops,
                                         const RunnerRegistry& runners) {
    for (const auto& op : graph.ops) {
        if (ops.knows_type(op.type) || runners.knows_type(op.type)) {
            continue;
        }
        const auto entry = connector_vocabulary_lookup(op.type);
        if (!entry.has_value()) {
            // Not connector-shaped: a plugin or inline op the deploy path
            // resolves (and refuses, loudly, if genuinely unknown).
            continue;
        }
        std::string msg = "connector '" + std::string(entry->connector) +
                          "' is not available in this clink build: operator '" + op.type +
                          "' (op id '" + op.id + "') has no registered factory.\n\n" +
                          available_connectors_by_role();
        if (!entry->build_flag.empty()) {
            msg += "\n\nRebuild with " + std::string(entry->build_flag) + "=ON.";
        }
        return msg;
    }
    return {};
}

}  // namespace clink::cluster
