// Cassandra connector factory registration (cassandra_sink_string,
// cassandra_upsert_sink_string). Sink only.

#include <charconv>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/cassandra/cassandra_upsert_sink.hpp"
#include "clink/cassandra/connection_params.hpp"
#include "clink/cassandra/install.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

#include "sink_core.hpp"

namespace clink::cassandra {

namespace {

using clink::plugin::BuildContext;

// Split a comma-separated option value, trimming spaces, dropping empties.
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < s.size()) {
        std::size_t j = s.find(',', i);
        if (j == std::string::npos) {
            j = s.size();
        }
        std::size_t b = i;
        std::size_t e = j;
        while (b < e && s[b] == ' ') {
            ++b;
        }
        while (e > b && s[e - 1] == ' ') {
            --e;
        }
        if (e > b) {
            out.push_back(s.substr(b, e - b));
        }
        i = j + 1;
    }
    return out;
}

// An integer option, read whole: "12abc" or "abc" is refused rather than read as
// 12 or as the default, and so is a value outside [lo, hi].
std::int64_t int_param(const BuildContext& ctx,
                       const char* factory,
                       const char* key,
                       std::int64_t fallback,
                       std::int64_t lo,
                       std::int64_t hi) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    const std::string& text = it->second;
    std::int64_t v = 0;
    const auto res = std::from_chars(text.data(), text.data() + text.size(), v);
    if (text.empty() || res.ec != std::errc{} || res.ptr != text.data() + text.size() || v < lo ||
        v > hi) {
        throw std::runtime_error(std::string{factory} + ": '" + key + "' must be an integer from " +
                                 std::to_string(lo) + " to " + std::to_string(hi) + "; it is '" +
                                 text + "'");
    }
    return v;
}

CassandraConsistency consistency_param(const BuildContext& ctx, const char* factory) {
    const std::string text = ctx.param_or("consistency", "local_one");
    if (const auto c = parse_cassandra_consistency(text)) {
        return *c;
    }
    std::string lower = text;
    for (auto& ch : lower) {
        ch = (ch >= 'A' && ch <= 'Z') ? static_cast<char>(ch - 'A' + 'a') : ch;
    }
    std::string why = "it is not a consistency level";
    if (lower == "any") {
        why = "ANY acknowledges a write no replica need hold yet";
    } else if (lower == "serial" || lower == "local_serial") {
        why = "the serial levels govern lightweight transactions, which the sink does not use";
    }
    throw std::runtime_error(std::string{factory} + ": 'consistency' is '" + text + "', which " +
                             "the sink does not accept: " + why +
                             ". Use one of one, two, three, quorum, all, local_quorum, "
                             "each_quorum, local_one");
}

CassandraConnParams conn_params(const BuildContext& ctx, const char* factory) {
    CassandraConnParams c;
    c.contact_points = ctx.param_or("contact_points", "127.0.0.1");
    c.port = static_cast<int>(int_param(ctx, factory, "port", 9042, 1, 65535));
    c.username = ctx.param_or("username", "");
    c.password = ctx.param_or("password", "");
    c.connect_timeout_ms = static_cast<int>(
        int_param(ctx, factory, "connect_timeout_ms", 5000, 1, detail::kMaxTimeoutMs));
    c.request_timeout_ms = static_cast<int>(
        int_param(ctx, factory, "request_timeout_ms", 12000, 1, detail::kMaxTimeoutMs));
    c.consistency = consistency_param(ctx, factory);
    return c;
}

std::size_t in_flight_param(const BuildContext& ctx, const char* factory) {
    return static_cast<std::size_t>(
        int_param(ctx,
                  factory,
                  "max_in_flight",
                  1024,
                  1,
                  static_cast<std::int64_t>(detail::kMaxInFlightLimit)));
}

std::size_t bytes_param(const BuildContext& ctx, const char* factory, const char* key) {
    return static_cast<std::size_t>(int_param(ctx,
                                              factory,
                                              key,
                                              std::int64_t{64} << 20,
                                              detail::kMinBytesBound,
                                              static_cast<std::int64_t>(detail::kMaxBytesBound)));
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "cassandra",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cassandra cpp-driver"},
        .runtime_dependencies = {"cassandra >= 4 or scylladb >= 6"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        // INSERT ... JSON is a CQL upsert, so replays collapse for a
        // stable primary key - which the user's schema controls, not clink.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms"},
        .available_in_sql = true,
        .limitations =
            {"at-least-once: a restart rewrites every record after the last checkpoint; a stable "
             "primary key makes the rewrite an overwrite, not a duplicate",
             "a write is acknowledged at the configured consistency (default local_one), which "
             "is the durability a barrier waits for",
             "writes to one row are ordered by timestamps from the writing sink subtask's clock: "
             "rows written by two subtasks, by another writer, or by a clock running behind an "
             "earlier write can end at an older value",
             "a failed or timed-out write fails the sink and the job restarts; the sink does not "
             "retry it",
             "no TLS: the connection is plain text",
             "the sink must be the only sink on its chain: Dag::add_sink refuses another sink "
             "beside it"},
    });
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "cassandra_upsert",
        .version = "2",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"cassandra cpp-driver"},
        .runtime_dependencies = {"cassandra >= 4 or scylladb >= 6"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "key_columns",
        .auth_methods = {"none", "password"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options = {"connect_timeout_ms", "request_timeout_ms"},
        .available_in_sql = true,
        .limitations =
            {"final-state convergence, not exactly-once delivery: writes are at-least-once and "
             "a replay converges each row to its last record",
             "convergence assumes each primary key is written by one sink subtask, in order, by "
             "a deterministic query, with no other writer and no clock running behind an earlier "
             "write to the row",
             "key_columns must be the table's whole primary key; collection, tuple, "
             "user-defined and custom key types are refused",
             "deletes are row deletions by key at the write's timestamp",
             "a write is acknowledged at the configured consistency (default local_one)",
             "no TLS: the connection is plain text",
             "the sink must be the only sink on its chain: Dag::add_sink refuses another sink "
             "beside it"},
    });

    // cassandra_sink_string: each JSON-object row written with
    // `INSERT INTO "ks"."tbl" JSON ?`. At-least-once; a barrier waits for every
    // write before it. Params:
    //   contact_points (127.0.0.1), port (9042), username/password (both or
    //   neither), connect_timeout_ms (5000), request_timeout_ms (12000),
    //   consistency (local_one), keyspace (required), table (required),
    //   max_in_flight (1024), max_in_flight_bytes (64 MiB).
    reg.register_sink<std::string>(
        "cassandra_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            constexpr const char* kFactory = "cassandra_sink";
            CassandraSink::Options o;
            o.conn = conn_params(ctx, kFactory);
            o.keyspace = ctx.param_or("keyspace");
            o.table = ctx.param_or("table");
            o.max_in_flight = in_flight_param(ctx, kFactory);
            o.max_in_flight_bytes = bytes_param(ctx, kFactory, "max_in_flight_bytes");
            o.name = kFactory;
            return std::make_shared<CassandraSink>(std::move(o));
        });

    // cassandra_upsert_sink_string: changelog-aware, mode='upsert'. Maintains a
    // table by its PRIMARY KEY - INSERT JSON for insert/update_after, DELETE by
    // key for delete/update_before. Same params as cassandra_sink_string plus
    // key_columns (the table's whole primary key, threaded from the SQL path),
    // max_buffered_records (10000) and max_buffered_bytes (64 MiB).
    reg.register_sink<std::string>(
        "cassandra_upsert_sink_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            constexpr const char* kFactory = "cassandra_upsert_sink";
            CassandraUpsertSink::Options o;
            o.conn = conn_params(ctx, kFactory);
            o.keyspace = ctx.param_or("keyspace");
            o.table = ctx.param_or("table");
            o.key_columns = split_csv(ctx.param_or("key_columns", ""));
            o.max_buffered_records = static_cast<std::size_t>(
                int_param(ctx,
                          kFactory,
                          "max_buffered_records",
                          10000,
                          1,
                          static_cast<std::int64_t>(detail::kMaxBufferedRecordsLimit)));
            o.max_buffered_bytes = bytes_param(ctx, kFactory, "max_buffered_bytes");
            o.max_in_flight = in_flight_param(ctx, kFactory);
            o.max_in_flight_bytes = bytes_param(ctx, kFactory, "max_in_flight_bytes");
            o.name = kFactory;
            return std::make_shared<CassandraUpsertSink>(std::move(o));
        });
}

}  // namespace clink::cassandra
