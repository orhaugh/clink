// Cassandra connector factory registration (cassandra_sink_string). Sink only.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/cassandra/cassandra_upsert_sink.hpp"
#include "clink/cassandra/connection_params.hpp"
#include "clink/cassandra/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::cassandra {

namespace {
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
}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // cassandra_sink_string: write each JSON-object row via `INSERT INTO ks.tbl JSON ?` (Cassandra
    // maps fields -> columns). At-least-once (async inserts confirmed at each barrier);
    // effectively-once for a stable primary key (INSERT is an upsert). Params:
    //   contact_points (127.0.0.1), port (9042), username/password, connect_timeout_ms (5000),
    //   keyspace (required), table (required).
    reg.register_sink<std::string>(
        "cassandra_sink_string", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            CassandraSink::Options o;
            o.conn.contact_points = ctx.param_or("contact_points", "127.0.0.1");
            o.conn.port = static_cast<int>(ctx.param_int64_or("port", 9042));
            o.conn.username = ctx.param_or("username", "");
            o.conn.password = ctx.param_or("password", "");
            o.conn.connect_timeout_ms =
                static_cast<int>(ctx.param_int64_or("connect_timeout_ms", 5000));
            o.keyspace = ctx.param_or("keyspace");
            o.table = ctx.param_or("table");
            o.name = "cassandra_sink";
            if (o.keyspace.empty() || o.table.empty()) {
                throw std::runtime_error("cassandra_sink: 'keyspace' and 'table' are required");
            }
            return std::make_shared<CassandraSink>(std::move(o));
        });

    // cassandra_upsert_sink_string: changelog-aware, mode='upsert'. Maintains a
    // table by PRIMARY KEY - INSERT JSON (a CQL upsert) for insert/update_after,
    // DELETE by key for delete/update_before. Effectively-once on the sink table
    // for a stable PK. Lets a retracting SQL query maintain a Cassandra table.
    // Same params as cassandra_sink_string plus key_columns (the PRIMARY KEY,
    // threaded from the SQL path).
    reg.register_sink<std::string>(
        "cassandra_upsert_sink_string",
        [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            CassandraUpsertSink::Options o;
            o.conn.contact_points = ctx.param_or("contact_points", "127.0.0.1");
            o.conn.port = static_cast<int>(ctx.param_int64_or("port", 9042));
            o.conn.username = ctx.param_or("username", "");
            o.conn.password = ctx.param_or("password", "");
            o.conn.connect_timeout_ms =
                static_cast<int>(ctx.param_int64_or("connect_timeout_ms", 5000));
            o.keyspace = ctx.param_or("keyspace");
            o.table = ctx.param_or("table");
            o.key_columns = split_csv(ctx.param_or("key_columns", ""));
            o.name = "cassandra_upsert_sink";
            if (o.keyspace.empty() || o.table.empty()) {
                throw std::runtime_error(
                    "cassandra_upsert_sink: 'keyspace' and 'table' are required");
            }
            return std::make_shared<CassandraUpsertSink>(std::move(o));
        });
}

}  // namespace clink::cassandra
