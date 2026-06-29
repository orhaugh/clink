// Cassandra connector factory registration (cassandra_sink_string). Sink only.

#include <cstdint>
#include <memory>
#include <string>

#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/cassandra/connection_params.hpp"
#include "clink/cassandra/install.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::cassandra {

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
}

}  // namespace clink::cassandra
