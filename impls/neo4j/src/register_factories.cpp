#include <memory>
#include <string>

#include "clink/connectors/capability.hpp"
#include "clink/neo4j/install.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

#include "neo4j_sink.hpp"
#include "options.hpp"

namespace clink::neo4j {

namespace {

clink::connectors::ConnectorCapabilities record() {
    return clink::connectors::ConnectorCapabilities{
        .name = "neo4j",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"clink http client (vendored cpp-httplib)"},
        .runtime_dependencies = {"Neo4j with the Query API (5.19 or later; enabled by default "
                                 "from 5.25); single-instance Community 2026.09.0 tested"},
        .formats = {"typed json"},
        .boundedness = clink::connectors::Boundedness::Either,
        .checkpoint_integrated = true,
        // Each barrier returns only once every batch holding a row from before
        // it is in a transaction the server reported committed, and a restart
        // replays the rest. The statement decides whether a replay is harmless.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "basic", "bearer"},
        .tls = clink::http_connector::HttpRequest::tls_supported(),
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms",
                            "request_timeout_ms",
                            "retry_window_ms",
                            "flush_interval_ms"},
        .available_in_sql = true,
        .limitations =
            {"at-least-once: a batch whose reply is lost, or that a restart replays, can be "
             "applied twice; a statement keyed by stable ids under uniqueness constraints "
             "converges, a CREATE or an increment does not",
             "exactly-once, effectively-once, mode='upsert' and changelog='true' are refused",
             "the statement is refused at open when it returns columns, uses CALL { ... } IN "
             "TRANSACTIONS, only reads, changes the schema, or needs parameters other than $rows",
             "DECIMAL, TIME and BYTEA columns are refused; CAST them",
             "one implicit transaction per batch, sent in order from each subtask; subtasks "
             "write concurrently",
             "must be the only sink on its chain: Dag::add_sink refuses another sink beside it",
             "tested against single-instance Neo4j Community; clusters and Aura are not"},
    };
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    // register_sink<Row> needs the Row type registered first: the same codec
    // and wire batcher clink::sql::install() registers (last write wins, and
    // both write the same).
    reg.register_type<clink::sql::Row>(
        std::string{clink::sql::kChannelRow},
        clink::sql::row_json_codec(),
        clink::sql::make_row_wire_batcher(clink::sql::row_json_codec()));
    reg.register_sink<clink::sql::Row>(
        "neo4j_sink",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<clink::sql::Row>> {
            return std::make_shared<Neo4jSink>(
                parse_options(ctx.params, ctx.subtask_idx, ctx.parallelism));
        });
    clink::connectors::declare_connector(record());
}

}  // namespace clink::neo4j
