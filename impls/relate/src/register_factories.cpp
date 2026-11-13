#include <memory>
#include <string>

#include "clink/connectors/capability.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/relate/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_channel.hpp"

#include "options.hpp"
#include "relate_sink.hpp"

namespace clink::relate {

namespace {

clink::connectors::ConnectorCapabilities record() {
    return clink::connectors::ConnectorCapabilities{
        .name = "relate",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"Relate's C library (librelate_ffi, C ABI v2), linked "
                               "statically; scripts/fetch-relate.sh installs the pinned version"},
        .runtime_dependencies = {"none: the store is a directory or a .rl file that this "
                                 "process opens; Relate 1.0.1 tested"},
        .formats = {"typed parameters"},
        .boundedness = clink::connectors::Boundedness::Either,
        .checkpoint_integrated = true,
        // Each barrier returns only once every batch holding a row from before
        // it is in a transaction Relate committed, and a restart replays the
        // rest. The statement decides whether a replay is harmless.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none"},
        .tls = false,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"retry_window_ms", "flush_interval_ms"},
        .available_in_sql = true,
        .limitations =
            {"at-least-once: a batch that a restart replays is applied again; a statement keyed "
             "by stable ids under uniqueness constraints converges, a CREATE or an increment "
             "does not",
             "exactly-once, effectively-once, mode='upsert' and changelog='true' are refused",
             "a Relate store has one writing process: every sink on a store must run in one "
             "process, which shares one handle among them; a second process fails at open",
             "the whole graph is held in the memory of the process that runs the sink",
             "the statement is refused at open when it returns columns, uses CALL { ... } IN "
             "TRANSACTIONS, only reads, changes the schema, or needs parameters other than $rows",
             "DECIMAL, TIME and BYTEA columns are refused; CAST them",
             "one transaction per batch, in order from each subtask; a write-write conflict "
             "between sinks is rolled back and retried within retry_window_ms",
             "a statement in flight cannot be interrupted: Relate's C interface has no cancel, "
             "so a cancelled task waits for the batch it is committing",
             "must be the only sink on its chain: Dag::add_sink refuses another sink beside it"},
    };
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    // register_sink<Row> needs the Row type registered first.
    clink::sql::register_row_channel(reg);
    reg.register_sink<clink::sql::Row>(
        "relate_sink",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<clink::sql::Row>> {
            return std::make_shared<RelateSink>(
                parse_options(ctx.params, ctx.subtask_idx, ctx.parallelism));
        });
    clink::connectors::declare_connector(record());
}

}  // namespace clink::relate
