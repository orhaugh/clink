#include "native/register_native.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "clink/connectors/capability.hpp"
#include "clink/metrics/counter.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

#include "native/errors.hpp"
#include "native/metrics.hpp"

#if defined(CLINK_CLICKHOUSE_NATIVE)
#include "native/insert_transport.hpp"
#include "native/native_sink.hpp"
#include "native/sink_options.hpp"
#endif

namespace clink::clickhouse::native {

namespace {

// The record claims TLS only for a build that has the native sink and a client
// that carries the TLS socket factory; without the sink there is nothing to
// secure, whatever the client was built with.
#if defined(CLINK_CLICKHOUSE_NATIVE) && defined(CLINK_CLICKHOUSE_NATIVE_TLS)
constexpr bool kNativeTls = true;
#else
constexpr bool kNativeTls = false;
#endif

// A refusal at build time comes before any RuntimeContext or op id exists, so
// it goes to the process registry with the reason alone.
void count_factory_refusal(const std::string& code) {
    MetricsRegistry::global()
        .counter(std::string(metric::kRefusalsTotal) + "{reason=\"" + code + "\"}")
        .increment();
}

clink::connectors::ConnectorCapabilities native_record() {
    std::vector<std::string> limitations = {
        "at-least-once: rows after the last completed checkpoint are replayed and may appear "
        "twice",
        "a resent INSERT is deduplicated only on targets that keep a deduplication log, on 26.3 "
        "and 26.8",
        "exactly-once, mode='upsert' and changelog='true' are refused",
        "targets whose effective async_insert is not 0 are refused at open, on every replica "
        "behind a Distributed target",
        "SharedMergeTree targets are refused: their deduplication is untested",
        "must be the only sink on its chain: Dag::add_sink refuses another sink beside it",
        "unaligned and adaptive checkpoints are refused at open until the engine captures "
        "in-flight rows at a fan-in",
        "a retry that holds the barrier beyond CLINK_EOS_FINAL_CKPT_TIMEOUT_MS (default 30 s) "
        "at the end of a bounded job or at a hot cutover spends a restart; nothing is lost",
        "Row form only: a columnar batch is materialised before conversion",
    };
#if !defined(CLINK_CLICKHOUSE_NATIVE)
    limitations.emplace_back(
        "not in this build: the clickhouse-cpp it found is older than 2.6.2, or of a version "
        "it could not read, so the factory refuses clickhouse.native_unavailable");
#endif
    return clink::connectors::ConnectorCapabilities{
        .name = "clickhouse_native",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"clickhouse-cpp 2.6.2"},
        .runtime_dependencies = {"clickhouse server, native protocol (port 9000, or 9440 with "
                                 "TLS); 26.3 and 26.8 tested"},
        .formats = {"native"},
        .boundedness = clink::connectors::Boundedness::Either,
        .checkpoint_integrated = true,
        // Each barrier returns only once the server has acknowledged every
        // INSERT holding a row from before it, and a restart replays the
        // rest; the token deduplicates a resend only where the table keeps a
        // log.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = kNativeTls,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout_ms",
                            "send_timeout_ms",
                            "receive_timeout_ms",
                            "retry_window_ms",
                            "batch_interval_ms"},
        .available_in_sql = true,
        .limitations = std::move(limitations),
    };
}

}  // namespace

void register_native(clink::plugin::PluginRegistry& registry) {
    // register_sink<Row> needs the Row type registered first. This is the
    // same codec and wire batcher clink::sql::install() registers, and
    // registration is last-write-wins, so whichever install runs second
    // changes nothing.
    registry.register_type<sql::Row>(std::string{sql::kChannelRow},
                                     sql::row_json_codec(),
                                     sql::make_row_wire_batcher(sql::row_json_codec()));

    // clickhouse_native_sink: Native blocks through clickhouse-cpp, on the
    // Row channel. Every key the SQL planner, the materialised-view code or
    // the ClickHouse source puts on the op is accepted; any other key is
    // refused, as is a value that would change the guarantee.
    registry.register_sink<sql::Row>(
        "clickhouse_native_sink",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<sql::Row>> {
#if defined(CLINK_CLICKHOUSE_NATIVE)
            try {
                return std::make_shared<NativeSink>(
                    parse_sink_options(ctx.params, ctx.subtask_idx, ctx.parallelism),
                    current_transport_factory());
            } catch (const NativeSinkError& e) {
                count_factory_refusal(e.code());
                throw;
            }
#else
            (void)ctx;
            count_factory_refusal(code::kNativeUnavailable);
            throw NativeSinkError(
                code::kNativeUnavailable,
                "clickhouse_native_sink is not available in this build: it needs clickhouse-cpp "
                "2.6.2 or later, and the client this build found is older or of a version it "
                "could not read. Rebuild against the pinned client, or use "
                "insert_format='jsoneachrow'.");
#endif
        });

    clink::connectors::declare_connector(native_record());
}

}  // namespace clink::clickhouse::native
