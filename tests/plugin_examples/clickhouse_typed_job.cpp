// clickhouse_typed_job - a bounded source of Trade structs into the typed
// native ClickHouse sink, registered by register_clickhouse_native_sink<Trade>
// under its default op type, clickhouse_native_sink_Trade.
//
// The source derives every record from its code, the same on every run, so it
// takes the generator record's name prefix and the delivery-guarantee gate
// reads it as replayable, as it does any generator.
//
// The job carries the connector: it links clink::clickhouse, so the sink's
// factory, its column plan and its writer are the module's own, built by the
// worker through the module's registration. Fixture for the plugin route in
// tests/test_clickhouse_native_sql.cpp.
//
// The sink's options are env:// references, resolved where each sink is
// built, so the graph is the same in every process and the test chooses the
// server at run time:
//   CLINK_CLICKHOUSE_TYPED_JOB_HOST, CLINK_CLICKHOUSE_TYPED_JOB_PORT,
//   CLINK_CLICKHOUSE_TYPED_JOB_DATABASE, CLINK_CLICKHOUSE_TYPED_JOB_TABLE,
//   CLINK_CLICKHOUSE_TYPED_JOB_RETRY_WINDOW_MS
// The target needs the columns (id Int64, qty UInt32, volume UInt64,
// px Float64, venue String).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "clink/api/pipeline.hpp"
#include "clink/clickhouse/native_sink.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

struct Trade {
    std::int64_t id;
    std::uint32_t qty;
    std::uint64_t volume;
    double px;
    std::string venue;
};
CLINK_FIELDS(Trade, id, qty, volume, px, venue);

namespace clickhouse_typed_job {

// Rows the source emits: ids 0 to kRows - 1.
constexpr std::int64_t kRows = 1000;

std::vector<clink::Record<Trade>> trades() {
    std::vector<clink::Record<Trade>> out;
    out.reserve(static_cast<std::size_t>(kRows));
    for (std::int64_t id = 0; id < kRows; ++id) {
        out.emplace_back(Trade{id,
                               static_cast<std::uint32_t>(id * 3),
                               (1ULL << 63) + static_cast<std::uint64_t>(id),
                               static_cast<double>(id) / 4.0,
                               "v" + std::to_string(id % 7)});
    }
    return out;
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    auto& registry = pipeline.registry();
    registry.register_type<Trade>();
    clink::clickhouse::register_clickhouse_native_sink<Trade>(registry);
    registry.register_source<Trade>(
        "generator_clickhouse_typed_trades",
        [](const clink::plugin::BuildContext&) -> std::shared_ptr<clink::Source<Trade>> {
            return std::make_shared<clink::VectorSource<Trade>>(
                trades(), "generator_clickhouse_typed_trades");
        });

    clink::api::SourceDescriptor source;
    source.op_type = "generator_clickhouse_typed_trades";
    source.channel_type = "Trade";
    source.params["determinism"] = "deterministic";

    clink::api::SinkDescriptor sink;
    sink.op_type = "clickhouse_native_sink_Trade";
    sink.channel_type = "Trade";
    sink.params = {{"host", "env://CLINK_CLICKHOUSE_TYPED_JOB_HOST"},
                   {"port", "env://CLINK_CLICKHOUSE_TYPED_JOB_PORT"},
                   {"database", "env://CLINK_CLICKHOUSE_TYPED_JOB_DATABASE"},
                   {"table", "env://CLINK_CLICKHOUSE_TYPED_JOB_TABLE"},
                   {"retry_window_ms", "env://CLINK_CLICKHOUSE_TYPED_JOB_RETRY_WINDOW_MS"}};

    pipeline.source<Trade>(source, "src")
        .uid("generator_clickhouse_typed_trades")
        .sink(sink, "snk");
}

}  // namespace clickhouse_typed_job

CLINK_REGISTER_JOB("clickhouse-typed",
                   "1.0",
                   "a bounded source of Trade structs into the typed native ClickHouse sink",
                   clickhouse_typed_job::define_job);
