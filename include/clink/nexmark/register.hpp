#pragma once

// Register the 'nexmark_source' Row source factory (the Nexmark generator) into a
// PluginRegistry, so a SQL table declared WITH (connector='nexmark', ...) builds
// it. Kept out of the core SQL install() so the SQL library carries no benchmark
// dependency; the Nexmark harness calls this after clink::sql::install().
//
// WITH-option params (all optional): events_num (total events, 0=unbounded),
// tps (event rate -> datetime spacing), seed, base_time_ms. The table DDL should
// also set event_time_column='datetime' and watermark_lag_ms so the planner's
// assign_timestamps_row op generates watermarks from the generated datetime.
// (Column names are lower-case: the SQL parser lower-cases identifiers.)

#include <memory>
#include <string>

#include "clink/nexmark/generator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"

namespace clink::nexmark {

// Register the nexmark_source generator + a blackhole (discard) sink. Call after
// clink::sql::install(). The blackhole sink lets a benchmark measure query
// throughput without sink I/O distorting it (the runner still counts records_in).
inline void register_nexmark_factories(clink::plugin::PluginRegistry& reg) {
    using clink::sql::Row;
    reg.register_source<Row>(
        "nexmark_source",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Source<Row>> {
            NexmarkConfig cfg;
            cfg.events_num = ctx.param_int64_or("events_num", cfg.events_num);
            cfg.tps = ctx.param_int64_or("tps", cfg.tps);
            cfg.base_time_ms = ctx.param_int64_or("base_time_ms", cfg.base_time_ms);
            cfg.seed = static_cast<std::uint64_t>(
                ctx.param_int64_or("seed", static_cast<std::int64_t>(cfg.seed)));
            const std::string t = ctx.param_or("nexmark_type");
            cfg.type_filter = t == "person" ? 0 : t == "auction" ? 1 : t == "bid" ? 2 : -1;
            auto gen = std::make_shared<NexmarkGenerator>(cfg);
            const bool bounded = cfg.events_num > 0;
            return std::make_shared<GeneratorSource<Row>>(
                [gen]() { return gen->next(); }, "nexmark_source", bounded);
        });
    reg.register_sink<Row>(
        "blackhole_sink_row",
        [](const clink::plugin::BuildContext& /*ctx*/) -> std::shared_ptr<Sink<Row>> {
            return std::make_shared<FunctionSink<Row>>([](const Row&) {}, "blackhole_sink_row");
        });
}

}  // namespace clink::nexmark
