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

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#include "clink/nexmark/generator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"

namespace clink::nexmark {

// Steady-state measurement marks, shared (in-process) between the benchmark
// main() and every nexmark_source instance of one run. The benchmark's
// submit->complete wall includes job deploy / cluster bring-up; this isolates
// the steady-state ingest rate by stamping wall-clock once per source instance
// at the warm-up boundary (the warmup_events-th logical event) and once at
// drain. With several per-type source instances running concurrently under the
// same downstream backpressure, the earliest warm stamp and latest end stamp
// bracket the whole run. NOTE: for windowed queries the bounded source drains
// (and so stamps end_ns) BEFORE the terminal watermark fires all panes, so the
// steady interval measures the streaming-ingest body, not the pane-fire tail;
// the cold whole-job wall (reported alongside) covers the full job.
struct SteadyMarks {
    std::int64_t warmup_events = 0;
    std::atomic<std::int64_t> warm_ns{0};  // earliest wall at the warm-up boundary (min, nonzero)
    std::atomic<std::int64_t> end_ns{0};   // latest wall at source drain (max)

    static std::int64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
    void mark_warm(std::int64_t ns) {
        std::int64_t cur = warm_ns.load(std::memory_order_relaxed);
        while ((cur == 0 || ns < cur) &&
               !warm_ns.compare_exchange_weak(cur, ns, std::memory_order_relaxed)) {
        }
    }
    void mark_end(std::int64_t ns) {
        std::int64_t cur = end_ns.load(std::memory_order_relaxed);
        while (ns > cur && !end_ns.compare_exchange_weak(cur, ns, std::memory_order_relaxed)) {
        }
    }
};

// Register the nexmark_source generator + a blackhole (discard) sink. Call after
// clink::sql::install(). The blackhole sink lets a benchmark measure query
// throughput without sink I/O distorting it (the runner still counts records_in).
// If `marks` is non-null the source stamps steady-state marks (negligible
// per-event cost: one bool check after a stamp-once-per-instance flag).
inline void register_nexmark_factories(clink::plugin::PluginRegistry& reg,
                                       std::shared_ptr<SteadyMarks> marks = nullptr) {
    using clink::sql::Row;
    reg.register_source<Row>(
        "nexmark_source",
        [marks](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Source<Row>> {
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
                [gen, marks, warmed = false]() mutable -> std::optional<Record<Row>> {
                    auto rec = gen->next();
                    if (marks) {
                        if (rec) {
                            if (!warmed && gen->event_id() >= marks->warmup_events) {
                                warmed = true;
                                marks->mark_warm(SteadyMarks::now_ns());
                            }
                        } else {
                            marks->mark_end(SteadyMarks::now_ns());
                        }
                    }
                    return rec;
                },
                "nexmark_source",
                bounded);
        });
    reg.register_sink<Row>(
        "blackhole_sink_row",
        [](const clink::plugin::BuildContext& /*ctx*/) -> std::shared_ptr<Sink<Row>> {
            return std::make_shared<FunctionSink<Row>>([](const Row&) {}, "blackhole_sink_row");
        });
}

}  // namespace clink::nexmark
