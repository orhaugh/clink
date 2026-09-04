// Stable tier: the standard operator library (construction).
// Compile-only; frozen (see README.md). Additions only.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

#include <arrow/api.h>

#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/async_co_process_function.hpp"
#include "clink/operators/async_lookup_operator.hpp"
#include "clink/operators/async_map_operator.hpp"
#include "clink/operators/async_process_function.hpp"
#include "clink/operators/evicting_tumbling_window_operator.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/flat_map_operator.hpp"
#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/keyed_aggregate_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/reduce_operator.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/operators/session_window_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/sliding_window_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/split_by_variant_operator.hpp"
#include "clink/operators/throttle_map.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/operators/window_evictor.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/time/watermark_strategy.hpp"

namespace {

using namespace std::chrono_literals;

[[maybe_unused]] void sources_and_sinks() {
    std::vector<clink::Record<std::int64_t>> rows{clink::Record<std::int64_t>{1}};
    (void)std::make_shared<clink::VectorSource<std::int64_t>>(rows);
    (void)std::make_shared<clink::VectorSource<std::int64_t>>(rows, "named");
    (void)std::make_shared<clink::PacedVectorSource<std::int64_t>>(rows, 10ms);
    (void)std::make_shared<clink::GeneratorSource<std::int64_t>>(
        []() -> std::optional<clink::Record<std::int64_t>> { return std::nullopt; },
        "gen",
        /*bounded=*/true);

    auto collecting = std::make_shared<clink::CollectingSink<std::int64_t>>();
    (void)collecting->collected();
    (void)std::make_shared<clink::FunctionSink<std::int64_t>>([](const std::int64_t&) {});
}

[[maybe_unused]] void stateless_operators() {
    (void)std::make_shared<clink::MapOperator<std::int64_t, std::string>>(
        [](const std::int64_t& v) { return std::to_string(v); });
    (void)std::make_shared<clink::MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v; }, "named-map");
    (void)std::make_shared<clink::FilterOperator<std::int64_t>>(
        [](const std::int64_t& v) { return v > 0; });
    (void)std::make_shared<clink::FlatMapOperator<std::string, std::string>>(
        [](const std::string& s) { return std::vector<std::string>{s}; });
    (void)std::make_shared<clink::KeyByOperator<std::string, std::string>>(
        [](const std::string& s) { return s; });
    (void)std::make_shared<clink::ThrottleMap<std::int64_t>>(1ms);

    using Either = std::variant<std::int64_t, std::string>;
    (void)std::make_shared<clink::SplitByVariantOperator<Either, std::int64_t, std::string>>(
        std::tuple{clink::OutputTag<std::int64_t>{"ints"},
                   clink::OutputTag<std::string>{"strings"}});
}

[[maybe_unused]] void keyed_and_windowed_operators() {
    auto initial = []() -> std::uint64_t { return 0; };
    auto combine = [](const std::uint64_t& acc, const std::string&) { return acc + 1; };
    auto merge = [](const std::uint64_t& a, const std::uint64_t& b) { return a + b; };

    (void)std::make_shared<clink::ReduceOperator<std::string, std::string, std::uint64_t>>(initial,
                                                                                           combine);
    (void)std::make_shared<clink::ReduceOperator<std::string, std::string, std::uint64_t>>(
        initial, combine, "reduce", clink::ReduceEmitMode::OnEachInput);
    (void)std::make_shared<clink::KeyedAggregateOperator<std::string, std::string, std::uint64_t>>(
        initial, combine, clink::string_codec(), clink::uint64_codec());

    auto tumbling =
        std::make_shared<clink::TumblingWindowOperator<std::string, std::string, std::uint64_t>>(
            1000ms, initial, combine);
    tumbling->allowed_lateness(100ms);
    (void)std::make_shared<clink::TumblingWindowOperator<std::string, std::string, std::uint64_t>>(
        1000ms, initial, combine, clink::string_codec(), clink::uint64_codec());
    (void)std::make_shared<clink::SlidingWindowOperator<std::string, std::string, std::uint64_t>>(
        1000ms, 500ms, initial, combine);
    (void)std::make_shared<clink::SessionWindowOperator<std::string, std::string, std::uint64_t>>(
        1000ms, initial, combine, merge);
    (void)std::make_shared<
        clink::EvictingTumblingWindowOperator<std::string, std::string, std::uint64_t>>(
        1000ms,
        [](const std::vector<clink::Record<std::string>>& rs, const clink::TimeWindow&) {
            return static_cast<std::uint64_t>(rs.size());
        },
        std::make_unique<clink::CountEvictor<std::string>>(10));

    std::unique_ptr<clink::Trigger<std::string, clink::TimeWindow>> t1 =
        std::make_unique<clink::EventTimeTrigger<std::string>>();
    std::unique_ptr<clink::Trigger<std::string, clink::TimeWindow>> t2 =
        std::make_unique<clink::ProcessingTimeTrigger<std::string>>();
    std::unique_ptr<clink::Trigger<std::string, clink::TimeWindow>> t3 =
        std::make_unique<clink::CountTrigger<std::string>>(5);
    std::unique_ptr<clink::Evictor<std::string, clink::TimeWindow>> e1 =
        std::make_unique<clink::TimeEvictor<std::string>>(5s);
    (void)t1;
    (void)t2;
    (void)t3;
    (void)e1;
}

[[maybe_unused]] void watermarks() {
    auto extractor = [](const std::int64_t& v) { return clink::EventTime{v}; };
    auto assigner = std::make_shared<clink::WatermarkAssignerOperator<std::int64_t>>(
        extractor, std::make_unique<clink::MonotonicWatermarkStrategy<std::int64_t>>());
    assigner->with_idleness(5s);
    (void)std::make_unique<clink::BoundedOutOfOrdernessStrategy<std::int64_t>>(200ms);
    (void)std::make_unique<clink::PartitionAwareBoundedOutOfOrdernessStrategy<std::int64_t>>(200ms);
}

[[maybe_unused]] void async_operators() {
    clink::AsyncRetryStrategy retry{.max_attempts = 3,
                                    .initial_backoff = 10ms,
                                    .backoff_multiplier = 2.0,
                                    .max_backoff = 1s,
                                    .should_retry = nullptr};
    auto map = std::make_shared<clink::AsyncMapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v; }, /*worker_count=*/2, /*max_in_flight=*/8);
    map->set_retry_strategy(retry);
    (void)std::make_shared<clink::AsyncMapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v; }, retry);
    (void)std::make_shared<clink::AsyncLookupOperator<std::int64_t, std::string>>(
        [](const std::int64_t& v) -> clink::async::Task<std::string> {
            co_return std::to_string(v);
        },
        /*max_in_flight=*/8,
        /*ordered=*/true);
}

// The public path from a process function object to an operator (added with
// the Stable-tier promise; the detail adapters behind them are not promised).
class Echo final : public clink::ProcessFunction<std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
};
class Running final : public clink::KeyedProcessFunction<std::string, std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
};
class Zip final : public clink::CoProcessFunction<std::int64_t, std::string, std::string> {
public:
    void process_element1(const std::int64_t&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
    void process_element2(const std::string&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
};
class KeyedZip final
    : public clink::KeyedCoProcessFunction<std::string, std::int64_t, std::string, std::string> {
public:
    void process_element1(const std::int64_t&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
    void process_element2(const std::string&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
};
class AsyncRunning final
    : public clink::AsyncKeyedProcessFunction<std::string, std::int64_t, std::int64_t> {
public:
    clink::async::Task<void> process_element(const std::string&,
                                             const std::int64_t& v,
                                             clink::AsyncKeyedProcessContext<std::int64_t>&,
                                             clink::Collector<std::int64_t>& out) override {
        out.collect(v);
        co_return;
    }
};
class AsyncZip final
    : public clink::
          AsyncKeyedCoProcessFunction<std::string, std::int64_t, std::string, std::string> {
public:
    clink::async::Task<void> process_element1(const std::string&,
                                              const std::int64_t&,
                                              clink::AsyncKeyedProcessContext<std::string>&,
                                              clink::Collector<std::string>&) override {
        co_return;
    }
    clink::async::Task<void> process_element2(const std::string&,
                                              const std::string&,
                                              clink::AsyncKeyedProcessContext<std::string>&,
                                              clink::Collector<std::string>&) override {
        co_return;
    }
};

[[maybe_unused]] void process_function_operators() {
    auto key = [](const std::int64_t& v) { return std::to_string(v); };
    auto skey = [](const std::string& s) { return s; };
    auto timer_key = [](const std::string& s) { return s; };

    std::shared_ptr<clink::Operator<std::int64_t, std::int64_t>> plain =
        clink::make_process_operator(std::make_shared<Echo>());
    (void)clink::make_process_operator(std::make_shared<Echo>(), "named");
    std::shared_ptr<clink::Operator<std::int64_t, std::int64_t>> keyed =
        clink::make_keyed_process_operator(std::make_shared<Running>(), key);
    (void)clink::make_keyed_process_operator(std::make_shared<Running>(), key, timer_key, "named");
    std::shared_ptr<clink::CoOperator<std::int64_t, std::string, std::string>> co =
        clink::make_co_process_operator(std::make_shared<Zip>());
    std::shared_ptr<clink::CoOperator<std::int64_t, std::string, std::string>> keyed_co =
        clink::make_keyed_co_process_operator(std::make_shared<KeyedZip>(), key, skey);
    (void)clink::make_keyed_co_process_operator(
        std::make_shared<KeyedZip>(), key, skey, timer_key, "named");
    std::shared_ptr<clink::Operator<std::int64_t, std::int64_t>> async_keyed =
        clink::make_async_keyed_process_operator(
            std::make_shared<AsyncRunning>(), key, clink::string_codec());
    std::shared_ptr<clink::CoOperator<std::int64_t, std::string, std::string>> async_co =
        clink::make_async_keyed_co_process_operator(
            std::make_shared<AsyncZip>(), key, skey, clink::string_codec());
    (void)plain;
    (void)keyed;
    (void)co;
    (void)keyed_co;
    (void)async_keyed;
    (void)async_co;
}

[[maybe_unused]] void sql_function_registries() {
    clink::ScalarFunctionRegistry::global().register_function(
        "conformance_identity",
        arrow::int64(),
        [](const std::vector<clink::config::JsonValue>& args) {
            return args.empty() ? clink::config::JsonValue{} : args.front();
        });
    (void)clink::ScalarFunctionRegistry::global().lookup("conformance_identity");
    (void)clink::ScalarFunctionRegistry::global().names();
    clink::ScalarFunctionRegistry::global().remove("conformance_identity");

    clink::AggFunctionRegistry::global().register_function(
        "conformance_count",
        arrow::int64(),
        []() { return clink::config::JsonValue{0.0}; },
        [](clink::config::JsonValue acc, const std::vector<clink::config::JsonValue>&) {
            return acc;
        },
        [](const clink::config::JsonValue& acc) { return acc; });
    (void)clink::AggFunctionRegistry::global().contains("conformance_count");
    clink::AggFunctionRegistry::global().remove("conformance_count");
}

}  // namespace
