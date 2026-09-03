// Stable tier: the fluent Pipeline API and the CEP pattern API.
// Compile-only; frozen (see README.md). Additions only.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/channel_name.hpp"
#include "clink/api/descriptors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/cep/cep.hpp"
#include "clink/cep/pattern.hpp"
#include "clink/cep/pattern_stream.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/time/event_time.hpp"

struct Fact {
    std::int64_t count{};
    std::int64_t last_seen_ms{};
};
CLINK_FIELDS(Fact, count, last_seen_ms);

namespace {

using namespace std::chrono_literals;

class Running final : public clink::KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        out.collect(v + current_key());
    }
};

class Stringly final : public clink::KeyedProcessFunction<std::string, std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
};

class Plain final : public clink::ProcessFunction<std::int64_t, std::int64_t> {
public:
    void process_element(const std::int64_t& v,
                         clink::ProcessFunctionContext<std::int64_t>&,
                         clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
};

class Together final
    : public clink::KeyedCoProcessFunction<std::int64_t, std::int64_t, std::int64_t, std::int64_t> {
public:
    void process_element1(const std::int64_t& v,
                          clink::ProcessFunctionContext<std::int64_t>&,
                          clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
    void process_element2(const std::int64_t& v,
                          clink::ProcessFunctionContext<std::int64_t>&,
                          clink::Collector<std::int64_t>& out) override {
        out.collect(v);
    }
};

[[maybe_unused]] void pipeline_and_streams() {
    clink::api::Pipeline p = clink::api::Pipeline::create();
    p.set_parallelism(2);
    (void)p.default_parallelism();
    p.add_plugin("/opt/clink/plugins/mine.so");
    p.expect_state_version("counter", "Fact", 2, "slot");
    p.expect_state_shape<Fact>("counter", "slot");
    (void)p.graph();

    clink::api::DataStream<std::int64_t> ints = p.from_elements<std::int64_t>({1, 2, 3});
    clink::api::DataStream<std::int64_t> ranged = p.source<std::int64_t>(
        clink::api::IntRangeSource::builder().count(10).start(0).step(1).build());
    (void)ranged;

    clink::api::DataStream<std::int64_t> mapped =
        ints.map<std::int64_t>([](const std::int64_t& v) { return v * 2; });
    clink::api::DataStream<std::int64_t> filtered =
        mapped.filter([](const std::int64_t& v) { return v > 0; });
    clink::api::DataStream<std::int64_t> flat = filtered.flat_map<std::int64_t>(
        [](const std::int64_t& v) { return std::vector<std::int64_t>{v}; });
    clink::api::DataStream<std::int64_t> processed =
        flat.process<std::int64_t>(std::make_shared<Plain>());
    clink::api::DataStream<std::int64_t> stamped = processed.assign_timestamps_monotonic(
        [](const std::int64_t& v) { return clink::EventTime{v}; });
    clink::api::DataStream<std::int64_t> bounded = stamped.assign_timestamps_bounded(
        [](const std::int64_t& v) { return clink::EventTime{v}; }, 100ms);
    bounded.name("display name").uid("stable-uid").rescalable(1, 8);
    (void)bounded.id();
    (void)bounded.channel_type();
    (void)bounded.env();

    clink::OutputTag<std::int64_t> late{"late"};
    clink::api::DataStream<std::int64_t> side = bounded.side_output<std::int64_t>(late);
    side.sink(clink::api::FileInt64Sink::builder().path("/tmp/conformance-late.txt").build());

    clink::api::KeyedDataStream<std::int64_t> keyed =
        bounded.key_by([](const std::int64_t& v) { return v % 4; });
    (void)keyed.key_by();
    keyed.name("keyed").uid("keyed-uid").rescalable(1, 4);

    clink::api::DataStream<std::int64_t> reduced =
        keyed.reduce([](const std::int64_t& a, const std::int64_t& b) { return a + b; });
    (void)reduced;
    clink::api::DataStream<std::int64_t> via_int64_key =
        keyed.process<std::int64_t>(std::make_shared<Running>());
    (void)via_int64_key;
    clink::api::DataStream<std::int64_t> via_typed_key = keyed.process<std::string, std::int64_t>(
        std::make_shared<Stringly>(), [](const std::int64_t& v) { return std::to_string(v); });
    (void)via_typed_key;

    clink::api::KeyedDataStream<std::int64_t> other =
        bounded.key_by([](const std::int64_t& v) { return v % 4; });
    clink::api::DataStream<std::int64_t> connected =
        keyed.connect_process<std::int64_t, std::int64_t, std::int64_t>(
            other,
            std::make_shared<Together>(),
            [](const std::int64_t& v) { return v; },
            [](const std::int64_t& v) { return v; });
    (void)connected;

    auto init = []() -> std::int64_t { return 0; };
    auto add = [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; };
    auto emit = [](std::int64_t key, const clink::TimeWindow& w, const std::int64_t& agg) {
        return key + w.start + agg;
    };

    clink::api::TumblingWindowedDataStream<std::int64_t> tumbling = keyed.tumbling_window(1s);
    tumbling.parallelism(2).allowed_lateness(100ms).late_output_tag(late);
    clink::api::DataStream<std::int64_t> t_agg = tumbling.aggregate<std::int64_t>(init, add);
    clink::api::DataStream<std::int64_t> t_emit =
        tumbling.aggregate<std::int64_t, std::int64_t>(init, add, emit);
    (void)t_agg;
    (void)t_emit;
    clink::api::EvictingTumblingWindowedDataStream<std::int64_t> evicting =
        tumbling.evicting([]() -> std::unique_ptr<clink::Evictor<std::int64_t, clink::TimeWindow>> {
            return std::make_unique<clink::CountEvictor<std::int64_t>>(10);
        });
    evicting.parallelism(2).allowed_lateness(100ms);

    clink::api::SlidingWindowedDataStream<std::int64_t> sliding = keyed.sliding_window(1s, 500ms);
    sliding.allowed_lateness(100ms).with_trigger(
        []() -> std::unique_ptr<clink::Trigger<std::int64_t, clink::TimeWindow>> {
            return std::make_unique<clink::EventTimeTrigger<std::int64_t>>();
        });
    (void)sliding.aggregate<std::int64_t>(init, add);
    (void)sliding.aggregate<std::int64_t, std::int64_t>(init, add, emit);

    auto merge = [](const std::int64_t& a, const std::int64_t& b) { return a + b; };
    clink::api::SessionWindowedDataStream<std::int64_t> session = keyed.session_window(30s);
    session.allowed_lateness(100ms).late_output_tag(late);
    (void)session.aggregate<std::int64_t>(init, add, merge);
    (void)session.aggregate<std::int64_t, std::int64_t>(init, add, merge, emit);

    keyed.sink(clink::api::FileInt64Sink::builder().path("/tmp/conformance.txt").build());
}

[[maybe_unused]] void descriptors_and_builders() {
    clink::api::SourceDescriptor src{.op_type = "int64_range_source",
                                     .channel_type = "int64",
                                     .params = {{"count", "3"}},
                                     .parallelism = 1};
    clink::api::SinkDescriptor sink{
        .op_type = "file_int64_sink", .channel_type = "int64", .params = {}, .parallelism = 1};
    clink::api::OperatorDescriptor op{.op_type = "map",
                                      .in_channel_type = "int64",
                                      .out_channel_type = "int64",
                                      .params = {},
                                      .parallelism = 1};
    (void)src;
    (void)sink;
    (void)op;

    (void)clink::api::IntRangeSource::builder().count(5).parallelism(2).build();
    (void)clink::api::StringLinesSource::builder().build();
    (void)clink::api::FileTextSource::builder().path("/tmp/in.txt").build();
    (void)clink::api::FileTextSink::builder().path("/tmp/out.txt").build();
    (void)clink::api::FileInt64Sink::builder().path("/tmp/out-ints.txt").build();

    (void)clink::api::ChannelName<std::int64_t>::get();
    (void)clink::api::ChannelName<std::string>::get();
}

[[maybe_unused]] void cep_patterns() {
    clink::api::Pipeline p = clink::api::Pipeline::create();
    clink::api::KeyedDataStream<std::int64_t> keyed =
        p.from_elements<std::int64_t>({1, 2, 3}).key_by([](const std::int64_t& v) { return v; });

    clink::cep::Pattern<std::int64_t> pattern =
        clink::cep::Pattern<std::int64_t>::begin("start")
            .where([](const std::int64_t& v) { return v > 0; })
            .followed_by("down")
            .where([](const std::int64_t& v, const clink::cep::PatternMatch<std::int64_t>&) {
                return v < 10;
            })
            .one_or_more()
            .next("up")
            .times(1, 3)
            .optional()
            .within(10s)
            .after_match_skip(clink::cep::SkipStrategy::skip_past_last_event());
    (void)clink::cep::SkipStrategy::no_skip();
    (void)clink::cep::SkipStrategy::skip_to_next();
    (void)clink::cep::SkipStrategy::skip_to_first("start");
    (void)clink::cep::SkipStrategy::skip_to_last("up");

    clink::cep::PatternStream<std::int64_t> matches =
        clink::cep::pattern(keyed, pattern, clink::int64_codec());
    clink::api::DataStream<std::int64_t> selected =
        matches.select<std::int64_t>([](const clink::cep::PatternMatch<std::int64_t>& m) {
            return static_cast<std::int64_t>(m.size());
        });
    selected.sink(clink::api::FileInt64Sink::builder().path("/tmp/conformance-cep.txt").build());

    clink::cep::PatternStream<std::int64_t> unkeyed =
        clink::cep::pattern(p.from_elements<std::int64_t>({1}), pattern, clink::int64_codec());
    (void)unkeyed;
}

}  // namespace
