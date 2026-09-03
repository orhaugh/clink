// Stable tier: authoring a plugin of types, operators and connectors.
// Compile-only; frozen (see README.md). Additions only.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "clink/core/codec.hpp"
#include "clink/core/derived_codec.hpp"
#include "clink/core/fields.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/install_defaults.hpp"
#include "clink/plugin/plugin.hpp"

struct Reading {
    std::int64_t sensor{};
    double value{};
};
CLINK_FIELDS(Reading, sensor, value);

namespace {

class Sum final : public clink::KeyedProcessFunction<std::int64_t, Reading, double> {
public:
    void process_element(const Reading& r,
                         clink::ProcessFunctionContext<double>&,
                         clink::Collector<double>& out) override {
        out.collect(r.value);
    }
};

class Pair final : public clink::KeyedCoProcessFunction<std::int64_t, Reading, Reading, double> {
public:
    void process_element1(const Reading& r,
                          clink::ProcessFunctionContext<double>&,
                          clink::Collector<double>& out) override {
        out.collect(r.value);
    }
    void process_element2(const Reading& r,
                          clink::ProcessFunctionContext<double>&,
                          clink::Collector<double>& out) override {
        out.collect(r.value);
    }
};

class Halves final : public clink::CoOperator<Reading, Reading, double> {
public:
    void process_element1(const clink::StreamElement<Reading>&, clink::Emitter<double>&) override {}
    void process_element2(const clink::StreamElement<Reading>&, clink::Emitter<double>&) override {}
};

void register_all(clink::plugin::PluginRegistry& reg) {
    // Types: explicit codec, codec + batcher, or derived from CLINK_FIELDS.
    reg.register_type<double>("double", clink::trivial_codec<double>());
    reg.register_type<std::int64_t>("int64", clink::int64_codec(), clink::int64_arrow_batcher());
    reg.register_type<Reading>();
    (void)reg.codec_for<Reading>();

    reg.register_source<Reading>(
        "readings",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Source<Reading>> {
            (void)ctx.param_or("path", "/tmp/readings");
            (void)ctx.param_int64_or("count", 10);
            (void)ctx.subtask_idx;
            (void)ctx.parallelism;
            (void)clink::plugin::BuildContext::resolve_secret("env://CLINK_CONFORMANCE");
            return std::make_shared<clink::VectorSource<Reading>>(
                std::vector<clink::Record<Reading>>{});
        });
    reg.register_operator<Reading, double>(
        "reading_value",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::Operator<Reading, double>> {
            return std::make_shared<clink::MapOperator<Reading, double>>(
                [](const Reading& r) { return r.value; });
        });
    reg.register_sink<double>(
        "discard", [](const clink::plugin::BuildContext&) -> std::shared_ptr<clink::Sink<double>> {
            return std::make_shared<clink::FunctionSink<double>>([](const double&) {});
        });
    reg.register_co_operator<Reading, Reading, double>(
        "halves",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::CoOperator<Reading, Reading, double>> {
            return std::make_shared<Halves>();
        });
    reg.register_selector<std::int64_t>(
        "by_parity", [](const std::int64_t& v) { return static_cast<int>(v % 2); });
    reg.register_key_extractor<Reading>("by_sensor", [](const Reading& r) { return r.sensor; });
    reg.register_keyed_operator<std::int64_t, Reading, double>(
        "sum_by_sensor",
        [](const clink::plugin::BuildContext&) { return std::make_shared<Sum>(); },
        [](const Reading& r) { return r.sensor; });
    reg.register_keyed_co_operator<std::int64_t, Reading, Reading, double>(
        "pair_by_sensor",
        [](const clink::plugin::BuildContext&) { return std::make_shared<Pair>(); },
        [](const Reading& r) { return r.sensor; },
        [](const Reading& r) { return r.sensor; });

    // Every clink-shipped connector this build links, in one call.
    clink::plugin::install_defaults(reg);
}

[[maybe_unused]] void metadata() {
    constexpr clink::plugin::PluginMetadata meta{
        .name = "api-conformance", .version = "1.0", .description = "frozen", .author = "clink"};
    (void)meta;
}

}  // namespace

CLINK_DECLARE_PLUGIN("api-conformance-plugin", "1.0", "frozen conformance plugin");
CLINK_REGISTER_PLUGIN(register_all);
