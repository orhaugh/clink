#include "clink/kafka/pipeline_loader.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "clink/connectors/kafka_sink.hpp"
#include "clink/connectors/kafka_source.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/map_operator.hpp"

namespace clink::kafka {

using clink::config::JsonObject;
using clink::config::JsonValue;

namespace {

using StageOutput = std::variant<StageHandle<KafkaMessage>, std::vector<StageHandle<KafkaMessage>>>;

StageHandle<KafkaMessage> resolve_input(
    const std::string& reference, const std::unordered_map<std::string, StageOutput>& stages) {
    const auto dot = reference.find('.');
    const std::string base = dot == std::string::npos ? reference : reference.substr(0, dot);

    auto it = stages.find(base);
    if (it == stages.end()) {
        throw std::runtime_error("KafkaPipelineLoader: input stage '" + base + "' is not defined");
    }

    if (dot == std::string::npos) {
        if (std::holds_alternative<std::vector<StageHandle<KafkaMessage>>>(it->second)) {
            throw std::runtime_error("KafkaPipelineLoader: stage '" + base +
                                     "' is a split - must address a branch via 'name.<idx>'");
        }
        return std::get<StageHandle<KafkaMessage>>(it->second);
    }

    if (!std::holds_alternative<std::vector<StageHandle<KafkaMessage>>>(it->second)) {
        throw std::runtime_error("KafkaPipelineLoader: stage '" + base +
                                 "' is not a split - branch reference '" + reference +
                                 "' is invalid");
    }
    const std::string idx_text = reference.substr(dot + 1);
    std::size_t idx{};
    try {
        idx = static_cast<std::size_t>(std::stoul(idx_text));
    } catch (const std::exception&) {
        throw std::runtime_error("KafkaPipelineLoader: bad branch index '" + idx_text + "'");
    }
    const auto& branches = std::get<std::vector<StageHandle<KafkaMessage>>>(it->second);
    if (idx >= branches.size()) {
        throw std::runtime_error("KafkaPipelineLoader: branch index " + std::to_string(idx) +
                                 " >= " + std::to_string(branches.size()) + " for stage '" + base +
                                 "'");
    }
    return branches[idx];
}

KafkaSource::Options kafka_source_opts_from(const JsonValue& params) {
    KafkaSource::Options opts;
    opts.brokers = params.string_or("brokers", "");
    opts.topic = params.string_or("topic", "");
    opts.group_id = params.string_or("group_id", opts.group_id);
    opts.client_id = params.string_or("client_id", opts.client_id);
    opts.auto_offset_reset = params.string_or("auto_offset_reset", opts.auto_offset_reset);
    opts.metric_prefix = params.string_or("metric_prefix", opts.metric_prefix);
    if (opts.brokers.empty() || opts.topic.empty()) {
        throw std::runtime_error(
            "KafkaPipelineLoader: kafka_source requires 'brokers' and 'topic'");
    }
    return opts;
}

KafkaSink::Options kafka_sink_opts_from(const JsonValue& params) {
    KafkaSink::Options opts;
    opts.brokers = params.string_or("brokers", "");
    opts.topic = params.string_or("topic", "");
    opts.client_id = params.string_or("client_id", opts.client_id);
    opts.acks = params.string_or("acks", opts.acks);
    opts.compression_type = params.string_or("compression_type", opts.compression_type);
    opts.linger_ms = std::chrono::milliseconds{
        params.int_or("linger_ms", static_cast<std::int64_t>(opts.linger_ms.count()))};
    opts.metric_prefix = params.string_or("metric_prefix", opts.metric_prefix);
    if (opts.brokers.empty() || opts.topic.empty()) {
        throw std::runtime_error("KafkaPipelineLoader: kafka_sink requires 'brokers' and 'topic'");
    }
    return opts;
}

}  // namespace

void KafkaPipelineLoader::load(const JsonValue& config, Dag& dag) {
    if (!config.is_object() || !config.contains("pipeline")) {
        throw std::runtime_error("KafkaPipelineLoader: missing 'pipeline' root");
    }
    const auto& pipeline = config.at("pipeline");
    if (!pipeline.contains("stages") || !pipeline.at("stages").is_array()) {
        throw std::runtime_error("KafkaPipelineLoader: 'pipeline.stages' must be a JSON array");
    }

    std::unordered_map<std::string, StageOutput> stages;

    for (const auto& stage_v : pipeline.at("stages").as_array()) {
        if (!stage_v.is_object()) {
            throw std::runtime_error("KafkaPipelineLoader: stage must be an object");
        }
        const std::string name = stage_v.string_or("name", "");
        const std::string type = stage_v.string_or("type", "");
        if (name.empty() || type.empty()) {
            throw std::runtime_error(
                "KafkaPipelineLoader: stage requires 'name' and 'type' strings");
        }
        if (stages.find(name) != stages.end()) {
            throw std::runtime_error("KafkaPipelineLoader: duplicate stage name '" + name + "'");
        }

        const JsonValue empty_params = JsonObject{};
        const JsonValue& params = stage_v.contains("params") ? stage_v.at("params") : empty_params;

        if (type == "kafka_source") {
            auto opts = kafka_source_opts_from(params);
            auto source = std::make_shared<KafkaSource>(std::move(opts));
            auto handle = dag.add_source<KafkaMessage>(source);
            stages.emplace(name, StageOutput{handle});
            continue;
        }

        if (type == "kafka_sink") {
            const std::string input_ref = stage_v.string_or("input", "");
            if (input_ref.empty()) {
                throw std::runtime_error("KafkaPipelineLoader: kafka_sink '" + name +
                                         "' requires 'input'");
            }
            auto upstream = resolve_input(input_ref, stages);
            auto opts = kafka_sink_opts_from(params);
            auto sink = std::make_shared<KafkaSink>(std::move(opts));
            dag.add_sink<KafkaMessage>(upstream, sink);
            stages.emplace(name, StageOutput{StageHandle<KafkaMessage>{}});
            continue;
        }

        if (type == "map") {
            const std::string fn_name = params.string_or("fn", "");
            auto it = map_fns_.find(fn_name);
            if (it == map_fns_.end()) {
                throw std::runtime_error("KafkaPipelineLoader: unknown map fn '" + fn_name + "'");
            }
            const std::string input_ref = stage_v.string_or("input", "");
            if (input_ref.empty()) {
                throw std::runtime_error("KafkaPipelineLoader: map '" + name +
                                         "' requires 'input'");
            }
            auto upstream = resolve_input(input_ref, stages);
            auto op = std::make_shared<MapOperator<KafkaMessage, KafkaMessage>>(it->second, name);
            auto handle = dag.add_operator<KafkaMessage, KafkaMessage>(upstream, op);
            stages.emplace(name, StageOutput{handle});
            continue;
        }

        if (type == "filter") {
            const std::string fn_name = params.string_or("fn", "");
            auto it = filter_fns_.find(fn_name);
            if (it == filter_fns_.end()) {
                throw std::runtime_error("KafkaPipelineLoader: unknown filter fn '" + fn_name +
                                         "'");
            }
            const std::string input_ref = stage_v.string_or("input", "");
            if (input_ref.empty()) {
                throw std::runtime_error("KafkaPipelineLoader: filter '" + name +
                                         "' requires 'input'");
            }
            auto upstream = resolve_input(input_ref, stages);
            auto op = std::make_shared<FilterOperator<KafkaMessage>>(it->second, name);
            auto handle = dag.add_operator<KafkaMessage, KafkaMessage>(upstream, op);
            stages.emplace(name, StageOutput{handle});
            continue;
        }

        if (type == "split") {
            const std::string fn_name = params.string_or("fn", "");
            auto it = selector_fns_.find(fn_name);
            if (it == selector_fns_.end()) {
                throw std::runtime_error("KafkaPipelineLoader: unknown split fn '" + fn_name + "'");
            }
            const auto branches = static_cast<std::size_t>(params.int_or("branches", 2));
            const std::string input_ref = stage_v.string_or("input", "");
            if (input_ref.empty()) {
                throw std::runtime_error("KafkaPipelineLoader: split '" + name +
                                         "' requires 'input'");
            }
            auto upstream = resolve_input(input_ref, stages);
            auto outs = dag.add_split<KafkaMessage>(upstream, it->second, branches, name);
            stages.emplace(name, StageOutput{std::move(outs)});
            continue;
        }

        throw std::runtime_error("KafkaPipelineLoader: unknown stage type '" + type + "' (stage '" +
                                 name + "')");
    }
}

}  // namespace clink::kafka
