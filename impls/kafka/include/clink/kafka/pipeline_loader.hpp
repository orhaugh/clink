#pragma once

#include <functional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "clink/config/json.hpp"
#include "clink/connectors/kafka_message.hpp"
#include "clink/runtime/dag.hpp"

// Kafka-specific JSON-driven pipeline loader. Lives at
// include/clink/kafka/ during Phase 1 of the impls split; in Phase 2
// this header moves to impls/kafka/include/clink/kafka/.
//
// Renamed from clink::config::PipelineLoader to make the dependency
// on KafkaMessage explicit at the type level; previously the loader
// lived in clink::config but only handled Kafka pipelines, which
// gave the false impression that the config layer was connector-
// agnostic. Generic JSON DAGs over arbitrary record types would
// require templatising; that's deferred until a second concrete
// loader (e.g. Postgres CDC) needs the same JSON shape.

namespace clink::kafka {

// JSON shape (unchanged from PipelineLoader):
//
//   {
//     "pipeline": {
//       "stages": [
//         {"name": "input",   "type": "kafka_source", "params": {...}},
//         {"name": "uppercase","type": "map", "input": "input",
//          "params": {"fn": "uppercase"}},
//         {"name": "validate","type": "filter","input": "uppercase",
//          "params": {"fn": "non_empty"}},
//         {"name": "router", "type": "split", "input": "validate",
//          "params": {"fn": "by_length", "branches": 2}},
//         {"name": "out",    "type": "kafka_sink",
//          "input": "router.0", "params": {...}},
//         {"name": "errors", "type": "kafka_sink",
//          "input": "router.1", "params": {...}}
//       ]
//     }
//   }
//
// Stage types: kafka_source, kafka_sink, map, filter, split.
// `input` references another stage by `name` or `name.<branch>`.
class KafkaPipelineLoader {
public:
    using MapFn = std::function<KafkaMessage(const KafkaMessage&)>;
    using FilterFn = std::function<bool(const KafkaMessage&)>;
    using SelectorFn = std::function<int(const KafkaMessage&)>;

    KafkaPipelineLoader() = default;

    void register_map_fn(std::string name, MapFn fn) { map_fns_[std::move(name)] = std::move(fn); }
    void register_filter_fn(std::string name, FilterFn fn) {
        filter_fns_[std::move(name)] = std::move(fn);
    }
    void register_selector_fn(std::string name, SelectorFn fn) {
        selector_fns_[std::move(name)] = std::move(fn);
    }

    void load(const clink::config::JsonValue& config, Dag& dag);

private:
    std::unordered_map<std::string, MapFn> map_fns_;
    std::unordered_map<std::string, FilterFn> filter_fns_;
    std::unordered_map<std::string, SelectorFn> selector_fns_;
};

}  // namespace clink::kafka
