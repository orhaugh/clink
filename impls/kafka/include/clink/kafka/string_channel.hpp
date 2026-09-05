#pragma once
//
// The string-channel Kafka source and sink exactly as the SQL planner's
// kafka_source_string / kafka_sink_string factories build them from a table's
// WITH options (BuildContext::params): brokers, topic, security, batching,
// deterministic partition ownership, and a registry-framed value format
// (format='avro' | 'protobuf' | 'json-schema' with schema_registry_url) that
// decodes each message into one JSON object text on the way in and encodes
// the channel's JSON rows on the way out. For hosts that assemble a pipeline
// by hand from the same options, and for tests that drive the connector the
// way the planner does.

#include <memory>
#include <string>
#include <vector>

#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::kafka {

std::shared_ptr<Source<std::string>> build_string_source(const clink::plugin::BuildContext& ctx);
std::shared_ptr<Sink<std::string>> build_string_sink(const clink::plugin::BuildContext& ctx);

// The formats the string-channel factories accept in this build: text, json,
// bytes, and whichever registry formats impls/schema_registry compiled in.
std::vector<std::string> string_channel_formats();

}  // namespace clink::kafka
