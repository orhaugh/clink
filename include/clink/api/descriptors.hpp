#pragma once

#include <cstdint>
#include <map>
#include <string>

namespace clink::api {

// SourceDescriptor / SinkDescriptor / OperatorDescriptor are the
// type-erased rectangles the fluent API hands to Pipeline
// when wiring up a pipeline. Each carries the registered operator type
// name (lookup key in OperatorRegistry / RunnerRegistry) and a flat
// string map of params. The connector-specific Builder classes produce
// these via .build().
//
// channel_type is informational on the descriptor (the env can use the
// DataStream<T>'s static T-to-channel-name mapping at submit time
// instead), but builders that know their own output channel name
// pre-populate it so untyped consumers can still get it right.

struct SourceDescriptor {
    std::string op_type;
    std::string channel_type;
    std::map<std::string, std::string> params;
    std::uint32_t parallelism{1};
};

struct SinkDescriptor {
    std::string op_type;
    std::string channel_type;
    std::map<std::string, std::string> params;
    std::uint32_t parallelism{1};
};

struct OperatorDescriptor {
    std::string op_type;
    std::string in_channel_type;
    std::string out_channel_type;
    std::map<std::string, std::string> params;
    std::uint32_t parallelism{1};
};

}  // namespace clink::api
