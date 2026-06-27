#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::redis {

// Register the Redis Streams connector factories (redis_source, redis_sink) into
// the given registry. Called by clink_node at startup when CLINK_LINKED_REDIS.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::redis
