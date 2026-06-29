#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::cassandra {

// Register the Cassandra sink factory (cassandra_sink_string).
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::cassandra
