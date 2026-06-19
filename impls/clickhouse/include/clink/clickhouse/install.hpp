// clink::clickhouse::install - register the clickhouse_sink factory
// with a PluginRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::clickhouse {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::clickhouse
