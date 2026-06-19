// clink::postgres::install - register the Postgres SELECT source
// and logical-replication CDC source factories with a PluginRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::postgres {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::postgres
