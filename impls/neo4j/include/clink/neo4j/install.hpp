#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::neo4j {

// Register the Neo4j sink factory (neo4j_sink, on the SQL Row channel) and its
// capability record.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::neo4j
