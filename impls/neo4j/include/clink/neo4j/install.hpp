#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::neo4j {

// Register the Neo4j sink factory (neo4j_sink, on the SQL Row channel), the
// lookup join operator (neo4j_lookup_join_row, Row to Row) and the connector's
// capability record.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::neo4j
