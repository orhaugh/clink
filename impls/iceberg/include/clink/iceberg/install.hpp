#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::iceberg {

// Register the Iceberg connector factory (iceberg_row_sink) into the given
// registry. Called by clink_node at startup when CLINK_LINKED_ICEBERG.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::iceberg
