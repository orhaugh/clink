#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::mongodb {

// Register the MongoDB connector factories (mongo_cdc_source, mongo_sink) into the
// given registry. Called by clink_node at startup when CLINK_LINKED_MONGODB.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::mongodb
