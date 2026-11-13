#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::relate {

// Register the Relate sink factory (relate_sink, on the SQL Row channel) and
// the connector's capability record.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::relate
