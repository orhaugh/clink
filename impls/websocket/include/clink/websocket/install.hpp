#pragma once

// Registers the WebSocket connector factories (websocket_source_string)
// into a PluginRegistry. Linked by clink_node under CLINK_LINKED_WEBSOCKET
// and by the impl's tests.

#include "clink/plugin/plugin.hpp"

namespace clink::websocket {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::websocket
