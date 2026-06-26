// clink::http_connector::install - register the HTTP connector factories
// (http_sink) with a PluginRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::http_connector {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::http_connector
