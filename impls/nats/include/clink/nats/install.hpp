#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::nats {

// Register the NATS JetStream source + sink factories (nats_source_string, nats_sink_string).
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::nats
