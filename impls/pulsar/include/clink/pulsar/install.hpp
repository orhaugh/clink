#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::pulsar {

// Register the Apache Pulsar source + sink factories (pulsar_source_string, pulsar_sink_string).
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::pulsar
