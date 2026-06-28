#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::mqtt {

// Register the MQTT connector factories (mqtt_source, mqtt_sink) into the given
// registry. Called by clink_node at startup when CLINK_LINKED_MQTT.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::mqtt
