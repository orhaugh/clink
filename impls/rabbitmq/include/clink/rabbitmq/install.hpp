#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::rabbitmq {

// Register the RabbitMQ source + sink factories (rabbitmq_source_string, rabbitmq_sink_string)
// with the plugin registry.
void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::rabbitmq
