// clink::kafka::install - register the Kafka text source/sink
// factories with a PluginRegistry. Callers (clink_node, the
// integration test main, plugin authors who want Kafka prewired) call
// this once after cluster::ensure_built_ins_registered() to make the
// kafka_text_source / kafka_text_sink op-type names resolvable
// through that registry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::kafka {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::kafka
