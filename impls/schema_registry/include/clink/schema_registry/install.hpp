#pragma once
//
// API parity with the connector impls. The schema registry library registers
// no sources, sinks or operators: the Kafka connector reaches it through
// formats.hpp. install() is a no-op that exists so a build can assert the
// library linked.

namespace clink::plugin {
class PluginRegistry;
}

namespace clink::schema_registry {
void install(clink::plugin::PluginRegistry& reg);
}
