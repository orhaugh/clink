#include "clink/plugin/plugin.hpp"
#include "clink/schema_registry/install.hpp"

namespace clink::schema_registry {

// No sources, sinks or operators: the Kafka connector reaches the formats
// through formats.hpp. Kept for API parity with the connector impls.
void install(clink::plugin::PluginRegistry& /*reg*/) {}

}  // namespace clink::schema_registry
