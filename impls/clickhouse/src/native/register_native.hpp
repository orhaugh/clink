#pragma once

// Declared apart from native_sink.hpp so that the registration builds, and
// refuses by name, against a client too old for the native sink: this header
// includes no clickhouse-cpp header.

namespace clink::plugin {
class PluginRegistry;
}

namespace clink::clickhouse::native {

// Factory `clickhouse_native_sink` on the Row channel, and the
// `clickhouse_native` capability record. Called from install().
void register_native(clink::plugin::PluginRegistry& registry);

}  // namespace clink::clickhouse::native
