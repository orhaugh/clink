#pragma once

// Declared apart from native_sink.hpp so that the registration builds, and
// refuses by name, against a client too old for the native sink: this header
// includes no clickhouse-cpp header.

namespace clink::plugin {
class PluginRegistry;
}

namespace clink::clickhouse::native {

// Registers the SQL Row channel type (idempotently, so the order of this and
// the SQL frontend's own install does not matter), the factory
// `clickhouse_native_sink` on that channel, and the `clickhouse_native`
// capability record. On a build without the native sink the factory refuses
// with clickhouse.native_unavailable, and the record still names the
// connector, with TLS off. Called from install().
void register_native(clink::plugin::PluginRegistry& registry);

}  // namespace clink::clickhouse::native
