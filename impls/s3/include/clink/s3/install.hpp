// clink::s3::install - register the s3_text_sink factory with a
// PluginRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::s3 {

void install(clink::plugin::PluginRegistry& reg);

// Register the remote-read:// state-backend scheme (an S3-backed, async-capable
// disaggregated RemoteReadBackend) on the global StateBackendFactory.
// Idempotent; called once at startup (clink_node) when clink::s3 is linked.
void install_state_backend();

}  // namespace clink::s3
