// clink::s3::install - register the s3_text_sink factory with a
// PluginRegistry.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::s3 {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::s3
