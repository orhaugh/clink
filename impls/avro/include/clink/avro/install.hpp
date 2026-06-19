// clink::avro::install - kept for API parity with the other impls.
//
// The Avro impl ships only Codec<T> templates; there are no built-in
// sources or sinks for `install()` to register. Calling it is still
// valid (it's a no-op). Unlike the other impls, clink::plugin::install_defaults
// does NOT call this: the codecs are header-only and need no registration,
// so there is no `#ifdef CLINK_HAS_AVRO` block in install_defaults.

#pragma once

#include "clink/plugin/plugin.hpp"

namespace clink::avro {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::avro
