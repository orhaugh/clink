#include "clink/avro/install.hpp"

namespace clink::avro {

// The Avro impl ships only Codec<T> templates; there are no built-in
// sources or sinks. install() is kept for API parity with the other
// impls (and for clink::plugin::install_defaults's `#ifdef CLINK_HAS_AVRO`
// path), but does nothing.
void install(clink::plugin::PluginRegistry& /*reg*/) {}

}  // namespace clink::avro
