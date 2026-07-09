#pragma once

#include "clink/core/base64.hpp"

// The implementation lives in the core header (clink::base64_*) so the
// cluster layer can share it; this alias preserves the connector-local
// names the Pub/Sub source and sink were written against.
namespace clink::http_connector {

using clink::base64_decode;
using clink::base64_encode;

}  // namespace clink::http_connector
