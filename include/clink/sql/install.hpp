#pragma once

#include "clink/plugin/plugin.hpp"

// Called once at clink_node startup (when clink::sql is linked). Registers
// the dynamic-schema Row channel type plus the operator/source/sink
// factories that flow Row records through the runtime:
//
//   types:
//     "row" -> Codec<Row> (JSON-encoded per-record wire)
//
//   sources:
//     file_json_source - reads NDJSON file lines as Row records
//
//   sinks:
//     file_json_sink   - writes Row records as one JSON object per line
//
//   operators:
//     identity_row             - passthrough (matches identity_string)
//     filter_row_predicate     - WHERE clause for multi-column rows
//     project_row              - column selection / reordering

namespace clink::sql {

void install(clink::plugin::PluginRegistry& reg);

}  // namespace clink::sql
