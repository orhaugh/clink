#pragma once

#include <string>

#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace clink::sql {

// Registers the SQL Row channel type. row_json_codec is the per-record codec, and the wire
// batcher preserves columnar data across worker boundaries: a columnar Batch<Row> ships its
// typed Arrow RecordBatch verbatim and is received as a columnar batch, so the operator chain
// after a shuffle stays on the columnar path, while a row-form batch falls back to the
// lossless per-record JSON layout.
//
// clink::sql::install() calls it, and so does every impl whose install() registers a source,
// operator or sink on the Row channel: those registrations need the type registered at call
// time ("T not registered" otherwise), and the installs run in no fixed order. Registration
// is last-write-wins and every caller writes this same registration, so the order changes
// nothing, and no install replaces the columnar wire batcher with a codec-only one.
inline void register_row_channel(clink::plugin::PluginRegistry& reg) {
    reg.register_type<Row>(
        std::string{kChannelRow}, row_json_codec(), make_row_wire_batcher(row_json_codec()));
}

}  // namespace clink::sql
