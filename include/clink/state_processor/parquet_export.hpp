#pragma once

// Parquet export of keyed state - the analytics projection of a
// snapshot. Where the canonical Arrow IPC stream is the exact-fidelity,
// restorable form of a snapshot (opaque encoded keys, byte-for-byte),
// this writes the DECODED entry model as a Parquet table so state lands
// directly queryable in DuckDB / Spark / pyarrow / a lake:
//
//   op_id       : uint64   operator id
//   key_group   : uint8    leading key byte (>= 128 = operator state)
//   slot        : utf8     state-slot name ("<raw>" for unparsed keys)
//   user_key    : binary   raw user-key bytes after the '|' separator
//   value_bytes : binary   raw codec value bytes
//
// One row per entry, in the collected model's deterministic order
// (op ascending, slot name, user key). Values stay codec-encoded bytes:
// their internal layout belongs to the operator's codecs, not this
// layer. A non-empty StateVersionMap rides the file's key-value
// metadata under "clink.state_versions", mirroring the IPC form.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include "clink/state/schema_version.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"
#include "clink/state_processor/state_diff.hpp"

namespace clink::state_processor {

namespace parquet_export_detail {

[[noreturn]] inline void throw_arrow(const std::string& where, const arrow::Status& s) {
    throw std::runtime_error("write_state_parquet " + where + ": " + s.ToString());
}

}  // namespace parquet_export_detail

// Write the collected entry model as one Parquet file. Overwrites
// `path`. Throws on any Arrow/Parquet failure or unwritable path.
inline void write_state_parquet(const StateEntries& entries,
                                const StateVersionMap& versions,
                                const std::filesystem::path& path) {
    using parquet_export_detail::throw_arrow;

    arrow::UInt64Builder op_b;
    arrow::UInt8Builder kg_b;
    arrow::StringBuilder slot_b;
    arrow::BinaryBuilder key_b;
    arrow::BinaryBuilder val_b;

    auto append = [&](auto& builder, auto&& value, const char* what) {
        if (auto s = builder.Append(std::forward<decltype(value)>(value)); !s.ok()) {
            throw_arrow(what, s);
        }
    };

    std::int64_t rows = 0;
    for (const auto& [op, slots] : entries) {
        for (const auto& [slot, slot_entries] : slots) {
            for (const auto& [user_key, entry] : slot_entries) {
                append(op_b, op.value(), "(append op)");
                append(kg_b, entry.key_group, "(append key_group)");
                append(slot_b, slot, "(append slot)");
                if (auto s = key_b.Append(reinterpret_cast<const uint8_t*>(user_key.data()),
                                          static_cast<int32_t>(user_key.size()));
                    !s.ok()) {
                    throw_arrow("(append user_key)", s);
                }
                if (auto s = val_b.Append(reinterpret_cast<const uint8_t*>(entry.value.data()),
                                          static_cast<int32_t>(entry.value.size()));
                    !s.ok()) {
                    throw_arrow("(append value)", s);
                }
                ++rows;
            }
        }
    }

    std::shared_ptr<arrow::Array> op_arr, kg_arr, slot_arr, key_arr, val_arr;
    if (auto s = op_b.Finish(&op_arr); !s.ok())
        throw_arrow("(finish op)", s);
    if (auto s = kg_b.Finish(&kg_arr); !s.ok())
        throw_arrow("(finish key_group)", s);
    if (auto s = slot_b.Finish(&slot_arr); !s.ok())
        throw_arrow("(finish slot)", s);
    if (auto s = key_b.Finish(&key_arr); !s.ok())
        throw_arrow("(finish user_key)", s);
    if (auto s = val_b.Finish(&val_arr); !s.ok())
        throw_arrow("(finish value)", s);

    auto schema = arrow::schema({
        arrow::field("op_id", arrow::uint64(), /*nullable=*/false),
        arrow::field("key_group", arrow::uint8(), /*nullable=*/false),
        arrow::field("slot", arrow::utf8(), /*nullable=*/false),
        arrow::field("user_key", arrow::binary(), /*nullable=*/false),
        arrow::field("value_bytes", arrow::binary(), /*nullable=*/false),
    });
    if (!versions.empty()) {
        auto meta = std::make_shared<arrow::KeyValueMetadata>();
        meta->Append(kStateVersionsMetadataKey, versions.pack());
        schema = schema->WithMetadata(meta);
    }
    auto table = arrow::Table::Make(schema, {op_arr, kg_arr, slot_arr, key_arr, val_arr}, rows);

    auto sink_result = arrow::io::FileOutputStream::Open(path.string());
    if (!sink_result.ok())
        throw_arrow("(open " + path.string() + ")", sink_result.status());
    auto sink = *sink_result;
    if (auto s = parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), sink, /*chunk_size=*/64 * 1024);
        !s.ok()) {
        throw_arrow("(write table)", s);
    }
    if (auto s = sink->Close(); !s.ok())
        throw_arrow("(close)", s);
}

}  // namespace clink::state_processor
