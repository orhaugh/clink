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
#include <optional>
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

// The QUERY projection: like write_state_parquet but with the key and
// value bytes RENDERED so `clink state-query` (and any SQL engine over
// the file) can filter and read them as plain columns:
//
//   op_id     : int64    OperatorId bit-cast (exact, reversible)
//   key_group : int64    leading key byte (>= 128 = operator state)
//   slot      : utf8
//   user_key  : utf8     printable bytes verbatim, else "0x" + hex
//   key_int   : int64?   the user key's little-endian int64 reading
//                        when it is exactly 8 bytes, else null
//   value     : utf8     rendered like user_key
//   value_int : int64?   rendered like key_int (counters/sums/offsets)
//
// The rendering is lossy for non-printable bytes (query convenience,
// not fidelity); the Arrow/Parquet exports remain the exact forms.
namespace parquet_export_detail {

inline bool printable(const std::string& s) {
    for (const unsigned char c : s) {
        if (c < 0x20 || c > 0x7E) {
            return false;
        }
    }
    return true;
}

inline std::string render_text(const std::string& s) {
    if (printable(s)) {
        return s;
    }
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out = "0x";
    out.reserve(2 + s.size() * 2);
    for (const unsigned char c : s) {
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0xF]);
    }
    return out;
}

inline std::optional<std::int64_t> int64_reading(const std::string& s) {
    if (s.size() != 8) {
        return std::nullopt;
    }
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<unsigned char>(s[static_cast<std::size_t>(i)]))
             << (i * 8);
    }
    return static_cast<std::int64_t>(u);
}

}  // namespace parquet_export_detail

inline void write_state_query_parquet(const StateEntries& entries,
                                      const std::filesystem::path& path) {
    using parquet_export_detail::int64_reading;
    using parquet_export_detail::render_text;
    using parquet_export_detail::throw_arrow;

    // Leading nullable event_time column: the layout the engine's Row
    // parquet source (make_row_columnar_arrow_batcher) expects at
    // position 0. State entries carry no event time, so it is all-null.
    arrow::Int64Builder et_b;
    arrow::Int64Builder op_b;
    arrow::Int64Builder kg_b;
    arrow::StringBuilder slot_b;
    arrow::StringBuilder key_b;
    arrow::Int64Builder key_int_b;
    arrow::StringBuilder val_b;
    arrow::Int64Builder val_int_b;

    std::int64_t rows = 0;
    for (const auto& [op, slots] : entries) {
        for (const auto& [slot, slot_entries] : slots) {
            for (const auto& [user_key, entry] : slot_entries) {
                (void)et_b.AppendNull();
                (void)op_b.Append(static_cast<std::int64_t>(op.value()));
                (void)kg_b.Append(static_cast<std::int64_t>(entry.key_group));
                (void)slot_b.Append(slot);
                (void)key_b.Append(render_text(user_key));
                if (auto k = int64_reading(user_key)) {
                    (void)key_int_b.Append(*k);
                } else {
                    (void)key_int_b.AppendNull();
                }
                (void)val_b.Append(render_text(entry.value));
                if (auto v = int64_reading(entry.value)) {
                    (void)val_int_b.Append(*v);
                } else {
                    (void)val_int_b.AppendNull();
                }
                ++rows;
            }
        }
    }

    std::shared_ptr<arrow::Array> et_arr, op_arr, kg_arr, slot_arr, key_arr, key_int_arr, val_arr,
        val_int_arr;
    auto fin = [&](auto& b, std::shared_ptr<arrow::Array>& out, const char* what) {
        if (auto st = b.Finish(&out); !st.ok()) {
            throw_arrow(what, st);
        }
    };
    fin(et_b, et_arr, "(finish event_time)");
    fin(op_b, op_arr, "(finish op)");
    fin(kg_b, kg_arr, "(finish key_group)");
    fin(slot_b, slot_arr, "(finish slot)");
    fin(key_b, key_arr, "(finish user_key)");
    fin(key_int_b, key_int_arr, "(finish key_int)");
    fin(val_b, val_arr, "(finish value)");
    fin(val_int_b, val_int_arr, "(finish value_int)");

    // Every field nullable: the Row parquet source's batcher declares its
    // whole schema nullable and validates the file schema for equality,
    // nullability included.
    auto schema = arrow::schema({
        arrow::field("event_time", arrow::int64(), /*nullable=*/true),
        arrow::field("op_id", arrow::int64(), /*nullable=*/true),
        arrow::field("key_group", arrow::int64(), /*nullable=*/true),
        arrow::field("slot", arrow::utf8(), /*nullable=*/true),
        arrow::field("user_key", arrow::utf8(), /*nullable=*/true),
        arrow::field("key_int", arrow::int64(), /*nullable=*/true),
        arrow::field("value", arrow::utf8(), /*nullable=*/true),
        arrow::field("value_int", arrow::int64(), /*nullable=*/true),
    });
    auto table = arrow::Table::Make(
        schema,
        {et_arr, op_arr, kg_arr, slot_arr, key_arr, key_int_arr, val_arr, val_int_arr},
        rows);

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
