// clickhouse_row_codec - byte-level Codec<ClickHouseRow> used to
// register the typed channel `clickhouse.row` so pipelines can carry
// ClickHouseRow records (column names + types + stringified values)
// end-to-end without flattening to a delimiter-joined std::string at
// the connector boundary.
//
// Wire format (all integers little-endian, lengths uint32):
//   [u32 names_count]
//     repeated names_count times:  [u32 name_len] name_bytes
//   [u32 types_count]
//     repeated types_count times:  [u32 type_len] type_bytes
//   [u32 values_count]
//     repeated values_count times: [u32 val_len]  value_bytes
//
// Mirrors postgres_row_codec; adds a `types` block since
// ClickHouseRow carries both column names and types.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/connectors/clickhouse_row.hpp"
#include "clink/core/codec.hpp"

namespace clink {

namespace detail {

inline void clickhouse_row_append_u32(Codec<ClickHouseRow>::Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void clickhouse_row_append_string(Codec<ClickHouseRow>::Bytes& out, const std::string& s) {
    clickhouse_row_append_u32(out, static_cast<std::uint32_t>(s.size()));
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    out.insert(out.end(), p, p + s.size());
}

inline bool clickhouse_row_read_u32(Codec<ClickHouseRow>::BytesView buf,
                                    std::size_t& pos,
                                    std::uint32_t& out) {
    if (pos + 4 > buf.size()) {
        return false;
    }
    out = 0;
    for (int i = 0; i < 4; ++i) {
        out |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[pos + i])) << (i * 8);
    }
    pos += 4;
    return true;
}

inline bool clickhouse_row_read_string(Codec<ClickHouseRow>::BytesView buf,
                                       std::size_t& pos,
                                       std::string& out) {
    std::uint32_t len = 0;
    if (!clickhouse_row_read_u32(buf, pos, len)) {
        return false;
    }
    if (pos + len > buf.size()) {
        return false;
    }
    out.assign(reinterpret_cast<const char*>(buf.data() + pos), len);
    pos += len;
    return true;
}

inline bool clickhouse_row_read_string_vec(Codec<ClickHouseRow>::BytesView buf,
                                           std::size_t& pos,
                                           std::vector<std::string>& out) {
    std::uint32_t count = 0;
    if (!clickhouse_row_read_u32(buf, pos, count)) {
        return false;
    }
    out.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        std::string s;
        if (!clickhouse_row_read_string(buf, pos, s)) {
            return false;
        }
        out.push_back(std::move(s));
    }
    return true;
}

}  // namespace detail

inline Codec<ClickHouseRow> clickhouse_row_codec() {
    return Codec<ClickHouseRow>{
        .encode =
            [](const ClickHouseRow& row) {
                Codec<ClickHouseRow>::Bytes out;
                const auto names = row.column_names();
                const auto types = row.column_types();
                const std::uint32_t name_count =
                    names ? static_cast<std::uint32_t>(names->size()) : std::uint32_t{0};
                detail::clickhouse_row_append_u32(out, name_count);
                if (names) {
                    for (const auto& n : *names) {
                        detail::clickhouse_row_append_string(out, n);
                    }
                }
                const std::uint32_t type_count =
                    types ? static_cast<std::uint32_t>(types->size()) : std::uint32_t{0};
                detail::clickhouse_row_append_u32(out, type_count);
                if (types) {
                    for (const auto& t : *types) {
                        detail::clickhouse_row_append_string(out, t);
                    }
                }
                const auto& values = row.values();
                detail::clickhouse_row_append_u32(out, static_cast<std::uint32_t>(values.size()));
                for (const auto& v : values) {
                    detail::clickhouse_row_append_string(out, v);
                }
                // Null mask (M5): [u32 count][count bytes]. Empty => all non-null.
                const auto& nulls = row.nulls();
                detail::clickhouse_row_append_u32(out, static_cast<std::uint32_t>(nulls.size()));
                for (char c : nulls) {
                    out.push_back(static_cast<std::byte>(c));
                }
                return out;
            },
        .decode = [](Codec<ClickHouseRow>::BytesView buf) -> std::optional<ClickHouseRow> {
            std::size_t pos = 0;

            auto names = std::make_shared<std::vector<std::string>>();
            if (!detail::clickhouse_row_read_string_vec(buf, pos, *names)) {
                return std::nullopt;
            }
            auto types = std::make_shared<std::vector<std::string>>();
            if (!detail::clickhouse_row_read_string_vec(buf, pos, *types)) {
                return std::nullopt;
            }
            std::vector<std::string> values;
            if (!detail::clickhouse_row_read_string_vec(buf, pos, values)) {
                return std::nullopt;
            }
            // Null mask (M5). Tolerant: a buffer written before the mask existed
            // simply ends here, leaving nulls empty (= all non-null).
            std::vector<char> nulls;
            std::uint32_t nulls_count = 0;
            if (detail::clickhouse_row_read_u32(buf, pos, nulls_count)) {
                if (pos + nulls_count > buf.size()) {
                    return std::nullopt;
                }
                nulls.resize(nulls_count);
                for (std::uint32_t i = 0; i < nulls_count; ++i) {
                    nulls[i] = static_cast<char>(buf[pos + i]);
                }
                pos += nulls_count;
            }

            ClickHouseRow::Names typed_names;
            ClickHouseRow::Types typed_types;
            if (!names->empty()) {
                typed_names = std::shared_ptr<const std::vector<std::string>>{std::move(names)};
            }
            if (!types->empty()) {
                typed_types = std::shared_ptr<const std::vector<std::string>>{std::move(types)};
            }
            return ClickHouseRow{std::move(typed_names),
                                 std::move(typed_types),
                                 std::move(values),
                                 std::move(nulls)};
        }};
}

// Channel name used when register_type<ClickHouseRow>() is called by
// the ClickHouse impl's install().
inline constexpr const char* kChannelClickHouseRow = "clickhouse.row";

}  // namespace clink
