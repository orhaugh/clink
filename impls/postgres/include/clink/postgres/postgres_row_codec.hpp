// postgres_row_codec - byte-level Codec<PostgresRow> used to register
// the typed channel `postgres.row` so clink pipelines can carry
// PostgresRow records (column names + values) end-to-end without
// flattening to a delimiter-joined std::string at the connector
// boundary.
//
// Wire format (all integers little-endian, lengths uint32):
//   [u32 names_count]
//     repeated names_count times:
//       [u32 name_len] name_bytes
//   [u32 values_count]
//     repeated values_count times:
//       [u32 value_len] value_bytes
//
// The PostgresRow API exposes column names via a shared_ptr<const
// vector<string>>; on the wire each row carries its own names so a
// downstream consumer can resolve `.at("colname")` without out-of-
// band schema. Names list may be empty (PostgresRow allows null
// names_); decoders are tolerant.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "clink/connectors/postgres_row.hpp"
#include "clink/core/codec.hpp"

namespace clink {

namespace detail {

inline void postgres_row_append_u32(Codec<PostgresRow>::Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void postgres_row_append_string(Codec<PostgresRow>::Bytes& out, const std::string& s) {
    postgres_row_append_u32(out, static_cast<std::uint32_t>(s.size()));
    const auto* p = reinterpret_cast<const std::byte*>(s.data());
    out.insert(out.end(), p, p + s.size());
}

inline bool postgres_row_read_u32(Codec<PostgresRow>::BytesView buf,
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

inline bool postgres_row_read_string(Codec<PostgresRow>::BytesView buf,
                                     std::size_t& pos,
                                     std::string& out) {
    std::uint32_t len = 0;
    if (!postgres_row_read_u32(buf, pos, len)) {
        return false;
    }
    if (pos + len > buf.size()) {
        return false;
    }
    out.assign(reinterpret_cast<const char*>(buf.data() + pos), len);
    pos += len;
    return true;
}

}  // namespace detail

inline Codec<PostgresRow> postgres_row_codec() {
    return Codec<PostgresRow>{
        .encode =
            [](const PostgresRow& row) {
                Codec<PostgresRow>::Bytes out;
                const auto names = row.column_names();
                const std::uint32_t name_count =
                    names ? static_cast<std::uint32_t>(names->size()) : std::uint32_t{0};
                detail::postgres_row_append_u32(out, name_count);
                if (names) {
                    for (const auto& n : *names) {
                        detail::postgres_row_append_string(out, n);
                    }
                }
                const auto& values = row.values();
                detail::postgres_row_append_u32(out, static_cast<std::uint32_t>(values.size()));
                for (const auto& v : values) {
                    detail::postgres_row_append_string(out, v);
                }
                // Null mask (M5): [u32 count][count bytes]. Empty => all non-null.
                const auto& nulls = row.nulls();
                detail::postgres_row_append_u32(out, static_cast<std::uint32_t>(nulls.size()));
                for (char c : nulls) {
                    out.push_back(static_cast<std::byte>(c));
                }
                return out;
            },
        .decode = [](Codec<PostgresRow>::BytesView buf) -> std::optional<PostgresRow> {
            std::size_t pos = 0;
            std::uint32_t name_count = 0;
            if (!detail::postgres_row_read_u32(buf, pos, name_count)) {
                return std::nullopt;
            }
            auto names = std::make_shared<std::vector<std::string>>();
            names->reserve(name_count);
            for (std::uint32_t i = 0; i < name_count; ++i) {
                std::string n;
                if (!detail::postgres_row_read_string(buf, pos, n)) {
                    return std::nullopt;
                }
                names->push_back(std::move(n));
            }
            std::uint32_t value_count = 0;
            if (!detail::postgres_row_read_u32(buf, pos, value_count)) {
                return std::nullopt;
            }
            std::vector<std::string> values;
            values.reserve(value_count);
            for (std::uint32_t i = 0; i < value_count; ++i) {
                std::string v;
                if (!detail::postgres_row_read_string(buf, pos, v)) {
                    return std::nullopt;
                }
                values.push_back(std::move(v));
            }
            // Null mask (M5). The trailing-section read assumes `buf` is the EXACT
            // bytes of one encoded row (the wire/state framing guarantees this -
            // do not compose this codec inside a container/pair codec that would
            // hand it a trailing tail). Tolerant: a buffer written before the mask
            // existed simply ends here, leaving nulls empty (= all non-null).
            std::vector<char> nulls;
            std::uint32_t nulls_count = 0;
            if (detail::postgres_row_read_u32(buf, pos, nulls_count)) {
                if (pos + nulls_count > buf.size()) {
                    return std::nullopt;
                }
                nulls.resize(nulls_count);
                for (std::uint32_t i = 0; i < nulls_count; ++i) {
                    nulls[i] = static_cast<char>(buf[pos + i]);
                }
                pos += nulls_count;
            }
            // Defend against a corrupt/foreign buffer whose mask arity disagrees
            // with the values: drop the mask (treat as all-non-null) rather than
            // mis-report NULLs.
            if (nulls.size() != values.size()) {
                nulls.clear();
            }
            // Drop empty names list - a default-constructed PostgresRow
            // has names_ == nullptr, and we don't want to fake an empty
            // shared list that .at("name") would treat as "no match".
            PostgresRow::Names typed_names;
            if (!names->empty()) {
                typed_names = std::shared_ptr<const std::vector<std::string>>{std::move(names)};
            }
            return PostgresRow{std::move(typed_names), std::move(values), std::move(nulls)};
        }};
}

// Channel name used when register_type<PostgresRow>() is called by the
// Postgres impl's install().
inline constexpr const char* kChannelPostgresRow = "postgres.row";

}  // namespace clink
