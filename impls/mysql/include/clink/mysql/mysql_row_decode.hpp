#pragma once

// Decoder for the MySQL binlog ROW IMAGE (the raw packed bytes in a WRITE/UPDATE/
// DELETE_ROWS event). mariadb_rpl's own mariadb_rpl_extract_rows is unusable
// against a MySQL master - it only decodes INT/VARCHAR and garbles CHAR, TEXT,
// DATETIME, DECIMAL, DOUBLE, ENUM/SET, JSON, BIT, etc. (verified by live probe) -
// so we decode the row bytes ourselves from the TABLE_MAP column types + metadata,
// the standard MySQL row-binary format used by every CDC tool.
//
// Pure + header-only: it takes raw bytes + the parsed column metadata + the
// resolved schema (names/signedness/enum labels) and produces CdcEvents. No rpl
// handle, so it is unit-testable with hand-built / live-captured byte strings.

#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <mariadb/mysql.h>  // enum_field_types constants

#include "clink/connectors/cdc_event.hpp"
#include "clink/mysql/mysql_json.hpp"

namespace clink::mysql {

// One captured column resolved from information_schema: name, unsigned-ness (the
// binlog type carries no signedness), and the ENUM/SET member labels (so the
// stored ordinal/bitmask renders as text).
struct CdcColumn {
    std::string name;
    bool is_unsigned{false};
    std::vector<std::string> enum_set_labels;
};

struct CdcTableSchema {
    std::string db;
    std::string table;
    std::vector<CdcColumn> columns;  // ordinal order, aligned with the TABLE_MAP

    std::string qualified() const { return db + "." + table; }
};

// Per-column binlog metadata parsed from the TABLE_MAP. `type` is the binlog
// column type byte (enum_field_types); m0/m1 are the up-to-two metadata bytes for
// that column (interpreted per type at decode time).
struct ColumnMeta {
    std::uint8_t type{0};
    std::uint8_t m0{0};
    std::uint8_t m1{0};
};

struct MysqlDecodeResult {
    std::vector<CdcEvent> events;
    std::uint64_t dropped{0};
};

// The kind of a rows event, decoupled from the rpl API so this decoder depends
// only on raw bytes. The source maps WRITE/UPDATE/DELETE_ROWS to these.
enum class RowOp { Insert, Update, Delete };

namespace detail {

// Number of TABLE_MAP metadata bytes a given column type consumes.
inline int metadata_bytes(std::uint8_t type) {
    switch (type) {
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
            return 1;
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_BIT:
            return 2;
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
            return 1;  // rare: only when not packed as STRING (MariaDB)
        default:
            return 0;
    }
}

inline std::uint64_t read_le(const std::uint8_t* p, int n) {
    std::uint64_t v = 0;
    for (int i = 0; i < n; ++i) {
        v |= static_cast<std::uint64_t>(p[i]) << (8 * i);
    }
    return v;
}
inline std::uint64_t read_be(const std::uint8_t* p, int n) {
    std::uint64_t v = 0;
    for (int i = 0; i < n; ++i) {
        v = (v << 8) | p[i];
    }
    return v;
}
inline std::string fmt(const char* f, ...) {
    char buf[64];
    va_list ap;
    va_start(ap, f);
    std::vsnprintf(buf, sizeof(buf), f, ap);
    va_end(ap);
    return std::string(buf);
}
inline std::string to_hex(const std::uint8_t* p, std::size_t n) {
    static const char* h = "0123456789abcdef";
    std::string out;
    out.reserve(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(h[p[i] >> 4]);
        out.push_back(h[p[i] & 0x0F]);
    }
    return out;
}

// Sign-extend an n-byte little-endian value.
inline std::int64_t read_le_signed(const std::uint8_t* p, int n) {
    std::uint64_t v = read_le(p, n);
    const std::uint64_t sign_bit = std::uint64_t{1} << (8 * n - 1);
    if ((v & sign_bit) != 0) {
        v |= ~((std::uint64_t{1} << (8 * n)) - 1);  // extend the sign
    }
    return static_cast<std::int64_t>(v);
}

// MySQL packed NEWDECIMAL (binary BCD) -> decimal text. precision/scale from meta.
inline std::string decode_decimal(const std::uint8_t* data, int precision, int scale) {
    static const int dig2bytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    const int intg = precision - scale;
    const int intg0 = intg / 9, intg0x = intg % 9;
    const int frac0 = scale / 9, frac0x = scale % 9;
    const int ibytes = intg0 * 4 + dig2bytes[intg0x];
    const int fbytes = frac0 * 4 + dig2bytes[frac0x];
    const int total = ibytes + fbytes;
    std::vector<std::uint8_t> buf(data, data + total);
    const bool positive = (buf[0] & 0x80) != 0;
    buf[0] ^= 0x80;  // flip sign bit
    if (!positive) {
        for (auto& b : buf) {
            b = static_cast<std::uint8_t>(~b);  // negatives are stored inverted
        }
    }
    std::string out;
    if (!positive) {
        out.push_back('-');
    }
    const std::uint8_t* p = buf.data();
    // integer part
    std::string ipart;
    if (intg0x != 0) {
        ipart += std::to_string(static_cast<long>(read_be(p, dig2bytes[intg0x])));
        p += dig2bytes[intg0x];
    }
    for (int i = 0; i < intg0; ++i) {
        ipart += fmt("%09ld", static_cast<long>(read_be(p, 4)));
        p += 4;
    }
    // strip leading zeros (keep at least one digit)
    std::size_t nz = ipart.find_first_not_of('0');
    out += (nz == std::string::npos) ? "0" : ipart.substr(nz);
    // fractional part
    if (scale > 0) {
        std::string fpart;
        for (int i = 0; i < frac0; ++i) {
            fpart += fmt("%09ld", static_cast<long>(read_be(p, 4)));
            p += 4;
        }
        if (frac0x != 0) {
            // last partial group: digits = frac0x, value in dig2bytes[frac0x] bytes
            char tmp[16];
            std::snprintf(tmp,
                          sizeof(tmp),
                          "%0*ld",
                          frac0x,
                          static_cast<long>(read_be(p, dig2bytes[frac0x])));
            fpart += tmp;
        }
        out.push_back('.');
        out += fpart;
    }
    return out;
}

// DATETIME2 (5-byte big-endian packed) + fractional. Returns "Y-m-d H:M:S[.ffffff]".
inline std::string decode_datetime2(const std::uint8_t* p, int frac_prec) {
    const std::uint64_t v = read_be(p, 5);
    const std::uint32_t ym = static_cast<std::uint32_t>((v >> 22) & 0x1FFFF);
    const unsigned year = ym / 13, month = ym % 13;
    const unsigned day = static_cast<unsigned>((v >> 17) & 0x1F);
    const unsigned hour = static_cast<unsigned>((v >> 12) & 0x1F);
    const unsigned minute = static_cast<unsigned>((v >> 6) & 0x3F);
    const unsigned second = static_cast<unsigned>(v & 0x3F);
    std::string s = fmt("%04u-%02u-%02u %02u:%02u:%02u", year, month, day, hour, minute, second);
    const int fb = (frac_prec + 1) / 2;
    if (fb > 0) {
        std::uint64_t raw = read_be(p + 5, fb);
        std::uint64_t us = (frac_prec <= 2) ? raw * 10000 : (frac_prec <= 4) ? raw * 100 : raw;
        s += fmt(".%06llu", static_cast<unsigned long long>(us));
    }
    return s;
}

// TIMESTAMP2 (4-byte big-endian epoch seconds, UTC) + fractional.
inline std::string decode_timestamp2(const std::uint8_t* p, int frac_prec) {
    const std::time_t epoch = static_cast<std::time_t>(read_be(p, 4));
    std::tm tmv{};
    gmtime_r(&epoch, &tmv);
    std::string s = fmt("%04d-%02d-%02d %02d:%02d:%02d",
                        tmv.tm_year + 1900,
                        tmv.tm_mon + 1,
                        tmv.tm_mday,
                        tmv.tm_hour,
                        tmv.tm_min,
                        tmv.tm_sec);
    const int fb = (frac_prec + 1) / 2;
    if (fb > 0) {
        std::uint64_t raw = read_be(p + 4, fb);
        std::uint64_t us = (frac_prec <= 2) ? raw * 10000 : (frac_prec <= 4) ? raw * 100 : raw;
        s += fmt(".%06llu", static_cast<unsigned long long>(us));
    }
    return s;
}

// TIME2 (3-byte big-endian packed, with 0x800000 bias) + fractional.
inline std::string decode_time2(const std::uint8_t* p, int frac_prec) {
    std::int64_t v = static_cast<std::int64_t>(read_be(p, 3)) - 0x800000;
    const char* sign = "";
    if (v < 0) {
        sign = "-";
        v = -v;
    }
    const unsigned hour = static_cast<unsigned>((v >> 12) & 0x3FF);
    const unsigned minute = static_cast<unsigned>((v >> 6) & 0x3F);
    const unsigned second = static_cast<unsigned>(v & 0x3F);
    std::string s = fmt("%s%02u:%02u:%02u", sign, hour, minute, second);
    const int fb = (frac_prec + 1) / 2;
    if (fb > 0) {
        std::uint64_t raw = read_be(p + 3, fb);
        std::uint64_t us = (frac_prec <= 2) ? raw * 10000 : (frac_prec <= 4) ? raw * 100 : raw;
        s += fmt(".%06llu", static_cast<unsigned long long>(us));
    }
    return s;
}

}  // namespace detail

// Parse the TABLE_MAP metadata blob into one ColumnMeta per column, walking
// type-by-type (each type consumes a fixed number of metadata bytes).
inline std::vector<ColumnMeta> parse_table_metadata(const std::uint8_t* types,
                                                    std::uint32_t n,
                                                    const std::uint8_t* meta,
                                                    std::size_t meta_len) {
    std::vector<ColumnMeta> out;
    out.reserve(n);
    std::size_t mp = 0;
    for (std::uint32_t i = 0; i < n; ++i) {
        ColumnMeta cm;
        cm.type = types[i];
        const int mb = detail::metadata_bytes(cm.type);
        if (mb >= 1 && mp < meta_len) {
            cm.m0 = meta[mp++];
        }
        if (mb >= 2 && mp < meta_len) {
            cm.m1 = meta[mp++];
        }
        out.push_back(cm);
    }
    return out;
}

// Decode a single column value (already known non-null) at `*pos`, advancing it.
// Renders to the CdcField text contract. Sets `ok=false` on a bounds overrun.
inline std::string decode_value(const std::uint8_t* data,
                                std::size_t size,
                                std::size_t& pos,
                                const ColumnMeta& cm,
                                const CdcColumn& col,
                                bool& ok) {
    using namespace detail;
    auto need = [&](std::size_t k) -> bool {
        if (pos + k > size) {
            ok = false;
            return false;
        }
        return true;
    };
    switch (cm.type) {
        case MYSQL_TYPE_TINY:
            if (!need(1))
                return {};
            {
                std::string s = col.is_unsigned ? std::to_string(read_le(data + pos, 1))
                                                : std::to_string(read_le_signed(data + pos, 1));
                pos += 1;
                return s;
            }
        case MYSQL_TYPE_SHORT:
            if (!need(2))
                return {};
            {
                std::string s = col.is_unsigned ? std::to_string(read_le(data + pos, 2))
                                                : std::to_string(read_le_signed(data + pos, 2));
                pos += 2;
                return s;
            }
        case MYSQL_TYPE_INT24:
            if (!need(3))
                return {};
            {
                std::string s = col.is_unsigned ? std::to_string(read_le(data + pos, 3))
                                                : std::to_string(read_le_signed(data + pos, 3));
                pos += 3;
                return s;
            }
        case MYSQL_TYPE_LONG:
            if (!need(4))
                return {};
            {
                std::string s = col.is_unsigned ? std::to_string(read_le(data + pos, 4))
                                                : std::to_string(read_le_signed(data + pos, 4));
                pos += 4;
                return s;
            }
        case MYSQL_TYPE_LONGLONG:
            if (!need(8))
                return {};
            {
                std::string s =
                    col.is_unsigned
                        ? std::to_string(read_le(data + pos, 8))
                        : std::to_string(static_cast<std::int64_t>(read_le(data + pos, 8)));
                pos += 8;
                return s;
            }
        case MYSQL_TYPE_YEAR:
            if (!need(1))
                return {};
            {
                const unsigned y = static_cast<unsigned>(data[pos]);
                pos += 1;
                return y == 0 ? "0" : std::to_string(y + 1900);
            }
        case MYSQL_TYPE_FLOAT: {
            if (!need(4))
                return {};
            std::uint32_t b = static_cast<std::uint32_t>(read_le(data + pos, 4));
            pos += 4;
            float f = 0;
            std::memcpy(&f, &b, 4);
            return fmt("%.9g", static_cast<double>(f));
        }
        case MYSQL_TYPE_DOUBLE: {
            if (!need(8))
                return {};
            std::uint64_t b = read_le(data + pos, 8);
            pos += 8;
            double d = 0;
            std::memcpy(&d, &b, 8);
            return fmt("%.17g", d);
        }
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL: {
            const int precision = cm.m0, scale = cm.m1;
            static const int dig2bytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
            const int intg = precision - scale;
            const int total =
                (intg / 9) * 4 + dig2bytes[intg % 9] + (scale / 9) * 4 + dig2bytes[scale % 9];
            if (!need(static_cast<std::size_t>(total)))
                return {};
            std::string s = decode_decimal(data + pos, precision, scale);
            pos += total;
            return s;
        }
        case MYSQL_TYPE_DATE: {
            if (!need(3))
                return {};
            std::uint32_t v = static_cast<std::uint32_t>(read_le(data + pos, 3));
            pos += 3;
            return fmt("%04u-%02u-%02u", v >> 9, (v >> 5) & 0x0F, v & 0x1F);
        }
        case MYSQL_TYPE_TIME: {  // legacy: 3 bytes, HHMMSS as a packed decimal
            if (!need(3))
                return {};
            std::int64_t v = read_le_signed(data + pos, 3);
            pos += 3;
            const char* sign = v < 0 ? "-" : "";
            std::int64_t a = v < 0 ? -v : v;
            return fmt("%s%02lld:%02lld:%02lld", sign, a / 10000, (a / 100) % 100, a % 100);
        }
        case MYSQL_TYPE_DATETIME: {  // legacy: 8 bytes YYYYMMDDHHMMSS decimal
            if (!need(8))
                return {};
            std::uint64_t v = read_le(data + pos, 8);
            pos += 8;
            std::uint64_t date = v / 1000000, t = v % 1000000;
            return fmt("%04llu-%02llu-%02llu %02llu:%02llu:%02llu",
                       date / 10000,
                       (date / 100) % 100,
                       date % 100,
                       t / 10000,
                       (t / 100) % 100,
                       t % 100);
        }
        case MYSQL_TYPE_TIMESTAMP: {  // legacy: 4-byte LE epoch
            if (!need(4))
                return {};
            std::time_t e = static_cast<std::time_t>(read_le(data + pos, 4));
            pos += 4;
            std::tm tv{};
            gmtime_r(&e, &tv);
            return fmt("%04d-%02d-%02d %02d:%02d:%02d",
                       tv.tm_year + 1900,
                       tv.tm_mon + 1,
                       tv.tm_mday,
                       tv.tm_hour,
                       tv.tm_min,
                       tv.tm_sec);
        }
        case MYSQL_TYPE_DATETIME2: {
            const int frac = cm.m0;
            const std::size_t len = 5 + static_cast<std::size_t>((frac + 1) / 2);
            if (!need(len))
                return {};
            std::string s = decode_datetime2(data + pos, frac);
            pos += len;
            return s;
        }
        case MYSQL_TYPE_TIMESTAMP2: {
            const int frac = cm.m0;
            const std::size_t len = 4 + static_cast<std::size_t>((frac + 1) / 2);
            if (!need(len))
                return {};
            std::string s = decode_timestamp2(data + pos, frac);
            pos += len;
            return s;
        }
        case MYSQL_TYPE_TIME2: {
            const int frac = cm.m0;
            const std::size_t len = 3 + static_cast<std::size_t>((frac + 1) / 2);
            if (!need(len))
                return {};
            std::string s = decode_time2(data + pos, frac);
            pos += len;
            return s;
        }
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING: {
            const int max_len = cm.m0 | (cm.m1 << 8);
            const int lp = max_len < 256 ? 1 : 2;
            if (!need(static_cast<std::size_t>(lp)))
                return {};
            std::size_t n = static_cast<std::size_t>(read_le(data + pos, lp));
            pos += lp;
            if (!need(n))
                return {};
            std::string s(reinterpret_cast<const char*>(data + pos), n);
            pos += n;
            return s;
        }
        case MYSQL_TYPE_STRING: {
            // STRING packs the real type + length: byte0 has the type bits, byte1
            // the low length bits. ENUM/SET arrive here with real type 0xf7/0xf8.
            int real_type = cm.m0;
            int length;
            if ((cm.m0 & 0x30) != 0x30) {
                length = cm.m1 | (((cm.m0 & 0x30) ^ 0x30) << 4);
                real_type = cm.m0 | 0x30;
            } else {
                length = cm.m1;
            }
            if (real_type == MYSQL_TYPE_ENUM) {
                const int sz = length;  // 1 or 2 bytes
                if (!need(static_cast<std::size_t>(sz)))
                    return {};
                std::uint64_t idx = read_le(data + pos, sz);
                pos += sz;
                if (idx >= 1 && idx <= col.enum_set_labels.size()) {
                    return col.enum_set_labels[idx - 1];
                }
                return std::to_string(idx);
            }
            if (real_type == MYSQL_TYPE_SET) {
                const int sz = length;  // 1..8 bytes
                if (!need(static_cast<std::size_t>(sz)))
                    return {};
                std::uint64_t bits = read_le(data + pos, sz);
                pos += sz;
                if (col.enum_set_labels.empty()) {
                    return std::to_string(bits);
                }
                std::string out;
                for (std::size_t i = 0; i < col.enum_set_labels.size() && i < 64; ++i) {
                    if ((bits & (std::uint64_t{1} << i)) != 0) {
                        if (!out.empty())
                            out.push_back(',');
                        out += col.enum_set_labels[i];
                    }
                }
                return out;
            }
            // CHAR / BINARY: 1-byte length prefix if max length < 256, else 2.
            const int lp = length < 256 ? 1 : 2;
            if (!need(static_cast<std::size_t>(lp)))
                return {};
            std::size_t n = static_cast<std::size_t>(read_le(data + pos, lp));
            pos += lp;
            if (!need(n))
                return {};
            std::string s(reinterpret_cast<const char*>(data + pos), n);
            pos += n;
            return s;
        }
        case MYSQL_TYPE_ENUM: {  // rare direct (MariaDB); m0 = pack length
            const int sz = cm.m0 ? cm.m0 : 1;
            if (!need(static_cast<std::size_t>(sz)))
                return {};
            std::uint64_t idx = read_le(data + pos, sz);
            pos += sz;
            if (idx >= 1 && idx <= col.enum_set_labels.size())
                return col.enum_set_labels[idx - 1];
            return std::to_string(idx);
        }
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB: {
            const int lp = cm.m0 ? cm.m0 : 2;  // length-bytes count from metadata
            if (!need(static_cast<std::size_t>(lp)))
                return {};
            std::size_t n = static_cast<std::size_t>(read_le(data + pos, lp));
            pos += lp;
            if (!need(n))
                return {};
            std::string s(reinterpret_cast<const char*>(data + pos), n);
            pos += n;
            return s;
        }
        case MYSQL_TYPE_JSON: {
            const int lp = cm.m0 ? cm.m0 : 4;
            if (!need(static_cast<std::size_t>(lp)))
                return {};
            std::size_t n = static_cast<std::size_t>(read_le(data + pos, lp));
            pos += lp;
            if (!need(n))
                return {};
            std::string_view raw(reinterpret_cast<const char*>(data + pos), n);
            pos += n;
            if (auto text = jsonb::decode(raw)) {
                return *text;
            }
            return "null";
        }
        case MYSQL_TYPE_GEOMETRY: {
            const int lp = cm.m0 ? cm.m0 : 4;
            if (!need(static_cast<std::size_t>(lp)))
                return {};
            std::size_t n = static_cast<std::size_t>(read_le(data + pos, lp));
            pos += lp;
            if (!need(n))
                return {};
            // strip the 4-byte SRID prefix, hex-encode the WKB (stable + reversible)
            const std::uint8_t* g = data + pos;
            std::size_t glen = n;
            if (glen >= 4) {
                g += 4;
                glen -= 4;
            }
            pos += n;
            return to_hex(g, glen);
        }
        case MYSQL_TYPE_BIT: {
            // metadata: value = m0 | (m1<<8); bits = (value>>8)*8 + (value&0xFF).
            const int v = cm.m0 | (cm.m1 << 8);
            const int bits = (v >> 8) * 8 + (v & 0xFF);
            const int nbytes = (bits + 7) / 8;
            if (!need(static_cast<std::size_t>(nbytes)))
                return {};
            std::uint64_t val = read_be(data + pos, nbytes);  // BIT is big-endian
            pos += nbytes;
            return std::to_string(val);
        }
        default: {
            // Unknown type: we cannot know its length, so we cannot safely advance.
            // Signal failure so the whole row is dropped + counted (no desync).
            ok = false;
            return {};
        }
    }
}

// Decode one row image (null bitmap + present non-null values) into CdcFields.
// `present` = number of columns present in the row's column bitmap (FULL => all).
// Returns false on any bounds/format error (the caller drops + counts the row).
inline bool decode_row_image(const std::uint8_t* data,
                             std::size_t size,
                             std::size_t& pos,
                             const std::vector<ColumnMeta>& metas,
                             const CdcTableSchema& schema,
                             std::vector<CdcField>& out) {
    const std::size_t n = metas.size();
    if (n != schema.columns.size()) {
        return false;  // schema/table-map disagree -> poison
    }
    const std::size_t null_bytes = (n + 7) / 8;
    if (pos + null_bytes > size) {
        return false;
    }
    const std::uint8_t* null_bitmap = data + pos;
    pos += null_bytes;
    out.clear();
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        CdcField f;
        f.name = schema.columns[i].name;
        const bool is_null = (null_bitmap[i / 8] & (1u << (i % 8))) != 0;
        if (is_null) {
            f.is_null = true;
        } else {
            bool ok = true;
            f.value = decode_value(data, size, pos, metas[i], schema.columns[i], ok);
            if (!ok) {
                return false;
            }
        }
        out.push_back(std::move(f));
    }
    return true;
}

// Decode a whole rows-event payload (all rows concatenated) into CdcEvents.
// WRITE -> Insert per row; DELETE -> Delete per row; UPDATE -> Update per
// (before,after) pair using the AFTER image. A row that fails to decode is
// dropped + counted, and on a hard desync the remainder of the payload is
// abandoned (also counted) rather than emitting garbage.
inline MysqlDecodeResult decode_rows_payload(const std::uint8_t* data,
                                             std::size_t size,
                                             RowOp op,
                                             const std::vector<ColumnMeta>& metas,
                                             const CdcTableSchema& schema,
                                             const std::string& lsn,
                                             std::int64_t xid) {
    MysqlDecodeResult res;
    std::size_t pos = 0;
    // UPDATE_ROWS row_data carries the after-image column bitmap (ceil(n/8) bytes)
    // the rpl library leaves unparsed, ONCE before the [before,after] row pairs
    // (verified against MySQL 8.0). WRITE/DELETE have no such leading bitmap.
    if (op == RowOp::Update) {
        pos += (metas.size() + 7) / 8;
        if (pos > size) {
            ++res.dropped;
            return res;
        }
    }
    auto make_event = [&](CdcEvent::Op cop, std::vector<CdcField>&& fields) {
        CdcEvent ev;
        ev.op = cop;
        ev.table = schema.qualified();
        ev.lsn = lsn;
        ev.xid = xid;
        ev.values = std::move(fields);
        res.events.push_back(std::move(ev));
    };
    while (pos < size) {
        if (op == RowOp::Update) {
            std::vector<CdcField> before;
            std::vector<CdcField> after;
            if (!decode_row_image(data, size, pos, metas, schema, before) || pos >= size ||
                !decode_row_image(data, size, pos, metas, schema, after)) {
                ++res.dropped;
                break;  // desync: abandon the rest of the payload
            }
            make_event(CdcEvent::Op::Update, std::move(after));
        } else {
            std::vector<CdcField> img;
            if (!decode_row_image(data, size, pos, metas, schema, img)) {
                ++res.dropped;
                break;
            }
            make_event(op == RowOp::Insert ? CdcEvent::Op::Insert : CdcEvent::Op::Delete,
                       std::move(img));
        }
    }
    return res;
}

}  // namespace clink::mysql
