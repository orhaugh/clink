#include "clink/connectors/postgres_decoder.hpp"

#include <array>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <utility>

namespace clink::pg {

std::uint16_t read_be16(const char* p) noexcept {
    return static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(static_cast<unsigned char>(p[0])) << 8) |
        static_cast<std::uint16_t>(static_cast<unsigned char>(p[1])));
}

std::uint32_t read_be32(const char* p) noexcept {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

std::uint64_t read_be64(const char* p) noexcept {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

void write_be64(char* p, std::uint64_t v) noexcept {
    for (int i = 7; i >= 0; --i) {
        p[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

std::string read_cstring(std::string_view& cursor) {
    const auto end = cursor.find('\0');
    if (end == std::string_view::npos) {
        cursor = {};
        return {};
    }
    std::string s{cursor.substr(0, end)};
    cursor.remove_prefix(end + 1);
    return s;
}

std::string format_lsn(std::uint64_t lsn) {
    constexpr std::uint64_t hi_shift = 32;
    std::array<char, 32> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  "%X/%X",
                  static_cast<std::uint32_t>(lsn >> hi_shift),
                  static_cast<std::uint32_t>(lsn));
    return std::string{buf.data()};
}

bool starts_with(std::string_view s, std::string_view prefix) noexcept {
    return s.size() >= prefix.size() && s.substr(0, prefix.size()) == prefix;
}

const char* lookup_builtin_type_name(std::uint32_t oid) noexcept {
    switch (oid) {
        case 16:
            return "bool";
        case 17:
            return "bytea";
        case 18:
            return "char";
        case 20:
            return "int8";
        case 21:
            return "int2";
        case 23:
            return "int4";
        case 25:
            return "text";
        case 700:
            return "float4";
        case 701:
            return "float8";
        case 1042:
            return "bpchar";
        case 1043:
            return "varchar";
        case 1082:
            return "date";
        case 1083:
            return "time";
        case 1114:
            return "timestamp";
        case 1184:
            return "timestamptz";
        case 1700:
            return "numeric";
        case 2950:
            return "uuid";
        case 3802:
            return "jsonb";
        case 114:
            return "json";
        default:
            return nullptr;
    }
}

std::int64_t postgres_epoch_us_now() noexcept {
    using namespace std::chrono;
    const auto unix_us =
        duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
    return static_cast<std::int64_t>(unix_us) - kPgEpochOffsetUs;
}

std::int64_t parse_xid(std::string_view payload) {
    auto pos = payload.find(' ');
    if (pos == std::string_view::npos) {
        return 0;
    }
    try {
        return std::stoll(std::string{payload.substr(pos + 1)});
    } catch (const std::exception&) {
        return 0;
    }
}

std::vector<CdcField> parse_test_decoding_fields(std::string_view rest) {
    std::vector<CdcField> fields;
    std::size_t i = 0;
    while (i < rest.size()) {
        while (i < rest.size() && rest[i] == ' ') {
            ++i;
        }
        const std::size_t name_start = i;
        while (i < rest.size() && rest[i] != '[') {
            ++i;
        }
        if (i >= rest.size()) {
            break;
        }
        std::string name(rest.substr(name_start, i - name_start));
        // Capture the type from inside the brackets.
        ++i;  // past '['
        const std::size_t type_start = i;
        while (i < rest.size() && rest[i] != ']') {
            ++i;
        }
        if (i >= rest.size()) {
            break;
        }
        std::string type_name(rest.substr(type_start, i - type_start));
        ++i;  // past ']'
        if (i >= rest.size() || rest[i] != ':') {
            break;
        }
        ++i;  // past ':'
        std::string value;
        bool is_null = false;
        if (i < rest.size() && rest[i] == '\'') {
            ++i;
            while (i < rest.size()) {
                if (rest[i] == '\'') {
                    if (i + 1 < rest.size() && rest[i + 1] == '\'') {
                        value.push_back('\'');
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                value.push_back(rest[i]);
                ++i;
            }
        } else {
            // Unquoted token. test_decoding emits the literal string `null`
            // for SQL NULL values.
            while (i < rest.size() && rest[i] != ' ') {
                value.push_back(rest[i]);
                ++i;
            }
            if (value == "null") {
                is_null = true;
                value.clear();
            }
        }
        fields.push_back(CdcField{.name = std::move(name),
                                  .value = std::move(value),
                                  .type = std::move(type_name),
                                  .is_null = is_null});
    }
    return fields;
}

CdcEvent parse_test_decoding(std::string_view payload, std::string lsn_text) {
    CdcEvent ev;
    ev.lsn = std::move(lsn_text);

    if (starts_with(payload, "BEGIN ")) {
        ev.op = CdcEvent::Op::Begin;
        ev.xid = parse_xid(payload);
        return ev;
    }
    if (starts_with(payload, "COMMIT ")) {
        ev.op = CdcEvent::Op::Commit;
        ev.xid = parse_xid(payload);
        return ev;
    }
    if (starts_with(payload, "table ")) {
        const auto after_table = payload.substr(6);
        const auto colon1 = after_table.find(':');
        if (colon1 == std::string_view::npos) {
            ev.op = CdcEvent::Op::Unknown;
            ev.values.push_back(CdcField{
                .name = "raw", .value = std::string{payload}, .type = "", .is_null = false});
            return ev;
        }
        ev.table = std::string{after_table.substr(0, colon1)};
        std::size_t i = colon1 + 1;
        while (i < after_table.size() && after_table[i] == ' ') {
            ++i;
        }
        const auto colon2 = after_table.find(':', i);
        if (colon2 == std::string_view::npos) {
            ev.op = CdcEvent::Op::Unknown;
            ev.values.push_back(CdcField{
                .name = "raw", .value = std::string{payload}, .type = "", .is_null = false});
            return ev;
        }
        const auto op_text = after_table.substr(i, colon2 - i);
        if (op_text == "INSERT") {
            ev.op = CdcEvent::Op::Insert;
        } else if (op_text == "UPDATE") {
            ev.op = CdcEvent::Op::Update;
        } else if (op_text == "DELETE") {
            ev.op = CdcEvent::Op::Delete;
        } else {
            ev.op = CdcEvent::Op::Unknown;
        }
        std::size_t k = colon2 + 1;
        while (k < after_table.size() && after_table[k] == ' ') {
            ++k;
        }
        ev.values = parse_test_decoding_fields(after_table.substr(k));
        return ev;
    }

    ev.op = CdcEvent::Op::Unknown;
    ev.values.push_back(
        CdcField{.name = "raw", .value = std::string{payload}, .type = "", .is_null = false});
    return ev;
}

}  // namespace clink::pg
