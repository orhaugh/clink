// The names, key types and changelog records the Cassandra sinks accept, and the
// redaction applied to server messages before they are repeated.

#include "clink/cassandra/cassandra_rules.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <utility>

#include "clink/cassandra/connection_params.hpp"
#include "clink/config/json.hpp"

namespace clink::cassandra {

namespace {

char lower(char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

std::string folded(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(), lower);
    return out;
}

bool is_ws(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

bool is_hex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    return c - 'A' + 10;
}

// ---- the record's top-level members, as written --------------------------------------
//
// The record has already been parsed as JSON, so these walk text known to be
// valid; they throw only if that ever stops being true.

std::size_t skip_ws(std::string_view s, std::size_t i) {
    while (i < s.size() && is_ws(s[i])) {
        ++i;
    }
    return i;
}

[[noreturn]] void malformed() {
    throw CassandraRecordError("the record is not a well-formed JSON object");
}

// One past the closing quote of the string that opens at s[i].
std::size_t string_end(std::string_view s, std::size_t i) {
    for (++i; i < s.size(); ++i) {
        if (s[i] == '\\') {
            ++i;
        } else if (s[i] == '"') {
            return i + 1;
        }
    }
    malformed();
}

// One past the end of the value that starts at s[i].
std::size_t value_end(std::string_view s, std::size_t i) {
    if (i >= s.size()) {
        malformed();
    }
    if (s[i] == '"') {
        return string_end(s, i);
    }
    if (s[i] == '{' || s[i] == '[') {
        int depth = 0;
        while (i < s.size()) {
            const char c = s[i];
            if (c == '"') {
                i = string_end(s, i);
                continue;
            }
            if (c == '{' || c == '[') {
                ++depth;
            } else if (c == '}' || c == ']') {
                if (--depth == 0) {
                    return i + 1;
                }
            }
            ++i;
        }
        malformed();
    }
    while (i < s.size() && s[i] != ',' && s[i] != '}' && s[i] != ']' && !is_ws(s[i])) {
        ++i;
    }
    return i;
}

void append_utf8(std::string& out, std::uint32_t cp) {
    if (cp < 0x80) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

std::uint32_t read_u16(std::string_view s, std::size_t i) {
    if (i + 4 > s.size()) {
        malformed();
    }
    std::uint32_t v = 0;
    for (std::size_t k = 0; k < 4; ++k) {
        if (!is_hex(s[i + k])) {
            malformed();
        }
        v = (v << 4) | static_cast<std::uint32_t>(hex_value(s[i + k]));
    }
    return v;
}

// The text of a JSON string literal, quotes included, with its escapes decoded.
std::string unescape(std::string_view lit) {
    std::string out;
    const std::size_t end = lit.size() - 1;  // the closing quote
    for (std::size_t i = 1; i < end; ++i) {
        const char c = lit[i];
        if (c != '\\') {
            out.push_back(c);
            continue;
        }
        const char e = lit[++i];
        switch (e) {
            case 'b':
                out.push_back('\b');
                break;
            case 'f':
                out.push_back('\f');
                break;
            case 'n':
                out.push_back('\n');
                break;
            case 'r':
                out.push_back('\r');
                break;
            case 't':
                out.push_back('\t');
                break;
            case 'u': {
                std::uint32_t cp = read_u16(lit, i + 1);
                i += 4;
                if (cp >= 0xD800 && cp < 0xDC00 && i + 6 < end && lit[i + 1] == '\\' &&
                    lit[i + 2] == 'u') {
                    const std::uint32_t lo = read_u16(lit, i + 3);
                    if (lo >= 0xDC00 && lo < 0xE000) {
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        i += 6;
                    }
                }
                if (cp >= 0xD800 && cp < 0xE000) {
                    cp = 0xFFFD;  // a lone surrogate names no character
                }
                append_utf8(out, cp);
                break;
            }
            default:  // '"', '\\', '/'
                out.push_back(e);
                break;
        }
    }
    return out;
}

struct Member {
    std::string name;  // decoded
    std::string_view raw_name;
    std::string_view raw_value;
};

std::vector<Member> top_level_members(std::string_view s) {
    std::vector<Member> out;
    std::size_t i = skip_ws(s, 0);
    if (i >= s.size() || s[i] != '{') {
        malformed();
    }
    i = skip_ws(s, i + 1);
    if (i < s.size() && s[i] == '}') {
        return out;
    }
    for (;;) {
        if (i >= s.size() || s[i] != '"') {
            malformed();
        }
        const std::size_t name_end = string_end(s, i);
        Member m;
        m.raw_name = s.substr(i, name_end - i);
        m.name = unescape(m.raw_name);
        i = skip_ws(s, name_end);
        if (i >= s.size() || s[i] != ':') {
            malformed();
        }
        i = skip_ws(s, i + 1);
        const std::size_t v_end = value_end(s, i);
        m.raw_value = s.substr(i, v_end - i);
        out.push_back(std::move(m));
        i = skip_ws(s, v_end);
        if (i < s.size() && s[i] == ',') {
            i = skip_ws(s, i + 1);
            continue;
        }
        if (i < s.size() && s[i] == '}') {
            return out;
        }
        malformed();
    }
}

// ---- key values ----------------------------------------------------------------------

bool json_integer(std::string_view t) {
    std::size_t i = 0;
    if (i < t.size() && t[i] == '-') {
        ++i;
    }
    if (i >= t.size()) {
        return false;
    }
    if (t[i] == '0') {
        return i + 1 == t.size();
    }
    for (; i < t.size(); ++i) {
        if (t[i] < '0' || t[i] > '9') {
            return false;
        }
    }
    return true;
}

bool json_number(std::string_view t) {
    return !t.empty() && (t[0] == '-' || (t[0] >= '0' && t[0] <= '9'));
}

bool json_string(std::string_view t) {
    return !t.empty() && t[0] == '"';
}

bool uuid_text(std::string_view s, bool time_based) {
    if (s.size() != 36) {
        return false;
    }
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s[i] != '-') {
                return false;
            }
        } else if (!is_hex(s[i])) {
            return false;
        }
    }
    return !time_based || s[14] == '1';
}

std::string key_rule_broken(std::string_view raw, CqlKeyType type) {
    const auto in_range = [&raw](std::int64_t lo, std::int64_t hi) {
        std::int64_t v = 0;
        const auto res = std::from_chars(raw.data(), raw.data() + raw.size(), v);
        return res.ec == std::errc{} && res.ptr == raw.data() + raw.size() && v >= lo && v <= hi;
    };
    switch (type) {
        case CqlKeyType::TinyInt:
            return json_integer(raw) && in_range(-128, 127) ? ""
                                                            : "a JSON integer from -128 to 127";
        case CqlKeyType::SmallInt:
            return json_integer(raw) && in_range(-32768, 32767)
                       ? ""
                       : "a JSON integer from -32768 to 32767";
        case CqlKeyType::Int:
            return json_integer(raw) && in_range(std::numeric_limits<std::int32_t>::min(),
                                                 std::numeric_limits<std::int32_t>::max())
                       ? ""
                       : "a JSON integer within 32 bits";
        case CqlKeyType::BigInt:
            return json_integer(raw) && in_range(std::numeric_limits<std::int64_t>::min(),
                                                 std::numeric_limits<std::int64_t>::max())
                       ? ""
                       : "a JSON integer within 64 bits";
        case CqlKeyType::Varint:
            return json_integer(raw) ? "" : "a JSON integer";
        case CqlKeyType::Decimal:
            return json_number(raw) || json_string(raw) ? "" : "a JSON number or string";
        case CqlKeyType::Float:
        case CqlKeyType::Double:
            return json_number(raw) ? "" : "a JSON number";
        case CqlKeyType::Text:
            return json_string(raw) ? "" : "a JSON string";
        case CqlKeyType::Ascii: {
            if (!json_string(raw)) {
                return "a JSON string of ASCII characters";
            }
            const std::string s = unescape(raw);
            return std::all_of(s.begin(),
                               s.end(),
                               [](char c) { return static_cast<unsigned char>(c) < 0x80; })
                       ? ""
                       : "a JSON string of ASCII characters";
        }
        case CqlKeyType::Boolean:
            return raw == "true" || raw == "false" ? "" : "true or false";
        case CqlKeyType::Uuid:
        case CqlKeyType::TimeUuid: {
            const bool time_based = type == CqlKeyType::TimeUuid;
            const char* rule = time_based ? "a JSON string holding a version-1 UUID"
                                          : "a JSON string holding a UUID";
            return json_string(raw) && uuid_text(unescape(raw), time_based) ? "" : rule;
        }
        case CqlKeyType::Timestamp:
        case CqlKeyType::Date:
        case CqlKeyType::Time:
            return json_string(raw) || json_integer(raw) ? "" : "a JSON string or integer";
        case CqlKeyType::Inet:
            return json_string(raw) ? "" : "a JSON string";
        case CqlKeyType::Blob: {
            const char* rule = "a JSON string \"0x\" followed by pairs of hex digits";
            if (!json_string(raw)) {
                return rule;
            }
            const std::string s = unescape(raw);
            if (s.size() < 2 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') || s.size() % 2 != 0) {
                return rule;
            }
            return std::all_of(s.begin() + 2, s.end(), is_hex) ? "" : rule;
        }
    }
    return "a value of its type";
}

constexpr std::array<std::string_view, 4> kRowKinds = {
    "insert", "update_after", "delete", "update_before"};

}  // namespace

// ---- consistency ---------------------------------------------------------------------

std::optional<CassandraConsistency> parse_cassandra_consistency(std::string_view name) {
    static const std::map<std::string, CassandraConsistency, std::less<>> kByName = {
        {"one", CassandraConsistency::One},
        {"two", CassandraConsistency::Two},
        {"three", CassandraConsistency::Three},
        {"quorum", CassandraConsistency::Quorum},
        {"all", CassandraConsistency::All},
        {"local_quorum", CassandraConsistency::LocalQuorum},
        {"each_quorum", CassandraConsistency::EachQuorum},
        {"local_one", CassandraConsistency::LocalOne},
    };
    const auto it = kByName.find(folded(name));
    if (it == kByName.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::string_view cassandra_consistency_name(CassandraConsistency c) noexcept {
    switch (c) {
        case CassandraConsistency::One:
            return "one";
        case CassandraConsistency::Two:
            return "two";
        case CassandraConsistency::Three:
            return "three";
        case CassandraConsistency::Quorum:
            return "quorum";
        case CassandraConsistency::All:
            return "all";
        case CassandraConsistency::LocalQuorum:
            return "local_quorum";
        case CassandraConsistency::EachQuorum:
            return "each_quorum";
        case CassandraConsistency::LocalOne:
            return "local_one";
    }
    return "local_one";
}

// ---- names ---------------------------------------------------------------------------

std::optional<std::string> cql_identifier(std::string_view name,
                                          std::size_t max_length,
                                          std::string* why) {
    const auto refuse = [why](std::string reason) -> std::optional<std::string> {
        if (why != nullptr) {
            *why = std::move(reason);
        }
        return std::nullopt;
    };
    if (name.empty()) {
        return refuse("it is empty");
    }
    if (name.size() > max_length) {
        return refuse("it is longer than " + std::to_string(max_length) + " characters");
    }
    const auto letter = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); };
    if (!letter(name[0])) {
        return refuse("it does not start with a letter");
    }
    for (const char c : name) {
        if (!letter(c) && !(c >= '0' && c <= '9') && c != '_') {
            return refuse(
                "it holds a character other than a letter, a digit or an underscore; names that "
                "need quoting in CQL are not supported");
        }
    }
    return folded(name);
}

// ---- key types -----------------------------------------------------------------------

std::optional<CqlKeyType> cql_key_type(std::string_view schema_type) {
    static const std::map<std::string, CqlKeyType, std::less<>> kByName = {
        {"tinyint", CqlKeyType::TinyInt},
        {"smallint", CqlKeyType::SmallInt},
        {"int", CqlKeyType::Int},
        {"bigint", CqlKeyType::BigInt},
        {"varint", CqlKeyType::Varint},
        {"decimal", CqlKeyType::Decimal},
        {"float", CqlKeyType::Float},
        {"double", CqlKeyType::Double},
        {"text", CqlKeyType::Text},
        {"varchar", CqlKeyType::Text},
        {"ascii", CqlKeyType::Ascii},
        {"boolean", CqlKeyType::Boolean},
        {"uuid", CqlKeyType::Uuid},
        {"timeuuid", CqlKeyType::TimeUuid},
        {"timestamp", CqlKeyType::Timestamp},
        {"date", CqlKeyType::Date},
        {"time", CqlKeyType::Time},
        {"inet", CqlKeyType::Inet},
        {"blob", CqlKeyType::Blob},
    };
    const auto it = kByName.find(schema_type);
    if (it == kByName.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::string_view cql_key_type_name(CqlKeyType t) noexcept {
    switch (t) {
        case CqlKeyType::TinyInt:
            return "tinyint";
        case CqlKeyType::SmallInt:
            return "smallint";
        case CqlKeyType::Int:
            return "int";
        case CqlKeyType::BigInt:
            return "bigint";
        case CqlKeyType::Varint:
            return "varint";
        case CqlKeyType::Decimal:
            return "decimal";
        case CqlKeyType::Float:
            return "float";
        case CqlKeyType::Double:
            return "double";
        case CqlKeyType::Text:
            return "text";
        case CqlKeyType::Ascii:
            return "ascii";
        case CqlKeyType::Boolean:
            return "boolean";
        case CqlKeyType::Uuid:
            return "uuid";
        case CqlKeyType::TimeUuid:
            return "timeuuid";
        case CqlKeyType::Timestamp:
            return "timestamp";
        case CqlKeyType::Date:
            return "date";
        case CqlKeyType::Time:
            return "time";
        case CqlKeyType::Inet:
            return "inet";
        case CqlKeyType::Blob:
            return "blob";
    }
    return "text";
}

// ---- records -------------------------------------------------------------------------

CassandraUpsertRecord prepare_cassandra_upsert_record(std::string_view record,
                                                      const std::vector<CassandraKeyColumn>& key) {
    // The full parse is the syntax and UTF-8 check; the walk below reads the
    // members as written, so no number passes through a double on its way out.
    clink::config::JsonValue parsed;
    try {
        parsed = clink::config::parse(record);
    } catch (const std::exception& e) {
        // The reader also refuses an integer of 2^64 or more and a number past
        // a double's range, which JSON itself allows; its reason says which.
        throw CassandraRecordError(
            "the record is not valid JSON, or holds a number the sink's "
            "JSON reader cannot read (" +
            redact_cassandra_message(e.what()) + ")");
    }
    if (!parsed.is_object()) {
        throw CassandraRecordError("the record is not a JSON object");
    }
    const std::vector<Member> members = top_level_members(record);

    std::map<std::string, std::size_t> by_folded;
    for (std::size_t i = 0; i < members.size(); ++i) {
        const auto [it, fresh] = by_folded.emplace(folded(members[i].name), i);
        if (!fresh) {
            throw CassandraRecordError(
                "two of the record's fields name the same column once folded to lower case (" +
                std::string{it->first.size() <= 64 ? it->first : it->first.substr(0, 64)} +
                "), so which value the server would keep is undefined");
        }
    }

    CassandraUpsertRecord out;
    std::optional<std::size_t> kind_at;
    if (const auto it = by_folded.find("__row_kind"); it != by_folded.end()) {
        kind_at = it->second;
        const std::string_view raw = members[it->second].raw_value;
        const std::string kind = json_string(raw) ? unescape(raw) : std::string{};
        if (std::find(kRowKinds.begin(), kRowKinds.end(), kind) == kRowKinds.end()) {
            throw CassandraRecordError(
                "its __row_kind is not one of the changelog kinds the sink accepts (\"insert\", "
                "\"update_after\", \"delete\", \"update_before\")");
        }
        out.is_delete = kind == "delete" || kind == "update_before";
    }

    out.key_json.reserve(key.size());
    for (const auto& col : key) {
        const auto it = by_folded.find(col.name);
        if (it == by_folded.end()) {
            throw CassandraRecordError("the key column '" + col.name + "' is missing");
        }
        const std::string_view raw = members[it->second].raw_value;
        if (raw == "null") {
            throw CassandraRecordError("the key column '" + col.name + "' is null");
        }
        if (const std::string rule = key_rule_broken(raw, col.type); !rule.empty()) {
            throw CassandraRecordError("the key column '" + col.name + "' is " +
                                       std::string{cql_key_type_name(col.type)} + " and needs " +
                                       rule);
        }
        out.key_json.emplace_back(raw);
        out.identity += std::to_string(raw.size());
        out.identity.push_back(':');
        out.identity.append(raw);
    }

    if (!out.is_delete) {
        if (!kind_at) {
            out.insert_json.assign(record);
        } else {
            out.insert_json.reserve(record.size());
            out.insert_json.push_back('{');
            bool first = true;
            for (std::size_t i = 0; i < members.size(); ++i) {
                if (i == *kind_at) {
                    continue;
                }
                if (!first) {
                    out.insert_json.push_back(',');
                }
                first = false;
                out.insert_json.append(members[i].raw_name);
                out.insert_json.push_back(':');
                out.insert_json.append(members[i].raw_value);
            }
            out.insert_json.push_back('}');
        }
    }
    return out;
}

// ---- messages ------------------------------------------------------------------------

std::string redact_cassandra_message(std::string_view message) {
    if (const auto src = message.find("[Source:"); src != std::string_view::npos) {
        message = message.substr(0, src);
    }
    std::string out;
    out.reserve(std::min<std::size_t>(message.size(), 300));
    char quote = 0;
    for (const char c : message) {
        if (quote != 0) {
            if (c == quote) {
                out += "...";
                out.push_back(c);
                quote = 0;
            }
            continue;
        }
        if (c == '\'' || c == '"') {
            quote = c;
            out.push_back(c);
            continue;
        }
        out.push_back((c == '\n' || c == '\r' || c == '\t') ? ' ' : c);
    }
    if (quote != 0) {
        out += "...";
    }
    while (!out.empty() && out.back() == ' ') {
        out.pop_back();
    }
    if (out.size() > 300) {
        out.resize(297);
        out += "...";
    }
    return out;
}

}  // namespace clink::cassandra
