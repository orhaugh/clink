// The PostgreSQL sinks' pure rules. See pg_sink_rules.hpp.

#include "clink/postgres/pg_sink_rules.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "clink/connectors/postgres_sql.hpp"

namespace clink::postgres {

namespace {

// The longest plain decimal a number is written with; a longer one is written
// as its token, which PostgreSQL reads as an exact numeric too.
constexpr std::size_t kPlainChars = 4096;
// A number's exponent is read up to this magnitude.
constexpr std::int64_t kExponentCap = 1'000'000;

bool contains(const std::vector<std::string>& v, std::string_view x) {
    return std::find(v.begin(), v.end(), x) != v.end();
}

std::string joined(const std::vector<std::string>& v) {
    std::string out;
    for (const auto& x : v) {
        out += (out.empty() ? "" : ", ") + x;
    }
    return out;
}

bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

bool is_integer_type(std::string_view t) {
    return t == "int2" || t == "int4" || t == "int8";
}

bool is_float_type(std::string_view t) {
    return t == "float4" || t == "float8";
}

bool is_text_type(std::string_view t) {
    return t == "text" || t == "varchar";
}

bool is_timestamp_type(std::string_view t) {
    return t == "timestamp" || t == "timestamptz";
}

// Code points in valid UTF-8.
std::size_t code_points(std::string_view s) {
    std::size_t n = 0;
    for (const char c : s) {
        n += (static_cast<unsigned char>(c) & 0xC0U) != 0x80U ? 1 : 0;
    }
    return n;
}

// `s` without surrounding spaces when it is a decimal number as PostgreSQL's
// numeric input reads one, else empty.
std::string_view decimal_text(std::string_view s) {
    while (!s.empty() && s.front() == ' ') {
        s.remove_prefix(1);
    }
    while (!s.empty() && s.back() == ' ') {
        s.remove_suffix(1);
    }
    std::size_t i = 0;
    if (i < s.size() && (s[i] == '+' || s[i] == '-')) {
        ++i;
    }
    std::size_t digits = 0;
    while (i < s.size() && is_digit(s[i])) {
        ++i;
        ++digits;
    }
    if (i < s.size() && s[i] == '.') {
        ++i;
        while (i < s.size() && is_digit(s[i])) {
            ++i;
            ++digits;
        }
    }
    if (digits == 0) {
        return {};
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
        ++i;
        if (i < s.size() && (s[i] == '+' || s[i] == '-')) {
            ++i;
        }
        const auto exponent_begin = i;
        while (i < s.size() && is_digit(s[i])) {
            ++i;
        }
        if (i == exponent_begin) {
            return {};
        }
    }
    return i == s.size() ? s : std::string_view{};
}

std::string hex_bytea(std::string_view s) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out = "'\\x";
    for (const char c : s) {
        const auto b = static_cast<unsigned char>(c);
        out.push_back(kHex[b >> 4U]);
        out.push_back(kHex[b & 0x0FU]);
    }
    out.push_back('\'');
    return out;
}

void refuse_rounding(const std::string& number, const PgColumn& col, std::int64_t scale) {
    if (needed_scale(number) <= scale) {
        return;
    }
    throw RecordRefused(
        "a number that column '" + col.name + "' (" + col.type + ") would round, as it keeps " +
        (scale >= 0
             ? std::to_string(scale) + " digit(s) after the point"
             : "multiples of 1" + std::string(static_cast<std::size_t>(-scale), '0') + " only"));
}

std::string string_literal(const std::string& text,
                           const PgColumn* col,
                           const sqljson::EscapeFn& esc) {
    // A bytea column takes the string's bytes, NUL among them.
    if (col != nullptr && col->type_name == "bytea") {
        return hex_bytea(text);
    }
    if (text.find('\0') != std::string::npos) {
        throw RecordRefused("a string holding U+0000, which PostgreSQL text cannot hold");
    }
    if (col != nullptr) {
        if ((col->type_name == "varchar" || col->type_name == "bpchar") && col->char_length >= 0 &&
            code_points(text) > static_cast<std::size_t>(col->char_length)) {
            throw RecordRefused("a string of " + std::to_string(code_points(text)) +
                                " characters, more than column '" + col->name + "' (" + col->type +
                                ") holds");
        }
        if (col->type_name == "numeric" && col->numeric_constrained) {
            if (const auto number = decimal_text(text); !number.empty()) {
                refuse_rounding(std::string{number}, *col, col->numeric_scale);
            }
        }
    }
    return "'" + esc(text) + "'";
}

std::string timestamp_literal(const std::string& token,
                              const PgColumn* col,
                              sqljson::TimeKind time) {
    const auto plain = plain_decimal(token, 32);
    std::int64_t ms = 0;
    const auto [end, ec] = std::from_chars(plain.data(), plain.data() + plain.size(), ms);
    if (plain.empty() || plain.find('.') != std::string::npos || ec != std::errc{} ||
        end != plain.data() + plain.size()) {
        throw RecordRefused("a TIMESTAMP that is not a whole number of milliseconds");
    }
    const int precision = col == nullptr || col->time_precision < 0 ? 6 : col->time_precision;
    if (precision < 3) {
        std::int64_t unit = 1;
        for (int k = precision; k < 3; ++k) {
            unit *= 10;
        }
        if (ms % unit != 0) {
            throw RecordRefused("a TIMESTAMP with milliseconds that column " +
                                (col != nullptr ? "'" + col->name + "' " : std::string{}) +
                                "would round away (it keeps " + std::to_string(precision) +
                                " fractional digits)");
        }
    }
    try {
        return sqljson::epoch_ms_to_sql_timestamp(ms, time, sqljson::kPostgres);
    } catch (const std::runtime_error&) {
        throw RecordRefused("a TIMESTAMP outside years 1 to 9999");
    }
}

}  // namespace

// ---- the destination table ------------------------------------------------------------

const PgColumn* PgTable::column(std::string_view name) const {
    for (const auto& c : columns) {
        if (c.name == name) {
            return &c;
        }
    }
    return nullptr;
}

bool pg_key_identity_exact(const PgColumn& c, bool epoch_ms) {
    if (is_integer_type(c.type_name) || c.type_name == "numeric") {
        return true;
    }
    if (is_text_type(c.type_name)) {
        return c.collation_deterministic;
    }
    if (is_timestamp_type(c.type_name)) {
        return epoch_ms && (c.time_precision < 0 || c.time_precision >= 3);
    }
    return false;
}

std::vector<std::string> check_pg_target(const PgSinkTarget& t, const PgTable& s) {
    std::vector<std::string> why;
    if (!s.exists) {
        why.push_back("table " + t.table + " does not exist, or the sink's user cannot see it");
        return why;
    }
    const bool keyed = t.mode != PgSinkMode::Append;
    if (keyed && s.kind != 'r' && s.kind != 'p') {
        why.push_back("table " + t.table + " is " +
                      (s.kind == 'v'   ? std::string{"a view"}
                       : s.kind == 'f' ? std::string{"a foreign table"}
                                       : std::string{"not a table"}) +
                      ", which ON CONFLICT cannot write by key");
        return why;
    }
    for (const auto& name : t.columns) {
        const auto* c = s.column(name);
        if (c == nullptr) {
            why.push_back("table " + t.table + " has no column '" + name + "'");
        } else if (c->generated) {
            why.push_back("column '" + name + "' is a generated column, which takes no value");
        } else if (c->identity_always) {
            why.push_back("column '" + name +
                          "' is GENERATED ALWAYS AS IDENTITY, which takes no value");
        }
    }
    for (const auto& [name, kind] : t.timestamp_columns) {
        (void)kind;
        const auto* c = s.column(name);
        if (c != nullptr && contains(t.columns, name) && !is_timestamp_type(c->type_name)) {
            why.push_back("column '" + name + "' carries a TIMESTAMP and is " + c->type +
                          " in the table; it must be timestamp or timestamptz");
        }
    }
    if (!why.empty() || !keyed) {
        return why;
    }
    for (const auto& k : t.conflict_columns) {
        if (!contains(t.columns, k)) {
            why.push_back("key column '" + k + "' is not one of the columns the sink writes");
        }
    }
    for (const auto& u : t.update_columns) {
        if (!contains(t.columns, u)) {
            why.push_back("update column '" + u + "' is not one of the columns the sink writes");
        }
    }
    const std::set<std::string> key(t.conflict_columns.begin(), t.conflict_columns.end());
    bool arbiter = false;
    std::vector<std::string> unusable;
    for (const auto& idx : s.unique_indexes) {
        const bool same = idx.columns.size() == key.size() &&
                          std::set<std::string>(idx.columns.begin(), idx.columns.end()) == key;
        if (same && !idx.partial && !idx.expression) {
            arbiter = true;
        } else if (same) {
            unusable.push_back(idx.name);
        }
    }
    if (!arbiter) {
        why.push_back("table " + t.table + " has no PRIMARY KEY or UNIQUE index on exactly (" +
                      joined(t.conflict_columns) + ") for ON CONFLICT to write by" +
                      (unusable.empty() ? std::string{}
                                        : " (" + joined(unusable) +
                                              " covers part of the rows or an expression)"));
    }
    if (t.mode == PgSinkMode::Changelog && t.parallelism > 1) {
        for (const auto& k : t.conflict_columns) {
            const auto* c = s.column(k);
            if (c != nullptr &&
                !pg_key_identity_exact(
                    *c, sqljson::time_kind_of(t.timestamp_columns, k) != sqljson::TimeKind::None)) {
                why.push_back(
                    "with " + std::to_string(t.parallelism) + " subtasks key column '" + k + "' (" +
                    c->type +
                    (c->collation.empty() ? std::string{} : ", collation " + c->collation) +
                    ") must tell values apart as the stream does (an integer, a numeric, text or "
                    "varchar under a deterministic collation, or a timestamp of millisecond "
                    "precision or finer written from epoch milliseconds): two subtasks' changes "
                    "to one row would reach it in either order");
            }
        }
    }
    return why;
}

// ---- rendering --------------------------------------------------------------------------

std::int64_t needed_scale(std::string_view number) {
    std::size_t i = 0;
    if (i < number.size() && (number[i] == '+' || number[i] == '-')) {
        ++i;
    }
    std::string digits;
    std::int64_t point = 0;  // digits after the point, as written
    for (; i < number.size() && is_digit(number[i]); ++i) {
        digits.push_back(number[i]);
    }
    if (i < number.size() && number[i] == '.') {
        for (++i; i < number.size() && is_digit(number[i]); ++i) {
            digits.push_back(number[i]);
            ++point;
        }
    }
    std::int64_t exponent = 0;
    if (i < number.size() && (number[i] == 'e' || number[i] == 'E')) {
        ++i;
        bool negative = false;
        if (i < number.size() && (number[i] == '+' || number[i] == '-')) {
            negative = number[i] == '-';
            ++i;
        }
        for (; i < number.size() && is_digit(number[i]); ++i) {
            exponent = std::min<std::int64_t>((exponent * 10) + (number[i] - '0'), kExponentCap);
        }
        if (negative) {
            exponent = -exponent;
        }
    }
    const auto last = digits.find_last_not_of('0');
    if (last == std::string::npos) {
        return std::numeric_limits<std::int64_t>::min();  // zero
    }
    const auto trailing_zeros = static_cast<std::int64_t>(digits.size() - 1 - last);
    return point - exponent - trailing_zeros;
}

std::string render_pg_literal(const JsonMember& v,
                              const PgColumn* col,
                              sqljson::TimeKind time,
                              const sqljson::EscapeFn& esc) {
    switch (v.kind) {
        case JsonMember::Kind::Null:
            return "NULL";
        case JsonMember::Kind::Bool:
            return v.text == "true" ? "TRUE" : "FALSE";
        case JsonMember::Kind::Composite:
            return "'" + esc(v.text) + "'";
        case JsonMember::Kind::String:
            return string_literal(v.text, col, esc);
        case JsonMember::Kind::Number:
            break;
    }
    if (time != sqljson::TimeKind::None) {
        return timestamp_literal(v.text, col, time);
    }
    if (col != nullptr && is_float_type(col->type_name)) {
        // A FLOAT column holds the number the server reads from the token. Past
        // a DOUBLE's range it is refused here, where the server's refusal would
        // quote it.
        if (!std::isfinite(std::strtod(v.text.c_str(), nullptr))) {
            throw RecordRefused("a number outside the range of a DOUBLE");
        }
        return v.text;
    }
    if (col != nullptr && is_integer_type(col->type_name)) {
        refuse_rounding(v.text, *col, 0);
    } else if (col != nullptr && col->type_name == "numeric" && col->numeric_constrained) {
        refuse_rounding(v.text, *col, col->numeric_scale);
    }
    auto plain = plain_decimal(v.text, kPlainChars);
    return plain.empty() ? v.text : plain;
}

std::string render_pg_row(const JsonMembers& m,
                          const std::vector<std::string>& columns,
                          const PgTable& table,
                          const sqljson::TimeColumns& times,
                          const sqljson::EscapeFn& esc) {
    std::string out = "(";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        const auto it = m.find(columns[i]);
        if (it == m.end()) {
            out += "NULL";
            continue;
        }
        try {
            out += render_pg_literal(it->second,
                                     table.column(columns[i]),
                                     sqljson::time_kind_of(times, columns[i]),
                                     esc);
        } catch (const RecordRefused& e) {
            throw RecordRefused("member \"" + columns[i] + "\" is " + e.what());
        }
    }
    out.push_back(')');
    return out;
}

std::string render_pg_key(const JsonMembers& m,
                          const std::vector<std::string>& key_columns,
                          const PgTable& table,
                          const sqljson::TimeColumns& times,
                          const sqljson::EscapeFn& esc) {
    std::string out = key_columns.size() > 1 ? "(" : "";
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        if (i > 0) {
            out.push_back(',');
        }
        const auto it = m.find(key_columns[i]);
        if (it == m.end() || it->second.kind == JsonMember::Kind::Null) {
            throw RecordRefused("its key member \"" + key_columns[i] +
                                "\" is missing or null, so it names no row");
        }
        try {
            out += render_pg_literal(it->second,
                                     table.column(key_columns[i]),
                                     sqljson::time_kind_of(times, key_columns[i]),
                                     esc);
        } catch (const RecordRefused& e) {
            throw RecordRefused("key member \"" + key_columns[i] + "\" is " + e.what());
        }
    }
    if (key_columns.size() > 1) {
        out.push_back(')');
    }
    return out;
}

void check_pg_key_spelling(const JsonMembers& m,
                           const std::vector<std::string>& key_columns,
                           const PgTable& table) {
    for (const auto& k : key_columns) {
        const auto it = m.find(k);
        const auto* c = table.column(k);
        if (it == m.end() || c == nullptr || it->second.kind == JsonMember::Kind::Null) {
            continue;  // stream_key refuses a missing or null key
        }
        const bool text = is_text_type(c->type_name);
        if (it->second.kind != (text ? JsonMember::Kind::String : JsonMember::Kind::Number)) {
            throw RecordRefused("its key member \"" + k + "\" is not a JSON " +
                                (text ? "string" : "number") + ", as column '" + k + "' (" +
                                c->type +
                                ") needs with more than one subtask: a key written two ways "
                                "can reach two subtasks, which write one row in either order");
        }
    }
}

// ---- statements -------------------------------------------------------------------------

namespace {

std::string quoted_list(const std::vector<std::string>& columns) {
    std::string out;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        out += (i > 0 ? "," : "") + pgsql::quote_ident(columns[i]);
    }
    return out;
}

}  // namespace

std::string pg_insert_head(const std::string& table, const std::vector<std::string>& columns) {
    return "INSERT INTO " + pgsql::quote_table_ident(table) + " (" + quoted_list(columns) +
           ") VALUES ";
}

std::string pg_update_tail(const std::vector<std::string>& conflict_columns,
                           const std::vector<std::string>& set_columns) {
    if (set_columns.empty()) {
        return pg_nothing_tail(conflict_columns);
    }
    std::string sql = " ON CONFLICT (" + quoted_list(conflict_columns) + ") DO UPDATE SET ";
    for (std::size_t i = 0; i < set_columns.size(); ++i) {
        const auto q = pgsql::quote_ident(set_columns[i]);
        sql += (i > 0 ? "," : "") + q + " = EXCLUDED." + q;
    }
    return sql;
}

std::string pg_nothing_tail(const std::vector<std::string>& conflict_columns) {
    return " ON CONFLICT (" + quoted_list(conflict_columns) + ") DO NOTHING";
}

std::string pg_delete_head(const std::string& table, const std::vector<std::string>& key_columns) {
    std::string sql = "DELETE FROM " + pgsql::quote_table_ident(table) + " WHERE ";
    if (key_columns.size() == 1) {
        return sql + pgsql::quote_ident(key_columns.front()) + " IN (";
    }
    return sql + "(" + quoted_list(key_columns) + ") IN (";
}

// ---- server messages ------------------------------------------------------------------

std::string redact_pg_message(std::string_view primary) {
    std::string out{primary};
    // value "300000" is out of range for type smallint
    for (std::size_t from = 0; (from = out.find("value \"", from)) != std::string::npos;) {
        const auto begin = from + 7;
        const auto end = out.find('"', begin);
        if (end == std::string::npos) {
            break;
        }
        out.replace(from + 6, end + 1 - (from + 6), "<value>");
        from += 13;
    }
    // invalid input syntax for type integer: "12abc"
    if (const auto at = out.rfind(": \"");
        at != std::string::npos && out.size() > at + 3 && out.back() == '"') {
        out.replace(at + 2, std::string::npos, "<value>");
    }
    return out;
}

}  // namespace clink::postgres
