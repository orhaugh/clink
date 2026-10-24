#pragma once

// Shared, dialect-parameterised SQL-string construction for the JSON-row JDBC
// sinks (mysql_sink, postgres_sink). M6 of the connector-pattern migration: the
// identifier quoting, JSON-value -> SQL-literal mapping, schema-column parsing and
// the INSERT head + multi-row VALUES list were duplicated across mysql_sql.hpp and
// postgres_sql.hpp. They are identical except for three dialect knobs (identifier
// quote char, boolean literal spelling) so they live here once; each connector
// keeps only its dialect-specific ON-DUPLICATE / ON-CONFLICT clause and its own
// SELECT builder. Pure (no client-lib dependency: escaping is injected as a
// callback) so it unit-tests without a server.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::sqljson {

// Escapes a string VALUE for inclusion inside the dialect's string quotes. The
// live path injects the client lib's charset-correct escaper; tests pass a stub.
using EscapeFn = std::function<std::string(std::string_view)>;

// The handful of lexical differences between the supported SQL dialects.
struct Dialect {
    char ident_quote;        // `  (MySQL)  vs  "  (Postgres / standard SQL)
    const char* bool_true;   // "1" (MySQL) vs "TRUE" (Postgres)
    const char* bool_false;  // "0"         vs "FALSE"
    const char* name;        // for error messages
};

inline constexpr Dialect kMysql{'`', "1", "0", "mysql"};
inline constexpr Dialect kPostgres{'"', "TRUE", "FALSE", "postgres"};

// Quote an identifier (table / column), validating it is a plain identifier.
// Rejects anything outside [A-Za-z0-9_$] (incl. the quote char itself, NUL, dot,
// space) so a user-supplied WITH-option name cannot break out of the quoting.
inline std::string quote_ident(std::string_view name, const Dialect& d) {
    if (name.empty()) {
        throw std::runtime_error(std::string(d.name) + ": empty identifier");
    }
    for (char c : name) {
        const auto uc = static_cast<unsigned char>(c);
        const bool ok = (uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z') ||
                        (uc >= '0' && uc <= '9') || c == '_' || c == '$';
        if (!ok) {
            throw std::runtime_error(std::string(d.name) + ": invalid identifier '" +
                                     std::string(name) + "' (only [A-Za-z0-9_$] allowed)");
        }
    }
    std::string out;
    out.reserve(name.size() + 2);
    out.push_back(d.ident_quote);
    out.append(name);
    out.push_back(d.ident_quote);
    return out;
}

// Quote a TABLE identifier, which - unlike a column - may be
// schema-qualified: "schema.table" quotes as "schema"."table", each part
// validated by quote_ident so the dot is a separator and never part of a
// name. At most one dot: catalog-qualified three-part names are not a
// thing either dialect's sink supports, and rejecting them here beats a
// server error mid-checkpoint. The connector docs have always shown
// 'public.users' for the Postgres sinks; the plain quote_ident rejected
// the dot and the documented form failed at open() - caught by QUAL-02's
// local rig on its first deployed pipeline.
inline std::string quote_table_ident(std::string_view name, const Dialect& d) {
    const auto dot = name.find('.');
    if (dot == std::string_view::npos) {
        return quote_ident(name, d);
    }
    const auto schema = name.substr(0, dot);
    const auto table = name.substr(dot + 1);
    if (table.find('.') != std::string_view::npos) {
        throw std::runtime_error(std::string(d.name) + ": invalid table identifier '" +
                                 std::string(name) + "' (at most one '.', schema.table)");
    }
    return quote_ident(schema, d) + "." + quote_ident(table, d);
}

// Extract column names from a serialize_row_schema string
// ("name:typecode;name:typecode") - the schema the SQL Row path injects as the
// schema_columns param. Dialect-independent.
inline std::vector<std::string> columns_from_schema(const std::string& schema) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < schema.size()) {
        std::size_t semi = schema.find(';', i);
        if (semi == std::string::npos) {
            semi = schema.size();
        }
        const std::string entry = schema.substr(i, semi - i);
        const std::size_t colon = entry.find(':');
        const std::string name = colon == std::string::npos ? entry : entry.substr(0, colon);
        if (!name.empty()) {
            out.push_back(name);
        }
        i = semi + 1;
    }
    return out;
}

// Which columns hold a TIMESTAMP, and whether it has a time zone. The SQL Row
// path writes a TIMESTAMP into the JSON row as epoch milliseconds, and a
// timestamp column refuses an integer, so a column named here is written as a
// timestamp literal instead. The planner names them in the sink's
// timestamp_columns param; parse_timestamp_columns reads it.
enum class TimeKind : std::uint8_t { None, Timestamp, TimestampTz };
using TimeColumns = std::map<std::string, TimeKind, std::less<>>;

// "name:ts,name:tstz" -> column -> kind. An entry of another kind is refused,
// as a misspelt one would otherwise write that column as a number again.
inline TimeColumns parse_timestamp_columns(std::string_view csv) {
    TimeColumns out;
    std::size_t i = 0;
    while (i < csv.size()) {
        auto comma = csv.find(',', i);
        if (comma == std::string_view::npos) {
            comma = csv.size();
        }
        const auto entry = csv.substr(i, comma - i);
        i = comma + 1;
        if (entry.empty()) {
            continue;
        }
        const auto colon = entry.rfind(':');
        const auto kind =
            colon == std::string_view::npos ? std::string_view{} : entry.substr(colon + 1);
        if (colon == std::string_view::npos || colon == 0 || (kind != "ts" && kind != "tstz")) {
            throw std::runtime_error("timestamp_columns: entry '" + std::string(entry) +
                                     "' is not <column>:ts or <column>:tstz");
        }
        out[std::string(entry.substr(0, colon))] =
            kind == "ts" ? TimeKind::Timestamp : TimeKind::TimestampTz;
    }
    return out;
}

// Epoch milliseconds as a timestamp literal: 'YYYY-MM-DD HH:MM:SS.mmm', the
// instant in UTC. A TIMESTAMP WITH TIME ZONE says so to PostgreSQL with +00, so
// a timestamptz column stores that instant whatever the session's time zone;
// MySQL has no zone-aware literal its columns all honour, and the MySQL sinks
// run their session in UTC instead. A year outside 1 to 9999 is refused.
inline std::string epoch_ms_to_sql_timestamp(std::int64_t ms, TimeKind kind, const Dialect& d) {
    using namespace std::chrono;
    const sys_time<milliseconds> tp{milliseconds{ms}};
    const auto day = floor<days>(tp);
    const year_month_day ymd{day};
    const hh_mm_ss<milliseconds> t{tp - day};
    const int year = static_cast<int>(ymd.year());
    if (year < 1 || year > 9999) {
        throw std::runtime_error(std::string(d.name) + ": a TIMESTAMP of " + std::to_string(ms) +
                                 " ms since 1970 is outside years 1 to 9999");
    }
    char buf[48];
    std::snprintf(buf,
                  sizeof(buf),
                  "'%04d-%02u-%02u %02lld:%02lld:%02lld.%03lld%s'",
                  year,
                  static_cast<unsigned>(ymd.month()),
                  static_cast<unsigned>(ymd.day()),
                  static_cast<long long>(t.hours().count()),
                  static_cast<long long>(t.minutes().count()),
                  static_cast<long long>(t.seconds().count()),
                  static_cast<long long>(t.subseconds().count()),
                  kind == TimeKind::TimestampTz && d.ident_quote == '"' ? "+00" : "");
    return std::string{buf};
}

// Map a JSON value to a SQL literal. null -> NULL; bool -> dialect true/false;
// integer -> its exact text, or a timestamp literal in a TIMESTAMP column;
// other number -> %.17g; string -> escaped + single-quoted; object/array ->
// their JSON text, escaped + quoted.
inline std::string json_value_to_sql_literal(const clink::config::JsonValue& v,
                                             const EscapeFn& esc,
                                             const Dialect& d,
                                             TimeKind time = TimeKind::None) {
    if (v.is_null()) {
        return "NULL";
    }
    if (v.is_bool()) {
        return v.as_bool() ? d.bool_true : d.bool_false;
    }
    if (time != TimeKind::None && v.is_integral_number()) {
        return epoch_ms_to_sql_timestamp(v.as_int(), time, d);
    }
    if (v.is_integral_number()) {
        // Read exactly: as_number() widens to a double, which holds integers
        // only up to 2^53 and would round a larger one to a neighbouring value.
        return std::to_string(v.as_int());
    }
    if (v.is_number()) {
        const double n = v.as_number();
        constexpr double kInt64Lo = -9223372036854775808.0;
        constexpr double kInt64HiExclusive = 9223372036854775808.0;
        if (n >= kInt64Lo && n < kInt64HiExclusive &&
            n == static_cast<double>(static_cast<std::int64_t>(n))) {
            return std::to_string(static_cast<std::int64_t>(n));
        }
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.17g", n);
        return std::string{buf};
    }
    if (v.is_string()) {
        return "'" + esc(v.as_string()) + "'";
    }
    return "'" + esc(v.serialize(0)) + "'";  // object/array: store the JSON text
}

// Build the INSERT head + multi-row VALUES list common to both dialects:
//   INSERT INTO <table> (<c1>,<c2>,...) VALUES (r1...),(r2...)
// `columns` is the authoritative projection; a column absent from (or null in) a
// row becomes SQL NULL. Throws on empty columns or a row that is not a JSON
// object (fail loud rather than silently NULL every column). The dialect-specific
// conflict clause is appended by the caller.
inline TimeKind time_kind_of(const TimeColumns& times, const std::string& column) {
    const auto it = times.find(column);
    return it == times.end() ? TimeKind::None : it->second;
}

inline std::string build_insert_head_values(const std::string& table,
                                            const std::vector<std::string>& columns,
                                            const std::vector<std::string>& json_rows,
                                            const EscapeFn& esc,
                                            const Dialect& d,
                                            const TimeColumns& times = {}) {
    if (columns.empty()) {
        throw std::runtime_error(std::string(d.name) + ": 'columns' is required for the sink");
    }
    std::string sql = "INSERT INTO " + quote_table_ident(table, d) + " (";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        sql += quote_ident(columns[i], d);
        if (i + 1 < columns.size()) {
            sql += ",";
        }
    }
    sql += ") VALUES ";
    for (std::size_t r = 0; r < json_rows.size(); ++r) {
        const auto j = clink::config::parse(json_rows[r]);
        if (!j.is_object()) {
            throw std::runtime_error(std::string(d.name) +
                                     ": sink row is not a JSON object: " + json_rows[r]);
        }
        const auto& obj = j.as_object();
        sql += "(";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            auto it = obj.find(columns[i]);
            sql += (it != obj.end()) ? json_value_to_sql_literal(
                                           it->second, esc, d, time_kind_of(times, columns[i]))
                                     : "NULL";
            if (i + 1 < columns.size()) {
                sql += ",";
            }
        }
        sql += ")";
        if (r + 1 < json_rows.size()) {
            sql += ",";
        }
    }
    return sql;
}

// Build a DELETE that removes the rows matching a batch of changelog-delete keys:
//   single key column:  DELETE FROM <t> WHERE <k> IN (v1, v2, ...)
//   composite key:      DELETE FROM <t> WHERE (<k1>,<k2>) IN ((a,b), (c,d), ...)
// `key_columns` is the primary key; each row must carry a non-null value for every
// key column (a changelog delete without its full key is a bug - fail loud). Used
// by the changelog-aware upsert sinks to apply delete/update_before tombstones.
inline std::string build_delete_by_keys_sql(const std::string& table,
                                            const std::vector<std::string>& key_columns,
                                            const std::vector<std::string>& json_rows,
                                            const EscapeFn& esc,
                                            const Dialect& d,
                                            const TimeColumns& times = {}) {
    if (key_columns.empty()) {
        throw std::runtime_error(std::string(d.name) + ": delete requires key_columns");
    }
    const bool composite = key_columns.size() > 1;
    std::string sql = "DELETE FROM " + quote_table_ident(table, d) + " WHERE ";
    if (composite) {
        sql += "(";
        for (std::size_t i = 0; i < key_columns.size(); ++i) {
            sql += quote_ident(key_columns[i], d);
            if (i + 1 < key_columns.size()) {
                sql += ",";
            }
        }
        sql += ") IN (";
    } else {
        sql += quote_ident(key_columns[0], d) + " IN (";
    }
    for (std::size_t r = 0; r < json_rows.size(); ++r) {
        const auto j = clink::config::parse(json_rows[r]);
        if (!j.is_object()) {
            throw std::runtime_error(std::string(d.name) +
                                     ": delete row is not a JSON object: " + json_rows[r]);
        }
        const auto& obj = j.as_object();
        if (composite) {
            sql += "(";
        }
        for (std::size_t i = 0; i < key_columns.size(); ++i) {
            auto it = obj.find(key_columns[i]);
            if (it == obj.end() || it->second.is_null()) {
                throw std::runtime_error(std::string(d.name) +
                                         ": changelog delete row missing key '" + key_columns[i] +
                                         "': " + json_rows[r]);
            }
            sql +=
                json_value_to_sql_literal(it->second, esc, d, time_kind_of(times, key_columns[i]));
            if (i + 1 < key_columns.size()) {
                sql += ",";
            }
        }
        if (composite) {
            sql += ")";
        }
        if (r + 1 < json_rows.size()) {
            sql += ",";
        }
    }
    sql += ")";
    return sql;
}

}  // namespace clink::sqljson
