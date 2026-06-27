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

#include <cstdint>
#include <cstdio>
#include <functional>
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

// Map a JSON value to a SQL literal. null -> NULL; bool -> dialect true/false;
// integral number -> integer text; other number -> %.17g; string -> escaped +
// single-quoted; object/array -> their JSON text, escaped + quoted.
inline std::string json_value_to_sql_literal(const clink::config::JsonValue& v,
                                             const EscapeFn& esc,
                                             const Dialect& d) {
    if (v.is_null()) {
        return "NULL";
    }
    if (v.is_bool()) {
        return v.as_bool() ? d.bool_true : d.bool_false;
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
inline std::string build_insert_head_values(const std::string& table,
                                            const std::vector<std::string>& columns,
                                            const std::vector<std::string>& json_rows,
                                            const EscapeFn& esc,
                                            const Dialect& d) {
    if (columns.empty()) {
        throw std::runtime_error(std::string(d.name) + ": 'columns' is required for the sink");
    }
    std::string sql = "INSERT INTO " + quote_ident(table, d) + " (";
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
            sql += (it != obj.end()) ? json_value_to_sql_literal(it->second, esc, d) : "NULL";
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

}  // namespace clink::sqljson
