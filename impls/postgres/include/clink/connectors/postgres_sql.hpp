#pragma once

// Pure SQL-string construction for the Postgres JSON sink (M4): identifier
// quoting, JSON value -> SQL literal mapping, and the batched INSERT / ON CONFLICT
// builder. Free of any libpq dependency (escaping is injected as a callback) so
// it is unit-testable WITHOUT a live server. The Postgres analogue of
// impls/mysql/mysql_sql.hpp (double-quote idents, TRUE/FALSE bools, ON CONFLICT
// instead of MySQL's backticks / 1-0 bools / ON DUPLICATE KEY UPDATE).

#include <cstdint>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::pgsql {

// Escapes a string VALUE for inclusion inside single quotes (the live path uses
// libpq PQescapeStringConn; tests pass a deterministic stub).
using EscapeFn = std::function<std::string(std::string_view)>;

// Double-quote an identifier (table / column), validating it is a plain
// identifier. Rejects anything outside [A-Za-z0-9_$] (incl. double-quote, NUL,
// dot, space) so a user-supplied WITH-option name cannot break out of quoting.
inline std::string quote_ident(std::string_view name) {
    if (name.empty()) {
        throw std::runtime_error("postgres: empty identifier");
    }
    for (char c : name) {
        const auto uc = static_cast<unsigned char>(c);
        const bool ok = (uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z') ||
                        (uc >= '0' && uc <= '9') || c == '_' || c == '$';
        if (!ok) {
            throw std::runtime_error("postgres: invalid identifier '" + std::string(name) +
                                     "' (only [A-Za-z0-9_$] allowed)");
        }
    }
    return "\"" + std::string(name) + "\"";
}

// Extract column names from a serialize_row_schema string
// ("name:typecode;name:typecode") - the schema the SQL Row path injects as the
// schema_columns param. Lets the sink use the declared table schema as its
// projection when an explicit columns= is not supplied.
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

// Map a JSON value to a SQL literal. null -> NULL; bool -> TRUE/FALSE; integral
// number -> integer text; other number -> %.17g; string -> escaped + quoted;
// object/array -> their JSON text, escaped + quoted.
inline std::string json_value_to_sql_literal(const clink::config::JsonValue& v,
                                             const EscapeFn& esc) {
    if (v.is_null()) {
        return "NULL";
    }
    if (v.is_bool()) {
        return v.as_bool() ? "TRUE" : "FALSE";
    }
    if (v.is_number()) {
        const double d = v.as_number();
        constexpr double kInt64Lo = -9223372036854775808.0;
        constexpr double kInt64HiExclusive = 9223372036854775808.0;
        if (d >= kInt64Lo && d < kInt64HiExclusive &&
            d == static_cast<double>(static_cast<std::int64_t>(d))) {
            return std::to_string(static_cast<std::int64_t>(d));
        }
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.17g", d);
        return std::string{buf};
    }
    if (v.is_string()) {
        return "'" + esc(v.as_string()) + "'";
    }
    return "'" + esc(v.serialize(0)) + "'";  // object/array: store the JSON text
}

// Whether a column is in the conflict-target set.
inline bool in_set(const std::vector<std::string>& set, const std::string& c) {
    for (const auto& x : set) {
        if (x == c) {
            return true;
        }
    }
    return false;
}

// Build one multi-row INSERT for a batch of JSON-object rows. `columns` is the
// authoritative projection; a column absent from (or null in) a row becomes SQL
// NULL. on_conflict: "" (plain INSERT), "update" (ON CONFLICT (...) DO UPDATE),
// or "nothing" (ON CONFLICT (...) DO NOTHING). When non-empty, conflict_columns
// (the unique/PK target) is required. For "update", update_columns limits the SET
// list (empty = all columns except the conflict target); a degenerate empty SET
// collapses to DO NOTHING. Throws on a row that is not a JSON object.
inline std::string build_insert_sql(const std::string& table,
                                    const std::vector<std::string>& columns,
                                    const std::string& on_conflict,
                                    const std::vector<std::string>& conflict_columns,
                                    const std::vector<std::string>& update_columns,
                                    const std::vector<std::string>& json_rows,
                                    const EscapeFn& esc) {
    if (columns.empty()) {
        throw std::runtime_error("postgres: 'columns' is required for the sink");
    }
    const bool has_conflict = on_conflict == "update" || on_conflict == "nothing";
    if (!on_conflict.empty() && !has_conflict) {
        throw std::runtime_error("postgres: on_conflict must be 'update' or 'nothing' (got '" +
                                 on_conflict + "')");
    }
    if (has_conflict && conflict_columns.empty()) {
        throw std::runtime_error(
            "postgres: on_conflict requires conflict_columns (the unique/primary-key target)");
    }

    std::string sql = "INSERT INTO " + quote_ident(table) + " (";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        sql += quote_ident(columns[i]);
        if (i + 1 < columns.size()) {
            sql += ",";
        }
    }
    sql += ") VALUES ";
    for (std::size_t r = 0; r < json_rows.size(); ++r) {
        const auto j = clink::config::parse(json_rows[r]);
        if (!j.is_object()) {
            // A well-formed non-object JSON (e.g. "5", "[1,2]") would otherwise map
            // every column to NULL silently. Fail loudly - a row to a multi-column
            // sink must be a JSON object. (Unreachable on the SQL Row path, where
            // row_to_json_string always emits an object.)
            throw std::runtime_error("postgres: sink row is not a JSON object: " + json_rows[r]);
        }
        sql += "(";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            const clink::config::JsonValue* val = nullptr;
            const auto& obj = j.as_object();
            auto it = obj.find(columns[i]);
            if (it != obj.end()) {
                val = &it->second;
            }
            sql += (val != nullptr) ? json_value_to_sql_literal(*val, esc) : "NULL";
            if (i + 1 < columns.size()) {
                sql += ",";
            }
        }
        sql += ")";
        if (r + 1 < json_rows.size()) {
            sql += ",";
        }
    }

    if (!has_conflict) {
        return sql;
    }
    sql += " ON CONFLICT (";
    for (std::size_t i = 0; i < conflict_columns.size(); ++i) {
        sql += quote_ident(conflict_columns[i]);
        if (i + 1 < conflict_columns.size()) {
            sql += ",";
        }
    }
    sql += ")";
    if (on_conflict == "nothing") {
        sql += " DO NOTHING";
        return sql;
    }
    // "update": SET the requested columns, defaulting to all non-conflict columns.
    std::vector<std::string> set_cols;
    if (!update_columns.empty()) {
        set_cols = update_columns;
    } else {
        for (const auto& c : columns) {
            if (!in_set(conflict_columns, c)) {
                set_cols.push_back(c);
            }
        }
    }
    if (set_cols.empty()) {
        sql += " DO NOTHING";  // nothing left to update (all columns are the key)
        return sql;
    }
    sql += " DO UPDATE SET ";
    for (std::size_t i = 0; i < set_cols.size(); ++i) {
        const std::string q = quote_ident(set_cols[i]);
        sql += q + "=EXCLUDED." + q;
        if (i + 1 < set_cols.size()) {
            sql += ",";
        }
    }
    return sql;
}

}  // namespace clink::pgsql
