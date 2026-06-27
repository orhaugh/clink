#pragma once

// Postgres-dialect SQL-string construction for the JSON sink. The identifier
// quoting, JSON-value mapping, schema-column parsing and INSERT head + VALUES list
// are shared with the MySQL sink in clink::sqljson (M6); this header keeps the
// public clink::pgsql:: surface (thin delegations) plus the Postgres-specific
// ON CONFLICT clause. Pure (escaping injected as a callback) so it unit-tests
// without a server.

#include <string>
#include <string_view>
#include <vector>

#include "clink/connectors/sql_json_builder.hpp"

namespace clink::pgsql {

using EscapeFn = sqljson::EscapeFn;

// Double-quote + validate an identifier (table / column).
inline std::string quote_ident(std::string_view name) {
    return sqljson::quote_ident(name, sqljson::kPostgres);
}

// Column names from the SQL Row path's schema_columns param.
inline std::vector<std::string> columns_from_schema(const std::string& schema) {
    return sqljson::columns_from_schema(schema);
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

// Build one multi-row INSERT for a batch of JSON-object rows. on_conflict: ""
// (plain INSERT), "update" (ON CONFLICT (...) DO UPDATE), or "nothing" (ON
// CONFLICT (...) DO NOTHING). When non-empty, conflict_columns (the unique/PK
// target) is required. For "update", update_columns limits the SET list (empty =
// all columns except the conflict target); a degenerate empty SET collapses to DO
// NOTHING. Throws on empty columns, an invalid on_conflict, a missing target, or
// a row that is not a JSON object.
inline std::string build_insert_sql(const std::string& table,
                                    const std::vector<std::string>& columns,
                                    const std::string& on_conflict,
                                    const std::vector<std::string>& conflict_columns,
                                    const std::vector<std::string>& update_columns,
                                    const std::vector<std::string>& json_rows,
                                    const EscapeFn& esc) {
    const bool has_conflict = on_conflict == "update" || on_conflict == "nothing";
    if (!on_conflict.empty() && !has_conflict) {
        throw std::runtime_error("postgres: on_conflict must be 'update' or 'nothing' (got '" +
                                 on_conflict + "')");
    }
    if (has_conflict && conflict_columns.empty()) {
        throw std::runtime_error(
            "postgres: on_conflict requires conflict_columns (the unique/primary-key target)");
    }

    std::string sql =
        sqljson::build_insert_head_values(table, columns, json_rows, esc, sqljson::kPostgres);
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
