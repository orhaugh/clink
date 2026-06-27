#pragma once

// MySQL-dialect SQL-string construction for the connector. The identifier
// quoting, JSON-value mapping, schema-column parsing and INSERT head + VALUES list
// are shared with the Postgres sink in clink::sqljson (M6); this header keeps the
// public clink::mysql:: surface (thin delegations) plus the MySQL-specific upsert
// clause (ON DUPLICATE KEY UPDATE) and the incremental cursor SELECT builder.
// Pure (escaping injected as a callback) so it unit-tests without a server.

#include <string>
#include <string_view>
#include <vector>

#include "clink/connectors/sql_json_builder.hpp"

namespace clink::mysql {

using EscapeFn = sqljson::EscapeFn;

// Backtick-quote + validate an identifier (table / column).
inline std::string quote_ident(std::string_view name) {
    return sqljson::quote_ident(name, sqljson::kMysql);
}

// Column names from the SQL Row path's schema_columns param.
inline std::vector<std::string> columns_from_schema(const std::string& schema) {
    return sqljson::columns_from_schema(schema);
}

// Build one multi-row INSERT for a batch of JSON-object rows. `columns` is the
// authoritative projection; a column absent from (or null in) a row becomes SQL
// NULL. `upsert` appends ON DUPLICATE KEY UPDATE (MySQL 8.0.19+ row-alias form);
// `update_columns` limits the SET list (empty = all columns). Throws on empty
// columns or a row that is not a JSON object.
inline std::string build_insert_sql(const std::string& table,
                                    const std::vector<std::string>& columns,
                                    bool upsert,
                                    const std::vector<std::string>& update_columns,
                                    const std::vector<std::string>& json_rows,
                                    const EscapeFn& esc) {
    std::string sql =
        sqljson::build_insert_head_values(table, columns, json_rows, esc, sqljson::kMysql);
    if (upsert) {
        sql += " AS clink_new ON DUPLICATE KEY UPDATE ";
        const auto& set_cols = update_columns.empty() ? columns : update_columns;
        for (std::size_t i = 0; i < set_cols.size(); ++i) {
            const std::string q = quote_ident(set_cols[i]);
            sql += q + "=clink_new." + q;
            if (i + 1 < set_cols.size()) {
                sql += ",";
            }
        }
    }
    return sql;
}

// Separator joining the two halves of a composite (cursor, id) checkpoint string.
// A non-printing control byte that never appears in a real key value.
inline constexpr char kCompositeCursorSep = '\x1f';

// Build the incremental cursor SELECT: every column (SELECT *) so the downstream
// json_string_to_row bridge can pick what it needs by name, rows strictly AFTER
// the cursor (exclusive, so the boundary row is never re-emitted), ordered
// ascending, capped at batch_size. An empty cursor omits the WHERE (cold start
// reads from the beginning).
//
// When `id_column` is non-empty it is a UNIQUE tie-breaker and the SELECT uses
// keyset pagination over the (cursor_column, id_column) tuple - WHERE
// (cursor_column, id_column) > (cv, idv) ORDER BY both. This is the robust form:
// rows sharing a cursor_column value are no longer dropped at a page boundary
// (the data-loss hazard of a non-unique cursor). `cursor` is then the composite
// "cv<sep>idv". With `id_column` empty the cursor_column itself must be unique.
inline std::string build_select_sql(const std::string& table,
                                    const std::string& cursor_column,
                                    const std::string& id_column,
                                    const std::string& cursor,
                                    int batch_size,
                                    const EscapeFn& esc) {
    std::string sql = "SELECT * FROM " + quote_ident(table);
    const std::string qc = quote_ident(cursor_column);
    if (id_column.empty()) {
        if (!cursor.empty()) {
            sql += " WHERE " + qc + " > '" + esc(cursor) + "'";
        }
        sql += " ORDER BY " + qc + " ASC LIMIT " + std::to_string(batch_size);
        return sql;
    }
    const std::string qi = quote_ident(id_column);
    if (!cursor.empty()) {
        std::string cv = cursor;
        std::string idv;
        if (const auto sep = cursor.find(kCompositeCursorSep); sep != std::string::npos) {
            cv = cursor.substr(0, sep);
            idv = cursor.substr(sep + 1);
        }
        sql += " WHERE (" + qc + "," + qi + ") > ('" + esc(cv) + "','" + esc(idv) + "')";
    }
    sql += " ORDER BY " + qc + " ASC," + qi + " ASC LIMIT " + std::to_string(batch_size);
    return sql;
}

}  // namespace clink::mysql
