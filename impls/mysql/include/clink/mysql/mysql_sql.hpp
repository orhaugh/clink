#pragma once

// Pure SQL-string construction for the MySQL connector: identifier quoting, JSON
// value -> SQL literal mapping, and the batched INSERT / cursor SELECT builders.
// Deliberately free of any mariadb dependency (escaping is injected as a
// callback) so it is unit-testable WITHOUT a live server.

#include <cstdint>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::mysql {

// Escapes a string VALUE for inclusion inside single quotes. Injected so the live
// path uses mariadb's charset-correct mysql_real_escape_string while tests pass a
// deterministic stub.
using EscapeFn = std::function<std::string(std::string_view)>;

// Backtick-quote an identifier (table / column), validating it is a plain
// identifier. Rejects anything outside [A-Za-z0-9_$] (incl. backtick, NUL, dot,
// space) so a user-supplied WITH-option name cannot break out of the quoting.
inline std::string quote_ident(std::string_view name) {
    if (name.empty()) {
        throw std::runtime_error("mysql: empty identifier");
    }
    for (char c : name) {
        const auto uc = static_cast<unsigned char>(c);
        const bool ok = (uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z') ||
                        (uc >= '0' && uc <= '9') || c == '_' || c == '$';
        if (!ok) {
            throw std::runtime_error("mysql: invalid identifier '" + std::string(name) +
                                     "' (only [A-Za-z0-9_$] allowed; use database= for schemas)");
        }
    }
    return "`" + std::string(name) + "`";
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

// Map a JSON value to a SQL literal. null -> NULL; bool -> 1/0; integral number ->
// integer text; other number -> %.17g (round-trippable double); string -> escaped
// + quoted; object/array -> their JSON text, escaped + quoted.
inline std::string json_value_to_sql_literal(const clink::config::JsonValue& v,
                                             const EscapeFn& esc) {
    if (v.is_null()) {
        return "NULL";
    }
    if (v.is_bool()) {
        return v.as_bool() ? "1" : "0";
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

// Build one multi-row INSERT for a batch of JSON-object rows. `columns` is the
// authoritative projection + ordering; a column absent from (or null in) a row
// becomes SQL NULL. `upsert` appends INSERT ... ON DUPLICATE KEY UPDATE (MySQL
// 8.0.19+ row-alias form); `update_columns` limits the SET list (empty = all
// columns). Throws (via parse) on a row that is not a JSON object.
inline std::string build_insert_sql(const std::string& table,
                                    const std::vector<std::string>& columns,
                                    bool upsert,
                                    const std::vector<std::string>& update_columns,
                                    const std::vector<std::string>& json_rows,
                                    const EscapeFn& esc) {
    if (columns.empty()) {
        throw std::runtime_error("mysql: 'columns' is required for the sink");
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
        const bool is_obj = j.is_object();
        sql += "(";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            const clink::config::JsonValue* val = nullptr;
            if (is_obj) {
                const auto& obj = j.as_object();
                auto it = obj.find(columns[i]);
                if (it != obj.end()) {
                    val = &it->second;
                }
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
