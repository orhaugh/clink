#pragma once

// Postgres-dialect SQL-string construction for the JSON sink. The identifier
// quoting, JSON-value mapping, schema-column parsing and INSERT head + VALUES list
// are shared with the MySQL sink in clink::sqljson (M6); this header keeps the
// public clink::pgsql:: surface (thin delegations) plus the Postgres-specific
// ON CONFLICT clause. Pure (escaping injected as a callback) so it unit-tests
// without a server.

#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/sql_json_builder.hpp"

namespace clink::pgsql {

using EscapeFn = sqljson::EscapeFn;

// Double-quote + validate an identifier (table / column).
inline std::string quote_ident(std::string_view name) {
    return sqljson::quote_ident(name, sqljson::kPostgres);
}

// Table position only: allows one schema qualifier ("public"."users").
inline std::string quote_table_ident(std::string_view name) {
    return sqljson::quote_table_ident(name, sqljson::kPostgres);
}

// Column names from the SQL Row path's schema_columns param.
inline std::vector<std::string> columns_from_schema(const std::string& schema) {
    return sqljson::columns_from_schema(schema);
}

// Build a DELETE ... WHERE <pk> IN (...) for a batch of changelog-delete rows
// (the changelog-aware upsert sink applies tombstones by primary key).
inline std::string build_delete_sql(const std::string& table,
                                    const std::vector<std::string>& key_columns,
                                    const std::vector<std::string>& json_rows,
                                    const EscapeFn& esc) {
    return sqljson::build_delete_by_keys_sql(
        table, key_columns, json_rows, esc, sqljson::kPostgres);
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

// Rows that share a conflict key, merged into one row per key, in the order
// each key first appears. PostgreSQL refuses an ON CONFLICT DO UPDATE whose
// rows touch one key twice ("command cannot affect row a second time"), and a
// stream updates a key more than once in an interval as a matter of course, so
// the flush failed, and its replay failed the same way, for good. The merged
// row is what applying the rows one at a time leaves: the first row's values,
// with each SET column taken from the last row (a column a later row leaves out
// becomes NULL, as EXCLUDED would make it). A row that is not a JSON object is
// left as it is for build_insert_head_values to refuse.
inline std::vector<std::string> merge_rows_by_key(const std::vector<std::string>& json_rows,
                                                  const std::vector<std::string>& conflict_columns,
                                                  const std::vector<std::string>& set_cols) {
    // One entry per output row: a row passed through as it came, or a merged one.
    struct Entry {
        bool merged{false};
        std::string raw;
        std::size_t slot{0};
    };
    std::vector<Entry> entries;
    std::vector<config::JsonObject> merged;
    std::map<std::string, std::size_t> slot_of_key;
    for (const auto& row : json_rows) {
        config::JsonValue v;
        try {
            v = config::parse(row);
        } catch (const std::exception&) {
            entries.push_back(Entry{.raw = row});
            continue;
        }
        if (!v.is_object()) {
            entries.push_back(Entry{.raw = row});
            continue;
        }
        const auto& obj = v.as_object();
        std::string key;
        for (const auto& c : conflict_columns) {
            const auto it = obj.find(c);
            if (it == obj.end()) {
                key += "null";
            } else {
                it->second.serialize_into(key);
            }
            key += '\x1f';
        }
        const auto [at, fresh] = slot_of_key.try_emplace(key, merged.size());
        if (fresh) {
            merged.push_back(obj);
            entries.push_back(Entry{.merged = true, .slot = at->second});
            continue;
        }
        auto& into = merged[at->second];
        for (const auto& c : set_cols) {
            const auto it = obj.find(c);
            into[c] = it == obj.end() ? config::JsonValue{} : it->second;
        }
    }
    std::vector<std::string> out;
    out.reserve(entries.size());
    for (auto& e : entries) {
        out.push_back(e.merged ? config::JsonValue{merged[e.slot]}.serialize(0) : std::move(e.raw));
    }
    return out;
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

    // The SET list for "update": the requested columns, defaulting to all
    // non-conflict columns.
    std::vector<std::string> set_cols;
    if (on_conflict == "update") {
        if (!update_columns.empty()) {
            set_cols = update_columns;
        } else {
            for (const auto& c : columns) {
                if (!in_set(conflict_columns, c)) {
                    set_cols.push_back(c);
                }
            }
        }
    }
    std::string sql = sqljson::build_insert_head_values(
        table,
        columns,
        on_conflict == "update" && !set_cols.empty()
            ? merge_rows_by_key(json_rows, conflict_columns, set_cols)
            : json_rows,
        esc,
        sqljson::kPostgres);
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
