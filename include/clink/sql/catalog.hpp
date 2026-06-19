#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/sql/ast.hpp"

#include "arrow/api.h"

// SQL catalog: table-name -> TableDef. Holds the schemas declared via
// CREATE TABLE plus the connector-binding properties (connector name,
// topic, bootstrap, path, etc.) the planner needs to lower a logical
// scan/sink down to a clink built-in.
//
// Phase 1 is in-memory only. Persistence to <ha-dir>/catalog/ comes in
// Phase 2.

namespace clink::sql {

struct ColumnSpec {
    std::string name;
    std::shared_ptr<arrow::DataType> type;
};

struct TableDef {
    std::string name;
    std::vector<ColumnSpec> columns;
    // Insertion-order preserving for deterministic EXPLAIN output and
    // round-tripping JSON catalogs in Phase 2.
    std::map<std::string, std::string> properties;

    // Phase 22a: primary key + sink mode.
    //
    // primary_key holds the column names that identify a row for
    // upsert sinks. Populated from a `PRIMARY KEY (col, ...)` column
    // constraint OR from the WITH-option `primary_key='col1,col2'`.
    // Both are accepted; the in-column form is canonical.
    std::vector<std::string> primary_key;

    // Returns properties.at("mode") if present, else "append".
    // Upsert sinks set mode='upsert' in their WITH-options.
    [[nodiscard]] std::string mode() const {
        auto it = properties.find("mode");
        if (it != properties.end())
            return it->second;
        return "append";
    }
    [[nodiscard]] bool is_upsert() const { return mode() == "upsert"; }

    // Phase 23a: sink delivery guarantee. Default 'at_least_once'.
    // 'exactly_once' enables a 2PC-aware sink op: the planner routes
    // to a transactional variant (file_2pc_sink_row,
    // kafka_2pc_sink_string, ...) and the runtime stages records per
    // checkpoint barrier + commits when the JM marks the checkpoint
    // globally durable.
    [[nodiscard]] std::string delivery_guarantee() const {
        auto it = properties.find("delivery_guarantee");
        if (it != properties.end())
            return it->second;
        return "at_least_once";
    }
    [[nodiscard]] bool is_exactly_once() const { return delivery_guarantee() == "exactly_once"; }

    // Phase 30a: cross-sink commit group. Multiple sinks that share a
    // non-empty commit_group must commit together or all abort
    // together. Defaults to "" (no group; sink commits independently).
    // Only meaningful for exactly_once sinks (a non-2PC sink has no
    // commit phase to coordinate); the binder rejects commit_group
    // on non-exactly_once tables in 30a.
    [[nodiscard]] std::string commit_group() const {
        auto it = properties.find("commit_group");
        if (it != properties.end())
            return it->second;
        return "";
    }
    [[nodiscard]] bool has_commit_group() const { return !commit_group().empty(); }

    // Lookup / enrichment table. `connector='lookup'` marks a table
    // that is not a stream to scan but a keyed point-lookup source:
    // the `function` property names a Row -> async::Task<Row> coroutine
    // registered in AsyncFunctionRegistry::global(). Such a table is
    // only valid as the right side of a JOIN (a lookup join enriches
    // the probe stream against it); using it as a plain FROM source is
    // a bind error.
    [[nodiscard]] bool is_lookup() const {
        auto it = properties.find("connector");
        return it != properties.end() && it->second == "lookup";
    }
    [[nodiscard]] std::string lookup_function() const {
        auto it = properties.find("function");
        if (it != properties.end())
            return it->second;
        return "";
    }

    // MATTBL: a materialized view is realised as an ordinary backing TableDef
    // tagged with these well-known string properties so the JSON catalog
    // round-trips them for free. `view_kind='materialized'` is the
    // discriminator; `definition_sql` is the original SELECT text (stored for
    // future restart-from-checkpoint re-binding, not used in v1); `freshness`
    // is the refresh budget ('0'/'continuous' in v1). The table is otherwise a
    // normal connector table: referencing queries scan it and the maintenance
    // INSERT writes it.
    [[nodiscard]] bool is_materialized_view() const {
        auto it = properties.find("view_kind");
        return it != properties.end() && it->second == "materialized";
    }
    [[nodiscard]] std::string definition_sql() const {
        auto it = properties.find("definition_sql");
        if (it != properties.end())
            return it->second;
        return "";
    }
    [[nodiscard]] std::string freshness() const {
        auto it = properties.find("freshness");
        if (it != properties.end())
            return it->second;
        return "";
    }
};

class Catalog {
public:
    Catalog() = default;

    // Register a TableDef. Throws TranslationError when a table with
    // the same name is already registered (no IF NOT EXISTS yet).
    // When a persistence dir is set, also writes <dir>/<name>.json.
    void register_table(TableDef def);

    // Convenience: translate a CREATE TABLE AST into a TableDef and
    // register it. Resolves column types through sql_type_to_arrow.
    // Throws TranslationError if a column type isn't supported in
    // Phase 1, or if the table is already registered.
    void register_table(const ast::CreateTableStmt& stmt);

    // Remove a table by name. Returns true if it existed. When a
    // persistence dir is set, removes <dir>/<name>.json too.
    bool drop_table(const std::string& name);

    // Lookup. Returns nullptr if not found.
    [[nodiscard]] const TableDef* get_table(const std::string& name) const;

    // List currently-registered table names in registration order.
    [[nodiscard]] std::vector<std::string> list_tables() const;

    // Persistence (Phase 2):
    //
    //   set_persistence_dir(dir) - subsequent register / drop calls
    //   auto-persist to <dir>/<table>.json. The directory is created
    //   on demand. Pass empty string to clear (turns off persistence).
    //
    //   load_from_dir(dir) - replaces in-memory state with every
    //   *.json under <dir>. Idempotent. Does NOT set the persistence
    //   dir as a side effect - call set_persistence_dir separately if
    //   you also want subsequent mutations to auto-save.
    //
    // Both throw std::runtime_error on filesystem or parse errors.
    void set_persistence_dir(std::string dir);
    [[nodiscard]] const std::string& persistence_dir() const noexcept { return persistence_dir_; }
    void load_from_dir(const std::string& dir);

    // Round-trip a single table's JSON form. Useful for tests and
    // for the HTTP catalog API in Phase 2.3.
    [[nodiscard]] static std::string to_json(const TableDef& def);
    [[nodiscard]] static TableDef from_json(const std::string& text);

private:
    std::unordered_map<std::string, TableDef> tables_;
    std::vector<std::string> order_;  // registration order, for list_tables
    std::string persistence_dir_;
};

}  // namespace clink::sql
