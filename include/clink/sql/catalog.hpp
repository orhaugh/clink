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
// In-memory only for now. Persistence to <ha-dir>/catalog/ comes later.

namespace clink::sql {

struct ColumnSpec {
    std::string name;
    std::shared_ptr<arrow::DataType> type;
};

struct TableDef {
    std::string name;
    std::vector<ColumnSpec> columns;
    // Insertion-order preserving for deterministic EXPLAIN output and
    // round-tripping JSON catalogs.
    std::map<std::string, std::string> properties;

    // Primary key + sink mode.
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

    // Sink delivery guarantee. Default 'at_least_once'.
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

    // Cross-sink commit group. Multiple sinks that share a non-empty
    // commit_group must commit together or all abort together. Defaults
    // to "" (no group; sink commits independently). Only meaningful for
    // exactly_once sinks (a non-2PC sink has no commit phase to
    // coordinate); the binder rejects commit_group on non-exactly_once
    // tables.
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
    // A logical (non-materialized) view: view_kind='logical'. CREATE VIEW does
    // not yet register these, so this is always false today; it lets the
    // object-kind-aware DROP reject DROP VIEW / DROP TABLE mismatches uniformly.
    [[nodiscard]] bool is_logical_view() const {
        auto it = properties.find("view_kind");
        return it != properties.end() && it->second == "logical";
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

// SQL-native AI: a model registered via CREATE MODEL. A model is pure declaration
// (no C++ factory) - a name, an INPUT and OUTPUT column schema, and provider
// properties (provider, task, endpoint, ...) - so it lives in the catalog beside a
// table declaration rather than in a runtime registry. ML_PREDICT reads the OUTPUT
// columns from the catalog at bind time to build its derived-table schema; the
// physical planner reads `provider` to choose the sync vs async predict operator,
// and passes the remaining properties to the runtime provider factory.
struct ModelDef {
    std::string name;
    std::vector<ColumnSpec> input_columns;
    std::vector<ColumnSpec> output_columns;
    // Insertion-order preserving, like TableDef::properties.
    std::map<std::string, std::string> properties;

    // Named WITH-option accessors (empty string when absent).
    [[nodiscard]] std::string provider() const {
        auto it = properties.find("provider");
        return it != properties.end() ? it->second : "";
    }
    [[nodiscard]] std::string task() const {
        auto it = properties.find("task");
        return it != properties.end() ? it->second : "";
    }

    // The model's OUTPUT columns as an Arrow schema, for the binder to append to
    // ML_PREDICT's derived-table schema.
    [[nodiscard]] std::shared_ptr<arrow::Schema> output_schema() const {
        arrow::FieldVector fields;
        fields.reserve(output_columns.size());
        for (const auto& c : output_columns) {
            fields.push_back(arrow::field(c.name, c.type));
        }
        return arrow::schema(std::move(fields));
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
    // Throws TranslationError if a column type isn't supported, or if
    // the table is already registered.
    void register_table(const ast::CreateTableStmt& stmt);

    // Remove a table by name. Returns true if it existed. When a
    // persistence dir is set, removes <dir>/<name>.json too. Drops any object
    // kind (table / materialized view); object-kind matching is the caller's
    // job via drop_object below.
    bool drop_table(const std::string& name);

    // Object-kind-aware drop (Postgres semantics: DROP TABLE rejects a
    // materialized view and vice versa). Returns NotFound if `name` is unknown,
    // KindMismatch if it exists but is not the requested kind, else removes it
    // (via drop_table) and returns Dropped.
    enum class DropResult { Dropped, NotFound, KindMismatch };
    DropResult drop_object(const std::string& name, ast::DropKind expected);

    // ANALYZE write-back: merge computed statistics (the row_count / ndv_<col> /
    // nulls_<col> / hist_<col> / mcv_<col> WITH-option keys) into table `name`'s
    // properties, overwriting any prior value for those keys (last-write-wins,
    // matching the WITH-clause semantics). Returns false if the table is
    // unknown. Re-persists the table when a persistence dir is set.
    bool merge_table_stats(const std::string& name,
                           const std::map<std::string, std::string>& stats);

    // CREATE VIEW: register a logical (non-materialized) view - a TableDef tagged
    // view_kind='logical' (carrying the view's output columns for name
    // resolution) plus the defining SELECT, which the binder expands inline at
    // each reference. Throws TranslationError if the name is already taken.
    // Logical views are session-scoped in v1: the defining query is NOT
    // persisted to the catalog dir (unlike a materialized view's backing table).
    void register_logical_view(TableDef def, ast::SelectStmt query);

    // The defining SELECT for a logical view, or nullptr if `name` is not a
    // registered logical view. The pointer is stable until the view is dropped.
    [[nodiscard]] const ast::SelectStmt* get_view_query(const std::string& name) const;

    // ALTER TABLE: apply ADD COLUMN / DROP COLUMN commands to a base table's
    // catalog declaration, re-persisting when a persistence dir is set. A
    // streaming table has no stored data to rewrite, so this just mutates the
    // declared columns. All commands apply atomically (a later failure rolls the
    // whole statement back). Throws TranslationError on: an unknown table
    // (unless IF EXISTS), a non-table target (view / materialized view), adding
    // an existing column (unless ADD COLUMN IF NOT EXISTS), dropping an absent
    // column (unless DROP COLUMN IF EXISTS), dropping the event-time or a
    // primary-key column, or leaving the table with no columns.
    void alter_table(const ast::AlterTableStmt& stmt);

    // ALTER TABLE RENAME: rename a base table (re-keying the catalog and the
    // persisted JSON) or rename one of its columns. A column rename cascades to
    // the event-time-column and primary-key references that name it. Throws
    // TranslationError on: an unknown table (unless IF EXISTS), a non-table target
    // (view / materialized view), a destination table/column name that already
    // exists, or (column rename) an absent source column. Dependent views that
    // reference the old name are NOT rewritten in v1 (they fail at their next
    // query). Re-persists when a persistence dir is set.
    void rename(const ast::RenameStmt& stmt);

    // Lookup. Returns nullptr if not found.
    [[nodiscard]] const TableDef* get_table(const std::string& name) const;

    // List currently-registered table names in registration order.
    [[nodiscard]] std::vector<std::string> list_tables() const;

    // SQL-native AI: model registry (CREATE MODEL). Models occupy a namespace
    // separate from tables (a model and a table may not share a name - both
    // register_model overloads reject a collision with an existing table, and
    // register_table is unaffected). Models persist to a `models/` subdir of the
    // catalog dir (kept separate from the flat table files), so a CREATE MODEL
    // survives a JobManager restart / HA takeover the same way a table does, and a
    // job that re-plans against the catalog (a scheduled REFRESH, a client re-submit)
    // still resolves the model.
    void register_model(ModelDef def);

    // Convenience: translate a CREATE MODEL AST into a ModelDef and register it.
    // Resolves INPUT / OUTPUT column types through sql_type_to_arrow. Honours
    // IF NOT EXISTS. Throws TranslationError on a duplicate model name, a name
    // already used by a table, or an unsupported column type.
    void register_model(const ast::CreateModelStmt& stmt);

    // Lookup. Returns nullptr if not found.
    [[nodiscard]] const ModelDef* get_model(const std::string& name) const;

    // Remove a model by name. Returns true if it existed.
    bool drop_model(const std::string& name);

    // List currently-registered model names in registration order.
    [[nodiscard]] std::vector<std::string> list_models() const;

    // Persistence:
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
    // for the HTTP catalog API.
    [[nodiscard]] static std::string to_json(const TableDef& def);
    [[nodiscard]] static TableDef from_json(const std::string& text);

    // Round-trip a single model's JSON form (the persisted catalog format).
    [[nodiscard]] static std::string to_json(const ModelDef& def);
    [[nodiscard]] static ModelDef model_from_json(const std::string& text);

private:
    std::unordered_map<std::string, TableDef> tables_;
    // Defining SELECT for each logical view (view_kind='logical' in tables_).
    // Node-stable so get_view_query can hand the binder a pointer.
    std::unordered_map<std::string, ast::SelectStmt> view_queries_;
    std::vector<std::string> order_;  // registration order, for list_tables
    std::string persistence_dir_;
    // SQL-native AI models (CREATE MODEL). In-memory only in v1.
    std::unordered_map<std::string, ModelDef> models_;
    std::vector<std::string> models_order_;  // registration order, for list_models
};

}  // namespace clink::sql
