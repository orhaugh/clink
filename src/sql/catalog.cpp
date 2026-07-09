#include "clink/sql/catalog.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include "clink/config/json.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/type.hpp"

namespace clink::sql {

namespace {

namespace fs = std::filesystem;
using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;

fs::path table_json_path(const std::string& dir, const std::string& name) {
    return fs::path(dir) / (name + ".json");
}

// Models persist under a `models/` subdir so their files never collide with the flat
// per-table files at the catalog root (load_from_dir parses every root .json as a
// TableDef; the subdir is skipped there and loaded separately).
fs::path models_dir(const std::string& dir) {
    return fs::path(dir) / "models";
}
fs::path model_json_path(const std::string& dir, const std::string& name) {
    return models_dir(dir) / (name + ".json");
}

// Scalar UDF declarations persist under `functions/`, same scheme as models.
fs::path functions_dir(const std::string& dir) {
    return fs::path(dir) / "functions";
}
fs::path function_json_path(const std::string& dir, const std::string& name) {
    return functions_dir(dir) / (name + ".json");
}

void write_file_atomic(const fs::path& target, const std::string& text) {
    // <target>.tmp then rename - survives crashes mid-write.
    auto tmp = target;
    tmp += ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("catalog: cannot open " + tmp.string() + " for write");
        }
        out.write(text.data(), static_cast<std::streamsize>(text.size()));
        if (!out) {
            throw std::runtime_error("catalog: write failed for " + tmp.string());
        }
    }
    fs::rename(tmp, target);
}

// Lift primary_key from the WITH-options bag into the typed TableDef
// field. Trims whitespace per entry so `'a, b'`
// works as expected.
void lift_typed_fields(TableDef& def) {
    def.primary_key.clear();
    auto pk_it = def.properties.find("primary_key");
    if (pk_it == def.properties.end()) {
        return;
    }
    const auto& csv = pk_it->second;
    std::size_t pos = 0;
    while (pos <= csv.size()) {
        auto end = csv.find(',', pos);
        if (end == std::string::npos)
            end = csv.size();
        auto k = csv.substr(pos, end - pos);
        auto a = k.find_first_not_of(" \t");
        auto b = k.find_last_not_of(" \t");
        if (a != std::string::npos)
            def.primary_key.push_back(k.substr(a, b - a + 1));
        if (end == csv.size())
            break;
        pos = end + 1;
    }
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) {
        throw std::runtime_error("catalog: cannot open " + p.string());
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

void Catalog::register_table(TableDef def) {
    if (tables_.find(def.name) != tables_.end()) {
        throw TranslationError("table already exists: " + def.name, 0);
    }
    std::string name = def.name;
    if (!persistence_dir_.empty()) {
        fs::create_directories(persistence_dir_);
        write_file_atomic(table_json_path(persistence_dir_, name), to_json(def));
    }
    tables_.emplace(name, std::move(def));
    order_.push_back(std::move(name));
}

bool Catalog::merge_table_stats(const std::string& name,
                                const std::map<std::string, std::string>& stats) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return false;
    }
    for (const auto& [k, v] : stats) {
        it->second.properties[k] = v;  // overwrite any prior stat value
    }
    if (!persistence_dir_.empty()) {
        fs::create_directories(persistence_dir_);
        write_file_atomic(table_json_path(persistence_dir_, name), to_json(it->second));
    }
    return true;
}

void Catalog::register_table(const ast::CreateTableStmt& stmt) {
    // CREATE TABLE IF NOT EXISTS: a no-op when the table is already
    // registered (matches PG - the existing definition is kept).
    if (stmt.if_not_exists && get_table(stmt.table_name) != nullptr) {
        return;
    }
    TableDef def;
    def.name = stmt.table_name;
    def.columns.reserve(stmt.columns.size());
    for (const auto& col : stmt.columns) {
        def.columns.push_back(ColumnSpec{col.name, sql_type_to_arrow(col.type)});
    }
    for (const auto& opt : stmt.options) {
        // Duplicate keys: last-write-wins matches PG's WITH-clause
        // semantics (the last DefElem for a given defname survives).
        def.properties[opt.key] = opt.value;
    }
    lift_typed_fields(def);
    register_table(std::move(def));
}

void Catalog::register_model(ModelDef def) {
    if (models_.find(def.name) != models_.end()) {
        throw TranslationError("model already exists: " + def.name, 0);
    }
    if (tables_.find(def.name) != tables_.end()) {
        throw TranslationError(
            "cannot create model '" + def.name + "': a table with that name already exists", 0);
    }
    std::string name = def.name;
    if (!persistence_dir_.empty()) {
        fs::create_directories(models_dir(persistence_dir_));
        write_file_atomic(model_json_path(persistence_dir_, name), to_json(def));
    }
    models_.emplace(name, std::move(def));
    models_order_.push_back(std::move(name));
}

void Catalog::register_model(const ast::CreateModelStmt& stmt) {
    // CREATE MODEL IF NOT EXISTS: a no-op when the model is already registered.
    if (stmt.if_not_exists && get_model(stmt.model_name) != nullptr) {
        return;
    }
    ModelDef def;
    def.name = stmt.model_name;
    def.input_columns.reserve(stmt.input_columns.size());
    for (const auto& col : stmt.input_columns) {
        def.input_columns.push_back(ColumnSpec{col.name, sql_type_to_arrow(col.type)});
    }
    def.output_columns.reserve(stmt.output_columns.size());
    for (const auto& col : stmt.output_columns) {
        def.output_columns.push_back(ColumnSpec{col.name, sql_type_to_arrow(col.type)});
    }
    for (const auto& opt : stmt.options) {
        def.properties[opt.key] = opt.value;  // last-write-wins, like CREATE TABLE
    }
    register_model(std::move(def));
}

const ModelDef* Catalog::get_model(const std::string& name) const {
    auto it = models_.find(name);
    return it == models_.end() ? nullptr : &it->second;
}

bool Catalog::drop_model(const std::string& name) {
    auto it = models_.find(name);
    if (it == models_.end()) {
        return false;
    }
    models_.erase(it);
    auto pos = std::find(models_order_.begin(), models_order_.end(), name);
    if (pos != models_order_.end()) {
        models_order_.erase(pos);
    }
    if (!persistence_dir_.empty()) {
        std::error_code ec;
        fs::remove(model_json_path(persistence_dir_, name), ec);
    }
    return true;
}

std::vector<std::string> Catalog::list_models() const {
    return models_order_;
}

void Catalog::register_logical_view(TableDef def, ast::SelectStmt query) {
    if (tables_.find(def.name) != tables_.end()) {
        throw TranslationError("object '" + def.name + "' is already registered", 0);
    }
    const std::string name = def.name;
    tables_.emplace(name, std::move(def));
    order_.push_back(name);
    view_queries_.emplace(name, std::move(query));
    // Intentionally NOT persisted: a logical view is session-scoped in v1 (the
    // defining SELECT is not serialised to the catalog dir, unlike a
    // materialized view's backing connector table).
}

void Catalog::alter_table(const ast::AlterTableStmt& stmt) {
    auto it = tables_.find(stmt.table_name);
    if (it == tables_.end()) {
        if (stmt.if_exists) {
            return;
        }
        throw TranslationError("table does not exist: " + stmt.table_name, stmt.loc.pos);
    }
    if (it->second.is_materialized_view() || it->second.is_logical_view()) {
        throw TranslationError(
            "\"" + stmt.table_name + "\" is not a table (ALTER TABLE applies to base tables only)",
            stmt.loc.pos);
    }
    // Apply to a copy so a mid-statement failure leaves the table unchanged.
    TableDef next = it->second;
    auto find_col = [&next](const std::string& name) {
        return std::find_if(next.columns.begin(), next.columns.end(), [&name](const ColumnSpec& c) {
            return c.name == name;
        });
    };
    for (const auto& cmd : stmt.cmds) {
        if (cmd.kind == ast::AlterTableCmd::Kind::AddColumn) {
            if (find_col(cmd.column_name) != next.columns.end()) {
                if (cmd.missing_ok) {
                    continue;  // ADD COLUMN IF NOT EXISTS
                }
                throw TranslationError(
                    "column '" + cmd.column_name + "' already exists in '" + stmt.table_name + "'",
                    cmd.loc.pos);
            }
            next.columns.push_back(ColumnSpec{cmd.column_name, sql_type_to_arrow(cmd.type)});
        } else if (cmd.kind == ast::AlterTableCmd::Kind::AlterColumnType) {
            auto cit = find_col(cmd.column_name);
            if (cit == next.columns.end()) {
                throw TranslationError(
                    "column '" + cmd.column_name + "' does not exist in '" + stmt.table_name + "'",
                    cmd.loc.pos);
            }
            // Catalog-level type swap: a streaming table has no stored rows to
            // cast, so the new type just changes how the source decodes the
            // column and how downstream binds it. sql_type_to_arrow validates /
            // rejects an unsupported type.
            cit->type = sql_type_to_arrow(cmd.type);
        } else if (cmd.kind == ast::AlterTableCmd::Kind::DropColumn) {
            auto cit = find_col(cmd.column_name);
            if (cit == next.columns.end()) {
                if (cmd.missing_ok) {
                    continue;  // DROP COLUMN IF EXISTS
                }
                throw TranslationError(
                    "column '" + cmd.column_name + "' does not exist in '" + stmt.table_name + "'",
                    cmd.loc.pos);
            }
            // Read the event-time / primary-key guards freshly: an earlier SET /
            // RESET in this same statement may have changed which column they name.
            const auto et = next.properties.find("event_time_column");
            if (et != next.properties.end() && et->second == cmd.column_name) {
                throw TranslationError(
                    "cannot drop column '" + cmd.column_name + "': it is the event-time column",
                    cmd.loc.pos);
            }
            if (std::find(next.primary_key.begin(), next.primary_key.end(), cmd.column_name) !=
                next.primary_key.end()) {
                throw TranslationError(
                    "cannot drop column '" + cmd.column_name + "': it is part of the primary key",
                    cmd.loc.pos);
            }
            next.columns.erase(cit);
        } else {  // SetOptions / ResetOptions: mutate the WITH-option bag.
            for (const auto& opt : cmd.options) {
                if (cmd.kind == ast::AlterTableCmd::Kind::SetOptions) {
                    next.properties[opt.key] = opt.value;  // last-write-wins
                } else {
                    next.properties.erase(opt.key);  // RESET
                }
            }
            // A changed primary_key property must re-lift the typed field so a
            // later command in this statement (and the binder) sees it.
            lift_typed_fields(next);
        }
    }
    if (next.columns.empty()) {
        throw TranslationError("ALTER TABLE would leave '" + stmt.table_name + "' with no columns",
                               stmt.loc.pos);
    }
    it->second = std::move(next);  // commit
    if (!persistence_dir_.empty()) {
        fs::create_directories(persistence_dir_);
        write_file_atomic(table_json_path(persistence_dir_, stmt.table_name), to_json(it->second));
    }
}

void Catalog::rename(const ast::RenameStmt& stmt) {
    auto it = tables_.find(stmt.table_name);
    if (it == tables_.end()) {
        if (stmt.if_exists) {
            return;
        }
        throw TranslationError("table does not exist: " + stmt.table_name, stmt.loc.pos);
    }
    if (it->second.is_materialized_view() || it->second.is_logical_view()) {
        throw TranslationError("\"" + stmt.table_name +
                                   "\" is not a table (ALTER TABLE RENAME applies to base tables "
                                   "only)",
                               stmt.loc.pos);
    }

    if (stmt.kind == ast::RenameStmt::Kind::Table) {
        if (stmt.new_name == stmt.table_name) {
            return;  // rename to itself: no-op
        }
        if (tables_.find(stmt.new_name) != tables_.end()) {
            throw TranslationError("object '" + stmt.new_name + "' already exists", stmt.loc.pos);
        }
        TableDef def = std::move(it->second);
        tables_.erase(it);
        def.name = stmt.new_name;
        tables_.emplace(stmt.new_name, std::move(def));
        std::replace(order_.begin(), order_.end(), stmt.table_name, stmt.new_name);
        if (!persistence_dir_.empty()) {
            fs::create_directories(persistence_dir_);
            write_file_atomic(table_json_path(persistence_dir_, stmt.new_name),
                              to_json(tables_.at(stmt.new_name)));
            std::error_code ec;
            fs::remove(table_json_path(persistence_dir_, stmt.table_name), ec);
        }
        return;
    }

    // Column rename. Validate before mutating; the rename then cascades to the
    // event-time-column and primary-key references that name the old column.
    TableDef& def = it->second;
    auto find_col = [&def](const std::string& name) {
        return std::find_if(def.columns.begin(), def.columns.end(), [&name](const ColumnSpec& c) {
            return c.name == name;
        });
    };
    auto cit = find_col(stmt.old_column);
    if (cit == def.columns.end()) {
        throw TranslationError(
            "column '" + stmt.old_column + "' does not exist in '" + stmt.table_name + "'",
            stmt.loc.pos);
    }
    if (stmt.new_name == stmt.old_column) {
        return;  // no-op
    }
    if (find_col(stmt.new_name) != def.columns.end()) {
        throw TranslationError(
            "column '" + stmt.new_name + "' already exists in '" + stmt.table_name + "'",
            stmt.loc.pos);
    }
    cit->name = stmt.new_name;
    if (auto et = def.properties.find("event_time_column");
        et != def.properties.end() && et->second == stmt.old_column) {
        et->second = stmt.new_name;
    }
    if (auto pk = def.properties.find("primary_key"); pk != def.properties.end()) {
        // Rewrite the CSV, replacing the renamed column token (trimmed).
        std::string out;
        const std::string& csv = pk->second;
        std::size_t pos = 0;
        bool first = true;
        while (pos <= csv.size()) {
            auto end = csv.find(',', pos);
            if (end == std::string::npos) {
                end = csv.size();
            }
            auto tok = csv.substr(pos, end - pos);
            auto a = tok.find_first_not_of(" \t");
            auto b = tok.find_last_not_of(" \t");
            if (a != std::string::npos) {
                const std::string trimmed = tok.substr(a, b - a + 1);
                if (!first) {
                    out += ',';
                }
                out += (trimmed == stmt.old_column) ? stmt.new_name : trimmed;
                first = false;
            }
            if (end == csv.size()) {
                break;
            }
            pos = end + 1;
        }
        pk->second = out;
    }
    lift_typed_fields(def);  // re-derive primary_key from the rewritten CSV
    if (!persistence_dir_.empty()) {
        fs::create_directories(persistence_dir_);
        write_file_atomic(table_json_path(persistence_dir_, stmt.table_name), to_json(def));
    }
}

const ast::SelectStmt* Catalog::get_view_query(const std::string& name) const {
    auto it = view_queries_.find(name);
    return it == view_queries_.end() ? nullptr : &it->second;
}

bool Catalog::drop_table(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end())
        return false;
    tables_.erase(it);
    view_queries_.erase(name);  // no-op unless `name` is a logical view
    auto pos = std::find(order_.begin(), order_.end(), name);
    if (pos != order_.end())
        order_.erase(pos);
    if (!persistence_dir_.empty()) {
        std::error_code ec;
        fs::remove(table_json_path(persistence_dir_, name), ec);
        // ignore missing-file errors; drop is idempotent at the FS layer
    }
    return true;
}

Catalog::DropResult Catalog::drop_object(const std::string& name, ast::DropKind expected) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return DropResult::NotFound;
    }
    const TableDef& def = it->second;
    bool kind_ok = false;
    switch (expected) {
        case ast::DropKind::Table:
            kind_ok = !def.is_materialized_view() && !def.is_logical_view();
            break;
        case ast::DropKind::MaterializedView:
            kind_ok = def.is_materialized_view();
            break;
        case ast::DropKind::View:
            kind_ok = def.is_logical_view();
            break;
    }
    if (!kind_ok) {
        return DropResult::KindMismatch;
    }
    drop_table(name);
    return DropResult::Dropped;
}

const TableDef* Catalog::get_table(const std::string& name) const {
    auto it = tables_.find(name);
    return it == tables_.end() ? nullptr : &it->second;
}

std::vector<std::string> Catalog::list_tables() const {
    return order_;
}

void Catalog::set_persistence_dir(std::string dir) {
    persistence_dir_ = std::move(dir);
}

void Catalog::load_from_dir(const std::string& dir) {
    tables_.clear();
    order_.clear();
    models_.clear();
    models_order_.clear();
    functions_.clear();
    functions_order_.clear();
    fs::path root(dir);
    if (!fs::exists(root))
        return;  // empty catalog is a valid initial state
    if (!fs::is_directory(root)) {
        throw std::runtime_error("catalog: persistence dir is not a directory: " + dir);
    }
    // Sort entries so registration order is deterministic across runs;
    // alphabetical by filename. Matches what users see in `ls`.
    std::vector<fs::path> entries;
    for (auto& e : fs::directory_iterator(root)) {
        if (!e.is_regular_file())
            continue;
        if (e.path().extension() != ".json")
            continue;
        entries.push_back(e.path());
    }
    std::sort(entries.begin(), entries.end());
    for (const auto& p : entries) {
        auto def = from_json(read_file(p));
        std::string name = def.name;
        tables_.emplace(name, std::move(def));
        order_.push_back(std::move(name));
    }

    // Models live in the `models/` subdir (skipped by the root file scan above).
    const fs::path mdir = models_dir(dir);
    if (fs::is_directory(mdir)) {
        std::vector<fs::path> ments;
        for (auto& e : fs::directory_iterator(mdir)) {
            if (e.is_regular_file() && e.path().extension() == ".json") {
                ments.push_back(e.path());
            }
        }
        std::sort(ments.begin(), ments.end());
        for (const auto& p : ments) {
            auto def = model_from_json(read_file(p));
            std::string name = def.name;
            models_.emplace(name, std::move(def));
            models_order_.push_back(std::move(name));
        }
    }

    // Scalar UDF declarations live in `functions/`, same scheme.
    const fs::path fdir = functions_dir(dir);
    if (fs::is_directory(fdir)) {
        std::vector<fs::path> fents;
        for (auto& e : fs::directory_iterator(fdir)) {
            if (e.is_regular_file() && e.path().extension() == ".json") {
                fents.push_back(e.path());
            }
        }
        std::sort(fents.begin(), fents.end());
        for (const auto& p : fents) {
            auto def = function_from_json(read_file(p));
            std::string name = def.name;
            functions_.emplace(name, std::move(def));
            functions_order_.push_back(std::move(name));
        }
    }
}

std::string Catalog::to_json(const TableDef& def) {
    JsonArray columns_arr;
    columns_arr.reserve(def.columns.size());
    for (const auto& c : def.columns) {
        JsonObject col;
        col["name"] = JsonValue{c.name};
        col["type"] = JsonValue{arrow_to_sql_type_string(*c.type)};
        columns_arr.emplace_back(std::move(col));
    }
    JsonObject props;
    for (const auto& [k, v] : def.properties) {
        props[k] = JsonValue{v};
    }
    JsonObject root;
    root["name"] = JsonValue{def.name};
    root["columns"] = JsonValue{std::move(columns_arr)};
    root["properties"] = JsonValue{std::move(props)};
    return JsonValue{std::move(root)}.serialize(2);
}

TableDef Catalog::from_json(const std::string& text) {
    auto j = clink::config::parse(text);
    if (!j.is_object()) {
        throw std::runtime_error("catalog: TableDef JSON must be an object");
    }
    TableDef def;
    if (!j.contains("name") || !j.at("name").is_string()) {
        throw std::runtime_error("catalog: TableDef JSON missing name");
    }
    def.name = j.at("name").as_string();
    if (j.contains("columns")) {
        if (!j.at("columns").is_array()) {
            throw std::runtime_error("catalog: columns must be an array");
        }
        for (const auto& c : j.at("columns").as_array()) {
            if (!c.is_object()) {
                throw std::runtime_error("catalog: column entry must be an object");
            }
            if (!c.contains("name") || !c.at("name").is_string()) {
                throw std::runtime_error("catalog: column missing name");
            }
            if (!c.contains("type") || !c.at("type").is_string()) {
                throw std::runtime_error("catalog: column missing type");
            }
            auto type_ast = parse_sql_type_expression(c.at("type").as_string());
            def.columns.push_back(
                ColumnSpec{c.at("name").as_string(), sql_type_to_arrow(type_ast)});
        }
    }
    if (j.contains("properties")) {
        if (!j.at("properties").is_object()) {
            throw std::runtime_error("catalog: properties must be an object");
        }
        for (const auto& [k, v] : j.at("properties").as_object()) {
            if (!v.is_string()) {
                throw std::runtime_error("catalog: property values must be strings");
            }
            def.properties[k] = v.as_string();
        }
    }
    lift_typed_fields(def);
    return def;
}

namespace {
JsonArray columns_to_json(const std::vector<ColumnSpec>& cols) {
    JsonArray arr;
    arr.reserve(cols.size());
    for (const auto& c : cols) {
        JsonObject col;
        col["name"] = JsonValue{c.name};
        col["type"] = JsonValue{arrow_to_sql_type_string(*c.type)};
        arr.emplace_back(std::move(col));
    }
    return arr;
}
void columns_from_json(const JsonValue& arr, std::vector<ColumnSpec>& out) {
    if (!arr.is_array()) {
        throw std::runtime_error("catalog: model columns must be an array");
    }
    for (const auto& c : arr.as_array()) {
        if (!c.is_object() || !c.contains("name") || !c.at("name").is_string() ||
            !c.contains("type") || !c.at("type").is_string()) {
            throw std::runtime_error("catalog: model column must have string name + type");
        }
        auto type_ast = parse_sql_type_expression(c.at("type").as_string());
        out.push_back(ColumnSpec{c.at("name").as_string(), sql_type_to_arrow(type_ast)});
    }
}
}  // namespace

std::string Catalog::to_json(const ModelDef& def) {
    JsonObject props;
    for (const auto& [k, v] : def.properties) {
        props[k] = JsonValue{v};
    }
    JsonObject root;
    root["name"] = JsonValue{def.name};
    root["input_columns"] = JsonValue{columns_to_json(def.input_columns)};
    root["output_columns"] = JsonValue{columns_to_json(def.output_columns)};
    root["properties"] = JsonValue{std::move(props)};
    return JsonValue{std::move(root)}.serialize(2);
}

void Catalog::register_function(FunctionDef def, bool or_replace) {
    if (!or_replace && functions_.find(def.name) != functions_.end()) {
        throw TranslationError("function already exists: " + def.name, 0);
    }
    std::string name = def.name;
    if (!persistence_dir_.empty()) {
        fs::create_directories(functions_dir(persistence_dir_));
        write_file_atomic(function_json_path(persistence_dir_, name), to_json(def));
    }
    const bool existed = functions_.find(name) != functions_.end();
    functions_[name] = std::move(def);
    if (!existed) {
        functions_order_.push_back(std::move(name));
    }
}

const FunctionDef* Catalog::get_function(const std::string& name) const {
    auto it = functions_.find(name);
    return it == functions_.end() ? nullptr : &it->second;
}

bool Catalog::drop_function(const std::string& name) {
    auto it = functions_.find(name);
    if (it == functions_.end()) {
        return false;
    }
    functions_.erase(it);
    auto pos = std::find(functions_order_.begin(), functions_order_.end(), name);
    if (pos != functions_order_.end()) {
        functions_order_.erase(pos);
    }
    if (!persistence_dir_.empty()) {
        std::error_code ec;
        fs::remove(function_json_path(persistence_dir_, name), ec);
    }
    return true;
}

std::vector<std::string> Catalog::list_functions() const {
    return functions_order_;
}

std::string Catalog::to_json(const FunctionDef& def) {
    JsonArray args;
    args.reserve(def.arg_types.size());
    for (const auto& t : def.arg_types) {
        args.emplace_back(t);
    }
    JsonArray defs;
    defs.reserve(def.definitions.size());
    for (const auto& d : def.definitions) {
        defs.emplace_back(d);
    }
    JsonObject root;
    root["name"] = JsonValue{def.name};
    root["language"] = JsonValue{def.language};
    root["arg_types"] = JsonValue{std::move(args)};
    root["return_type"] = JsonValue{def.return_type};
    root["definitions"] = JsonValue{std::move(defs)};
    root["module_b64"] = JsonValue{def.module_b64};
    return JsonValue{std::move(root)}.serialize(2);
}

FunctionDef Catalog::function_from_json(const std::string& text) {
    auto j = clink::config::parse(text);
    if (!j.is_object()) {
        throw std::runtime_error("catalog: FunctionDef JSON must be an object");
    }
    FunctionDef def;
    if (!j.contains("name") || !j.at("name").is_string()) {
        throw std::runtime_error("catalog: FunctionDef JSON missing name");
    }
    def.name = j.at("name").as_string();
    if (!j.contains("language") || !j.at("language").is_string()) {
        throw std::runtime_error("catalog: FunctionDef JSON missing language");
    }
    def.language = j.at("language").as_string();
    if (j.contains("arg_types") && j.at("arg_types").is_array()) {
        for (const auto& t : j.at("arg_types").as_array()) {
            def.arg_types.push_back(t.as_string());
        }
    }
    def.return_type = j.string_or("return_type", "");
    if (j.contains("definitions") && j.at("definitions").is_array()) {
        for (const auto& d : j.at("definitions").as_array()) {
            def.definitions.push_back(d.as_string());
        }
    }
    def.module_b64 = j.string_or("module_b64", "");
    return def;
}

ModelDef Catalog::model_from_json(const std::string& text) {
    auto j = clink::config::parse(text);
    if (!j.is_object()) {
        throw std::runtime_error("catalog: ModelDef JSON must be an object");
    }
    ModelDef def;
    if (!j.contains("name") || !j.at("name").is_string()) {
        throw std::runtime_error("catalog: ModelDef JSON missing name");
    }
    def.name = j.at("name").as_string();
    if (j.contains("input_columns")) {
        columns_from_json(j.at("input_columns"), def.input_columns);
    }
    if (j.contains("output_columns")) {
        columns_from_json(j.at("output_columns"), def.output_columns);
    }
    if (j.contains("properties")) {
        if (!j.at("properties").is_object()) {
            throw std::runtime_error("catalog: properties must be an object");
        }
        for (const auto& [k, v] : j.at("properties").as_object()) {
            if (!v.is_string()) {
                throw std::runtime_error("catalog: property values must be strings");
            }
            def.properties[k] = v.as_string();
        }
    }
    return def;
}

}  // namespace clink::sql
