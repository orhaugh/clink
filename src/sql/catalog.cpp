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

bool Catalog::drop_table(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end())
        return false;
    tables_.erase(it);
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

}  // namespace clink::sql
