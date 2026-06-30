#include "clink/sql/view.hpp"

#include <cstddef>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "clink/sql/binder.hpp"
#include "clink/sql/parser.hpp"  // TranslationError

namespace clink::sql {

void register_view(Catalog& catalog, ast::CreateViewStmt stmt) {
    // Conflict handling. A name already taken by a NON-view object is a hard
    // error even with OR REPLACE (Postgres: "X is not a view"); an existing
    // logical view is replaced only with OR REPLACE.
    bool replacing = false;
    if (const TableDef* existing = catalog.get_table(stmt.view_name)) {
        if (!existing->is_logical_view()) {
            throw TranslationError("'" + stmt.view_name + "' already exists and is not a view",
                                   stmt.loc.pos);
        }
        if (!stmt.or_replace) {
            throw TranslationError(
                "view '" + stmt.view_name + "' already exists (use CREATE OR REPLACE VIEW)",
                stmt.loc.pos);
        }
        replacing = true;
    }

    // Bind the defining query once to derive the view's output columns. Done
    // BEFORE dropping any prior definition, so a failing OR REPLACE bind leaves
    // the original view intact. A fresh binder isolates any WITH-clause state; a
    // reference to another view inside the query expands through make_table_plan,
    // so nested views resolve here. (An OR REPLACE that introduces a reference
    // cycle binds against the prior definition here and succeeds; the cycle is
    // then caught by the binder's expanding-views guard at query time.)
    Binder schema_binder(catalog);
    auto plan = schema_binder.bind_select(stmt.query);
    auto schema = plan->schema();
    if (!schema || schema->num_fields() == 0) {
        throw TranslationError(
            "view '" + stmt.view_name + "': defining query has no output columns", stmt.loc.pos);
    }

    // An optional column-alias list renames the output columns positionally, so
    // it must match the query's output arity. With no alias list the query's own
    // output names are used. The aliased names are what every reference to the
    // view sees; the inline expansion (make_table_plan) reconciles the re-bound
    // sub-plan's columns to them positionally.
    const auto& aliases = stmt.column_aliases;
    if (!aliases.empty() && aliases.size() != static_cast<std::size_t>(schema->num_fields())) {
        throw TranslationError("view '" + stmt.view_name + "': column-alias list names " +
                                   std::to_string(aliases.size()) +
                                   " column(s) but the query produces " +
                                   std::to_string(schema->num_fields()),
                               stmt.loc.pos);
    }

    TableDef def;
    def.name = stmt.view_name;
    def.columns.reserve(static_cast<std::size_t>(schema->num_fields()));
    std::set<std::string> seen;
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& field = schema->field(i);
        const std::string out_name =
            aliases.empty() ? field->name() : aliases[static_cast<std::size_t>(i)];
        if (!seen.insert(out_name).second) {
            throw TranslationError(
                "view '" + stmt.view_name + "': duplicate output column '" + out_name +
                    (aliases.empty() ? "' (give the columns distinct aliases in the SELECT)"
                                     : "' in the column-alias list"),
                stmt.loc.pos);
        }
        def.columns.push_back(ColumnSpec{out_name, field->type()});
    }
    def.properties["view_kind"] = "logical";
    // The bind succeeded; now it is safe to drop the prior definition (OR
    // REPLACE) and register the new one.
    if (replacing) {
        catalog.drop_table(stmt.view_name);
    }
    catalog.register_logical_view(std::move(def), std::move(stmt.query));
}

namespace {

// Names of logical views whose defining query currently binds against `catalog`.
// A view that already fails to bind (e.g. it references a since-dropped object) is
// excluded, so a later rename cannot be falsely blamed for an existing breakage.
std::set<std::string> views_that_bind(const Catalog& catalog) {
    std::set<std::string> ok;
    for (const auto& name : catalog.list_tables()) {
        const TableDef* def = catalog.get_table(name);
        if (def == nullptr || !def->is_logical_view()) {
            continue;
        }
        const ast::SelectStmt* query = catalog.get_view_query(name);
        if (query == nullptr) {
            continue;
        }
        try {
            Binder binder(catalog);
            (void)binder.bind_select(*query);
            ok.insert(name);
        } catch (...) {
            // Already un-bindable; not affected by the rename under test.
        }
    }
    return ok;
}

// The rename that undoes `stmt`: swap the table names (table rename) or the column
// names (column rename). Catalog::rename is its own inverse under this swap, so
// applying it restores both the in-memory state and the persisted files.
ast::RenameStmt inverse_rename(const ast::RenameStmt& stmt) {
    ast::RenameStmt inv = stmt;
    if (stmt.kind == ast::RenameStmt::Kind::Table) {
        inv.table_name = stmt.new_name;
        inv.new_name = stmt.table_name;
    } else {
        inv.old_column = stmt.new_name;
        inv.new_name = stmt.old_column;
    }
    return inv;
}

}  // namespace

void rename_object(Catalog& catalog, const ast::RenameStmt& stmt) {
    const std::set<std::string> before = views_that_bind(catalog);
    catalog.rename(stmt);  // mutation + persistence (throws on the usual guards)
    if (before.empty()) {
        return;  // no dependents to break
    }
    std::vector<std::string> broken;
    for (const auto& name : before) {
        const ast::SelectStmt* query = catalog.get_view_query(name);
        if (query == nullptr) {
            continue;
        }
        try {
            Binder binder(catalog);
            (void)binder.bind_select(*query);
        } catch (...) {
            broken.push_back(name);
        }
    }
    if (broken.empty()) {
        return;
    }
    catalog.rename(inverse_rename(stmt));  // roll back: restore in-memory + persisted
    std::string list;
    for (std::size_t i = 0; i < broken.size(); ++i) {
        if (i != 0) {
            list += ", ";
        }
        list += "'" + broken[i] + "'";
    }
    throw TranslationError(
        "rename would break dependent view(s) " + list +
            "; drop and recreate them (a logical view tracks its sources by name)",
        stmt.loc.pos);
}

}  // namespace clink::sql
