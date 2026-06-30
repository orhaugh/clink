#include "clink/sql/view.hpp"

#include <cstddef>
#include <set>
#include <string>
#include <utility>

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

    TableDef def;
    def.name = stmt.view_name;
    def.columns.reserve(static_cast<std::size_t>(schema->num_fields()));
    std::set<std::string> seen;
    for (const auto& field : schema->fields()) {
        if (!seen.insert(field->name()).second) {
            throw TranslationError("view '" + stmt.view_name + "': duplicate output column '" +
                                       field->name() +
                                       "' (give the columns distinct aliases in the SELECT)",
                                   stmt.loc.pos);
        }
        def.columns.push_back(ColumnSpec{field->name(), field->type()});
    }
    def.properties["view_kind"] = "logical";
    // The bind succeeded; now it is safe to drop the prior definition (OR
    // REPLACE) and register the new one.
    if (replacing) {
        catalog.drop_table(stmt.view_name);
    }
    catalog.register_logical_view(std::move(def), std::move(stmt.query));
}

}  // namespace clink::sql
