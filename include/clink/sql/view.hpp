#pragma once

#include "clink/sql/ast.hpp"
#include "clink/sql/catalog.hpp"

// CREATE [OR REPLACE] VIEW: logical (non-materialized) view registration.
//
// Unlike a materialized view (a backing connector table + a maintenance job), a
// logical view has no storage and no job. register_view binds the defining
// SELECT once to derive the view's output columns, then stores a TableDef tagged
// view_kind='logical' plus the defining query in the catalog
// (Catalog::register_logical_view). A reference to the view is expanded inline
// by the binder (make_table_plan re-binds the stored query as a sub-plan), so a
// view is pure query rewrite with no runtime cost.

namespace clink::sql {

// Register `stmt` as a logical view in `catalog`. Throws TranslationError if:
//   * the name already names a non-view object (table / materialized view);
//   * the name already names a view and OR REPLACE was not given;
//   * the defining query has no output columns or duplicate output column names;
//   * a column-alias list is given whose length differs from the query's output
//     arity, or which has duplicate names.
// OR REPLACE drops an existing logical view of the same name first. Logical
// views are session-scoped in v1 (not persisted to the catalog dir).
void register_view(Catalog& catalog, ast::CreateViewStmt stmt);

// ALTER TABLE RENAME with dependent-view safety. Applies `stmt` via
// Catalog::rename, then re-binds every logical view that bound beforehand; if the
// rename broke any of them it is rolled back and TranslationError is thrown naming
// the affected view(s). A logical view is stored by name and re-bound on
// reference, so a rename of a table or column it depends on would otherwise leave
// it un-bindable at its next query; a blind AST rewrite cannot soundly retarget
// references through CTE scoping and subqueries, so the rename is rejected and the
// user drops/recreates the view instead. With no dependent views this is exactly
// Catalog::rename.
void rename_object(Catalog& catalog, const ast::RenameStmt& stmt);

}  // namespace clink::sql
