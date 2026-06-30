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
//   * the defining query has no output columns or duplicate output column names.
// OR REPLACE drops an existing logical view of the same name first. Logical
// views are session-scoped in v1 (not persisted to the catalog dir).
void register_view(Catalog& catalog, ast::CreateViewStmt stmt);

}  // namespace clink::sql
