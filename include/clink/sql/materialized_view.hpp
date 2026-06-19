#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "clink/sql/ast.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

// MATTBL: materialized-view desugaring.
//
// A materialized view is not a new engine abstraction. It is realised with
// today's primitives as:
//
//   1. a backing TableDef (the storage) registered in the catalog, whose
//      columns are derived from the defining query's output schema and whose
//      connector/format/mode/primary_key come from the WITH options;
//   2. a continuous maintenance job: INSERT INTO <backing> <defining SELECT>,
//      compiled and submitted like any hand-written streaming INSERT;
//   3. referencing queries (SELECT ... FROM <view>) that scan the backing
//      table through the ordinary source path.
//
// plan_materialized_view performs steps 1 and 2: it registers the backing
// table into `catalog` (which auto-persists when a persistence dir is set) and
// returns the bound, un-optimised maintenance plan. The caller optimises,
// compiles, and submits it exactly as it would a plain INSERT.
//
// v1 honours only continuous freshness (freshness='0'/'continuous'); a relaxed
// interval is rejected with a clear "scheduled refresh not yet supported"
// error rather than silently submitting a streaming job under a relaxed label.

namespace clink::sql {

struct MaterializedViewPlan {
    // The backing table that was registered into the catalog (a copy, for the
    // caller's convenience; the authoritative entry lives in the catalog).
    TableDef backing;
    // INSERT INTO <backing> <defining SELECT>, bound to a LogicalSink. The
    // caller optimises + compiles + submits this as the maintenance job.
    std::unique_ptr<LogicalPlan> maintenance;
};

// Desugar a CREATE MATERIALIZED VIEW into a backing table + maintenance plan.
// Registers the backing table in `catalog` as a side effect (so the subsequent
// maintenance bind and any later referencing query can resolve it). `stmt` is
// taken by value because its defining query is moved into the synthesised
// maintenance INSERT. `definition_sql` is the original statement text stored on
// the backing table for future re-binding (unused in v1; pass "" if unavailable).
//
// Throws TranslationError on any v1 guard failure: a non-both-capable backing
// connector, a non-continuous freshness, or a defining query that cannot bind.
// A name collision with an existing table surfaces from catalog.register_table.
MaterializedViewPlan plan_materialized_view(ast::CreateMaterializedViewStmt stmt,
                                            Catalog& catalog,
                                            std::string_view definition_sql = {});

}  // namespace clink::sql
