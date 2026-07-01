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

// The refresh arm chosen from the FRESHNESS budget. Continuous keeps a live
// streaming maintenance job; Full recomputes the whole backing on demand (a bounded
// INSERT that atomically overwrites), driven by a manual REFRESH (and, in a later
// increment, a scheduler at the freshness cadence).
enum class RefreshArm { Continuous, Full };

struct MaterializedViewPlan {
    // The backing table that was registered into the catalog (a copy, for the
    // caller's convenience; the authoritative entry lives in the catalog).
    TableDef backing;
    RefreshArm arm = RefreshArm::Continuous;
    // The bound INSERT INTO <backing> <defining SELECT>, rooted in a LogicalSink.
    // Continuous: the caller submits it as a live maintenance job. Full: the caller
    // submits it as a one-shot BOUNDED job (the backing is tagged write_mode=overwrite
    // so the sink atomically publishes on completion) - an initial population; a
    // subsequent REFRESH re-runs the same recompute.
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

// Build the bounded recompute plan for a REFRESH MATERIALIZED VIEW: look up the
// (already-registered) full-refresh backing table, re-parse its stored definition,
// and return the bound INSERT INTO <backing> <SELECT>. The backing carries
// write_mode=overwrite, so compiling + submitting this as a bounded job recomputes
// the whole result and atomically overwrites the backing on completion.
//
// Throws TranslationError if the view is unknown, is not a materialized view, is a
// continuous (not full-refresh) view, or has no stored definition.
std::unique_ptr<LogicalPlan> plan_materialized_view_refresh(const std::string& view_name,
                                                            Catalog& catalog);

}  // namespace clink::sql
