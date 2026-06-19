#pragma once

#include <memory>

#include "clink/sql/logical_plan.hpp"

// Rule-based optimizer. Phase 6 ships with one user-visible
// optimisation:
//
//   Projection pushdown - walk the plan top-down, collect the union of
//   columns each upstream consumer references, and annotate the source
//   Scan with that set. The physical planner emits the trimmed list as
//   a 'projected_columns' param on the source op. The file_json (Row)
//   source consumes it and drops unreferenced columns at decode (still
//   parses the whole JSON line, so the win is narrower downstream rows,
//   not parse cost). The column-needs analysis unions the table's
//   event_time_column (assign_timestamps reads it) and preserves the
//   synthetic __row_kind marker, so narrowing never starves a downstream
//   or physical op. Parquet column-skip awaits a Row-channel connector.
//
//   Predicate pushdown - relocate single-side WHERE conjuncts below an
//   INNER equi/interval join (or the probe side of a lookup join) into
//   the matching scan, de-aliased to the raw column.
//
// The optimizer is purely transformative on the LogicalPlan tree; it
// does not change anything observable to the binder or the physical
// planner beyond setting LogicalScan::projected_columns().

namespace clink::sql {

// Apply every optimizer rule in sequence. Returns the rewritten plan
// (may return the same node if no rule fires).
std::unique_ptr<LogicalPlan> optimize(std::unique_ptr<LogicalPlan> plan);

}  // namespace clink::sql
