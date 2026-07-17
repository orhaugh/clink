#pragma once

// Column-level lineage capture for a compiled SQL INSERT.
//
// Runs at SQL compile time, where the bound LogicalSink and its upstream
// LogicalScan leaves are in scope (the Coordinator only sees the lowered
// operator graph and has no column context). Produces the JSON object the
// Coordinator later attaches to sink datasets via extract_lineage:
//
//   {"<sink_op_id>":[
//      {"output":"<col>","transformation":"IDENTITY|TRANSFORMATION|AGGREGATION",
//       "inputs":[{"namespace":"<ns>","name":"<name>","field":"<col>"}]}
//   ]}
//
// Source datasets are identified by the SAME (namespace, name) the lineage
// normaliser derives from the lowered source op, so the captured input refs
// line up with the source vertices in the lineage graph.
//
// Best-effort: columns it cannot trace (opaque async lookups, window bounds,
// unrecognised node shapes) are omitted; it never throws on a plan shape it
// does not fully understand.

#include <string>

namespace clink::sql {

class LogicalSink;

// Returns the column-lineage carrier JSON for `sink` keyed by `sink_op_id`,
// or an empty string when nothing traceable was captured.
std::string capture_column_lineage(const LogicalSink& sink, const std::string& sink_op_id);

}  // namespace clink::sql
