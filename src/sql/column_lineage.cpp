#include "clink/sql/column_lineage.hpp"

#include <algorithm>
#include <unordered_set>

#include "clink/config/json.hpp"
#include "clink/lineage/lineage_graph.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"

namespace clink::sql {
namespace {

// Transformation strength, in increasing precedence. When a column flows
// through several nodes the strongest label wins (an aggregate over a
// computed expression is an AGGREGATION).
enum class Strength { Identity = 0, Transformation = 1, Aggregation = 2 };

std::string strength_name(Strength s) {
    switch (s) {
        case Strength::Aggregation:
            return "AGGREGATION";
        case Strength::Transformation:
            return "TRANSFORMATION";
        case Strength::Identity:
        default:
            return "IDENTITY";
    }
}

Strength strongest(Strength a, Strength b) {
    return static_cast<int>(a) >= static_cast<int>(b) ? a : b;
}

struct TraceResult {
    std::vector<lineage::ColumnInputField> sources;
    Strength strength{Strength::Identity};
};

// Collect every {"col": "<name>"} leaf from a lowered value-expression IR.
void collect_cols(const config::JsonValue& v, std::vector<std::string>& out) {
    if (v.is_object()) {
        const auto& obj = v.as_object();
        if (auto it = obj.find("col"); it != obj.end() && it->second.is_string()) {
            out.push_back(it->second.as_string());
        }
        for (const auto& [k, val] : obj) {
            if (k != "col") {
                collect_cols(val, out);
            }
        }
    } else if (v.is_array()) {
        for (const auto& e : v.as_array()) {
            collect_cols(e, out);
        }
    }
}

// True when the expression is a bare column reference ({"col": ...} only),
// i.e. an identity passthrough rather than a computation.
bool is_identity_expr(const config::JsonValue& v) {
    return v.is_object() && v.contains("col") && !v.contains("op");
}

// Index of a field by name in an Arrow schema, or -1.
int field_index(const std::shared_ptr<arrow::Schema>& schema, const std::string& name) {
    if (!schema) {
        return -1;
    }
    return schema->GetFieldIndex(name);  // -1 when absent
}

// Source dataset identity for a base-table scan, computed the SAME way the
// lineage normaliser derives it from the lowered source op, so the captured
// input refs match the source vertices.
lineage::LineageDataset scan_dataset(const TableDef& table) {
    auto it = table.properties.find("connector");
    if (it == table.properties.end() || it->second.empty()) {
        lineage::LineageDataset d;
        d.name = table.name;
        return d;
    }
    return lineage::dataset_from_family(it->second, table.properties, {});
}

class Tracer {
public:
    // Trace one output column of `node` back to the source columns it derives
    // from. Recursion bottoms out at LogicalScan (a real source column).
    TraceResult trace(const LogicalPlan& node, const std::string& col, int depth = 0) {
        TraceResult r;
        if (depth > kMaxDepth) {
            return r;  // pathological plan; give up rather than recurse forever
        }
        const std::string k = node.kind();

        if (k == "Scan") {
            const auto& scan = static_cast<const LogicalScan&>(node);
            const auto ds = scan_dataset(scan.table());
            r.sources.push_back({ds.ns, ds.name, col});
            return r;
        }
        if (k == "Filter") {
            return trace(static_cast<const LogicalFilter&>(node).input(), col, depth + 1);
        }
        if (k == "Project") {
            return trace_project(static_cast<const LogicalProject&>(node), col, depth);
        }
        if (k == "Aggregate") {
            const auto& a = static_cast<const LogicalAggregate&>(node);
            return trace_aggregate(node,
                                   a.input(),
                                   a.group_keys(),
                                   a.key_output_names(),
                                   a.aggregates(),
                                   {},
                                   {},
                                   col,
                                   depth);
        }
        if (k == "WindowAggregate") {
            const auto& a = static_cast<const LogicalWindowAggregate&>(node);
            return trace_aggregate(node,
                                   a.input(),
                                   a.group_keys(),
                                   a.key_output_names(),
                                   a.aggregates(),
                                   a.window_start_output(),
                                   a.window_end_output(),
                                   col,
                                   depth);
        }
        if (k == "EquiJoin") {
            const auto& j = static_cast<const LogicalEquiJoin&>(node);
            return trace_join(node, j.left(), j.right(), col, depth);
        }
        if (k == "IntervalJoin") {
            const auto& j = static_cast<const LogicalIntervalJoin&>(node);
            return trace_join(node, j.left(), j.right(), col, depth);
        }
        if (k == "LookupJoin") {
            // Probe columns map positionally to the probe input; dimension
            // columns come from an opaque async function and are not traceable.
            const auto& lj = static_cast<const LogicalLookupJoin&>(node);
            const int idx = field_index(node.schema(), col);
            const int probe_n = lj.input().schema() ? lj.input().schema()->num_fields() : 0;
            if (idx >= 0 && idx < probe_n) {
                return trace(lj.input(), lj.input().schema()->field(idx)->name(), depth + 1);
            }
            return r;  // dim column: opaque
        }

        // Default: single-input passthrough (Distinct, Limit, TopN, RowNumber,
        // OverAggregate, ...). If the column is present unchanged in the sole
        // input's schema, follow it; otherwise it is node-synthesised.
        const auto ins = node.inputs();
        if (ins.size() == 1 && ins[0] != nullptr && field_index(ins[0]->schema(), col) >= 0) {
            return trace(*ins[0], col, depth + 1);
        }
        return r;
    }

private:
    static constexpr int kMaxDepth = 256;

    TraceResult trace_project(const LogicalProject& p, const std::string& col, int depth) {
        TraceResult r;
        const ProjectOutput* out = nullptr;
        for (const auto& o : p.outputs()) {
            if (o.name == col) {
                out = &o;
                break;
            }
        }
        if (out == nullptr) {
            return r;  // not produced here (shouldn't happen for a valid plan)
        }
        config::JsonValue expr;
        try {
            expr = config::parse(out->expr_json);
        } catch (...) {
            return r;
        }
        r.strength = is_identity_expr(expr) ? Strength::Identity : Strength::Transformation;
        std::vector<std::string> cols;
        collect_cols(expr, cols);
        for (const auto& c : cols) {
            const auto sub = trace(p.input(), c, depth + 1);
            r.strength = strongest(r.strength, sub.strength);
            for (const auto& s : sub.sources) {
                r.sources.push_back(s);
            }
        }
        return r;
    }

    TraceResult trace_aggregate(const LogicalPlan& node,
                                const LogicalPlan& input,
                                const std::vector<std::string>& group_keys,
                                const std::vector<std::string>& key_output_names,
                                const std::vector<AggregateOutput>& aggregates,
                                const std::string& window_start_output,
                                const std::string& window_end_output,
                                const std::string& col,
                                int depth) {
        TraceResult r;
        if (col == window_start_output || col == window_end_output) {
            return r;  // synthetic window bound: no upstream source
        }
        // Group key (output name honours a SELECT alias via key_output_names).
        for (std::size_t i = 0; i < group_keys.size(); ++i) {
            const std::string& out_name =
                (i < key_output_names.size() && !key_output_names[i].empty()) ? key_output_names[i]
                                                                              : group_keys[i];
            if (out_name == col) {
                return trace(input, group_keys[i], depth + 1);
            }
        }
        // Aggregate function output.
        for (const auto& agg : aggregates) {
            if (agg.output_name == col) {
                if (!agg.input_column.empty()) {
                    r = trace(input, agg.input_column, depth + 1);
                }
                r.strength = Strength::Aggregation;  // COUNT(*) -> no source, still AGGREGATION
                return r;
            }
        }
        (void)node;
        return r;
    }

    TraceResult trace_join(const LogicalPlan& node,
                           const LogicalPlan& left,
                           const LogicalPlan& right,
                           const std::string& col,
                           int depth) {
        // Output schema is [left columns..., right columns...] in order, so map
        // the output ordinal back to the owning child's column by position.
        const int idx = field_index(node.schema(), col);
        if (idx < 0) {
            return {};
        }
        const int left_n = left.schema() ? left.schema()->num_fields() : 0;
        if (idx < left_n) {
            return trace(left, left.schema()->field(idx)->name(), depth + 1);
        }
        if (right.schema() != nullptr && (idx - left_n) < right.schema()->num_fields()) {
            return trace(right, right.schema()->field(idx - left_n)->name(), depth + 1);
        }
        return {};
    }
};

}  // namespace

std::string capture_column_lineage(const LogicalSink& sink, const std::string& sink_op_id) {
    try {
        const auto& sink_cols = sink.table().columns;
        const auto in_schema = sink.input().schema();
        if (!in_schema) {
            return {};
        }
        Tracer tracer;
        std::vector<lineage::ColumnLineageField> fields;
        // INSERT maps the child plan's output columns positionally to the sink
        // table columns.
        const std::size_t n = std::min<std::size_t>(
            sink_cols.size(), static_cast<std::size_t>(in_schema->num_fields()));
        for (std::size_t i = 0; i < n; ++i) {
            const std::string in_col = in_schema->field(static_cast<int>(i))->name();
            auto res = tracer.trace(sink.input(), in_col);

            // Dedup sources by (ns, name, field).
            std::vector<lineage::ColumnInputField> deduped;
            std::unordered_set<std::string> seen;
            for (const auto& s : res.sources) {
                const std::string key = s.ns + '\x1f' + s.name + '\x1f' + s.field;
                if (seen.insert(key).second) {
                    deduped.push_back(s);
                }
            }

            // Emit a field only when it carries useful information: a traced
            // source, or a recognised aggregation (e.g. COUNT(*)).
            if (deduped.empty() && res.strength != Strength::Aggregation) {
                continue;
            }
            lineage::ColumnLineageField f;
            f.output = sink_cols[i].name;
            f.transformation = strength_name(res.strength);
            f.inputs = std::move(deduped);
            fields.push_back(std::move(f));
        }

        if (fields.empty()) {
            return {};
        }
        // Carrier shape: {"<sink_op_id>": <field-array>}. Build it from the
        // shared field-array serialiser (the exact array shape extract_lineage
        // parses), wrapped in an object keyed by the sink op id.
        config::JsonObject obj;
        obj.emplace(sink_op_id, config::parse(lineage::column_lineage_to_json(fields)));
        return config::JsonValue(std::move(obj)).serialize();
    } catch (...) {
        return {};  // best-effort: never fail compilation on lineage
    }
}

}  // namespace clink::sql
