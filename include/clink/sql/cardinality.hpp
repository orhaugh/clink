#pragma once

// Cardinality / statistics estimation over clink's LogicalPlan (clink::sql).
//
// estimate_stats() walks a bound LogicalPlan bottom-up and returns the
// estimated RelStats (row count + per-output-column stats) of each node. It is
// RESULT-NEUTRAL - pure analysis, it never rewrites the plan - and is the
// foundation the cost-based join reorderer builds on: row_count is the
// cardinality the reorderer minimises and per-column NDV drives join-output
// cardinality (a proxy for streaming operator state).
//
// Estimation rules are textbook (Selinger selectivity defaults + the standard
// join-cardinality formula |L|*|R| / max(V(L.key), V(R.key))). Stats come from
// the source tables' declared WITH options (statistics.hpp); when a table
// declares none, a scan falls back to a default cardinality so the arithmetic
// is well-defined (the reorderer treats all-unknown inputs as "no signal" and
// leaves the order alone).

#include <algorithm>
#include <cmath>
#include <string>

#include "clink/config/json.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/statistics.hpp"

namespace clink::sql {

namespace cardinality_detail {

// Fallback cardinality for a scan whose table declares no row_count. Arbitrary
// but stable; relative comparisons (the only thing the reorderer needs) are
// meaningful only between tables that DO declare stats.
inline constexpr double kDefaultScanRows = 1'000'000.0;
// Selinger defaults.
inline constexpr double kEqSelectivity = 0.1;           // col = literal, NDV unknown
inline constexpr double kRangeSelectivity = 1.0 / 3.0;  // <, <=, >, >=
inline constexpr double kLikeSelectivity = 0.1;
inline constexpr double kDefaultSelectivity = 0.5;  // unrecognised predicate

// NDV to use for a join key / group key when known, else the relation's row
// count (the all-distinct worst case), else a default.
inline double ndv_or(const RelStats& r, const std::string& col) {
    const auto cs = r.column(col);
    if (cs.ndv_known()) {
        return cs.ndv;
    }
    return r.row_count_known() ? r.row_count : kDefaultScanRows;
}

// Estimate the selectivity of a json_predicate against a relation's stats.
inline double selectivity(const clink::config::JsonValue& pred, const RelStats& in) {
    if (!pred.is_object() || !pred.contains("op")) {
        return kDefaultSelectivity;
    }
    const auto& op = pred.at("op").as_string();
    if (op == "and") {
        double s = 1.0;
        for (const auto& sub : pred.at("args").as_array()) {
            s *= selectivity(sub, in);
        }
        return s;
    }
    if (op == "or") {
        double prod_not = 1.0;
        for (const auto& sub : pred.at("args").as_array()) {
            prod_not *= (1.0 - selectivity(sub, in));
        }
        return 1.0 - prod_not;
    }
    if (op == "not") {
        return 1.0 - selectivity(pred.at("arg"), in);
    }
    auto col_of = [&]() -> std::string {
        return pred.contains("col") ? pred.at("col").as_string() : std::string{};
    };
    if (op == "eq") {
        const auto cs = in.column(col_of());
        return cs.ndv_known() ? std::min(1.0, 1.0 / cs.ndv) : kEqSelectivity;
    }
    if (op == "ne") {
        const auto cs = in.column(col_of());
        return cs.ndv_known() ? std::max(0.0, 1.0 - 1.0 / cs.ndv) : (1.0 - kEqSelectivity);
    }
    if (op == "lt" || op == "le" || op == "gt" || op == "ge") {
        return kRangeSelectivity;
    }
    if (op == "is_null") {
        return in.column(col_of()).null_fraction;
    }
    if (op == "is_not_null") {
        return 1.0 - in.column(col_of()).null_fraction;
    }
    if (op == "like") {
        return kLikeSelectivity;
    }
    if (op == "in") {
        const auto cs = in.column(col_of());
        const double per = cs.ndv_known() ? (1.0 / cs.ndv) : kEqSelectivity;
        const double n = pred.contains("values")
                             ? static_cast<double>(pred.at("values").as_array().size())
                             : 1.0;
        return std::min(1.0, per * n);
    }
    return kDefaultSelectivity;
}

// Cap every column's NDV at `rows` (a column can't have more distinct values
// than there are rows).
inline void cap_ndv(RelStats& s, double rows) {
    for (auto& [_, cs] : s.columns) {
        if (cs.ndv_known() && cs.ndv > rows) {
            cs.ndv = rows;
        }
    }
}

}  // namespace cardinality_detail

inline RelStats estimate_stats(const LogicalPlan& node);

namespace cardinality_detail {

// Output stats of one join side, with columns renamed to the join's flat output
// names: a base-table side (non-empty alias) prefixes "<alias>_<col>"; a
// sub-join side (empty alias) passes its already-flat names through. Mirrors
// EquiJoinRowOp::build_.
inline void merge_join_side(RelStats& out, const RelStats& side, const std::string& alias) {
    for (const auto& [name, cs] : side.columns) {
        out.columns[alias.empty() ? name : alias + "_" + name] = cs;
    }
}

}  // namespace cardinality_detail

// Estimate the output RelStats of a LogicalPlan node, bottom-up.
inline RelStats estimate_stats(const LogicalPlan& node) {
    using namespace cardinality_detail;
    const auto kind = node.kind();

    if (kind == "Scan") {
        const auto& scan = static_cast<const LogicalScan&>(node);
        RelStats s = table_stats_from(scan.table());
        if (!s.row_count_known()) {
            s.row_count = kDefaultScanRows;
        }
        cap_ndv(s, s.row_count);
        return s;
    }
    if (kind == "Filter") {
        const auto& f = static_cast<const LogicalFilter&>(node);
        RelStats in = estimate_stats(f.input());
        double sel = kDefaultSelectivity;
        try {
            sel = selectivity(clink::config::parse(f.predicate_json()), in);
        } catch (...) {
            sel = kDefaultSelectivity;
        }
        sel = std::clamp(sel, 0.0, 1.0);
        RelStats out = in;
        out.row_count = in.row_count * sel;
        cap_ndv(out, out.row_count);
        return out;
    }
    if (kind == "Project") {
        const auto& p = static_cast<const LogicalProject&>(node);
        RelStats in = estimate_stats(p.input());
        RelStats out;
        out.row_count = in.row_count;
        for (const auto& o : p.outputs()) {
            try {
                auto expr = clink::config::parse(o.expr_json);
                if (expr.is_object() && expr.contains("col")) {
                    out.columns[o.name] = in.column(expr.at("col").as_string());
                }
            } catch (...) {
                // computed / unparseable -> unknown stats for this output
            }
        }
        return out;
    }
    if (kind == "EquiJoin" || kind == "IntervalJoin") {
        // IntervalJoin shares the equi-key cardinality model (the band predicate
        // only narrows it further; treated as the equi case for v1). The two are
        // unrelated final classes with identical key/alias accessors, so resolve
        // the endpoints through the correct concrete type (never cross-cast) and
        // feed the shared formula. Casting an IntervalJoin via LogicalEquiJoin&
        // would be undefined behaviour.
        auto join_stats = [&](const LogicalPlan& left,
                              const LogicalPlan& right,
                              const std::string& l_key,
                              const std::string& r_key,
                              const std::string& l_alias,
                              const std::string& r_alias) {
            RelStats l = estimate_stats(left);
            RelStats r = estimate_stats(right);
            const double lk = ndv_or(l, l_key);
            const double rk = ndv_or(r, r_key);
            double divisor = std::max(lk, rk);
            if (divisor <= 0.0) {
                divisor = std::max({l.row_count, r.row_count, 1.0});
            }
            RelStats out;
            out.row_count = (l.row_count * r.row_count) / divisor;
            merge_join_side(out, l, l_alias);
            merge_join_side(out, r, r_alias);
            cap_ndv(out, out.row_count);
            return out;
        };
        if (kind == "EquiJoin") {
            const auto& j = static_cast<const LogicalEquiJoin&>(node);
            return join_stats(j.left(),
                              j.right(),
                              j.left_key_column(),
                              j.right_key_column(),
                              j.left_alias(),
                              j.right_alias());
        }
        const auto& j = static_cast<const LogicalIntervalJoin&>(node);
        return join_stats(j.left(),
                          j.right(),
                          j.left_key_column(),
                          j.right_key_column(),
                          j.left_alias(),
                          j.right_alias());
    }
    if (kind == "Aggregate" || kind == "WindowAggregate") {
        const auto& a = static_cast<const LogicalAggregate&>(node);
        RelStats in = estimate_stats(a.input());
        RelStats out;
        if (a.group_keys().empty()) {
            out.row_count = 1.0;
        } else {
            double groups = 1.0;
            for (const auto& k : a.group_keys()) {
                groups *= ndv_or(in, k);
            }
            out.row_count = std::min(groups, in.row_count_known() ? in.row_count : groups);
        }
        // Group-key output columns keep their NDV (capped at the group count);
        // aggregate outputs have unknown stats.
        const auto& outs = a.key_output_names().empty() ? a.group_keys() : a.key_output_names();
        for (std::size_t i = 0; i < a.group_keys().size() && i < outs.size(); ++i) {
            out.columns[outs[i]] = in.column(a.group_keys()[i]);
        }
        cap_ndv(out, out.row_count);
        return out;
    }

    // Generic fallback: single-input nodes pass their input's stats through;
    // binary set nodes sum row counts and merge columns; a leaf is unknown.
    const auto ins = node.inputs();
    if (ins.size() == 1 && ins[0] != nullptr) {
        return estimate_stats(*ins[0]);
    }
    if (ins.size() == 2 && ins[0] != nullptr && ins[1] != nullptr) {
        RelStats a = estimate_stats(*ins[0]);
        RelStats b = estimate_stats(*ins[1]);
        RelStats out;
        out.row_count = a.row_count + b.row_count;
        out.columns = a.columns;
        for (const auto& [n, cs] : b.columns) {
            out.columns.try_emplace(n, cs);
        }
        return out;
    }
    return RelStats{};
}

// Convenience: estimated output row count of a plan node.
inline double estimate_rows(const LogicalPlan& node) {
    return estimate_stats(node).row_count;
}

}  // namespace clink::sql
