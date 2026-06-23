#include "clink/sql/optimizer.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <set>
#include <string>

#include "clink/config/json.hpp"
#include "clink/metrics/sql_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/sql/join_reorder.hpp"
#include "clink/sql/logical_plan.hpp"

namespace clink::sql {

namespace {

// Test seam: when set, optimize() throws before running any pass, to exercise
// the optimizer's exception guard. Never set in production. (See
// optimizer.hpp::detail::set_optimize_force_throw.)
bool g_optimize_force_throw = false;

// Walk a JSON expression tree (predicate or value) and collect every
// {"col": "<name>"} reference. Used to compute the set of source columns the
// chain actually depends on. Recurses into EVERY nested object value and array
// element, not just arg/args: this catches CASE (branches[].when/.then, else)
// and is forward-safe against new binder IR shapes. Over-collecting is harmless
// (a slightly wider source read); under-collecting drops a needed column once
// the projection hint is armed, so we bias to over-collect.
void collect_columns(const clink::config::JsonValue& expr, std::set<std::string>& out) {
    if (expr.is_array()) {
        for (const auto& e : expr.as_array()) {
            collect_columns(e, out);
        }
        return;
    }
    if (!expr.is_object()) {
        return;
    }
    const auto& obj = expr.as_object();
    if (auto it = obj.find("col"); it != obj.end() && it->second.is_string()) {
        out.insert(it->second.as_string());
    }
    for (const auto& [key, value] : obj) {
        if (key == "col") {
            continue;  // a leaf column name, already captured above
        }
        collect_columns(value, out);
    }
}

// Run projection pushdown over a chain whose source is a single
// LogicalScan. The caller hands us the union of columns referenced
// by every node above the scan; we set the scan's projection hint
// to that set (preserving the source-schema declaration order so
// the hint is deterministic).
void set_scan_projection(LogicalScan& scan, const std::set<std::string>& used) {
    if (used.empty())
        return;
    std::set<std::string> keep = used;
    // The physical planner injects an assign_timestamps_row op that reads the
    // table's event_time_column on EVERY scan that declares one, independent of
    // the query (a window is not required). That op is invisible to the logical
    // analysis, so union the event-time column in here, or arming the hint would
    // silently starve watermark assignment. This is the single owner of that
    // invariant (the source never sees the property - build_params strips it).
    if (auto it = scan.table().properties.find("event_time_column");
        it != scan.table().properties.end() && !it->second.empty()) {
        keep.insert(it->second);
    }
    std::vector<std::string> ordered;
    for (const auto& col : scan.table().columns) {
        if (keep.count(col.name) > 0)
            ordered.push_back(col.name);
    }
    scan.set_projected_columns(std::move(ordered));
}

// Bottom-up traversal accumulating used-column sets as we cross each
// node. Returns the set of columns the input of `node` must produce.
// Phase 6.3 first cut: single-source chains only - we walk through
// Filter / Project / WindowAggregate / Sink and recurse into the
// child. IntervalJoin opts out (it conceptually projects both sides
// in full); we just don't push through it.
std::set<std::string> pushdown_(LogicalPlan& node) {
    std::set<std::string> used;
    if (node.kind() == "Scan") {
        return used;  // Sentinel; caller decides whether to annotate.
    }
    if (node.kind() == "Filter") {
        auto& f = static_cast<LogicalFilter&>(node);
        try {
            auto pred = clink::config::parse(f.predicate_json());
            collect_columns(pred, used);
        } catch (...) {
            // Malformed predicate - shouldn't happen, but bail safely.
        }
        // Filter passes everything else through to the child unchanged,
        // so add the caller's needs by union'ing with our child's
        // requirements. Iteration order: we want the downstream needs
        // too, computed by the caller before invoking pushdown_ - so
        // this function returns "what THIS node uses from its input".
        // The driver function unions across levels.
        return used;
    }
    if (node.kind() == "Project") {
        auto& p = static_cast<LogicalProject&>(node);
        for (const auto& out : p.outputs()) {
            try {
                auto expr = clink::config::parse(out.expr_json);
                collect_columns(expr, used);
            } catch (...) {
            }
        }
        return used;
    }
    if (node.kind() == "WindowAggregate") {
        auto& wa = static_cast<LogicalWindowAggregate&>(node);
        used.insert(wa.window().time_column);
        for (const auto& gk : wa.group_keys())
            used.insert(gk);
        for (const auto& agg : wa.aggregates()) {
            if (!agg.input_column.empty())
                used.insert(agg.input_column);
        }
        return used;
    }
    if (node.kind() == "Aggregate") {
        auto& wa = static_cast<LogicalAggregate&>(node);
        for (const auto& gk : wa.group_keys())
            used.insert(gk);
        for (const auto& agg : wa.aggregates()) {
            if (!agg.input_column.empty())
                used.insert(agg.input_column);
        }
        return used;
    }
    return used;  // unknown kinds: stay conservative
}

// Driver: walks the chain (sink -> ... -> scan), unioning each
// level's local column-use set. When it reaches the scan, sets the
// projected_columns hint.
void apply_projection_pushdown(LogicalPlan& node, std::set<std::string>& cumulative) {
    auto local = pushdown_(node);
    for (const auto& c : local)
        cumulative.insert(c);

    auto inputs = node.inputs();
    if (inputs.empty())
        return;
    // Phase 6.3: handle single-input chains. Multi-input nodes
    // (interval join) propagate cumulative-as-is to each side, but
    // we only annotate one Scan in a single-source chain.
    if (inputs.size() == 1) {
        // We need a mutable pointer to the child to set its
        // projected_columns; LogicalPlan::inputs() returns const
        // pointers, but the parent owns the unique_ptr. Reach into
        // the concrete parent type to grab the mutable child.
        LogicalPlan* mutable_child = nullptr;
        if (node.kind() == "Sink") {
            mutable_child = const_cast<LogicalPlan*>(&static_cast<LogicalSink&>(node).input());
        } else if (node.kind() == "Project") {
            mutable_child = const_cast<LogicalPlan*>(&static_cast<LogicalProject&>(node).input());
        } else if (node.kind() == "Filter") {
            mutable_child = const_cast<LogicalPlan*>(&static_cast<LogicalFilter&>(node).input());
        } else if (node.kind() == "WindowAggregate") {
            mutable_child =
                const_cast<LogicalPlan*>(&static_cast<LogicalWindowAggregate&>(node).input());
        } else if (node.kind() == "Aggregate") {
            mutable_child = const_cast<LogicalPlan*>(&static_cast<LogicalAggregate&>(node).input());
        }
        if (mutable_child != nullptr) {
            if (mutable_child->kind() == "Scan") {
                set_scan_projection(static_cast<LogicalScan&>(*mutable_child), cumulative);
            } else {
                apply_projection_pushdown(*mutable_child, cumulative);
            }
        }
    }
    // Multi-input: projection pushdown through stream-stream joins (SQLOPT-4).
    // The join output is flat "<alias>_<col>"; translate the cumulative needs
    // into each side's raw columns and recurse, retaining the join's own key /
    // timestamp columns. Skip the whole join (both scans full read) if there is
    // nothing referenced above (empty cumulative) or any column does not
    // de-alias to exactly one side - never under-include. Lookup joins are
    // single-input (handled above and left full because the probe key column is
    // not exposed on the node); only equi / interval are narrowed here.
    if ((node.kind() == "EquiJoin" || node.kind() == "IntervalJoin") && !cumulative.empty()) {
        auto schema_cols = [](const LogicalPlan& p) {
            std::set<std::string> s;
            if (auto sch = p.schema()) {
                for (const auto& f : sch->fields()) {
                    s.insert(f->name());
                }
            }
            return s;
        };
        std::string la;
        std::string ra;
        std::set<std::string> lcols;
        std::set<std::string> rcols;
        std::vector<std::string> lextra;
        std::vector<std::string> rextra;
        LogicalPlan* lc = nullptr;
        LogicalPlan* rc = nullptr;
        if (node.kind() == "EquiJoin") {
            auto& j = static_cast<LogicalEquiJoin&>(node);
            la = j.left_alias();
            ra = j.right_alias();
            lcols = schema_cols(j.left());
            rcols = schema_cols(j.right());
            lextra = {j.left_key_column()};
            rextra = {j.right_key_column()};
            lc = j.left_mut().get();
            rc = j.right_mut().get();
        } else {
            auto& j = static_cast<LogicalIntervalJoin&>(node);
            la = j.left_alias();
            ra = j.right_alias();
            lcols = schema_cols(j.left());
            rcols = schema_cols(j.right());
            lextra = {j.left_key_column(), j.left_ts_column()};
            rextra = {j.right_key_column(), j.right_ts_column()};
            lc = j.left_mut().get();
            rc = j.right_mut().get();
        }
        const std::string lp = la + "_";
        const std::string rp = ra + "_";
        std::set<std::string> lneed;
        std::set<std::string> rneed;
        bool ok = true;
        for (const auto& c : cumulative) {
            const bool il = c.size() > lp.size() && c.compare(0, lp.size(), lp) == 0 &&
                            lcols.count(c.substr(lp.size())) > 0;
            const bool ir = c.size() > rp.size() && c.compare(0, rp.size(), rp) == 0 &&
                            rcols.count(c.substr(rp.size())) > 0;
            if (il && ir) {  // ambiguous alias-prefix overlap
                ok = false;
                break;
            }
            if (il) {
                lneed.insert(c.substr(lp.size()));
            } else if (ir) {
                rneed.insert(c.substr(rp.size()));
            } else {  // a column that is neither side (computed / unknown)
                ok = false;
                break;
            }
        }
        if (ok) {
            for (const auto& k : lextra) {
                lneed.insert(k);
            }
            for (const auto& k : rextra) {
                rneed.insert(k);
            }
            auto recurse_child = [&](LogicalPlan* child, std::set<std::string> needed) {
                if (child == nullptr) {
                    return;
                }
                if (child->kind() == "Scan") {
                    set_scan_projection(static_cast<LogicalScan&>(*child), needed);
                } else {
                    apply_projection_pushdown(*child, needed);
                }
            };
            recurse_child(lc, std::move(lneed));
            recurse_child(rc, std::move(rneed));
        }
    }
}

// ---- predicate pushdown through INNER equi-joins (SQLOPT-4) ----
//
// A top-level join binds as Project -> Filter -> EquiJoin(scanL, scanR) with the
// outer WHERE in the Filter over the flat "<alias>_<col>" join output. For an
// INNER join (both sides preserved, no null-padding), a conjunct referencing
// only one side is safe to evaluate on that side BEFORE the join, shrinking the
// join's buffered state and output. We relocate such conjuncts into a
// LogicalFilter on the matching scan and leave the rest (cross-side, ambiguous,
// constant) in the original Filter (an empty residual is the vacuously-true
// `and`, a harmless pass-through). OUTER joins are deliberately untouched: a
// predicate on the null-padded side must run AFTER padding, so pushing it would
// change results.

// Flatten a predicate's top-level AND into its conjuncts (nested ANDs folded).
std::vector<clink::config::JsonValue> flatten_and(const clink::config::JsonValue& pred) {
    std::vector<clink::config::JsonValue> out;
    if (pred.is_object()) {
        const auto& o = pred.as_object();
        auto op = o.find("op");
        auto args = o.find("args");
        if (op != o.end() && op->second.is_string() && op->second.as_string() == "and" &&
            args != o.end() && args->second.is_array()) {
            for (const auto& sub : args->second.as_array()) {
                auto inner = flatten_and(sub);
                for (auto& x : inner) {
                    out.push_back(std::move(x));
                }
            }
            return out;
        }
    }
    out.push_back(pred);
    return out;
}

// Combine conjuncts back into one predicate. Empty -> a vacuously-true `and`.
clink::config::JsonValue make_and(std::vector<clink::config::JsonValue> conjuncts) {
    if (conjuncts.size() == 1) {
        return std::move(conjuncts[0]);
    }
    clink::config::JsonObject obj;
    obj["op"] = clink::config::JsonValue{std::string{"and"}};
    clink::config::JsonArray args;
    args.reserve(conjuncts.size());
    for (auto& c : conjuncts) {
        args.push_back(std::move(c));
    }
    obj["args"] = clink::config::JsonValue{std::move(args)};
    return clink::config::JsonValue{std::move(obj)};
}

// Return a copy of a predicate subtree with `prefix` stripped from every {"col"}
// whose value starts with it. Used to rewrite a single-side conjunct from the
// join's flat "<alias>_<col>" names to the raw "<col>" the side scan exposes.
clink::config::JsonValue rewrite_cols(const clink::config::JsonValue& node,
                                      const std::string& prefix) {
    if (!node.is_object()) {
        return node;
    }
    clink::config::JsonObject out;
    for (const auto& [k, v] : node.as_object()) {
        if (k == "col" && v.is_string()) {
            const auto& s = v.as_string();
            out[k] = (s.size() > prefix.size() && s.compare(0, prefix.size(), prefix) == 0)
                         ? clink::config::JsonValue{s.substr(prefix.size())}
                         : v;
        } else if (k == "arg") {
            out[k] = rewrite_cols(v, prefix);
        } else if (k == "args" && v.is_array()) {
            clink::config::JsonArray arr;
            arr.reserve(v.as_array().size());
            for (const auto& sub : v.as_array()) {
                arr.push_back(rewrite_cols(sub, prefix));
            }
            out[k] = clink::config::JsonValue{std::move(arr)};
        } else {
            out[k] = v;
        }
    }
    return clink::config::JsonValue{std::move(out)};
}

// Classify a conjunct: 0 = all columns are the left side, 1 = all the right
// side, -1 = keep above (constant, cross-side, references neither side, or an
// ambiguous alias-prefix overlap). A column is "side X" only when it spells
// "<X_alias>_<rawcol>" AND rawcol is an actual column of side X (disambiguates
// an alias that is a prefix of the other alias).
int conjunct_side(const clink::config::JsonValue& conj,
                  const std::string& left_alias,
                  const std::set<std::string>& left_cols,
                  const std::string& right_alias,
                  const std::set<std::string>& right_cols) {
    std::set<std::string> cols;
    collect_columns(conj, cols);
    if (cols.empty()) {
        return -1;
    }
    const std::string lp = left_alias + "_";
    const std::string rp = right_alias + "_";
    bool any_left = false;
    bool any_right = false;
    for (const auto& c : cols) {
        const bool is_left = c.size() > lp.size() && c.compare(0, lp.size(), lp) == 0 &&
                             left_cols.count(c.substr(lp.size())) > 0;
        const bool is_right = c.size() > rp.size() && c.compare(0, rp.size(), rp) == 0 &&
                              right_cols.count(c.substr(rp.size())) > 0;
        if (is_left && is_right) {
            return -1;  // ambiguous overlap: keep above
        }
        if (is_left) {
            any_left = true;
        } else if (is_right) {
            any_right = true;
        } else {
            return -1;  // matches neither side: keep above
        }
    }
    if (any_left && !any_right) {
        return 0;
    }
    if (any_right && !any_left) {
        return 1;
    }
    return -1;  // mixed: keep above
}

// One pushable side of a join: its alias, the raw columns it owns, and the
// mutable child slot to wrap in a Filter (nullptr = not pushable: the dim side
// of a lookup join, or a non-scan child).
struct PushSide {
    std::string alias;
    std::set<std::string> cols;
    std::unique_ptr<LogicalPlan>* slot;
};

std::set<std::string> scan_cols(const LogicalPlan& p) {
    std::set<std::string> s;
    if (p.kind() == "Scan") {
        for (const auto& c : static_cast<const LogicalScan&>(p).table().columns) {
            s.insert(c.name);
        }
    }
    return s;
}

// Split f's conjuncts: each single-side conjunct (whose side has a pushable
// slot) is de-aliased and relocated into a Filter on that child; the rest stay
// in f (empty residual => vacuously-true empty `and` pass-through).
void push_conjuncts(LogicalFilter& f, const PushSide& left, const PushSide& right) {
    clink::config::JsonValue pred;
    try {
        pred = clink::config::parse(f.predicate_json());
    } catch (...) {
        return;  // malformed predicate: leave as-is
    }
    auto conjuncts = flatten_and(pred);

    std::vector<clink::config::JsonValue> left_push;
    std::vector<clink::config::JsonValue> right_push;
    std::vector<clink::config::JsonValue> residual;
    for (auto& c : conjuncts) {
        const int side = conjunct_side(c, left.alias, left.cols, right.alias, right.cols);
        if (side == 0 && left.slot != nullptr) {
            left_push.push_back(rewrite_cols(c, left.alias + "_"));
        } else if (side == 1 && right.slot != nullptr) {
            right_push.push_back(rewrite_cols(c, right.alias + "_"));
        } else {
            residual.push_back(std::move(c));
        }
    }
    if (left_push.empty() && right_push.empty()) {
        return;  // nothing single-sided to push
    }
    // Serialise every new predicate string BEFORE mutating any slot, so a throw
    // here (serialize/make_and) cannot leave a slot moved-out (null child). Once
    // the strings exist, the only remaining steps are the unique_ptr moves +
    // LogicalFilter construction over an already-valid child. The residual is
    // updated last; should set_predicate_json throw after a side was wrapped, the
    // pushed conjuncts simply remain in `f` too (an AND filter is idempotent, so
    // the plan stays correct, just briefly redundant).
    std::string left_pred =
        left_push.empty() ? std::string{} : make_and(std::move(left_push)).serialize(0);
    std::string right_pred =
        right_push.empty() ? std::string{} : make_and(std::move(right_push)).serialize(0);
    std::string residual_pred = make_and(std::move(residual)).serialize(0);
    if (!left_pred.empty()) {
        *left.slot = std::make_unique<LogicalFilter>(std::move(*left.slot), std::move(left_pred));
    }
    if (!right_pred.empty()) {
        *right.slot =
            std::make_unique<LogicalFilter>(std::move(*right.slot), std::move(right_pred));
    }
    f.set_predicate_json(std::move(residual_pred));
}

// Walk the plan; for a Filter directly over a join, push its single-side
// conjuncts into the join's scan children. EQUI / INTERVAL: INNER only (an
// OUTER null-padded-side predicate must run after the join), both base-table
// scans pushable. LOOKUP: only the probe (preserved) side is a scan and is
// always safe to push (even for a LEFT lookup); the dim is an async function.
void push_predicates(LogicalPlan& node) {
    if (node.kind() == "Filter") {
        auto& f = static_cast<LogicalFilter&>(node);
        const auto in_kind = f.input().kind();
        if (in_kind == "EquiJoin") {
            auto& j = const_cast<LogicalEquiJoin&>(static_cast<const LogicalEquiJoin&>(f.input()));
            if (j.join_type() == JoinType::Inner) {
                PushSide l{j.left_alias(),
                           scan_cols(j.left()),
                           j.left().kind() == "Scan" ? &j.left_mut() : nullptr};
                PushSide r{j.right_alias(),
                           scan_cols(j.right()),
                           j.right().kind() == "Scan" ? &j.right_mut() : nullptr};
                push_conjuncts(f, l, r);
            }
        } else if (in_kind == "IntervalJoin") {
            auto& j = const_cast<LogicalIntervalJoin&>(
                static_cast<const LogicalIntervalJoin&>(f.input()));
            if (j.join_type() == JoinType::Inner) {
                PushSide l{j.left_alias(),
                           scan_cols(j.left()),
                           j.left().kind() == "Scan" ? &j.left_mut() : nullptr};
                PushSide r{j.right_alias(),
                           scan_cols(j.right()),
                           j.right().kind() == "Scan" ? &j.right_mut() : nullptr};
                push_conjuncts(f, l, r);
            }
        } else if (in_kind == "LookupJoin") {
            auto& j =
                const_cast<LogicalLookupJoin&>(static_cast<const LogicalLookupJoin&>(f.input()));
            PushSide probe{
                j.probe_alias(),
                std::set<std::string>(j.probe_columns().begin(), j.probe_columns().end()),
                j.input().kind() == "Scan" ? &j.input_mut() : nullptr};
            PushSide dim{j.dim_alias(),
                         std::set<std::string>(j.dim_columns().begin(), j.dim_columns().end()),
                         nullptr};  // dim is an async function: never pushable
            push_conjuncts(f, probe, dim);
        }
    }
    for (const auto* in : node.inputs()) {
        if (in != nullptr) {
            push_predicates(const_cast<LogicalPlan&>(*in));
        }
    }
}

}  // namespace

std::unique_ptr<LogicalPlan> optimize(std::unique_ptr<LogicalPlan> plan) {
    if (plan == nullptr)
        return plan;
    const auto t0 = std::chrono::steady_clock::now();
    // The optimizer is a sound, semantics-preserving rewrite, so a throw from any
    // pass is a planner BUG, not a user error. Rather than let it escape and fail
    // an otherwise-valid query, catch it and fall back to the best plan we have.
    //
    // For a LOGIC throw (the realistic planner-bug class) every pass leaves a
    // VALID, if less optimised, plan: push_predicates serialises new predicates
    // before mutating any slot (an AND filter is idempotent, so a partial push is
    // correct), reorder_joins decides reorderability non-destructively and its
    // rebuild is logic-throw-free (analyze verifies every edge), and
    // apply_projection_pushdown only adds optional column hints. A caught pass
    // simply does not run the passes after it, and we run the resulting plan.
    //
    // The one case that does NOT leave a valid plan is std::bad_alloc (OOM)
    // raised mid-reorder, after reorder_subtree has moved a join's leaves out and
    // before it reassigns the slot: the slot is left null. We still catch it here
    // and return; the null child is then rejected by the physical planner's
    // require_no_null_children backstop as a clean TranslationError. So the
    // guarantee is: a planner-pass throw never crashes the process and never
    // yields a silently-wrong plan - it either falls back to a valid plan or
    // fails compilation cleanly (only under genuine OOM).
    try {
        if (g_optimize_force_throw) {
            throw std::runtime_error("optimize: forced throw (test seam)");
        }
        // Predicate pushdown first (it wraps INNER-equi-join scan children in
        // Filters, which the reorderer's per-relation cardinality then reflects),
        // then cost-based join reordering, then projection pushdown over the
        // resulting tree.
        push_predicates(*plan);
        reorder_joins(plan);
        std::set<std::string> cumulative;
        apply_projection_pushdown(*plan, cumulative);
    } catch (const std::exception& e) {
        clink::metrics::sql::optimize_failed();
        clink::log::warn("sql.optimize",
                         std::string("optimizer pass failed, running the query on the "
                                     "un-/partially-optimized plan: ") +
                             e.what());
        return plan;
    } catch (...) {
        clink::metrics::sql::optimize_failed();
        clink::log::warn("sql.optimize",
                         "optimizer pass failed with a non-standard exception, running the "
                         "query on the un-/partially-optimized plan");
        return plan;
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::sql::optimize_completed(static_cast<std::uint64_t>(dt));
    return plan;
}

namespace detail {
void set_optimize_force_throw(bool on) {
    g_optimize_force_throw = on;
}
}  // namespace detail

}  // namespace clink::sql
