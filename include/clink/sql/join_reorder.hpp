#pragma once

// Cost-based join reordering over clink's LogicalPlan (clink::sql).
//
// Reorders a connected tree of INNER equi-joins to minimise total intermediate
// cardinality, driven by the cardinality estimator (cardinality.hpp). Greedy
// "minimum-intermediate-cardinality" ordering (start from the smallest
// relation, repeatedly add the connected relation that yields the smallest
// intermediate join), left-deep. The reorder is applied ONLY when its estimated
// total cost is strictly lower than the original order's, so it can never make a
// plan worse per the estimates; with no declared statistics every relation looks
// identical and the order is left alone.
//
// CORRECTNESS: INNER joins are commutative + associative, and reconstruction
// re-derives every join's keys/aliases/schema (the same flat "<alias>_<col>"
// naming bind_join_rel uses), so the reordered tree produces the same rows and
// the same set of output columns (clink Rows are name-keyed, so column order is
// irrelevant). Only pure INNER-equi-join subtrees over single-alias leaves are
// reordered; any outer / interval / lookup / semi join makes the subtree opaque
// and it is left as-is (still descended into for nested INNER subtrees).
//
// A sub-join's stored edge key is the flat output name of one of its leaves; it
// is resolved through an EXACT flat-name -> (alias, col) map built from the real
// leaf columns, never by re-parsing the flat string (which is not uniquely
// invertible when one alias is a prefix of another, e.g. `s` vs `s_t`). If any
// flat name is ambiguous or unresolvable the subtree is left un-reordered. The
// whole decision (reorderable? cheaper?) is made non-destructively before any
// subplan is moved, so a subtree that is not rewritten is a TRUE no-op: it is
// neither reshaped nor at risk of a half-applied rewrite.
//
// Hand-rolled on clink's own IR (see project memory: no external optimizer fits
// a JVM-free Arrow-native streaming engine). Algorithm borrowed from DuckDB's
// join-order optimizer + the Selinger/Moerkotte literature.

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>

#include "clink/sql/cardinality.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/statistics.hpp"

namespace clink::sql {

namespace reorder_detail {

// A base relation (a single-alias leaf subplan) participating in a join graph.
// `plan` is null during the non-destructive analyze/cost phase and is filled
// (by move_leaves) only when a reorder is actually applied.
struct JoinRel {
    std::unique_ptr<LogicalPlan> plan;
    std::string alias;
    std::vector<ColumnSpec> columns;  // the leaf's output (raw) columns
    RelStats stats;                   // the leaf's estimated stats, raw-named columns
};

// One equi-join edge between two relation aliases on their raw key columns.
struct JoinEdge {
    std::string a_alias, a_col;
    std::string b_alias, b_col;
};

inline bool is_inner_equi_join(const LogicalPlan& n) {
    return n.kind() == "EquiJoin" &&
           static_cast<const LogicalEquiJoin&>(n).join_type() == JoinType::Inner;
}

// Test seam: when set, reorder_subtree throws right after move_leaves has moved a
// join's leaves out (slot temporarily null), to exercise the mid-reorder throw
// window the optimize() guard + physical-planner null-child backstop must absorb.
// Never set in production.
inline bool& force_throw_after_move_flag() {
    static bool flag = false;
    return flag;
}

inline std::vector<ColumnSpec> columns_of(const LogicalPlan& n) {
    std::vector<ColumnSpec> cols;
    auto schema = n.schema();
    if (schema) {
        cols.reserve(static_cast<std::size_t>(schema->num_fields()));
        for (int i = 0; i < schema->num_fields(); ++i) {
            cols.push_back(ColumnSpec{schema->field(i)->name(), schema->field(i)->type()});
        }
    }
    return cols;
}

// Flat output column name a relation contributes to a join: "<alias>_<col>".
inline std::string flat_name(const std::string& alias, const std::string& col) {
    return alias + "_" + col;
}

// Non-destructively analyse the maximal INNER-equi-join subtree rooted at `node`
// (an INNER EquiJoin). Collects the base relations (alias + columns + stats, with
// a null plan), the equi-join edges, and an EXACT flat-name -> (alias, raw_col)
// origin map built from the leaves' real columns. A sub-join side's stored edge
// key is a flat "<alias>_<col>" name; it is resolved by looking it up in `origin`
// rather than re-parsing (the flat encoding is not uniquely invertible when one
// alias is a prefix of another, e.g. `s` vs `s_t`). Sets pure=false on any
// non-INNER join block, and unambiguous=false if two distinct leaf columns
// collapse to one flat name or a sub-join key cannot be resolved; either way the
// caller leaves the subtree un-reordered. No plans are moved.
inline void analyze(const LogicalPlan& node,
                    std::vector<JoinRel>& rels,
                    std::vector<JoinEdge>& edges,
                    std::map<std::string, std::pair<std::string, std::string>>& origin,
                    bool& pure,
                    bool& unambiguous) {
    const auto& j = static_cast<const LogicalEquiJoin&>(node);  // caller guarantees INNER EquiJoin
    const std::string l_alias = j.left_alias();
    const std::string r_alias = j.right_alias();
    const std::string l_key = j.left_key_column();
    const std::string r_key = j.right_key_column();

    // Process a side: recurse INNER joins, register single-alias leaves. Returns
    // true if the side is itself a sub-join (its edge endpoint is a flat name to
    // resolve via `origin`), false if it is a base leaf (endpoint is (alias,key)).
    auto handle_side = [&](const LogicalPlan& child, const std::string& alias) -> bool {
        if (is_inner_equi_join(child)) {
            analyze(child, rels, edges, origin, pure, unambiguous);
            return true;
        }
        const auto k = child.kind();
        if (k == "EquiJoin" || k == "IntervalJoin" || k == "LookupJoin" || k == "SemiJoin") {
            pure = false;  // opaque multi-relation block -> not reorderable
            return false;
        }
        // Single-alias leaf (Scan, or Filter/Project over a scan).
        std::vector<ColumnSpec> cols = columns_of(child);
        if (alias.empty()) {
            unambiguous = false;  // a leaf with no alias cannot be re-qualified
        }
        for (const auto& c : cols) {
            const auto entry = std::make_pair(alias, c.name);
            auto [it, inserted] = origin.emplace(flat_name(alias, c.name), entry);
            if (!inserted && it->second != entry) {
                unambiguous = false;  // two distinct origins collapse to one flat name
            }
        }
        JoinRel rel;
        rel.alias = alias;
        rel.stats = estimate_stats(child);
        rel.columns = std::move(cols);
        rels.push_back(std::move(rel));
        return false;
    };

    const bool left_is_join = handle_side(j.left(), l_alias);
    const bool right_is_join = handle_side(j.right(), r_alias);

    // Resolve this join's edge endpoints to (alias, raw_col). A leaf endpoint is
    // (alias, key) directly; a sub-join endpoint is the exact origin lookup.
    auto resolve = [&](bool is_join,
                       const std::string& alias,
                       const std::string& key) -> std::pair<std::string, std::string> {
        if (!is_join) {
            return {alias, key};
        }
        auto it = origin.find(key);
        if (it == origin.end()) {
            unambiguous = false;  // unresolvable flat key -> leave un-reordered
            return {std::string{}, std::string{}};
        }
        return it->second;
    };
    auto [la, lc] = resolve(left_is_join, l_alias, l_key);
    auto [ra, rc] = resolve(right_is_join, r_alias, r_key);
    if (la.empty() || ra.empty()) {
        unambiguous = false;
    } else {
        edges.push_back(JoinEdge{la, lc, ra, rc});
    }
}

// Destructively move the single-alias leaf subplans of an INNER-equi-join subtree
// out into `out`, in the SAME depth-first order analyze() collects them (left
// side then right side, recursing INNER joins). Only called on a subtree analyze
// confirmed pure, so every non-INNER-join child is a real leaf.
inline void move_leaves(std::unique_ptr<LogicalPlan> node,
                        std::vector<std::unique_ptr<LogicalPlan>>& out) {
    auto& j = static_cast<LogicalEquiJoin&>(*node);
    auto left = std::move(j.left_mut());
    auto right = std::move(j.right_mut());
    auto side = [&](std::unique_ptr<LogicalPlan> child) {
        if (is_inner_equi_join(*child)) {
            move_leaves(std::move(child), out);
        } else {
            out.push_back(std::move(child));
        }
    };
    side(std::move(left));
    side(std::move(right));
}

// Total intermediate cardinality of a left-deep join in the given relation
// order. valid=false if any step has no connecting edge (a cross join).
inline double order_cost(const std::vector<std::size_t>& order,
                         const std::vector<JoinRel>& rels,
                         const std::vector<JoinEdge>& edges,
                         bool& valid) {
    using namespace cardinality_detail;
    valid = true;
    // acc cardinality + per-(flat column) NDV + alias set.
    RelStats acc;
    std::set<std::string> acc_aliases;
    auto seed = [&](const JoinRel& r) {
        acc.row_count = r.stats.row_count_known() ? r.stats.row_count : kDefaultScanRows;
        acc.columns.clear();
        for (const auto& [name, cs] : r.stats.columns) {
            acc.columns[flat_name(r.alias, name)] = cs;
        }
        acc_aliases = {r.alias};
    };
    seed(rels[order[0]]);
    double cost = 0.0;
    for (std::size_t i = 1; i < order.size(); ++i) {
        const JoinRel& x = rels[order[i]];
        // Find an edge connecting x to the accumulated set.
        const JoinEdge* e = nullptr;
        bool x_is_b = false;
        for (const auto& cand : edges) {
            if (acc_aliases.count(cand.a_alias) && cand.b_alias == x.alias) {
                e = &cand;
                x_is_b = true;
                break;
            }
            if (acc_aliases.count(cand.b_alias) && cand.a_alias == x.alias) {
                e = &cand;
                x_is_b = false;
                break;
            }
        }
        if (e == nullptr) {
            valid = false;
            return cost;
        }
        const std::string acc_alias = x_is_b ? e->a_alias : e->b_alias;
        const std::string acc_col = x_is_b ? e->a_col : e->b_col;
        const std::string x_col = x_is_b ? e->b_col : e->a_col;
        const double acc_ndv = ndv_or(acc, flat_name(acc_alias, acc_col));
        const double x_rows = x.stats.row_count_known() ? x.stats.row_count : kDefaultScanRows;
        const double x_ndv = x.stats.column(x_col).ndv_known() ? x.stats.column(x_col).ndv : x_rows;
        const double card = (acc.row_count * x_rows) / std::max({acc_ndv, x_ndv, 1.0});
        cost += card;
        // Extend acc.
        acc.row_count = card;
        for (const auto& [name, cs] : x.stats.columns) {
            acc.columns[flat_name(x.alias, name)] = cs;
        }
        acc_aliases.insert(x.alias);
        cap_ndv(acc, acc.row_count);
    }
    return cost;
}

// Greedy minimum-intermediate-cardinality order over the relations: start from
// the smallest, repeatedly append the connected relation giving the smallest
// next intermediate. Returns an order (possibly the identity if disconnected).
inline std::vector<std::size_t> greedy_order(const std::vector<JoinRel>& rels,
                                             const std::vector<JoinEdge>& edges) {
    using namespace cardinality_detail;
    const std::size_t n = rels.size();
    auto rows_of = [&](std::size_t i) {
        return rels[i].stats.row_count_known() ? rels[i].stats.row_count : kDefaultScanRows;
    };
    std::vector<std::size_t> order;
    std::set<std::size_t> used;
    // Start with the smallest relation.
    std::size_t start = 0;
    for (std::size_t i = 1; i < n; ++i) {
        if (rows_of(i) < rows_of(start)) {
            start = i;
        }
    }
    order.push_back(start);
    used.insert(start);
    // Maintain acc stats incrementally (mirror order_cost's accumulation).
    RelStats acc;
    std::set<std::string> acc_aliases;
    acc.row_count = rows_of(start);
    for (const auto& [name, cs] : rels[start].stats.columns) {
        acc.columns[flat_name(rels[start].alias, name)] = cs;
    }
    acc_aliases = {rels[start].alias};

    while (order.size() < n) {
        std::size_t best = n;
        double best_card = 0.0;
        std::string best_acc_alias, best_acc_col, best_x_col;
        for (std::size_t i = 0; i < n; ++i) {
            if (used.count(i)) {
                continue;
            }
            // connected?
            for (const auto& e : edges) {
                bool x_is_b = false;
                bool conn = false;
                if (acc_aliases.count(e.a_alias) && e.b_alias == rels[i].alias) {
                    conn = true;
                    x_is_b = true;
                } else if (acc_aliases.count(e.b_alias) && e.a_alias == rels[i].alias) {
                    conn = true;
                    x_is_b = false;
                }
                if (!conn) {
                    continue;
                }
                const std::string acc_alias = x_is_b ? e.a_alias : e.b_alias;
                const std::string acc_col = x_is_b ? e.a_col : e.b_col;
                const std::string x_col = x_is_b ? e.b_col : e.a_col;
                const double acc_ndv = ndv_or(acc, flat_name(acc_alias, acc_col));
                const double x_rows = rows_of(i);
                const double x_ndv = rels[i].stats.column(x_col).ndv_known()
                                         ? rels[i].stats.column(x_col).ndv
                                         : x_rows;
                const double card = (acc.row_count * x_rows) / std::max({acc_ndv, x_ndv, 1.0});
                if (best == n || card < best_card) {
                    best = i;
                    best_card = card;
                    best_acc_alias = acc_alias;
                    best_acc_col = acc_col;
                    best_x_col = x_col;
                }
            }
        }
        if (best == n) {
            // Disconnected remainder: append the rest in original order (keeps a
            // valid plan; the cost guard will reject if this isn't better).
            for (std::size_t i = 0; i < n; ++i) {
                if (!used.count(i)) {
                    order.push_back(i);
                    used.insert(i);
                }
            }
            break;
        }
        order.push_back(best);
        used.insert(best);
        acc.row_count = best_card;
        for (const auto& [name, cs] : rels[best].stats.columns) {
            acc.columns[flat_name(rels[best].alias, name)] = cs;
        }
        acc_aliases.insert(rels[best].alias);
        cap_ndv(acc, acc.row_count);
    }
    return order;
}

// A relation being accumulated during reconstruction (mirrors bind_join_rel's
// BoundRel: tracks output stream columns + qualified-ref -> stream-name map).
struct BuildRel {
    std::unique_ptr<LogicalPlan> plan;
    std::vector<ColumnSpec> columns;  // output stream columns (flat once joined)
    std::set<std::string> aliases;
    bool is_base = false;
    std::string alias;
    std::map<std::string, std::string> qual_to_stream;
};

inline BuildRel make_base(JoinRel&& r) {
    BuildRel b;
    b.is_base = true;
    b.alias = r.alias;
    b.aliases = {r.alias};
    b.columns = r.columns;  // raw cols (the leaf's output stream)
    for (const auto& c : r.columns) {
        b.qual_to_stream[r.alias + "." + c.name] = c.name;
    }
    b.plan = std::move(r.plan);
    return b;
}

// Build acc JOIN x (x always a base leaf), connected by `edge`.
inline BuildRel join_build(BuildRel acc, JoinRel&& xr, const JoinEdge& edge) {
    // Orient the edge: one endpoint is in acc, the other is x.
    std::string acc_alias, acc_col, x_col;
    if (acc.aliases.count(edge.a_alias) && edge.b_alias == xr.alias) {
        acc_alias = edge.a_alias;
        acc_col = edge.a_col;
        x_col = edge.b_col;
    } else {
        acc_alias = edge.b_alias;
        acc_col = edge.b_col;
        x_col = edge.a_col;
    }
    BuildRel x = make_base(std::move(xr));

    const std::string left_key = acc.qual_to_stream.at(acc_alias + "." + acc_col);
    const std::string right_key = x.qual_to_stream.at(x.alias + "." + x_col);
    const std::string left_alias_param = acc.is_base ? acc.alias : std::string{};
    const std::string right_alias_param = x.alias;

    BuildRel out;
    out.is_base = false;
    out.aliases = acc.aliases;
    out.aliases.insert(x.aliases.begin(), x.aliases.end());
    auto add_side = [&out](const BuildRel& side) {
        for (const auto& c : side.columns) {
            out.columns.push_back(side.is_base ? ColumnSpec{side.alias + "_" + c.name, c.type} : c);
        }
        for (const auto& [q, s] : side.qual_to_stream) {
            out.qual_to_stream[q] = side.is_base ? (side.alias + "_" + s) : s;
        }
    };
    add_side(acc);
    add_side(x);

    arrow::FieldVector fields;
    fields.reserve(out.columns.size());
    for (const auto& c : out.columns) {
        fields.push_back(arrow::field(c.name, c.type));
    }
    out.plan = std::make_unique<LogicalEquiJoin>(std::move(acc.plan),
                                                 std::move(x.plan),
                                                 left_alias_param,
                                                 right_alias_param,
                                                 left_key,
                                                 right_key,
                                                 arrow::schema(std::move(fields)),
                                                 JoinType::Inner);
    return out;
}

// Rebuild a left-deep INNER-join tree in the given relation order.
inline std::unique_ptr<LogicalPlan> rebuild(std::vector<std::size_t> order,
                                            std::vector<JoinRel> rels,
                                            const std::vector<JoinEdge>& edges) {
    BuildRel acc = make_base(std::move(rels[order[0]]));
    for (std::size_t i = 1; i < order.size(); ++i) {
        // Find the edge connecting rels[order[i]] to acc.
        const JoinRel& x = rels[order[i]];
        const JoinEdge* e = nullptr;
        for (const auto& cand : edges) {
            if ((acc.aliases.count(cand.a_alias) && cand.b_alias == x.alias) ||
                (acc.aliases.count(cand.b_alias) && cand.a_alias == x.alias)) {
                e = &cand;
                break;
            }
        }
        acc = join_build(std::move(acc), std::move(rels[order[i]]), *e);
    }
    return std::move(acc.plan);
}

}  // namespace reorder_detail

// Reorder the INNER-equi-join trees in a plan to minimise intermediate
// cardinality. In-place on the given slot; returns whether anything changed.
inline void reorder_joins(std::unique_ptr<LogicalPlan>& slot);

namespace reorder_detail {

// Reorder the maximal INNER-equi-join subtree rooted at `slot` (an INNER
// EquiJoin). Only a pure, unambiguous inner-join tree of >=3 single-alias leaves
// whose greedy order is strictly cheaper is rewritten; in every other case the
// node is left structurally untouched (a true no-op) and we descend into its
// children for any nested reorderable subtrees. The reorderability decision is
// made non-destructively (analyze) BEFORE any subplan is moved, so an ambiguous
// or declined subtree is never half-rewritten and can never throw.
inline void reorder_subtree(std::unique_ptr<LogicalPlan>& slot) {
    std::vector<JoinRel> rels;
    std::vector<JoinEdge> edges;
    std::map<std::string, std::pair<std::string, std::string>> origin;
    bool pure = true;
    bool unambiguous = true;
    analyze(*slot, rels, edges, origin, pure, unambiguous);

    // Leave this node intact and recurse into its children for nested subtrees.
    auto descend = [&]() {
        auto& j = static_cast<LogicalEquiJoin&>(*slot);
        clink::sql::reorder_joins(j.left_mut());
        clink::sql::reorder_joins(j.right_mut());
    };

    const std::size_t m = rels.size();
    if (!pure || !unambiguous || m < 3) {
        descend();
        return;
    }

    std::vector<std::size_t> identity(m);
    for (std::size_t i = 0; i < m; ++i) {
        identity[i] = i;
    }
    bool orig_valid = false;
    const double orig_cost = order_cost(identity, rels, edges, orig_valid);
    const std::vector<std::size_t> greedy = greedy_order(rels, edges);
    bool greedy_valid = false;
    const double greedy_cost = order_cost(greedy, rels, edges, greedy_valid);

    const bool apply =
        greedy_valid && greedy != identity && (!orig_valid || greedy_cost < orig_cost);
    if (!apply) {
        descend();  // declined: a true structural no-op, recurse children instead
        return;
    }

    // Apply: move the leaf subplans out (in analyze's DFS order, so they align
    // with `rels`), reorder any nested subtrees inside each leaf, then rebuild a
    // left-deep tree in the greedy order and commit with a single move-assign.
    //
    // Exception safety: rebuild() is LOGIC-throw-free here - analyze() verified
    // every edge endpoint against the exact origin map and order_cost proved the
    // greedy order fully connected, so join_build's map lookups and rebuild's
    // edge search never miss. The only residual throw is std::bad_alloc (genuine
    // OOM) from the allocations in the recursion below or in rebuild. Because the
    // leaves are moved out of `slot` first, such a throw would leave `slot` null;
    // that is then caught one level up by optimize()'s guard, and the resulting
    // null child is rejected with a clean TranslationError by the physical
    // planner's require_no_null_children backstop - a clean compile failure under
    // OOM, never a crash or a corrupt executable plan.
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    move_leaves(std::move(slot), plans);
    if (force_throw_after_move_flag()) {
        throw std::runtime_error("reorder_subtree: forced throw after move (test seam)");
    }
    for (std::size_t i = 0; i < m && i < plans.size(); ++i) {
        rels[i].plan = std::move(plans[i]);
        clink::sql::reorder_joins(rels[i].plan);
    }
    auto rebuilt = rebuild(greedy, std::move(rels), edges);
    slot = std::move(rebuilt);
}

}  // namespace reorder_detail

inline void reorder_joins(std::unique_ptr<LogicalPlan>& slot) {
    if (!slot) {
        return;
    }
    if (reorder_detail::is_inner_equi_join(*slot)) {
        reorder_detail::reorder_subtree(slot);
        return;
    }
    // Descend through single-input wrapper nodes to find a join subtree.
    const auto k = slot->kind();
    if (k == "Sink") {
        reorder_joins(static_cast<LogicalSink&>(*slot).input_mut());
    } else if (k == "Filter") {
        reorder_joins(static_cast<LogicalFilter&>(*slot).input_mut());
    } else if (k == "Project") {
        reorder_joins(static_cast<LogicalProject&>(*slot).input_mut());
    } else if (k == "Aggregate") {
        reorder_joins(static_cast<LogicalAggregate&>(*slot).input_mut());
    } else if (k == "WindowAggregate") {
        reorder_joins(static_cast<LogicalWindowAggregate&>(*slot).input_mut());
    }
    // Other nodes: no INNER-join tree directly beneath a multi-way-join SELECT.
}

}  // namespace clink::sql
