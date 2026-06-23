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
// Hand-rolled on clink's own IR (see project memory: no external optimizer fits
// a JVM-free Arrow-native streaming engine). Algorithm borrowed from DuckDB's
// join-order optimizer + the Selinger/Moerkotte literature.

#include <algorithm>
#include <map>
#include <memory>
#include <set>
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
struct JoinRel {
    std::unique_ptr<LogicalPlan> plan;
    std::string alias;
    std::vector<ColumnSpec> columns;  // the leaf's output (raw) columns
    RelStats stats;                   // estimate_stats(*plan), raw-named columns
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

// Parse a flat "<alias>_<col>" name against a set of aliases (longest alias
// prefix wins), returning (alias, col). Falls back to ("", name) if no alias
// matches (caller treats that as un-reorderable).
inline std::pair<std::string, std::string> parse_flat(const std::string& name,
                                                      const std::set<std::string>& aliases) {
    std::string best;
    for (const auto& a : aliases) {
        if (name.size() > a.size() + 1 && name.compare(0, a.size(), a) == 0 &&
            name[a.size()] == '_' && a.size() > best.size()) {
            best = a;
        }
    }
    if (best.empty()) {
        return {"", name};
    }
    return {best, name.substr(best.size() + 1)};
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

// Flatten a maximal INNER-equi-join subtree into relations + edges, MOVING the
// leaf subplans out. Returns the alias set of `node`'s subtree; sets ok=false if
// the subtree contains a non-INNER-join block (opaque, not reorderable).
inline std::set<std::string> flatten(std::unique_ptr<LogicalPlan> node,
                                     std::vector<JoinRel>& rels,
                                     std::vector<JoinEdge>& edges,
                                     bool& ok) {
    auto& j = static_cast<LogicalEquiJoin&>(*node);  // caller guarantees INNER EquiJoin
    const std::string l_alias = j.left_alias();
    const std::string r_alias = j.right_alias();
    const std::string l_key = j.left_key_column();
    const std::string r_key = j.right_key_column();
    auto left = std::move(j.left_mut());
    auto right = std::move(j.right_mut());

    auto handle_side =
        [&](std::unique_ptr<LogicalPlan> child,
            const std::string& alias,
            const std::string& key) -> std::pair<std::set<std::string>, std::string> {
        // returns (alias_set, resolved-key-as "alias.col" via the endpoint below
        // is handled by the caller); here we register leaves and recurse joins.
        if (is_inner_equi_join(*child)) {
            auto set = flatten(std::move(child), rels, edges, ok);
            return {set, std::string{}};  // sub-join: key parsed by caller against `set`
        }
        if (child->kind() == "EquiJoin" || child->kind() == "IntervalJoin" ||
            child->kind() == "LookupJoin" || child->kind() == "SemiJoin") {
            ok = false;  // opaque multi-relation block -> not reorderable
            return {std::set<std::string>{}, std::string{}};
        }
        // Single-alias leaf (Scan, or Filter/Project over a scan).
        JoinRel rel;
        rel.columns = columns_of(*child);
        rel.alias = alias;
        rel.stats = estimate_stats(*child);
        rel.plan = std::move(child);
        rels.push_back(std::move(rel));
        return {std::set<std::string>{alias}, std::string{}};
    };

    const bool left_is_join = is_inner_equi_join(*left);
    const bool right_is_join = is_inner_equi_join(*right);
    auto [l_set, _l] = handle_side(std::move(left), l_alias, l_key);
    auto [r_set, _r] = handle_side(std::move(right), r_alias, r_key);

    // Resolve this join's edge endpoints to (alias, raw_col).
    auto resolve = [&](bool is_join,
                       const std::string& alias,
                       const std::string& key,
                       const std::set<std::string>& set) -> std::pair<std::string, std::string> {
        return is_join ? parse_flat(key, set) : std::pair<std::string, std::string>{alias, key};
    };
    auto [la, lc] = resolve(left_is_join, l_alias, l_key, l_set);
    auto [ra, rc] = resolve(right_is_join, r_alias, r_key, r_set);
    if (la.empty() || ra.empty()) {
        ok = false;
    } else {
        edges.push_back(JoinEdge{la, lc, ra, rc});
    }

    std::set<std::string> all = l_set;
    all.insert(r_set.begin(), r_set.end());
    return all;
}

// Flat output column name a relation contributes to a join: "<alias>_<col>".
inline std::string flat_name(const std::string& alias, const std::string& col) {
    return alias + "_" + col;
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

// Count the leaf relations of the maximal INNER-equi-join subtree at `n`,
// non-destructively. Sets pure=false if the subtree contains any non-INNER join
// block (outer / interval / lookup / semi), which makes it not reorderable as a
// pure inner-join tree.
inline int count_relations(const LogicalPlan& n, bool& pure) {
    if (is_inner_equi_join(n)) {
        const auto& j = static_cast<const LogicalEquiJoin&>(n);
        return count_relations(j.left(), pure) + count_relations(j.right(), pure);
    }
    const auto k = n.kind();
    if (k == "EquiJoin" || k == "IntervalJoin" || k == "LookupJoin" || k == "SemiJoin") {
        pure = false;  // an opaque multi-relation block inside the subtree
    }
    return 1;
}

// Reorder the maximal INNER-equi-join subtree rooted at `slot` (an INNER
// EquiJoin). Only a pure inner-join tree of >=3 single-alias leaves is
// reordered; otherwise the node is left structurally untouched and we descend
// into its children for any nested reorderable subtrees.
inline void reorder_subtree(std::unique_ptr<LogicalPlan>& slot) {
    bool pure = true;
    const int n = count_relations(*slot, pure);
    if (!pure || n < 3) {
        auto& j = static_cast<LogicalEquiJoin&>(*slot);
        clink::sql::reorder_joins(j.left_mut());
        clink::sql::reorder_joins(j.right_mut());
        return;
    }

    std::vector<JoinRel> rels;
    std::vector<JoinEdge> edges;
    bool ok = true;
    flatten(std::move(slot), rels, edges, ok);
    // Recurse into each leaf relation for nested reorderable subtrees (e.g. a
    // derived table containing its own joins).
    for (auto& r : rels) {
        clink::sql::reorder_joins(r.plan);
    }

    const std::size_t m = rels.size();
    std::vector<std::size_t> identity(m);
    for (std::size_t i = 0; i < m; ++i) {
        identity[i] = i;
    }
    if (!ok) {
        slot = rebuild(identity, std::move(rels), edges);  // restore as-is
        return;
    }
    bool orig_valid = false;
    const double orig_cost = order_cost(identity, rels, edges, orig_valid);
    const std::vector<std::size_t> greedy = greedy_order(rels, edges);
    bool greedy_valid = false;
    const double greedy_cost = order_cost(greedy, rels, edges, greedy_valid);

    const bool apply =
        greedy_valid && greedy != identity && (!orig_valid || greedy_cost < orig_cost);
    slot = rebuild(apply ? greedy : identity, std::move(rels), edges);
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
