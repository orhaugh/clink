// Cost-optimizer foundation: statistics + cardinality estimation.
//
// estimate_stats() walks a bound LogicalPlan and produces row-count + per-column
// NDV estimates from the tables' declared WITH-option statistics. It is
// result-neutral (analysis only); these tests assert the estimates for scans,
// filters (eq / range using NDV), joins (the |L|*|R| / max(V) formula), and
// GROUP BY. This is the input the cost-based join reorderer will minimise.

#include <gtest/gtest.h>

#include "clink/sql/binder.hpp"
#include "clink/sql/cardinality.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/statistics.hpp"

#include "arrow/api.h"

namespace clink::sql {
namespace {

const ast::SelectStmt& as_select(const ast::Script& s) {
    return std::get<ast::SelectStmt>(s.statements[0]);
}

// Register a file/json table with the given column DDL + WITH-option stats.
void reg(Catalog& cat, const std::string& name, const std::string& cols, const std::string& opts) {
    auto ddl = parse("CREATE TABLE " + name + " " + cols +
                     " WITH (connector='file', format='json', path='/tmp/x'" +
                     (opts.empty() ? "" : ", " + opts) + ")");
    cat.register_table(std::get<ast::CreateTableStmt>(ddl.statements[0]));
}

double rows_of(const Catalog& cat, const std::string& sql) {
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse(sql)));
    return estimate_rows(*plan);
}

TEST(SqlCardinality, TableStatsFromWithOptions) {
    Catalog cat;
    reg(cat, "orders", "(user_id BIGINT, amount BIGINT)", "row_count='1000', ndv_user_id='100'");
    Binder b(cat);
    auto plan = b.bind_select(as_select(parse("SELECT * FROM orders")));
    auto s = estimate_stats(*plan);
    EXPECT_DOUBLE_EQ(s.row_count, 1000.0);
    EXPECT_DOUBLE_EQ(s.column("user_id").ndv, 100.0);
    EXPECT_FALSE(s.column("amount").ndv_known());  // no ndv_amount declared
}

TEST(SqlCardinality, UnknownTableUsesDefaultScanRows) {
    Catalog cat;
    reg(cat, "t", "(k BIGINT)", "");  // no stats declared
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM t"), cardinality_detail::kDefaultScanRows);
}

TEST(SqlCardinality, EqualityFilterUsesNdv) {
    Catalog cat;
    reg(cat, "orders", "(user_id BIGINT, amount BIGINT)", "row_count='1000', ndv_user_id='100'");
    // 1000 rows * (1 / ndv=100) = 10.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE user_id = 5"), 10.0);
}

TEST(SqlCardinality, EqualityFilterDefaultSelectivityWhenNdvUnknown) {
    Catalog cat;
    reg(cat, "orders", "(user_id BIGINT, amount BIGINT)", "row_count='1000'");
    // ndv_amount unknown -> Selinger default 0.1 -> 100.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount = 5"), 100.0);
}

TEST(SqlCardinality, RangeFilterOneThird) {
    Catalog cat;
    reg(cat, "orders", "(user_id BIGINT, amount BIGINT)", "row_count='999'");
    // range selectivity 1/3 -> 333.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount > 50"), 333.0);
}

TEST(SqlCardinality, AndFilterMultipliesSelectivities) {
    Catalog cat;
    reg(cat, "orders", "(user_id BIGINT, amount BIGINT)", "row_count='1000', ndv_user_id='100'");
    // user_id = 5 (1/100) AND amount > 0 (1/3) -> 1000 * 0.01 * 0.3333 ~= 3.33.
    EXPECT_NEAR(rows_of(cat, "SELECT * FROM orders WHERE user_id = 5 AND amount > 0"), 3.333, 0.01);
}

TEST(SqlCardinality, JoinCardinalityFormula) {
    Catalog cat;
    reg(cat, "a", "(k BIGINT, av BIGINT)", "row_count='1000', ndv_k='1000'");
    reg(cat, "b", "(k BIGINT, bv BIGINT)", "row_count='2000', ndv_k='100'");
    // |a|*|b| / max(V(a.k)=1000, V(b.k)=100) = 1000*2000/1000 = 2000.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM a JOIN b ON a.k = b.k"), 2000.0);
}

TEST(SqlCardinality, GroupByGroupsEqualKeyNdv) {
    Catalog cat;
    reg(cat, "orders", "(user_id BIGINT, amount BIGINT)", "row_count='1000', ndv_user_id='100'");
    // distinct groups = ndv(user_id) = 100.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT user_id, SUM(amount) AS s FROM orders GROUP BY user_id"),
                     100.0);
}

TEST(SqlCardinality, ThreeWayJoinProducesFiniteEstimate) {
    Catalog cat;
    reg(cat, "a", "(k BIGINT, av BIGINT)", "row_count='100', ndv_k='100'");
    reg(cat, "b", "(k BIGINT, bv BIGINT)", "row_count='100', ndv_k='100'");
    reg(cat, "c", "(k BIGINT, cv BIGINT)", "row_count='100', ndv_k='100'");
    // (a join b): 100*100/100 = 100 (key b_k ndv kept at 100); join c on b.k:
    // 100*100/max(V(b_k),V(c.k)) = 100*100/100 = 100.
    const double r = rows_of(cat, "SELECT a_av FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k");
    EXPECT_GT(r, 0.0);
    EXPECT_NEAR(r, 100.0, 1.0);
}

// ----- cost-based join reordering -----

// The driving (bottom-left) base table of the join tree: descend through
// wrapper nodes to the join, then down its left spine to the first scan.
std::string driving_table(const LogicalPlan& root) {
    const LogicalPlan* p = &root;
    while (p != nullptr) {
        const auto k = p->kind();
        if (k == "EquiJoin") {
            p = &static_cast<const LogicalEquiJoin&>(*p).left();
            continue;
        }
        if (k == "Scan") {
            return static_cast<const LogicalScan&>(*p).table().name;
        }
        auto ins = p->inputs();
        if (ins.empty() || ins[0] == nullptr) {
            return "";
        }
        p = ins[0];  // descend Project/Filter/etc to find the join
    }
    return "";
}

TEST(SqlCardinality, ReordersThreeWayJoinSmallestDrives) {
    Catalog cat;
    reg(cat, "big", "(k BIGINT, bv BIGINT)", "row_count='1000000', ndv_k='1000000'");
    reg(cat, "mid", "(k BIGINT, mv BIGINT)", "row_count='1000', ndv_k='1000'");
    reg(cat, "small", "(k BIGINT, sv BIGINT)", "row_count='10', ndv_k='10'");
    Binder b(cat);
    // Syntactic order drives with `big`; cost-based reorder should drive with
    // `small` (start from the smallest relation).
    auto plan = b.bind_select(as_select(
        parse("SELECT big_bv FROM big JOIN mid ON big.k = mid.k JOIN small ON mid.k = small.k")));
    EXPECT_EQ(driving_table(*plan), "big");  // pre-optimization (syntactic)
    auto opt = optimize(std::move(plan));
    EXPECT_EQ(driving_table(*opt), "small")
        << "cost-based reorder should put the smallest relation at the bottom-left";
}

TEST(SqlCardinality, NoReorderWithoutStats) {
    Catalog cat;
    reg(cat, "big", "(k BIGINT, bv BIGINT)", "");
    reg(cat, "mid", "(k BIGINT, mv BIGINT)", "");
    reg(cat, "small", "(k BIGINT, sv BIGINT)", "");
    Binder b(cat);
    auto opt = optimize(b.bind_select(as_select(
        parse("SELECT big_bv FROM big JOIN mid ON big.k = mid.k JOIN small ON mid.k = small.k"))));
    // No declared stats -> every relation looks identical -> cost guard leaves
    // the syntactic order (big drives) untouched.
    EXPECT_EQ(driving_table(*opt), "big");
}

// Regression: with aliases where one is a prefix of the other up to '_' (`s` vs
// `s_t`), the sub-join's flat output names are not uniquely invertible by a
// longest-prefix parse. The reorderer must resolve edge endpoints through the
// EXACT flat-name -> (alias, col) map (or bail to no-reorder), NEVER guess and
// throw std::out_of_range (map::at) out of optimize() on an otherwise-valid query.
TEST(SqlCardinality, AliasPrefixCollisionDoesNotThrowFromOptimize) {
    Catalog cat;
    reg(cat, "s", "(id BIGINT, t_x BIGINT)", "row_count='100', ndv_id='100', ndv_t_x='100'");
    reg(cat, "s_t", "(id BIGINT)", "row_count='100', ndv_id='100'");
    reg(cat, "c", "(k BIGINT)", "row_count='100', ndv_k='100'");
    Binder b(cat);
    // The root join key s.t_x flattens to "s_t_x"; a longest-prefix parse against
    // {s, s_t} would mis-attribute it to alias s_t (which has no column x) and
    // throw. The exact map resolves it to (s, t_x).
    auto plan = b.bind_select(
        as_select(parse("SELECT s_id FROM s JOIN s_t ON s.id = s_t.id JOIN c ON s.t_x = c.k")));
    EXPECT_NO_THROW({
        auto opt = optimize(std::move(plan));
        EXPECT_NE(opt, nullptr);
    });
}

// Regression: estimate_stats must read a LogicalIntervalJoin through its own type,
// never via a LogicalEquiJoin& cross-cast (undefined behaviour; caught by UBSan
// -fsanitize=vptr). A 2-table eq+BETWEEN join binds to a LogicalIntervalJoin and
// exercises the IntervalJoin branch of the estimator directly.
TEST(SqlCardinality, IntervalJoinEstimatesThroughCorrectType) {
    Catalog cat;
    reg(cat, "clicks", "(user_id BIGINT, click_ts BIGINT)", "row_count='1000', ndv_user_id='100'");
    reg(cat, "imps", "(user_id BIGINT, imp_ts BIGINT)", "row_count='2000', ndv_user_id='100'");
    Binder b(cat);
    auto plan =
        b.bind_select(as_select(parse("SELECT * FROM clicks c JOIN imps i ON c.user_id = i.user_id "
                                      "AND c.click_ts BETWEEN i.imp_ts - INTERVAL '5' SECOND "
                                      "                   AND i.imp_ts + INTERVAL '10' SECOND")));
    EXPECT_NO_THROW({
        auto s = estimate_stats(*plan);
        EXPECT_GT(s.row_count, 0.0);
    });
}

// Regression: a declined reorder must be a true STRUCTURAL no-op, not silently
// reshape a parenthesised right-deep input `a JOIN (b JOIN c)` to left-deep.
TEST(SqlCardinality, DeclinedReorderPreservesTreeShape) {
    Catalog cat;
    reg(cat, "a", "(k BIGINT, av BIGINT)", "");
    reg(cat, "b", "(k BIGINT)", "");
    reg(cat, "c", "(k BIGINT)", "");
    Binder b(cat);
    auto opt = optimize(b.bind_select(
        as_select(parse("SELECT a_av FROM a JOIN (b JOIN c ON b.k = c.k) ON a.k = b.k"))));
    // Descend to the first join node.
    const LogicalPlan* p = opt.get();
    while (p != nullptr && p->kind() != "EquiJoin") {
        auto ins = p->inputs();
        p = (ins.empty() || ins[0] == nullptr) ? nullptr : ins[0];
    }
    ASSERT_NE(p, nullptr);
    // No stats -> reorder declined -> the right-deep shape is left intact, i.e.
    // the root join's RIGHT child is still the (b JOIN c) sub-join.
    EXPECT_EQ(static_cast<const LogicalEquiJoin&>(*p).right().kind(), "EquiJoin")
        << "a declined reorder must not reshape right-deep input to left-deep";
}

}  // namespace
}  // namespace clink::sql
