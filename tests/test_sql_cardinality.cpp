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

}  // namespace
}  // namespace clink::sql
