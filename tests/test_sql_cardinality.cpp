// Cost-optimizer foundation: statistics + cardinality estimation.
//
// estimate_stats() walks a bound LogicalPlan and produces row-count + per-column
// NDV estimates from the tables' declared WITH-option statistics. It is
// result-neutral (analysis only); these tests assert the estimates for scans,
// filters (eq / range using NDV), joins (the |L|*|R| / max(V) formula), and
// GROUP BY. This is the input the cost-based join reorderer will minimise.

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/sql_metrics.hpp"
#include "clink/sql/analyze.hpp"
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

// A planner bug that throws from an optimizer pass must NOT escape and fail an
// otherwise-valid query: optimize() catches it, records the failure, and returns
// the un-optimized (but valid, executable) plan.
TEST(SqlCardinality, OptimizeGuardFallsBackOnPassThrow) {
    Catalog cat;
    reg(cat, "big", "(k BIGINT, bv BIGINT)", "row_count='1000000', ndv_k='1000000'");
    reg(cat, "mid", "(k BIGINT, mv BIGINT)", "row_count='1000', ndv_k='1000'");
    reg(cat, "small", "(k BIGINT, sv BIGINT)", "row_count='10', ndv_k='10'");
    const std::string q =
        "SELECT big_bv FROM big JOIN mid ON big.k = mid.k JOIN small ON mid.k = small.k";

    // Baseline: with the guard inactive the cost-based reorder fires (small drives).
    Binder b1(cat);
    auto normal = optimize(b1.bind_select(as_select(parse(q))));
    EXPECT_EQ(driving_table(*normal), "small");

    const std::uint64_t before =
        MetricsRegistry::global().counter(clink::metrics::kSqlOptimizeErrors).value();
    detail::set_optimize_force_throw(true);
    Binder b2(cat);
    std::unique_ptr<LogicalPlan> guarded;
    EXPECT_NO_THROW({ guarded = optimize(b2.bind_select(as_select(parse(q)))); });
    detail::set_optimize_force_throw(false);  // reset before any ASSERT can return early

    ASSERT_NE(guarded, nullptr);
    // The guard returned the un-optimized syntactic plan: big still drives.
    EXPECT_EQ(driving_table(*guarded), "big")
        << "a thrown optimizer pass must fall back to the un-optimized plan";
    // ...and the failure was counted.
    EXPECT_EQ(MetricsRegistry::global().counter(clink::metrics::kSqlOptimizeErrors).value(),
              before + 1);
}

// ----- histograms (range selectivity) + MCV (equality skew) -----

TEST(SqlCardinality, HistogramDrivesRangeSelectivity) {
    Catalog cat;
    // Equi-depth single bucket [0,100]: amount < 25 -> 25% (NOT the 1/3 default).
    reg(cat, "orders", "(amount BIGINT)", "row_count='1000', hist_amount='0,100'");
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount < 25"), 250.0);
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount > 75"), 250.0);
    // Out-of-range literals clamp to 0% / 100%.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount < 0"), 0.0);
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount > 200"), 0.0);
}

TEST(SqlCardinality, MultiBucketHistogramInterpolates) {
    Catalog cat;
    // 3 equal-mass buckets [0,10] [10,20] [20,100]; x < 5 -> half of bucket 0 ->
    // (0 + 0.5)/3 ~= 16.67%, well away from the flat 1/3.
    reg(cat, "t", "(x BIGINT)", "row_count='3000', hist_x='0,10,20,100'");
    EXPECT_NEAR(rows_of(cat, "SELECT * FROM t WHERE x < 5"), 500.0, 1.0);
}

TEST(SqlCardinality, MalformedHistogramFallsBackToDefault) {
    Catalog cat;
    // A single boundary is no bucket -> ignored -> range uses the 1/3 default.
    reg(cat, "orders", "(amount BIGINT)", "row_count='999', hist_amount='50'");
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM orders WHERE amount > 50"), 333.0);
}

TEST(SqlCardinality, McvDrivesEqualitySelectivityUnderSkew) {
    Catalog cat;
    reg(cat, "events", "(status BIGINT)", "row_count='1000', ndv_status='5', mcv_status='200:0.8'");
    // Hot value 200 -> 80% (NOT the uniform 1/ndv = 20%).
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM events WHERE status = 200"), 800.0);
    // Cold value -> residual mass over residual ndv = (1-0.8)/(5-1) = 0.05 -> 50.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM events WHERE status = 404"), 50.0);
    // != hot value -> 1 - 0.8 -> 200.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM events WHERE status <> 200"), 200.0);
}

TEST(SqlCardinality, McvDrivesInListSelectivity) {
    Catalog cat;
    reg(cat,
        "events",
        "(status BIGINT)",
        "row_count='1000', ndv_status='5', mcv_status='200:0.8,404:0.1'");
    // IN (200, 404) -> 0.8 + 0.1 = 0.9 -> 900.
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM events WHERE status IN (200, 404)"), 900.0);
}

// The histogram changes the optimizer's DECISION, not just the estimate: a range
// filter whose histogram selectivity (1%) makes `a` tiny enough to beat `b`,
// whereas the flat 1/3 default (333 > 100) would leave `b` driving.
TEST(SqlCardinality, HistogramSelectivityFlipsJoinDriver) {
    Catalog cat;
    reg(cat, "a", "(k BIGINT, x BIGINT)", "row_count='1000', ndv_k='1000', hist_x='0,1000'");
    reg(cat, "b", "(k BIGINT)", "row_count='100', ndv_k='100'");
    Binder bd(cat);
    // a_x < 10 -> hist 1% -> ~10 rows < b's 100 -> a drives. (Without the
    // histogram, 1/3 -> ~333 > 100 -> b would drive.) Post-join columns are flat
    // (<alias>_<col>); predicate pushdown de-aliases a_x -> x into a's scan.
    auto opt = optimize(
        bd.bind_select(as_select(parse("SELECT a_k FROM a JOIN b ON a.k = b.k WHERE a_x < 10"))));
    EXPECT_EQ(driving_table(*opt), "a")
        << "the histogram-estimated tiny filtered relation should drive the join";
}

// ----- ANALYZE: stats computed by scanning rows (StatsCollector) -----

Row stat_row(std::int64_t status, std::int64_t amount) {
    Row r;
    r.values["status"] = clink::config::JsonValue{status};
    r.values["amount"] = clink::config::JsonValue{amount};
    return r;
}

TEST(SqlAnalyze, StatsCollectorComputesExactStats) {
    StatsCollector c({"status", "amount"}, /*hist_buckets=*/4, /*max_mcv=*/8);
    // 100 rows: status skewed (80x200, 15x404, 5x500); amount = 0..99 (uniform).
    for (int i = 0; i < 80; ++i) {
        c.observe(stat_row(200, i));
    }
    for (int i = 80; i < 95; ++i) {
        c.observe(stat_row(404, i));
    }
    for (int i = 95; i < 100; ++i) {
        c.observe(stat_row(500, i));
    }
    EXPECT_EQ(c.row_count(), 100u);
    auto opts = c.to_with_options();
    EXPECT_EQ(opts["row_count"], "100");
    EXPECT_EQ(opts["ndv_status"], "3");
    EXPECT_EQ(opts["ndv_amount"], "100");
    // 200 is the hot value at 80/100 = 0.8, and sorts first (frequency-descending).
    EXPECT_EQ(opts["mcv_status"].rfind("200:0.8", 0), 0u) << "got: " << opts["mcv_status"];
    EXPECT_FALSE(opts["hist_amount"].empty());  // numeric column -> histogram
    EXPECT_TRUE(opts.find("hist_status") == opts.end() || !opts["hist_status"].empty());
}

// The full compute -> WITH-option -> parse -> selectivity round trip: stats
// computed by a scan, serialised the way ANALYZE writes them back, drive the
// estimator exactly as declared stats do.
TEST(SqlAnalyze, ComputedStatsRoundTripDriveSelectivity) {
    StatsCollector c({"status", "amount"}, /*hist_buckets=*/4, /*max_mcv=*/8);
    for (int i = 0; i < 80; ++i) {
        c.observe(stat_row(200, i));
    }
    for (int i = 80; i < 95; ++i) {
        c.observe(stat_row(404, i));
    }
    for (int i = 95; i < 100; ++i) {
        c.observe(stat_row(500, i));
    }
    std::string opts;
    for (const auto& [k, v] : c.to_with_options()) {
        if (!opts.empty()) {
            opts += ", ";
        }
        opts += k + "='" + v + "'";
    }
    Catalog cat;
    reg(cat, "t", "(status BIGINT, amount BIGINT)", opts);
    // The analyzed MCV puts status=200 at 0.8 -> 100 * 0.8 = 80 rows (NOT the
    // uniform 1/ndv=1/3 the un-analyzed table would have given).
    EXPECT_DOUBLE_EQ(rows_of(cat, "SELECT * FROM t WHERE status = 200"), 80.0);
    EXPECT_EQ(estimate_stats(*Binder(cat).bind_select(as_select(parse("SELECT * FROM t"))))
                  .column("amount")
                  .histogram.empty(),
              false)
        << "the analyzed amount histogram should survive the round trip";
}

}  // namespace
}  // namespace clink::sql
