// Expressions over aggregates in a grouped SELECT: arithmetic, CASE, casts and
// scalar functions of aggregates, aggregates of different columns combined,
// expressions over the group keys and window bounds, and HAVING over the same.
//
// The binder used to classify each SELECT item as exactly one of "an aggregate
// call" or "a column reference", and anything else reached a std::get on the
// column-reference arm: `SELECT k, SUM(n) / 8, COUNT(*) FROM t GROUP BY k`
// escaped as std::bad_variant_access, and without a second, bare aggregate the
// same item was reported as "GROUP BY without aggregate functions". These tests
// pin the standard answer on a plain GROUP BY and on tumbling, hopping,
// cumulating and session windows, end to end through the embedded engine (the
// `clink run` path), plus the shapes clink refuses and the words it refuses
// them with.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"

#include "arrow/api.h"

namespace clink::sql {

namespace {

namespace fs = std::filesystem;

// --- Binder level -----------------------------------------------------------

void register_grouped_events(Catalog& cat) {
    auto s = parse(
        "CREATE TABLE gev (k BIGINT, n BIGINT, m BIGINT, s VARCHAR, ts BIGINT) "
        "WITH (connector='file', format='json', path='/tmp/gev.ndjson', event_time_column='ts')");
    cat.register_table(std::get<ast::CreateTableStmt>(s.statements[0]));
}

std::unique_ptr<LogicalPlan> bind_grouped(const std::string& sql) {
    Catalog cat;
    register_grouped_events(cat);
    Binder b(cat);
    auto script = parse(sql);
    return b.bind_select(std::get<ast::SelectStmt>(script.statements[0]));
}

// The bind error text for `sql`, or "" when it binds. Any exception other than
// TranslationError (std::bad_variant_access was the defect) fails the test.
std::string bind_error_text(const std::string& sql) {
    try {
        (void)bind_grouped(sql);
    } catch (const TranslationError& e) {
        return e.what();
    } catch (const std::exception& e) {
        ADD_FAILURE() << "expected a bind error, got " << typeid(e).name() << ": " << e.what()
                      << "\n  " << sql;
        return "<not a bind error>";
    }
    return {};
}

const LogicalPlan* find_kind(const LogicalPlan* p, const std::string& kind) {
    while (p != nullptr) {
        if (p->kind() == kind) {
            return p;
        }
        auto ins = p->inputs();
        p = ins.empty() ? nullptr : ins[0];
    }
    return nullptr;
}

std::vector<std::string> field_names(const arrow::Schema& s) {
    std::vector<std::string> out;
    for (const auto& f : s.fields()) {
        out.push_back(f->name());
    }
    return out;
}

TEST(SqlGroupedExpressions, ArithmeticOverAnAggregateBindsInsteadOfThrowing) {
    // The reported shape, with and without a second, bare aggregate beside it.
    for (const char* sql :
         {"SELECT k, SUM(n) / 8 FROM gev GROUP BY k",
          "SELECT k, SUM(n) / 8, COUNT(*) FROM gev GROUP BY k",
          "SELECT k, SUM(n) / 8 FROM gev GROUP BY k, TUMBLE(ts, 1000)",
          "SELECT k, SUM(n) / 8, COUNT(*) FROM gev GROUP BY k, TUMBLE(ts, 1000)"}) {
        EXPECT_EQ(bind_error_text(sql), "") << sql;
    }
}

TEST(SqlGroupedExpressions, TheOutputSchemaFollowsTheSelectListAndItsTypes) {
    auto plan = bind_grouped(
        "SELECT k, SUM(n) / 8 AS q, CAST(SUM(n) AS DOUBLE) / COUNT(*) AS avg_n, "
        "CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS c, SUM(n) + SUM(m) AS total, "
        "COUNT(*) AS cnt, k + 100 AS k100 FROM gev GROUP BY k");
    auto schema = plan->schema();
    EXPECT_EQ(field_names(*schema),
              (std::vector<std::string>{"k", "q", "avg_n", "c", "total", "cnt", "k100"}));
    EXPECT_TRUE(schema->field(0)->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->field(1)->type()->Equals(*arrow::int64()))
        << "BIGINT / BIGINT is integer division: " << schema->field(1)->type()->ToString();
    EXPECT_TRUE(schema->field(2)->type()->Equals(*arrow::float64()));
    EXPECT_TRUE(schema->field(3)->type()->Equals(*arrow::utf8()));
    EXPECT_TRUE(schema->field(4)->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->field(5)->type()->Equals(*arrow::int64()));
    EXPECT_TRUE(schema->field(6)->type()->Equals(*arrow::int64()));
    // One accumulator per distinct aggregate call: SUM(n) appears three times
    // and COUNT(*) three times, SUM(m) once.
    const auto* agg = find_kind(plan.get(), "Aggregate");
    ASSERT_NE(agg, nullptr) << plan->explain();
    EXPECT_EQ(static_cast<const LogicalAggregate*>(agg)->aggregates().size(), 3u)
        << plan->explain();
}

TEST(SqlGroupedExpressions, AnUnaliasedExpressionTakesAPositionalName) {
    auto plan = bind_grouped("SELECT k, SUM(n) / 8 FROM gev GROUP BY k");
    EXPECT_EQ(field_names(*plan->schema()), (std::vector<std::string>{"k", "_col1"}));
}

TEST(SqlGroupedExpressions, WindowBoundsAreUsableInsideExpressions) {
    auto plan = bind_grouped(
        "SELECT k, window_start AS ws, window_end - window_start AS width, SUM(n) * 2 AS d "
        "FROM gev GROUP BY k, HOP(ts, 2000, 1000)");
    EXPECT_EQ(field_names(*plan->schema()), (std::vector<std::string>{"k", "ws", "width", "d"}));
    EXPECT_NE(find_kind(plan.get(), "WindowAggregate"), nullptr) << plan->explain();
}

TEST(SqlGroupedExpressions, HavingMayNameAnAggregateTheSelectDoesNotCompute) {
    auto plan = bind_grouped("SELECT k FROM gev GROUP BY k HAVING COUNT(*) > 1");
    EXPECT_EQ(field_names(*plan->schema()), (std::vector<std::string>{"k"}));
    const auto* filter = find_kind(plan.get(), "Filter");
    ASSERT_NE(filter, nullptr) << plan->explain();
    // The filter reads the COUNT(*) the aggregate computes for it.
    const auto& pred = static_cast<const LogicalFilter*>(filter)->predicate_json();
    const auto* agg = find_kind(plan.get(), "Aggregate");
    ASSERT_NE(agg, nullptr) << plan->explain();
    const auto& aggs = static_cast<const LogicalAggregate*>(agg)->aggregates();
    ASSERT_EQ(aggs.size(), 1u) << plan->explain();
    EXPECT_EQ(aggs[0].agg_fn, "count");
    EXPECT_NE(pred.find("\"" + aggs[0].output_name + "\""), std::string::npos) << pred;
    auto windowed = bind_grouped(
        "SELECT k, SUM(n) / 4 AS q FROM gev GROUP BY k, SESSION(ts, 500) "
        "HAVING SUM(n) + SUM(m) > 20 AND COUNT(*) > 1");
    EXPECT_EQ(field_names(*windowed->schema()), (std::vector<std::string>{"k", "q"}));
}

TEST(SqlGroupedExpressions, HavingTellsDistinctAggregatesApart) {
    // COUNT(DISTINCT n) is not COUNT(n): HAVING must not reuse the SELECT's
    // COUNT(n) accumulator for it.
    auto plan =
        bind_grouped("SELECT k, COUNT(n) AS c FROM gev GROUP BY k HAVING COUNT(DISTINCT n) > 1");
    const auto* agg = find_kind(plan.get(), "Aggregate");
    ASSERT_NE(agg, nullptr) << plan->explain();
    const auto& aggs = static_cast<const LogicalAggregate*>(agg)->aggregates();
    ASSERT_EQ(aggs.size(), 2u) << plan->explain();
    EXPECT_NE(aggs[0].distinct, aggs[1].distinct);
}

TEST(SqlGroupedExpressions, AnAggregateInsideCaseOrCastInHavingIsRewritten) {
    for (const char* sql :
         {"SELECT k, SUM(n) AS t FROM gev GROUP BY k HAVING CASE WHEN SUM(n) > 1 THEN 1 "
          "ELSE 0 END = 1",
          "SELECT k, SUM(n) AS t FROM gev GROUP BY k HAVING CAST(SUM(n) AS DOUBLE) > 1.5",
          "SELECT k, SUM(n) AS t FROM gev GROUP BY k HAVING COALESCE(MAX(m), 0) < SUM(n)"}) {
        auto plan = bind_grouped(sql);
        const auto* filter = find_kind(plan.get(), "Filter");
        ASSERT_NE(filter, nullptr) << sql;
        const auto& pred = static_cast<const LogicalFilter*>(filter)->predicate_json();
        EXPECT_EQ(pred.find("\"sum\""), std::string::npos)
            << "an aggregate call survived into the post-aggregate predicate: " << pred;
    }
}

TEST(SqlGroupedExpressions, ShapesClinkCannotSupportAreRefusedByName) {
    struct Case {
        const char* sql;
        const char* must_mention;
    };
    const std::vector<Case> cases = {
        // Not standard SQL either: n is neither grouped nor aggregated.
        {"SELECT k, n + SUM(n) FROM gev GROUP BY k", "'n' must appear in GROUP BY"},
        {"SELECT k, SUM(n) FROM gev GROUP BY k HAVING m > 1", "'m' must appear in GROUP BY"},
        // An aggregate argument must be a column reference.
        {"SELECT k, SUM(n * 2) FROM gev GROUP BY k", "argument must be a column reference"},
        {"SELECT k, SUM(n * 2) + 1 FROM gev GROUP BY k", "argument must be a column reference"},
        {"SELECT k, SUM(SUM(n)) FROM gev GROUP BY k", "argument must be a column reference"},
        // A window function cannot sit inside a grouped expression.
        {"SELECT k, SUM(n) + ROW_NUMBER() OVER (ORDER BY ts) FROM gev GROUP BY k",
         "window function"},
        // window_start / window_end exist only under a window TVF.
        {"SELECT k, window_end - window_start, SUM(n) FROM gev GROUP BY k", "window TVF"},
        // An aggregate expression still needs a GROUP BY in a streaming SELECT.
        {"SELECT SUM(n) / 8 FROM gev", "require a GROUP BY"},
        {"SELECT k FROM gev HAVING COUNT(*) > 1", "GROUP BY"},
        // A bare boolean expression is not a predicate shape clink lowers, in
        // HAVING as in WHERE; compare it explicitly instead.
        {"SELECT k, SUM(n) FROM gev GROUP BY k HAVING CASE WHEN SUM(n) > 1 THEN TRUE ELSE FALSE "
         "END",
         "predicate kind not supported"},
    };
    for (const auto& c : cases) {
        const auto err = bind_error_text(c.sql);
        EXPECT_NE(err.find(c.must_mention), std::string::npos)
            << "query: " << c.sql << "\n  error: " << (err.empty() ? "(bound)" : err);
    }
}

TEST(SqlGroupedExpressions, AnAggregateAliasDoesNotShadowAnUngroupedColumnInTheSelect) {
    // gev has a column m. An aggregate aliased m is a name HAVING may use, but
    // in another SELECT item m is still the source column, which is neither
    // grouped nor aggregated. Standard SQL refuses both item orders.
    for (const char* sql : {"SELECT k, SUM(n) AS m, m + 1 AS x FROM gev GROUP BY k",
                            "SELECT m + 1 AS x, k, SUM(n) AS m FROM gev GROUP BY k",
                            "SELECT k, SUM(n) AS m, m + 1 AS x FROM gev GROUP BY k, TUMBLE(ts, "
                            "1000)"}) {
        const auto err = bind_error_text(sql);
        EXPECT_NE(err.find("'m' must appear in GROUP BY"), std::string::npos)
            << "query: " << sql << "\n  error: " << (err.empty() ? "(bound)" : err);
    }
    // HAVING still resolves the alias to the aggregate on this path.
    auto plan =
        bind_grouped("SELECT k, SUM(n) AS m, SUM(n) / 2 AS h FROM gev GROUP BY k HAVING m > 1");
    const auto* filter = find_kind(plan.get(), "Filter");
    ASSERT_NE(filter, nullptr) << plan->explain();
    const auto* agg = find_kind(plan.get(), "Aggregate");
    ASSERT_NE(agg, nullptr) << plan->explain();
    const auto& aggs = static_cast<const LogicalAggregate*>(agg)->aggregates();
    ASSERT_EQ(aggs.size(), 1u) << plan->explain();
    const auto& pred = static_cast<const LogicalFilter*>(filter)->predicate_json();
    EXPECT_NE(pred.find("\"" + aggs[0].output_name + "\""), std::string::npos) << pred;
}

TEST(SqlGroupedExpressions, PlainAggregateQueriesKeepTheirPlanShape) {
    // The new path is taken only when a SELECT item is an expression or HAVING
    // needs an aggregate the SELECT does not compute; a query whose results
    // were already correct keeps the plan (and so the job-graph fingerprint)
    // it had.
    auto plain = bind_grouped("SELECT k, SUM(n) AS t, COUNT(*) AS c FROM gev GROUP BY k");
    EXPECT_EQ(plain->kind(), "Aggregate");
    auto having = bind_grouped("SELECT k, SUM(n) AS t FROM gev GROUP BY k HAVING SUM(n) > 10");
    ASSERT_EQ(having->kind(), "Filter");
    EXPECT_EQ(static_cast<const LogicalFilter&>(*having).input().kind(), "Aggregate");
    auto windowed = bind_grouped(
        "SELECT k, COUNT(*) AS c FROM gev GROUP BY TUMBLE(ts, 1000), k HAVING "
        "COUNT(*) > 1");
    ASSERT_EQ(windowed->kind(), "Filter");
    EXPECT_EQ(static_cast<const LogicalFilter&>(*windowed).input().kind(), "WindowAggregate");
}

// --- End to end -------------------------------------------------------------
//
// Five events, two keys. Offsetting ts by 10000 keeps every hopping window's
// start positive. Per key:
//   k=1: (n=10, m=1, ts=10100), (n=30, m=2, ts=10300), (n=7, m=6, ts=11600)
//   k=2: (n=20, m=4, ts=10200), (n=5,  m=3, ts=11500)

class GroupedExpressionRun : public ::testing::Test {
protected:
    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        dir_ = fs::temp_directory_path() /
               ("clink_grouped_expr_" + std::to_string(::getpid()) + "_" + info->name());
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        std::ofstream in(dir_ / "in.ndjson", std::ios::trunc);
        in << R"({"k":1,"n":10,"m":1,"ts":10100})" << "\n"
           << R"({"k":2,"n":20,"m":4,"ts":10200})" << "\n"
           << R"({"k":1,"n":30,"m":2,"ts":10300})" << "\n"
           << R"({"k":2,"n":5,"m":3,"ts":11500})" << "\n"
           << R"({"k":1,"n":7,"m":6,"ts":11600})" << "\n";
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    // Run `INSERT INTO o <select>` with `o` declared as `sink_cols`; return the
    // sink's lines as JSON objects, in file order.
    std::vector<clink::config::JsonValue> run(const std::string& sink_cols,
                                              const std::string& select) {
        const fs::path out = dir_ / ("out" + std::to_string(runs_++) + ".ndjson");
        const std::string script =
            "CREATE TABLE ev (k BIGINT, n BIGINT, m BIGINT, ts BIGINT) WITH (connector='file', "
            "format='json', path='" +
            (dir_ / "in.ndjson").string() +
            "', event_time_column='ts', watermark_lag_ms='0');"
            "CREATE TABLE o (" +
            sink_cols + ") WITH (connector='file', format='json', path='" + out.string() +
            "');"
            "INSERT INTO o " +
            select;
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        const int rc = engine.execute_script(script);
        EXPECT_EQ(rc, 0) << "clink rejected the script: " << err.str() << "\n  " << select;
        if (rc != 0) {
            return {};
        }
        EXPECT_TRUE(engine.await_all()) << "job failed: " << err.str();
        std::vector<clink::config::JsonValue> rows;
        std::ifstream f(out);
        std::string line;
        while (std::getline(f, line)) {
            if (!line.empty()) {
                rows.push_back(clink::config::parse(line));
            }
        }
        return rows;
    }

    fs::path dir_;
    int runs_ = 0;
};

// One output row rendered as "col=value|col=value" over `cols`, so whole rows
// compare as strings. Integral numbers print without a fraction.
std::string render(const clink::config::JsonValue& row, const std::vector<std::string>& cols) {
    std::string out;
    for (const auto& c : cols) {
        if (!out.empty()) {
            out += '|';
        }
        out += c + '=';
        const auto& obj = row.as_object();
        const auto it = obj.find(c);
        if (it == obj.end() || it->second.is_null()) {
            out += "null";
        } else if (it->second.is_number()) {
            const double d = it->second.as_number();
            char buf[64];
            if (d == static_cast<double>(static_cast<std::int64_t>(d))) {
                std::snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(d));
            } else {
                std::snprintf(buf, sizeof(buf), "%.6g", d);
            }
            out += buf;
        } else if (it->second.is_string()) {
            out += it->second.as_string();
        } else {
            out += it->second.serialize(0);
        }
    }
    return out;
}

std::multiset<std::string> rendered(const std::vector<clink::config::JsonValue>& rows,
                                    const std::vector<std::string>& cols) {
    std::multiset<std::string> out;
    for (const auto& r : rows) {
        out.insert(render(r, cols));
    }
    return out;
}

// An unbounded GROUP BY emits the running row per input; the last emission per
// key is the final answer (parallelism 1 keeps per-key order).
std::multiset<std::string> finals_by_key(const std::vector<clink::config::JsonValue>& rows,
                                         const std::vector<std::string>& cols) {
    std::map<std::string, std::string> last;
    for (const auto& r : rows) {
        last[render(r, {cols[0]})] = render(r, cols);
    }
    std::multiset<std::string> out;
    for (const auto& [_, v] : last) {
        out.insert(v);
    }
    return out;
}

const std::vector<std::string> kExprCols{"k", "q", "total", "c", "avg_n", "spread"};
const std::string kExprSink =
    "k BIGINT, q BIGINT, total BIGINT, c VARCHAR, avg_n DOUBLE, spread BIGINT";
const std::string kExprSelect =
    "SELECT k, SUM(n) / 4 AS q, SUM(n) + SUM(m) AS total, "
    "CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS c, "
    "CAST(SUM(n) AS DOUBLE) / COUNT(*) AS avg_n, ABS(MIN(n) - MAX(n)) AS spread FROM ev ";

TEST_F(GroupedExpressionRun, TheReportedQueryRunsOnAPlainGroupBy) {
    // The exact shape from the report, unaliased: SUM(n) / 8 is integer
    // division, so k=1 -> 47 / 8 = 5 and k=2 -> 25 / 8 = 3.
    const auto rows = run("k BIGINT, x BIGINT", "SELECT k, SUM(n) / 8 FROM ev GROUP BY k");
    EXPECT_EQ(finals_by_key(rows, {"k", "x"}), (std::multiset<std::string>{"k=1|x=5", "k=2|x=3"}));
}

TEST_F(GroupedExpressionRun, TheReportedQueryRunsOverATumblingWindow) {
    const auto rows =
        run("k BIGINT, x BIGINT", "SELECT k, SUM(n) / 8 FROM ev GROUP BY k, TUMBLE(ts, 1000)");
    // [10000, 11000): k=1 40 -> 5, k=2 20 -> 2; [11000, 12000): k=1 7 -> 0, k=2 5 -> 0.
    EXPECT_EQ(rendered(rows, {"k", "x"}),
              (std::multiset<std::string>{"k=1|x=5", "k=2|x=2", "k=1|x=0", "k=2|x=0"}));
}

TEST_F(GroupedExpressionRun, ExpressionsOverAggregatesOnAPlainGroupBy) {
    const auto rows = run(kExprSink, kExprSelect + "GROUP BY k");
    EXPECT_EQ(finals_by_key(rows, kExprCols),
              (std::multiset<std::string>{
                  "k=1|q=11|total=56|c=many|avg_n=15.6667|spread=23",
                  "k=2|q=6|total=32|c=many|avg_n=12.5|spread=15",
              }));
}

TEST_F(GroupedExpressionRun, ExpressionsOverAggregatesOnATumblingWindow) {
    auto cols = kExprCols;
    cols.insert(cols.begin() + 1, {"ws", "width"});
    const auto rows =
        run("k BIGINT, ws BIGINT, width BIGINT, q BIGINT, total BIGINT, c VARCHAR, "
            "avg_n DOUBLE, spread BIGINT",
            "SELECT k, window_start AS ws, window_end - window_start AS width, "
            "SUM(n) / 4 AS q, SUM(n) + SUM(m) AS total, "
            "CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS c, "
            "CAST(SUM(n) AS DOUBLE) / COUNT(*) AS avg_n, "
            "ABS(MIN(n) - MAX(n)) AS spread FROM ev GROUP BY k, TUMBLE(ts, 1000)");
    EXPECT_EQ(rendered(rows, cols),
              (std::multiset<std::string>{
                  "k=1|ws=10000|width=1000|q=10|total=43|c=many|avg_n=20|spread=20",
                  "k=2|ws=10000|width=1000|q=5|total=24|c=one|avg_n=20|spread=0",
                  "k=1|ws=11000|width=1000|q=1|total=13|c=one|avg_n=7|spread=0",
                  "k=2|ws=11000|width=1000|q=1|total=8|c=one|avg_n=5|spread=0",
              }));
}

TEST_F(GroupedExpressionRun, ExpressionsOverAggregatesOnAHoppingWindow) {
    auto cols = kExprCols;
    cols.insert(cols.begin() + 1, {"ws", "width"});
    const auto rows =
        run("k BIGINT, ws BIGINT, width BIGINT, q BIGINT, total BIGINT, c VARCHAR, "
            "avg_n DOUBLE, spread BIGINT",
            "SELECT k, window_start AS ws, window_end - window_start AS width, "
            "SUM(n) / 4 AS q, SUM(n) + SUM(m) AS total, "
            "CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS c, "
            "CAST(SUM(n) AS DOUBLE) / COUNT(*) AS avg_n, "
            "ABS(MIN(n) - MAX(n)) AS spread FROM ev GROUP BY k, HOP(ts, 2000, 1000)");
    EXPECT_EQ(rendered(rows, cols),
              (std::multiset<std::string>{
                  "k=1|ws=9000|width=2000|q=10|total=43|c=many|avg_n=20|spread=20",
                  "k=2|ws=9000|width=2000|q=5|total=24|c=one|avg_n=20|spread=0",
                  "k=1|ws=10000|width=2000|q=11|total=56|c=many|avg_n=15.6667|spread=23",
                  "k=2|ws=10000|width=2000|q=6|total=32|c=many|avg_n=12.5|spread=15",
                  "k=1|ws=11000|width=2000|q=1|total=13|c=one|avg_n=7|spread=0",
                  "k=2|ws=11000|width=2000|q=1|total=8|c=one|avg_n=5|spread=0",
              }));
}

TEST_F(GroupedExpressionRun, ExpressionsOverAggregatesOnACumulatingWindow) {
    // CUMULATE(ts, step 1000, size 2000): one window from 10000, emitted at
    // 11000 (the first two k=1 rows, the first k=2 row) and at 12000 (all).
    auto cols = kExprCols;
    cols.insert(cols.begin() + 1, {"ws", "width"});
    const auto rows =
        run("k BIGINT, ws BIGINT, width BIGINT, q BIGINT, total BIGINT, c VARCHAR, "
            "avg_n DOUBLE, spread BIGINT",
            "SELECT k, window_start AS ws, window_end - window_start AS width, "
            "SUM(n) / 4 AS q, SUM(n) + SUM(m) AS total, "
            "CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END AS c, "
            "CAST(SUM(n) AS DOUBLE) / COUNT(*) AS avg_n, "
            "ABS(MIN(n) - MAX(n)) AS spread FROM ev GROUP BY k, CUMULATE(ts, 1000, 2000)");
    EXPECT_EQ(rendered(rows, cols),
              (std::multiset<std::string>{
                  "k=1|ws=10000|width=1000|q=10|total=43|c=many|avg_n=20|spread=20",
                  "k=2|ws=10000|width=1000|q=5|total=24|c=one|avg_n=20|spread=0",
                  "k=1|ws=10000|width=2000|q=11|total=56|c=many|avg_n=15.6667|spread=23",
                  "k=2|ws=10000|width=2000|q=6|total=32|c=many|avg_n=12.5|spread=15",
              }));
}

TEST_F(GroupedExpressionRun, ExpressionsOverAggregatesOnASessionWindow) {
    // Gap 500: k=1 has sessions {10100, 10300} and {11600}; k=2 has {10200}
    // and {11500}.
    const auto rows = run(kExprSink, kExprSelect + "GROUP BY k, SESSION(ts, 500)");
    EXPECT_EQ(rendered(rows, kExprCols),
              (std::multiset<std::string>{
                  "k=1|q=10|total=43|c=many|avg_n=20|spread=20",
                  "k=1|q=1|total=13|c=one|avg_n=7|spread=0",
                  "k=2|q=5|total=24|c=one|avg_n=20|spread=0",
                  "k=2|q=1|total=8|c=one|avg_n=5|spread=0",
              }));
}

TEST_F(GroupedExpressionRun, HavingOverExpressionsAndHiddenAggregatesOnAPlainGroupBy) {
    // SUM(n) / 8 > 3: k=1 ends at 5 and passes, k=2 never exceeds 3.
    const auto by_expr = run("k BIGINT, q BIGINT",
                             "SELECT k, SUM(n) / 8 AS q FROM ev GROUP BY k HAVING SUM(n) / 8 > 3");
    EXPECT_EQ(finals_by_key(by_expr, {"k", "q"}), (std::multiset<std::string>{"k=1|q=5"}));
    // An aggregate only HAVING names: COUNT(*) reaches 3 for k=1 alone.
    const auto hidden =
        run("k BIGINT, mx BIGINT", "SELECT k, MAX(n) AS mx FROM ev GROUP BY k HAVING COUNT(*) > 2");
    EXPECT_EQ(finals_by_key(hidden, {"k", "mx"}), (std::multiset<std::string>{"k=1|mx=30"}));
}

TEST_F(GroupedExpressionRun, HavingOnAGroupKeyTheSelectAliases) {
    // The aggregate emits a key under its SELECT alias, so HAVING k > 1 must
    // read the column it is emitted as. It read `k`, found nothing, and
    // filtered every row out: k=2 (total 25) vanished.
    const auto rows =
        run("kk BIGINT, t BIGINT", "SELECT k AS kk, SUM(n) AS t FROM ev GROUP BY k HAVING k > 1");
    EXPECT_EQ(finals_by_key(rows, {"kk", "t"}), (std::multiset<std::string>{"kk=2|t=25"}));
}

TEST_F(GroupedExpressionRun, HavingOverHiddenAggregatesOnEveryWindowKind) {
    const std::vector<std::string> cols{"k", "q"};
    const auto tumble =
        run("k BIGINT, q BIGINT",
            "SELECT k, SUM(n) / 4 AS q FROM ev GROUP BY k, TUMBLE(ts, 1000) HAVING COUNT(*) > 1");
    EXPECT_EQ(rendered(tumble, cols), (std::multiset<std::string>{"k=1|q=10"}));
    const auto hop = run(
        "k BIGINT, q BIGINT",
        "SELECT k, SUM(n) / 4 AS q FROM ev GROUP BY k, HOP(ts, 2000, 1000) HAVING COUNT(*) > 1");
    EXPECT_EQ(rendered(hop, cols), (std::multiset<std::string>{"k=1|q=10", "k=1|q=11", "k=2|q=6"}));
    const auto cumulate = run("k BIGINT, q BIGINT",
                              "SELECT k, SUM(n) / 4 AS q FROM ev GROUP BY k, "
                              "CUMULATE(ts, 1000, 2000) HAVING COUNT(*) > 1");
    EXPECT_EQ(rendered(cumulate, cols),
              (std::multiset<std::string>{"k=1|q=10", "k=1|q=11", "k=2|q=6"}));
    const auto session = run("k BIGINT, q BIGINT",
                             "SELECT k, SUM(n) / 4 AS q FROM ev GROUP BY k, SESSION(ts, 500) "
                             "HAVING SUM(n) + SUM(m) > 20");
    EXPECT_EQ(rendered(session, cols), (std::multiset<std::string>{"k=1|q=10", "k=2|q=5"}));
}

}  // namespace

}  // namespace clink::sql
