// SQL runtime metric helpers + end-to-end parse/bind/optimize/plan
// counter emission.

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/sql_metrics.hpp"
#include "clink/sql/parser.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

// Per-phase SQL durations are histograms now (OBS-1b).
double hist_sum(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().sum;
}

}  // namespace

TEST(SqlMetrics, HelperAccumulation) {
    namespace mm = clink::metrics;
    const auto p_before = counter_value(mm::kSqlParses);
    const auto bs_before = counter_value(mm::kSqlBinds);
    const auto op_before = counter_value(mm::kSqlOptimizes);
    const auto pp_before = counter_value(mm::kSqlPhysicalPlans);

    mm::sql::parse_completed(1000);
    mm::sql::bind_completed(2000);
    mm::sql::optimize_completed(3000);
    mm::sql::physical_plan_completed(4000);
    mm::sql::parse_failed();
    mm::sql::bind_failed();

    EXPECT_EQ(counter_value(mm::kSqlParses) - p_before, 1u);
    EXPECT_EQ(counter_value(mm::kSqlBinds) - bs_before, 1u);
    EXPECT_EQ(counter_value(mm::kSqlOptimizes) - op_before, 1u);
    EXPECT_EQ(counter_value(mm::kSqlPhysicalPlans) - pp_before, 1u);
    EXPECT_GE(counter_value(mm::kSqlParseErrors), 1u);
    EXPECT_GE(counter_value(mm::kSqlBindErrors), 1u);
    EXPECT_GE(hist_sum(mm::kSqlParseNs), 1000.0);
}

TEST(SqlMetrics, ParserIncrementsParsesTotal) {
    const auto p_before = counter_value(clink::metrics::kSqlParses);
    auto script = clink::sql::parse("SELECT 1");
    EXPECT_EQ(counter_value(clink::metrics::kSqlParses) - p_before, 1u);
}

TEST(SqlMetrics, ParserIncrementsErrorOnSyntaxFailure) {
    const auto err_before = counter_value(clink::metrics::kSqlParseErrors);
    EXPECT_THROW(clink::sql::parse("SELECT FROM"), clink::sql::ParseError);
    EXPECT_EQ(counter_value(clink::metrics::kSqlParseErrors) - err_before, 1u);
}
