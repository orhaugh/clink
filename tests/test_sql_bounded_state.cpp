// SQL bounded-state gate.
//
// A windowless GROUP BY over an unbounded source keeps one accumulator per
// group for the life of the job; SELECT DISTINCT keeps every distinct row
// it has ever seen; an unwindowed join keeps both sides. Over a bounded
// input all three are fine. Over an unbounded one they are an
// out-of-memory incident with a delay fuse, and clink used to accept them
// without a word.
//
// The gate refuses them unless the query carries a bounded input, a
// window, an explicit `state_ttl`, or `ALLOW UNBOUNDED STATE`. These tests
// pin each of those four routes plus the shape of the diagnostic, because
// a rejection a user cannot act on is only marginally better than silence.

#include <string>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/bounded_state.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/preparse.hpp"

namespace {
// The SQL runtime registrations must exist before a plan can compile.
//
// Also declares a capability record for `kafka`. The gate reads
// boundedness from the capability registry, and a connector declares
// itself from its own install() - which this binary does not call, because
// clink_sql_tests links clink::sql + clink::core and no impls. Without the
// declaration Kafka is UNKNOWN, and unknown deliberately does not trip the
// gate, so the rejection tests would silently pass for the wrong reason.
// Supplying the fact under test is the point.
void ensure_sql_installed_for_bounded_state_tests() {
    static const bool once = [] {
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
        clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
            .name = "kafka",
            .version = "test",
            .is_source = true,
            .is_sink = true,
            .boundedness = clink::connectors::Boundedness::Unbounded,
            .replayable = true,
            .offset_model = clink::connectors::OffsetModel::LogOffset,
            .checkpoint_integrated = true,
            .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        });
        return true;
    }();
    (void)once;
}
}  // namespace

namespace {

using namespace clink::sql;

// --- retention parsing ------------------------------------------------------

TEST(SqlBoundedState, RetentionAcceptsTheUnitsUsersActuallyWrite) {
    EXPECT_EQ(parse_retention_ms(""), 0);
    EXPECT_EQ(parse_retention_ms("500"), 500);  // bare = milliseconds
    EXPECT_EQ(parse_retention_ms("500ms"), 500);
    EXPECT_EQ(parse_retention_ms("30s"), 30'000);
    EXPECT_EQ(parse_retention_ms("15m"), 900'000);
    EXPECT_EQ(parse_retention_ms("1h"), 3'600'000);
    EXPECT_EQ(parse_retention_ms("7d"), 604'800'000);
    EXPECT_EQ(parse_retention_ms(" 2 hours "), 7'200'000);
}

TEST(SqlBoundedState, AMistypedRetentionThrowsRatherThanSilentlyMeaningNoRetention) {
    // The failure that matters: "1hour" quietly parsed as 0 would disable
    // the gate for a query whose author believed they had set a bound.
    EXPECT_THROW(parse_retention_ms("soon"), std::invalid_argument);
    EXPECT_THROW(parse_retention_ms("1 fortnight"), std::invalid_argument);
    EXPECT_THROW(parse_retention_ms("h"), std::invalid_argument);
}

// --- which node kinds are gated ---------------------------------------------

TEST(SqlBoundedState, OnlyTrulyUnboundedNodeKindsAreFlagged) {
    // Gated: they retain per key for the life of the job.
    EXPECT_TRUE(retains_unbounded_state("Aggregate"));
    EXPECT_TRUE(retains_unbounded_state("Distinct"));
    EXPECT_TRUE(retains_unbounded_state("EquiJoin"));
    EXPECT_TRUE(retains_unbounded_state("SemiJoin"));
    EXPECT_TRUE(retains_unbounded_state("SetOp"));

    // NOT gated: a windowed aggregate releases its state when the window
    // fires, TopN-per-key is bounded by N, an interval join is bounded by
    // its time condition, and a lookup join keeps nothing. Flagging these
    // would be noise, and a gate that cries wolf gets switched off.
    EXPECT_FALSE(retains_unbounded_state("WindowAggregate"));
    EXPECT_FALSE(retains_unbounded_state("TopNPerKey"));
    EXPECT_FALSE(retains_unbounded_state("IntervalJoin"));
    EXPECT_FALSE(retains_unbounded_state("LookupJoin"));
    EXPECT_FALSE(retains_unbounded_state("Project"));
    EXPECT_FALSE(retains_unbounded_state("Filter"));
    EXPECT_FALSE(retains_unbounded_state("Scan"));
}

// --- the ALLOW UNBOUNDED STATE clause ---------------------------------------

TEST(SqlBoundedState, TheOverrideClauseIsStrippedAndReported) {
    const auto r = preparse::preparse("SELECT k FROM t GROUP BY k ALLOW UNBOUNDED STATE");
    EXPECT_TRUE(r.allow_unbounded_state);
    // Stripped, so libpg_query never sees a clause it cannot parse.
    EXPECT_EQ(r.rewritten_sql.find("ALLOW"), std::string::npos);
    EXPECT_NE(r.rewritten_sql.find("GROUP BY k"), std::string::npos);
}

TEST(SqlBoundedState, TheOverrideClauseIsCaseAndWhitespaceInsensitive) {
    for (const auto* sql : {"SELECT 1 allow unbounded state",
                            "SELECT 1 Allow   Unbounded\n  State",
                            "SELECT 1 ALLOW UNBOUNDED STATE;",
                            "SELECT 1 ALLOW UNBOUNDED STATE ; "}) {
        EXPECT_TRUE(preparse::preparse(sql).allow_unbounded_state) << sql;
    }
}

TEST(SqlBoundedState, TheOverrideClauseIsNotMatchedInsideAnIdentifier) {
    // "...FROM tallow unbounded state" must not read as the clause. The
    // matcher requires a word boundary before ALLOW; without that, a table
    // whose name happens to end in "allow" would silently disable the gate.
    const auto r = preparse::preparse("SELECT 1 FROM tallow unbounded state");
    EXPECT_FALSE(r.allow_unbounded_state);
}

TEST(SqlBoundedState, AQueryWithoutTheClauseIsUnaffected) {
    const auto r = preparse::preparse("SELECT k FROM t GROUP BY k");
    EXPECT_FALSE(r.allow_unbounded_state);
    EXPECT_NE(r.rewritten_sql.find("GROUP BY k"), std::string::npos);
}

TEST(SqlBoundedState, TheOverrideSurvivesParseOntoTheScript) {
    const auto script = parse("SELECT 1 ALLOW UNBOUNDED STATE");
    EXPECT_TRUE(script.allow_unbounded_state);
    EXPECT_FALSE(parse("SELECT 1").allow_unbounded_state);
}

// --- the report -------------------------------------------------------------

TEST(SqlBoundedState, TheRejectionNamesTheConstructAndAWayOut) {
    BoundedStateReport r;
    r.findings.push_back(UnboundedStateFinding{
        .node_kind = "Aggregate",
        .description = "one accumulator per group, kept for the life of the job",
        .remedy = "add a window (TUMBLE / HOP / SESSION), set 'state_ttl', or write ALLOW "
                  "UNBOUNDED STATE"});
    const auto msg = r.error_message();
    // Actionable: what is wrong, what it costs, and what to do instead.
    EXPECT_NE(msg.find("Aggregate"), std::string::npos);
    EXPECT_NE(msg.find("one accumulator per group"), std::string::npos);
    EXPECT_NE(msg.find("TUMBLE"), std::string::npos);
    EXPECT_NE(msg.find("ALLOW UNBOUNDED STATE"), std::string::npos);
    EXPECT_FALSE(r.ok());
}

TEST(SqlBoundedState, AnEmptyReportHasNoMessage) {
    const BoundedStateReport r;
    EXPECT_TRUE(r.ok());
    EXPECT_TRUE(r.error_message().empty());
}

// --- end-to-end through the planner -----------------------------------------

namespace {

// Compile a script and return the rejection message, or "" if it compiled.
std::string compile_error(const std::string& ddl, const std::string& query) {
    clink::cluster::ensure_built_ins_registered();
    ensure_sql_installed_for_bounded_state_tests();
    Catalog cat;
    const auto parsed_ddl = parse(ddl);
    for (const auto& st : parsed_ddl.statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto script = parse(query);
    try {
        const Binder binder(cat);
        auto plan = binder.bind_insert(std::get<ast::InsertStmt>(script.statements.front()));
        PhysicalPlanner planner;
        planner.set_allow_unbounded_state(script.allow_unbounded_state);
        (void)planner.compile(*dynamic_cast<const LogicalSink*>(plan.get()));
        return {};
    } catch (const std::exception& e) {
        return e.what();
    }
}

}  // namespace

TEST(SqlBoundedState, AWindowlessGroupByOverAKnownUnboundedSourceIsRejected) {
    const auto err = compile_error(
        "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', "
        "topic='t', bootstrap_servers='localhost:9092');"
        "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/o');",
        "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k");
    ASSERT_FALSE(err.empty()) << "a windowless GROUP BY over Kafka must be refused";
    EXPECT_NE(err.find("Aggregate"), std::string::npos);
    EXPECT_NE(err.find("ALLOW UNBOUNDED STATE"), std::string::npos);
}

TEST(SqlBoundedState, TheSameQueryOverABoundedSourceIsAccepted) {
    // A file source ends, so every accumulator is released at end of
    // stream. The gate asks about the SOURCE, it does not ban the
    // construct.
    EXPECT_EQ(compile_error(
                  "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='file', format='json', "
                  "path='/tmp/in.ndjson');"
                  "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
                  "path='/tmp/o');",
                  "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k"),
              "");
}

TEST(SqlBoundedState, AnExplicitStateTtlUnlocksTheSameUnboundedQuery) {
    EXPECT_EQ(compile_error(
                  "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', "
                  "topic='t', bootstrap_servers='localhost:9092', state_ttl='1h');"
                  "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
                  "path='/tmp/o');",
                  "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k"),
              "")
        << "an explicit retention should satisfy the gate";
}

TEST(SqlBoundedState, TheOverrideClauseUnlocksTheSameUnboundedQuery) {
    EXPECT_EQ(compile_error(
                  "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', "
                  "topic='t', bootstrap_servers='localhost:9092');"
                  "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
                  "path='/tmp/o');",
                  "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k ALLOW UNBOUNDED STATE"),
              "")
        << "the documented escape hatch does not work";
}

TEST(SqlBoundedState, RetentionIsSatisfiedByAnyOfTheFourRoutes) {
    EXPECT_TRUE(StateRetention{.ttl_ms = 1000}.bounded());
    EXPECT_TRUE(StateRetention{.allow_unbounded = true}.bounded());
    EXPECT_FALSE(StateRetention{}.bounded());
}

}  // namespace
