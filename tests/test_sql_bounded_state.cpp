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

#include <map>
#include <sstream>
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
#include "clink/sql/script_runner.hpp"

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
        // Built-ins first: install() registers Row-channel operators, and
        // register_operator<In,Out> throws unless register_type<Row> has
        // already run.
        clink::cluster::ensure_built_ins_registered();
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

// --- state_ttl reaches the operator ------------------------------------------

namespace {

// Compile and return the emitted aggregate op's params, so a test can
// assert what the RUNTIME will actually be told.
std::map<std::string, std::string> aggregate_params(const std::string& ddl,
                                                    const std::string& query) {
    clink::cluster::ensure_built_ins_registered();
    ensure_sql_installed_for_bounded_state_tests();
    Catalog cat;
    for (const auto& st : parse(ddl).statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto script = parse(query);
    const Binder binder(cat);
    auto plan = binder.bind_insert(std::get<ast::InsertStmt>(script.statements.front()));
    PhysicalPlanner planner;
    planner.set_allow_unbounded_state(script.allow_unbounded_state);
    const auto spec = planner.compile(*dynamic_cast<const LogicalSink*>(plan.get()));
    for (const auto& op : spec.ops) {
        if (op.type == "aggregate_row") {
            return op.params;
        }
    }
    return {};
}

const char* kKafkaSrcWithTtl =
    "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', topic='t', "
    "bootstrap_servers='localhost:9092', state_ttl='1h');"
    "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', path='/tmp/o');";

}  // namespace

TEST(SqlBoundedState, StateTtlReachesTheAggregateOperatorNotJustTheGate) {
    // The gap this closes: `state_ttl` used to satisfy the gate and change
    // nothing at runtime, declaring an intent nothing acted on. The
    // operator has to be TOLD.
    const auto params =
        aggregate_params(kKafkaSrcWithTtl, "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k");
    ASSERT_FALSE(params.empty()) << "no aggregate_row operator was emitted";
    ASSERT_TRUE(params.count("state_ttl_ms")) << "the aggregate was not given the retention, so "
                                                 "the declared TTL would do nothing at runtime";
    EXPECT_EQ(params.at("state_ttl_ms"), "3600000");
    // Event time by default: a processing-time TTL on a backfill expires
    // everything the instant it is written.
    EXPECT_EQ(params.at("state_ttl_domain"), "event_time");
}

TEST(SqlBoundedState, TheDomainIsSelectableAndProcessingTimeIsHonoured) {
    const std::string ddl =
        "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', topic='t', "
        "bootstrap_servers='localhost:9092', state_ttl='30s', "
        "state_ttl_domain='processing_time');"
        "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/o');";
    const auto params =
        aggregate_params(ddl, "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k");
    ASSERT_TRUE(params.count("state_ttl_ms"));
    EXPECT_EQ(params.at("state_ttl_ms"), "30000");
    EXPECT_EQ(params.at("state_ttl_domain"), "processing_time");
}

TEST(SqlBoundedState, NoRetentionMeansNoParamsRatherThanAZero) {
    // A job that never asked for retention must be byte-identical to
    // before: absent, not "state_ttl_ms=0".
    const std::string ddl =
        "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/in');"
        "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/o');";
    const auto params =
        aggregate_params(ddl, "INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k");
    ASSERT_FALSE(params.empty());
    EXPECT_EQ(params.count("state_ttl_ms"), 0U);
    EXPECT_EQ(params.count("state_ttl_domain"), 0U);
}

TEST(SqlBoundedState, TheShortestDeclaredRetentionAcrossTheInputsWins) {
    // Taking the longest would let a generous setting on one table
    // silently relax a strict one on another.
    const std::string ddl =
        "CREATE TABLE a (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', topic='a', "
        "bootstrap_servers='localhost:9092', state_ttl='6h');"
        "CREATE TABLE b (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', topic='b', "
        "bootstrap_servers='localhost:9092', state_ttl='10m');"
        "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/o');";
    const auto params = aggregate_params(
        ddl,
        "INSERT INTO dst SELECT k, SUM(v) FROM (SELECT k, v FROM a UNION ALL SELECT k, v "
        "FROM b) u GROUP BY k");
    ASSERT_TRUE(params.count("state_ttl_ms"));
    EXPECT_EQ(params.at("state_ttl_ms"), "600000") << "the longer retention won";
}

namespace {

// Params of the first op of `type` in the compiled plan.
std::map<std::string, std::string> op_params(const std::string& ddl,
                                             const std::string& query,
                                             const std::string& type) {
    clink::cluster::ensure_built_ins_registered();
    ensure_sql_installed_for_bounded_state_tests();
    Catalog cat;
    for (const auto& st : parse(ddl).statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto script = parse(query);
    const Binder binder(cat);
    auto plan = binder.bind_insert(std::get<ast::InsertStmt>(script.statements.front()));
    PhysicalPlanner planner;
    planner.set_allow_unbounded_state(script.allow_unbounded_state);
    const auto spec = planner.compile(*dynamic_cast<const LogicalSink*>(plan.get()));
    for (const auto& op : spec.ops) {
        if (op.type == type) {
            return op.params;
        }
    }
    return {};
}

std::string kafka_table(const std::string& name, const std::string& topic) {
    return "CREATE TABLE " + name +
           " (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', topic='" + topic +
           "', bootstrap_servers='localhost:9092', state_ttl='1h');";
}
const char* kFileDst =
    "CREATE TABLE dst (k BIGINT, v BIGINT) WITH (connector='file', format='json', path='/tmp/o');";

}  // namespace

// Every node kind the gate flags must also RECEIVE the retention. A gate
// that refuses a DISTINCT without `state_ttl`, then compiles it with the
// TTL going nowhere, has moved the problem rather than fixed it.

TEST(SqlBoundedState, DistinctReceivesTheRetention) {
    const auto params = op_params(kafka_table("src", "t") + kFileDst,
                                  "INSERT INTO dst SELECT DISTINCT k, v FROM src",
                                  "distinct_row");
    ASSERT_FALSE(params.empty()) << "no distinct_row op emitted";
    ASSERT_TRUE(params.count("state_ttl_ms")) << "DISTINCT was not given the declared retention";
    EXPECT_EQ(params.at("state_ttl_ms"), "3600000");
}

TEST(SqlBoundedState, EquiJoinReceivesTheRetention) {
    const auto params = op_params(kafka_table("a", "a") + kafka_table("b", "b") + kFileDst,
                                  "INSERT INTO dst SELECT a.k, b.v FROM a JOIN b ON a.k = b.k",
                                  "equi_join_row");
    ASSERT_FALSE(params.empty()) << "no equi_join_row op emitted";
    ASSERT_TRUE(params.count("state_ttl_ms")) << "the join was not given the declared retention";
    EXPECT_EQ(params.at("state_ttl_ms"), "3600000");
}

TEST(SqlBoundedState, SemiJoinReceivesTheRetention) {
    const auto params = op_params(kafka_table("a", "a") + kafka_table("b", "b") + kFileDst,
                                  "INSERT INTO dst SELECT k, v FROM a WHERE k IN (SELECT k FROM b)",
                                  "semi_join_row");
    ASSERT_FALSE(params.empty()) << "no semi_join_row op emitted";
    ASSERT_TRUE(params.count("state_ttl_ms"))
        << "the semi-join was not given the declared retention";
    EXPECT_EQ(params.at("state_ttl_ms"), "3600000");
}

TEST(SqlBoundedState, SetOperationReceivesTheRetention) {
    // INTERSECT emits a changelog, and a plain file sink is append-only -
    // an unrelated pre-existing constraint, so the sink declares it.
    const char* changelog_dst =
        "CREATE TABLE dst (k BIGINT, v BIGINT) WITH (connector='file', format='json', "
        "path='/tmp/o', changelog='true');";
    const auto params = op_params(kafka_table("a", "a") + kafka_table("b", "b") + changelog_dst,
                                  "INSERT INTO dst SELECT k, v FROM a INTERSECT SELECT k, v FROM b",
                                  "set_op_row");
    ASSERT_FALSE(params.empty()) << "no set_op_row op emitted";
    ASSERT_TRUE(params.count("state_ttl_ms")) << "the set op was not given the declared retention";
    EXPECT_EQ(params.at("state_ttl_ms"), "3600000");
}

// A declared retention must satisfy the gate only for the operators it
// actually bounds.
//
// ROW_NUMBER / TopN is the one gated kind the planner never stamps a TTL
// onto, and top_n_row reads none - yet a declared state_ttl used to
// clear EVERY finding, so a ranking query over an unbounded source
// compiled with the gate reporting it bounded and its state growing for
// the life of the job. The gate certifying a bound it had not checked is
// worse than no gate: it is the reason the author stopped worrying.
TEST(SqlBoundedState, ADeclaredTtlDoesNotSatisfyTheGateForRowNumber) {
    clink::cluster::ensure_built_ins_registered();
    ensure_sql_installed_for_bounded_state_tests();
    Catalog cat;
    const auto ddl = kafka_table("src", "t") + kFileDst;
    for (const auto& st : parse(ddl).statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    // A BARE ROW_NUMBER: the running count per partition, retained for
    // the life of the job. (A `WHERE rn <= N` filter would bind to
    // TopNPerKey instead, which the gate deliberately does not flag
    // because it keeps at most N rows per key.) The source declares
    // state_ttl='1h' - see kafka_table.
    const auto script = parse(
        "INSERT INTO dst SELECT k, rn FROM "
        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM src) s");
    const Binder binder(cat);
    auto plan = binder.bind_insert(std::get<ast::InsertStmt>(script.statements.front()));

    const auto report = check_plan_bounded_state(*plan, /*allow_unbounded=*/false);
    ASSERT_FALSE(report.findings.empty())
        << "a declared state_ttl satisfied the gate for ROW_NUMBER, whose state no TTL reaches";
    bool found_row_number = false;
    for (const auto& f : report.findings) {
        if (f.node_kind == "RowNumber") {
            found_row_number = true;
            EXPECT_NE(f.remedy.find("does NOT bound this operator"), std::string::npos)
                << "the remedy still offers state_ttl: " << f.remedy;
        }
    }
    EXPECT_TRUE(found_row_number) << "the RowNumber finding was suppressed";
}

// The same declared TTL must still satisfy the gate for the operators it
// genuinely bounds, or the fix above would simply have broken them.
TEST(SqlBoundedState, ADeclaredTtlStillSatisfiesTheGateForAnAggregate) {
    clink::cluster::ensure_built_ins_registered();
    ensure_sql_installed_for_bounded_state_tests();
    Catalog cat;
    const auto ddl = kafka_table("src", "t") + kFileDst;
    for (const auto& st : parse(ddl).statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto script = parse("INSERT INTO dst SELECT k, SUM(v) FROM src GROUP BY k");
    const Binder binder(cat);
    auto plan = binder.bind_insert(std::get<ast::InsertStmt>(script.statements.front()));
    const auto report = check_plan_bounded_state(*plan, /*allow_unbounded=*/false);
    EXPECT_TRUE(report.findings.empty())
        << "a declared state_ttl no longer satisfies the gate for an aggregate, which it bounds";
}

TEST(SqlBoundedState, RetentionIsSatisfiedByAnyOfTheFourRoutes) {
    EXPECT_TRUE(StateRetention{.ttl_ms = 1000}.bounded());
    EXPECT_TRUE(StateRetention{.allow_unbounded = true}.bounded());
    EXPECT_FALSE(StateRetention{}.bounded());
}

}  // namespace

// --- the override has to reach the planner, not just the parser ---------------
//
// compile_error() above wires script.allow_unbounded_state into the planner
// itself, which is the right unit test of the GATE and is exactly why this
// defect survived: the harness did the wiring the product had forgotten. In
// production nothing called set_allow_unbounded_state at all, so the clause
// was inert - the engine refused the query while its own diagnostic advised
// writing the clause the user had just written. These two go through
// run_script, the loop every real front door uses.

namespace {

// Compile a script the way a front door does. Returns the diagnostics text
// (empty when every statement compiled) and reports how many specs reached
// the SubmitFn.
std::string run_script_error(const std::string& sql, int* submitted = nullptr) {
    // Declares the kafka capability this binary links no impl for; without
    // it the connector is UNKNOWN, unknown deliberately does not trip the
    // gate, and both tests below would pass for the wrong reason.
    ensure_sql_installed_for_bounded_state_tests();
    Catalog cat;
    std::ostringstream out;
    std::ostringstream err;
    int n = 0;
    const ScriptRunOptions opts;
    const ScriptIO io{.out = &out, .err = &err};
    const int rc = run_script(
        sql, cat, opts, io, [&](const clink::cluster::JobGraphSpec&, const std::string&) {
            ++n;
            return 0;
        });
    if (submitted != nullptr) {
        *submitted = n;
    }
    return rc == 0 ? std::string{} : err.str();
}

const char* kUnboundedScript =
    "CREATE TABLE src (k BIGINT, v BIGINT) WITH (connector='kafka', format='json', "
    "topic='t', bootstrap_servers='localhost:9092');"
    "CREATE TABLE dst (k BIGINT, s BIGINT) WITH (connector='file', format='json', "
    "path='/tmp/o');"
    "INSERT INTO dst SELECT k, SUM(v) AS s FROM src GROUP BY k";

}  // namespace

TEST(SqlBoundedState, TheOverrideReachesThePlannerThroughTheScriptRunner) {
    int submitted = 0;
    const std::string sql = std::string{kUnboundedScript} + " ALLOW UNBOUNDED STATE";
    const auto err = run_script_error(sql, &submitted);
    EXPECT_EQ(err, "") << "ALLOW UNBOUNDED STATE was inert: the gate refused a query that "
                          "declared the very override its diagnostic recommends";
    EXPECT_EQ(submitted, 1) << "the job did not reach the submit path";
}

TEST(SqlBoundedState, WithoutTheOverrideTheScriptRunnerStillRefuses) {
    // The vacuity guard for the test above: if the gate were simply not
    // running under run_script, that test would pass for the wrong reason.
    int submitted = 0;
    const auto err = run_script_error(kUnboundedScript, &submitted);
    EXPECT_NE(err.find("ALLOW UNBOUNDED STATE"), std::string::npos)
        << "the bounded-state gate did not fire under run_script at all";
    EXPECT_EQ(submitted, 0) << "an unbounded job reached the submit path";
}
