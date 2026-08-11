// Rejection of SQL that clink accepts but does not act on.
//
// Every case here was found by compiling the construct and observing that
// it was ACCEPTED, then checking whether anything downstream used it. The
// answer was no in each case, which means the text was decoration: a user
// writing `CHECK (k > 0)` got no validation, and a user writing
// `k BIGINT PRIMARY KEY` on an upsert sink got no key.
//
// The rule applied throughout: a semantically meaningful construct must
// either be implemented or refused. Accepting and ignoring is the one
// option that is never right, because it produces a job that behaves
// differently from what its own source text says.

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/operators/json_value_expr.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/table_option_check.hpp"

namespace {

using namespace clink::sql;

void ensure_installed() {
    static const bool once = [] {
        clink::cluster::ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg;
        clink::sql::install(reg);
        return true;
    }();
    (void)once;
}

// Register `ddl` and return the error, or "" on success.
std::string register_ddl(const std::string& ddl) {
    ensure_installed();
    try {
        Catalog cat;
        for (const auto& st : parse(ddl).statements) {
            if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
                cat.register_table(*ct);
            }
        }
        return {};
    } catch (const std::exception& e) {
        return e.what();
    }
}

const char* kBase = "WITH (connector='file', format='json', path='/tmp/x')";

// --- column constraints -------------------------------------------------

TEST(SqlUnsupportedSemantics, ConstraintsClinkDoesNotEvaluateAreRefused) {
    // Each of these parsed, registered, and did nothing. A user reading
    // their own DDL back would reasonably believe the data was validated.
    struct Case {
        const char* sql;
        const char* expect;
    };
    const Case cases[] = {
        {"CREATE TABLE t (k BIGINT NOT NULL, v BIGINT) ", "NOT NULL"},
        {"CREATE TABLE t (k BIGINT UNIQUE, v BIGINT) ", "UNIQUE"},
        {"CREATE TABLE t (k BIGINT CHECK (k > 0), v BIGINT) ", "CHECK"},
        {"CREATE TABLE t (k BIGINT DEFAULT 7, v BIGINT) ", "DEFAULT"},
        {"CREATE TABLE t (k BIGINT REFERENCES o(id), v BIGINT) ", "REFERENCES"},
    };
    for (const auto& c : cases) {
        const auto err = register_ddl(std::string(c.sql) + kBase + ";");
        ASSERT_FALSE(err.empty()) << c.sql << " was accepted and ignored";
        EXPECT_NE(err.find(c.expect), std::string::npos) << err;
        // The diagnostic must name the column and offer a way forward -
        // a rejection a user cannot act on is barely better than silence.
        EXPECT_NE(err.find("column 'k'"), std::string::npos) << err;
        EXPECT_NE(err.find("WHERE"), std::string::npos)
            << "the diagnostic offers no alternative: " << err;
    }
}

TEST(SqlUnsupportedSemantics, PlainColumnsAndExplicitNullStillWork) {
    EXPECT_EQ(register_ddl(std::string("CREATE TABLE t (k BIGINT, v BIGINT) ") + kBase + ";"), "");
    // NULL is the default and asserts nothing, so ignoring it changes
    // nothing and refusing it would be noise.
    EXPECT_EQ(register_ddl(std::string("CREATE TABLE t (k BIGINT NULL, v BIGINT) ") + kBase + ";"),
              "");
}

// --- the PRIMARY KEY bug ------------------------------------------------

TEST(SqlUnsupportedSemantics, InColumnPrimaryKeyReachesTheCatalog) {
    // The bug this test exists for: catalog.hpp promised "both are
    // accepted; the in-column form is canonical", and the in-column form
    // produced an EMPTY primary key. On an upsert sink that means no key
    // to upsert on, so the effectively-once guarantee its capability
    // record advertises was void - silently.
    ensure_installed();
    Catalog cat;
    for (const auto& st :
         parse(std::string("CREATE TABLE t (k BIGINT PRIMARY KEY, v BIGINT) ") + kBase + ";")
             .statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto* t = cat.get_table("t");
    ASSERT_NE(t, nullptr);
    ASSERT_EQ(t->primary_key.size(), 1U) << "the in-column PRIMARY KEY was dropped again";
    EXPECT_EQ(t->primary_key[0], "k");
}

TEST(SqlUnsupportedSemantics, AMultiColumnInlinePrimaryKeyIsCollected) {
    ensure_installed();
    Catalog cat;
    for (const auto& st :
         parse(
             std::string("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT PRIMARY KEY, c BIGINT) ") +
             kBase + ";")
             .statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto* t = cat.get_table("t");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->primary_key, (std::vector<std::string>{"a", "b"}));
}

TEST(SqlUnsupportedSemantics, TheWithOptionWinsOverAnInlinePrimaryKey) {
    // The explicit list is the more specific statement, and the form that
    // survives a catalog JSON round trip.
    ensure_installed();
    Catalog cat;
    for (const auto& st : parse("CREATE TABLE t (k BIGINT PRIMARY KEY, v BIGINT) WITH "
                                "(connector='file', format='json', path='/tmp/x', "
                                "primary_key='v');")
                              .statements) {
        if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
            cat.register_table(*ct);
        }
    }
    const auto* t = cat.get_table("t");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->primary_key, (std::vector<std::string>{"v"}));
}

// --- WITH-option checking ------------------------------------------------

TEST(SqlUnsupportedSemantics, AMisspeltInterpretedOptionIsRefused) {
    // The silent case that motivated this: an unrecognised option is
    // passed through to the connector and otherwise ignored, so
    // `delivery_gurantee='exactly_once'` leaves an at-least-once sink and
    // says nothing.
    const auto err = register_ddl(
        "CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', path='/tmp/x', "
        "delivery_gurantee='exactly_once');");
    ASSERT_FALSE(err.empty()) << "a misspelt delivery_guarantee was accepted";
    EXPECT_NE(err.find("delivery_guarantee"), std::string::npos)
        << "the diagnostic does not suggest the intended option: " << err;
}

TEST(SqlUnsupportedSemantics, ANearMissOfPrimaryKeyIsRefused) {
    const auto err = register_ddl(
        "CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', path='/tmp/x', "
        "primary_keys='k');");
    ASSERT_FALSE(err.empty());
    EXPECT_NE(err.find("primary_key"), std::string::npos) << err;
}

TEST(SqlUnsupportedSemantics, ConnectorPassthroughOptionsAreLeftAlone) {
    // The option space out there is open-ended and belongs to the
    // connector. Rejecting an unrecognised option outright would break
    // every connector that accepts something clink has never heard of,
    // which is most of them - so only NEAR MISSES of interpreted options
    // are refused.
    EXPECT_EQ(register_ddl("CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', "
                           "path='/tmp/x', bootstrap_servers='h:9092', topic='t', "
                           "some_vendor_knob='42');"),
              "");
}

TEST(SqlUnsupportedSemantics, ValuesOutsideAClosedDomainAreRefused) {
    const auto bad_mode = register_ddl(
        "CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', path='/tmp/x', "
        "mode='upsrt');");
    ASSERT_FALSE(bad_mode.empty()) << "mode='upsrt' silently behaved as append";
    EXPECT_NE(bad_mode.find("'append'"), std::string::npos) << bad_mode;

    const auto bad_bool = register_ddl(
        "CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', path='/tmp/x', "
        "changelog='yes');");
    ASSERT_FALSE(bad_bool.empty()) << "changelog='yes' silently meant false";
}

TEST(SqlUnsupportedSemantics, EveryLegitimateModeValueIsAccepted) {
    // Guard against the domain list being narrowed by guesswork. The first
    // version of it omitted 'cdc' and rejected every CDC table in the
    // suite.
    for (const char* mode : {"append", "upsert", "cdc"}) {
        EXPECT_EQ(register_ddl(std::string("CREATE TABLE t (k BIGINT) WITH (connector='file', "
                                           "format='json', path='/tmp/x', mode='") +
                               mode + "');"),
                  "")
            << "mode='" << mode << "' was refused";
    }
}

TEST(SqlUnsupportedSemantics, AnUnrecognisedDeliveryGuaranteeIsRefused) {
    const auto err = register_ddl(
        "CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', path='/tmp/x', "
        "delivery_guarantee='definitely_once');");
    ASSERT_FALSE(err.empty());
    EXPECT_NE(err.find("not a recognised guarantee"), std::string::npos) << err;
}

TEST(SqlUnsupportedSemantics, EveryDeclaredGuaranteeSpellingIsAccepted) {
    // The valid set comes from the connector capability contract, so the
    // two cannot drift apart.
    for (const char* g : {"at_most_once",
                          "at_least_once",
                          "effectively_once_idempotent",
                          "exactly_once_atomic_publish",
                          "exactly_once_two_phase_commit",
                          "exactly_once"}) {
        EXPECT_EQ(register_ddl(std::string("CREATE TABLE t (k BIGINT) WITH (connector='file', "
                                           "format='json', path='/tmp/x', "
                                           "delivery_guarantee='") +
                               g + "');"),
                  "")
            << g << " was refused";
    }
}

TEST(SqlUnsupportedSemantics, AnUnparseableStateTtlIsRefused) {
    // A mistyped retention that silently became "no retention" would
    // defeat the bounded-state gate, which exists to catch exactly that.
    const auto err = register_ddl(
        "CREATE TABLE t (k BIGINT) WITH (connector='file', format='json', path='/tmp/x', "
        "state_ttl='soon');");
    ASSERT_FALSE(err.empty()) << "state_ttl='soon' silently meant no retention";
    EXPECT_NE(err.find("state_ttl"), std::string::npos) << err;
}

// --- unknown scalar functions --------------------------------------------

// Compile `sql` through parse -> bind and return the error, or "" on
// success. Unlike register_ddl above this goes far enough to bind the
// SELECT, which is where a function name is resolved.
std::string compile_query(const std::string& sql) {
    ensure_installed();
    try {
        Catalog cat;
        for (const auto& st : parse(sql).statements) {
            if (const auto* ct = std::get_if<ast::CreateTableStmt>(&st)) {
                cat.register_table(*ct);
                continue;
            }
            if (const auto* ins = std::get_if<ast::InsertStmt>(&st)) {
                (void)Binder{cat}.bind_insert(*ins);
            }
        }
        return {};
    } catch (const std::exception& e) {
        return e.what();
    }
}

const char* kSrcDst =
    "CREATE TABLE src (k BIGINT, s VARCHAR) WITH (connector='file', format='json', "
    "path='/tmp/a'); "
    "CREATE TABLE dst (k BIGINT) WITH (connector='file', format='json', path='/tmp/b'); ";

TEST(SqlUnsupportedSemantics, AnUnknownFunctionIsRejectedAtCompileTimeNotRunTime) {
    // The failure this replaces: the name parsed, bound, planned and
    // DEPLOYED, then threw "json_value_expr: unknown op 'x'" out of the
    // projection operator when the first record arrived. On a cluster that
    // is a job that starts and then dies per record, with an internal
    // diagnostic and a restart loop if restarts are configured.
    const auto err = compile_query(std::string(kSrcDst) +
                                   "INSERT INTO dst SELECT no_such_function(k) FROM src;");
    ASSERT_FALSE(err.empty()) << "an unknown function was accepted and deferred to runtime";
    EXPECT_NE(err.find("no_such_function"), std::string::npos) << err;
    EXPECT_NE(err.find("not a known function"), std::string::npos) << err;
}

TEST(SqlUnsupportedSemantics, RandomIsRejectedForTheSameReasonNowIs) {
    // RANDOM() was the specific inconsistency: NOW() was refused to keep
    // SQL deterministic, and RANDOM() - just as nondeterministic, and just
    // as damaging to the replay guarantee - sailed through to runtime.
    // It is not implemented, so it now fails at compile time like any
    // other unknown name.
    const auto err =
        compile_query(std::string(kSrcDst) + "INSERT INTO dst SELECT random() FROM src;");
    ASSERT_FALSE(err.empty()) << "random() was accepted";
    EXPECT_NE(err.find("random"), std::string::npos) << err;

    // NOW() keeps its own, more specific message - it exists as a concept
    // and is refused on determinism grounds, which is worth saying.
    const auto now_err =
        compile_query(std::string(kSrcDst) + "INSERT INTO dst SELECT now() FROM src;");
    ASSERT_FALSE(now_err.empty());
    EXPECT_NE(now_err.find("deterministic"), std::string::npos)
        << "now() lost its determinism-specific diagnostic: " << now_err;
}

TEST(SqlUnsupportedSemantics, ATypoOfARealFunctionIsNamedInTheDiagnostic) {
    const auto err = compile_query(std::string(kSrcDst) +
                                   "INSERT INTO dst SELECT substringg(s, 1, 2) FROM src;");
    ASSERT_FALSE(err.empty());
    EXPECT_NE(err.find("substring()"), std::string::npos)
        << "the diagnostic does not suggest the intended function: " << err;
}

TEST(SqlUnsupportedSemantics, EveryBuiltInScalarFunctionIsStillAccepted) {
    // The guard against the check being built on a hand-kept list. Every
    // name the EVALUATOR dispatches must bind, or the check refuses
    // functions that work - the same failure as the mode='cdc' domain, in
    // the opposite direction.
    //
    // Called through the shared table rather than a literal list here, so
    // adding an op cannot make this test stale.
    const auto& names = clink::operators::value_expr_detail::value_op_names();
    ASSERT_GT(names.size(), 30U) << "the op table looks truncated";
    for (const auto& name : names) {
        EXPECT_TRUE(clink::operators::value_expr_detail::lookup_value_op(name).has_value())
            << name << " is listed but not dispatchable";
    }
}

TEST(SqlUnsupportedSemantics, ARegisteredUdfIsAccepted) {
    // The check must not break CREATE FUNCTION. A UDF registered before
    // binding resolves; the evaluator still looks UDFs up late, so DROP
    // FUNCTION behaves as it did.
    ensure_installed();
    clink::ScalarFunctionRegistry::global().register_function(
        "clink_test_udf_for_bind_check",
        arrow::int64(),
        [](const std::vector<clink::config::JsonValue>&) {
            return clink::config::JsonValue{std::int64_t{1}};
        });
    const auto err = compile_query(std::string(kSrcDst) +
                                   "INSERT INTO dst SELECT clink_test_udf_for_bind_check(k) FROM "
                                   "src;");
    clink::ScalarFunctionRegistry::global().remove("clink_test_udf_for_bind_check");
    EXPECT_TRUE(err.empty()) << "a registered UDF was refused: " << err;
}

// --- the checker in isolation --------------------------------------------

TEST(SqlUnsupportedSemantics, InterpretedOptionListIsNonEmptyAndDeduplicated) {
    const auto& opts = interpreted_table_options();
    ASSERT_FALSE(opts.empty());
    auto sorted = opts;
    std::sort(sorted.begin(), sorted.end());
    EXPECT_EQ(std::adjacent_find(sorted.begin(), sorted.end()), sorted.end())
        << "the interpreted-option list has a duplicate";
}

TEST(SqlUnsupportedSemantics, CheckerReportsNothingForACleanTable) {
    const std::map<std::string, std::string> props{
        {"connector", "file"}, {"format", "json"}, {"path", "/tmp/x"}, {"mode", "upsert"}};
    EXPECT_TRUE(check_table_options("t", props).empty());
}

}  // namespace

// --- the query side: clauses must execute or refuse, never decorate --------
//
// The DDL cases above pinned options and constraints. The same rule holds
// for QUERY clauses: libpg_query parses the full Postgres grammar, so the
// binder/planner sees every clause a user can write, and any it neither
// implements nor refuses becomes decoration - the query compiles, runs, and
// silently does something other than what its own text says (an ORDER BY
// that does not order, a FILTER that does not filter). This battery drives
// each suspicious clause through the real bind -> optimize -> plan pipeline
// and requires an exception. A clause that graduates to implemented moves
// OUT of the battery and into a semantics test - the battery is the fence,
// not the roadmap.

namespace {

// Compile an INSERT..SELECT end to end; return the error, or "" if it was
// accepted.
std::string compile_query_error(const std::string& sql) {
    ensure_installed();
    try {
        Catalog cat;
        const auto ddl = parse(
            "CREATE TABLE bat_src (k BIGINT, v BIGINT, ts TIMESTAMP(3)) "
            "WITH (connector='file', format='json', path='/tmp/bat_src.ndjson');"
            "CREATE TABLE bat_src2 (k BIGINT, w BIGINT) "
            "WITH (connector='file', format='json', path='/tmp/bat_src2.ndjson');"
            "CREATE TABLE bat_out (k BIGINT, v BIGINT) "
            "WITH (connector='file', format='json', path='/tmp/bat_out.ndjson')");
        for (const auto& st : ddl.statements) {
            cat.register_table(std::get<ast::CreateTableStmt>(st));
        }
        clink::sql::Binder b(cat);
        auto plan = b.bind_insert(std::get<ast::InsertStmt>(parse(sql).statements[0]));
        plan = clink::sql::optimize(std::move(plan));
        clink::sql::PhysicalPlanner pp;
        (void)pp.compile(static_cast<const clink::sql::LogicalSink&>(*plan));
        return {};
    } catch (const std::exception& e) {
        return e.what();
    }
}

struct QueryClauseCase {
    const char* label;
    const char* sql;
};

}  // namespace

TEST(SqlUnsupportedSemantics, QueryClausesAreImplementedOrRefusedNeverDecorative) {
    const std::vector<QueryClauseCase> must_refuse = {
        // Ordering a plain unbounded stream is not implementable and must
        // not pretend: an ORDER BY that does not order is the canonical
        // decorative clause.
        {"top-level ORDER BY without LIMIT",
         "INSERT INTO bat_out SELECT k, v FROM bat_src ORDER BY v"},
        {"DISTINCT ON", "INSERT INTO bat_out SELECT DISTINCT ON (k) k, v FROM bat_src"},
        {"aggregate FILTER clause",
         "INSERT INTO bat_out SELECT k, SUM(v) FILTER (WHERE v > 0) FROM bat_src GROUP BY k"},
        {"GROUPING SETS",
         "INSERT INTO bat_out SELECT k, SUM(v) FROM bat_src GROUP BY GROUPING SETS ((k), ())"},
        {"ROLLUP", "INSERT INTO bat_out SELECT k, SUM(v) FROM bat_src GROUP BY ROLLUP (k)"},
        {"CUBE", "INSERT INTO bat_out SELECT k, SUM(v) FROM bat_src GROUP BY CUBE (k)"},
        {"TABLESAMPLE", "INSERT INTO bat_out SELECT k, v FROM bat_src TABLESAMPLE BERNOULLI (10)"},
        {"WITH RECURSIVE",
         "INSERT INTO bat_out WITH RECURSIVE r AS (SELECT k, v FROM bat_src) "
         "SELECT k, v FROM r"},
        {"FETCH WITH TIES",
         "INSERT INTO bat_out SELECT k, v FROM bat_src ORDER BY v "
         "FETCH FIRST 3 ROWS WITH TIES"},
        {"window frame EXCLUDE",
         "INSERT INTO bat_out SELECT k, SUM(v) OVER (PARTITION BY k ORDER BY ts "
         "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM bat_src"},
        {"named WINDOW clause",
         "INSERT INTO bat_out SELECT k, SUM(v) OVER w FROM bat_src "
         "WINDOW w AS (PARTITION BY k ORDER BY ts)"},
        {"INTERSECT ALL",
         "INSERT INTO bat_out SELECT k, v FROM bat_src INTERSECT ALL "
         "SELECT k, w FROM bat_src2"},
    };
    std::vector<std::string> decorative;
    for (const auto& c : must_refuse) {
        const auto err = compile_query_error(c.sql);
        if (err.empty()) {
            decorative.push_back(c.label);
        }
    }
    std::string joined;
    for (const auto& d : decorative) {
        joined += "\n  - " + d;
    }
    EXPECT_TRUE(decorative.empty())
        << decorative.size()
        << " clause(s) were ACCEPTED but are not implemented - they compiled to a plan "
           "that silently drops what the query text says:"
        << joined;
}
