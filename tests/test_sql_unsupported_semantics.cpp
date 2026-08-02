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
#include "clink/plugin/plugin.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/parser.hpp"
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
