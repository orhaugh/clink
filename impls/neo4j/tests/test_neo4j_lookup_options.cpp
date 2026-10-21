// The lookup's options: what the planner and the lookup table supply, the
// connection rules it shares with the sink, its bounds, and the key, result
// and metadata column contract. Every refusal names the lookup.

#include <cstdlib>
#include <map>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "options.hpp"

using clink::neo4j::Kind;
using clink::neo4j::LookupOptions;
using clink::neo4j::Neo4jError;
using clink::neo4j::parse_lookup_options;
namespace code = clink::neo4j::code;

namespace {

// What the planner puts on neo4j_lookup_join_row for
//   CREATE TABLE ctx (account VARCHAR, shared BIGINT, evidence VARCHAR,
//                     matched BOOLEAN, observed_at TIMESTAMP(3), bookmark VARCHAR)
//     WITH (connector='lookup', function='neo4j', ...);
//   SELECT ... FROM s LEFT JOIN ctx d ON s.account = d.account;
std::map<std::string, std::string> base() {
    return {
        {"function_name", "neo4j"},
        {"probe_alias", "s"},
        {"dim_alias", "d"},
        {"probe_columns", "event_id,account"},
        {"dim_columns", "account,shared,evidence,matched,observed_at,bookmark"},
        {"join_type", "left_outer"},
        {"probe_key_column", "account"},
        {"probe_key_type", "VARCHAR"},
        {"dim_key_column", "account"},
        {"dim_column_types",
         "account:VARCHAR;shared:BIGINT;evidence:VARCHAR;matched:BOOLEAN;observed_at:TIMESTAMP(3);"
         "bookmark:VARCHAR"},
        {"neo4j.endpoint", "http://graph.internal:7474"},
        {"neo4j.statement",
         "MATCH (a:Account {id: $key}) RETURN a.shared AS shared, a.evidence AS evidence"},
        {"neo4j.matched_column", "matched"},
        {"neo4j.observed_at_column", "observed_at"},
        {"neo4j.bookmark_column", "bookmark"},
    };
}

std::map<std::string, std::string> with(std::map<std::string, std::string> changes) {
    auto p = base();
    for (auto& [k, v] : changes) {
        p[k] = v;
    }
    return p;
}

std::map<std::string, std::string> without(const std::string& key) {
    auto p = base();
    p.erase(key);
    return p;
}

// The refusal's code and message, or empty strings when the options parse.
std::pair<std::string, std::string> refusal(const std::map<std::string, std::string>& params) {
    try {
        (void)parse_lookup_options(params, 0, 1);
    } catch (const Neo4jError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

}  // namespace

TEST(Neo4jLookupOptions, DefaultsTheJoinAndTheColumnRoles) {
    const LookupOptions o = parse_lookup_options(base(), 1, 2);
    EXPECT_EQ(o.subtask, 1U);
    EXPECT_EQ(o.parallelism, 2U);
    EXPECT_EQ(o.endpoint, "http://graph.internal:7474");
    EXPECT_EQ(o.query_path(), "/db/neo4j/query/v2");
    // A lookup holds up its record, so its defaults are tighter than the sink's.
    EXPECT_EQ(o.connect_timeout_ms, 2000U);
    EXPECT_EQ(o.request_timeout_ms, 5000U);
    EXPECT_EQ(o.retry_window_ms, 30000U);
    EXPECT_EQ(o.max_retries, 3U);
    EXPECT_EQ(o.max_response_bytes, 64U << 10);
    EXPECT_EQ(o.server_timeout_s, 0U);
    EXPECT_EQ(o.max_in_flight, 64U);
    EXPECT_EQ(o.max_concurrent_requests, 4U);
    EXPECT_TRUE(o.outer);
    EXPECT_EQ(o.probe_alias, "s");
    EXPECT_EQ(o.dim_alias, "d");
    EXPECT_EQ(o.probe_columns, (std::vector<std::string>{"event_id", "account"}));
    EXPECT_EQ(o.probe_key_column, "account");
    EXPECT_EQ(o.key_type.kind, Kind::Varchar);
    EXPECT_EQ(o.dim_key_column, "account");
    EXPECT_EQ(o.dim_columns.size(), 6U);
    // The statement returns every declared column but the key and the
    // metadata columns.
    ASSERT_EQ(o.result_columns.size(), 2U);
    EXPECT_EQ(o.result_columns[0].name, "shared");
    EXPECT_EQ(o.result_columns[1].name, "evidence");
    EXPECT_EQ(o.matched_column, "matched");
    EXPECT_EQ(o.observed_at_column, "observed_at");
    EXPECT_EQ(o.bookmark_column, "bookmark");

    const LookupOptions inner = parse_lookup_options(with({{"join_type", "inner"}}), 0, 1);
    EXPECT_FALSE(inner.outer);
}

TEST(Neo4jLookupOptions, TheConnectionRulesAreTheSinksWordedForTheLookup) {
    const std::map<std::string, std::string> cases[] = {
        with({{"neo4j.endpoint", "http://graph.internal:7474/db/neo4j"}}),
        with({{"neo4j.endpoint", "http://user:pw@graph.internal:7474"}}),
        with({{"neo4j.endpoint", "bolt://graph.internal:7687"}}),
        with({{"neo4j.username", "reader"}}),
        with({{"neo4j.username", "reader"},
              {"neo4j.password", "pw"},
              {"neo4j.bearer_token", "token"}}),
        with({{"neo4j.tls_ca_file", "/etc/ssl/ca.pem"}}),
        with({{"neo4j.database", "-bad"}}),
        without("neo4j.endpoint"),
    };
    for (const auto& params : cases) {
        const auto [c, m] = refusal(params);
        EXPECT_EQ(c, code::kOptionInvalid) << m;
        EXPECT_EQ(m.rfind("neo4j lookup: ", 0), 0U) << m;
        EXPECT_EQ(m.find("neo4j sink"), std::string::npos) << m;
    }
    const auto [c, m] = refusal(without("neo4j.statement"));
    EXPECT_EQ(c, code::kOptionInvalid);
    EXPECT_NE(m.find("$key"), std::string::npos) << m;

    const LookupOptions o = parse_lookup_options(with({{"neo4j.database", "graph.v2"},
                                                       {"neo4j.username", "reader"},
                                                       {"neo4j.password", "pw"}}),
                                                 0,
                                                 1);
    EXPECT_EQ(o.query_path(), "/db/graph.v2/query/v2");
    EXPECT_EQ(o.username, "reader");
}

TEST(Neo4jLookupOptions, UnknownOptionsAndStrayParametersAreRefused) {
    {
        const auto [c, m] = refusal(with({{"neo4j.ordered", "false"}}));
        EXPECT_EQ(c, code::kOptionInvalid);
        EXPECT_NE(m.find("unknown option 'ordered'"), std::string::npos) << m;
        EXPECT_NE(m.find("max_concurrent_requests"), std::string::npos) << m;
    }
    {
        const auto [c, m] = refusal(with({{"neo4j.batch_size", "10"}}));
        EXPECT_NE(m.find("unknown option 'batch_size'"), std::string::npos) << m;
    }
    {
        // Only the planner's join parameters may come without the prefix.
        const auto [c, m] = refusal(with({{"endpoint", "http://elsewhere:7474"}}));
        EXPECT_EQ(c, code::kOptionInvalid);
        EXPECT_NE(m.find("unexpected parameter 'endpoint'"), std::string::npos) << m;
    }
}

TEST(Neo4jLookupOptions, EnvReferencesAreReadAndNeverEchoed) {
    ::setenv("CLINK_LOOKUP_TEST_PW", "s3cret-lookup-pw", 1);
    ::setenv("CLINK_LOOKUP_TEST_DB", "not a database name", 1);
    ::unsetenv("CLINK_LOOKUP_TEST_UNSET");
    const LookupOptions o = parse_lookup_options(
        with({{"neo4j.username", "reader"}, {"neo4j.password", "env://CLINK_LOOKUP_TEST_PW"}}),
        0,
        1);
    EXPECT_EQ(o.password, "s3cret-lookup-pw");
    {
        const auto [c, m] = refusal(with(
            {{"neo4j.username", "reader"}, {"neo4j.password", "env://CLINK_LOOKUP_TEST_UNSET"}}));
        EXPECT_EQ(c, code::kSecretUnset);
        EXPECT_NE(m.find("env://CLINK_LOOKUP_TEST_UNSET"), std::string::npos) << m;
    }
    {
        const auto [c, m] = refusal(with({{"neo4j.database", "env://CLINK_LOOKUP_TEST_DB"}}));
        EXPECT_EQ(c, code::kOptionInvalid);
        EXPECT_NE(m.find("the value of env://CLINK_LOOKUP_TEST_DB"), std::string::npos) << m;
        EXPECT_EQ(m.find("not a database name"), std::string::npos) << m;
    }
}

TEST(Neo4jLookupOptions, EveryBoundIsFiniteAndChecked) {
    const std::map<std::string, std::string> refused[] = {
        {{"neo4j.max_in_flight", "0"}},
        {{"neo4j.max_in_flight", "10001"}},
        {{"neo4j.max_concurrent_requests", "0"}},
        {{"neo4j.max_concurrent_requests", "65"}},
        {{"neo4j.max_retries", "101"}},
        {{"neo4j.max_retries", "-1"}},
        {{"neo4j.request_timeout_ms", "0"}},
        {{"neo4j.connect_timeout_ms", "0"}},
        {{"neo4j.retry_window_ms", "86400001"}},
        {{"neo4j.max_response_bytes", "1023"}},
        {{"neo4j.server_timeout_s", "0"}},
        {{"neo4j.server_timeout_s", "0.5"}},
        // The client would give up before the server's limit acts.
        {{"neo4j.server_timeout_s", "5"}},
        {{"neo4j.server_timeout_s", "2"}, {"neo4j.request_timeout_ms", "2000"}},
    };
    for (const auto& changes : refused) {
        const auto [c, m] = refusal(with(changes));
        EXPECT_EQ(c, code::kOptionInvalid) << changes.begin()->first << ": " << m;
    }
    const LookupOptions o = parse_lookup_options(with({{"neo4j.max_in_flight", "500"},
                                                       {"neo4j.max_concurrent_requests", "16"},
                                                       {"neo4j.max_retries", "0"},
                                                       {"neo4j.retry_window_ms", "0"},
                                                       {"neo4j.server_timeout_s", "2"},
                                                       {"neo4j.request_timeout_ms", "3000"}}),
                                                 0,
                                                 1);
    EXPECT_EQ(o.max_in_flight, 500U);
    EXPECT_EQ(o.max_concurrent_requests, 16U);
    EXPECT_EQ(o.max_retries, 0U);
    EXPECT_EQ(o.retry_window_ms, 0U);
    EXPECT_EQ(o.server_timeout_s, 2U);
}

TEST(Neo4jLookupOptions, TheKeyIsAnIntegerOrTextOnBothSides) {
    {
        const auto [c, m] = refusal(with({{"probe_key_type", "DOUBLE"}}));
        EXPECT_EQ(c, code::kTypeUnsupported);
        EXPECT_NE(m.find("TINYINT, SMALLINT, INTEGER, BIGINT or VARCHAR"), std::string::npos) << m;
    }
    {
        const auto [c, m] = refusal(with({{"probe_key_type", "BIGINT"}}));
        EXPECT_EQ(c, code::kTypeUnsupported);
        EXPECT_NE(m.find("declare both as integers or both as VARCHAR"), std::string::npos) << m;
    }
    // The lookup's key column carries the probe's key on a match, so it must
    // hold every value the probe's can.
    auto narrow = with({{"probe_key_type", "BIGINT"},
                        {"dim_column_types",
                         "account:SMALLINT;shared:BIGINT;evidence:VARCHAR;matched:BOOLEAN;"
                         "observed_at:TIMESTAMP(3);bookmark:VARCHAR"}});
    {
        const auto [c, m] = refusal(narrow);
        EXPECT_EQ(c, code::kTypeUnsupported);
        EXPECT_NE(m.find("narrower"), std::string::npos) << m;
    }
    narrow["probe_key_type"] = "SMALLINT";
    EXPECT_EQ(parse_lookup_options(narrow, 0, 1).key_type.kind, Kind::SmallInt);
    {
        const auto [c, m] = refusal(with({{"dim_key_column", "nope"}}));
        EXPECT_EQ(c, code::kOptionInvalid);
    }
}

TEST(Neo4jLookupOptions, ResultColumnsAreScalarsWithoutLoss) {
    const auto types = [](const std::string& extra) {
        return with({{"dim_column_types",
                      "account:VARCHAR;" + extra +
                          ";matched:BOOLEAN;observed_at:TIMESTAMP(3);bookmark:VARCHAR"}});
    };
    {
        const auto [c, m] = refusal(types("score:REAL"));
        EXPECT_EQ(c, code::kTypeUnsupported);
        EXPECT_NE(m.find("declare DOUBLE"), std::string::npos) << m;
    }
    for (const char* t : {"tags:ARRAY<VARCHAR>",
                          "attrs:MAP<VARCHAR, VARCHAR>",
                          "at:TIMESTAMP(3)",
                          "day:DATE",
                          "amount:DECIMAL(10, 2)",
                          "node:ROW<id BIGINT>"}) {
        const auto [c, m] = refusal(types(t));
        EXPECT_EQ(c, code::kTypeUnsupported) << t << ": " << m;
    }
    for (const char* t : {"n:TINYINT",
                          "n:SMALLINT",
                          "n:INTEGER",
                          "n:BIGINT",
                          "n:DOUBLE",
                          "n:BOOLEAN",
                          "n:VARCHAR"}) {
        const auto o = parse_lookup_options(types(t), 0, 1);
        ASSERT_EQ(o.result_columns.size(), 1U) << t;
    }
    const auto [c, m] = refusal(with({{"dim_column_types",
                                       "account:VARCHAR;matched:BOOLEAN;observed_at:TIMESTAMP(3);"
                                       "bookmark:VARCHAR;shared:BIGINT;evidence:VARCHAR"},
                                      {"neo4j.matched_column", "shared"}}));
    EXPECT_EQ(c, code::kTypeUnsupported) << m;
    const auto [c2, m2] = refusal(with({{"dim_column_types", "account:VARCHAR;matched:BOOLEAN"},
                                        {"neo4j.observed_at_column", "matched"}}));
    EXPECT_FALSE(c2.empty());
    const auto [c3, m3] = refusal(
        with({{"dim_column_types",
               "account:VARCHAR;matched:BOOLEAN;observed_at:TIMESTAMP(3);bookmark:VARCHAR"}}));
    EXPECT_EQ(c3, code::kOptionInvalid);
    EXPECT_NE(m3.find("declares no column for the statement to return"), std::string::npos) << m3;
}

TEST(Neo4jLookupOptions, MetadataColumnsAreDeclaredTypedAndDistinct) {
    {
        const auto [c, m] = refusal(with({{"neo4j.matched_column", "missing"}}));
        EXPECT_EQ(c, code::kOptionInvalid);
        EXPECT_NE(m.find("names no declared column"), std::string::npos) << m;
    }
    {
        const auto [c, m] = refusal(with({{"neo4j.observed_at_column", "evidence"}}));
        EXPECT_EQ(c, code::kTypeUnsupported);
        EXPECT_NE(m.find("must be declared TIMESTAMP"), std::string::npos) << m;
    }
    {
        const auto [c, m] = refusal(with({{"neo4j.bookmark_column", "account"}}));
        EXPECT_EQ(c, code::kOptionInvalid);
        EXPECT_NE(m.find("already the key"), std::string::npos) << m;
    }
    {
        // TIMESTAMP WITH TIME ZONE is refused: observed_at is the client's
        // epoch milliseconds, in the zone-less type.
        const auto [c, m] =
            refusal(with({{"dim_column_types",
                           "account:VARCHAR;shared:BIGINT;evidence:VARCHAR;matched:BOOLEAN;"
                           "observed_at:TIMESTAMP(3) WITH TIME ZONE;bookmark:VARCHAR"}}));
        EXPECT_EQ(c, code::kTypeUnsupported) << m;
    }
    // Without metadata options, those columns are results the statement must
    // return.
    auto p = base();
    p.erase("neo4j.matched_column");
    p.erase("neo4j.observed_at_column");
    p.erase("neo4j.bookmark_column");
    p["dim_column_types"] = "account:VARCHAR;shared:BIGINT;evidence:VARCHAR";
    const auto o = parse_lookup_options(p, 0, 1);
    EXPECT_TRUE(o.matched_column.empty());
    EXPECT_EQ(o.result_columns.size(), 2U);
}

TEST(Neo4jLookupOptions, TheJoinIsThePlannersToDescribe) {
    for (const char* key : {"probe_alias",
                            "dim_alias",
                            "probe_key_column",
                            "probe_key_type",
                            "dim_key_column",
                            "dim_column_types",
                            "join_type"}) {
        const auto [c, m] = refusal(without(key));
        EXPECT_EQ(c, code::kOptionInvalid) << key;
        EXPECT_NE(m.find("which the SQL planner supplies"), std::string::npos) << m;
    }
    const auto [c, m] = refusal(with({{"join_type", "right_outer"}}));
    EXPECT_EQ(c, code::kOptionInvalid);
    EXPECT_NE(m.find("INNER and LEFT"), std::string::npos) << m;
}
