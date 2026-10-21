// Option parsing: every key the sink reads, its refusals, and the env://
// references a refusal must never echo the contents of.

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/http_connector/http_request.hpp"

#include "options.hpp"

using clink::neo4j::Neo4jSinkError;
using clink::neo4j::parse_options;
using clink::neo4j::SinkOptions;
namespace code = clink::neo4j::code;

namespace {

std::map<std::string, std::string> base() {
    return {{"endpoint", "http://neo4j.internal:7474"},
            {"statement", "UNWIND $rows AS row MERGE (a:Account {id: row.id})"},
            {"sql_column_types", "id:BIGINT;name:VARCHAR"}};
}

// The refusal's code and message, or empty strings when the options parse.
std::pair<std::string, std::string> refusal(const std::map<std::string, std::string>& params) {
    try {
        (void)parse_options(params, 0, 1);
    } catch (const Neo4jSinkError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

std::map<std::string, std::string> with(std::map<std::string, std::string> extra) {
    auto p = base();
    for (auto& [k, v] : extra) {
        p[k] = v;
    }
    return p;
}

}  // namespace

TEST(Neo4jOptions, DefaultsAndTheQueryPath) {
    const SinkOptions o = parse_options(base(), 2, 4);
    EXPECT_EQ(o.endpoint, "http://neo4j.internal:7474");
    EXPECT_FALSE(o.https);
    EXPECT_EQ(o.database, "neo4j");
    EXPECT_EQ(o.query_path(), "/db/neo4j/query/v2");
    EXPECT_EQ(o.batch_size, 1000U);
    EXPECT_EQ(o.batch_bytes, 4U << 20);
    EXPECT_EQ(o.flush_interval_ms, 1000U);
    EXPECT_EQ(o.connect_timeout_ms, 5000U);
    EXPECT_EQ(o.request_timeout_ms, 30000U);
    EXPECT_EQ(o.retry_window_ms, 60000U);
    EXPECT_EQ(o.max_response_bytes, 1U << 20);
    EXPECT_EQ(o.subtask, 2U);
    EXPECT_EQ(o.parallelism, 4U);
    ASSERT_EQ(o.columns.size(), 2U);
    EXPECT_EQ(o.columns[0].name, "id");
    EXPECT_EQ(o.columns[1].name, "name");
}

TEST(Neo4jOptions, EveryNumericOptionIsReadStrictly) {
    const SinkOptions o = parse_options(with({{"batch_size", "250"},
                                              {"batch_bytes", "65536"},
                                              {"flush_interval_ms", "0"},
                                              {"connect_timeout_ms", "750"},
                                              {"request_timeout_ms", "9000"},
                                              {"retry_window_ms", "0"},
                                              {"max_response_bytes", "4096"},
                                              {"database", "fraud-graph.v1"}}),
                                        0,
                                        1);
    EXPECT_EQ(o.batch_size, 250U);
    EXPECT_EQ(o.batch_bytes, 65536U);
    EXPECT_EQ(o.flush_interval_ms, 0U);
    EXPECT_EQ(o.connect_timeout_ms, 750U);
    EXPECT_EQ(o.request_timeout_ms, 9000U);
    EXPECT_EQ(o.retry_window_ms, 0U);
    EXPECT_EQ(o.max_response_bytes, 4096U);
    EXPECT_EQ(o.query_path(), "/db/fraud-graph.v1/query/v2");

    for (const auto& [key, value] :
         std::map<std::string, std::string>{{"batch_size", "0"},
                                            {"batch_size", "100001"},
                                            {"batch_size", "12abc"},
                                            {"batch_size", "-1"},
                                            {"batch_size", " 5"},
                                            {"batch_bytes", "1023"},
                                            {"connect_timeout_ms", "0"},
                                            {"request_timeout_ms", ""},
                                            {"max_response_bytes", "1e6"}}) {
        SCOPED_TRACE(key + "=" + value);
        const auto [c, m] = refusal(with({{key, value}}));
        EXPECT_EQ(c, code::kOptionInvalid);
        EXPECT_NE(m.find(key), std::string::npos) << m;
    }
}

TEST(Neo4jOptions, RequiredOptionsAndUnknownKeys) {
    auto p = base();
    p.erase("endpoint");
    EXPECT_EQ(refusal(p).first, code::kOptionInvalid);
    p = base();
    p["statement"] = "   ";
    EXPECT_EQ(refusal(p).first, code::kOptionInvalid);
    p = base();
    p.erase("sql_column_types");
    EXPECT_EQ(refusal(p).first, code::kOptionInvalid);

    const auto [c, m] = refusal(with({{"batchsize", "10"}}));
    EXPECT_EQ(c, code::kOptionInvalid);
    EXPECT_NE(m.find("unknown option 'batchsize'"), std::string::npos) << m;
    EXPECT_NE(m.find("batch_size"), std::string::npos) << "the refusal lists the accepted options";
}

TEST(Neo4jOptions, KeysThePlannerAddsAreAcceptedAndIgnored) {
    const auto o = parse_options(with({{"schema_columns", "id:int64"},
                                       {"primary_key", "id"},
                                       {"bounded", "true"},
                                       {"forced_singleton", "true"},
                                       {"columnar_decode", "false"}}),
                                 0,
                                 1);
    EXPECT_EQ(o.columns.size(), 2U);
}

TEST(Neo4jOptions, TheEndpointIsSchemeHostAndPortOnly) {
    EXPECT_EQ(parse_options(with({{"endpoint", "http://h:7474/"}}), 0, 1).endpoint,
              "http://h:7474");
    for (const std::string bad : {"h:7474",
                                  "ftp://h",
                                  "http://",
                                  "http://h:7474/db/neo4j",
                                  "http://h:7474?x=1",
                                  "http://user:pw@h:7474",
                                  "http://h:0",
                                  "http://h:99999",
                                  "http://h:12x"}) {
        SCOPED_TRACE(bad);
        const auto [c, m] = refusal(with({{"endpoint", bad}}));
        EXPECT_EQ(c, code::kOptionInvalid) << m;
        if (bad.find("pw@") != std::string::npos) {
            EXPECT_EQ(m.find("pw"), std::string::npos) << "the refusal echoed a credential: " << m;
        }
    }
    if (clink::http_connector::HttpRequest::tls_supported()) {
        EXPECT_TRUE(parse_options(with({{"endpoint", "https://h:7473"}}), 0, 1).https);
    } else {
        EXPECT_EQ(refusal(with({{"endpoint", "https://h:7473"}})).first, code::kTlsUnavailable);
    }
}

TEST(Neo4jOptions, TheDatabaseNameIsValidatedAndEncoded) {
    for (const std::string& bad : std::vector<std::string>{
             "", "-db", "a b", "db/../system", "db%2f", std::string(64, 'a')}) {
        SCOPED_TRACE(bad);
        EXPECT_EQ(refusal(with({{"database", bad}})).first, code::kOptionInvalid);
    }
    EXPECT_EQ(parse_options(with({{"database", "Graph_1"}}), 0, 1).query_path(),
              "/db/Graph_1/query/v2");
}

TEST(Neo4jOptions, CredentialsComeAsAPairOrABearerTokenNotBoth) {
    auto o = parse_options(with({{"username", "neo4j"}, {"password", "secret"}}), 0, 1);
    EXPECT_EQ(o.username, "neo4j");
    EXPECT_EQ(o.password, "secret");
    o = parse_options(with({{"bearer_token", "tok"}}), 0, 1);
    EXPECT_EQ(o.bearer_token, "tok");
    EXPECT_EQ(refusal(with({{"username", "neo4j"}})).first, code::kOptionInvalid);
    EXPECT_EQ(refusal(with({{"password", "secret"}})).first, code::kOptionInvalid);
    const auto [c, m] = refusal(
        with({{"username", "neo4j"}, {"password", "pw-9f2c"}, {"bearer_token", "tk-7e1a"}}));
    EXPECT_EQ(c, code::kOptionInvalid);
    EXPECT_EQ(m.find("pw-9f2c"), std::string::npos) << m;
    EXPECT_EQ(m.find("tk-7e1a"), std::string::npos) << m;
}

TEST(Neo4jOptions, AnEnvReferenceIsResolvedAndARefusalNamesOnlyTheReference) {
    ::setenv("CLINK_NEO4J_TEST_PW", "hunter2-from-env", 1);
    ::setenv("CLINK_NEO4J_TEST_BAD_BATCH", "not-a-number-secret", 1);
    const auto o = parse_options(
        with({{"username", "neo4j"}, {"password", "env://CLINK_NEO4J_TEST_PW"}}), 0, 1);
    EXPECT_EQ(o.password, "hunter2-from-env");

    auto [c, m] = refusal(with({{"batch_size", "env://CLINK_NEO4J_TEST_BAD_BATCH"}}));
    EXPECT_EQ(c, code::kOptionInvalid);
    EXPECT_NE(m.find("env://CLINK_NEO4J_TEST_BAD_BATCH"), std::string::npos) << m;
    EXPECT_EQ(m.find("not-a-number-secret"), std::string::npos) << m;

    ::unsetenv("CLINK_NEO4J_TEST_UNSET");
    std::tie(c, m) =
        refusal(with({{"username", "neo4j"}, {"password", "env://CLINK_NEO4J_TEST_UNSET"}}));
    EXPECT_EQ(c, code::kSecretUnset);
    EXPECT_NE(m.find("env://CLINK_NEO4J_TEST_UNSET"), std::string::npos) << m;
    ::setenv("CLINK_NEO4J_TEST_EMPTY", "", 1);
    EXPECT_EQ(
        refusal(with({{"password", "env://CLINK_NEO4J_TEST_EMPTY"}, {"username", "u"}})).first,
        code::kSecretUnset);
    ::unsetenv("CLINK_NEO4J_TEST_PW");
    ::unsetenv("CLINK_NEO4J_TEST_BAD_BATCH");
    ::unsetenv("CLINK_NEO4J_TEST_EMPTY");
}

TEST(Neo4jOptions, TlsOptionsNeedAnHttpsEndpointAndAReadableCaFile) {
    EXPECT_EQ(refusal(with({{"tls_ca_file", "/etc/hosts"}})).first, code::kOptionInvalid);
    // The hostname check has no off switch.
    EXPECT_EQ(refusal(with({{"tls_verify_hostname", "false"}})).first, code::kOptionInvalid);
    if (!clink::http_connector::HttpRequest::tls_supported()) {
        GTEST_SKIP() << "this build has no TLS";
    }
    const auto ca = std::filesystem::temp_directory_path() / "clink_neo4j_options_ca.pem";
    std::ofstream(ca) << "-----BEGIN CERTIFICATE-----\n";
    const auto o =
        parse_options(with({{"endpoint", "https://h:7473"}, {"tls_ca_file", ca.string()}}), 0, 1);
    EXPECT_EQ(o.tls_ca_file, ca.string());
    EXPECT_EQ(
        refusal(with({{"endpoint", "https://h:7473"}, {"tls_ca_file", "/nonexistent.pem"}})).first,
        code::kOptionInvalid);
    std::filesystem::remove(ca);
}

TEST(Neo4jOptions, JsonNonFiniteIsRefusedBecauseTheSinkWritesTypedFloats) {
    const auto [c, m] = refusal(with({{"json_non_finite", "null"}}));
    EXPECT_EQ(c, code::kOptionInvalid);
    EXPECT_NE(m.find("typed values"), std::string::npos) << m;
}

TEST(Neo4jOptions, OnlyAtLeastOnceAppendDeliveryIsOffered) {
    EXPECT_NO_THROW((void)parse_options(with({{"delivery_guarantee", "at_least_once"},
                                              {"mode", "append"},
                                              {"changelog", "false"},
                                              {"write_mode", "append"}}),
                                        0,
                                        1));
    for (const auto& [key, value] :
         std::map<std::string, std::string>{{"delivery_guarantee", "exactly_once"},
                                            {"delivery_guarantee", "effectively_once_idempotent"},
                                            {"delivery_guarantee", "exactly_once_two_phase_commit"},
                                            {"delivery_guarantee", "at_most_once"},
                                            {"mode", "upsert"},
                                            {"changelog", "true"},
                                            {"write_mode", "overwrite"}}) {
        SCOPED_TRACE(key + "=" + value);
        EXPECT_EQ(refusal(with({{key, value}})).first, code::kGuaranteeUnsupported);
    }
}

TEST(Neo4jOptions, ColumnsWithoutANeo4jMappingAreRefusedWithAHint) {
    for (const auto& [types, hint] :
         std::map<std::string, std::string>{{"amount:DECIMAL(18, 2)", "CAST the column to VARCHAR"},
                                            {"at:TIME", "CAST it to VARCHAR"},
                                            {"blob:BYTEA", "CAST it to VARCHAR"},
                                            {"m:MAP<BIGINT, VARCHAR>", "keyed by strings"},
                                            {"xs:DECIMAL(10, 0) ARRAY", "no decimal type"},
                                            {"r:ROW<a BIGINT, t TIME>", "TIME"},
                                            {"u:uint64", "no Neo4j mapping"}}) {
        SCOPED_TRACE(types);
        const auto [c, m] = refusal(with({{"sql_column_types", types}}));
        EXPECT_EQ(c, code::kTypeUnsupported);
        EXPECT_NE(m.find(hint), std::string::npos) << m;
    }
    EXPECT_EQ(refusal(with({{"sql_column_types", "no_type_here"}})).first, code::kOptionInvalid);
}
