// What the sink accepts and refuses before it touches a store: its options,
// the guarantees it does not offer, the declared column types, and how the
// schema option is split into statements.

#include <functional>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "options.hpp"

using clink::relate::parse_options;
using clink::relate::RelateError;
using clink::relate::split_statements;
namespace code = clink::relate::code;

namespace {

std::map<std::string, std::string> good() {
    return {{"path", "/tmp/case.rl"},
            {"statement", "UNWIND $rows AS row MERGE (a:Account {id: row.id})"},
            {"sql_column_types", "id:BIGINT;name:VARCHAR"}};
}

std::map<std::string, std::string> with(std::map<std::string, std::string> extra) {
    auto params = good();
    for (auto& [k, v] : extra) {
        params[k] = v;
    }
    return params;
}

std::map<std::string, std::string> without(const std::string& key) {
    auto params = good();
    params.erase(key);
    return params;
}

struct Refusal {
    std::string code;
    std::string message;
};

Refusal refusal(const std::map<std::string, std::string>& params) {
    try {
        (void)parse_options(params, 0, 1);
    } catch (const RelateError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

}  // namespace

TEST(RelateOptions, TheDefaultsAreReadFromAMinimalTable) {
    const auto o = parse_options(good(), 2, 4);
    EXPECT_EQ(o.path, "/tmp/case.rl");
    EXPECT_EQ(o.statement, "UNWIND $rows AS row MERGE (a:Account {id: row.id})");
    EXPECT_TRUE(o.schema.empty());
    EXPECT_EQ(o.batch_size, 1000U);
    EXPECT_EQ(o.batch_bytes, 4U << 20);
    EXPECT_EQ(o.flush_interval_ms, 1000U);
    EXPECT_EQ(o.retry_window_ms, 30000U);
    ASSERT_EQ(o.columns.size(), 2U);
    EXPECT_EQ(o.columns[0].name, "id");
    EXPECT_EQ(o.columns[1].name, "name");
    EXPECT_EQ(o.subtask, 2U);
    EXPECT_EQ(o.parallelism, 4U);
}

TEST(RelateOptions, PathAndStatementAreRequired) {
    EXPECT_EQ(refusal(without("path")).code, code::kOptionInvalid);
    EXPECT_EQ(refusal(with({{"path", "  "}})).code, code::kOptionInvalid);
    EXPECT_EQ(refusal(without("statement")).code, code::kOptionInvalid);
    EXPECT_EQ(refusal(with({{"statement", " \n "}})).code, code::kOptionInvalid);
}

TEST(RelateOptions, AStoreThatCannotOutliveTheTaskOrIsNotAPathIsRefused) {
    const auto memory = refusal(with({{"path", ":memory:"}}));
    EXPECT_EQ(memory.code, code::kOptionInvalid);
    EXPECT_NE(memory.message.find("gone when the task ends"), std::string::npos) << memory.message;
    EXPECT_EQ(refusal(with({{"path", "file:///tmp/case.rl"}})).code, code::kOptionInvalid);
}

TEST(RelateOptions, AnUnknownOptionIsRefusedWithTheOnesTheSinkReads) {
    const auto r = refusal(with({{"endpoint", "http://localhost:7474"}}));
    EXPECT_EQ(r.code, code::kOptionInvalid);
    EXPECT_NE(r.message.find("unknown option 'endpoint'"), std::string::npos) << r.message;
    EXPECT_NE(r.message.find("path, statement, schema"), std::string::npos) << r.message;
}

TEST(RelateOptions, KeysThePlannerPutsOnEverySinkAreAcceptedAndIgnored) {
    const auto o = parse_options(with({{"schema_columns", "id,name"},
                                       {"primary_key", "id"},
                                       {"bounded", "true"},
                                       {"query", "SELECT 1"}}),
                                 0,
                                 1);
    EXPECT_EQ(o.columns.size(), 2U);
}

TEST(RelateOptions, NumbersAreWholeAndInRange) {
    EXPECT_EQ(parse_options(with({{"batch_size", "100000"}}), 0, 1).batch_size, 100000U);
    for (const auto& [key, value] :
         std::vector<std::pair<std::string, std::string>>{{"batch_size", "0"},
                                                          {"batch_size", "100001"},
                                                          {"batch_size", "ten"},
                                                          {"batch_size", "10 "},
                                                          {"batch_bytes", "1023"},
                                                          {"batch_bytes", "12582913"},
                                                          {"flush_interval_ms", "-1"},
                                                          {"retry_window_ms", "3600001"}}) {
        SCOPED_TRACE(key + "=" + value);
        EXPECT_EQ(refusal(with({{key, value}})).code, code::kOptionInvalid);
    }
}

TEST(RelateOptions, GuaranteesTheSinkDoesNotOfferAreRefused) {
    EXPECT_EQ(parse_options(with({{"delivery_guarantee", "at_least_once"},
                                  {"mode", "append"},
                                  {"changelog", "false"},
                                  {"write_mode", "append"}}),
                            0,
                            1)
                  .batch_size,
              1000U);
    for (const auto& [key, value] : std::vector<std::pair<std::string, std::string>>{
             {"delivery_guarantee", "exactly_once"},
             {"delivery_guarantee", "effectively_once"},
             {"mode", "upsert"},
             {"changelog", "true"},
             {"write_mode", "overwrite"}}) {
        SCOPED_TRACE(key + "=" + value);
        EXPECT_EQ(refusal(with({{key, value}})).code, code::kGuaranteeUnsupported);
    }
    EXPECT_EQ(refusal(with({{"json_non_finite", "string"}})).code, code::kOptionInvalid);
}

TEST(RelateOptions, TheDeclaredColumnsAreRequiredAndTheirTypesChecked) {
    EXPECT_EQ(refusal(without("sql_column_types")).code, code::kOptionInvalid);
    EXPECT_EQ(refusal(with({{"sql_column_types", "id"}})).code, code::kOptionInvalid);
    const auto decimal = refusal(with({{"sql_column_types", "id:BIGINT;amount:DECIMAL(18, 2)"}}));
    EXPECT_EQ(decimal.code, code::kTypeUnsupported);
    EXPECT_NE(decimal.message.find("column 'amount' (DECIMAL(18, 2))"), std::string::npos)
        << decimal.message;
    EXPECT_NE(decimal.message.find("CAST the column to VARCHAR"), std::string::npos)
        << decimal.message;
    EXPECT_EQ(refusal(with({{"sql_column_types", "t:TIME"}})).code, code::kTypeUnsupported);
    EXPECT_EQ(refusal(with({{"sql_column_types", "b:BYTEA"}})).code, code::kTypeUnsupported);
    EXPECT_EQ(refusal(with({{"sql_column_types", "m:MAP<BIGINT, VARCHAR>"}})).code,
              code::kTypeUnsupported);
    EXPECT_EQ(refusal(with({{"sql_column_types", "a:DECIMAL(9, 2) ARRAY"}})).code,
              code::kTypeUnsupported);
    const auto nested = parse_options(
        with({{"sql_column_types",
               "tags:VARCHAR ARRAY;attrs:MAP<VARCHAR, BIGINT>;who:ROW<name VARCHAR, age INTEGER>;"
               "at:TIMESTAMP(3) WITH TIME ZONE"}}),
        0,
        1);
    EXPECT_EQ(nested.columns.size(), 4U);
}

TEST(RelateOptions, TheSchemaOptionIsSplitAtTheSemicolonsThatEndStatements) {
    EXPECT_TRUE(split_statements("").empty());
    EXPECT_TRUE(split_statements(" ; ;\n").empty());
    EXPECT_EQ(split_statements("CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.x)"),
              (std::vector<std::string>{"CREATE INDEX a IF NOT EXISTS FOR (n:A) ON (n.x)"}));
    EXPECT_EQ(split_statements("  CREATE A ;\n CREATE B;  "),
              (std::vector<std::string>{"CREATE A", "CREATE B"}));
    // A semicolon in a string, a backticked name or a comment ends nothing.
    EXPECT_EQ(split_statements("CREATE (:A {s: 'x;y'}); CREATE (:`B;C`)"),
              (std::vector<std::string>{"CREATE (:A {s: 'x;y'})", "CREATE (:`B;C`)"}));
    EXPECT_EQ(split_statements("CREATE (:A {s: \"it\\\";s\"}); CREATE B"),
              (std::vector<std::string>{"CREATE (:A {s: \"it\\\";s\"})", "CREATE B"}));
    EXPECT_EQ(split_statements("CREATE A // one; two\n; CREATE B /* three; four */"),
              (std::vector<std::string>{"CREATE A // one; two", "CREATE B /* three; four */"}));
    const auto o = parse_options(
        with({{"schema",
               "CREATE CONSTRAINT a IF NOT EXISTS FOR (n:A) REQUIRE n.id IS UNIQUE;\n"
               "CREATE CONSTRAINT b IF NOT EXISTS FOR (n:B) REQUIRE n.id IS UNIQUE;"}}),
        0,
        1);
    EXPECT_EQ(o.schema.size(), 2U);
}
