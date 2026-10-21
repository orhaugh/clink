// The lookup's reading of a reply: the typed values it decodes into declared
// columns, refusing anything lost or guessed; how it judges a read; and the
// open-time contract it holds a statement's EXPLAIN to.

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/http_connector/http_request.hpp"

#include "options.hpp"
#include "reply.hpp"
#include "types.hpp"

using clink::config::JsonValue;
using clink::http_connector::BoundedResponse;
using clink::neo4j::Column;
using clink::neo4j::decode_result;
using clink::neo4j::encode_value_as;
using clink::neo4j::judge_read;
using clink::neo4j::lookup_statement_problems;
using clink::neo4j::Neo4jError;
using clink::neo4j::parse_column_types;
using clink::neo4j::parse_reply;
using clink::neo4j::Reply;
using clink::neo4j::scan_operators;
using clink::neo4j::Verdict;
namespace code = clink::neo4j::code;

namespace {

Column col(const std::string& name, const std::string& type) {
    return parse_column_types(name + ":" + type).front();
}

JsonValue typed(const std::string& json) {
    return clink::config::parse(json);
}

// The refusal decoding `value` into `c` raises, or "" when it decodes.
std::string refusal(const Column& c, const std::string& value) {
    try {
        (void)decode_result(c, typed(value));
    } catch (const Neo4jError& e) {
        EXPECT_EQ(e.code(), code::kValueInvalid);
        return e.what();
    }
    return {};
}

BoundedResponse complete(int status, std::string body) {
    BoundedResponse r;
    r.status = status;
    r.body = std::move(body);
    r.complete = true;
    r.request_written = true;
    return r;
}

Reply explained(const std::string& query_type,
                const std::vector<std::string>& operators,
                const std::string& fields = R"("shared","evidence")",
                const std::string& notifications = "[]") {
    std::string plan = "{}";
    for (auto it = operators.rbegin(); it != operators.rend(); ++it) {
        plan = R"({"operatorType":")" + *it + R"(@neo4j","arguments":{},"children":[)" +
               (plan == "{}" ? std::string{} : plan) + "]}";
    }
    std::string body = R"({"data":{"fields":[)" + fields + R"(],"values":[]})";
    if (!query_type.empty()) {
        body += R"(,"queryType":")" + query_type + "\"";
    }
    if (plan != "{}") {
        body += R"(,"queryPlan":)" + plan;
    }
    body += R"(,"notifications":)" + notifications + "}";
    return parse_reply(body);
}

const std::vector<std::string> kColumns{"shared", "evidence"};

}  // namespace

TEST(Neo4jLookupTypes, IntegersFillIntegerColumnsOnlyWithinRange) {
    struct Case {
        const char* type;
        std::int64_t lo;
        std::int64_t hi;
    };
    for (const Case& c : {Case{"TINYINT", -128, 127},
                          Case{"SMALLINT", -32768, 32767},
                          Case{"INTEGER", -2147483648LL, 2147483647LL},
                          Case{"BIGINT",
                               std::numeric_limits<std::int64_t>::min(),
                               std::numeric_limits<std::int64_t>::max()}}) {
        const Column column = col("n", c.type);
        for (const std::int64_t v : {c.lo, std::int64_t{0}, c.hi}) {
            const JsonValue got = decode_result(
                column, typed(R"({"$type":"Integer","_value":")" + std::to_string(v) + "\"}"));
            ASSERT_TRUE(got.is_integral_number()) << c.type;
            EXPECT_EQ(got.as_int(), v) << c.type;
        }
        if (std::string(c.type) != "BIGINT") {
            EXPECT_NE(refusal(column,
                              R"({"$type":"Integer","_value":")" + std::to_string(c.hi + 1) + "\"}")
                          .find("outside the type's range"),
                      std::string::npos)
                << c.type;
            EXPECT_FALSE(
                refusal(column,
                        R"({"$type":"Integer","_value":")" + std::to_string(c.lo - 1) + "\"}")
                    .empty())
                << c.type;
        }
    }
    const Column big = col("n", "BIGINT");
    EXPECT_FALSE(refusal(big, R"({"$type":"Integer","_value":"9223372036854775808"})").empty());
    EXPECT_FALSE(refusal(big, R"({"$type":"Integer","_value":"12x"})").empty());
    EXPECT_FALSE(refusal(big, R"({"$type":"Integer","_value":12})").empty());
    // A Float is not quietly truncated into an integer column.
    EXPECT_NE(refusal(big, R"({"$type":"Float","_value":"2.0"})").find("toInteger()"),
              std::string::npos);
}

TEST(Neo4jLookupTypes, DoublesTakeFloatsAndOnlyExactIntegers) {
    const Column d = col("x", "DOUBLE");
    EXPECT_DOUBLE_EQ(
        decode_result(d, typed(R"({"$type":"Float","_value":"0.30000000000000004"})")).as_number(),
        0.30000000000000004);
    EXPECT_TRUE(
        std::isnan(decode_result(d, typed(R"({"$type":"Float","_value":"NaN"})")).as_number()));
    EXPECT_EQ(decode_result(d, typed(R"({"$type":"Float","_value":"-Infinity"})")).as_number(),
              -std::numeric_limits<double>::infinity());
    // 2^53 is exact; 2^53 + 1 is not.
    EXPECT_EQ(
        decode_result(d, typed(R"({"$type":"Integer","_value":"9007199254740992"})")).as_number(),
        9007199254740992.0);
    EXPECT_NE(refusal(d, R"({"$type":"Integer","_value":"9007199254740993"})")
                  .find("cannot hold exactly"),
              std::string::npos);
    EXPECT_FALSE(refusal(d, R"({"$type":"Float","_value":"1.5e"})").empty());
    EXPECT_FALSE(refusal(d, R"({"$type":"String","_value":"1.5"})").empty());
}

TEST(Neo4jLookupTypes, BooleansTextAndNulls) {
    EXPECT_TRUE(decode_result(col("b", "BOOLEAN"), typed(R"({"$type":"Boolean","_value":true})"))
                    .as_bool());
    EXPECT_FALSE(refusal(col("b", "BOOLEAN"), R"({"$type":"String","_value":"true"})").empty());
    EXPECT_EQ(decode_result(col("s", "VARCHAR"), typed(R"({"$type":"String","_value":"acct-07"})"))
                  .as_string(),
              "acct-07");
    EXPECT_FALSE(refusal(col("s", "VARCHAR"), R"({"$type":"Integer","_value":"7"})").empty());
    // U+0001 starts the engine's decimal encoding, so it cannot open a VARCHAR.
    EXPECT_NE(
        refusal(col("s", "VARCHAR"), R"({"$type":"String","_value":"\u0001x"})").find("U+0001"),
        std::string::npos);
    for (const char* type : {"BIGINT", "DOUBLE", "BOOLEAN", "VARCHAR"}) {
        EXPECT_TRUE(
            decode_result(col("n", type), typed(R"({"$type":"Null","_value":null})")).is_null())
            << type;
    }
}

TEST(Neo4jLookupTypes, GraphValuesAndUntypedJsonAreRefusedWithoutTheValue) {
    const Column s = col("s", "VARCHAR");
    const char* values[] = {
        R"({"$type":"Node","_value":{"_element_id":"4:x:1","_labels":["Account"],"_properties":{"id":{"$type":"String","_value":"secret-value"}}}})",
        R"({"$type":"Relationship","_value":{"_element_id":"5:x:1"}})",
        R"({"$type":"Path","_value":[]})",
        R"({"$type":"List","_value":[{"$type":"String","_value":"secret-value"}]})",
        R"({"$type":"Map","_value":{"k":{"$type":"String","_value":"secret-value"}}})",
        R"({"$type":"Date","_value":"2026-01-02"})",
        R"j({"$type":"Point","_value":"SRID=4326;POINT (1 2)"})j",
    };
    for (const char* v : values) {
        const auto m = refusal(s, v);
        EXPECT_FALSE(m.empty()) << v;
        EXPECT_EQ(m.find("secret-value"), std::string::npos) << m;
        EXPECT_NE(m.find("column 's' (VARCHAR)"), std::string::npos) << m;
    }
    EXPECT_NE(refusal(s, R"({"$type":"List","_value":[]})").find("join a list into text"),
              std::string::npos);
    EXPECT_NE(refusal(s, R"("secret-value")").find("not typed JSON"), std::string::npos);
    EXPECT_FALSE(refusal(s, R"({"_value":"x"})").empty());
}

TEST(Neo4jLookupTypes, TheKeyIsSentTypedAndAsAParameter) {
    std::string out;
    encode_value_as(
        col("k", "BIGINT").type, JsonValue{std::int64_t{-9223372036854775807LL - 1}}, "k", out);
    EXPECT_EQ(out, R"({"$type":"Integer","_value":"-9223372036854775808"})");
    out.clear();
    encode_value_as(col("k", "VARCHAR").type, JsonValue{std::string{"acct\"07"}}, "k", out);
    EXPECT_EQ(out, R"({"$type":"String","_value":"acct\"07"})");
    out.clear();
    try {
        encode_value_as(col("k", "BIGINT").type, JsonValue{std::string{"not a number"}}, "k", out);
        FAIL() << "a non-integer key was encoded";
    } catch (const Neo4jError& e) {
        EXPECT_EQ(e.code(), code::kValueInvalid);
        EXPECT_EQ(std::string(e.what()).find("not a number"), std::string::npos) << e.what();
    }
}

TEST(Neo4jLookupReply, AnAnsweredReadAndWhatIsNotOne) {
    Reply reply;
    auto o = judge_read(
        complete(
            202,
            R"({"data":{"fields":["shared","evidence"],"values":[[{"$type":"Integer","_value":"3"},{"$type":"String","_value":"a,b"}]]},"bookmarks":["FB:1"]})"),
        reply);
    EXPECT_EQ(o.verdict, Verdict::Committed);
    EXPECT_EQ(reply.row_count, 1U);
    EXPECT_EQ(reply.bookmark, "FB:1");
    ASSERT_EQ(reply.first_row.size(), 2U);

    o = judge_read(complete(202, R"({"data":{"fields":["shared","evidence"],"values":[]}})"),
                   reply);
    EXPECT_EQ(o.verdict, Verdict::Committed);
    EXPECT_EQ(reply.row_count, 0U);
    EXPECT_TRUE(reply.bookmark.empty());

    // A reply without rows is not an empty answer.
    o = judge_read(complete(202, R"({"bookmarks":["FB:1"]})"), reply);
    EXPECT_EQ(o.verdict, Verdict::Refused);
    // A row narrower than the columns.
    o = judge_read(
        complete(
            202,
            R"({"data":{"fields":["shared","evidence"],"values":[[{"$type":"Integer","_value":"3"}]]}})"),
        reply);
    EXPECT_EQ(o.verdict, Verdict::Refused);
    // Not JSON at all: lost, so a read may be retried.
    o = judge_read(complete(200, "<html>proxy</html>"), reply);
    EXPECT_EQ(o.verdict, Verdict::Unknown);
}

TEST(Neo4jLookupReply, ServerErrorsAndTransportFailures) {
    Reply reply;
    auto o = judge_read(
        complete(
            202,
            R"({"errors":[{"code":"Neo.TransientError.General.DatabaseUnavailable","message":"x"}]})"),
        reply);
    EXPECT_EQ(o.verdict, Verdict::RolledBack);
    o = judge_read(
        complete(400,
                 R"({"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"x"}]})"),
        reply);
    EXPECT_EQ(o.verdict, Verdict::Refused);
    // A failure the server streams after rows, as a timed-out read does.
    o = judge_read(
        complete(
            202,
            R"({"data":{"fields":["c"],"values":[]},"errors":[{"code":"Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration","message":"x"}]})"),
        reply);
    EXPECT_EQ(o.verdict, Verdict::Refused);
    EXPECT_EQ(o.code, "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration");

    BoundedResponse big;
    big.failure = BoundedResponse::Failure::TooLarge;
    big.request_written = true;
    o = judge_read(big, reply);
    EXPECT_EQ(o.verdict, Verdict::UnknownFinal);
    EXPECT_NE(o.detail.find("at most one row"), std::string::npos) << o.detail;
    BoundedResponse lost;
    lost.failure = BoundedResponse::Failure::Read;
    lost.request_written = true;
    lost.error = "connection reset";
    o = judge_read(lost, reply);
    EXPECT_EQ(o.verdict, Verdict::Unknown);
    BoundedResponse refused;
    refused.failure = BoundedResponse::Failure::Connect;
    o = judge_read(refused, reply);
    EXPECT_EQ(o.verdict, Verdict::NotSent);
}

TEST(Neo4jLookupReply, OnlyAPlainReadPassesTheOpenCheck) {
    EXPECT_TRUE(
        lookup_statement_problems(
            explained("r", {"ProduceResults", "Projection", "NodeUniqueIndexSeek"}), kColumns)
            .empty());
    for (const char* type : {"w", "rw", "s"}) {
        const auto p = lookup_statement_problems(explained(type, {"ProduceResults"}), kColumns);
        ASSERT_FALSE(p.empty()) << type;
        EXPECT_NE(p.front().find("not a read"), std::string::npos) << p.front();
    }
    // Read by type, and still refused by plan.
    for (const char* op : {"ProcedureCall", "LoadCSV", "TransactionForeach", "TransactionApply"}) {
        EXPECT_FALSE(
            lookup_statement_problems(explained("r", {"ProduceResults", op}), kColumns).empty())
            << op;
    }
    // A server that reports no query type is judged by its plan.
    for (const char* op : {"Create",
                           "Merge",
                           "SetProperty",
                           "Delete",
                           "DetachDelete",
                           "RemoveLabels",
                           "Foreach",
                           "CreateIndex",
                           "DoNothingIfExists(INDEX)"}) {
        const auto p = lookup_statement_problems(explained("", {"ProduceResults", op}), kColumns);
        ASSERT_FALSE(p.empty()) << op;
        EXPECT_NE(p.front().find("writes to the graph"), std::string::npos) << p.front();
    }
    // Neither a type nor a plan: nothing to establish it reads.
    EXPECT_FALSE(lookup_statement_problems(explained("", {}), kColumns).empty());
}

TEST(Neo4jLookupReply, TheStatementReturnsTheDeclaredColumnsAndTakesOnlyKey) {
    {
        const auto p = lookup_statement_problems(
            explained("r", {"ProduceResults"}, R"("shared","evidence","account")"), kColumns);
        ASSERT_EQ(p.size(), 1U);
        EXPECT_NE(p.front().find("not the key"), std::string::npos) << p.front();
    }
    EXPECT_FALSE(
        lookup_statement_problems(explained("r", {"ProduceResults"}, R"("shared")"), kColumns)
            .empty());
    // Order does not matter: the columns are read by name.
    EXPECT_TRUE(lookup_statement_problems(
                    explained("r", {"ProduceResults"}, R"("evidence","shared")"), kColumns)
                    .empty());
    const auto p = lookup_statement_problems(
        explained(
            "r",
            {"ProduceResults"},
            R"("shared","evidence")",
            R"([{"code":"Neo.ClientNotification.Statement.ParameterNotProvided","description":"missing: other"}])"),
        kColumns);
    ASSERT_EQ(p.size(), 1U);
    EXPECT_NE(p.front().find("other than $key"), std::string::npos) << p.front();
}

TEST(Neo4jLookupReply, ScansAreNamedForTheOpenWarning) {
    EXPECT_TRUE(scan_operators(explained("r", {"ProduceResults", "NodeUniqueIndexSeek"})).empty());
    EXPECT_EQ(scan_operators(explained("r", {"ProduceResults", "Filter", "NodeByLabelScan"})),
              (std::vector<std::string>{"NodeByLabelScan"}));
    EXPECT_EQ(scan_operators(explained("r", {"ProduceResults", "AllNodesScan"})),
              (std::vector<std::string>{"AllNodesScan"}));
}
