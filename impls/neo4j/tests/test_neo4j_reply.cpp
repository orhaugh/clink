// Judging an attempt from the whole exchange. The reply bodies are the ones a
// Neo4j 2026.09.0 Community server returned for these cases (docs/connectors/
// neo4j.md shows how they were taken), so the judging is checked against what
// the server actually sends, not against a reading of its documentation.

#include <string>

#include <gtest/gtest.h>

#include "clink/http_connector/http_request.hpp"

#include "reply.hpp"

using clink::http_connector::BoundedResponse;
using clink::neo4j::ErrorClass;
using clink::neo4j::judge_check;
using clink::neo4j::judge_commit;
using clink::neo4j::parse_reply;
using clink::neo4j::Reply;
using clink::neo4j::statement_problems;
using clink::neo4j::Verdict;
using Failure = BoundedResponse::Failure;

namespace {

constexpr char kCommitted[] =
    R"({"data":{"fields":[],"values":[]},"queryType":"w","resultAvailableAfter":22,)"
    R"("resultConsumedAfter":25,"bookmarks":["FB:kcwQoOBbiBjhSPeyHgi1xoJjABWQ"]})";
constexpr char kSyntax[] =
    R"({"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"Invalid input )"
    R"j('CREAT': expected 'CREATE' (line 1, column 21 (offset: 20))"}]})j";
constexpr char kConstraint[] =
    R"({"errors":[{"code":"Neo.ClientError.Schema.ConstraintValidationFailed","message":)"
    R"("Node(2) already exists with label `PC` and property `id` = 10"}]})";
constexpr char kUnauthorized[] =
    R"({"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":"Invalid credential."}]})";
constexpr char kCredentialsExpired[] =
    R"({"errors":[{"code":"Neo.ClientError.Security.CredentialsExpired","message":"ACCESS on )"
    R"(database 'neo4j' is not allowed."}]})";
constexpr char kNoDatabase[] =
    R"({"errors":[{"code":"Neo.ClientError.Database.DatabaseNotFound","message":"Graph not found: nosuchdb"}]})";
constexpr char kMemory[] =
    R"({"errors":[{"code":"Neo.TransientError.General.MemoryPoolOutOfMemoryError","message":)"
    R"("The allocation of an extra 2.0 MiB would use more than the limit"}]})";
// A failure while the result streamed: rows, then errors, and no bookmarks.
constexpr char kMidStream[] =
    R"({"data":{"fields":["x"],"values":[[{"$type":"Integer","_value":"1"}]]},)"
    R"("errors":[{"code":"Neo.ClientError.Statement.ArithmeticError","message":"/ by zero"}]})";

BoundedResponse complete(int status, std::string body) {
    BoundedResponse r;
    r.status = status;
    r.body = std::move(body);
    r.complete = true;
    r.request_written = true;
    return r;
}

BoundedResponse failed(Failure f, bool written) {
    BoundedResponse r;
    r.failure = f;
    r.request_written = written;
    r.error = "transport detail";
    return r;
}

}  // namespace

TEST(Neo4jReply, OnlyACompleteReplyWithBookmarksAndNoErrorsIsACommit) {
    EXPECT_EQ(judge_commit(complete(202, kCommitted)).verdict, Verdict::Committed);
    // The status is not proof: a 202 that streamed an error rolled back.
    const auto mid = judge_commit(complete(202, kMidStream));
    EXPECT_EQ(mid.verdict, Verdict::Refused);
    EXPECT_EQ(mid.code, "Neo.ClientError.Statement.ArithmeticError");
    // A 202 with neither bookmarks nor errors proves nothing either way.
    EXPECT_EQ(judge_commit(complete(202, R"({"data":{"fields":[],"values":[]}})")).verdict,
              Verdict::Unknown);
    // Bookmarks beside errors: believe neither.
    EXPECT_EQ(
        judge_commit(
            complete(
                202,
                R"({"bookmarks":["b"],"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"x"}]})"))
            .verdict,
        Verdict::Unknown);
    // An empty bookmark list is not a bookmark.
    EXPECT_EQ(judge_commit(complete(202, R"({"bookmarks":[]})")).verdict, Verdict::Unknown);
}

TEST(Neo4jReply, ATruncatedOrMalformedReplyIsAnUnknownOutcome) {
    const std::string body = kCommitted;
    for (const std::string& cut : {body.substr(0, body.size() / 2),
                                   body.substr(0, body.size() - 1),
                                   std::string("not json"),
                                   std::string("[1,2]")}) {
        SCOPED_TRACE(cut);
        EXPECT_EQ(judge_commit(complete(202, cut)).verdict, Verdict::Unknown);
    }
    EXPECT_EQ(judge_commit(complete(503, "")).verdict, Verdict::Unknown);
    EXPECT_EQ(judge_commit(complete(502, "<html>bad gateway</html>")).verdict, Verdict::Unknown);
}

TEST(Neo4jReply, ServerErrorsAreClassifiedAsTheDriverClassifiesThem) {
    const auto syntax = judge_commit(complete(400, kSyntax));
    EXPECT_EQ(syntax.verdict, Verdict::Refused);
    EXPECT_EQ(syntax.error_class, ErrorClass::Statement);
    EXPECT_NE(syntax.detail.find("Invalid input"), std::string::npos)
        << "a syntax error names the statement, not the data, so its message is kept";

    const auto auth = judge_commit(complete(401, kUnauthorized));
    EXPECT_EQ(auth.verdict, Verdict::Refused);
    EXPECT_EQ(auth.error_class, ErrorClass::Authentication);
    EXPECT_EQ(judge_commit(complete(400, kCredentialsExpired)).error_class,
              ErrorClass::Authorisation);
    EXPECT_EQ(judge_commit(complete(404, kNoDatabase)).verdict, Verdict::Refused);

    const auto transient = judge_commit(complete(400, kMemory));
    EXPECT_EQ(transient.verdict, Verdict::RolledBack);
    EXPECT_EQ(transient.error_class, ErrorClass::Retryable);

    using clink::neo4j::classify_code;
    EXPECT_EQ(classify_code("Neo.TransientError.Transaction.DeadlockDetected"),
              ErrorClass::Retryable);
    EXPECT_EQ(classify_code("Neo.TransientError.General.DatabaseUnavailable"),
              ErrorClass::Retryable);
    EXPECT_EQ(classify_code("Neo.ClientError.Cluster.NotALeader"), ErrorClass::Retryable);
    EXPECT_EQ(classify_code("Neo.ClientError.General.ForbiddenOnReadOnlyDatabase"),
              ErrorClass::Retryable);
    EXPECT_EQ(classify_code("Neo.ClientError.Security.AuthorizationExpired"),
              ErrorClass::Retryable);
    // Re-classified as client errors in 5.0; the driver never retries them.
    EXPECT_EQ(classify_code("Neo.TransientError.Transaction.Terminated"), ErrorClass::Other);
    EXPECT_EQ(classify_code("Neo.TransientError.Transaction.LockClientStopped"), ErrorClass::Other);
    EXPECT_EQ(classify_code("Neo.ClientError.Security.Forbidden"), ErrorClass::Authorisation);
    EXPECT_EQ(classify_code("Neo.ClientError.Security.TokenExpired"), ErrorClass::Authentication);
    EXPECT_EQ(classify_code("Neo.ClientError.Schema.ConstraintValidationFailed"), ErrorClass::Data);
    EXPECT_EQ(classify_code("Neo.ClientError.Statement.TypeError"), ErrorClass::Data);
    EXPECT_EQ(classify_code("Neo.DatabaseError.General.UnknownError"), ErrorClass::Other);
}

TEST(Neo4jReply, AMessageThatCanQuoteRowValuesIsWithheld) {
    const auto o = judge_commit(complete(400, kConstraint));
    EXPECT_EQ(o.verdict, Verdict::Refused);
    EXPECT_EQ(o.error_class, ErrorClass::Data);
    EXPECT_NE(o.detail.find("Neo.ClientError.Schema.ConstraintValidationFailed"),
              std::string::npos);
    EXPECT_EQ(o.detail.find("= 10"), std::string::npos)
        << "the property value leaked: " << o.detail;
    EXPECT_NE(o.detail.find("withheld"), std::string::npos);
    EXPECT_EQ(judge_commit(complete(202, kMidStream)).detail.find("by zero"), std::string::npos);
}

TEST(Neo4jReply, StatusCodesWithoutANeo4jErrorAreReadDeliberately) {
    EXPECT_EQ(judge_commit(complete(401, "")).error_class, ErrorClass::Authentication);
    EXPECT_EQ(judge_commit(complete(403, "")).error_class, ErrorClass::Authorisation);
    EXPECT_EQ(judge_commit(complete(404, "not found")).verdict, Verdict::Refused);
    EXPECT_EQ(judge_commit(complete(307, "")).verdict, Verdict::Refused);
    EXPECT_EQ(judge_commit(complete(429, "")).verdict, Verdict::RolledBack);
    EXPECT_EQ(judge_commit(complete(408, "")).verdict, Verdict::Unknown);
}

TEST(Neo4jReply, TransportFailuresSayWhetherTheServerCouldHaveActed) {
    EXPECT_EQ(judge_commit(failed(Failure::Connect, false)).verdict, Verdict::NotSent);
    EXPECT_EQ(judge_commit(failed(Failure::Write, false)).verdict, Verdict::NotSent);
    EXPECT_EQ(judge_commit(failed(Failure::TlsHandshake, false)).verdict, Verdict::NotSent);
    EXPECT_EQ(judge_commit(failed(Failure::TlsVerify, false)).verdict, Verdict::TlsRefused);
    EXPECT_EQ(judge_commit(failed(Failure::Read, true)).verdict, Verdict::Unknown);
    EXPECT_EQ(judge_commit(failed(Failure::Other, true)).verdict, Verdict::Unknown);
    EXPECT_EQ(judge_commit(failed(Failure::TooLarge, true)).verdict, Verdict::UnknownFinal);
}

TEST(Neo4jReply, TheOpenCheckReadsTheExplainOfTheStatement) {
    // EXPLAIN UNWIND $rows AS row MERGE (n:X {id: row.id}), trimmed to the
    // parts the check reads.
    const std::string ok =
        R"({"data":{"fields":[],"values":[]},"queryPlan":{"operatorType":"ProduceResults@neo4j",)"
        R"("arguments":{"runtime-version":{"$type":"String","_value":"2026.09.0"}},"children":[)"
        R"({"operatorType":"EmptyResult@neo4j","children":[{"operatorType":"Apply@neo4j",)"
        R"("children":[{"operatorType":"Unwind@neo4j","children":[]},{"operatorType":"Merge@neo4j",)"
        R"("children":[]}]}]}]},"queryType":"w","bookmarks":["b"]})";
    Reply reply;
    EXPECT_EQ(judge_check(complete(202, ok), reply).verdict, Verdict::Committed);
    EXPECT_EQ(reply.server_version, "2026.09.0");
    EXPECT_TRUE(statement_problems(reply).empty());

    // The reply of an EXPLAIN commits nothing, so it needs no bookmarks.
    EXPECT_EQ(judge_check(complete(202, R"({"data":{"fields":[],"values":[]}})"), reply).verdict,
              Verdict::Committed);

    const auto problems_of = [](const std::string& body) {
        return statement_problems(parse_reply(body));
    };
    const auto returns = problems_of(R"({"data":{"fields":["c"],"values":[]},"queryType":"rw"})");
    ASSERT_EQ(returns.size(), 1U);
    EXPECT_NE(returns[0].find("returns columns (c)"), std::string::npos);
    const auto in_tx = problems_of(
        R"({"data":{"fields":[],"values":[]},"queryType":"w","queryPlan":{"operatorType":)"
        R"("ProduceResults@neo4j","children":[{"operatorType":"TransactionForeach@neo4j","children":[]}]}})");
    ASSERT_EQ(in_tx.size(), 1U);
    EXPECT_NE(in_tx[0].find("IN TRANSACTIONS"), std::string::npos);
    EXPECT_EQ(problems_of(R"({"data":{"fields":[]},"queryType":"r"})").size(), 1U);
    EXPECT_EQ(problems_of(R"({"data":{"fields":[]},"queryType":"s"})").size(), 1U);
    const auto params = problems_of(
        R"({"data":{"fields":[]},"queryType":"w","notifications":[{"code":)"
        R"("Neo.ClientNotification.Statement.ParameterNotProvided","description":)"
        R"j("Did not supply query with enough parameters. (Missing parameters: since)"}]})j");
    ASSERT_EQ(params.size(), 1U);
    EXPECT_NE(params[0].find("since"), std::string::npos);
    // A server older than 2026.07 reports no query type: nothing to refuse on it.
    EXPECT_TRUE(problems_of(R"({"data":{"fields":[],"values":[]}})").empty());

    EXPECT_EQ(judge_check(complete(400, kSyntax), reply).verdict, Verdict::Refused);
    EXPECT_EQ(judge_check(complete(400, kMemory), reply).verdict, Verdict::RolledBack);
    EXPECT_EQ(judge_check(failed(Failure::Read, true), reply).verdict, Verdict::Unknown);
}
