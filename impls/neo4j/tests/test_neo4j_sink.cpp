// The sink against an in-process stand-in for the Query API: an httplib server
// that answers the open-time EXPLAIN and replays a scripted reply for each
// batch request. It decodes every request it gets, so the cases count what the
// server received, not what the sink says it sent. Transport faults below the
// reply (a reset before the status line, a stalled server) are the live
// suite's, through the TCP fault proxy; here a reply can still be cut short,
// stall, grow too large or carry an error.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <httplib.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "fake_query_api.hpp"
#include "neo4j_sink.hpp"
#include "options.hpp"

using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::neo4j::Neo4jSink;
using clink::neo4j::Neo4jSinkError;
using clink::neo4j::parse_options;
using clink::neo4j::testing::FakeQueryApi;
using clink::neo4j::testing::kCommitted;
using clink::neo4j::testing::Received;
using clink::neo4j::testing::Scripted;
using clink::sql::Row;
using namespace std::chrono_literals;
namespace code = clink::neo4j::code;

namespace {

Row row(std::int64_t id) {
    JsonObject obj;
    obj["id"] = JsonValue{id};
    obj["name"] = JsonValue{std::string("account-") + std::to_string(id)};
    Row r;
    r.values = clink::sql::row_columns_from_json(std::move(obj));
    return r;
}

clink::Batch<Row> rows(std::int64_t from, std::int64_t to) {
    clink::Batch<Row> b;
    for (auto id = from; id < to; ++id) {
        b.emplace(row(id));
    }
    return b;
}

// A sink wired to a runtime context, the way a runner wires it: metrics and a
// cancel signal the case controls.
struct Rig {
    explicit Rig(FakeQueryApi& api, std::map<std::string, std::string> extra = {}) {
        std::map<std::string, std::string> params = {
            {"endpoint", api.url()},
            {"statement",
             "UNWIND $rows AS row MERGE (a:Account {id: row.id}) SET a.name = row.name"},
            {"sql_column_types", "id:BIGINT;name:VARCHAR"},
            {"username", "neo4j"},
            {"password", "pw-unit-7c41"},
            {"flush_interval_ms", "0"},
            {"retry_window_ms", "10000"},
        };
        for (auto& [k, v] : extra) {
            params[k] = v;
        }
        sink = std::make_unique<Neo4jSink>(parse_options(params, 0, 1));
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{42}, "neo4j_sink", nullptr, &metrics);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink->attach_runtime(ctx.get());
    }
    ~Rig() {
        if (sink) {
            sink->close();
            sink->attach_runtime(nullptr);
        }
    }
    std::uint64_t counter(const std::string& name) {
        return metrics.counter(name + "{op_id=\"42\"}").value();
    }

    clink::MetricsRegistry metrics;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::unique_ptr<Neo4jSink> sink;
};

std::string refusal_code(const std::function<void()>& f) {
    try {
        f();
    } catch (const Neo4jSinkError& e) {
        return e.code();
    }
    return {};
}

}  // namespace

TEST(Neo4jSink, OpenExplainsTheStatementWithAnEmptyRowsParameter) {
    FakeQueryApi api;
    Rig rig(api);
    rig.sink->open();
    const auto explains = api.explains();
    ASSERT_EQ(explains.size(), 1U);
    EXPECT_EQ(explains[0].statement,
              "EXPLAIN UNWIND $rows AS row MERGE (a:Account {id: row.id}) SET a.name = row.name");
    EXPECT_TRUE(explains[0].ids.empty());
    EXPECT_EQ(explains[0].content_type, "application/vnd.neo4j.query");
    EXPECT_EQ(explains[0].accept, "application/vnd.neo4j.query");
    EXPECT_EQ(explains[0].authorization, "Basic bmVvNGo6cHctdW5pdC03YzQx");  // neo4j:pw-unit-7c41
    EXPECT_TRUE(api.batches().empty()) << "opening sends no batch";
}

TEST(Neo4jSink, OpenRefusesAStatementAgainstTheParameterContract) {
    const std::vector<std::string> replies = {
        R"({"data":{"fields":["c"],"values":[]},"queryType":"rw"})",
        R"({"data":{"fields":[],"values":[]},"queryType":"w","queryPlan":{"operatorType":)"
        R"("ProduceResults@neo4j","children":[{"operatorType":"TransactionForeach@neo4j"}]}})",
        R"({"data":{"fields":[]},"queryType":"r"})",
    };
    for (const auto& reply : replies) {
        SCOPED_TRACE(reply);
        FakeQueryApi api;
        api.explain_reply(202, reply);
        Rig rig(api);
        EXPECT_EQ(refusal_code([&] { rig.sink->open(); }), code::kStatementRefused);
    }
}

TEST(Neo4jSink, OpenRefusesCredentialsWithoutQuotingThem) {
    FakeQueryApi api;
    api.explain_reply(
        401,
        R"({"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":"Invalid credential."}]})");
    Rig rig(api);
    try {
        rig.sink->open();
        FAIL() << "a 401 was accepted";
    } catch (const Neo4jSinkError& e) {
        EXPECT_EQ(e.code(), code::kAuthentication);
        const std::string m = e.what();
        EXPECT_NE(m.find("database 'neo4j'"), std::string::npos) << m;
        EXPECT_NE(m.find("Neo.ClientError.Security.Unauthorized"), std::string::npos) << m;
        EXPECT_EQ(m.find("pw-unit-7c41"), std::string::npos) << m;
        EXPECT_EQ(m.find("bmVvNGo6"), std::string::npos) << "the auth header leaked: " << m;
    }
    EXPECT_EQ(api.explains().size(), 1U) << "an authentication failure is not retried";
}

TEST(Neo4jSink, RowsAreBatchedBySizeAndTheBarrierSendsThePartialBatch) {
    FakeQueryApi api;
    Rig rig(api, {{"batch_size", "3"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 8));
    auto got = api.batches();
    ASSERT_EQ(got.size(), 2U) << "seven rows at batch_size=3: two full batches";
    EXPECT_EQ(got[0].ids, (std::vector<std::int64_t>{1, 2, 3}));
    EXPECT_EQ(got[1].ids, (std::vector<std::int64_t>{4, 5, 6}));
    EXPECT_EQ(rig.sink->buffered_rows(), 1U);
    rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    got = api.batches();
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[2].ids, (std::vector<std::int64_t>{7}));
    EXPECT_EQ(rig.sink->buffered_rows(), 0U);
    EXPECT_EQ(rig.sink->counts().batches_committed, 3U);
    EXPECT_EQ(rig.sink->counts().rows_committed, 7U);
    // A barrier with nothing buffered sends nothing.
    rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{2}});
    EXPECT_EQ(api.batches().size(), 3U);
}

TEST(Neo4jSink, BatchBytesBoundsEveryRequest) {
    FakeQueryApi api;
    Rig rig(api, {{"batch_bytes", "1024"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 41));
    rig.sink->flush();
    const auto got = api.batches();
    ASSERT_GT(got.size(), 1U);
    std::vector<std::int64_t> all;
    for (const auto& b : got) {
        EXPECT_LE(b.bytes, 1024U);
        all.insert(all.end(), b.ids.begin(), b.ids.end());
    }
    std::vector<std::int64_t> expected;
    for (std::int64_t id = 1; id < 41; ++id) {
        expected.push_back(id);
    }
    EXPECT_EQ(all, expected) << "every row, once, in order";
}

TEST(Neo4jSink, ARowLargerThanABatchIsRefused) {
    FakeQueryApi api;
    Rig rig(api, {{"batch_bytes", "1024"}});
    rig.sink->open();
    JsonObject big;
    big["id"] = JsonValue{std::int64_t{1}};
    big["name"] = JsonValue{std::string(2000, 'x')};
    Row r;
    r.values = clink::sql::row_columns_from_json(std::move(big));
    clink::Batch<Row> b;
    b.emplace(std::move(r));
    EXPECT_EQ(refusal_code([&] { rig.sink->on_data(b); }), code::kRowTooLarge);
}

TEST(Neo4jSink, TheFlushIntervalSendsAPartialBatchAsInputArrives) {
    FakeQueryApi api;
    Rig rig(api, {{"flush_interval_ms", "50"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 2));
    EXPECT_TRUE(api.batches().empty());
    std::this_thread::sleep_for(80ms);
    rig.sink->on_data(rows(2, 3));
    const auto got = api.batches();
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0].ids, (std::vector<std::int64_t>{1, 2}));
}

TEST(Neo4jSink, ARetryableErrorIsRetriedWithTheSameRows) {
    FakeQueryApi api;
    api.script(
        {Scripted::Kind::Reply,
         400,
         R"({"errors":[{"code":"Neo.TransientError.Transaction.DeadlockDetected","message":"deadlock"}]})"});
    Rig rig(api);
    rig.sink->open();
    rig.sink->on_data(rows(1, 4));
    rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    const auto got = api.batches();
    ASSERT_EQ(got.size(), 2U);
    EXPECT_EQ(got[0].ids, got[1].ids);
    EXPECT_EQ(rig.sink->counts().attempts_failed, 1U);
    EXPECT_EQ(rig.sink->counts().retries, 1U);
    EXPECT_EQ(rig.sink->counts().outcome_unknown, 0U) << "a reported rollback is not unknown";
    EXPECT_EQ(rig.counter("clink_neo4j_rows_submitted_total"), 6U);
    EXPECT_EQ(rig.counter("clink_neo4j_rows_committed_total"), 3U);
    EXPECT_EQ(rig.counter("clink_neo4j_batches_committed_total"), 1U);
    EXPECT_EQ(rig.counter("clink_neo4j_retries_total"), 1U);
    EXPECT_EQ(
        rig.metrics
            .counter("clink_neo4j_attempts_failed_total{op_id=\"42\",verdict=\"rolled_back\"}")
            .value(),
        1U);
}

TEST(Neo4jSink, AReplyCutShortIsAnUnknownOutcomeThatIsCountedAndRetried) {
    FakeQueryApi api;
    api.script({Scripted::Kind::CutShort, 202, kCommitted});
    api.script(
        {Scripted::Kind::Reply, 202, R"({"data":{"fields":[],"values":[]}})"});  // no bookmarks
    Rig rig(api);
    rig.sink->open();
    rig.sink->on_data(rows(1, 3));
    rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    EXPECT_EQ(api.batches().size(), 3U);
    EXPECT_EQ(rig.sink->counts().outcome_unknown, 2U);
    EXPECT_EQ(rig.sink->counts().rows_maybe_applied_twice, 4U);
    EXPECT_EQ(rig.counter("clink_neo4j_outcome_unknown_total"), 2U);
    EXPECT_EQ(rig.counter("clink_neo4j_batches_committed_total"), 1U);
}

TEST(Neo4jSink, ARefusedBatchFailsTheBarrierOnceAndKeepsItsRows) {
    FakeQueryApi api;
    api.script(
        {Scripted::Kind::Reply,
         400,
         R"({"errors":[{"code":"Neo.ClientError.Schema.ConstraintValidationFailed",)"
         R"("message":"Node(9) already exists with label `Account` and property `id` = 2"}]})"});
    Rig rig(api);
    rig.sink->open();
    rig.sink->on_data(rows(1, 4));
    try {
        rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
        FAIL() << "a refused batch let the barrier through";
    } catch (const Neo4jSinkError& e) {
        EXPECT_EQ(e.code(), code::kServerRefused);
        const std::string m = e.what();
        EXPECT_NE(m.find("ConstraintValidationFailed"), std::string::npos) << m;
        EXPECT_EQ(m.find("= 2"), std::string::npos) << "a row value leaked: " << m;
        EXPECT_NE(m.find("rolled back"), std::string::npos) << m;
    }
    EXPECT_EQ(api.batches().size(), 1U) << "a refusal is not retried";
    EXPECT_EQ(rig.sink->buffered_rows(), 3U) << "the rows stay until a commit or the job fails";
    EXPECT_EQ(rig.counter("clink_neo4j_batches_committed_total"), 0U);
}

TEST(Neo4jSink, TheRetryWindowBoundsRetries) {
    FakeQueryApi api;
    for (int i = 0; i < 100; ++i) {
        api.script({Scripted::Kind::Reply, 503, "unavailable"});
    }
    Rig rig(api, {{"retry_window_ms", "400"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 2));
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(refusal_code(
                  [&] { rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}); }),
              code::kRetriesExhausted);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 5s);
    EXPECT_GE(api.batches().size(), 2U);
    EXPECT_EQ(rig.sink->counts().outcome_unknown, api.batches().size())
        << "a 503 without a Neo4j error does not say the statement did not run";
}

TEST(Neo4jSink, ARetryWindowOfZeroMakesOneAttempt) {
    FakeQueryApi api;
    api.script({Scripted::Kind::Reply, 503, "unavailable"});
    Rig rig(api, {{"retry_window_ms", "0"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 2));
    EXPECT_EQ(refusal_code([&] { rig.sink->flush(); }), code::kRetriesExhausted);
    EXPECT_EQ(api.batches().size(), 1U);
}

TEST(Neo4jSink, AnOversizedReplyIsUnknownAndNotRetried) {
    FakeQueryApi api;
    api.script({Scripted::Kind::Reply,
                202,
                R"({"data":{"fields":["x"],"values":[")" + std::string(5000, 'x') +
                    R"("]},"bookmarks":["b"]})"});
    Rig rig(api, {{"max_response_bytes", "1024"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 2));
    EXPECT_EQ(refusal_code([&] { rig.sink->flush(); }), code::kOutcomeUnknown);
    EXPECT_EQ(api.batches().size(), 1U);
    EXPECT_EQ(rig.sink->counts().outcome_unknown, 1U);
}

TEST(Neo4jSink, TheRequestDeadlineEndsAStalledAttemptAsUnknownAndItIsRetried) {
    FakeQueryApi api;
    api.script({Scripted::Kind::Stall, 202, kCommitted, 3000ms});
    Rig rig(api, {{"request_timeout_ms", "300"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 3));
    const auto start = std::chrono::steady_clock::now();
    rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    EXPECT_LT(std::chrono::steady_clock::now() - start, 2500ms)
        << "the attempt was not ended at its deadline";
    EXPECT_EQ(api.batches().size(), 2U);
    EXPECT_EQ(rig.sink->counts().outcome_unknown, 1U);
    EXPECT_EQ(rig.sink->counts().batches_committed, 1U);
}

TEST(Neo4jSink, ACancelEndsAStalledRequestPromptly) {
    FakeQueryApi api;
    api.script({Scripted::Kind::Stall, 202, kCommitted, 20000ms});
    Rig rig(api, {{"request_timeout_ms", "30000"}});
    rig.sink->open();
    rig.sink->on_data(rows(1, 3));
    std::thread canceller([&] {
        std::this_thread::sleep_for(300ms);
        rig.cancel->store(true);
    });
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(refusal_code(
                  [&] { rig.sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}); }),
              code::kCancelled);
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - start, 3s);
    EXPECT_EQ(api.batches().size(), 1U) << "a cancelled batch is not retried";
}

TEST(Neo4jSink, ACancelledCloseSendsNothingStillBuffered) {
    FakeQueryApi api;
    Rig rig(api);
    rig.sink->open();
    rig.sink->on_data(rows(1, 4));
    rig.sink->close_cancelled();
    EXPECT_TRUE(api.batches().empty());
}
