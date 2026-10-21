// The lookup join operator against an in-process Query API stand-in: what each
// read sends, how an answer, a no-match and a failure reach the output, the
// order of results around watermarks and barriers when replies come back out
// of order, and the bounds: concurrency, in-flight input, retries, deadlines,
// reply size and cancellation.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_kind.hpp"

#include "fake_lookup_api.hpp"
#include "lookup.hpp"
#include "options.hpp"

using namespace std::chrono_literals;
using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::Emitter;
using clink::EventTime;
using clink::StreamElement;
using clink::Watermark;
using clink::config::JsonValue;
using clink::neo4j::LookupCall;
using clink::neo4j::LookupPool;
using clink::neo4j::Neo4jError;
using clink::neo4j::Neo4jLookupJoin;
using clink::neo4j::parse_lookup_options;
using clink::neo4j::testing::FakeLookupApi;
using clink::neo4j::testing::LookupAnswer;
using clink::neo4j::testing::typed_int;
using clink::neo4j::testing::typed_null;
using clink::neo4j::testing::typed_str;
using clink::sql::Row;
namespace code = clink::neo4j::code;

namespace {

using Clock = std::chrono::steady_clock;

constexpr char kStatement[] =
    "MATCH (a:Account {id: $key}) RETURN a.shared AS shared, a.evidence AS evidence";

std::map<std::string, std::string> params(const FakeLookupApi& api,
                                          std::map<std::string, std::string> extra = {}) {
    std::map<std::string, std::string> p = {
        {"probe_alias", "s"},
        {"dim_alias", "d"},
        {"probe_columns", "event_id,account"},
        {"dim_columns", "account,shared,evidence,matched,observed_at,bookmark"},
        {"join_type", "left_outer"},
        {"probe_key_column", "account"},
        {"probe_key_type", "BIGINT"},
        {"dim_key_column", "account"},
        {"dim_column_types",
         "account:BIGINT;shared:BIGINT;evidence:VARCHAR;matched:BOOLEAN;observed_at:TIMESTAMP(3);"
         "bookmark:VARCHAR"},
        {"neo4j.endpoint", api.url()},
        {"neo4j.statement", kStatement},
        {"neo4j.username", "reader"},
        {"neo4j.password", "pw-lookup-unit-91"},
        {"neo4j.matched_column", "matched"},
        {"neo4j.observed_at_column", "observed_at"},
        {"neo4j.bookmark_column", "bookmark"},
        {"neo4j.retry_window_ms", "10000"},
    };
    for (auto& [k, v] : extra) {
        p["neo4j." + k] = v;
    }
    return p;
}

Row probe(std::int64_t event, std::optional<std::int64_t> account) {
    Row r;
    r.values["event_id"] = JsonValue{event};
    r.values["account"] = account ? JsonValue{*account} : JsonValue{};
    return r;
}

StreamElement<Row> data(std::vector<Row> rows) {
    Batch<Row> b;
    for (auto& r : rows) {
        b.emplace(std::move(r));
    }
    return StreamElement<Row>::data(std::move(b));
}

LookupAnswer one(std::int64_t shared, const std::string& evidence) {
    LookupAnswer a;
    a.rows = {{typed_int(shared), typed_str(evidence)}};
    return a;
}

// The operator wired the way a runner wires it, with an emitter that keeps
// what comes out as "row:<event_id>", "wm:<ms>" and "barrier:<id>" in order,
// and the rows themselves.
struct Rig {
    explicit Rig(const FakeLookupApi& api, std::map<std::string, std::string> extra = {}) {
        op = std::make_unique<Neo4jLookupJoin>(
            parse_lookup_options(params(api, std::move(extra)), 0, 1));
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{77}, "neo4j_lookup_join_row", nullptr, &metrics);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        op->attach_runtime(ctx.get());
    }
    ~Rig() {
        if (op) {
            op->close();
            op->attach_runtime(nullptr);
        }
    }
    void process(StreamElement<Row> e) { op->process(e, out); }
    [[nodiscard]] std::vector<std::string> order() const {
        const std::lock_guard<std::mutex> lock(mu);
        return seen;
    }
    [[nodiscard]] std::vector<Row> rows_out() const {
        const std::lock_guard<std::mutex> lock(mu);
        return rows;
    }
    std::uint64_t counter(const std::string& name) {
        return metrics.counter(name + "{op_id=\"77\"}").value();
    }

    clink::MetricsRegistry metrics;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::unique_ptr<Neo4jLookupJoin> op;
    mutable std::mutex mu;
    std::vector<std::string> seen;
    std::vector<Row> rows;
    Emitter<Row> out{[this](StreamElement<Row> e) {
        const std::lock_guard<std::mutex> lock(mu);
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                seen.push_back("row:" + std::to_string(r.value().values.at("s_event_id").as_int()));
                rows.push_back(r.value());
            }
        } else if (e.is_watermark()) {
            seen.push_back("wm:" + std::to_string(e.as_watermark().timestamp().millis()));
        } else if (e.is_barrier()) {
            seen.push_back("barrier:" + std::to_string(e.as_barrier().id().value()));
        }
        return true;
    }};
};

// The refusal `f` throws, as its code and message.
std::pair<std::string, std::string> failure(const std::function<void()>& f) {
    try {
        f();
    } catch (const Neo4jError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

TEST(Neo4jLookup, AMatchCarriesTheProbeTheGraphFactsAndWhenTheyWereRead) {
    FakeLookupApi api({"shared", "evidence"});
    api.answer("7", one(3, "acct-02,acct-03,acct-05"));
    Rig rig(api);
    rig.op->open();
    const auto before = now_ms();
    rig.process(data({probe(1, 7)}));
    rig.op->flush(rig.out);
    const auto after = now_ms();

    const auto rows = rig.rows_out();
    ASSERT_EQ(rows.size(), 1U);
    const auto& r = rows[0].values;
    EXPECT_EQ(r.at("s_event_id").as_int(), 1);
    EXPECT_EQ(r.at("s_account").as_int(), 7);
    EXPECT_EQ(r.at("d_account").as_int(), 7);
    EXPECT_EQ(r.at("d_shared").as_int(), 3);
    EXPECT_EQ(r.at("d_evidence").as_string(), "acct-02,acct-03,acct-05");
    EXPECT_TRUE(r.at("d_matched").as_bool());
    EXPECT_GE(r.at("d_observed_at").as_int(), before);
    EXPECT_LE(r.at("d_observed_at").as_int(), after);
    EXPECT_EQ(r.at("d_bookmark").as_string(), "FB:read");

    // One read: typed $key, read-only, the statement as configured.
    const auto reads = api.reads();
    ASSERT_EQ(reads.size(), 1U);
    EXPECT_EQ(reads[0].statement, kStatement);
    EXPECT_EQ(reads[0].access_mode, "READ");
    EXPECT_TRUE(reads[0].max_execution_time.empty());
    EXPECT_EQ(reads[0].key.at("$type").as_string(), "Integer");
    EXPECT_EQ(reads[0].key.at("_value").as_string(), "7");
    EXPECT_EQ(reads[0].authorization, "Basic cmVhZGVyOnB3LWxvb2t1cC11bml0LTkx");
    EXPECT_EQ(api.explains(), 1U);
    EXPECT_EQ(rig.counter("clink_neo4j_lookup_requests_total"), 1U);
    EXPECT_EQ(rig.counter("clink_neo4j_lookup_matches_total"), 1U);
}

TEST(Neo4jLookup, NoMatchAndANullKeyAreExplicitAndDistinct) {
    FakeLookupApi api({"shared", "evidence"});
    Rig rig(api);
    rig.op->open();
    rig.process(data({probe(1, 8), probe(2, std::nullopt)}));
    rig.op->flush(rig.out);
    const auto rows = rig.rows_out();
    ASSERT_EQ(rows.size(), 2U);
    // The graph had nothing for key 8: read, answered, no match.
    const auto& miss = rows[0].values;
    EXPECT_FALSE(miss.at("d_matched").as_bool());
    EXPECT_TRUE(miss.at("d_account").is_null());
    EXPECT_TRUE(miss.at("d_shared").is_null());
    EXPECT_TRUE(miss.at("d_evidence").is_null());
    EXPECT_TRUE(miss.at("d_observed_at").is_integral_number());
    EXPECT_EQ(miss.at("d_bookmark").as_string(), "FB:read");
    // A NULL key cannot equal anything: no match, and no request.
    const auto& null_key = rows[1].values;
    EXPECT_FALSE(null_key.at("d_matched").as_bool());
    EXPECT_TRUE(null_key.at("d_observed_at").is_null());
    EXPECT_TRUE(null_key.at("d_bookmark").is_null());
    EXPECT_EQ(api.reads().size(), 1U);
    EXPECT_EQ(rig.counter("clink_neo4j_lookup_no_match_total"), 1U);
    EXPECT_EQ(rig.counter("clink_neo4j_lookup_null_keys_total"), 1U);
}

TEST(Neo4jLookup, TwoRowsForOneKeyFailTheLookupInsteadOfChoosing) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer two;
    two.rows = {{typed_int(1), typed_str("a")}, {typed_int(2), typed_str("b")}};
    api.answer("9", two);
    Rig rig(api);
    rig.op->open();
    const auto [c, m] = failure([&] {
        rig.process(data({probe(1, 9)}));
        rig.op->flush(rig.out);
    });
    EXPECT_EQ(c, code::kMultipleRows);
    EXPECT_NE(m.find("returned 2 rows for one key"), std::string::npos) << m;
    EXPECT_TRUE(rig.rows_out().empty()) << "a failed lookup produced a row";
    EXPECT_EQ(
        rig.metrics
            .counter("clink_neo4j_lookup_failures_total{op_id=\"77\",code=\"neo4j.multiple_rows\"}")
            .value(),
        1U);
}

TEST(Neo4jLookup, AValueOfTheWrongTypeFailsWithoutQuotingIt) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer wrong;
    wrong.rows = {{typed_str("secret-shared-value"), typed_str("x")}};
    api.answer("5", wrong);
    LookupAnswer big;
    big.rows = {{R"({"$type":"Integer","_value":"9223372036854775808"})", typed_str("x")}};
    api.answer("6", big);
    for (const std::int64_t key : {5, 6}) {
        Rig rig(api);
        rig.op->open();
        const auto [c, m] = failure([&] {
            rig.process(data({probe(1, key)}));
            rig.op->flush(rig.out);
        });
        EXPECT_EQ(c, code::kValueInvalid) << key;
        EXPECT_NE(m.find("neo4j lookup: database 'neo4j'"), std::string::npos) << m;
        EXPECT_NE(m.find("column 'shared' (BIGINT)"), std::string::npos) << m;
        EXPECT_EQ(m.find("secret-shared-value"), std::string::npos) << m;
        EXPECT_TRUE(rig.rows_out().empty());
    }
}

TEST(Neo4jLookup, AMalformedReplyFailsAndIsNeverANoMatch) {
    const std::vector<std::string> bodies = {
        R"({"bookmarks":["FB:1"]})",
        R"({"data":{"fields":["other"],"values":[]}})",
        R"({"data":{"fields":["shared","evidence"],"values":[[{"$type":"Integer","_value":"1"}]]}})",
    };
    for (const auto& body : bodies) {
        FakeLookupApi api({"shared", "evidence"});
        LookupAnswer a;
        a.raw = body;
        api.answer("3", a);
        Rig rig(api, {{"max_retries", "0"}});
        rig.op->open();
        const auto [c, m] = failure([&] {
            rig.process(data({probe(1, 3)}));
            rig.op->flush(rig.out);
        });
        EXPECT_FALSE(c.empty()) << body;
        EXPECT_TRUE(rig.rows_out().empty()) << body;
    }
    // A reply cut short is lost, not empty: retried, then a failure.
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer cut = one(1, "x");
    cut.kind = LookupAnswer::Kind::CutShort;
    api.answer("3", cut);
    Rig rig(api, {{"max_retries", "1"}});
    rig.op->open();
    const auto [c, m] = failure([&] {
        rig.process(data({probe(1, 3)}));
        rig.op->flush(rig.out);
    });
    EXPECT_EQ(c, code::kRetriesExhausted) << m;
    EXPECT_EQ(api.reads().size(), 2U);
    EXPECT_TRUE(rig.rows_out().empty());
}

TEST(Neo4jLookup, ARetryableErrorIsRetriedAndThenAnswered) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer transient;
    transient.error_code = "Neo.TransientError.General.DatabaseUnavailable";
    LookupAnswer throttled;
    throttled.status = 429;
    throttled.raw = "{}";
    api.next(transient);
    api.next(throttled);
    api.answer("4", one(2, "acct-09"));
    Rig rig(api);
    rig.op->open();
    rig.process(data({probe(1, 4)}));
    rig.op->flush(rig.out);
    ASSERT_EQ(rig.rows_out().size(), 1U);
    EXPECT_EQ(rig.rows_out()[0].values.at("d_shared").as_int(), 2);
    EXPECT_EQ(api.reads().size(), 3U);
    EXPECT_EQ(rig.counter("clink_neo4j_lookup_retries_total"), 2U);
    EXPECT_EQ(rig.op->pool()->counts().retries, 2U);
}

TEST(Neo4jLookup, RetriesStopAtTheCountOrTheWindowWhicheverComesFirst) {
    {
        FakeLookupApi api({"shared", "evidence"});
        LookupAnswer down;
        down.status = 503;
        down.raw = "{}";
        api.answer("4", down);
        Rig rig(api, {{"max_retries", "2"}});
        rig.op->open();
        const auto [c, m] = failure([&] {
            rig.process(data({probe(1, 4)}));
            rig.op->flush(rig.out);
        });
        EXPECT_EQ(c, code::kRetriesExhausted);
        EXPECT_NE(m.find("after 3 attempt(s), max_retries=2"), std::string::npos) << m;
        EXPECT_EQ(api.reads().size(), 3U);
    }
    {
        FakeLookupApi api({"shared", "evidence"});
        LookupAnswer down;
        down.status = 503;
        down.raw = "{}";
        api.answer("4", down);
        Rig rig(api, {{"max_retries", "100"}, {"retry_window_ms", "400"}});
        rig.op->open();
        const auto start = Clock::now();
        const auto [c, m] = failure([&] {
            rig.process(data({probe(1, 4)}));
            rig.op->flush(rig.out);
        });
        EXPECT_EQ(c, code::kRetriesExhausted);
        EXPECT_NE(m.find("within retry_window_ms=400"), std::string::npos) << m;
        EXPECT_LT(Clock::now() - start, 2s);
    }
}

TEST(Neo4jLookup, AStalledReadEndsAtTheRequestDeadline) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer stall;
    stall.kind = LookupAnswer::Kind::Stall;
    api.answer("4", stall);
    Rig rig(api, {{"request_timeout_ms", "300"}, {"max_retries", "0"}});
    rig.op->open();
    const auto start = Clock::now();
    const auto [c, m] = failure([&] {
        rig.process(data({probe(1, 4)}));
        rig.op->flush(rig.out);
    });
    const auto took = Clock::now() - start;
    EXPECT_EQ(c, code::kRetriesExhausted);
    EXPECT_NE(m.find("passed request_timeout_ms=300"), std::string::npos) << m;
    EXPECT_GE(took, 280ms);
    EXPECT_LT(took, 1500ms);
}

TEST(Neo4jLookup, RefusalsAreNotRetriedAndTheServerTimeoutIsSentAndNamed) {
    {
        FakeLookupApi api({"shared", "evidence"});
        LookupAnswer refused;
        refused.status = 400;
        refused.error_code = "Neo.ClientError.Statement.ArithmeticError";
        api.answer("4", refused);
        Rig rig(api);
        rig.op->open();
        const auto [c, m] = failure([&] {
            rig.process(data({probe(1, 4)}));
            rig.op->flush(rig.out);
        });
        EXPECT_EQ(c, code::kServerRefused) << m;
        EXPECT_EQ(api.reads().size(), 1U);
    }
    {
        FakeLookupApi api({"shared", "evidence"});
        LookupAnswer slow;
        slow.error_code = "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration";
        api.answer("4", slow);
        Rig rig(api, {{"server_timeout_s", "1"}});
        rig.op->open();
        const auto [c, m] = failure([&] {
            rig.process(data({probe(1, 4)}));
            rig.op->flush(rig.out);
        });
        EXPECT_EQ(c, code::kTimeout);
        EXPECT_NE(m.find("server_timeout_s=1"), std::string::npos) << m;
        ASSERT_EQ(api.reads().size(), 1U);
        EXPECT_EQ(api.reads()[0].max_execution_time, "1");
    }
}

TEST(Neo4jLookup, AReplyPastTheByteBoundFails) {
    FakeLookupApi api({"shared", "evidence"});
    api.answer("4", one(1, std::string(8000, 'x')));
    Rig rig(api, {{"max_response_bytes", "4096"}});
    rig.op->open();
    const auto [c, m] = failure([&] {
        rig.process(data({probe(1, 4)}));
        rig.op->flush(rig.out);
    });
    EXPECT_EQ(c, code::kResponseTooLarge);
    EXPECT_NE(m.find("max_response_bytes=4096"), std::string::npos) << m;
    EXPECT_EQ(api.reads().size(), 1U);
}

TEST(Neo4jLookup, ConcurrencyNeverPassesMaxConcurrentRequests) {
    FakeLookupApi api({"shared", "evidence"});
    for (int k = 1; k <= 24; ++k) {
        LookupAnswer a = one(k, "x");
        a.delay = 60ms;
        api.answer(std::to_string(k), a);
    }
    Rig rig(api, {{"max_concurrent_requests", "3"}, {"max_in_flight", "24"}});
    rig.op->open();
    std::vector<Row> rows;
    for (int k = 1; k <= 24; ++k) {
        rows.push_back(probe(k, k));
    }
    rig.process(data(std::move(rows)));
    rig.op->flush(rig.out);
    EXPECT_EQ(rig.rows_out().size(), 24U);
    EXPECT_EQ(api.max_concurrent(), 3);
}

TEST(Neo4jLookup, AFullOperatorHoldsItsInputUntilALookupFinishes) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer stall = one(1, "x");
    stall.kind = LookupAnswer::Kind::Stall;
    for (int k = 1; k <= 5; ++k) {
        api.answer(std::to_string(k), stall);
    }
    Rig rig(api, {{"max_concurrent_requests", "8"}, {"max_in_flight", "4"}});
    rig.op->open();
    std::atomic<bool> returned{false};
    std::thread feeder([&] {
        std::vector<Row> rows;
        for (int k = 1; k <= 5; ++k) {
            rows.push_back(probe(k, k));
        }
        rig.process(data(std::move(rows)));
        returned.store(true);
    });
    // Four lookups start; the fifth record waits for room.
    for (int i = 0; i < 200 && api.reads().size() < 4; ++i) {
        std::this_thread::sleep_for(5ms);
    }
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(api.reads().size(), 4U);
    EXPECT_FALSE(returned.load()) << "the operator took a fifth record past max_in_flight=4";
    EXPECT_EQ(rig.op->in_flight(), 4U);
    api.release();
    feeder.join();
    rig.op->flush(rig.out);
    EXPECT_EQ(rig.order(), (std::vector<std::string>{"row:1", "row:2", "row:3", "row:4", "row:5"}));
}

TEST(Neo4jLookup, RepliesOutOfOrderAreEmittedInOrderAndNoMarkerOvertakesThem) {
    FakeLookupApi api({"shared", "evidence"});
    // Key 1 answers last, key 6 first.
    for (int k = 1; k <= 6; ++k) {
        LookupAnswer a = one(k, "x");
        a.delay = std::chrono::milliseconds{(7 - k) * 40};
        api.answer(std::to_string(k), a);
    }
    Rig rig(api, {{"max_concurrent_requests", "6"}});
    rig.op->open();
    rig.process(data({probe(1, 1), probe(2, 2), probe(3, 3)}));
    rig.process(StreamElement<Row>::watermark(Watermark{EventTime{1000}}));
    rig.process(data({probe(4, 4), probe(5, 5), probe(6, 6)}));
    rig.process(StreamElement<Row>::barrier(CheckpointBarrier{CheckpointId{5}}));
    rig.process(data({probe(7, 6)}));
    rig.op->flush(rig.out);
    EXPECT_EQ(rig.order(),
              (std::vector<std::string>{"row:1",
                                        "row:2",
                                        "row:3",
                                        "wm:1000",
                                        "row:4",
                                        "row:5",
                                        "row:6",
                                        "barrier:5",
                                        "row:7"}));
    // The replies did come back out of order.
    const auto reads = api.reads();
    ASSERT_GE(reads.size(), 3U);
}

TEST(Neo4jLookup, FinishedLookupsAreEmittedWithoutWaitingForTheNextElement) {
    FakeLookupApi api({"shared", "evidence"});
    api.answer("1", one(1, "x"));
    Rig rig(api);
    rig.op->open();
    rig.process(data({probe(1, 1)}));
    // The runner wakes an operator with lookups in flight on its own.
    ASSERT_TRUE(rig.op->in_flight() == 0 || rig.op->next_timer_deadline_ms().has_value());
    for (int i = 0; i < 200 && rig.rows_out().empty(); ++i) {
        std::this_thread::sleep_for(5ms);
        rig.op->fire_due_timers(rig.out, 0);
    }
    EXPECT_EQ(rig.order(), (std::vector<std::string>{"row:1"}));
    EXPECT_FALSE(rig.op->next_timer_deadline_ms().has_value());
}

TEST(Neo4jLookup, ARestoredOperatorReadsTheGraphAsItIsNow) {
    FakeLookupApi api({"shared", "evidence"});
    api.answer("1", one(1, "before"));
    {
        // Lookups in flight when the task dies: nothing of them survives.
        LookupAnswer slow = one(1, "before");
        slow.delay = 200ms;
        api.answer("2", slow);
        Rig rig(api);
        rig.op->open();
        rig.process(data({probe(1, 1), probe(2, 2)}));
        rig.op.reset();  // no flush, no close: the operator is simply gone
    }
    // The graph changes; the restored operator replays the same input.
    api.answer("1", one(2, "after"));
    api.answer("2", one(2, "after"));
    Rig rig(api);
    rig.op->open();
    rig.process(data({probe(1, 1), probe(2, 2)}));
    rig.op->flush(rig.out);
    const auto rows = rig.rows_out();
    ASSERT_EQ(rows.size(), 2U);
    for (const auto& r : rows) {
        EXPECT_EQ(r.values.at("d_evidence").as_string(), "after");
    }
}

TEST(Neo4jLookup, CancellingEndsQueuedAndRunningLookupsWithinTheBound) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer stall = one(1, "x");
    stall.kind = LookupAnswer::Kind::Stall;
    for (int k = 1; k <= 8; ++k) {
        api.answer(std::to_string(k), stall);
    }
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    auto opts = parse_lookup_options(params(api, {{"max_concurrent_requests", "2"}}), 0, 1);
    LookupPool pool(opts, clink::CancelSignal{cancel, nullptr}, nullptr, 1);
    pool.open();
    std::vector<std::shared_ptr<LookupCall>> calls;
    for (int k = 1; k <= 8; ++k) {
        auto c = std::make_shared<LookupCall>();
        c->key = typed_int(k);
        calls.push_back(c);
        pool.submit(c);
    }
    for (int i = 0; i < 200 && api.active() < 2; ++i) {
        std::this_thread::sleep_for(5ms);
    }
    ASSERT_EQ(api.active(), 2) << "two requests should be in flight, six queued";
    const auto start = Clock::now();
    cancel->store(true);
    for (const auto& c : calls) {
        while (!c->done.load() && Clock::now() - start < 5s) {
            std::this_thread::sleep_for(2ms);
        }
    }
    const auto took = Clock::now() - start;
    EXPECT_LT(took, 500ms) << "outstanding lookups outlived the cancel";
    for (const auto& c : calls) {
        ASSERT_TRUE(c->done.load());
        ASSERT_TRUE(c->error);
        try {
            std::rethrow_exception(c->error);
        } catch (const Neo4jError& e) {
            EXPECT_EQ(e.code(), code::kCancelled);
        }
    }
    const auto stop_start = Clock::now();
    pool.stop();
    EXPECT_LT(Clock::now() - stop_start, 500ms);
}

TEST(Neo4jLookup, ClosingWithLookupsInFlightReturnsPromptly) {
    FakeLookupApi api({"shared", "evidence"});
    LookupAnswer stall = one(1, "x");
    stall.kind = LookupAnswer::Kind::Stall;
    for (int k = 1; k <= 6; ++k) {
        api.answer(std::to_string(k), stall);
    }
    Rig rig(api, {{"max_concurrent_requests", "2"}});
    rig.op->open();
    std::vector<Row> rows;
    for (int k = 1; k <= 6; ++k) {
        rows.push_back(probe(k, k));
    }
    rig.process(data(std::move(rows)));
    EXPECT_EQ(rig.op->in_flight(), 6U);
    const auto start = Clock::now();
    rig.op->close();
    EXPECT_LT(Clock::now() - start, 500ms);
    EXPECT_TRUE(rig.rows_out().empty());
}

TEST(Neo4jLookup, TheOpenCheckRefusesStatementsThatMayNotOnlyRead) {
    struct Case {
        const char* type;
        std::vector<std::string> ops;
    };
    const std::vector<Case> cases = {
        {"rw", {"ProduceResults", "Create"}},
        {"s", {"CreateIndex"}},
        {"r", {"ProduceResults", "ProcedureCall"}},
        {"r", {"ProduceResults", "LoadCSV"}},
        {"", {"ProduceResults", "SetProperty", "NodeUniqueIndexSeek"}},
    };
    for (const auto& c : cases) {
        FakeLookupApi api({"shared", "evidence"});
        api.explain(c.type, c.ops);
        Rig rig(api);
        const auto [code_, m] = failure([&] { rig.op->open(); });
        EXPECT_EQ(code_, code::kStatementRefused) << c.type;
        EXPECT_NE(m.find("the statement is refused"), std::string::npos) << m;
        EXPECT_TRUE(api.reads().empty()) << "a refused statement was run";
    }
    {
        FakeLookupApi api({"shared"});
        Rig rig(api);
        const auto [code_, m] = failure([&] { rig.op->open(); });
        EXPECT_EQ(code_, code::kStatementRefused);
        EXPECT_NE(m.find("result columns are (shared, evidence)"), std::string::npos) << m;
    }
    {
        // A server that refuses maxExecutionTime is named as the cause.
        FakeLookupApi api({"shared", "evidence"});
        api.explain_body(
            400,
            R"({"errors":[{"code":"Neo.ClientError.Request.Invalid","message":"Bad Request"}]})");
        Rig rig(api, {{"server_timeout_s", "1"}});
        const auto [code_, m] = failure([&] { rig.op->open(); });
        EXPECT_EQ(code_, code::kServerRefused);
        EXPECT_NE(m.find("before Neo4j 2026.04"), std::string::npos) << m;
    }
}

TEST(Neo4jLookup, RowsHeldThroughAnOutageAllLeaveInOrderOnceTheServerAnswers) {
    FakeLookupApi api({"shared", "evidence"});
    for (int k = 1; k <= 60; ++k) {
        api.answer(std::to_string(k), one(k, "ev-" + std::to_string(k)));
    }
    // The first 40 requests meet a server that is down; the rest an answer.
    for (int i = 0; i < 40; ++i) {
        LookupAnswer down;
        down.status = 503;
        down.raw = "{}";
        api.next(down);
    }
    Rig rig(api,
            {{"max_concurrent_requests", "2"},
             {"max_in_flight", "8"},
             {"max_retries", "60"},
             {"retry_window_ms", "60000"}});
    rig.op->open();
    std::vector<std::string> want;
    for (int k = 1; k <= 60; ++k) {
        rig.process(data({probe(k, k)}));
        want.push_back("row:" + std::to_string(k));
        if (k % 10 == 0) {
            rig.process(StreamElement<Row>::barrier(
                CheckpointBarrier{CheckpointId{static_cast<std::uint64_t>(k / 10)}}));
            want.push_back("barrier:" + std::to_string(k / 10));
        }
    }
    rig.op->flush(rig.out);
    EXPECT_EQ(rig.order(), want);
    for (const auto& r : rig.rows_out()) {
        const auto k = r.values.at("s_event_id").as_int();
        EXPECT_EQ(r.values.at("d_evidence").as_string(), "ev-" + std::to_string(k));
    }
    EXPECT_GE(rig.counter("clink_neo4j_lookup_retries_total"), 40U);
}

TEST(Neo4jLookup, ARetractionIsEnrichedAndStaysARetraction) {
    FakeLookupApi api({"shared", "evidence"});
    api.answer("7", one(3, "acct-02"));
    Rig rig(api);
    rig.op->open();
    Row retraction = probe(1, 7);
    clink::sql::set_row_kind(retraction, clink::sql::kRowKindDelete);
    Row update = probe(2, 7);
    clink::sql::set_row_kind(update, clink::sql::kRowKindUpdateAfter);
    rig.process(data({retraction, update, probe(3, 7)}));
    rig.op->flush(rig.out);
    const auto rows = rig.rows_out();
    ASSERT_EQ(rows.size(), 3U);
    EXPECT_EQ(clink::sql::row_kind_of(rows[0]), clink::sql::kRowKindDelete);
    EXPECT_EQ(clink::sql::row_kind_of(rows[1]), clink::sql::kRowKindUpdateAfter);
    EXPECT_EQ(clink::sql::row_kind_of(rows[2]), clink::sql::kRowKindInsert);
    for (const auto& r : rows) {
        EXPECT_EQ(r.values.at("d_shared").as_int(), 3);
    }
}
