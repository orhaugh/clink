// The sink against real Relate stores. Relate runs in this process, so every
// case writes a store of its own under the temporary directory and reads it
// back through the C library: what the cases count is what the store holds,
// not what the sink says it wrote.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"
#include "relate_sink.hpp"
#include "store.hpp"
#include "store_reader.hpp"

using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::relate::parse_options;
using clink::relate::RelateError;
using clink::relate::RelateSink;
using clink::relate::Store;
using clink::relate::testing::count;
using clink::relate::testing::query;
using clink::relate::testing::TempDir;
using clink::sql::Row;
using namespace std::chrono_literals;
namespace code = clink::relate::code;

namespace {

constexpr char kMerge[] =
    "UNWIND $rows AS row MERGE (a:Account {id: row.id}) SET a.name = row.name";
constexpr char kAccountKey[] =
    "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE";
constexpr char kAccounts[] = "MATCH (a:Account) RETURN count(a)";

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
    explicit Rig(const std::string& store, std::map<std::string, std::string> extra = {}) {
        std::map<std::string, std::string> params = {
            {"path", store},
            {"statement", kMerge},
            {"schema", kAccountKey},
            {"sql_column_types", "id:BIGINT;name:VARCHAR"},
            {"flush_interval_ms", "0"},
        };
        for (auto& [k, v] : extra) {
            params[k] = v;
        }
        sink = std::make_unique<RelateSink>(parse_options(params, 0, 1));
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{42}, "relate_sink", nullptr, &metrics);
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
    std::unique_ptr<RelateSink> sink;
};

struct Refusal {
    std::string code;
    std::string message;
};

Refusal refusal(const std::function<void()>& f) {
    try {
        f();
    } catch (const RelateError& e) {
        return {e.code(), e.what()};
    }
    return {};
}

}  // namespace

TEST(RelateSink, OpenCreatesTheStoreAppliesTheSchemaAndWritesNothing) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store);
    rig.sink->open();
    EXPECT_TRUE(std::filesystem::exists(store));
    EXPECT_EQ(count(store, "MATCH (n) RETURN count(n)"), 0);
    EXPECT_EQ(count(store, "SHOW CONSTRAINTS YIELD name WHERE name = 'account_id' RETURN count(*)"),
              1);
}

TEST(RelateSink, ABatchIsCommittedWhenItIsFullAndTheRestAtTheEndOfInput) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store, {{"batch_size", "4"}});
    rig.sink->open();
    rig.sink->on_data(rows(0, 10));
    EXPECT_EQ(count(store, kAccounts), 8) << "two full batches are committed";
    EXPECT_EQ(rig.sink->buffered_rows(), 2U);
    rig.sink->flush();
    EXPECT_EQ(count(store, kAccounts), 10);
    EXPECT_EQ(rig.sink->buffered_rows(), 0U);
    EXPECT_EQ(rig.sink->counts().batches_committed, 3U);
    EXPECT_EQ(rig.sink->counts().rows_committed, 10U);
    EXPECT_EQ(rig.counter("clink_relate_batches_committed_total"), 3U);
    EXPECT_EQ(rig.counter("clink_relate_rows_committed_total"), 10U);
    EXPECT_EQ(rig.sink->counts().last_bookmark.rfind("relate:", 0), 0U)
        << rig.sink->counts().last_bookmark;
    EXPECT_EQ(query(store, "MATCH (a:Account {id: 7}) RETURN a.name"),
              (std::vector<std::vector<std::string>>{{"account-7"}}));
}

TEST(RelateSink, ABarrierReturnsOnlyOnceEveryRowBeforeItIsCommitted) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store);
    rig.sink->open();
    rig.sink->on_data(rows(0, 3));
    EXPECT_EQ(count(store, kAccounts), 0) << "rows wait for a full batch, a barrier or the end";
    rig.sink->on_barrier(clink::CheckpointBarrier{});
    EXPECT_EQ(count(store, kAccounts), 3);
    EXPECT_EQ(rig.sink->buffered_rows(), 0U);
    // A barrier with nothing buffered commits nothing.
    rig.sink->on_barrier(clink::CheckpointBarrier{});
    EXPECT_EQ(rig.sink->counts().batches_committed, 1U);
}

TEST(RelateSink, ABatchIsCommittedOnceItsBytesOrItsAgeReachTheLimit) {
    const TempDir dir;
    {
        const auto store = dir.path("bytes.rl");
        // A row weighs 16 + (2 + 24) + (4 + 16 + 9) = 71 bytes.
        Rig rig(store, {{"batch_bytes", "1024"}});
        rig.sink->open();
        rig.sink->on_data(rows(0, 20));
        EXPECT_EQ(count(store, kAccounts), 14) << "fourteen rows fit in 1024 bytes";
        EXPECT_EQ(rig.sink->buffered_rows(), 6U);
    }
    {
        const auto store = dir.path("age.rl");
        Rig rig(store, {{"flush_interval_ms", "30"}});
        rig.sink->open();
        rig.sink->on_data(rows(0, 2));
        EXPECT_EQ(count(store, kAccounts), 0);
        std::this_thread::sleep_for(60ms);
        rig.sink->on_data(rows(2, 3));
        EXPECT_EQ(count(store, kAccounts), 3) << "the batch is older than flush_interval_ms";
    }
}

TEST(RelateSink, ARowHeavierThanABatchIsRefused) {
    const TempDir dir;
    Rig rig(dir.path("case.rl"), {{"batch_bytes", "1024"}});
    rig.sink->open();
    JsonObject obj;
    obj["id"] = JsonValue{std::int64_t{1}};
    obj["name"] = JsonValue{std::string(2000, 'x')};
    Row heavy;
    heavy.values = clink::sql::row_columns_from_json(std::move(obj));
    clink::Batch<Row> b;
    b.emplace(std::move(heavy));
    const auto r = refusal([&] { rig.sink->on_data(b); });
    EXPECT_EQ(r.code, code::kRowTooLarge);
    EXPECT_NE(r.message.find("raise batch_bytes"), std::string::npos) << r.message;
}

TEST(RelateSink, AReplayedBatchLeavesTheGraphAsItWas) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store);
    rig.sink->open();
    rig.sink->on_data(rows(0, 5));
    rig.sink->flush();
    const auto first = rig.sink->counts().last_bookmark;
    rig.sink->on_data(rows(0, 5));
    rig.sink->on_data(rows(3, 8));
    rig.sink->flush();
    EXPECT_EQ(count(store, kAccounts), 8);
    EXPECT_EQ(count(store,
                    "MATCH (a:Account) WITH a.id AS id, count(*) AS n WHERE n > 1 "
                    "RETURN count(*)"),
              0);
    EXPECT_NE(rig.sink->counts().last_bookmark, first) << "the second batch wrote three accounts";
}

TEST(RelateSink, TheStatementIsCheckedWhenTheSinkOpens) {
    const TempDir dir;
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"UNWIND $rows AS row MERG (a:Account {id: row.id})", "SyntaxError"},
        {"UNWIND $rows AS row MERGE (a:Account {id: $other})", "ParameterMissing"},
        {"UNWIND $rows AS row MATCH (a:Account {id: row.id}) RETURN a.id", "it only reads"},
        {std::string(kMerge) + " RETURN a.id AS id", "it returns columns"},
        {"CREATE CONSTRAINT x IF NOT EXISTS FOR (n:X) REQUIRE n.id IS UNIQUE",
         "it changes the schema"},
        {"UNWIND $rows AS row CALL (row) { MERGE (a:Account {id: row.id}) } IN TRANSACTIONS "
         "OF 10 ROWS",
         "IN TRANSACTIONS"},
    };
    int n = 0;
    for (const auto& [statement, expect] : cases) {
        SCOPED_TRACE(statement);
        const auto store = dir.path("case" + std::to_string(n++) + ".rl");
        Rig rig(store, {{"statement", statement}});
        const auto r = refusal([&] { rig.sink->open(); });
        EXPECT_EQ(r.code, code::kStatementRefused);
        EXPECT_NE(r.message.find(expect), std::string::npos) << r.message;
        EXPECT_NE(r.message.find(store), std::string::npos) << "the refusal names the store";
        EXPECT_EQ(count(store, "MATCH (n) RETURN count(n)"), 0);
        EXPECT_EQ(count(store, "SHOW CONSTRAINTS YIELD name WHERE name = 'x' RETURN count(*)"), 0);
    }
}

TEST(RelateSink, CheckingTheStatementLeavesNothingBehind) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store,
            {{"statement",
              "MERGE (m:Meta {id: 1}) WITH m UNWIND $rows AS row "
              "MERGE (a:Account {id: row.id}) SET a.name = row.name"}});
    rig.sink->open();
    EXPECT_EQ(count(store, "MATCH (m:Meta) RETURN count(m)"), 0)
        << "the check ran the statement's first write and rolled it back";
}

TEST(RelateSink, TheSchemaOptionTakesSchemaStatementsOnly) {
    const TempDir dir;
    {
        const auto store = dir.path("data.rl");
        Rig rig(store, {{"schema", "CREATE (:Account {id: 1})"}});
        const auto r = refusal([&] { rig.sink->open(); });
        EXPECT_EQ(r.code, code::kSchemaRefused);
        EXPECT_NE(r.message.find("is not a schema statement"), std::string::npos) << r.message;
        EXPECT_EQ(count(store, kAccounts), 0) << "the statement was explained, not run";
    }
    {
        Rig rig(dir.path("syntax.rl"), {{"schema", "CREATE CONSTRAINT oops FOR"}});
        EXPECT_EQ(refusal([&] { rig.sink->open(); }).code, code::kSchemaRefused);
    }
    {
        // Without IF NOT EXISTS the second open fails, and the refusal says why.
        const auto store = dir.path("twice.rl");
        const std::string once =
            "CREATE CONSTRAINT account_id FOR (a:Account) REQUIRE a.id IS UNIQUE";
        {
            Rig rig(store, {{"schema", once}});
            rig.sink->open();
        }
        Rig rig(store, {{"schema", once}});
        const auto r = refusal([&] { rig.sink->open(); });
        EXPECT_EQ(r.code, code::kSchemaRefused);
        EXPECT_NE(r.message.find("IF NOT EXISTS"), std::string::npos) << r.message;
    }
    {
        // Two statements, applied in order, every time the sink opens.
        const auto store = dir.path("two.rl");
        const std::string both = std::string(kAccountKey) +
                                 "; CREATE INDEX account_name IF NOT EXISTS FOR (a:Account) ON "
                                 "(a.name);";
        for (int open = 0; open < 2; ++open) {
            Rig rig(store, {{"schema", both}});
            rig.sink->open();
        }
        EXPECT_EQ(count(store, "SHOW CONSTRAINTS YIELD name RETURN count(*)"), 1);
    }
}

TEST(RelateSink, ARefusedBatchIsRolledBackWholeAndItsRowsStayBuffered) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store, {{"statement", "UNWIND $rows AS row CREATE (:Account {id: row.id})"}});
    rig.sink->open();
    rig.sink->on_data(rows(0, 5));
    rig.sink->flush();
    ASSERT_EQ(count(store, kAccounts), 5);
    // Five new accounts and one that exists: the constraint refuses the batch.
    rig.sink->on_data(rows(5, 10));
    rig.sink->on_data(rows(3, 4));
    const auto r = refusal([&] { rig.sink->on_barrier(clink::CheckpointBarrier{}); });
    EXPECT_EQ(r.code, code::kBatchRefused);
    EXPECT_NE(r.message.find("ConstraintValidationFailed"), std::string::npos) << r.message;
    EXPECT_NE(r.message.find("batch 2 (6 rows"), std::string::npos) << r.message;
    EXPECT_EQ(count(store, kAccounts), 5) << "none of the refused batch is in the store";
    EXPECT_EQ(rig.sink->buffered_rows(), 6U);
    EXPECT_EQ(rig.sink->counts().attempts_failed, 1U);
    EXPECT_EQ(rig.sink->counts().retries, 0U) << "Relate said a retry cannot succeed";
    EXPECT_EQ(
        rig.metrics.counter("clink_relate_attempts_failed_total{op_id=\"42\",retryable=\"false\"}")
            .value(),
        1U);
}

TEST(RelateSink, SinksOnOneStoreShareItsHandleAndConvergeUnderConflict) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    constexpr std::int64_t kIds = 60;
    constexpr int kRounds = 20;
    constexpr int kMostSets = 25;
    Rig left(store, {{"batch_size", "10"}});
    Rig right(store, {{"batch_size", "10"}});
    left.sink->open();
    right.sink->open();
    {
        const auto hold = Store::open(store);
        EXPECT_EQ(hold->users(), 3U) << "two sinks and this case, on one handle";
    }
    // Both write the same accounts, over and over, at once: Relate lets one of
    // two transactions that wrote the same node commit and refuses the other as
    // a conflict a retry can win. A set of rounds has always produced several;
    // sets repeat until one has, so the case does not rest on that.
    const auto work = [&](Rig& rig) {
        for (int round = 0; round < kRounds; ++round) {
            rig.sink->on_data(rows(0, kIds));
            rig.sink->on_barrier(clink::CheckpointBarrier{});
        }
    };
    int sets = 0;
    while (sets < kMostSets && left.sink->counts().retries + right.sink->counts().retries == 0) {
        std::thread a(work, std::ref(left));
        std::thread b(work, std::ref(right));
        a.join();
        b.join();
        ++sets;
    }
    EXPECT_GT(left.sink->counts().retries + right.sink->counts().retries, 0U)
        << "no conflict in " << sets << " sets of rounds";
    EXPECT_EQ(count(store, kAccounts), kIds);
    EXPECT_EQ(count(store,
                    "MATCH (a:Account) WITH a.id AS id, count(*) AS n WHERE n > 1 "
                    "RETURN count(*)"),
              0);
    const auto written = static_cast<std::uint64_t>(kIds * kRounds * sets);
    EXPECT_EQ(left.sink->counts().rows_committed, written);
    EXPECT_EQ(right.sink->counts().rows_committed, written);
    // Every attempt that failed was a conflict, and every conflict was retried.
    EXPECT_EQ(left.sink->counts().retries, left.sink->counts().attempts_failed);
    EXPECT_EQ(right.sink->counts().retries, right.sink->counts().attempts_failed);
}

TEST(RelateSink, AStoreThatCannotBeOpenedIsRefusedWithWhatIsKnown) {
    const TempDir dir;
    {
        Rig rig(dir.path("no-such-directory/case.rl"));
        const auto r = refusal([&] { rig.sink->open(); });
        EXPECT_EQ(r.code, code::kStoreUnavailable);
        EXPECT_NE(r.message.find("is not a directory"), std::string::npos) << r.message;
    }
    {
        const auto not_a_store = dir.path("notes.rl");
        std::ofstream(not_a_store) << "this is not a Relate store\n";
        Rig rig(not_a_store);
        const auto r = refusal([&] { rig.sink->open(); });
        EXPECT_EQ(r.code, code::kStoreUnavailable);
        EXPECT_NE(r.message.find("RELATE_FFI_LOG=1"), std::string::npos) << r.message;
    }
}

TEST(RelateSink, ACancelledTaskCommitsNothingMore) {
    const TempDir dir;
    const auto store = dir.path("case.rl");
    Rig rig(store);
    rig.sink->open();
    rig.sink->on_data(rows(0, 3));
    rig.cancel->store(true);
    const auto r = refusal([&] { rig.sink->on_barrier(clink::CheckpointBarrier{}); });
    EXPECT_EQ(r.code, code::kCancelled);
    EXPECT_EQ(rig.sink->buffered_rows(), 3U);
    rig.sink->close_cancelled();
    EXPECT_EQ(count(store, kAccounts), 0);
}

TEST(RelateSink, RowsThatReachAClosedSinkAreRefused) {
    const TempDir dir;
    Rig rig(dir.path("case.rl"));
    rig.sink->open();
    rig.sink->close();
    rig.sink->on_data(rows(0, 1));
    EXPECT_EQ(refusal([&] { rig.sink->flush(); }).code, code::kEngineFailure);
}

TEST(RelateSink, AStoreWrittenAsADirectoryReadsTheSameAsOneWrittenAsAFile) {
    const TempDir dir;
    for (const auto& name : {std::string("case.rl"), std::string("case-directory")}) {
        SCOPED_TRACE(name);
        const auto store = dir.path(name);
        {
            Rig rig(store);
            rig.sink->open();
            rig.sink->on_data(rows(0, 25));
            rig.sink->flush();
        }
        // Every hold is released: the store is closed and opened again here.
        EXPECT_EQ(count(store, kAccounts), 25);
        EXPECT_EQ(std::filesystem::is_directory(store), name == "case-directory");
    }
}
