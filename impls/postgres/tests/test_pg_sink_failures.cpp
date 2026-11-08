// The PostgreSQL sinks' failure paths against a scripted server: every failure
// followed by another record, barrier and flush; reopening; the rollback of a
// failed flush; a COMMIT refused, answered with ROLLBACK, or whose reply is
// lost; deadlocks and lock waits within their bound; two rows of one table row
// in one statement; a cancel during a statement and during COMMIT; close();
// the statement and byte bounds; the table checks at open; and the changelog
// sink's order, netting and markers.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_json_sink.hpp"
#include "clink/connectors/postgres_json_upsert_sink.hpp"
#include "clink/connectors/postgres_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::PostgresJsonSink;
using clink::PostgresJsonSinkOptions;
using clink::PostgresJsonUpsertSink;
using clink::PostgresJsonUpsertSinkOptions;
using clink::PostgresSink;
using clink::postgres::PgColumn;
using clink::postgres::PgConnectSettings;
using clink::postgres::PgError;
using clink::postgres::PgOutcome;
using clink::postgres::PgSinkServer;
using clink::postgres::PgTable;
using clink::postgres::PgUniqueIndex;

namespace {

// What the stand-in server knows and does, shared by every connection a sink
// opens to it.
struct FakePgDb {
    std::mutex mu;
    std::vector<std::string> log;  // every statement, in order
    // Answers a statement (nth in the log) with an error, or nothing.
    std::function<std::optional<PgError>(const std::string& sql, std::size_t n)> on_exec;
    // A statement that blocks until the connection is cancelled.
    std::function<bool(const std::string& sql)> blocks;
    std::string commit_tag{"COMMIT"};
    PgTable table;

    std::vector<std::string> statements() {
        const std::lock_guard<std::mutex> lock(mu);
        return log;
    }
    // The statements after the session was set up, abbreviated to their verbs.
    std::vector<std::string> verbs() {
        std::vector<std::string> out;
        for (const auto& s : statements()) {
            if (s.rfind("SET ", 0) == 0) {
                continue;
            }
            out.push_back(s.substr(0, s.find_first_of(" (")));
        }
        return out;
    }
};

class FakePgServer final : public PgSinkServer {
public:
    explicit FakePgServer(std::shared_ptr<FakePgDb> db) : db_(std::move(db)) {}

    PgOutcome exec(std::string_view sql) override { return run(std::string{sql}); }
    PgOutcome exec_params(std::string_view sql,
                          const std::vector<std::optional<std::string>>& params) override {
        return run(std::string{sql} + " [" + std::to_string(params.size()) + " params]");
    }
    std::string escape(std::string_view v) override {
        std::string out;
        for (const char c : v) {
            if (c == '\'') {
                out.push_back('\'');
            }
            out.push_back(c);
        }
        return out;
    }
    PgTable describe(const std::string& /*table*/) override { return db_->table; }
    void cancel() noexcept override {
        cancelled_.store(true);
        cv_.notify_all();
    }

private:
    PgOutcome run(const std::string& s) {
        std::size_t n = 0;
        {
            const std::lock_guard<std::mutex> lock(db_->mu);
            db_->log.push_back(s);
            n = db_->log.size() - 1;
        }
        PgOutcome o;
        if (db_->blocks && db_->blocks(s)) {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait_for(lock, std::chrono::seconds{10}, [this] { return cancelled_.load(); });
            o.error = PgError{"57014", "canceling statement due to user request", false};
            return o;
        }
        if (db_->on_exec) {
            if (auto e = db_->on_exec(s, n)) {
                o.error = std::move(e);
                return o;
            }
        }
        o.tag = s == "COMMIT" ? db_->commit_tag : "OK";
        return o;
    }

    std::shared_ptr<FakePgDb> db_;
    std::atomic<bool> cancelled_{false};
    std::mutex m_;
    std::condition_variable cv_;
};

PgColumn col(std::string name, std::string type_name) {
    PgColumn c;
    c.name = std::move(name);
    c.type = type_name;
    c.type_name = std::move(type_name);
    return c;
}

// t (id int4 PRIMARY KEY, v text).
std::shared_ptr<FakePgDb> fake_db() {
    auto db = std::make_shared<FakePgDb>();
    db->table.exists = true;
    db->table.kind = 'r';
    db->table.columns = {col("id", "int4"), col("v", "text")};
    PgUniqueIndex pk;
    pk.name = "t_pkey";
    pk.primary = true;
    pk.columns = {"id"};
    db->table.unique_indexes = {pk};
    return db;
}

clink::postgres::PgSinkServerFactory factory_for(const std::shared_ptr<FakePgDb>& db) {
    return [db](const std::string&, const PgConnectSettings&, const std::string&) {
        return std::make_unique<FakePgServer>(db);
    };
}

PostgresJsonSinkOptions append_opts(const std::shared_ptr<FakePgDb>& db) {
    PostgresJsonSinkOptions o;
    o.conninfo = "host=nowhere";
    o.table = "t";
    o.columns = {"id", "v"};
    o.server_factory = factory_for(db);
    return o;
}

PostgresJsonUpsertSinkOptions upsert_opts(const std::shared_ptr<FakePgDb>& db) {
    PostgresJsonUpsertSinkOptions o;
    o.conninfo = "host=nowhere";
    o.table = "t";
    o.columns = {"id", "v"};
    o.key_columns = {"id"};
    o.server_factory = factory_for(db);
    return o;
}

Batch<std::string> batch(std::initializer_list<std::string> records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(std::string{r});
    }
    return b;
}

template <typename F>
std::string thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::size_t count(const std::vector<std::string>& v, const std::string& x) {
    return static_cast<std::size_t>(std::count(v.begin(), v.end(), x));
}

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

void expect_refuses_everything(clink::Sink<std::string>& sink) {
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"({"id":9,"v":"z"})"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
}

// A sink attached to a runtime context whose cancel flag the case sets.
struct PgAttached {
    template <typename S>
    explicit PgAttached(S& s) : detach([&s] { s.attach_runtime(nullptr); }) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4343}, "postgres_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~PgAttached() { detach(); }
    PgAttached(const PgAttached&) = delete;
    PgAttached& operator=(const PgAttached&) = delete;
    std::function<void()> detach;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

TEST(PgSinkFailures, TheSessionIsUtcStrictAndBounded) {
    auto db = fake_db();
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    const auto s = db->statements();
    ASSERT_GE(s.size(), 5U);
    EXPECT_EQ(s[0], "SET TIME ZONE 'UTC'");
    EXPECT_EQ(s[1], "SET client_encoding = 'UTF8'");
    EXPECT_EQ(s[2], "SET standard_conforming_strings = on");
    EXPECT_EQ(s[3], "SET lock_timeout = '30s'");
    EXPECT_EQ(s[4], "SET statement_timeout = '90s'");

    auto refused = fake_db();
    refused->on_exec = [](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("SET lock_timeout", 0) == 0) {
            return PgError{"42501", "permission denied", false};
        }
        return std::nullopt;
    };
    PostgresJsonSink cannot(append_opts(refused));
    EXPECT_NE(thrown([&] { cannot.open(); }).find("setting the session up failed"),
              std::string::npos);
}

TEST(PgSinkFailures, ARecordThatCannotBeReadFailsTheSinkUntilItIsOpenedAgain) {
    auto db = fake_db();
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({R"({"id":1,"v":"secret"} trailing)"})); });
    EXPECT_NE(why.find("cannot be written"), std::string::npos) << why;
    EXPECT_EQ(why.find("secret"), std::string::npos) << why;
    expect_refuses_everything(sink);
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(count(db->verbs(), "COMMIT"), 1U);
}

TEST(PgSinkFailures, ARecordThatCannotBeRenderedFailsTheSink) {
    auto db = fake_db();
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({R"({"id":1.5,"v":"a"})"})); });
    EXPECT_NE(why.find("would round"), std::string::npos) << why;
    expect_refuses_everything(sink);
    EXPECT_EQ(count(db->verbs(), "INSERT"), 0U);
}

// Appended, a retraction would add the row it removes.
TEST(PgSinkFailures, TheAppendSinkRefusesRetractionsAndUnknownMarkers) {
    for (const char* rec : {R"({"id":1,"v":"a","__row_kind":"delete"})",
                            R"({"id":1,"v":"a","__row_kind":"update_before"})",
                            R"({"id":1,"v":"a","__row_kind":"DELETE"})"}) {
        auto db = fake_db();
        PostgresJsonSink sink(append_opts(db));
        sink.open();
        sink.on_data(batch({R"({"id":2,"v":"b","__row_kind":"insert"})"}));
        EXPECT_NE(thrown([&] { sink.on_data(batch({rec})); }).find("__row_kind"), std::string::npos)
            << rec;
        expect_refuses_everything(sink);
    }
}

// A NULL key never conflicts, so a replay would insert the row again.
TEST(PgSinkFailures, AKeyedSinkRefusesAMissingOrNullKey) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.on_conflict = "update";
    o.conflict_columns = {"id"};
    PostgresJsonSink sink(std::move(o));
    sink.open();
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"({"id":null,"v":"a"})"})); }).find("names no row"),
              std::string::npos);
}

TEST(PgSinkFailures, AnOversizedRecordIsRefusedBeforeItIsBuffered) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.max_record_bytes = 64;
    PostgresJsonSink sink(std::move(o));
    sink.open();
    EXPECT_NE(thrown([&] {
                  sink.on_data(batch({R"({"id":1,"v":")" + std::string(100, 'x') + "\"}"}));
              }).find("more than the 64"),
              std::string::npos);
    expect_refuses_everything(sink);
}

TEST(PgSinkFailures, AFailedStatementRollsBackAndTheNextBarrierIsRefused) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("INSERT", 0) == 0) {
            return PgError{"22P02", R"(invalid input syntax for type integer: "4111-1111")", false};
        }
        return std::nullopt;
    };
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("PostgreSQL error 22P02"), std::string::npos) << why;
    EXPECT_NE(why.find("Nothing of this flush was committed"), std::string::npos) << why;
    EXPECT_EQ(why.find("4111"), std::string::npos) << why;
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"BEGIN", "INSERT", "ROLLBACK"}));
    expect_refuses_everything(sink);
}

TEST(PgSinkFailures, ALostCommitReplyIsAnUnknownOutcome) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql == "COMMIT") {
            return PgError{"", "server closed the connection unexpectedly", true};
        }
        return std::nullopt;
    };
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("may or may not have been committed"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(PgSinkFailures, ACommitTheServerRefusesOrRollsBackCommittedNothing) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql == "COMMIT") {
            return PgError{"40001", "could not serialize access", false};
        }
        return std::nullopt;
    };
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("nothing of it was committed"),
              std::string::npos);

    auto rolled = fake_db();
    rolled->commit_tag = "ROLLBACK";
    PostgresJsonSink other(append_opts(rolled));
    other.open();
    other.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] { other.on_barrier(kBarrier); }).find("answered the COMMIT"),
              std::string::npos);
}

TEST(PgSinkFailures, DeadlocksAndLockWaitsAreTriedAgainWithinTheirBound) {
    auto db = fake_db();
    std::atomic<int> refused{0};
    db->on_exec = [&](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("INSERT", 0) == 0 && refused.fetch_add(1) == 0) {
            return PgError{"40P01", "deadlock detected", false};
        }
        return std::nullopt;
    };
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(
        db->verbs(),
        (std::vector<std::string>{"BEGIN", "INSERT", "ROLLBACK", "BEGIN", "INSERT", "COMMIT"}));

    auto locked = fake_db();
    locked->on_exec = [](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("INSERT", 0) == 0) {
            return PgError{"55P03", "canceling statement due to lock timeout", false};
        }
        return std::nullopt;
    };
    auto o = append_opts(locked);
    o.max_retries = 1;
    PostgresJsonSink once(std::move(o));
    once.open();
    once.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] { once.on_barrier(kBarrier); }).find("gave up after 1 retries"),
              std::string::npos);
    expect_refuses_everything(once);
}

// Two stream keys PostgreSQL holds as one row ('1' and 1 in an integer column)
// in one upsert: the server refuses it (21000), and the flush is written again
// a row to a statement, in order, so the row ends as the later change left it.
TEST(PgSinkFailures, TwoKeysOfOneTableRowAreWrittenARowToAStatement) {
    auto db = fake_db();
    std::atomic<int> inserts{0};
    db->on_exec = [&](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("INSERT", 0) == 0 && inserts.fetch_add(1) == 0) {
            return PgError{
                "21000", "ON CONFLICT DO UPDATE command cannot affect row a second time", false};
        }
        return std::nullopt;
    };
    PostgresJsonUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"first"})", R"({"id":"1","v":"second"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(),
              (std::vector<std::string>{
                  "BEGIN", "INSERT", "ROLLBACK", "BEGIN", "INSERT", "INSERT", "COMMIT"}));
    const auto s = db->statements();
    std::vector<std::string> inserts_sent;
    for (const auto& x : s) {
        if (x.rfind("INSERT", 0) == 0) {
            inserts_sent.push_back(x);
        }
    }
    ASSERT_EQ(inserts_sent.size(), 3U);
    EXPECT_NE(inserts_sent[1].find("'first'"), std::string::npos);
    EXPECT_NE(inserts_sent[2].find("'second'"), std::string::npos);
}

TEST(PgSinkFailures, TheOrdinarySinkNeverPutsAKeyTwiceInAStatement) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.on_conflict = "update";
    o.conflict_columns = {"id"};
    PostgresJsonSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})", R"({"id":1,"v":"c"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"BEGIN", "INSERT", "INSERT", "COMMIT"}));
    std::vector<std::string> inserts;
    for (const auto& x : db->statements()) {
        if (x.rfind("INSERT", 0) == 0) {
            inserts.push_back(x);
        }
    }
    ASSERT_EQ(inserts.size(), 2U);
    EXPECT_NE(inserts[0].find("(1,'a'),(2,'b')"), std::string::npos) << inserts[0];
    EXPECT_NE(inserts[1].find("(1,'c')"), std::string::npos) << inserts[1];
    EXPECT_NE(inserts[1].find(R"(ON CONFLICT ("id") DO UPDATE SET "v" = EXCLUDED."v")"),
              std::string::npos)
        << inserts[1];
}

TEST(PgSinkFailures, ACancelEndsTheStatementInFlight) {
    auto db = fake_db();
    db->blocks = [](const std::string& sql) { return sql.rfind("INSERT", 0) == 0; };
    PostgresJsonSink sink(append_opts(db));
    PgAttached a(sink);
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        a.cancel->store(true);
    });
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    canceller.join();
    EXPECT_NE(why.find("the task was cancelled"), std::string::npos) << why;
    EXPECT_NO_THROW(sink.close());

    auto at_commit = fake_db();
    at_commit->blocks = [](const std::string& sql) { return sql == "COMMIT"; };
    PostgresJsonSink committing(append_opts(at_commit));
    PgAttached b(committing);
    committing.open();
    committing.on_data(batch({R"({"id":1,"v":"a"})"}));
    std::thread later([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        b.cancel->store(true);
    });
    const auto unknown = thrown([&] { committing.on_barrier(kBarrier); });
    later.join();
    EXPECT_NE(unknown.find("may or may not have been committed"), std::string::npos) << unknown;
}

TEST(PgSinkFailures, CloseNeverThrowsAndRollsBackATransactionLeftOpen) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("INSERT", 0) == 0) {
            throw std::runtime_error("the client library gave up");
        }
        return std::nullopt;
    };
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("the client library gave up"),
              std::string::npos);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"BEGIN", "INSERT", "ROLLBACK"}));
    EXPECT_NO_THROW(sink.close());
}

TEST(PgSinkFailures, CloseDiscardsWhatIsBuffered) {
    auto db = fake_db();
    PostgresJsonSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    sink.close();
    EXPECT_EQ(count(db->verbs(), "INSERT"), 0U);
}

TEST(PgSinkFailures, StatementsSplitWithinOneTransaction) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.max_statement_bytes = 1024;
    o.max_record_bytes = 700;
    PostgresJsonSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":")" + std::string(600, 'a') + R"("})",
                        R"({"id":2,"v":")" + std::string(600, 'b') + R"("})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"BEGIN", "INSERT", "INSERT", "COMMIT"}));
    for (const auto& x : db->statements()) {
        EXPECT_LE(x.size(), 1024U);
    }
}

TEST(PgSinkFailures, TheByteBoundFlushesInsideABatch) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.max_bytes = 1000;
    PostgresJsonSink sink(std::move(o));
    sink.open();
    Batch<std::string> b;
    for (int i = 0; i < 10; ++i) {
        b.emplace(R"({"id":)" + std::to_string(i) + R"(,"v":")" + std::string(300, 'x') + "\"}");
    }
    sink.on_data(b);
    // Three rows of about 310 bytes cross 1000: a flush per three or four rows.
    EXPECT_GE(count(db->verbs(), "COMMIT"), 2U);
}

TEST(PgSinkFailures, OpenRefusesATableTheModeCannotHonour) {
    auto keyless = fake_db();
    keyless->table.unique_indexes.clear();
    PostgresJsonUpsertSink upsert(upsert_opts(keyless));
    EXPECT_NE(thrown([&] { upsert.open(); }).find("no PRIMARY KEY or UNIQUE index"),
              std::string::npos);
    auto generated = fake_db();
    generated->table.columns[1].generated = true;
    PostgresJsonSink append(append_opts(generated));
    EXPECT_NE(thrown([&] { append.open(); }).find("generated column"), std::string::npos);
    auto absent = fake_db();
    absent->table = PgTable{};
    PostgresJsonSink none(append_opts(absent));
    EXPECT_NE(thrown([&] { none.open(); }).find("does not exist"), std::string::npos);
}

TEST(PgSinkFailures, ChangesApplyInTheOrderEachKeyWasLastChanged) {
    auto db = fake_db();
    db->table.columns[0] = col("id", "text");
    PostgresJsonUpsertSink sink(upsert_opts(db));
    sink.open();
    // abc deleted, then ABC upserted: two stream keys, applied in that order.
    sink.on_data(
        batch({R"({"id":"abc","v":null,"__row_kind":"delete"})", R"({"id":"ABC","v":"y"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"BEGIN", "DELETE", "INSERT", "COMMIT"}));
}

TEST(PgSinkFailures, AKeysLaterChangeReplacesItsEarlierOne) {
    auto db = fake_db();
    PostgresJsonUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})",
                        R"({"id":2,"v":"b"})",
                        R"({"id":1,"v":"c"})",
                        R"({"id":2,"v":null,"__row_kind":"update_before"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"BEGIN", "INSERT", "DELETE", "COMMIT"}));
    for (const auto& x : db->statements()) {
        if (x.rfind("INSERT", 0) == 0) {
            EXPECT_NE(x.find("(1,'c')"), std::string::npos) << x;
            EXPECT_EQ(x.find("'a'"), std::string::npos) << x;
        }
        if (x.rfind("DELETE", 0) == 0) {
            EXPECT_NE(x.find(R"(WHERE "id" IN (2))"), std::string::npos) << x;
        }
    }
}

TEST(PgSinkFailures, UnknownMarkersAndNullKeysFailTheChangelogSink) {
    for (const char* rec : {R"({"id":1,"v":"a","__row_kind":"upsert"})",
                            R"({"id":1,"v":"a","__row_kind":2})",
                            R"({"id":null,"v":"a"})"}) {
        auto db = fake_db();
        PostgresJsonUpsertSink sink(upsert_opts(db));
        sink.open();
        EXPECT_NE(thrown([&] { sink.on_data(batch({rec})); }).find("cannot be applied"),
                  std::string::npos)
            << rec;
        expect_refuses_everything(sink);
    }
}

TEST(PgSinkFailures, WithSubtasksAKeyMustBeWrittenAsItsColumnCompares) {
    auto db = fake_db();
    auto o = upsert_opts(db);
    o.parallelism = 2;
    PostgresJsonUpsertSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] {
                  sink.on_data(batch({R"({"id":"1","v":"b"})"}));
              }).find("is not a JSON number"),
              std::string::npos);
    expect_refuses_everything(sink);
}

TEST(PgSinkFailures, TheChangelogSinkRefusesIdentitiesItCannotHonour) {
    auto ci = fake_db();
    ci->table.columns[0] = col("id", "text");
    ci->table.columns[0].collation_deterministic = false;
    ci->table.columns[0].collation = "case_insensitive";
    auto parallel = upsert_opts(ci);
    parallel.parallelism = 2;
    PostgresJsonUpsertSink two(std::move(parallel));
    EXPECT_NE(thrown([&] { two.open(); }).find("case_insensitive"), std::string::npos);
    auto view = fake_db();
    view->table.kind = 'v';
    PostgresJsonUpsertSink on_view(upsert_opts(view));
    EXPECT_NE(thrown([&] { on_view.open(); }).find("a view"), std::string::npos);
}

TEST(PgSinkFailures, ThePostgresSinkStaysFailedAndDiscardsOnClose) {
    auto db = fake_db();
    std::atomic<int> runs{0};
    db->on_exec = [&](const std::string& sql, std::size_t) -> std::optional<PgError> {
        if (sql.rfind("INSERT", 0) == 0 && runs.fetch_add(1) == 0) {
            return PgError{
                "23505", "duplicate key value violates unique constraint \"t_pkey\"", false};
        }
        return std::nullopt;
    };
    PostgresSink::Options o;
    o.conninfo = "host=nowhere";
    o.sql = "INSERT INTO t (id, v) VALUES ($1, $2)";
    o.server_factory = factory_for(db);
    PostgresSink sink(std::move(o));
    sink.open();
    clink::Batch<std::vector<std::string>> rows;
    rows.emplace(std::vector<std::string>{"1", "a"});
    sink.on_data(rows);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("23505"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
    sink.open();
    sink.on_data(rows);
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs().back(), "COMMIT");
    EXPECT_NE(db->statements()[db->statements().size() - 2].find("[2 params]"), std::string::npos);
    // Buffered rows are discarded by close().
    sink.on_data(rows);
    sink.close();
    EXPECT_EQ(count(db->verbs(), "INSERT"), 2U);
}
