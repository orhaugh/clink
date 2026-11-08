// The MySQL sinks' failure paths, driven through a scripted stand-in for the
// server (MysqlSinkServer) so each failure lands exactly where the case names:
// a record refused while it is read or rendered, a statement refused, a
// deadlock and a lock wait timeout tried again within their bound, a warning,
// a COMMIT refused or whose reply is lost, a cancel while a statement runs.
// Each is followed by another barrier, which a failed sink must refuse until it
// is opened again. The ordering and bounds the sinks keep are checked on the
// statements the stand-in records.

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

#include "clink/core/record.hpp"
#include "clink/mysql/mysql_json_upsert_sink.hpp"
#include "clink/mysql/mysql_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::mysql::ColumnInfo;
using clink::mysql::MysqlJsonUpsertSink;
using clink::mysql::MysqlJsonUpsertSinkOptions;
using clink::mysql::MysqlSink;
using clink::mysql::MysqlSinkOptions;
using clink::mysql::MysqlSinkServer;
using clink::mysql::ServerError;
using clink::mysql::ServerWarning;
using clink::mysql::TableSchema;
using clink::mysql::UniqueIndex;

namespace {

// What the stand-in server knows and does, shared by every connection a sink
// opens to it.
struct FakeDb {
    std::mutex mu;
    std::vector<std::string> log;  // every statement, in order
    // Answers a statement (nth in the log) with an error, or nothing.
    std::function<std::optional<ServerError>(const std::string& sql, std::size_t n)> on_exec;
    // The warnings a statement raises.
    std::function<std::vector<ServerWarning>(const std::string& sql)> warnings_for;
    // A statement that blocks until the connection is cancelled.
    std::function<bool(const std::string& sql)> blocks;
    TableSchema schema;
    std::uint64_t packet{64U << 20};

    std::vector<std::string> statements() {
        const std::lock_guard<std::mutex> lock(mu);
        return log;
    }
    // The statements after the session was set up, abbreviated to their verbs.
    std::vector<std::string> verbs() {
        std::vector<std::string> out;
        for (const auto& s : statements()) {
            if (s.rfind("SET SESSION", 0) == 0) {
                continue;
            }
            out.push_back(s.substr(0, s.find_first_of(" (")));
        }
        return out;
    }
};

class FakeServer final : public MysqlSinkServer {
public:
    explicit FakeServer(std::shared_ptr<FakeDb> db) : db_(std::move(db)) {}

    std::optional<ServerError> exec(std::string_view sql) override {
        std::string s{sql};
        std::size_t n = 0;
        {
            const std::lock_guard<std::mutex> lock(db_->mu);
            db_->log.push_back(s);
            n = db_->log.size() - 1;
        }
        if (db_->blocks && db_->blocks(s)) {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait_for(lock, std::chrono::seconds{10}, [this] { return cancelled_.load(); });
            return ServerError{2013, "HY000", "Lost connection to server during query"};
        }
        if (cancelled_.load()) {
            return ServerError{2013, "HY000", "Lost connection to server during query"};
        }
        if (db_->on_exec) {
            if (auto e = db_->on_exec(s, n)) {
                return e;
            }
        }
        if (db_->warnings_for) {
            pending_ = db_->warnings_for(s);
        }
        return std::nullopt;
    }
    std::vector<ServerWarning> warnings() override { return std::exchange(pending_, {}); }
    std::string escape(std::string_view v) override {
        std::string out;
        for (const char c : v) {
            if (c == '\'' || c == '\\') {
                out.push_back('\\');
            }
            out.push_back(c);
        }
        return out;
    }
    TableSchema describe(const std::string& /*table*/) override { return db_->schema; }
    std::uint64_t max_allowed_packet() override { return db_->packet; }
    void cancel() noexcept override {
        cancelled_.store(true);
        cv_.notify_all();
    }

private:
    std::shared_ptr<FakeDb> db_;
    std::vector<ServerWarning> pending_;
    std::atomic<bool> cancelled_{false};
    std::mutex m_;
    std::condition_variable cv_;
};

ColumnInfo col(std::string name, std::string type, std::string coll = {}) {
    ColumnInfo c;
    c.name = std::move(name);
    c.data_type = type;
    c.column_type = type;
    c.collation = std::move(coll);
    c.collation_no_pad = true;
    return c;
}

// t (id INT PRIMARY KEY, v VARCHAR) on InnoDB.
std::shared_ptr<FakeDb> fake_db() {
    auto db = std::make_shared<FakeDb>();
    db->schema.exists = true;
    db->schema.engine = "InnoDB";
    db->schema.transactional = true;
    db->schema.columns = {col("id", "int"), col("v", "varchar", "utf8mb4_0900_ai_ci")};
    UniqueIndex pk;
    pk.name = "PRIMARY";
    pk.primary = true;
    pk.columns = {"id"};
    db->schema.unique_indexes = {pk};
    return db;
}

MysqlSinkOptions append_opts(const std::shared_ptr<FakeDb>& db) {
    MysqlSinkOptions o;
    o.table = "t";
    o.columns = {"id", "v"};
    o.server_factory = [db](const clink::mysql::ConnectOptions&) {
        return std::make_unique<FakeServer>(db);
    };
    return o;
}

MysqlJsonUpsertSinkOptions upsert_opts(const std::shared_ptr<FakeDb>& db) {
    MysqlJsonUpsertSinkOptions o;
    o.table = "t";
    o.columns = {"id", "v"};
    o.key_columns = {"id"};
    o.server_factory = [db](const clink::mysql::ConnectOptions&) {
        return std::make_unique<FakeServer>(db);
    };
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
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::size_t count(const std::vector<std::string>& v, const std::string& x) {
    return static_cast<std::size_t>(std::count(v.begin(), v.end(), x));
}

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

// A sink attached to a runtime context whose cancel flag the case sets.
struct Attached {
    explicit Attached(clink::Sink<std::string>& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4242}, "mysql_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    clink::Sink<std::string>& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

void expect_refuses_everything(clink::Sink<std::string>& sink) {
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"({"id":9,"v":"z"})"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
}

}  // namespace

TEST(MysqlSinkFailures, TheSessionIsStrictUtcAndBounded) {
    auto db = fake_db();
    MysqlSink sink(append_opts(db));
    sink.open();
    const auto log = db->statements();
    ASSERT_GE(log.size(), 2U);
    EXPECT_NE(log[0].find("time_zone = '+00:00'"), std::string::npos) << log[0];
    EXPECT_NE(log[0].find("sql_mode = 'STRICT_ALL_TABLES"), std::string::npos) << log[0];
    EXPECT_EQ(log[0].find("NO_AUTO_VALUE_ON_ZERO"), std::string::npos) << log[0];
    EXPECT_NE(log[1].find("innodb_lock_wait_timeout = 30"), std::string::npos) << log[1];
    MysqlJsonUpsertSink upsert(upsert_opts(db));
    upsert.open();
    EXPECT_NE(db->statements()[2].find("NO_AUTO_VALUE_ON_ZERO"), std::string::npos);
}

TEST(MysqlSinkFailures, ARecordThatCannotBeReadFailsTheSinkUntilItIsOpenedAgain) {
    auto db = fake_db();
    MysqlSink sink(append_opts(db));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({R"({"id":1,"v":"secret" )"})); });
    EXPECT_NE(why.find("a record cannot be written"), std::string::npos) << why;
    EXPECT_EQ(why.find("secret"), std::string::npos) << why;
    expect_refuses_everything(sink);
    EXPECT_TRUE(db->verbs().empty());
    // Opened again, it writes.
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"START", "INSERT", "COMMIT"}));
}

TEST(MysqlSinkFailures, ARecordThatCannotBeRenderedFailsTheSink) {
    auto db = fake_db();
    db->schema.columns.push_back(col("at", "datetime"));
    db->schema.columns.back().datetime_precision = 0;
    auto o = append_opts(db);
    o.columns = {"id", "v", "at"};
    o.timestamp_columns = {{"at", clink::sqljson::TimeKind::Timestamp}};
    MysqlSink sink(std::move(o));
    sink.open();
    const auto why =
        thrown([&] { sink.on_data(batch({R"({"id":1,"v":"a","at":1792863688105})"})); });
    EXPECT_NE(why.find("would round away"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

// Appended, a retraction would add the row it removes.
TEST(MysqlSinkFailures, TheAppendSinkRefusesRetractionsAndUnknownMarkers) {
    for (const char* rec : {R"({"id":1,"v":"a","__row_kind":"delete"})",
                            R"({"id":1,"v":"a","__row_kind":"update_before"})",
                            R"({"id":1,"v":"a","__row_kind":"DELETE"})"}) {
        auto db = fake_db();
        MysqlSink sink(append_opts(db));
        sink.open();
        sink.on_data(batch({R"({"id":2,"v":"b","__row_kind":"insert"})",
                            R"({"id":3,"v":"c","__row_kind":"update_after"})"}));
        EXPECT_NE(thrown([&] { sink.on_data(batch({rec})); }).find("__row_kind"), std::string::npos)
            << rec;
        expect_refuses_everything(sink);
        EXPECT_EQ(count(db->verbs(), "INSERT"), 0U) << rec;
    }
}

TEST(MysqlSinkFailures, AnOversizedRecordIsRefusedBeforeItIsBuffered) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.max_record_bytes = 64;
    MysqlSink sink(std::move(o));
    sink.open();
    const auto why =
        thrown([&] { sink.on_data(batch({R"({"id":1,"v":")" + std::string(100, 'x') + "\"}"})); });
    EXPECT_NE(why.find("more than the 64"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, AFailedStatementRollsBackAndTheNextBarrierIsRefused) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql.find("'two'") != std::string::npos) {
            return ServerError{1406, "22001", "Data too long for column 'v' at row 1"};
        }
        return std::nullopt;
    };
    auto o = append_opts(db);
    o.max_statement_bytes = 1024;
    o.max_record_bytes = 700;
    MysqlSink sink(std::move(o));
    sink.open();
    // Rows 1 and 2 do not fit one statement, so row 2 starts the second, with 3.
    sink.on_data(batch({R"({"id":1,"v":")" + std::string(600, 'a') + "\"}",
                        R"({"id":2,"v":")" + std::string(600, 'b') + "\"}",
                        R"({"id":3,"v":"two"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("MySQL error 1406"), std::string::npos) << why;
    EXPECT_NE(why.find("Nothing of this flush was committed"), std::string::npos) << why;
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"START", "INSERT", "INSERT", "ROLLBACK"}));
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, ALostCommitReplyIsAnUnknownOutcome) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql == "COMMIT") {
            return ServerError{2013, "HY000", "Lost connection to server during query"};
        }
        return std::nullopt;
    };
    MysqlSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("may or may not have been committed"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, ACommitTheServerRefusesCommittedNothing) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql == "COMMIT") {
            return ServerError{1180, "HY000", "Got error 1 during COMMIT"};
        }
        return std::nullopt;
    };
    MysqlJsonUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("refused the COMMIT"), std::string::npos) << why;
    EXPECT_NE(why.find("nothing of it was committed"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, ADeadlockIsTriedAgainWithinItsBound) {
    auto db = fake_db();
    std::atomic<int> refused{0};
    db->on_exec = [&](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql.rfind("INSERT", 0) == 0 && refused.fetch_add(1) == 0) {
            return ServerError{1213, "40001", "Deadlock found when trying to get lock"};
        }
        return std::nullopt;
    };
    MysqlSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(
        db->verbs(),
        (std::vector<std::string>{"START", "INSERT", "ROLLBACK", "START", "INSERT", "COMMIT"}));

    auto none = fake_db();
    none->on_exec = [](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql.rfind("INSERT", 0) == 0) {
            return ServerError{1213, "40001", "Deadlock found when trying to get lock"};
        }
        return std::nullopt;
    };
    auto o = append_opts(none);
    o.max_retries = 0;
    MysqlSink once(std::move(o));
    once.open();
    once.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] { once.on_barrier(kBarrier); }).find("gave up after 0 retries"),
              std::string::npos);
}

// Without transactions a rollback undoes nothing, so a lock error is not tried
// again (the statements before it would be written twice) and the failure says
// what stays.
TEST(MysqlSinkFailures, AFlushToATableWithoutTransactionsIsNeverTriedAgain) {
    auto db = fake_db();
    db->schema.engine = "MyISAM";
    db->schema.transactional = false;
    std::atomic<int> inserts{0};
    db->on_exec = [&](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql.rfind("INSERT", 0) == 0 && inserts.fetch_add(1) == 1) {
            return ServerError{1205, "HY000", "Lock wait timeout exceeded"};
        }
        return std::nullopt;
    };
    auto o = append_opts(db);
    o.max_statement_bytes = 1024;
    o.max_record_bytes = 700;
    MysqlSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":")" + std::string(600, 'a') + R"("})",
                        R"({"id":2,"v":")" + std::string(600, 'b') + R"("})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("What the flush's statements wrote before it stays"), std::string::npos)
        << why;
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"START", "INSERT", "INSERT", "ROLLBACK"}));
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, LockWaitTimeoutsExhaustTheRetries) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql.rfind("INSERT", 0) == 0) {
            return ServerError{1205, "HY000", "Lock wait timeout exceeded; try restarting"};
        }
        return std::nullopt;
    };
    auto o = append_opts(db);
    o.max_retries = 2;
    MysqlSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("gave up after 2 retries"), std::string::npos) << why;
    EXPECT_NE(why.find("1205"), std::string::npos) << why;
    const auto verbs = db->verbs();
    EXPECT_EQ(count(verbs, "INSERT"), 3U);
    EXPECT_EQ(count(verbs, "ROLLBACK"), 3U);
    EXPECT_EQ(count(verbs, "COMMIT"), 0U);
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, AWarningFailsTheFlush) {
    auto db = fake_db();
    db->warnings_for = [](const std::string& sql) {
        return sql.rfind("INSERT", 0) == 0
                   ? std::vector<ServerWarning>{{"Note",
                                                 1265,
                                                 "Data truncated for column 'v' at row 1"}}
                   : std::vector<ServerWarning>{};
    };
    MysqlSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("raised 1 warning"), std::string::npos) << why;
    EXPECT_NE(why.find("1265"), std::string::npos) << why;
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"START", "INSERT", "ROLLBACK"}));
    expect_refuses_everything(sink);
}

TEST(MysqlSinkFailures, ACancelEndsTheStatementInFlight) {
    for (const char* blocked : {"INSERT", "COMMIT"}) {
        auto db = fake_db();
        db->blocks = [blocked](const std::string& sql) { return sql.rfind(blocked, 0) == 0; };
        MysqlSink sink(append_opts(db));
        Attached a(sink);
        sink.open();
        sink.on_data(batch({R"({"id":1,"v":"a"})"}));
        std::thread canceller([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            a.cancel->store(true);
        });
        const auto started = std::chrono::steady_clock::now();
        const auto why = thrown([&] { sink.on_barrier(kBarrier); });
        canceller.join();
        EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds{5}) << blocked;
        EXPECT_NE(why.find("cancelled"), std::string::npos) << blocked << ": " << why;
        if (std::string{blocked} == "COMMIT") {
            EXPECT_NE(why.find("may or may not have been committed"), std::string::npos) << why;
        } else {
            EXPECT_NE(why.find("Nothing of this flush was committed"), std::string::npos) << why;
        }
        expect_refuses_everything(sink);
    }
}

TEST(MysqlSinkFailures, CloseNeverThrowsAndRollsBackATransactionLeftOpen) {
    auto db = fake_db();
    db->on_exec = [](const std::string& sql, std::size_t) -> std::optional<ServerError> {
        if (sql.rfind("INSERT", 0) == 0) {
            throw std::runtime_error("the stand-in fails unexpectedly");
        }
        return std::nullopt;
    };
    MysqlSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_FALSE(thrown([&] { sink.on_barrier(kBarrier); }).empty());
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
    EXPECT_EQ(db->verbs().back(), "ROLLBACK");
}

TEST(MysqlSinkFailures, CloseDiscardsWhatIsBuffered) {
    auto db = fake_db();
    MysqlSink sink(append_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    sink.close();
    EXPECT_TRUE(db->verbs().empty()) << "close() wrote what a checkpoint did not cover";
}

TEST(MysqlSinkFailures, StatementsSplitWithinOneTransactionAndThePacketLimit) {
    auto db = fake_db();
    db->packet = 2048;  // 1024 after the slack the sink keeps
    auto o = append_opts(db);
    o.max_record_bytes = 512;
    MysqlSink sink(std::move(o));
    sink.open();
    Batch<std::string> b;
    for (int i = 0; i < 10; ++i) {
        b.emplace(R"({"id":)" + std::to_string(i) + R"(,"v":")" + std::string(300, 'x') + "\"}");
    }
    sink.on_data(b);
    sink.on_barrier(kBarrier);
    const auto verbs = db->verbs();
    EXPECT_EQ(verbs.front(), "START");
    EXPECT_EQ(verbs.back(), "COMMIT");
    EXPECT_EQ(count(verbs, "START"), 1U);
    EXPECT_GE(count(verbs, "INSERT"), 4U);
    for (const auto& s : db->statements()) {
        EXPECT_LE(s.size(), 1024U);
    }
}

TEST(MysqlSinkFailures, TheByteBoundFlushesInsideABatch) {
    auto db = fake_db();
    auto o = append_opts(db);
    o.max_bytes = 100;
    MysqlSink sink(std::move(o));
    sink.open();
    Batch<std::string> b;
    for (int i = 0; i < 10; ++i) {
        b.emplace(R"({"id":)" + std::to_string(i) + R"(,"v":")" + std::string(30, 'y') + "\"}");
    }
    sink.on_data(b);
    // Each row is about 38 bytes as SQL: a flush every third row, not one at
    // the end of the batch.
    EXPECT_GE(count(db->verbs(), "COMMIT"), 3U);
}

TEST(MysqlSinkFailures, OpenRefusesATableTheModeCannotHonour) {
    auto db = fake_db();
    UniqueIndex uv;
    uv.name = "uv";
    uv.columns = {"v"};
    db->schema.unique_indexes.push_back(uv);
    auto o = append_opts(db);
    o.upsert = true;
    MysqlSink sink(std::move(o));
    EXPECT_NE(thrown([&] { sink.open(); }).find("2 unique indexes"), std::string::npos);
    MysqlJsonUpsertSink upsert(upsert_opts(db));
    EXPECT_NE(thrown([&] { upsert.open(); }).find("also has unique index uv"), std::string::npos);
}

// ---- the changelog sink ---------------------------------------------------------------

TEST(MysqlSinkFailures, ChangesApplyInTheOrderEachKeyWasLastChanged) {
    auto db = fake_db();
    db->schema.columns[0] = col("id", "varchar", "utf8mb4_0900_ai_ci");
    MysqlJsonUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":"abc","v":"x","__row_kind":"delete"})",
                        R"({"id":"ABC","v":"y","__row_kind":"insert"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"START", "DELETE", "INSERT", "COMMIT"}));
}

TEST(MysqlSinkFailures, AKeysLaterChangeReplacesItsEarlierOne) {
    auto db = fake_db();
    MysqlJsonUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})",
                        R"({"id":1,"v":"b"})",
                        R"({"id":2,"v":"c","__row_kind":"delete"})",
                        R"({"id":2,"v":"d","__row_kind":"update_after"})"}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->verbs(), (std::vector<std::string>{"START", "INSERT", "COMMIT"}));
    const auto log = db->statements();
    const auto insert = std::find_if(
        log.begin(), log.end(), [](const std::string& s) { return s.rfind("INSERT", 0) == 0; });
    ASSERT_NE(insert, log.end());
    EXPECT_NE(insert->find("(1,'b'),(2,'d')"), std::string::npos) << *insert;
    EXPECT_EQ(insert->find("'a'"), std::string::npos) << *insert;
}

TEST(MysqlSinkFailures, UnknownMarkersAndNullKeysFailTheChangelogSink) {
    for (const char* record : {R"({"id":1,"v":"a","__row_kind":"remove"})",
                               R"({"id":1,"v":"a","__row_kind":0})",
                               R"({"id":null,"v":"a"})",
                               R"({"v":"a","__row_kind":"delete"})"}) {
        auto db = fake_db();
        MysqlJsonUpsertSink sink(upsert_opts(db));
        sink.open();
        const auto why = thrown([&] { sink.on_data(batch({record})); });
        EXPECT_NE(why.find("cannot be applied"), std::string::npos) << record << ": " << why;
        expect_refuses_everything(sink);
    }
}

TEST(MysqlSinkFailures, TheKeyCountFlushesPerRecord) {
    auto db = fake_db();
    auto o = upsert_opts(db);
    o.batch_records = 2;
    MysqlJsonUpsertSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})",
                        R"({"id":2,"v":"a"})",
                        R"({"id":3,"v":"a"})",
                        R"({"id":4,"v":"a"})",
                        R"({"id":5,"v":"a"})"}));
    EXPECT_EQ(count(db->verbs(), "COMMIT"), 2U);
    sink.on_barrier(kBarrier);
    EXPECT_EQ(count(db->verbs(), "COMMIT"), 3U);
}

TEST(MysqlSinkFailures, TheChangelogSinkRefusesIdentitiesItCannotHonour) {
    auto db = fake_db();
    db->schema.columns[0] = col("id", "varchar", "utf8mb4_0900_ai_ci");
    auto parallel = upsert_opts(db);
    parallel.parallelism = 2;
    MysqlJsonUpsertSink two(std::move(parallel));
    EXPECT_NE(thrown([&] { two.open(); }).find("utf8mb4_0900_ai_ci"), std::string::npos);
    auto partial = upsert_opts(fake_db());
    partial.columns = {"id", "v"};
    auto db2 = fake_db();
    db2->schema.columns.push_back(col("w", "int"));
    partial = upsert_opts(db2);
    partial.columns = {"id", "v", "w"};
    partial.update_columns = {"v"};
    MysqlJsonUpsertSink part(std::move(partial));
    EXPECT_NE(thrown([&] { part.open(); }).find("leaves out column 'w'"), std::string::npos);
    auto db3 = fake_db();
    db3->schema.engine = "MyISAM";
    db3->schema.transactional = false;
    MysqlJsonUpsertSink myisam(upsert_opts(db3));
    EXPECT_NE(thrown([&] { myisam.open(); }).find("no transactions"), std::string::npos);
}

// With two subtasks a row's changes meet in one only if its key is written one
// way: the INT key 1 as a number, never as "1".
TEST(MysqlSinkFailures, WithSubtasksAKeyMustBeWrittenAsItsColumnCompares) {
    auto db = fake_db();
    auto o = upsert_opts(db);
    o.parallelism = 2;
    MysqlJsonUpsertSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NE(thrown([&] {
                  sink.on_data(batch({R"({"id":"1","v":"b"})"}));
              }).find("is not a JSON number"),
              std::string::npos);
    expect_refuses_everything(sink);
    EXPECT_EQ(count(db->verbs(), "INSERT"), 0U);
    // One subtask applies both spellings in order, as the table compares them.
    auto single = fake_db();
    MysqlJsonUpsertSink one(upsert_opts(single));
    one.open();
    one.on_data(batch({R"({"id":1,"v":"a"})", R"({"id":"1","v":"b"})"}));
    EXPECT_NO_THROW(one.on_barrier(kBarrier));
}
