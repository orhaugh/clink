// The Redis sinks' failure paths against a scripted server: a refused command
// and a lost reply, each followed by another record, barrier and flush;
// reopening; a cancel during a flush and before one; a queueing failure;
// close(); the record, count, byte and linger bounds; the command timeout; and
// the keyed sink's netting, values, keys and refusals.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_sink_core.hpp"
#include "clink/redis/redis_upsert_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::redis::ConnectOptions;
using clink::redis::RedisReplyView;
using clink::redis::RedisSink;
using clink::redis::RedisSinkOptions;
using clink::redis::RedisSinkServer;
using clink::redis::RedisUpsertSink;
using clink::redis::RedisUpsertSinkOptions;

namespace {

using Command = std::vector<std::string>;

// What the stand-in server knows and does, shared by every connection a sink
// opens to it.
struct FakeRedis {
    std::mutex mu;
    std::vector<Command> sent;     // every command whose reply was read, in order
    std::vector<Command> applied;  // those the server applied
    // Answers the nth command sent (n counts every command); Ok unless set.
    std::function<RedisReplyView(const Command& c, std::size_t n)> on_command;
    // A command whose reply does not come until the connection is cancelled.
    std::function<bool(const Command& c)> blocks;
    bool append_fails{false};
    bool connect_fails{false};
    std::vector<ConnectOptions> connects;

    std::vector<Command> applied_commands() {
        const std::lock_guard<std::mutex> lock(mu);
        return applied;
    }
    std::size_t sent_count() {
        const std::lock_guard<std::mutex> lock(mu);
        return sent.size();
    }
};

class FakeRedisServer final : public RedisSinkServer {
public:
    explicit FakeRedisServer(std::shared_ptr<FakeRedis> db) : db_(std::move(db)) {}

    void append(const Command& args) override {
        if (db_->append_fails) {
            throw std::runtime_error("redis: append 'XADD' failed: Out of memory");
        }
        queued_.push_back(args);
    }

    RedisReplyView reply() override {
        if (lost_) {
            return RedisReplyView{RedisReplyView::Kind::Lost, "Connection reset by peer"};
        }
        if (queued_.empty()) {
            throw std::logic_error("a reply was read with no command queued");
        }
        const Command c = queued_.front();
        queued_.pop_front();
        std::size_t n = 0;
        {
            const std::lock_guard<std::mutex> lock(db_->mu);
            db_->sent.push_back(c);
            n = db_->sent.size() - 1;
        }
        if (db_->blocks && db_->blocks(c)) {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait_for(lock, std::chrono::seconds{10}, [this] { return cancelled_.load(); });
            lost_ = true;
            return RedisReplyView{RedisReplyView::Kind::Lost, "Connection reset by peer"};
        }
        RedisReplyView r;
        if (db_->on_command) {
            r = db_->on_command(c, n);
        }
        if (r.kind == RedisReplyView::Kind::Lost) {
            lost_ = true;
        } else if (r.kind == RedisReplyView::Kind::Ok) {
            const std::lock_guard<std::mutex> lock(db_->mu);
            db_->applied.push_back(c);
        }
        return r;
    }

    void cancel() noexcept override {
        cancelled_.store(true);
        cv_.notify_all();
    }

private:
    std::shared_ptr<FakeRedis> db_;
    std::deque<Command> queued_;
    bool lost_{false};
    std::atomic<bool> cancelled_{false};
    std::mutex m_;
    std::condition_variable cv_;
};

clink::redis::RedisSinkServerFactory factory_for(const std::shared_ptr<FakeRedis>& db) {
    return [db](const ConnectOptions& o) -> std::unique_ptr<RedisSinkServer> {
        {
            const std::lock_guard<std::mutex> lock(db->mu);
            db->connects.push_back(o);
        }
        if (db->connect_fails) {
            throw std::runtime_error("redis: connect to nowhere:6379 failed: Connection refused");
        }
        return std::make_unique<FakeRedisServer>(db);
    };
}

RedisSinkOptions stream_opts(const std::shared_ptr<FakeRedis>& db) {
    RedisSinkOptions o;
    o.stream = "s";
    o.server_factory = factory_for(db);
    return o;
}

RedisUpsertSinkOptions upsert_opts(const std::shared_ptr<FakeRedis>& db) {
    RedisUpsertSinkOptions o;
    o.key_columns = {"id"};
    o.key_prefix = "p:";
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

const CheckpointBarrier kBarrier{clink::CheckpointId{1}};

RedisReplyView refused(std::string text) {
    return RedisReplyView{RedisReplyView::Kind::Error, std::move(text)};
}

RedisReplyView lost() {
    return RedisReplyView{RedisReplyView::Kind::Lost,
                          "redis: get_reply failed: Resource temporarily unavailable"};
}

void expect_refuses_everything(clink::Sink<std::string>& sink) {
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"({"id":9,"v":"z"})"})); }).find("failed earlier"),
              std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("failed earlier"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
}

// A sink attached to a runtime context whose cancel flag the case sets.
struct RedisAttached {
    template <typename S>
    explicit RedisAttached(S& s) : detach([&s] { s.attach_runtime(nullptr); }) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{4344}, "redis_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~RedisAttached() { detach(); }
    RedisAttached(const RedisAttached&) = delete;
    RedisAttached& operator=(const RedisAttached&) = delete;
    std::function<void()> detach;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

}  // namespace

// ---- the Streams sink ------------------------------------------------------------------

TEST(RedisSinkFailures, AFlushPipelinesOneXaddPerRecordInOrder) {
    auto db = std::make_shared<FakeRedis>();
    auto o = stream_opts(db);
    o.maxlen = 100;
    o.approx_maxlen = false;
    o.field = "payload";
    RedisSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b", std::string{"c\0d", 3}}));
    sink.on_barrier(kBarrier);
    EXPECT_EQ(db->applied_commands(),
              (std::vector<Command>{
                  {"XADD", "s", "MAXLEN", "=", "100", "*", "payload", "a"},
                  {"XADD", "s", "MAXLEN", "=", "100", "*", "payload", "b"},
                  {"XADD", "s", "MAXLEN", "=", "100", "*", "payload", std::string{"c\0d", 3}}}));

    auto plain = std::make_shared<FakeRedis>();
    RedisSink untrimmed(stream_opts(plain));
    untrimmed.open();
    untrimmed.on_data(batch({"x"}));
    untrimmed.flush();
    EXPECT_EQ(plain->applied_commands(), (std::vector<Command>{{"XADD", "s", "*", "v", "x"}}));
}

TEST(RedisSinkFailures, ARefusedCommandFailsTheSinkUntilItIsOpenedAgain) {
    auto db = std::make_shared<FakeRedis>();
    db->on_command = [](const Command&, std::size_t n) {
        return n == 1 ? refused("WRONGTYPE Operation against a key holding the wrong kind of value")
                      : RedisReplyView{};
    };
    RedisSink sink(stream_opts(db));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("Redis refused 1 of 3 commands"), std::string::npos) << why;
    EXPECT_NE(why.find("WRONGTYPE"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 2 were applied"), std::string::npos) << why;
    EXPECT_EQ(db->applied_commands().size(), 2U);
    expect_refuses_everything(sink);

    db->on_command = nullptr;
    sink.open();
    sink.on_data(batch({"d"}));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    EXPECT_EQ(db->applied_commands().size(), 3U);
}

TEST(RedisSinkFailures, ALostReplyIsAnUnknownOutcome) {
    auto db = std::make_shared<FakeRedis>();
    db->on_command = [](const Command&, std::size_t n) {
        return n == 1 ? lost() : RedisReplyView{};
    };
    RedisSink sink(stream_opts(db));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the connection to Redis failed"), std::string::npos) << why;
    EXPECT_NE(why.find("with 1 of 3 commands"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 2 may or may not have been"), std::string::npos) << why;
    expect_refuses_everything(sink);
}

TEST(RedisSinkFailures, ACancelEndsTheFlushInFlight) {
    auto db = std::make_shared<FakeRedis>();
    db->blocks = [](const Command&) { return true; };
    RedisSink sink(stream_opts(db));
    RedisAttached a(sink);
    sink.open();
    sink.on_data(batch({"a", "b"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{5});
    EXPECT_NE(why.find("the task was cancelled"), std::string::npos) << why;
    EXPECT_NE(why.find("with 0 of 2 commands"), std::string::npos) << why;
    EXPECT_NO_THROW(sink.close());
}

TEST(RedisSinkFailures, ACancelRequestedBeforeAFlushSendsNothing) {
    auto db = std::make_shared<FakeRedis>();
    RedisSink sink(stream_opts(db));
    RedisAttached a(sink);
    sink.open();
    sink.on_data(batch({"a"}));
    a.cancel->store(true);
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("cancelled before writing"), std::string::npos) << why;
    EXPECT_EQ(db->sent_count(), 0U);
}

TEST(RedisSinkFailures, AQueueingFailureSendsNothing) {
    auto db = std::make_shared<FakeRedis>();
    db->append_fails = true;
    RedisSink sink(stream_opts(db));
    sink.open();
    sink.on_data(batch({"a"}));
    const auto why = thrown([&] { sink.flush(); });
    EXPECT_NE(why.find("nothing of it was sent"), std::string::npos) << why;
    EXPECT_EQ(db->sent_count(), 0U);
    expect_refuses_everything(sink);
}

TEST(RedisSinkFailures, CloseDiscardsWhatIsBufferedAndNeverThrows) {
    auto db = std::make_shared<FakeRedis>();
    RedisSink sink(stream_opts(db));
    sink.open();
    sink.on_data(batch({"a"}));
    EXPECT_NO_THROW(sink.close());
    EXPECT_EQ(db->sent_count(), 0U);

    db->on_command = [](const Command&, std::size_t) { return lost(); };
    sink.open();
    sink.on_data(batch({"b"}));
    EXPECT_FALSE(thrown([&] { sink.flush(); }).empty());
    EXPECT_NO_THROW(sink.close());
    EXPECT_NO_THROW(sink.close());
}

TEST(RedisSinkFailures, AnOversizedRecordFailsTheSinkBeforeAnythingIsSent) {
    auto db = std::make_shared<FakeRedis>();
    auto o = stream_opts(db);
    o.max_record_bytes = 8;
    RedisSink sink(std::move(o));
    sink.open();
    const auto why = thrown([&] { sink.on_data(batch({"12345678", "secret-123"})); });
    EXPECT_NE(why.find("10 bytes, more than max_record_bytes (8)"), std::string::npos) << why;
    EXPECT_EQ(why.find("secret"), std::string::npos) << why;
    EXPECT_EQ(db->sent_count(), 0U);
    expect_refuses_everything(sink);
}

TEST(RedisSinkFailures, TheCountByteAndLingerBoundsFlush) {
    auto counted = std::make_shared<FakeRedis>();
    auto o = stream_opts(counted);
    o.batch_records = 2;
    RedisSink by_count(std::move(o));
    by_count.open();
    by_count.on_data(batch({"a", "b", "c"}));
    EXPECT_EQ(counted->applied_commands().size(), 2U);

    auto sized = std::make_shared<FakeRedis>();
    o = stream_opts(sized);
    o.max_bytes = 10;
    RedisSink by_bytes(std::move(o));
    by_bytes.open();
    by_bytes.on_data(batch({"123456", "123456", "1"}));
    EXPECT_EQ(sized->applied_commands().size(), 2U);

    auto aged = std::make_shared<FakeRedis>();
    o = stream_opts(aged);
    o.max_age = std::chrono::milliseconds{1};
    RedisSink by_age(std::move(o));
    by_age.open();
    by_age.on_data(batch({"a"}));
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    by_age.on_data(batch({"b"}));
    EXPECT_EQ(aged->applied_commands().size(), 2U);

    // Unset, the byte bound is 16 MiB, not off.
    auto defaulted = std::make_shared<FakeRedis>();
    o = stream_opts(defaulted);
    o.max_record_bytes = 32U << 20;
    RedisSink by_default(std::move(o));
    by_default.open();
    by_default.on_data(batch({std::string(16U << 20, 'x')}));
    EXPECT_EQ(defaulted->applied_commands().size(), 1U);
}

TEST(RedisSinkFailures, AFailedConnectLeavesTheSinkUnopened) {
    auto db = std::make_shared<FakeRedis>();
    db->connect_fails = true;
    RedisSink sink(stream_opts(db));
    EXPECT_NE(thrown([&] { sink.open(); }).find("Connection refused"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("before open()"), std::string::npos);
    EXPECT_NE(thrown([&] { sink.on_barrier(kBarrier); }).find("before open()"), std::string::npos);
    EXPECT_NO_THROW(sink.close());
}

TEST(RedisSinkFailures, TheCommandTimeoutDefaultsToThirtySeconds) {
    auto db = std::make_shared<FakeRedis>();
    RedisSink defaulted(stream_opts(db));
    defaulted.open();
    auto o = stream_opts(db);
    o.conn.command_timeout = std::chrono::milliseconds{500};
    RedisSink set(std::move(o));
    set.open();
    ASSERT_EQ(db->connects.size(), 2U);
    EXPECT_EQ(db->connects[0].command_timeout, std::chrono::milliseconds{30000});
    EXPECT_EQ(db->connects[1].command_timeout, std::chrono::milliseconds{500});
}

// ---- the keyed sink ---------------------------------------------------------------------

TEST(RedisUpsertSinkFailures, ChangesNetByKeyAndTheValueIsTheRecordAsWritten) {
    auto db = std::make_shared<FakeRedis>();
    RedisUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({"{\"id\":1, \"v\":1.50,\"w\":\"caf\xc3\xa9\"}",
                        R"({"id":2,"v":"x"})",
                        R"({"id":2,"__row_kind":"delete"})",
                        R"({"__row_kind":"update_after","id":3,"v":9007199254740993})",
                        R"({"id":4,"v":"old"})",
                        R"({"id":4,"v":"new","__row_kind":"insert"})"}));
    sink.on_barrier(kBarrier);
    const auto applied = db->applied_commands();
    EXPECT_EQ(std::set<Command>(applied.begin(), applied.end()),
              (std::set<Command>{{"SET", "p:1", "{\"id\":1, \"v\":1.50,\"w\":\"caf\xc3\xa9\"}"},
                                 {"DEL", "p:2"},
                                 {"SET", "p:3", R"({"id":3,"v":9007199254740993})"},
                                 {"SET", "p:4", R"({"id":4,"v":"new"})"}}));
    EXPECT_EQ(applied.size(), 4U);
}

TEST(RedisUpsertSinkFailures, NumberKeysAreExactPastADoublesPrecision) {
    auto db = std::make_shared<FakeRedis>();
    RedisUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":9007199254740993,"v":"a"})",
                        R"({"id":9007199254740992,"v":"b"})",
                        R"({"id":1.50,"v":"c"})"}));
    sink.flush();
    std::set<std::string> keys;
    for (const auto& c : db->applied_commands()) {
        keys.insert(c[1]);
    }
    EXPECT_EQ(keys, (std::set<std::string>{"p:9007199254740993", "p:9007199254740992", "p:1.5"}));
}

TEST(RedisUpsertSinkFailures, AnUnknownMarkerOrUnreadableRecordFailsTheSink) {
    auto db = std::make_shared<FakeRedis>();
    RedisUpsertSink sink(upsert_opts(db));
    sink.open();
    const auto why =
        thrown([&] { sink.on_data(batch({R"({"id":1,"pin":"4321","__row_kind":"upsert"})"})); });
    EXPECT_NE(why.find("a record cannot be written"), std::string::npos) << why;
    EXPECT_EQ(why.find("4321"), std::string::npos) << why;
    expect_refuses_everything(sink);

    sink.open();
    EXPECT_NE(thrown([&] { sink.on_data(batch({R"([1,2])"})); }).find("a record cannot be written"),
              std::string::npos);
    sink.open();
    EXPECT_NE(thrown([&] {
                  sink.on_data(batch({R"({"id":1,"id":2})"}));
              }).find("a record cannot be written"),
              std::string::npos);
    EXPECT_EQ(db->sent_count(), 0U);
}

TEST(RedisUpsertSinkFailures, AKeyThatNamesNoRowOrTwoIsRefused) {
    auto db = std::make_shared<FakeRedis>();
    for (const char* record : {R"({"v":1})", R"({"id":null})", R"({"id":{"a":1}})"}) {
        RedisUpsertSink sink(upsert_opts(db));
        sink.open();
        EXPECT_NE(thrown([&] { sink.on_data(batch({record})); }).find("a record cannot be written"),
                  std::string::npos)
            << record;
    }
    auto o = upsert_opts(db);
    o.key_columns = {"a", "b"};
    RedisUpsertSink composite(std::move(o));
    composite.open();
    EXPECT_NE(
        thrown([&] { composite.on_data(batch({R"({"a":"x\u001fy","b":"z"})"})); }).find("U+001F"),
        std::string::npos);
    EXPECT_EQ(db->sent_count(), 0U);
}

TEST(RedisUpsertSinkFailures, TheKeyAndByteBoundsFlush) {
    // Changes to one key are one buffered key.
    auto counted = std::make_shared<FakeRedis>();
    auto o = upsert_opts(counted);
    o.batch_records = 2;
    RedisUpsertSink by_count(std::move(o));
    by_count.open();
    by_count.on_data(batch({R"({"id":1,"v":"a"})", R"({"id":1,"v":"b"})", R"({"id":1,"v":"c"})"}));
    EXPECT_EQ(counted->sent_count(), 0U);
    by_count.on_data(batch({R"({"id":2,"v":"a"})"}));
    EXPECT_EQ(counted->applied_commands().size(), 2U);

    // The byte bound counts each key and its value once.
    auto sized = std::make_shared<FakeRedis>();
    o = upsert_opts(sized);
    o.max_bytes = 40;
    RedisUpsertSink by_bytes(std::move(o));
    by_bytes.open();
    by_bytes.on_data(batch({R"({"id":1,"v":"aaaaaaaaaa"})", R"({"id":1,"v":"bbbbbbbbbb"})"}));
    EXPECT_EQ(sized->sent_count(), 0U);
    by_bytes.on_data(batch({R"({"id":2,"v":"cccccccccc"})"}));
    EXPECT_EQ(sized->applied_commands().size(), 2U);
}

TEST(RedisUpsertSinkFailures, ARefusedCommandFailsTheSinkAndCloseDiscards) {
    auto db = std::make_shared<FakeRedis>();
    db->on_command = [](const Command& c, std::size_t) {
        return c[0] == "SET" ? refused("OOM command not allowed when used memory > 'maxmemory'.")
                             : RedisReplyView{};
    };
    RedisUpsertSink sink(upsert_opts(db));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})", R"({"id":2,"__row_kind":"delete"})"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("Redis refused 1 of 2 commands"), std::string::npos) << why;
    EXPECT_NE(why.find("the keys under prefix p:"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 1 were applied"), std::string::npos) << why;
    expect_refuses_everything(sink);
    EXPECT_NO_THROW(sink.close());

    db->on_command = nullptr;
    sink.open();
    sink.on_data(batch({R"({"id":3,"v":"c"})"}));
    sink.close();
    EXPECT_EQ(db->applied_commands(), (std::vector<Command>{{"DEL", "p:2"}}));
}
