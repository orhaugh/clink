// The Redis sinks' contract against a live server. SKIPPED unless
// CLINK_REDIS_TEST_URL names a Redis server; scripts/brokers-live.sh runs it
// against the pinned Redis 7 image and fails when any case is skipped.
//
// Every case judges by what the server holds, read through the test's own
// connection: the entries of a stream, the value of a key, the clients
// connected. Two cases pause the server's writes (CLIENT PAUSE ... WRITE) to
// make a reply late, and one lowers maxmemory to make the server refuse a
// write; each puts the server back as it found it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/runtime_context.hpp"

#ifdef CLINK_HAS_REDIS
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_upsert_sink.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::redis::Connection;
using clink::redis::ConnectOptions;
using clink::redis::RedisSink;
using clink::redis::RedisSinkOptions;
using clink::redis::RedisUpsertSink;
using clink::redis::RedisUpsertSinkOptions;
using clink::redis::Reply;

namespace {

bool redis_live() {
    return std::getenv("CLINK_REDIS_TEST_URL") != nullptr;
}

ConnectOptions redis_conn() {
    std::string url = std::getenv("CLINK_REDIS_TEST_URL");
    if (url.rfind("redis://", 0) == 0) {
        url = url.substr(8);
    }
    ConnectOptions o;
    if (const auto c = url.find(':'); c != std::string::npos) {
        o.host = url.substr(0, c);
        o.port = static_cast<std::uint16_t>(std::stoi(url.substr(c + 1)));
    } else {
        o.host = url;
    }
    return o;
}

#define REQUIRE_REDIS()                                                        \
    do {                                                                       \
        if (!redis_live()) {                                                   \
            GTEST_SKIP() << "set CLINK_REDIS_TEST_URL against a Redis server"; \
        }                                                                      \
    } while (0)

std::string unique(const std::string& what) {
    static std::atomic<int> n{0};
    return "clink:sinkc:" + std::to_string(static_cast<long>(::getpid())) + ":" +
           std::to_string(n.fetch_add(1)) + ":" + what;
}

Batch<std::string> batch(const std::vector<std::string>& records) {
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

std::string text_of(const Reply& r) {
    return (r.get() != nullptr && r->str != nullptr) ? std::string(r->str, r->len) : std::string{};
}

// The value each entry of `stream` holds under `field`, oldest first.
std::vector<std::string> entries(Connection& c,
                                 const std::string& stream,
                                 const std::string& field = "v") {
    std::vector<std::string> out;
    Reply r = c.command({"XRANGE", stream, "-", "+"});
    if (r.get() == nullptr || r->type != REDIS_REPLY_ARRAY) {
        return out;
    }
    for (std::size_t i = 0; i < r->elements; ++i) {
        const auto* fields = r->element[i]->element[1];
        for (std::size_t j = 0; j + 1 < fields->elements; j += 2) {
            if (std::string(fields->element[j]->str, fields->element[j]->len) == field) {
                out.emplace_back(fields->element[j + 1]->str, fields->element[j + 1]->len);
            }
        }
    }
    return out;
}

std::string get(Connection& c, const std::string& key) {
    Reply r = c.command({"GET", key});
    return r.is_nil() ? std::string{"<nil>"} : text_of(r);
}

long long integer(Connection& c, const std::vector<std::string>& command) {
    Reply r = c.command(command);
    return r.get() != nullptr && r->type == REDIS_REPLY_INTEGER ? r->integer : -1;
}

long long connected_clients(Connection& c) {
    const auto info = text_of(c.command({"INFO", "clients"}));
    const std::string tag = "connected_clients:";
    const auto at = info.find(tag);
    return at == std::string::npos ? -1 : std::stoll(info.substr(at + tag.size()));
}

// Waits up to `limit` for `done`, checking every 20 ms.
bool eventually(const std::function<bool()>& done,
                std::chrono::milliseconds limit = std::chrono::milliseconds{3000}) {
    const auto until = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < until) {
        if (done()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return done();
}

RedisSinkOptions stream_opts(const std::string& stream) {
    RedisSinkOptions o;
    o.conn = redis_conn();
    o.stream = stream;
    return o;
}

RedisUpsertSinkOptions upsert_opts(const std::string& prefix,
                                   std::vector<std::string> keys = {"id"}) {
    RedisUpsertSinkOptions o;
    o.conn = redis_conn();
    o.key_columns = std::move(keys);
    o.key_prefix = prefix;
    return o;
}

}  // namespace

// ---- the Streams sink ------------------------------------------------------------------

TEST(RedisSinkContractLive, StreamEntriesHoldEachRecordExactlyAndInOrder) {
    REQUIRE_REDIS();
    Connection c{redis_conn()};
    const auto s = unique("exact");
    const std::vector<std::string> records = {
        R"({"id":1,"amount":12345678901234567890.123456789,"e":1E+400})",
        std::string{"nul\0inside", 10},
        "caf\xc3\xa9 \xf0\x9f\x98\x80",
        "",
        std::string(1U << 20, 'x')};
    RedisSink sink(stream_opts(s));
    sink.open();
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(entries(c, s), records);
    c.command({"DEL", s});
}

// MAXLEN trims by length, not by what a consumer group has read: entries the
// group never read are gone.
TEST(RedisSinkContractLive, MaxlenTrimsEntriesAGroupHasNotRead) {
    REQUIRE_REDIS();
    Connection c{redis_conn()};
    const auto s = unique("maxlen");
    ASSERT_FALSE(c.command({"XGROUP", "CREATE", s, "g", "0", "MKSTREAM"}).is_error());
    auto o = stream_opts(s);
    o.maxlen = 5;
    o.approx_maxlen = false;
    RedisSink sink(std::move(o));
    sink.open();
    std::vector<std::string> records;
    for (int i = 0; i < 12; ++i) {
        records.push_back(std::to_string(i));
    }
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(entries(c, s), (std::vector<std::string>{"7", "8", "9", "10", "11"}));
    Reply read = c.command({"XREADGROUP", "GROUP", "g", "c1", "COUNT", "100", "STREAMS", s, ">"});
    ASSERT_NE(read.get(), nullptr);
    ASSERT_EQ(read->elements, 1U);
    EXPECT_EQ(read->element[0]->element[1]->elements, 5U) << "the group read only what was kept";
    c.command({"DEL", s});
}

TEST(RedisSinkContractLive, AKeyOfAnotherTypeFailsTheFlushUntilTheSinkIsOpenedAgain) {
    REQUIRE_REDIS();
    Connection c{redis_conn()};
    const auto s = unique("wrongtype");
    c.command({"SET", s, "not a stream"});
    RedisSink sink(stream_opts(s));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("Redis refused 3 of 3 commands"), std::string::npos) << why;
    EXPECT_NE(why.find("WRONGTYPE"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 0 were applied"), std::string::npos) << why;
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    EXPECT_EQ(get(c, s), "not a stream");

    c.command({"DEL", s});
    sink.open();
    sink.on_data(batch({"d"}));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(entries(c, s), (std::vector<std::string>{"d"}));
    c.command({"DEL", s});
}

// A reply that does not come within the command timeout leaves the flush
// unknown, and it is: the server applies the commands once its writes resume,
// after the sink has given up on them.
TEST(RedisSinkContractLive, ACommandThatOutlastsTheTimeoutIsAnUnknownOutcome) {
    REQUIRE_REDIS();
    Connection admin{redis_conn()};
    const auto s = unique("timeout");
    auto o = stream_opts(s);
    o.conn.command_timeout = std::chrono::milliseconds{300};
    RedisSink sink(std::move(o));
    sink.open();
    sink.on_data(batch({"a", "b", "c"}));
    ASSERT_FALSE(admin.command({"CLIENT", "PAUSE", "1500", "WRITE"}).is_error());
    const auto started = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::milliseconds{1400});
    EXPECT_NE(why.find("with 0 of 3 commands"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 3 may or may not have been"), std::string::npos) << why;
    EXPECT_TRUE(eventually([&] { return integer(admin, {"XLEN", s}) == 3; }))
        << "the paused commands ran once writes resumed";
    sink.close();
    admin.command({"DEL", s});
}

TEST(RedisSinkContractLive, ACancelEndsAFlushTheServerHasPaused) {
    REQUIRE_REDIS();
    Connection admin{redis_conn()};
    const auto s = unique("cancel");
    RedisSink sink(stream_opts(s));  // waits up to 30 s for a reply
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    clink::RuntimeContext ctx(clink::OperatorId{79}, "redis_sink", nullptr, nullptr);
    ctx.set_cancel_signal(clink::CancelSignal{cancel, nullptr});
    sink.attach_runtime(&ctx);
    sink.open();
    sink.on_data(batch({"a"}));
    ASSERT_FALSE(admin.command({"CLIENT", "PAUSE", "3000", "WRITE"}).is_error());
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        cancel->store(true);
    });
    const auto started = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::milliseconds{2000});
    EXPECT_NE(why.find("the task was cancelled"), std::string::npos) << why;
    EXPECT_NE(why.find("may or may not have been"), std::string::npos) << why;
    EXPECT_NO_THROW(sink.close());
    sink.attach_runtime(nullptr);
    admin.command({"CLIENT", "UNPAUSE"});
    admin.command({"DEL", s});
}

// A refused AUTH once left its socket open on every attempt, and a job
// restarting against a wrong password collected connections.
TEST(RedisSinkContractLive, ARefusedAuthLeavesNoConnectionOpen) {
    REQUIRE_REDIS();
    Connection admin{redis_conn()};
    const auto before = connected_clients(admin);
    ASSERT_GT(before, 0);
    auto o = stream_opts(unique("auth"));
    o.conn.password = "not-the-password";
    RedisSink sink(std::move(o));
    for (int i = 0; i < 20; ++i) {
        EXPECT_NE(thrown([&] { sink.open(); }).find("AUTH failed"), std::string::npos);
    }
    EXPECT_TRUE(eventually([&] { return connected_clients(admin) == before; }))
        << connected_clients(admin) << " clients connected, " << before << " before";
}

// ---- the keyed sink ---------------------------------------------------------------------

TEST(RedisSinkContractLive, KeyedValuesLandAsWrittenUnderExactKeys) {
    REQUIRE_REDIS();
    Connection c{redis_conn()};
    const auto p = unique("kv") + ":";
    RedisUpsertSink sink(upsert_opts(p));
    sink.open();
    sink.on_data(
        batch({R"({"id":9007199254740993,"amount":1.50,"big":123456789012345678901234567890})",
               R"({"id":9007199254740992,"note":"caf\u00e9 \"q\"","nested":{"b":[1,2.0]}})",
               R"({"__row_kind":"insert","id":1.50,"v":1e3})"}));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(get(c, p + "9007199254740993"),
              R"({"id":9007199254740993,"amount":1.50,"big":123456789012345678901234567890})");
    EXPECT_EQ(get(c, p + "9007199254740992"),
              R"({"id":9007199254740992,"note":"caf\u00e9 \"q\"","nested":{"b":[1,2.0]}})");
    EXPECT_EQ(get(c, p + "1.5"), R"({"id":1.50,"v":1e3})");
    for (const auto* k : {"9007199254740993", "9007199254740992", "1.5"}) {
        c.command({"DEL", p + k});
    }
}

TEST(RedisSinkContractLive, KeyedChangesNetAndCompositeKeysJoinOnTheSeparator) {
    REQUIRE_REDIS();
    Connection c{redis_conn()};
    const auto p = unique("net") + ":";
    {
        RedisUpsertSink sink(upsert_opts(p, {"a", "b"}));
        sink.open();
        sink.on_data(batch({R"({"a":"x","b":1,"v":"first"})",
                            R"({"a":"x","b":2,"v":"kept"})",
                            R"({"a":"x","b":1,"v":"second","__row_kind":"update_after"})",
                            R"({"a":"x","b":2,"__row_kind":"delete"})",
                            R"({"a":"x","b":2,"v":"back","__row_kind":"insert"})"}));
        sink.on_barrier(kBarrier);
        sink.on_data(batch({R"({"a":"x","b":1,"__row_kind":"delete"})"}));
        sink.on_barrier(kBarrier);
        sink.close();
    }
    EXPECT_EQ(get(c, p + "x\x1f" + "1"), "<nil>");
    EXPECT_EQ(get(c, p + "x\x1f" + "2"), R"({"a":"x","b":2,"v":"back"})");

    // A member holding the separator could name another row's key: the sink
    // fails before anything of the flush is sent.
    RedisUpsertSink refusing(upsert_opts(p, {"a", "b"}));
    refusing.open();
    const auto why = thrown([&] {
        refusing.on_data(
            batch({R"({"a":"y","b":1,"v":"fine"})", R"({"a":"x\u001f2","b":"","v":"bad"})"}));
    });
    EXPECT_NE(why.find("U+001F"), std::string::npos) << why;
    refusing.close();
    EXPECT_EQ(get(c, p + "y\x1f" + "1"), "<nil>");
    EXPECT_EQ(get(c, p + "x\x1f" + "2"), R"({"a":"x","b":2,"v":"back"})");
    c.command({"DEL", p + "x\x1f" + "2"});
}

// Under maxmemory the server refuses a SET and still applies a DEL, and the
// sink says which.
TEST(RedisSinkContractLive, AFlushTheServerPartlyRefusesSaysWhatWasApplied) {
    REQUIRE_REDIS();
    Connection admin{redis_conn()};
    const auto p = unique("oom") + ":";
    admin.command({"SET", p + "2", "doomed"});
    const auto config = [&](const std::string& name) {
        Reply r = admin.command({"CONFIG", "GET", name});
        return r.get() != nullptr && r->elements == 2
                   ? std::string(r->element[1]->str, r->element[1]->len)
                   : std::string{};
    };
    const auto maxmemory = config("maxmemory");
    const auto policy = config("maxmemory-policy");
    ASSERT_FALSE(maxmemory.empty());
    struct Restore {
        std::function<void()> f;
        ~Restore() { f(); }
    } restore{[&] {
        admin.command({"CONFIG", "SET", "maxmemory", maxmemory});
        admin.command({"CONFIG", "SET", "maxmemory-policy", policy});
    }};
    RedisUpsertSink sink(upsert_opts(p));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"refused"})", R"({"id":2,"__row_kind":"delete"})"}));
    ASSERT_FALSE(admin.command({"CONFIG", "SET", "maxmemory-policy", "noeviction"}).is_error());
    ASSERT_FALSE(admin.command({"CONFIG", "SET", "maxmemory", "1"}).is_error());
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    admin.command({"CONFIG", "SET", "maxmemory", maxmemory});
    EXPECT_NE(why.find("Redis refused 1 of 2 commands"), std::string::npos) << why;
    EXPECT_NE(why.find("OOM"), std::string::npos) << why;
    EXPECT_NE(why.find("the other 1 were applied"), std::string::npos) << why;
    EXPECT_EQ(get(admin, p + "1"), "<nil>");
    EXPECT_EQ(get(admin, p + "2"), "<nil>");
    sink.close();
}

// ---- the factories' path ----------------------------------------------------------------

TEST(RedisSinkContractLive, TheFactoriesBuildSinksThatWriteToTheirDatabase) {
    REQUIRE_REDIS();
    Connection c{redis_conn()};
    ASSERT_FALSE(c.command({"SELECT", "1"}).is_error());
    const auto s = unique("factory_stream");
    const auto p = unique("factory_kv") + ":";
    const auto conn = redis_conn();
    for (const std::string type : {"redis_sink", "redis_upsert_sink"}) {
        const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
            type, std::string{clink::cluster::kChannelString});
        ASSERT_NE(f, nullptr);
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = {{"host", conn.host},
                      {"port", std::to_string(conn.port)},
                      {"db", "1"},
                      {"stream", s},
                      {"key_columns", "id"},
                      {"key_prefix", p},
                      {"batch_records", "2"},
                      {"command_timeout_ms", "5000"}};
        auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
        sink->open();
        sink->on_data(batch({R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})", R"({"id":3,"v":"c"})"}));
        sink->on_barrier(kBarrier);
        sink->close();
    }
    EXPECT_EQ(entries(c, s),
              (std::vector<std::string>{
                  R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})", R"({"id":3,"v":"c"})"}));
    EXPECT_EQ(get(c, p + "2"), R"({"id":2,"v":"b"})");
    EXPECT_EQ(integer(c, {"DEL", s, p + "1", p + "2", p + "3"}), 4);
}

#else  // !CLINK_HAS_REDIS

TEST(RedisSinkContractLive, SkippedWithoutRedis) {
    GTEST_SKIP() << "built without hiredis";
}

#endif  // CLINK_HAS_REDIS
