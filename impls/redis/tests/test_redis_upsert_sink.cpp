// RedisUpsertSink tests - the changelog-aware key-value sink (mode='upsert').
//
// Construction validation runs anywhere. The LIVE integration tests are SKIPPED
// unless CLINK_REDIS_TEST_URL is set. They prove a changelog stream maintains a
// key-value view: insert/update_after SET by key, delete/update_before DEL,
// netted within a flush, replay-idempotent.

#include <cstdint>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_REDIS
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_upsert_sink.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::redis::Connection;
using clink::redis::ConnectOptions;
using clink::redis::RedisUpsertSink;
using clink::redis::RedisUpsertSinkOptions;
using clink::redis::Reply;

namespace {

// --- construction validation (no server) --------------------------------

TEST(RedisUpsertSink, RejectsEmptyKeyColumns) {
    RedisUpsertSinkOptions o;
    EXPECT_THROW(RedisUpsertSink{std::move(o)}, std::runtime_error);
}

// --- live integration ---------------------------------------------------

bool redis_configured() {
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

std::string uniq_prefix() {
    return "clup:" + std::to_string(static_cast<long>(::getpid())) + ":";
}

// The Redis key the sink writes for a single-int-PK row: prefix + <id> + US.
std::string key_for(const std::string& prefix, int id) {
    return prefix + std::to_string(id) + std::string(1, '\x1f');
}

RedisUpsertSinkOptions opts_for(const std::string& prefix) {
    RedisUpsertSinkOptions o;
    o.conn = redis_conn();
    o.key_columns = {"id"};
    o.key_prefix = prefix;
    return o;
}

void apply_changelog(const std::string& prefix, const std::vector<std::string>& rows) {
    RedisUpsertSink sink(opts_for(prefix));
    sink.open();
    Batch<std::string> b;
    for (const auto& r : rows) {
        b.emplace(std::string(r));
    }
    sink.on_data(b);
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.close();
}

int key_count(Connection& c, const std::string& prefix) {
    Reply r = c.command({"KEYS", prefix + "*"});
    return r.get() != nullptr ? static_cast<int>(r->elements) : -1;
}

std::string get_value(Connection& c, const std::string& prefix, int id) {
    Reply r = c.command({"GET", key_for(prefix, id)});
    if (r.is_nil() || r.get() == nullptr || r->str == nullptr) {
        return {};
    }
    return std::string(r->str, r->len);
}

void cleanup(const std::string& prefix) {
    Connection c{redis_conn()};
    Reply keys = c.command({"KEYS", prefix + "*"});
    if (keys.get() != nullptr) {
        for (std::size_t i = 0; i < keys->elements; ++i) {
            c.command({"DEL", std::string(keys->element[i]->str, keys->element[i]->len)});
        }
    }
}

#define REQUIRE_LIVE_REDIS()                            \
    do {                                                \
        if (!redis_configured())                        \
            GTEST_SKIP() << "set CLINK_REDIS_TEST_URL"; \
    } while (0)

TEST(RedisUpsertSinkLive, InsertThenUpdateByKey) {
    REQUIRE_LIVE_REDIS();
    const std::string p = uniq_prefix() + "upd:";
    Connection c{redis_conn()};

    apply_changelog(p, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    EXPECT_EQ(key_count(c, p), 2);

    apply_changelog(p, {R"({"id":1,"val":"a2","__row_kind":"update_after"})"});
    EXPECT_EQ(key_count(c, p), 2);
    // The stored value is clean JSON (no __row_kind) with the updated value.
    EXPECT_EQ(get_value(c, p, 1), R"({"id":1,"val":"a2"})");
    cleanup(p);
}

TEST(RedisUpsertSinkLive, DeleteRemovesByKey) {
    REQUIRE_LIVE_REDIS();
    const std::string p = uniq_prefix() + "del:";
    Connection c{redis_conn()};

    apply_changelog(p, {R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})"});
    apply_changelog(p, {R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(key_count(c, p), 1);
    EXPECT_TRUE(get_value(c, p, 1).empty());
    EXPECT_EQ(get_value(c, p, 2), R"({"id":2,"val":"b"})");
    cleanup(p);
}

TEST(RedisUpsertSinkLive, NettedWithinOneFlush) {
    REQUIRE_LIVE_REDIS();
    const std::string p = uniq_prefix() + "net:";
    Connection c{redis_conn()};

    apply_changelog(p,
                    {R"({"id":1,"val":"first"})",
                     R"({"id":2,"val":"x"})",
                     R"({"id":2,"val":"y","__row_kind":"update_after"})",
                     R"({"id":1,"__row_kind":"delete"})"});

    EXPECT_EQ(key_count(c, p), 1);
    EXPECT_EQ(get_value(c, p, 2), R"({"id":2,"val":"y"})");
    cleanup(p);
}

TEST(RedisUpsertSinkLive, ReplayIsIdempotent) {
    REQUIRE_LIVE_REDIS();
    const std::string p = uniq_prefix() + "replay:";
    Connection c{redis_conn()};

    const std::vector<std::string> batch = {
        R"({"id":1,"val":"a"})", R"({"id":2,"val":"b"})", R"({"id":1,"__row_kind":"delete"})"};
    apply_changelog(p, batch);
    apply_changelog(p, batch);

    EXPECT_EQ(key_count(c, p), 1);
    EXPECT_EQ(get_value(c, p, 2), R"({"id":2,"val":"b"})");
    cleanup(p);
}

}  // namespace

#else  // !CLINK_HAS_REDIS

TEST(RedisUpsertSink, SkippedWithoutRedis) {
    GTEST_SKIP() << "built without hiredis";
}

#endif  // CLINK_HAS_REDIS
