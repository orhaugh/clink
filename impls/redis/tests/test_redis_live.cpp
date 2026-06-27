// Redis Streams LIVE integration test. SKIPPED unless CLINK_REDIS_TEST_URL is set
// (e.g. redis://localhost:6379 from docker/integration-services.yml). Proves
// against a real redis-server: a sink->source round-trip delivers every record
// verbatim; an un-acked consumer re-delivers its PEL on restart (at-least-once);
// and XGROUP CREATE is idempotent across re-opens.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_REDIS
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_source.hpp"
#endif

#ifdef CLINK_HAS_REDIS

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::redis::Connection;
using clink::redis::ConnectOptions;
using clink::redis::RedisSink;
using clink::redis::RedisSinkOptions;
using clink::redis::RedisSource;
using clink::redis::RedisSourceOptions;

namespace {

bool redis_configured() {
    return std::getenv("CLINK_REDIS_TEST_URL") != nullptr;
}

// Parse redis://host:port (scheme and path optional) into ConnectOptions.
ConnectOptions redis_conn() {
    std::string url = std::getenv("CLINK_REDIS_TEST_URL");
    if (auto p = url.find("://"); p != std::string::npos) {
        url = url.substr(p + 3);
    }
    if (auto slash = url.find('/'); slash != std::string::npos) {
        url = url.substr(0, slash);
    }
    ConnectOptions o;
    if (auto c = url.rfind(':'); c != std::string::npos) {
        o.host = url.substr(0, c);
        o.port = static_cast<std::uint16_t>(std::stoi(url.substr(c + 1)));
    } else if (!url.empty()) {
        o.host = url;
    }
    return o;
}

std::string unique_stream() {
    static int n = 0;
    return "clink-it-" + std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n++);
}

void cleanup(const std::string& stream) {
    Connection c{redis_conn()};
    c.command({"DEL", stream});  // removes the stream and all its groups
}

RedisSinkOptions sink_opts(const std::string& stream) {
    RedisSinkOptions o;
    o.conn = redis_conn();
    o.stream = stream;
    return o;
}

RedisSourceOptions source_opts(const std::string& stream, const std::string& group) {
    RedisSourceOptions o;
    o.conn = redis_conn();
    o.stream = stream;
    o.group = group;
    o.start_id = "0";  // read the whole stream from the start (batch replay)
    o.block = std::chrono::milliseconds{200};  // responsive drain loop
    return o;
}

struct Captured {
    std::vector<std::string> values;
};
Emitter<std::string> capturing(Captured& sink) {
    return Emitter<std::string>{[&sink](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}

void drain(RedisSource& src, Captured& cap, std::size_t want, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{timeout_ms};
    while (cap.values.size() < want && std::chrono::steady_clock::now() < deadline) {
        auto em = capturing(cap);
        src.produce(em);
    }
}

void write_records(const std::string& stream, std::size_t n) {
    RedisSink sink(sink_opts(stream));
    sink.open();
    Batch<std::string> b;
    for (std::size_t i = 0; i < n; ++i) {
        b.emplace(R"({"i":)" + std::to_string(i) + "}");
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

}  // namespace

TEST(RedisLive, SinkThenSourceRoundTrip) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL (docker/integration-services.yml)";
    }
    const std::string stream = unique_stream();
    constexpr std::size_t kN = 20;
    write_records(stream, kN);

    RedisSource src(source_opts(stream, "g1"));
    src.open();
    Captured cap;
    drain(src, cap, kN, /*timeout_ms=*/15000);
    src.close();
    cleanup(stream);

    std::set<std::string> got(cap.values.begin(), cap.values.end());
    EXPECT_EQ(got.size(), kN) << "every sunk record should round-trip back verbatim";
    for (std::size_t i = 0; i < kN; ++i) {
        EXPECT_EQ(got.count(R"({"i":)" + std::to_string(i) + "}"), 1u) << "missing record " << i;
    }
}

TEST(RedisLive, UnackedConsumerReplaysPelOnRestart) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    const std::string stream = unique_stream();
    constexpr std::size_t kM = 10;
    write_records(stream, kM);

    // First reader: deliver all kM but DO NOT snapshot_offset (no XACK), so the
    // entries stay in this consumer's PEL.
    std::set<std::string> first;
    {
        RedisSource s1(source_opts(stream, "g2"));
        s1.open();
        Captured cap;
        drain(s1, cap, kM, /*timeout_ms=*/15000);
        s1.close();
        first.insert(cap.values.begin(), cap.values.end());
        ASSERT_EQ(first.size(), kM) << "first reader should see every record";
    }
    // Restart as the SAME consumer (same prefix + subtask_idx => "clink-0"): the
    // PEL re-drain (id "0") must re-deliver the un-acked entries.
    std::set<std::string> replayed;
    {
        RedisSource s2(source_opts(stream, "g2"));
        s2.open();
        Captured cap;
        drain(s2, cap, kM, /*timeout_ms=*/15000);
        s2.close();
        replayed.insert(cap.values.begin(), cap.values.end());
    }
    cleanup(stream);

    EXPECT_EQ(replayed, first) << "an un-acked consumer must replay its PEL on restart";
}

TEST(RedisLive, GroupCreateIsIdempotent) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    const std::string stream = unique_stream();
    write_records(stream, 1);

    RedisSource a(source_opts(stream, "g3"));
    RedisSource b(source_opts(stream, "g3"));
    EXPECT_NO_THROW(a.open());
    EXPECT_NO_THROW(b.open());  // second XGROUP CREATE -> BUSYGROUP, swallowed
    a.close();
    b.close();
    cleanup(stream);
}

#endif  // CLINK_HAS_REDIS
