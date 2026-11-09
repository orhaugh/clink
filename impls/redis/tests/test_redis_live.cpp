// Redis Streams LIVE integration test. SKIPPED unless CLINK_REDIS_TEST_URL is set
// (e.g. redis://localhost:6379 from docker/integration-services.yml); the live
// gate, scripts/brokers-live.sh, sets it and fails on any skip. Proves
// against a real redis-server: a sink->source round-trip delivers every record
// verbatim; an un-acked consumer re-delivers its PEL on restart (at-least-once);
// XGROUP CREATE is idempotent across re-opens; and the source's acknowledgement
// contract (RedisSourceAckLive), judged by the server's own pending-entries list
// and by what a restarted consumer reads, never by the source's counters.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"

#ifdef CLINK_HAS_REDIS
#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_sink.hpp"
#include "clink/redis/redis_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#endif

#ifdef CLINK_HAS_REDIS

using clink::Batch;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
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

    EXPECT_EQ(cap.values.size(), kN) << "no over-emission / duplicate delivery";
    std::set<std::string> got(cap.values.begin(), cap.values.end());
    EXPECT_EQ(got.size(), kN) << "every sunk record should round-trip back verbatim";
    for (std::size_t i = 0; i < kN; ++i) {
        EXPECT_EQ(got.count(R"({"i":)" + std::to_string(i) + "}"), 1u) << "missing record " << i;
    }
}

// XACK once the checkpoint completes is the offset commit: after acking, a
// restart of the SAME consumer must NOT replay them (the complement of the
// PEL-replay test).
TEST(RedisLive, AckedEntriesAreNotReplayed) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    const std::string stream = unique_stream();
    constexpr std::size_t kM = 8;
    write_records(stream, kM);

    InMemoryStateBackend backend;
    const OperatorId op{1};
    {
        RedisSource s1(source_opts(stream, "ga"));
        s1.open();
        Captured cap;
        drain(s1, cap, kM, /*timeout_ms=*/15000);
        s1.snapshot_offset(backend, op, clink::CheckpointId{1});
        s1.notify_checkpoint_complete(clink::CheckpointId{1});
        s1.close();  // XACKs what the completed checkpoint covers
        ASSERT_EQ(std::set<std::string>(cap.values.begin(), cap.values.end()).size(), kM);
    }
    std::size_t replayed = 0;
    {
        RedisSource s2(source_opts(stream, "ga"));
        s2.open();
        Captured cap;
        // PEL is empty (acked) and there are no new entries, so this drains
        // nothing and times out with zero records.
        drain(s2, cap, /*want=*/1, /*timeout_ms=*/1500);
        s2.close();
        replayed = cap.values.size();
    }
    cleanup(stream);
    EXPECT_EQ(replayed, 0u) << "acked entries must not be replayed";
}

// An entry with multiple fields (not the sink's single "v") renders as a JSON
// object of all field/value pairs (the non-round-trip producer path).
TEST(RedisLive, MultiFieldEntryRendersJsonObject) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    const std::string stream = unique_stream();
    {
        Connection c{redis_conn()};
        c.command({"XADD", stream, "*", "a", "1", "b", "two"});  // two fields, no "v"
    }
    RedisSource src(source_opts(stream, "gm"));
    src.open();
    Captured cap;
    drain(src, cap, 1, /*timeout_ms=*/15000);
    src.close();
    cleanup(stream);

    ASSERT_EQ(cap.values.size(), 1u);
    auto j = clink::config::parse(cap.values[0]);
    ASSERT_TRUE(j.is_object());
    EXPECT_EQ(j.as_object().at("a").as_string(), "1");
    EXPECT_EQ(j.as_object().at("b").as_string(), "two");
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

// --- the source's acknowledgement contract --------------------------------

namespace {

void write_tagged(const std::string& stream, const std::string& tag, std::size_t n) {
    RedisSink sink(sink_opts(stream));
    sink.open();
    Batch<std::string> b;
    for (std::size_t i = 0; i < n; ++i) {
        b.emplace(tag + std::to_string(i));
    }
    sink.on_data(b);
    sink.flush();
    sink.close();
}

std::vector<std::string> tagged(const std::string& tag, std::size_t n) {
    std::vector<std::string> v;
    for (std::size_t i = 0; i < n; ++i) {
        v.push_back(tag + std::to_string(i));
    }
    return v;
}

// The group's pending entries, as the server counts them.
long long pending(const std::string& stream, const std::string& group) {
    Connection c{redis_conn()};
    auto r = c.command({"XPENDING", stream, group});
    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements < 1 || r->element[0] == nullptr) {
        return -1;
    }
    return r->element[0]->integer;
}

std::set<std::string> as_set(const std::vector<std::string>& v) {
    return {v.begin(), v.end()};
}

// What a restart of the same consumer reads back from its PEL.
std::set<std::string> replay(const RedisSourceOptions& o, std::size_t want) {
    RedisSource s(o);
    s.open();
    Captured cap;
    drain(s, cap, want, /*timeout_ms=*/5000);
    s.close();
    return as_set(cap.values);
}

struct LiveStream {
    LiveStream() : name(unique_stream()) {}
    ~LiveStream() { cleanup(name); }
    std::string name;
};

}  // namespace

TEST(RedisSourceAckLive, ASnapshotThenAbortLeavesEntriesPendingForTheRestart) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    write_tagged(st.name, "a", 6);
    const auto o = source_opts(st.name, "gab");
    InMemoryStateBackend backend;
    {
        RedisSource src(o);
        src.open();
        Captured cap;
        drain(src, cap, 6, 15000);
        ASSERT_EQ(cap.values.size(), 6u);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_aborted(clink::CheckpointId{1});
        drain(src, cap, 7, 300);  // turns in which an XACK would run
        src.close();
    }
    EXPECT_EQ(pending(st.name, "gab"), 6) << "an aborted checkpoint acknowledged its entries";
    EXPECT_EQ(replay(o, 6), as_set(tagged("a", 6)));
}

TEST(RedisSourceAckLive, ACrashAfterTheSnapshotLeavesEntriesPending) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    write_tagged(st.name, "b", 5);
    const auto o = source_opts(st.name, "gcs");
    InMemoryStateBackend backend;
    {
        RedisSource src(o);
        src.open();
        Captured cap;
        drain(src, cap, 5, 15000);
        ASSERT_EQ(cap.values.size(), 5u);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        // The process dies here: no completion, no close.
    }
    EXPECT_EQ(pending(st.name, "gcs"), 5);
    EXPECT_EQ(replay(o, 5), as_set(tagged("b", 5)));
}

TEST(RedisSourceAckLive, ACompletionAcknowledgesOnlyTheIntervalsItCovers) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    const auto o = source_opts(st.name, "gci");
    InMemoryStateBackend backend;
    RedisSource src(o);
    src.open();
    Captured cap;
    write_tagged(st.name, "x", 3);
    drain(src, cap, 3, 15000);
    src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
    write_tagged(st.name, "y", 2);
    drain(src, cap, 5, 15000);
    src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{2});
    write_tagged(st.name, "z", 1);
    drain(src, cap, 6, 15000);  // after the last barrier
    ASSERT_EQ(cap.values.size(), 6u);

    src.notify_checkpoint_complete(clink::CheckpointId{1});
    drain(src, cap, 7, 300);
    EXPECT_EQ(pending(st.name, "gci"), 3) << "checkpoint 1's completion reached past its interval";

    src.notify_checkpoint_complete(clink::CheckpointId{2});
    src.notify_checkpoint_complete(clink::CheckpointId{1});  // late
    src.notify_checkpoint_complete(clink::CheckpointId{2});  // repeated
    src.notify_checkpoint_aborted(clink::CheckpointId{1});   // late and contradictory
    src.notify_checkpoint_complete(clink::CheckpointId{9});  // above anything sealed
    drain(src, cap, 7, 300);
    EXPECT_EQ(pending(st.name, "gci"), 1) << "only the open interval's entry stays pending";

    src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{10});
    src.notify_checkpoint_complete(clink::CheckpointId{10});
    drain(src, cap, 7, 300);
    EXPECT_EQ(pending(st.name, "gci"), 0);
    EXPECT_EQ(src.acknowledged_total(), 6u) << "an entry was acknowledged twice";
    src.close();
}

TEST(RedisSourceAckLive, ACrashAfterCompletionBeforeTheXackReplaysTheInterval) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    write_tagged(st.name, "d", 4);
    const auto o = source_opts(st.name, "gcc");
    InMemoryStateBackend backend;
    {
        RedisSource src(o);
        src.open();
        Captured cap;
        drain(src, cap, 4, 15000);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        // The process dies before the source's next turn sends the XACK.
    }
    EXPECT_EQ(pending(st.name, "gcc"), 4);
    EXPECT_EQ(replay(o, 4), as_set(tagged("d", 4))) << "duplicates, not a loss";
}

// The source's connection dies between the completion and the XACK: the turn
// that would send it fails the subtask, and the restart reads the entries again.
TEST(RedisSourceAckLive, AFailedXackAfterCompletionFailsTheSubtaskAndLosesNothing) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    write_tagged(st.name, "e", 4);
    const auto o = source_opts(st.name, "gfx");
    InMemoryStateBackend backend;
    {
        RedisSource src(o);
        src.open();
        Captured cap;
        drain(src, cap, 4, 15000);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        // Kill the source's connection: the client whose last command was its
        // XREADGROUP on this stream's group.
        Connection admin{redis_conn()};
        auto list = admin.command({"CLIENT", "LIST", "TYPE", "normal"});
        ASSERT_TRUE(static_cast<bool>(list));
        const std::string clients(list->str, list->len);
        int killed = 0;
        std::size_t pos = 0;
        while ((pos = clients.find("id=", pos)) != std::string::npos) {
            const auto eol = clients.find('\n', pos);
            const auto line = clients.substr(pos, eol - pos);
            if (line.find("cmd=xreadgroup") != std::string::npos) {
                const auto id = line.substr(3, line.find(' ') - 3);
                admin.command({"CLIENT", "KILL", "ID", id});
                ++killed;
            }
            pos = eol;
        }
        ASSERT_GE(killed, 1) << "found no source connection to kill:\n" << clients;
        EXPECT_EQ(pending(st.name, "gfx"), 4) << "the entries were not in flight";
        auto em = capturing(cap);
        EXPECT_THROW(src.produce(em), std::runtime_error) << "a failed XACK went unnoticed";
    }
    EXPECT_EQ(pending(st.name, "gfx"), 4);
    EXPECT_EQ(replay(o, 4), as_set(tagged("e", 4)));
}

// The XACK reaches the server but its reply does not reach the source (the
// server holds writes past the client's timeout). Whether it applied or not,
// the restart reads again exactly the entries still pending, and every entry is
// either acknowledged or read again.
TEST(RedisSourceAckLive, AnXackWhoseReplyIsLostLeavesNothingUnrecoverable) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    write_tagged(st.name, "f", 4);
    const auto o = source_opts(st.name, "gam");  // block 200 ms: a 2.4 s command timeout
    InMemoryStateBackend backend;
    {
        RedisSource src(o);
        src.open();
        Captured cap;
        drain(src, cap, 4, 15000);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        Connection admin{redis_conn()};
        admin.command({"CLIENT", "PAUSE", "3500", "WRITE"});
        auto em = capturing(cap);
        EXPECT_THROW(src.produce(em), std::runtime_error) << "a lost XACK reply went unnoticed";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{1500});  // the pause ends
    const auto still = pending(st.name, "gam");
    ASSERT_TRUE(still == 0 || still == 4) << still;
    const auto back = replay(o, static_cast<std::size_t>(still));
    EXPECT_EQ(back.size(), static_cast<std::size_t>(still));
    if (still == 4) {
        EXPECT_EQ(back, as_set(tagged("f", 4)));
    }
}

TEST(RedisSourceAckLive, TheBoundPausesReadingUntilACheckpointCompletes) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    write_tagged(st.name, "p", 7);
    auto o = source_opts(st.name, "gbd");
    o.max_pending = 3;
    InMemoryStateBackend backend;
    RedisSource src(o);
    src.open();
    Captured cap;
    drain(src, cap, 7, 1500);
    EXPECT_EQ(cap.values.size(), 3u) << "the source read past max_pending";
    EXPECT_EQ(pending(st.name, "gbd"), 3);
    src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
    src.notify_checkpoint_complete(clink::CheckpointId{1});
    drain(src, cap, 7, 1500);
    EXPECT_EQ(cap.values.size(), 6u);
    EXPECT_EQ(pending(st.name, "gbd"), 3);
    src.close();
}

TEST(RedisSourceAckLive, CloseAcknowledgesCompletedIntervalsOnly) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    const auto o = source_opts(st.name, "gcl");
    InMemoryStateBackend backend;
    {
        RedisSource src(o);
        src.open();
        Captured cap;
        write_tagged(st.name, "g", 2);
        drain(src, cap, 2, 15000);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
        src.notify_checkpoint_complete(clink::CheckpointId{1});
        write_tagged(st.name, "h", 2);
        drain(src, cap, 4, 15000);
        src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{2});  // never completes
        write_tagged(st.name, "k", 1);
        drain(src, cap, 5, 15000);  // after the last barrier
        ASSERT_EQ(cap.values.size(), 5u);
        src.cancel();
        src.close();
    }
    EXPECT_EQ(pending(st.name, "gcl"), 3)
        << "close acknowledged what no completed checkpoint covers";
    EXPECT_EQ(replay(o, 3), (std::set<std::string>{"h0", "h1", "k0"}));
}

// A run at parallelism 2 leaves entries pending on its second consumer. A run at
// parallelism 1 would never read them again, so it refuses to start; a run at
// parallelism 2 starts and its second subtask reads them again.
TEST(RedisSourceAckLive, AScaleDownIsRefusedWhileARemovedConsumerHoldsPendingEntries) {
    if (!redis_configured()) {
        GTEST_SKIP() << "set CLINK_REDIS_TEST_URL";
    }
    LiveStream st;
    auto at = [&](std::uint32_t parallelism, std::uint32_t subtask) {
        auto o = source_opts(st.name, "gsd");
        o.parallelism = parallelism;
        o.subtask_idx = subtask;
        return o;
    };
    write_tagged(st.name, "s", 5);
    {
        RedisSource second(at(2, 1));
        second.open();
        Captured cap;
        drain(second, cap, 5, 15000);
        ASSERT_EQ(cap.values.size(), 5u);
        // Dies holding them: never acknowledged.
    }
    ASSERT_EQ(pending(st.name, "gsd"), 5);
    {
        RedisSource lone(at(1, 0));
        try {
            lone.open();
            ADD_FAILURE() << "a source at parallelism 1 started beside clink-1's pending entries";
        } catch (const std::runtime_error& e) {
            const std::string what = e.what();
            EXPECT_NE(what.find("clink-1 (5 pending)"), std::string::npos) << what;
            EXPECT_NE(what.find("parallelism 2 or more"), std::string::npos) << what;
        }
    }
    EXPECT_EQ(pending(st.name, "gsd"), 5) << "the refused source touched the pending entries";
    EXPECT_EQ(replay(at(2, 1), 5), as_set(tagged("s", 5)));
}

#ifdef CLINK_REDIS_TLS
// TLS round-trip. SKIPPED unless CLINK_REDIS_TLS_TEST_URL is set to a TLS-enabled
// redis (host:port). Connects with tls=true + tls_verify=false (self-signed dev
// cert) and proves an encrypted PING + an XADD/XLEN round-trip work over TLS.
TEST(RedisLive, TlsConnectionRoundTrip) {
    const char* url = std::getenv("CLINK_REDIS_TLS_TEST_URL");
    if (url == nullptr) {
        GTEST_SKIP() << "set CLINK_REDIS_TLS_TEST_URL to a TLS redis host:port";
    }
    std::string hp = url;
    ConnectOptions o;
    if (auto c = hp.rfind(':'); c != std::string::npos) {
        o.host = hp.substr(0, c);
        o.port = static_cast<std::uint16_t>(std::stoi(hp.substr(c + 1)));
    } else {
        o.host = hp;
    }
    o.tls = true;
    o.tls_verify = false;  // self-signed dev cert: encrypt but do not authenticate
    Connection c{o};
    clink::redis::Reply pong = c.command({"PING"});
    ASSERT_TRUE(static_cast<bool>(pong));
    const std::string stream = unique_stream();
    c.command({"XADD", stream, "*", "v", "hello-tls"});
    clink::redis::Reply len = c.command({"XLEN", stream});
    ASSERT_TRUE(static_cast<bool>(len));
    EXPECT_EQ(len->integer, 1);
    c.command({"DEL", stream});
}
#endif  // CLINK_REDIS_TLS

#endif  // CLINK_HAS_REDIS
