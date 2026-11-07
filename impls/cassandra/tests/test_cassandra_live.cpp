// The Cassandra and ScyllaDB sinks against a live server. Skipped unless
// CLINK_CASSANDRA_TEST_CONTACT_POINTS names one (scripts/cassandra-live.sh sets
// it, with the port, the product and its version). The tests create their own
// tables, drive the production sinks, and read back whole rows with a session
// of their own; faults on the wire come from a TCP proxy between sink and server.
//
// What they hold the sinks to:
//   - the server is the product and version the gate started;
//   - every row of a long input arrives through a small window of writes in
//     flight, value for value;
//   - repeated writes to one row in one interval end at the last record;
//   - inserts and deletes of one row across early write-outs end at the last
//     record, deletes binding the key as the insert wrote it, composite keys with
//     Unicode, control characters and integers past 2^53 included;
//   - the table's key is checked at open and each record's key before a write;
//   - a refused write, Unavailable, a timed-out write the server applied, and a
//     lost connection each fail the sink, which then refuses the next barrier;
//   - a cancel ends a wait at once, and close returns within the request timeout;
//   - a failed sink reopens and writes again;
//   - a decimal kept exactly when sent as a string.

#include <atomic>
#include <chrono>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/cassandra/cassandra_upsert_sink.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "cassandra_live.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::cassandra::CassandraConsistency;
using clink::cassandra::CassandraSink;
using clink::cassandra::CassandraUpsertSink;
namespace live = clink::cassandra::live;

namespace {

using namespace std::chrono_literals;

#define REQUIRE_LIVE()                                                             \
    do {                                                                           \
        if (!live::configured()) {                                                 \
            GTEST_SKIP() << "set CLINK_CASSANDRA_TEST_CONTACT_POINTS (and _PORT)"; \
        }                                                                          \
    } while (0)

template <typename F>
std::string error_of(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

std::string table_name(const char* base) {
    return std::string{base} + "_" + std::to_string(static_cast<long>(::getpid()));
}

std::string fq(const std::string& table) {
    return std::string{live::kKeyspace} + "." + table;
}

Batch<std::string> batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

CheckpointBarrier barrier(std::uint64_t id) {
    return CheckpointBarrier{CheckpointId{id}};
}

CassandraSink::Options plain(const std::string& table) {
    CassandraSink::Options o;
    o.conn = live::conn();
    o.keyspace = live::kKeyspace;
    o.table = table;
    return o;
}

CassandraUpsertSink::Options upsert(const std::string& table, std::vector<std::string> key) {
    CassandraUpsertSink::Options o;
    o.conn = live::conn();
    o.keyspace = live::kKeyspace;
    o.table = table;
    o.key_columns = std::move(key);
    return o;
}

// A sink attached to a runtime context whose cancel signal the case controls.
template <typename S>
struct Attached {
    explicit Attached(S& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{9043}, "cassandra_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    S& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

// Every row's (id, v) from SELECT JSON, as "id=v", sorted.
std::multiset<std::string> id_v(live::Session& s, const std::string& table) {
    std::multiset<std::string> out;
    for (const auto& r : s.rows(fq(table))) {
        const auto& o = r.as_object();
        const auto v = o.find("v");
        out.insert(
            o.at("id").serialize(0) + "=" +
            (v == o.end() || v->second.is_null() ? std::string{"<null>"} : v->second.as_string()));
    }
    return out;
}

}  // namespace

TEST(CassandraLive, TheServerIsTheProductAndVersionTheGateStarted) {
    REQUIRE_LIVE();
    const std::string want = live::env_or("CLINK_CASSANDRA_TEST_VERSION");
    if (want.empty()) {
        GTEST_SKIP() << "set CLINK_CASSANDRA_TEST_VERSION to the version the server should be";
    }
    live::Session s;
    std::string got;
    if (live::scylladb()) {
        const auto v = s.column("SELECT version FROM system.versions WHERE key = 'local'");
        ASSERT_EQ(v.size(), 1u) << "no system.versions row: not ScyllaDB";
        got = v[0].value_or("");
    } else {
        const auto v = s.column("SELECT release_version FROM system.local");
        ASSERT_EQ(v.size(), 1u);
        got = v[0].value_or("");
        // ScyllaDB answers release_version too, with the Cassandra version it
        // speaks for; system.versions tells the two apart.
        EXPECT_THROW((void)s.column("SELECT version FROM system.versions"), std::runtime_error)
            << "the server has system.versions: it is ScyllaDB, not Cassandra";
    }
    EXPECT_EQ(got.rfind(want, 0), 0u)
        << live::product() << " reports version " << got << ", the gate started " << want;
    std::printf("[cassandra-live] product=%s version=%s\n", live::product().c_str(), got.c_str());
}

TEST(CassandraLive, JsonInsertThenReadBack) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("plain");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    CassandraSink sink(plain(table));
    sink.open();
    sink.on_data(batch({R"({"id":1,"v":"a"})", R"({"id":2,"v":"b"})", R"({"ID":3,"V":"c"})"}));
    sink.flush();
    sink.close();
    EXPECT_EQ(id_v(s, table), (std::multiset<std::string>{"1=a", "2=b", "3=c"}));
}

TEST(CassandraLive, EveryRowArrivesThroughASmallWindow) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("window");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    auto o = plain(table);
    o.max_in_flight = 8;
    o.max_in_flight_bytes = 4096;
    CassandraSink sink(std::move(o));
    sink.open();
    std::multiset<std::string> want;
    for (int b = 0; b < 10; ++b) {
        std::vector<std::string> recs;
        for (int i = 0; i < 500; ++i) {
            const int id = b * 500 + i;
            const std::string v = "value-" + std::to_string(id) +
                                  std::string(static_cast<std::size_t>(id % 300), 'x');
            recs.push_back("{\"id\":" + std::to_string(id) + ",\"v\":\"" + v + "\"}");
            want.insert(std::to_string(id) + "=" + v);
        }
        sink.on_data(batch(recs));
        EXPECT_LE(sink.in_flight(), 8u);
        sink.on_barrier(barrier(static_cast<std::uint64_t>(b + 1)));
        EXPECT_EQ(sink.in_flight(), 0u);
    }
    sink.close();
    EXPECT_EQ(id_v(s, table), want);
}

TEST(CassandraLive, RepeatedWritesToOneRowEndAtTheLastRecord) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("repeat");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    auto o = plain(table);
    o.max_in_flight = 64;
    CassandraSink sink(std::move(o));
    sink.open();
    for (int round = 0; round < 5; ++round) {
        std::vector<std::string> recs;
        for (int i = 0; i < 400; ++i) {
            recs.push_back("{\"id\":" + std::to_string(i % 4) + ",\"v\":\"r" +
                           std::to_string(round) + "-" + std::to_string(i) + "\"}");
        }
        sink.on_data(batch(recs));
        sink.on_barrier(barrier(static_cast<std::uint64_t>(round + 1)));
        EXPECT_EQ(id_v(s, table),
                  (std::multiset<std::string>{"0=r" + std::to_string(round) + "-396",
                                              "1=r" + std::to_string(round) + "-397",
                                              "2=r" + std::to_string(round) + "-398",
                                              "3=r" + std::to_string(round) + "-399"}))
            << "round " << round;
    }
    sink.close();
}

TEST(CassandraLive, InsertsAndDeletesAcrossEarlyWriteOutsEndAtTheLastRecord) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("changelog");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    auto o = upsert(table, {"id"});
    o.max_buffered_records = 1;  // every record its own write-out
    o.max_in_flight = 16;
    CassandraUpsertSink sink(std::move(o));
    sink.open();
    const auto rec = [](int id, const std::string& v, const char* kind) {
        return "{\"id\":" + std::to_string(id) + ",\"v\":\"" + v + "\",\"__row_kind\":\"" + kind +
               "\"}";
    };
    std::vector<std::string> recs;
    std::multiset<std::string> want;
    for (int id = 0; id < 50; ++id) {
        recs.push_back(rec(id, "a", "insert"));
        recs.push_back(rec(id, "a", "update_before"));
        recs.push_back(rec(id, "b", "update_after"));
        if (id % 2 == 0) {
            recs.push_back(rec(id, "b", "delete"));  // deleted last
        } else {
            recs.push_back(rec(id, "b", "delete"));
            recs.push_back(rec(id, "c", "insert"));  // deleted, then back
            want.insert(std::to_string(id) + "=c");
        }
    }
    sink.on_data(batch(recs));
    sink.on_barrier(barrier(1));
    sink.close();
    EXPECT_EQ(id_v(s, table), want);
}

TEST(CassandraLive, CompositeKeysWithUnicodeControlCharactersAndWideIntegers) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("composite");
    live::fresh_table(s, table, "tenant text, seq bigint, v text, PRIMARY KEY ((tenant), seq)");
    CassandraUpsertSink sink(upsert(table, {"tenant", "seq"}));
    sink.open();
    // Tenants: e-acute composed and decomposed (two keys), a NUL, a newline, a
    // quote and an emoji, written as JSON escapes. Built at run time so no tool
    // turns the escapes into characters on the way to the compiler.
    const std::string u = std::string{"\\"} + "u";
    const std::vector<std::string> tenants = {"caf" + u + "00e9",
                                              "cafe" + u + "0301",
                                              "a" + u + "0000b",
                                              "line" + u + "000abreak",
                                              "q" + u + "0022q",
                                              u + "d83d" + u + "de00"};
    const std::vector<std::string> seqs = {"1", "9007199254740993", "-9223372036854775808"};
    std::vector<std::string> recs;
    for (const auto& t : tenants) {
        for (const auto& q : seqs) {
            recs.push_back("{\"tenant\":\"" + t + "\",\"seq\":" + q + ",\"v\":\"" + t + "/" + q +
                           "\"}");
        }
    }
    sink.on_data(batch(recs));
    sink.on_barrier(barrier(1));
    EXPECT_EQ(s.rows(fq(table)).size(), tenants.size() * seqs.size());
    // Delete each tenant's 2^53 + 1 row: exactly those go, by the key as written.
    std::vector<std::string> dels;
    for (const auto& t : tenants) {
        dels.push_back("{\"tenant\":\"" + t +
                       "\",\"seq\":9007199254740993,\"__row_kind\":\"delete\"}");
    }
    sink.on_data(batch(dels));
    sink.on_barrier(barrier(2));
    sink.close();
    const auto rows = s.rows(fq(table));
    EXPECT_EQ(rows.size(), tenants.size() * 2);
    std::set<std::string> left;
    for (const auto& r : rows) {
        const auto& o = r.as_object();
        EXPECT_NE(o.at("seq").serialize(0), "9007199254740993");
        // Each row's v names its own key, so a row under the wrong key shows.
        const std::string key = o.at("tenant").as_string() + "/" + o.at("seq").serialize(0);
        EXPECT_EQ(o.at("v").as_string(), key);
        left.insert(key);
    }
    EXPECT_EQ(left.size(), tenants.size() * 2);  // the composed and decomposed forms stay apart
}

TEST(CassandraLive, TheTablesKeyIsCheckedAtOpenAndEachRecordsKeyBeforeAWrite) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("keycheck");
    live::fresh_table(s, table, "tenant text, seq bigint, v text, PRIMARY KEY ((tenant), seq)");
    {
        CassandraUpsertSink sink(upsert(table, {"tenant"}));
        const auto why = error_of([&] { sink.open(); });
        EXPECT_NE(why.find("partition key (tenant) and clustering columns (seq)"),
                  std::string::npos)
            << why;
    }
    {
        CassandraUpsertSink sink(upsert("no_such_table", {"id"}));
        EXPECT_NE(error_of([&] { sink.open(); }).find("does not exist"), std::string::npos);
    }
    CassandraUpsertSink sink(upsert(table, {"tenant", "seq"}));
    sink.open();
    const auto why = error_of([&] {
        sink.on_data(
            batch({R"({"tenant":"t","seq":1,"v":"a"})", R"({"tenant":"t","seq":"2","v":"b"})"}));
    });
    EXPECT_NE(why.find("'seq' is bigint"), std::string::npos) << why;
    EXPECT_TRUE(s.rows(fq(table)).empty());  // nothing of the batch was written
}

TEST(CassandraLive, ARefusedWriteFailsTheSinkAndTheNextBarrier) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("refused");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    CassandraSink sink(plain(table));
    sink.open();
    const auto why = error_of([&] {
        sink.on_data(batch({R"({"id":1,"v":"ok"})", R"({"id":2,"no_such_column":"secret-x"})"}));
        sink.on_barrier(barrier(1));
    });
    EXPECT_NE(why.find("a write was not acknowledged"), std::string::npos) << why;
    EXPECT_NE(why.find("would again"), std::string::npos) << why;  // permanent
    EXPECT_EQ(why.find("secret-x"), std::string::npos) << why;
    EXPECT_TRUE(sink.failed());
    EXPECT_NE(error_of([&] { sink.on_barrier(barrier(2)); }).find("failed earlier"),
              std::string::npos);
    // Reopened, the sink writes again.
    sink.open();
    sink.on_data(batch({R"({"id":3,"v":"after"})"}));
    sink.on_barrier(barrier(3));
    sink.close();
    EXPECT_EQ(id_v(s, table).count("3=after"), 1u);
}

TEST(CassandraLive, UnavailableReplicasFailTheWriteAtTheConsistencyAsked) {
    REQUIRE_LIVE();
    live::Session s;
    const std::string ks = "clink_it_rf3";
    // Three replicas asked for on a one-node cluster: QUORUM needs two.
    const auto made =
        s.exec("CREATE KEYSPACE IF NOT EXISTS " + ks +
               " WITH replication = {'class':'SimpleStrategy','replication_factor':3}");
    if (!made.empty()) {
        GTEST_SKIP() << live::product()
                     << " refuses a keyspace with more replicas than nodes: " << made;
    }
    const auto table = table_name("rf3");
    s.must("DROP TABLE IF EXISTS " + ks + "." + table);
    s.must("CREATE TABLE " + ks + "." + table + " (id int PRIMARY KEY, v text)");
    auto o = plain(table);
    o.keyspace = ks;
    o.conn.consistency = CassandraConsistency::Quorum;
    CassandraSink sink(std::move(o));
    sink.open();
    const auto why = error_of([&] {
        sink.on_data(batch({R"({"id":1,"v":"a"})"}));
        sink.on_barrier(barrier(1));
    });
    // The coordinator answers Unavailable; the driver's default retry policy
    // takes that to the next host, and with none left reports No hosts
    // available. Either way nothing was written.
    EXPECT_TRUE(why.find("Unavailable") != std::string::npos ||
                why.find("No hosts available") != std::string::npos)
        << why;
    EXPECT_NE(why.find("no replica wrote it"), std::string::npos) << why;
    EXPECT_NE(error_of([&] { sink.on_barrier(barrier(2)); }).find("failed earlier"),
              std::string::npos);
    // At ONE the same table takes the write.
    auto one = plain(table);
    one.keyspace = ks;
    one.conn.consistency = CassandraConsistency::One;
    CassandraSink ok(std::move(one));
    ok.open();
    ok.on_data(batch({R"({"id":1,"v":"a"})"}));
    EXPECT_NO_THROW(ok.on_barrier(barrier(1)));
    ok.close();
}

TEST(CassandraLive, AWriteTheServerAppliedButNeverAnsweredIsAmbiguous) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("held");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(live::port()));
    proxy.set_request_marker("hold-this-write");
    auto o = plain(table);
    o.conn.contact_points = "127.0.0.1";
    o.conn.port = proxy.port();
    o.conn.request_timeout_ms = 1500;
    CassandraSink sink(std::move(o));
    sink.open();
    proxy.hold_at_upload(1);
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] {
        sink.on_data(batch({R"({"id":7,"v":"hold-this-write"})"}));
        sink.on_barrier(barrier(1));
    });
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 10s);
    EXPECT_NE(why.find("may or may not have been applied"), std::string::npos) << why;
    EXPECT_NE(error_of([&] { sink.on_barrier(barrier(2)); }).find("failed earlier"),
              std::string::npos);
    proxy.drop_held();
    sink.close();
    // The server did apply it: the outcome the sink could not learn. A replay
    // rewrites the same row.
    EXPECT_EQ(id_v(s, table), (std::multiset<std::string>{"7=hold-this-write"}));
}

TEST(CassandraLive, ALostConnectionFailsTheSink) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("lost");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(live::port()));
    auto o = plain(table);
    o.conn.contact_points = "127.0.0.1";
    o.conn.port = proxy.port();
    o.conn.request_timeout_ms = 2000;
    CassandraSink sink(std::move(o));
    sink.open();
    proxy.set_blackhole(true);
    std::vector<std::string> recs;
    for (int i = 0; i < 20; ++i) {
        recs.push_back("{\"id\":" + std::to_string(i) + ",\"v\":\"x\"}");
    }
    std::thread dropper([&] {
        std::this_thread::sleep_for(300ms);
        proxy.drop_all();
    });
    const auto why = error_of([&] {
        sink.on_data(batch(recs));
        sink.on_barrier(barrier(1));
    });
    dropper.join();
    EXPECT_NE(why.find("a write was not acknowledged"), std::string::npos) << why;
    EXPECT_TRUE(sink.failed());
    EXPECT_NE(error_of([&] { sink.on_barrier(barrier(2)); }).find("failed earlier"),
              std::string::npos);
    proxy.set_blackhole(false);
    sink.close();
}

TEST(CassandraLive, ACancelEndsTheWaitAndCloseReturnsWithinTheRequestTimeout) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("cancel");
    live::fresh_table(s, table, "id int PRIMARY KEY, v text");
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(live::port()));
    proxy.set_request_marker("hold-this-write");
    auto o = upsert(table, {"id"});
    o.conn.contact_points = "127.0.0.1";
    o.conn.port = proxy.port();
    o.conn.request_timeout_ms = 3000;
    CassandraUpsertSink sink(std::move(o));
    Attached<CassandraUpsertSink> a(sink);
    sink.open();
    proxy.hold_at_upload(1);
    sink.on_data(batch({R"({"id":1,"v":"hold-this-write"})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(200ms);
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { sink.on_barrier(barrier(1)); });
    const auto waited = std::chrono::steady_clock::now() - t0;
    canceller.join();
    EXPECT_NE(why.find("cancelled while waiting for the writes in flight"), std::string::npos)
        << why;
    EXPECT_LT(waited, 1500ms);
    const auto c0 = std::chrono::steady_clock::now();
    sink.close();
    EXPECT_LT(std::chrono::steady_clock::now() - c0, 3000ms + 3s);
    proxy.drop_held();
}

TEST(CassandraLive, ADecimalSentAsAStringIsKeptExactly) {
    REQUIRE_LIVE();
    live::Session s;
    const auto table = table_name("decimal");
    live::fresh_table(s, table, "id int PRIMARY KEY, d decimal");
    CassandraSink sink(plain(table));
    sink.open();
    sink.on_data(batch({R"({"id":1,"d":"12345678901234567890.123456789"})",
                        R"({"id":2,"d":12345678901234567890.123456789})"}));
    sink.flush();
    sink.close();
    const auto as_string = s.column("SELECT CAST(d AS text) FROM " + fq(table) + " WHERE id = 1");
    ASSERT_EQ(as_string.size(), 1u);
    EXPECT_EQ(as_string[0].value_or(""), "12345678901234567890.123456789");
    // Sent as a JSON number, the server reads the value as it reads JSON numbers;
    // the sink sent the digits as written. Recorded, not asserted.
    const auto as_number = s.column("SELECT CAST(d AS text) FROM " + fq(table) + " WHERE id = 2");
    std::printf(
        "[cassandra-live] %s stores the JSON number 12345678901234567890.123456789 in a "
        "decimal column as %s\n",
        live::product().c_str(),
        as_number.empty() ? "?" : as_number[0].value_or("null").c_str());
}
