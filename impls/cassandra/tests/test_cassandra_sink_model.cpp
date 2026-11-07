// The Cassandra sinks through their production code against a model of the
// server (cassandra_model.hpp): options, the table check at open, the bounds on
// buffered and in-flight writes, write order and timestamps, the failed state,
// cancellation, close and reopen, and what reaches error messages and metrics.

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_sink.hpp"
#include "clink/cassandra/cassandra_upsert_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "cassandra_model.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::cassandra::CassandraConsistency;
using clink::cassandra::CassandraFault;
using clink::cassandra::CassandraSink;
using clink::cassandra::CassandraUpsertSink;
using clink::cassandra::CassandraWrite;
using clink::cassandra::testing::CassandraModel;
using clink::cassandra::testing::outcome;

namespace {

using namespace std::chrono_literals;

// A JSON \u escape, built at run time so that no tool on the way to the
// compiler can turn the escape into the character it names.
std::string esc(const char* hex) {
    return std::string{"\\"} + "u" + hex;
}

template <typename F>
std::string error_of(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

Batch<std::string> batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(r);
    }
    return b;
}

std::string row(int id, const std::string& v) {
    return "{\"id\":" + std::to_string(id) + ",\"v\":\"" + v + "\"}";
}

std::string change(int id, const std::string& v, const char* kind) {
    return "{\"id\":" + std::to_string(id) + ",\"v\":\"" + v + "\",\"__row_kind\":\"" + kind +
           "\"}";
}

CassandraSink::Options plain_options(const std::shared_ptr<CassandraModel>& m) {
    CassandraSink::Options o;
    o.keyspace = "ks";
    o.table = "t";
    o.service = m;
    return o;
}

CassandraUpsertSink::Options upsert_options(const std::shared_ptr<CassandraModel>& m) {
    CassandraUpsertSink::Options o;
    o.keyspace = "ks";
    o.table = "t";
    o.key_columns = {"id"};
    o.service = m;
    return o;
}

// A sink attached to a runtime context whose cancel signal the case controls.
template <typename S>
struct Attached {
    explicit Attached(S& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{9042}, "cassandra_sink", nullptr, nullptr);
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

std::uint64_t counter(const char* metric) {
    return clink::MetricsRegistry::global()
        .counter(clink::metrics::connector_metric_name(metric, "cassandra", "sink"))
        .value();
}

CheckpointBarrier barrier(std::uint64_t id) {
    return CheckpointBarrier{CheckpointId{id}};
}

// Waits up to two seconds for `pred`.
template <typename P>
bool eventually(P pred) {
    for (int i = 0; i < 200; ++i) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

}  // namespace

// ---- options ------------------------------------------------------------------------

TEST(CassandraSinkOptions, TheConstructorRefusesWhatTheServerCouldNotTake) {
    const auto m = std::make_shared<CassandraModel>();
    const auto refused = [&m](auto mutate) {
        auto o = plain_options(m);
        mutate(o);
        return error_of([&o] { CassandraSink s(std::move(o)); });
    };
    using O = CassandraSink::Options;
    EXPECT_NE(refused([](O& o) { o.keyspace.clear(); }).find("'keyspace' is required"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.table = "my-table"; }).find("not a name the sink accepts"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.table = std::string(49, 't'); }).find("longer than 48"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.conn.port = 0; }).find("'port' must be 1 to 65535"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.conn.request_timeout_ms = 0; }).find("'request_timeout_ms'"),
              std::string::npos);
    EXPECT_NE(
        refused([](O& o) { o.conn.connect_timeout_ms = 600001; }).find("'connect_timeout_ms'"),
        std::string::npos);
    EXPECT_NE(refused([](O& o) { o.conn.username = "u"; }).find("go together"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.max_in_flight = 0; }).find("'max_in_flight' must be 1 to 8192"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.max_in_flight = 8193; }).find("'max_in_flight'"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.max_in_flight_bytes = 1023; }).find("'max_in_flight_bytes'"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.conn.contact_points = " , "; }).find("names no host"),
              std::string::npos);
    EXPECT_EQ(refused([](O& o) { o.table = "Sensor_Out"; }), "");
    EXPECT_EQ(m->connects(), 0);  // nothing touched the server
}

TEST(CassandraSinkOptions, TheUpsertSinkNeedsItsKeyAndBufferBounds) {
    const auto m = std::make_shared<CassandraModel>();
    const auto refused = [&m](auto mutate) {
        auto o = upsert_options(m);
        mutate(o);
        return error_of([&o] { CassandraUpsertSink s(std::move(o)); });
    };
    using O = CassandraUpsertSink::Options;
    EXPECT_NE(refused([](O& o) { o.key_columns.clear(); }).find("'key_columns'"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.key_columns = {"id", "a b"}; }).find("not a name"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.max_buffered_records = 0; }).find("'max_buffered_records'"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.max_buffered_bytes = 10; }).find("'max_buffered_bytes'"),
              std::string::npos);
}

TEST(CassandraSinkOptions, TheFactoriesReadNumbersWholeAndCheckTheirRanges) {
    const auto& reg = clink::cluster::OperatorRegistry::default_instance();
    const auto* plain = reg.find_sink("cassandra_sink_string", std::string{"string"});
    const auto* upsert = reg.find_sink("cassandra_upsert_sink_string", std::string{"string"});
    ASSERT_NE(plain, nullptr);
    ASSERT_NE(upsert, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"keyspace", "ks"}, {"table", "t"}};
    EXPECT_NO_THROW((void)plain->build(ctx));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"port", "9042x", "'port' must be an integer from 1 to 65535; it is '9042x'"},
             {"port", "", "'port' must be an integer"},
             {"max_in_flight", "abc", "'max_in_flight' must be an integer from 1 to 8192"},
             {"max_in_flight", "8193", "'max_in_flight' must be an integer from 1 to 8192"},
             {"max_in_flight_bytes", "1023", "'max_in_flight_bytes' must be an integer"},
             {"request_timeout_ms", "0", "'request_timeout_ms' must be an integer from 1"},
             {"connect_timeout_ms", "-5", "'connect_timeout_ms' must be an integer from 1"},
             {"consistency", "any", "ANY acknowledges a write no replica need hold yet"},
             {"consistency", "LOCAL_SERIAL", "serial levels govern lightweight transactions"},
             {"consistency", "quorom", "it is not a consistency level"},
             {"table", "bad-name", "not a name the sink accepts"},
             {"username", "cassandra", "go together"}}) {
        auto c = ctx;
        c.params[key] = value;
        const auto why = error_of([&] { (void)plain->build(c); });
        EXPECT_NE(why.find(rule), std::string::npos) << key << "=" << value << ": " << why;
    }
    auto c = ctx;
    c.params["consistency"] = "LOCAL_QUORUM";
    EXPECT_NO_THROW((void)plain->build(c));

    EXPECT_NE(error_of([&] { (void)upsert->build(ctx); }).find("'key_columns'"), std::string::npos);
    auto u = ctx;
    u.params["key_columns"] = "tenant, seq";
    EXPECT_NO_THROW((void)upsert->build(u));
    u.params["max_buffered_records"] = "1000001";
    EXPECT_NE(error_of([&] { (void)upsert->build(u); }).find("'max_buffered_records'"),
              std::string::npos);
}

// ---- open ---------------------------------------------------------------------------

TEST(CassandraSinkOpen, ItConnectsWithTheGivenSettingsAndQuotesTheNames) {
    const auto m = std::make_shared<CassandraModel>();
    auto o = upsert_options(m);
    o.keyspace = "Clink_Demo";
    o.table = "Sensor_Out";
    o.conn.consistency = CassandraConsistency::LocalQuorum;
    o.conn.request_timeout_ms = 3000;
    m->set_key({{"tenant", "text"}}, {{"seq", "bigint"}});
    o.key_columns = {"SEQ", "Tenant"};
    CassandraUpsertSink s(std::move(o));
    s.open();
    EXPECT_EQ(m->insert_cql(), R"(INSERT INTO "clink_demo"."sensor_out" JSON ?)");
    EXPECT_EQ(m->delete_cql(),
              R"(DELETE FROM "clink_demo"."sensor_out" WHERE "tenant" = fromJson(?) AND )"
              R"("seq" = fromJson(?))");
    EXPECT_EQ(m->last_conn().consistency, CassandraConsistency::LocalQuorum);
    EXPECT_EQ(m->last_conn().request_timeout_ms, 3000);
    s.close();
}

TEST(CassandraSinkOpen, AMissingKeyspaceOrTableIsRefusedAndTheSessionClosed) {
    const auto m = std::make_shared<CassandraModel>();
    m->set_missing(/*keyspace=*/true, /*table=*/false);
    CassandraSink a(plain_options(m));
    EXPECT_NE(error_of([&] { a.open(); }).find("keyspace 'ks' does not exist"), std::string::npos);
    EXPECT_EQ(m->closes(), 1);
    m->set_missing(false, true);
    CassandraSink b(plain_options(m));
    EXPECT_NE(error_of([&] { b.open(); }).find("table 'ks.t' does not exist"), std::string::npos);
    EXPECT_NE(error_of([&] { b.on_data(batch({row(1, "a")})); }).find("not open"),
              std::string::npos);
    EXPECT_NO_THROW(b.close());
}

TEST(CassandraSinkOpen, KeyColumnsMustBeTheWholePrimaryKey) {
    const auto m = std::make_shared<CassandraModel>();
    m->set_key({{"tenant", "text"}, {"day", "date"}}, {{"seq", "bigint"}});
    for (const auto& [given, rule] : std::vector<std::pair<std::vector<std::string>, std::string>>{
             {{"tenant", "day"}, "must name the table's whole primary key"},
             {{"tenant", "day", "seq", "v"}, "must name the table's whole primary key"},
             {{"tenant", "seq"}, "partition key (tenant, day) and clustering columns (seq)"},
             {{"tenant", "day", "seq", "Seq"}, "names 'seq' twice"}}) {
        auto o = upsert_options(m);
        o.key_columns = given;
        CassandraUpsertSink s(std::move(o));
        const auto why = error_of([&] { s.open(); });
        EXPECT_NE(why.find(rule), std::string::npos) << why;
    }
    auto o = upsert_options(m);
    o.key_columns = {"seq", "day", "tenant"};  // any order
    CassandraUpsertSink s(std::move(o));
    EXPECT_NO_THROW(s.open());
}

TEST(CassandraSinkOpen, KeyColumnsOfUnsupportedTypesAreRefused) {
    const auto m = std::make_shared<CassandraModel>();
    m->set_key({{"id", "int"}}, {{"tags", "set"}});
    auto o = upsert_options(m);
    o.key_columns = {"id", "tags"};
    CassandraUpsertSink s(std::move(o));
    EXPECT_NE(error_of([&] { s.open(); }).find("'tags' is of type set"), std::string::npos);
}

TEST(CassandraSinkOpen, ConnectAndPrepareFailuresLeaveNothingOpen) {
    const auto m = std::make_shared<CassandraModel>();
    auto o = plain_options(m);
    o.conn.username = "app";
    o.conn.password = "hunter2-secret";
    m->fail_connect(outcome(CassandraFault::Permanent,
                            "Bad credentials",
                            "Provided username app and/or password are incorrect"));
    CassandraSink s(o);
    const auto why = error_of([&] { s.open(); });
    EXPECT_NE(why.find("cannot connect to 127.0.0.1 port 9042"), std::string::npos) << why;
    EXPECT_EQ(why.find("hunter2-secret"), std::string::npos);
    EXPECT_EQ(m->closes(), 1);
    m->fail_connect({});
    m->fail_prepare(outcome(CassandraFault::Permanent, "Invalid query", "unconfigured table t"));
    EXPECT_NE(error_of([&] { s.open(); }).find("cannot prepare"), std::string::npos);
    EXPECT_EQ(m->closes(), 2);
    m->fail_prepare({});
    EXPECT_NO_THROW(s.open());
    EXPECT_EQ(m->closes(), 2);  // nothing left open by the failed attempts
    EXPECT_NE(error_of([&] { s.open(); }).find("already open"), std::string::npos);
}

// ---- bounds -------------------------------------------------------------------------

TEST(CassandraSinkBounds, WritesInFlightNeverExceedTheBound) {
    const auto m = std::make_shared<CassandraModel>();
    auto o = plain_options(m);
    o.max_in_flight = 8;
    CassandraSink s(std::move(o));
    s.open();
    std::vector<std::string> recs;
    for (int i = 0; i < 1000; ++i) {
        recs.push_back(row(i, "v" + std::to_string(i)));
    }
    s.on_data(batch(recs));
    s.on_barrier(barrier(1));
    EXPECT_LE(m->max_outstanding(), 8u);
    EXPECT_EQ(m->visible_rows(), 1000u);
    EXPECT_EQ(s.in_flight(), 0u);
}

TEST(CassandraSinkBounds, AFullWindowHoldsTheInputBackUntilAWriteIsAcknowledged) {
    const auto m = std::make_shared<CassandraModel>();
    m->hold_all(true);
    auto o = plain_options(m);
    o.max_in_flight = 4;
    CassandraSink s(std::move(o));
    s.open();
    std::atomic<bool> returned{false};
    std::thread t([&] {
        s.on_data(
            batch({row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d"), row(5, "e"), row(6, "f")}));
        returned = true;
    });
    ASSERT_TRUE(eventually([&] { return m->outstanding() == 4; }));
    std::this_thread::sleep_for(100ms);
    EXPECT_FALSE(returned.load());  // the fifth record waits for room
    EXPECT_EQ(m->submitted_count(), 4u);
    while (!returned.load()) {
        m->release_held();
        std::this_thread::sleep_for(5ms);
    }
    t.join();
    m->release_held();
    s.flush();
    EXPECT_EQ(m->visible_rows(), 6u);
    EXPECT_LE(m->max_outstanding(), 4u);
}

TEST(CassandraSinkBounds, TheByteBoundCountsRecordTextAndALargeRecordGoesAlone) {
    const auto m = std::make_shared<CassandraModel>();
    m->hold_all(true);
    auto o = plain_options(m);
    o.max_in_flight_bytes = 1024;
    CassandraSink s(std::move(o));
    s.open();
    const std::string pad(280, 'x');  // each record about 300 bytes
    std::thread releaser([&] {
        for (int i = 0; i < 400; ++i) {
            m->release_held();
            std::this_thread::sleep_for(2ms);
        }
    });
    s.on_data(batch({row(1, pad),
                     row(2, pad),
                     row(3, pad),
                     row(4, pad),
                     row(5, std::string(4000, 'y')),
                     row(6, pad)}));
    s.flush();
    releaser.join();
    const auto before = m->outstanding_at_submit();
    ASSERT_EQ(before.size(), 6u);
    for (std::size_t i = 0; i < before.size(); ++i) {
        EXPECT_LE(before[i], 2u) << "write " << i;  // at most three 300-byte writes in 1 KiB
    }
    EXPECT_EQ(before[4], 0u);  // the 4 KB record went alone
    EXPECT_EQ(m->visible_rows(), 6u);
}

TEST(CassandraSinkBounds, TheUpsertBufferWritesOutEarlyAtItsRowBound) {
    const auto m = std::make_shared<CassandraModel>();
    auto o = upsert_options(m);
    o.max_buffered_records = 3;
    CassandraUpsertSink s(std::move(o));
    s.open();
    for (int i = 0; i < 10; ++i) {
        s.on_data(batch({row(i, "v")}));
        EXPECT_LE(s.buffered_rows(), 3u);
    }
    EXPECT_GE(m->submitted_count(), 9u);  // written before any barrier
    s.on_barrier(barrier(1));
    EXPECT_EQ(m->visible_rows(), 10u);
    EXPECT_EQ(s.buffered_rows(), 0u);
}

TEST(CassandraSinkBounds, TheUpsertBufferWritesOutEarlyAtItsByteBound) {
    const auto m = std::make_shared<CassandraModel>();
    auto o = upsert_options(m);
    o.max_buffered_bytes = 1024;
    CassandraUpsertSink s(std::move(o));
    s.open();
    const std::string pad(380, 'x');
    s.on_data(batch({row(1, pad), row(2, pad)}));
    EXPECT_EQ(m->submitted_count(), 0u);
    s.on_data(batch({row(3, pad)}));  // would pass 1 KiB: the first two go out
    EXPECT_EQ(m->submitted_count(), 2u);
    EXPECT_EQ(s.buffered_rows(), 1u);
    s.on_data(batch({row(4, std::string(3000, 'y'))}));  // larger than the bound
    EXPECT_EQ(m->submitted_count(), 4u);
    EXPECT_EQ(s.buffered_rows(), 0u);
    s.flush();
    EXPECT_EQ(m->visible_rows(), 4u);
}

// ---- order and identity -------------------------------------------------------------

TEST(CassandraSinkOrder, TheLastRecordForARowWinsWhateverOrderTheServerAppliesThem) {
    const auto m = std::make_shared<CassandraModel>();
    m->hold_all(true);
    CassandraSink s(plain_options(m));
    s.open();
    s.on_data(batch({row(1, "a"), row(1, "b"), row(1, "c"), row(1, "d"), row(1, "e")}));
    m->release_held(/*reversed=*/true);
    s.flush();
    EXPECT_EQ(m->row("1"), row(1, "e"));
    const auto w = m->submitted();
    for (std::size_t i = 1; i < w.size(); ++i) {
        EXPECT_GT(w[i].timestamp_us, w[i - 1].timestamp_us);
    }
}

TEST(CassandraSinkOrder, TimestampsKeepRisingAcrossBarriers) {
    const auto m = std::make_shared<CassandraModel>();
    CassandraSink s(plain_options(m));
    s.open();
    for (int b = 1; b <= 50; ++b) {
        s.on_data(batch({row(1, "v" + std::to_string(b))}));
        s.on_barrier(barrier(static_cast<std::uint64_t>(b)));
    }
    const auto w = m->submitted();
    for (std::size_t i = 1; i < w.size(); ++i) {
        ASSERT_GT(w[i].timestamp_us, w[i - 1].timestamp_us);
    }
    EXPECT_EQ(m->row("1"), row(1, "v50"));
}

TEST(CassandraSinkOrder, ChangesToOneRowNetWithinAnInterval) {
    const auto m = std::make_shared<CassandraModel>();
    CassandraUpsertSink s(upsert_options(m));
    s.open();
    s.on_data(batch({change(1, "a", "insert"),
                     change(1, "a", "delete"),
                     change(2, "x", "delete"),
                     change(2, "y", "insert"),
                     change(3, "p", "update_before"),
                     change(3, "q", "update_after")}));
    s.flush();
    const auto w = m->submitted();
    ASSERT_EQ(w.size(), 3u);
    EXPECT_EQ(w[0].kind, CassandraWrite::Kind::Delete);  // row 1's last record, the second
    EXPECT_EQ(w[1].kind, CassandraWrite::Kind::Insert);  // row 2
    EXPECT_EQ(w[2].kind, CassandraWrite::Kind::Insert);  // row 3
    EXPECT_FALSE(m->row("1"));
    EXPECT_EQ(m->row("2"), row(2, "y"));
    EXPECT_EQ(m->row("3"), row(3, "q"));
}

TEST(CassandraSinkOrder, InsertDeleteInsertAcrossEarlyWriteOutsEndsAtTheLastRecord) {
    for (const bool reversed : {false, true}) {
        const auto m = std::make_shared<CassandraModel>();
        m->hold_all(true);
        auto o = upsert_options(m);
        o.max_buffered_records = 1;
        CassandraUpsertSink s(std::move(o));
        s.open();
        s.on_data(batch({change(1, "a", "insert"), change(2, "b", "insert")}));
        s.on_data(batch({change(1, "a", "delete"), change(2, "b", "delete")}));
        s.on_data(batch({change(1, "c", "insert")}));
        std::thread t([&] {
            std::this_thread::sleep_for(50ms);
            m->release_held(reversed);
        });
        s.flush();
        t.join();
        EXPECT_EQ(m->row("1"), row(1, "c")) << "reversed=" << reversed;
        EXPECT_FALSE(m->row("2")) << "reversed=" << reversed;
        EXPECT_EQ(m->submitted_count(), 5u);
    }
}

TEST(CassandraSinkOrder, TwoSpellingsOfOneKeyAreWrittenInRecordOrder) {
    const auto m = std::make_shared<CassandraModel>();
    m->set_key({{"k", "text"}}, {});
    m->hold_all(true);
    auto o = upsert_options(m);
    o.key_columns = {"k"};
    CassandraUpsertSink s(std::move(o));
    s.open();
    s.on_data(batch({R"({"k":"caf)" + esc("00e9") + R"(","v":"1"})",
                     R"({"k":"café","v":"2","__row_kind":"delete"})"}));
    std::thread t([&] {
        std::this_thread::sleep_for(50ms);
        m->release_held(/*reversed=*/true);
    });
    s.flush();
    t.join();
    EXPECT_EQ(m->submitted_count(), 2u);
    EXPECT_EQ(m->visible_rows(), 0u);  // the delete came last and wins
}

TEST(CassandraSinkOrder, DeletesBindTheKeyAsTheInsertWroteIt) {
    const auto m = std::make_shared<CassandraModel>();
    m->set_key({{"id", "bigint"}}, {{"k", "text"}});
    auto o = upsert_options(m);
    o.key_columns = {"id", "k"};
    CassandraUpsertSink s(std::move(o));
    s.open();
    s.on_data(batch({R"({"id":9007199254740993,"k":"a\u0000\n\"b","__row_kind":"delete"})"}));
    s.flush();
    const auto w = m->submitted();
    ASSERT_EQ(w.size(), 1u);
    EXPECT_EQ(w[0].key_json, (std::vector<std::string>{"9007199254740993", R"("a\u0000\n\"b")"}));
}

// ---- failures -----------------------------------------------------------------------

TEST(CassandraSinkFailure, AWriteThatFailsStopsTheSinkUntilItReopens) {
    for (const auto fault :
         {CassandraFault::Permanent, CassandraFault::Ambiguous, CassandraFault::NotApplied}) {
        const auto m = std::make_shared<CassandraModel>();
        m->script([fault](const CassandraWrite&, std::size_t i) {
            CassandraModel::Plan p;
            if (i == 4) {
                p.apply = fault == CassandraFault::Ambiguous;  // applied, then timed out
                p.outcome = outcome(
                    fault, "Write timeout", "Operation timed out - received only 0 responses.");
            }
            return p;
        });
        CassandraSink s(plain_options(m));
        s.open();
        const std::uint64_t submitted0 = counter("writes_submitted_total");
        const std::uint64_t acked0 = counter("records_total");
        std::vector<std::string> recs;
        for (int i = 0; i < 10; ++i) {
            recs.push_back(row(i, "secret-payload-" + std::to_string(i)));
        }
        const auto why = error_of([&] {
            s.on_data(batch(recs));
            s.on_barrier(barrier(1));
        });
        EXPECT_NE(why.find("a write was not acknowledged"), std::string::npos) << why;
        // Seen while making room for the sixth write: the first four were
        // acknowledged, the fifth was not, and nothing after it was sent.
        EXPECT_NE(why.find("submitted 5 write(s) and the server acknowledged 4"), std::string::npos)
            << why;
        EXPECT_EQ(why.find("secret-payload"), std::string::npos) << why;
        EXPECT_TRUE(s.failed());
        EXPECT_EQ(counter("writes_submitted_total") - submitted0, 5u);
        EXPECT_EQ(counter("records_total") - acked0, 4u);
        // The barrier after the failure is refused, and so is any later input.
        EXPECT_NE(error_of([&] { s.on_barrier(barrier(2)); }).find("failed earlier"),
                  std::string::npos);
        EXPECT_NE(error_of([&] { s.on_data(batch({row(99, "z")})); }).find("failed earlier"),
                  std::string::npos);
        EXPECT_NE(error_of([&] { s.flush(); }).find("failed earlier"), std::string::npos);
        // Reopen: a fresh session, and the sink writes again.
        m->script({});
        s.open();
        EXPECT_FALSE(s.failed());
        s.on_data(batch({row(99, "z")}));
        s.flush();
        EXPECT_EQ(m->row("99"), row(99, "z"));
        EXPECT_EQ(m->connects(), 2);
    }
}

TEST(CassandraSinkFailure, AFailureSeenWhileMakingRoomStopsTheInput) {
    const auto m = std::make_shared<CassandraModel>();
    m->script([](const CassandraWrite&, std::size_t i) {
        CassandraModel::Plan p;
        if (i == 0) {
            p.apply = false;
            p.outcome = outcome(CassandraFault::NotApplied,
                                "Unavailable",
                                "Cannot achieve consistency level QUORUM");
        }
        return p;
    });
    auto o = plain_options(m);
    o.max_in_flight = 2;
    CassandraSink s(std::move(o));
    s.open();
    const auto why = error_of([&] { s.on_data(batch({row(1, "a"), row(2, "b"), row(3, "c")})); });
    EXPECT_NE(why.find("Unavailable"), std::string::npos) << why;
    EXPECT_NE(why.find("no replica wrote it"), std::string::npos);
    EXPECT_TRUE(s.failed());
}

TEST(CassandraSinkFailure, ServerMessagesArrivedWithoutTheValuesTheyQuote) {
    const auto m = std::make_shared<CassandraModel>();
    m->script([](const CassandraWrite&, std::size_t) {
        CassandraModel::Plan p;
        p.apply = false;
        p.outcome = outcome(CassandraFault::Permanent,
                            "Invalid query",
                            "Error decoding JSON value for v: Unable to make int from "
                            "'card-4111-1111' [Source: {\"v\":\"card-4111-1111\"}]");
        return p;
    });
    CassandraSink s(plain_options(m));
    s.open();
    const auto why = error_of([&] {
        s.on_data(batch({R"({"id":1,"v":"card-4111-1111"})"}));
        s.flush();
    });
    EXPECT_NE(why.find("Unable to make int from '...'"), std::string::npos) << why;
    EXPECT_EQ(why.find("card-4111"), std::string::npos) << why;
}

TEST(CassandraSinkFailure, ARefusedRecordFailsItsWholeBatchAndDropsTheBuffer) {
    const auto m = std::make_shared<CassandraModel>();
    CassandraUpsertSink s(upsert_options(m));
    s.open();
    s.on_data(batch({row(1, "a"), row(2, "b")}));
    const auto why = error_of(
        [&] { s.on_data(batch({row(3, "c"), R"({"id":"four","v":"secret-d"})", row(5, "e")})); });
    EXPECT_NE(why.find("record 2 of a batch of 3 is refused"), std::string::npos) << why;
    EXPECT_NE(why.find("'id' is int"), std::string::npos) << why;
    EXPECT_NE(why.find("2 buffered row(s) not yet written are dropped"), std::string::npos) << why;
    EXPECT_EQ(why.find("secret-d"), std::string::npos);
    EXPECT_EQ(m->submitted_count(), 0u);
    EXPECT_NE(error_of([&] { s.on_barrier(barrier(1)); }).find("failed earlier"),
              std::string::npos);
}

TEST(CassandraSinkFailure, CancellingTheTaskEndsTheWaitAtOnce) {
    const auto m = std::make_shared<CassandraModel>();
    m->hold_all(true);
    CassandraUpsertSink s(upsert_options(m));
    Attached<CassandraUpsertSink> a(s);
    s.open();
    s.on_data(batch({row(1, "a"), row(2, "b")}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(100ms);
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { s.on_barrier(barrier(1)); });
    const auto waited = std::chrono::steady_clock::now() - t0;
    canceller.join();
    EXPECT_NE(why.find("cancelled while waiting for the writes in flight"), std::string::npos)
        << why;
    EXPECT_NE(why.find("2 write(s) still in flight are given up"), std::string::npos) << why;
    EXPECT_LT(waited, 2s);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(m->outstanding(), 0u);  // the tickets were released
    EXPECT_NO_THROW(s.close());
}

TEST(CassandraSinkFailure, CloseWritesNothingAndReleasesWhatIsInFlight) {
    const auto m = std::make_shared<CassandraModel>();
    m->hold_all(true);
    {
        CassandraUpsertSink s(upsert_options(m));
        s.open();
        s.on_data(batch({row(1, "a")}));
        s.close();  // buffered, never written
        EXPECT_EQ(m->submitted_count(), 0u);
        EXPECT_NE(error_of([&] { s.on_data(batch({row(2, "b")})); }).find("not open"),
                  std::string::npos);
    }
    {
        CassandraSink s(plain_options(m));
        s.open();
        s.on_data(batch({row(3, "c")}));
        EXPECT_EQ(s.in_flight(), 1u);
        s.close();
        EXPECT_EQ(m->outstanding(), 0u);
    }
    {
        CassandraSink s(plain_options(m));
        s.open();
        s.on_data(batch({row(4, "d")}));
        // Destroyed with a write in flight: released, never awaited.
    }
    EXPECT_EQ(m->outstanding(), 0u);
    EXPECT_GE(m->closes(), 3);
}
