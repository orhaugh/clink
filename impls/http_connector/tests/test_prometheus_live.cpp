// The Prometheus Pushgateway sink against a live gateway (scripts/prometheus-live.sh
// starts a pinned one): every case pushes through the production sink and
// then reads the gateway back with its own scrape of /metrics, never from
// anything the sink reports. Each case uses a metric and job of its own, so
// the cases cannot see one another's series.
//
//   CLINK_PUSHGATEWAY_TEST_ENDPOINT  the gateway (the cases skip without it)
//   CLINK_PUSHGATEWAY_TEST_VERSION   the version the gate started

#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/prometheus_push_sink.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "prom_live.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::http_connector::DlqPolicy;
using clink::http_connector::make_prometheus_pushgateway_sink;
using clink::http_connector::PromPushOptions;
using clink::http_connector::PromPushSeries;
namespace live = clink::http_connector::prom_live;

namespace {

using namespace std::chrono_literals;
using Labels = std::map<std::string, std::string>;

#define REQUIRE_LIVE()                                                                         \
    do {                                                                                       \
        if (!live::configured()) {                                                             \
            GTEST_SKIP() << "set CLINK_PUSHGATEWAY_TEST_ENDPOINT (scripts/prometheus-live.sh " \
                            "does)";                                                           \
        }                                                                                      \
    } while (0)

// A JSON \u escape, built at run time so that no tool on the way to the
// compiler can turn the escape into the character it names.
std::string esc(const char* hex) {
    return std::string{"\\"} + "u" + hex;
}

// A name no other case or run uses.
std::string unique(const std::string& what) {
    static std::atomic<int> n{0};
    return "clink_live_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(++n) + "_" + what;
}

PromPushOptions options(const std::string& metric,
                        const std::string& job,
                        PromPushSeries mode = PromPushSeries::Batch) {
    PromPushOptions o;
    o.url = live::endpoint();
    o.job = job;
    o.metric_name = metric;
    o.value_field = "v";
    o.push_series = mode;
    o.max_retries = 2;
    return o;
}

Batch<std::string> batch(const std::vector<std::string>& recs) {
    Batch<std::string> b;
    for (const auto& r : recs) {
        b.emplace(r);
    }
    return b;
}

void push_all(const PromPushOptions& o, const std::vector<std::string>& recs) {
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch(recs));
    sink->flush();
    sink->close();
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

struct Capture : clink::DeadLetterQueue {
    void report(const clink::BadRecord& r) override {
        const std::lock_guard<std::mutex> lock(mu);
        records.push_back(r);
    }
    std::mutex mu;
    std::vector<clink::BadRecord> records;
};

struct Attached {
    explicit Attached(clink::Sink<std::string>& s, std::uint64_t restored_from = 0) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{9091}, "prometheus_sink", nullptr, nullptr);
        ctx->set_dead_letter_queue(&dlq);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        ctx->set_restore_from_checkpoint_id(restored_from);
        sink.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    clink::Sink<std::string>& sink;
    Capture dlq;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

// One label k=<key> per series, its value as text.
live::Series by_k(const std::map<std::string, std::string>& kv) {
    live::Series out;
    for (const auto& [k, v] : kv) {
        out[Labels{{"k", k}}] = v;
    }
    return out;
}

// The gateway applies a delete asynchronously; wait until the job is gone.
bool await_gone(const std::string& metric, const std::string& job) {
    for (int i = 0; i < 100; ++i) {
        if (live::series(metric, job).empty()) {
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

}  // namespace

TEST(PushgatewayLive, TheServerIsTheVersionTheGateStarted) {
    REQUIRE_LIVE();
    const auto want = live::env_or("CLINK_PUSHGATEWAY_TEST_VERSION");
    if (want.empty()) {
        GTEST_SKIP() << "set CLINK_PUSHGATEWAY_TEST_VERSION";
    }
    EXPECT_EQ(live::version(), want);
}

TEST(PushgatewayLive, EveryLabelComesBackFromAScrapeAsTheRecordGaveIt) {
    REQUIRE_LIVE();
    const auto m = unique("labels");
    const auto job = unique("job");
    push_all(options(m, job),
             {R"({"v":1,"s":"plain"})",
              R"({"v":2,"s":"q\"b\\s\nn"})",
              R"({"v":3,"s":"x\ty\rz"})",
              R"({"v":4,"s":"x)" + esc("0000") + "y" + esc("0007") + R"("})",
              R"({"v":5,"s":"caf)" + esc("00e9") + " " + esc("20ac") + " " + esc("d83d") +
                  esc("de00") + R"("})",
              R"({"v":6,"id":9007199254740993})",
              R"({"v":7,"id":9007199254740992})",
              R"({"v":8,"id":18446744073709551615})",
              R"({"v":9,"id":1.50,"ok":true})",
              R"({"v":10,"s":"","e":null,"z":"zz"})"});
    const live::Series want = {
        {Labels{{"s", "plain"}}, "1"},
        {Labels{{"s", "q\"b\\s\nn"}}, "2"},
        {Labels{{"s", "x\ty\rz"}}, "3"},
        {Labels{{"s", std::string("x\0y\a", 4)}}, "4"},
        {Labels{{"s", "caf\xC3\xA9 \xE2\x82\xAC \xF0\x9F\x98\x80"}}, "5"},
        {Labels{{"id", "9007199254740993"}}, "6"},
        {Labels{{"id", "9007199254740992"}}, "7"},
        {Labels{{"id", "18446744073709551615"}}, "8"},
        {Labels{{"id", "1.50"}, {"ok", "true"}}, "9"},
        {Labels{{"z", "zz"}}, "10"},
    };
    EXPECT_EQ(live::series(m, job), want);
    std::printf(
        "[prometheus-live] labels: %zu series read back exactly, control characters, "
        "neighbours above 2^53 and integers past 64 bits included\n",
        want.size());
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, APushTakesWhatTheGatewayTakesToAnswer) {
    REQUIRE_LIVE();
    const auto m = unique("timing");
    const auto job = unique("job");
    auto sink = make_prometheus_pushgateway_sink(options(m, job));
    sink->open();
    std::vector<long long> ms;
    for (int i = 0; i < 5; ++i) {
        sink->on_data(batch({R"({"v":)" + std::to_string(i) + R"(,"k":"a"})"}));
        const auto t0 = std::chrono::steady_clock::now();
        sink->flush();
        ms.push_back(std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - t0)
                         .count());
    }
    sink->close();
    EXPECT_EQ(live::series(m, job), by_k({{"a", "4"}}));
    std::printf(
        "[prometheus-live] five pushes of one series took %lld, %lld, %lld, %lld and "
        "%lld ms\n",
        ms[0],
        ms[1],
        ms[2],
        ms[3],
        ms[4]);
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, TheGroupsLabelsComeBackHoweverThePathEncodesThem) {
    REQUIRE_LIVE();
    const auto m = unique("path");
    const auto job = unique("job") + "/a b";  // a slash and a space: base64 in the path
    auto o = options(m, job);
    o.grouping = "path=/var/tmp,empty=,city=caf\xC3\xA9,plus=a+b,tilde=~~~,pct=50%,plain=x-1.y_2";
    push_all(o, {R"({"v":1})"});
    const live::Series want = {{Labels{{"path", "/var/tmp"},
                                       {"empty", ""},
                                       {"city", "caf\xC3\xA9"},
                                       {"plus", "a+b"},
                                       {"tilde", "~~~"},
                                       {"pct", "50%"},
                                       {"plain", "x-1.y_2"}},
                                "1"}};
    EXPECT_EQ(live::series(m, job), want);
}

TEST(PushgatewayLive, ValuesComeBackAsTheGaugeHoldsThem) {
    REQUIRE_LIVE();
    const auto m = unique("values");
    const auto job = unique("job");
    push_all(options(m, job),
             {R"({"v":"NaN","k":"nan"})",
              R"({"v":"+Inf","k":"pinf"})",
              R"({"v":"-Infinity","k":"ninf"})",
              R"({"v":9007199254740992,"k":"max"})",
              R"({"v":-9007199254740992,"k":"min"})",
              R"({"v":0.1,"k":"tenth"})",
              R"({"v":1e308,"k":"big"})",
              R"({"v":5e-324,"k":"tiny"})",
              R"({"v":true,"k":"yes"})",
              R"({"v":"3.25","k":"text"})"});
    const auto got = live::series(m, job);
    const std::map<std::string, double> want = {{"max", 9007199254740992.0},
                                                {"min", -9007199254740992.0},
                                                {"tenth", 0.1},
                                                {"big", 1e308},
                                                {"tiny", 5e-324},
                                                {"yes", 1.0},
                                                {"text", 3.25}};
    ASSERT_EQ(got.size(), want.size() + 3);
    EXPECT_EQ(got.at(Labels{{"k", "nan"}}), "NaN");
    EXPECT_EQ(got.at(Labels{{"k", "pinf"}}), "+Inf");
    EXPECT_EQ(got.at(Labels{{"k", "ninf"}}), "-Inf");
    for (const auto& [k, v] : want) {
        const auto& text = got.at(Labels{{"k", k}});
        double d = 0;
        const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), d);
        EXPECT_TRUE(ec == std::errc{} && ptr == text.data() + text.size()) << text;
        EXPECT_EQ(d, v) << k << " came back as " << text;
    }
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, RefusedRecordsNeverReachTheGatewayAndTheirNeighboursDo) {
    REQUIRE_LIVE();
    const auto m = unique("refused");
    const auto job = unique("job");
    auto o = options(m, job);
    o.grouping = "region=eu";
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})",
                         R"({"v":1,"k":"x","device-id":"d"})",
                         R"({"v":1,"k":"x","nested":{"a":1}})",
                         R"({"v":null,"k":"x"})",
                         R"({"v":1,"k":"x","region":"us"})",
                         R"({"v":9007199254740993,"k":"x"})",
                         R"({"v":"abc","k":"x"})",
                         R"({"v":2,"k":"b","region":"eu"})"}));
    sink->flush();
    sink->close();
    EXPECT_EQ(live::series(m, job),
              (live::Series{{Labels{{"k", "a"}, {"region", "eu"}}, "1"},
                            {Labels{{"k", "b"}, {"region", "eu"}}, "2"}}));
    EXPECT_EQ(at.dlq.records.size(), 6U);
    live::delete_group("/metrics/job/" + job + "/region/eu");
}

TEST(PushgatewayLive, UnderFailARefusalPushesNothingAndTheSinkStops) {
    REQUIRE_LIVE();
    const auto m = unique("fail");
    const auto job = unique("job");
    auto sink = make_prometheus_pushgateway_sink(options(m, job));
    sink->open();
    const auto why = error_of(
        [&] { sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":1,"bad-name":"x"})"})); });
    EXPECT_NE(why.find("not a Prometheus label name"), std::string::npos) << why;
    EXPECT_NE(error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); }), "");
    EXPECT_TRUE(live::series(m, job).empty()) << "nothing reached the gateway";
}

TEST(PushgatewayLive, InBatchModeEveryTriggerReplacesTheSeriesOfThePreviousPush) {
    REQUIRE_LIVE();
    const auto m = unique("batch");
    const auto job = unique("job");
    // Another metric in the same group: a POST replaces only the pushed metric.
    ASSERT_EQ(
        live::push_raw("/metrics/job/" + job, "# TYPE " + m + "_other gauge\n" + m + "_other 5\n"),
        200);
    auto o = options(m, job);
    o.batch_records = 2;
    o.max_age = 100ms;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})"}));  // batch_records
    EXPECT_EQ(live::series(m, job), by_k({{"a", "1"}, {"b", "2"}}));
    sink->on_data(batch({R"({"v":3,"k":"c"})"}));
    std::this_thread::sleep_for(150ms);
    sink->on_data(batch({R"({"v":4,"k":"d"})"}));  // linger
    EXPECT_EQ(live::series(m, job), by_k({{"c", "3"}, {"d", "4"}}));
    sink->on_data(batch({R"({"v":5,"k":"e"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // a barrier
    EXPECT_EQ(live::series(m, job), by_k({{"e", "5"}}));
    sink->on_data(batch({R"({"v":6,"k":"f"})"}));
    sink->flush();  // end of input
    EXPECT_EQ(live::series(m, job), by_k({{"f", "6"}}));
    sink->close();
    EXPECT_EQ(live::series(m + "_other", job), (live::Series{{Labels{}, "5"}}));

    // batch_bytes: the sink pushes before its buffer passes it, so the gateway
    // ends with the series of the last push only.
    const auto mb = unique("batch_bytes");
    auto b = options(mb, job);
    b.batch_bytes = 1024;
    b.max_sample_bytes = 512;
    std::vector<std::string> recs;
    for (int i = 0; i < 6; ++i) {
        recs.push_back(R"({"v":)" + std::to_string(i) + R"(,"k":")" + std::to_string(i) +
                       std::string(300, 'x') + R"("})");
    }
    push_all(b, recs);
    const auto last = live::series(mb, job);
    EXPECT_GE(last.size(), 1U);
    EXPECT_LT(last.size(), 6U);
    EXPECT_TRUE(last.contains(Labels{{"k", "5" + std::string(300, 'x')}}));
    std::printf(
        "[prometheus-live] batch mode: each of batch_records, linger, a barrier and end "
        "of input replaced the previous push's series; batch_bytes left %zu of 6\n",
        last.size());
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, InAllModeEveryTriggerLeavesEverySeriesAtItsLatestValue) {
    REQUIRE_LIVE();
    const auto m = unique("all");
    const auto job = unique("job");
    auto o = options(m, job, PromPushSeries::All);
    o.max_age = 100ms;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_EQ(live::series(m, job), by_k({{"a", "1"}, {"b", "2"}}));
    sink->on_data(batch({R"({"v":3,"k":"c"})"}));
    std::this_thread::sleep_for(150ms);
    sink->on_data(batch({R"({"v":4,"k":"a"})"}));  // linger
    EXPECT_EQ(live::series(m, job), by_k({{"a", "4"}, {"b", "2"}, {"c", "3"}}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});  // nothing new
    EXPECT_EQ(live::series(m, job), by_k({{"a", "4"}, {"b", "2"}, {"c", "3"}}));
    sink->on_data(batch({R"({"v":5,"k":"a"})", R"({"v":6,"k":"a"})", R"({"v":7,"k":"d"})"}));
    sink->flush();
    EXPECT_EQ(live::series(m, job), by_k({{"a", "6"}, {"b", "2"}, {"c", "3"}, {"d", "7"}}));
    sink->close();
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, ARestoredRunInAllModeKeepsTheSeriesItsPredecessorPushed) {
    REQUIRE_LIVE();
    const auto m = unique("restored");
    const auto job = unique("job");
    auto o = options(m, job, PromPushSeries::All);
    o.grouping = "instance=w1";
    const auto run = [&](std::uint64_t restored_from, const std::vector<std::string>& recs) {
        auto sink = make_prometheus_pushgateway_sink(o);
        Attached at(*sink, restored_from);
        sink->open();
        sink->on_data(batch(recs));
        sink->on_barrier(CheckpointBarrier{CheckpointId{restored_from + 1}});
        sink->close();
    };
    const auto inst = [](std::map<std::string, std::string> kv) {
        live::Series out;
        for (const auto& [k, v] : kv) {
            out[Labels{{"instance", "w1"}, {"k", k}}] = v;
        }
        return out;
    };
    run(0, {R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})", R"({"v":3,"k":"c"})"});
    EXPECT_EQ(live::series(m, job), inst({{"a", "1"}, {"b", "2"}, {"c", "3"}}));
    run(5, {R"({"v":20,"k":"b"})", R"({"v":4,"k":"d"})"});
    EXPECT_EQ(live::series(m, job), inst({{"a", "1"}, {"b", "20"}, {"c", "3"}, {"d", "4"}}))
        << "a restored run adopts the group's series and keeps its own values";
    run(0, {R"({"v":5,"k":"e"})"});
    EXPECT_EQ(live::series(m, job), inst({{"e", "5"}})) << "a fresh start replaces them";
    live::delete_group("/metrics/job/" + job + "/instance/w1");
}

TEST(PushgatewayLive, TwoWritersOfOneGroupReplaceEachOthersSeries) {
    REQUIRE_LIVE();
    const auto m = unique("writers");
    const auto job = unique("job");
    push_all(options(m, job), {R"({"v":1,"writer":"one"})"});
    push_all(options(m, job), {R"({"v":2,"writer":"two"})"});
    EXPECT_EQ(live::series(m, job), (live::Series{{Labels{{"writer", "two"}}, "2"}}))
        << "the second writer's push replaced the first's series";
    push_all(options(m, job, PromPushSeries::All), {R"({"v":3,"writer":"one"})"});
    EXPECT_EQ(live::series(m, job), (live::Series{{Labels{{"writer", "one"}}, "3"}}))
        << "and in all mode as well: the sink must be the group's only writer";
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, ATypeConflictIsRefusedWholeAndThePushStaysPending) {
    REQUIRE_LIVE();
    const auto m = unique("conflict");
    const auto job = unique("job");
    const auto other = "/metrics/job/" + job + "_other";
    ASSERT_EQ(live::push_raw(other, "# TYPE " + m + " counter\n" + m + " 1\n"), 200);
    auto o = options(m, job);
    o.max_retries = 5;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); });
    EXPECT_NE(why.find("refused the push"), std::string::npos) << why;
    // The gateway names whichever of the two it gathered second.
    EXPECT_TRUE(why.find("is not a COUNTER") != std::string::npos ||
                why.find("is not a GAUGE") != std::string::npos)
        << why;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 1s) << "a refusal is not retried";
    EXPECT_TRUE(live::series(m, job).empty()) << "nothing of the push was applied";
    live::delete_group(other);
    ASSERT_TRUE(await_gone(m, job + "_other"));
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    EXPECT_EQ(live::series(m, job), by_k({{"a", "1"}}));
    std::printf("[prometheus-live] a type conflict: refused once (%s), pushed after it cleared\n",
                why.substr(0, why.find(" (")).c_str());
    sink->close();
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, AReplyLostAfterTheGatewayAppliedAPushIsSentAgainAndChangesNothing) {
    REQUIRE_LIVE();
    const auto m = unique("lost");
    const auto job = unique("job");
    const auto port = static_cast<std::uint16_t>(
        std::stoi(live::endpoint().substr(live::endpoint().rfind(':') + 1)));
    clink::itest::TcpFaultProxy proxy(port);
    proxy.set_request_marker("POST /metrics/");
    auto o = options(m, job);
    o.url = "http://127.0.0.1:" + std::to_string(proxy.port());
    o.request_timeout_ms = 1000;
    o.max_retries = 3;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    proxy.hold_at_upload(1);
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // sent again after the timeout
    proxy.drop_held();
    sink->close();
    EXPECT_EQ(live::series(m, job), by_k({{"a", "1"}, {"b", "2"}}))
        << "the resend replaced the series with the same values";
    std::printf(
        "[prometheus-live] a reply withheld after the gateway applied the push: resent, "
        "the same 2 series\n");
    live::delete_group("/metrics/job/" + job);
}

TEST(PushgatewayLive, AGatewayThatLostItsSeriesHasThemAgainAtTheNextBarrier) {
    REQUIRE_LIVE();
    const auto m = unique("heal");
    const auto job = unique("job");
    auto sink = make_prometheus_pushgateway_sink(options(m, job, PromPushSeries::All));
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    ASSERT_EQ(live::series(m, job), by_k({{"a", "1"}, {"b", "2"}}));
    live::delete_group("/metrics/job/" + job);  // as a gateway restarted without persistence
    ASSERT_TRUE(await_gone(m, job));
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    EXPECT_EQ(live::series(m, job), by_k({{"a", "1"}, {"b", "2"}}));
    sink->close();
    live::delete_group("/metrics/job/" + job);
}
