// The Pulsar sink's contract against a live broker. SKIPPED unless
// CLINK_PULSAR_TEST_ENDPOINT names a broker (a pulsar:// URL);
// scripts/brokers-live.sh runs it against the pinned Pulsar image and fails when
// any case is skipped.
//
// Every case judges by what the topic holds, read through a consumer of the
// test's own from the earliest position. The cases that need a broker that
// stops answering connect the sink through a TCP proxy
// (tests/integration/tcp_fault_proxy.hpp) and blackhole it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <pulsar/c/client.h>
#include <pulsar/c/consumer.h>
#include <pulsar/c/consumer_configuration.h>
#include <pulsar/c/message.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::pulsar::PulsarSink;

namespace {

bool pulsar_live() {
    return std::getenv("CLINK_PULSAR_TEST_ENDPOINT") != nullptr;
}
std::string url() {
    return std::getenv("CLINK_PULSAR_TEST_ENDPOINT");
}
std::uint16_t url_port() {
    const auto u = url();
    return static_cast<std::uint16_t>(std::stoi(u.substr(u.rfind(':') + 1)));
}

#define REQUIRE_PULSAR()                                                        \
    do {                                                                        \
        if (!pulsar_live()) {                                                   \
            GTEST_SKIP() << "set CLINK_PULSAR_TEST_ENDPOINT (a pulsar:// URL)"; \
        }                                                                       \
    } while (0)

std::string topic(const std::string& what) {
    static std::atomic<int> n{0};
    return "persistent://public/default/clink-sinkc-" + what + "-" +
           std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n.fetch_add(1));
}

// Every message the topic holds, oldest first, read by a consumer of the
// test's own that starts at the earliest position.
std::vector<std::string> read_topic(const std::string& t) {
    pulsar_client_configuration_t* cc = pulsar_client_configuration_create();
    pulsar_client_t* client = pulsar_client_create(url().c_str(), cc);
    pulsar_client_configuration_free(cc);
    pulsar_consumer_configuration_t* conf = pulsar_consumer_configuration_create();
    pulsar_consumer_configuration_set_consumer_type(conf, pulsar_ConsumerExclusive);
    pulsar_consumer_set_subscription_initial_position(conf, initial_position_earliest);
    pulsar_consumer_t* consumer = nullptr;
    const auto r = pulsar_client_subscribe(client, t.c_str(), "clink-reader", conf, &consumer);
    pulsar_consumer_configuration_free(conf);
    if (r != pulsar_result_Ok) {
        pulsar_client_close(client);
        pulsar_client_free(client);
        throw std::runtime_error("the test cannot read topic " + t);
    }
    std::vector<std::string> out;
    for (;;) {
        pulsar_message_t* m = nullptr;
        if (pulsar_consumer_receive_with_timeout(consumer, &m, 2000) != pulsar_result_Ok) {
            break;
        }
        out.emplace_back(static_cast<const char*>(pulsar_message_get_data(m)),
                         pulsar_message_get_length(m));
        (void)pulsar_consumer_acknowledge(consumer, m);
        pulsar_message_free(m);
    }
    (void)pulsar_consumer_unsubscribe(consumer);
    pulsar_consumer_free(consumer);
    (void)pulsar_client_close(client);
    pulsar_client_free(client);
    return out;
}

PulsarSink::Options sink_opts(const std::string& t) {
    PulsarSink::Options o;
    o.conn.service_url = url();
    o.topic = t;
    return o;
}

PulsarSink::Options proxied_opts(const std::string& t, const clink::itest::TcpFaultProxy& p) {
    auto o = sink_opts(t);
    o.conn.service_url = "pulsar://127.0.0.1:" + std::to_string(p.port());
    return o;
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

struct Attached {
    explicit Attached(PulsarSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{83}, "pulsar_sink", nullptr, nullptr);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        s.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    void cancel_after(std::chrono::milliseconds wait) {
        canceller = std::thread([this, wait] {
            std::this_thread::sleep_for(wait);
            cancel->store(true);
        });
    }
    PulsarSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::thread canceller;
};

}  // namespace

TEST(PulsarSinkContractLive, MessagesArePersistedExactlyAsWritten) {
    REQUIRE_PULSAR();
    const auto t = topic("exact");
    const std::vector<std::string> records = {R"({"id":1,"amount":12345678901234567890.5})",
                                              std::string{"nul\0inside", 10},
                                              "caf\xc3\xa9 \xf0\x9f\x98\x80",
                                              std::string(512U << 10, 'x')};
    PulsarSink sink(sink_opts(t));
    sink.open();
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(read_topic(t), records);
}

// The client queues at most 1000 unanswered messages by default and refuses
// the rest (ProducerQueueIsFull), which failed every barrier after a burst; the
// sink's own window now holds the burst back instead.
TEST(PulsarSinkContractLive, ABurstPastTheClientsQueueIsHeldBackByTheWindow) {
    REQUIRE_PULSAR();
    const auto t = topic("burst");
    PulsarSink sink(sink_opts(t));
    sink.open();
    std::vector<std::string> records;
    for (int i = 0; i < 20000; ++i) {
        records.push_back("m" + std::to_string(i));
    }
    sink.on_data(batch(records));
    EXPECT_NO_THROW(sink.on_barrier(kBarrier));
    sink.close();
    EXPECT_EQ(read_topic(t), records);
}

TEST(PulsarSinkContractLive, AMessageTheClientRefusesFailsTheFlushAndTheRestArePersisted) {
    REQUIRE_PULSAR();
    const auto t = topic("toobig");
    PulsarSink sink(sink_opts(t));
    sink.open();
    sink.on_data(batch({"a", std::string(6U << 20, 'x'), "c"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the client failed 1 send(s)"), std::string::npos) << why;
    EXPECT_NE(
        why.find("of the 3 messages published since the last complete flush, 2 were "
                 "persisted, 1 failed (one failed on the client's send timeout may still be on the "
                 "topic), and the other 0 are unanswered"),
        std::string::npos)
        << why;
    EXPECT_NE(thrown([&] { sink.flush(); }).find("failed earlier"), std::string::npos);
    sink.close();
    EXPECT_EQ(read_topic(t), (std::vector<std::string>{"a", "c"}));
}

// A send the broker does not answer within send_timeout fails in the client,
// and the flush with it; but the message was on the wire, and the broker
// persists it once the network carries again: a send failed on its timeout
// may still be on the topic.
TEST(PulsarSinkContractLive, ASendFailedOnItsTimeoutMayStillBePersisted) {
    REQUIRE_PULSAR();
    const auto t = topic("timeout");
    clink::itest::TcpFaultProxy proxy(url_port());
    auto o = proxied_opts(t, proxy);
    o.send_timeout = std::chrono::milliseconds{1000};
    PulsarSink sink(std::move(o));
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a", "b", "c"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_FALSE(why.empty());
    EXPECT_LT(took, std::chrono::seconds{6}) << why;
    EXPECT_NE(why.find("the client failed 3 send(s)"), std::string::npos) << why;
    proxy.set_blackhole(false);
    sink.close();
    EXPECT_EQ(read_topic(t), (std::vector<std::string>{"a", "b", "c"}));
}

TEST(PulsarSinkContractLive, ACancelEndsAWaitForAnswersOnASilentBroker) {
    REQUIRE_PULSAR();
    const auto t = topic("cancel");
    clink::itest::TcpFaultProxy proxy(url_port());
    auto o = proxied_opts(t, proxy);
    o.conn.operation_timeout_s = 2;
    PulsarSink sink(std::move(o));  // each send waits up to 30 s
    Attached a(sink);
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a"}));
    a.cancel_after(std::chrono::milliseconds{300});
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    a.canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{2});
    EXPECT_NE(why.find("the task was cancelled while waiting for a flush's answers"),
              std::string::npos)
        << why;
    // The client's close fails what is unanswered and waits at most its
    // operation timeout for the broker.
    const auto c0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    const auto closing = std::chrono::steady_clock::now() - c0;
    EXPECT_LT(closing, std::chrono::seconds{10});
    std::printf("[pulsar-close] close on a silent broker took %lld ms\n",
                static_cast<long long>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(closing).count()));
    proxy.set_blackhole(false);
}

TEST(PulsarSinkContractLive, TheFactoryBuildsASinkThatPublishes) {
    REQUIRE_PULSAR();
    const auto t = topic("factory");
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "pulsar_sink_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {
        {"service_url", url()}, {"topic", t}, {"max_pending", "2"}, {"send_timeout_ms", "10000"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    sink->on_data(batch({R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    sink->on_barrier(kBarrier);
    sink->close();
    EXPECT_EQ(read_topic(t),
              (std::vector<std::string>{R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
}
