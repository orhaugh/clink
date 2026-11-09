// The NATS JetStream sink's contract against a live server. SKIPPED unless
// CLINK_NATS_TEST_ENDPOINT names a JetStream-enabled server (a NATS URL);
// scripts/brokers-live.sh runs it against the pinned nats image and fails when
// any case is skipped.
//
// Every case judges by what the stream holds, read through a JetStream
// connection of the test's own: its messages by sequence and its count. The
// cases that need a server that stops answering, or stops reading, connect the
// sink through a TCP proxy (tests/integration/tcp_fault_proxy.hpp) and
// blackhole it.

#include <atomic>
#include <chrono>
#include <cstdint>
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
#include <nats/nats.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/nats/nats_sink.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::nats::NatsSink;

namespace {

bool nats_live() {
    return std::getenv("CLINK_NATS_TEST_ENDPOINT") != nullptr;
}
std::string url() {
    return std::getenv("CLINK_NATS_TEST_ENDPOINT");
}

#define REQUIRE_NATS()                                                        \
    do {                                                                      \
        if (!nats_live()) {                                                   \
            GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT (a JetStream URL)"; \
        }                                                                     \
    } while (0)

std::string unique(const std::string& what) {
    static std::atomic<int> n{0};
    return what + "_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(n.fetch_add(1));
}

// The port the server URL names, for a proxy in front of it.
std::uint16_t url_port() {
    const auto u = url();
    return static_cast<std::uint16_t>(std::stoi(u.substr(u.rfind(':') + 1)));
}

// The test's own JetStream connection: streams made, read and removed apart
// from the sink's publisher.
class TestJs {
public:
    TestJs() {
        if (natsConnection_ConnectTo(&nc_, url().c_str()) != NATS_OK ||
            natsConnection_JetStream(&js_, nc_, nullptr) != NATS_OK) {
            throw std::runtime_error("the test cannot reach the JetStream server");
        }
    }
    ~TestJs() {
        jsCtx_Destroy(js_);
        natsConnection_Destroy(nc_);
    }
    TestJs(const TestJs&) = delete;
    TestJs& operator=(const TestJs&) = delete;

    // A stream over `subject`; with max_msgs, one that refuses publishes once full.
    void stream(const std::string& name, const std::string& subject, std::int64_t max_msgs = -1) {
        jsStreamConfig cfg;
        jsStreamConfig_Init(&cfg);
        cfg.Name = name.c_str();
        const char* subjects[] = {subject.c_str()};
        cfg.Subjects = subjects;
        cfg.SubjectsLen = 1;
        if (max_msgs >= 0) {
            cfg.MaxMsgs = max_msgs;
            cfg.Discard = js_DiscardNew;
        }
        jsErrCode jerr = static_cast<jsErrCode>(0);
        if (js_AddStream(nullptr, js_, &cfg, nullptr, &jerr) != NATS_OK) {
            throw std::runtime_error("the test cannot add stream " + name);
        }
    }

    void remove(const std::string& name) {
        (void)js_DeleteStream(js_, name.c_str(), nullptr, nullptr);
    }

    std::uint64_t count(const std::string& name) {
        jsStreamInfo* si = nullptr;
        if (js_GetStreamInfo(&si, js_, name.c_str(), nullptr, nullptr) != NATS_OK) {
            return 0;
        }
        const auto n = si->State.Msgs;
        jsStreamInfo_Destroy(si);
        return n;
    }

    // Every message the stream holds, by sequence.
    std::vector<std::string> bodies(const std::string& name) {
        std::vector<std::string> out;
        const auto n = count(name);
        for (std::uint64_t seq = 1; seq <= n; ++seq) {
            natsMsg* m = nullptr;
            if (js_GetMsg(&m, js_, name.c_str(), seq, nullptr, nullptr) != NATS_OK) {
                throw std::runtime_error("the test cannot read message " + std::to_string(seq));
            }
            out.emplace_back(natsMsg_GetData(m),
                             static_cast<std::size_t>(natsMsg_GetDataLength(m)));
            natsMsg_Destroy(m);
        }
        return out;
    }

private:
    natsConnection* nc_{nullptr};
    jsCtx* js_{nullptr};
};

NatsSink::Options sink_opts(const std::string& subject) {
    NatsSink::Options o;
    o.conn.url = url();
    o.subject = subject;
    return o;
}

NatsSink::Options proxied_opts(const std::string& subject, const clink::itest::TcpFaultProxy& p) {
    auto o = sink_opts(subject);
    o.conn.url = "nats://127.0.0.1:" + std::to_string(p.port());
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

bool eventually(const std::function<bool()>& done,
                std::chrono::milliseconds limit = std::chrono::milliseconds{5000}) {
    const auto until = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < until) {
        if (done()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return done();
}

struct Attached {
    explicit Attached(NatsSink& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{82}, "nats_sink", nullptr, nullptr);
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
    NatsSink& sink;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
    std::thread canceller;
};

}  // namespace

TEST(NatsSinkContractLive, MessagesAreStoredExactlyAsWritten) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("EXACT");
    const auto subject = "clink.sinkc." + unique("exact");
    t.stream(st, subject);
    const std::vector<std::string> records = {R"({"id":1,"amount":12345678901234567890.5})",
                                              std::string{"nul\0inside", 10},
                                              "caf\xc3\xa9 \xf0\x9f\x98\x80",
                                              "",
                                              std::string(512U << 10, 'x')};
    NatsSink sink(sink_opts(subject));
    sink.open();
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(t.bodies(st), records);
    t.remove(st);
}

TEST(NatsSinkContractLive, ManyMessagesPublishWithinTheWindow) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("WINDOW");
    const auto subject = "clink.sinkc." + unique("window");
    t.stream(st, subject);
    auto o = sink_opts(subject);
    o.max_pending = 10;
    NatsSink sink(std::move(o));
    sink.open();
    std::vector<std::string> records;
    for (int i = 0; i < 1000; ++i) {
        records.push_back(std::to_string(i));
    }
    sink.on_data(batch(records));
    sink.on_barrier(kBarrier);
    sink.close();
    EXPECT_EQ(t.bodies(st), records);
    t.remove(st);
}

// A stream at max_msgs with the discard-new policy refuses what it cannot
// take: the flush fails once every message is answered, saying how many were
// stored and refused, and the stream holds exactly the stored ones.
TEST(NatsSinkContractLive, AFullStreamRefusesPublishesAndTheSinkSaysHowMany) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("FULL");
    const auto subject = "clink.sinkc." + unique("full");
    t.stream(st, subject, 2);
    NatsSink sink(sink_opts(subject));
    sink.open();
    sink.on_data(batch({"a", "b", "c", "d", "e"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the server refused 3 publish(es)"), std::string::npos) << why;
    EXPECT_NE(why.find("of the 5 messages published since the last complete flush, 2 were "
                       "stored, 3 refused, and the other 0 are unanswered"),
              std::string::npos)
        << why;
    EXPECT_NE(thrown([&] { sink.on_data(batch({"f"})); }).find("failed earlier"),
              std::string::npos);
    sink.close();
    EXPECT_EQ(t.bodies(st), (std::vector<std::string>{"a", "b"}));
    t.remove(st);
}

TEST(NatsSinkContractLive, PublishesNoStreamReceivesAreRefused) {
    REQUIRE_NATS();
    NatsSink sink(sink_opts("clink.sinkc.unbound." + unique("s")));
    sink.open();
    sink.on_data(batch({"a", "b"}));
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    EXPECT_NE(why.find("the server refused 2 publish(es)"), std::string::npos) << why;
    EXPECT_NE(why.find("0 were stored, 2 refused, and the other 0"), std::string::npos) << why;
    sink.close();
}

// Answers that do not come within publish_timeout fail the flush with the
// messages unanswered, and they are only that: once the network carries
// again, the stream stores the messages the sink gave up on.
TEST(NatsSinkContractLive, AnswersThatDoNotComeLeaveTheMessagesUnknownAndTheyMayLand) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("SILENT");
    const auto subject = "clink.sinkc." + unique("silent");
    t.stream(st, subject);
    clink::itest::TcpFaultProxy proxy(url_port());
    auto o = proxied_opts(subject, proxy);
    o.publish_timeout = std::chrono::milliseconds{500};
    NatsSink sink(std::move(o));
    sink.open();
    proxy.set_blackhole(true);
    sink.on_data(batch({"a", "b", "c"}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(took, std::chrono::milliseconds{500});
    EXPECT_LT(took, std::chrono::seconds{3});
    EXPECT_NE(why.find("within the publish timeout (500 ms)"), std::string::npos) << why;
    EXPECT_NE(why.find("0 were stored, and the other 3 are unanswered and may or may not"),
              std::string::npos)
        << why;
    proxy.set_blackhole(false);
    EXPECT_TRUE(eventually([&] { return t.count(st) == 3; }))
        << "the unanswered messages reached the stream once the network carried again";
    sink.close();
    t.remove(st);
}

TEST(NatsSinkContractLive, ACancelEndsAWaitForAnswersOnASilentServer) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("CANCEL");
    const auto subject = "clink.sinkc." + unique("cancel");
    t.stream(st, subject);
    clink::itest::TcpFaultProxy proxy(url_port());
    NatsSink sink(proxied_opts(subject, proxy));  // waits up to 30 s for answers
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
    const auto c0 = std::chrono::steady_clock::now();
    EXPECT_NO_THROW(sink.close());
    EXPECT_LT(std::chrono::steady_clock::now() - c0, std::chrono::seconds{2});
    proxy.set_blackhole(false);
    t.remove(st);
}

// A server that stops reading fills the socket, and nats.c writes with no
// deadline unless one is set: write_timeout ends the stalled write, and the
// sink fails rather than blocking.
TEST(NatsSinkContractLive, AWriteToAServerThatStopsReadingFailsWithinTheWriteTimeout) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("STALL");
    const auto subject = "clink.sinkc." + unique("stall");
    t.stream(st, subject);
    clink::itest::TcpFaultProxy proxy(url_port());
    auto o = proxied_opts(subject, proxy);
    o.conn.write_timeout = std::chrono::milliseconds{500};
    o.publish_timeout = std::chrono::milliseconds{3000};
    o.max_pending_bytes = std::size_t{1} << 30;
    NatsSink sink(std::move(o));
    sink.open();
    proxy.set_blackhole(true);
    const std::vector<std::string> records(64, std::string(512U << 10, 'x'));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] {
        sink.on_data(batch(records));
        sink.on_barrier(kBarrier);
    });
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_FALSE(why.empty()) << "the flush succeeded through a server that stopped reading";
    EXPECT_LT(took, std::chrono::seconds{10}) << why;
    EXPECT_NO_THROW(sink.close());
    proxy.set_blackhole(false);
    t.remove(st);
}

// A peer that accepts the connection and says nothing holds the connect:
// connect_timeout bounds it.
TEST(NatsSinkContractLive, AConnectToAServerThatSaysNothingFailsAtTheConnectTimeout) {
    REQUIRE_NATS();
    clink::itest::TcpFaultProxy proxy(url_port());
    proxy.set_blackhole(true);
    auto o = proxied_opts("clink.sinkc.any", proxy);
    o.conn.connect_timeout = std::chrono::milliseconds{500};
    NatsSink sink(std::move(o));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink.open(); });
    EXPECT_FALSE(why.empty());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds{3}) << why;
    EXPECT_NE(thrown([&] { sink.on_data(batch({"a"})); }).find("before open()"), std::string::npos);
}

TEST(NatsSinkContractLive, TheFactoryBuildsASinkThatPublishes) {
    REQUIRE_NATS();
    TestJs t;
    const auto st = unique("FACTORY");
    const auto subject = "clink.sinkc." + unique("factory");
    t.stream(st, subject);
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "nats_sink_string", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {
        {"url", url()}, {"subject", subject}, {"max_pending", "2"}, {"publish_timeout_ms", "5000"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    sink->on_data(batch({R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    sink->on_barrier(kBarrier);
    sink->close();
    EXPECT_EQ(t.bodies(st),
              (std::vector<std::string>{R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    t.remove(st);
}
