// The Pub/Sub sink's contract against the Pub/Sub emulator. SKIPPED unless
// CLINK_PUBSUB_EMULATOR_HOST names one (host:port); scripts/brokers-live.sh runs
// it against docker/integration-services.yml's emulator and fails when any case
// is skipped.
//
// Every case judges by what a subscription of the test's own receives, pulled
// over the emulator's REST API with a client apart from the sink's. The cases
// that need a reply other than the emulator's put an HTTP proxy in front of it
// (tests/integration/bulk_fault_proxy.hpp), and the one that needs a request
// held in flight a TCP proxy (tests/integration/tcp_fault_proxy.hpp).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/base64.hpp"
#include "clink/http_connector/pubsub.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/bulk_fault_proxy.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using namespace clink::http_connector;

namespace {

bool emulator_configured() {
    return std::getenv("CLINK_PUBSUB_EMULATOR_HOST") != nullptr;
}
std::string emulator_hostport() {
    return std::getenv("CLINK_PUBSUB_EMULATOR_HOST");
}
std::string emulator_url() {
    return "http://" + emulator_hostport();
}

#define REQUIRE_EMULATOR()                                                          \
    do {                                                                            \
        if (!emulator_configured()) {                                               \
            GTEST_SKIP() << "set CLINK_PUBSUB_EMULATOR_HOST (e.g. localhost:8085)"; \
        }                                                                           \
    } while (0)

const std::string kProject = "clink-sink-test";

std::string unique(const std::string& what) {
    static std::atomic<int> n{0};
    return what + "-" + std::to_string(static_cast<long>(::getpid())) + "-" +
           std::to_string(n.fetch_add(1));
}

// The test's own view of a topic: a subscription made before anything is
// published, pulled and acknowledged over REST with a client of its own.
class TopicReader {
public:
    explicit TopicReader(std::string topic)
        : topic_(std::move(topic)), sub_(topic_ + "-reader"), cli_(emulator_url()) {
        cli_.set_read_timeout(30, 0);
        const auto t =
            cli_.Put("/v1/projects/" + kProject + "/topics/" + topic_, "", "application/json");
        if (!t || t->status != 200) {
            throw std::runtime_error("the test cannot create topic " + topic_);
        }
        const auto s = cli_.Put("/v1/projects/" + kProject + "/subscriptions/" + sub_,
                                R"({"topic":"projects/)" + kProject + "/topics/" + topic_ +
                                    R"(","ackDeadlineSeconds":60})",
                                "application/json");
        if (!s || s->status != 200) {
            throw std::runtime_error("the test cannot create a subscription to " + topic_);
        }
    }

    // Every message delivered so far, in arrival order, pulled until two pulls
    // in a row come back empty, each acknowledged.
    std::vector<std::string> drain() {
        std::vector<std::string> out;
        int empty = 0;
        while (empty < 2) {
            const auto r =
                cli_.Post("/v1/projects/" + kProject + "/subscriptions/" + sub_ + ":pull",
                          R"({"maxMessages":1000,"returnImmediately":true})",
                          "application/json");
            if (!r || r->status != 200) {
                throw std::runtime_error("the test cannot pull from " + sub_);
            }
            const auto got = pubsub_parse_pull_response(r->body);
            if (got.empty()) {
                ++empty;
                std::this_thread::sleep_for(std::chrono::milliseconds{200});
                continue;
            }
            empty = 0;
            std::vector<std::string> ids;
            for (const auto& m : got) {
                out.push_back(m.payload);
                ids.push_back(m.ack_id);
            }
            (void)cli_.Post("/v1/projects/" + kProject + "/subscriptions/" + sub_ + ":acknowledge",
                            pubsub_ack_body(ids),
                            "application/json");
        }
        return out;
    }

    [[nodiscard]] const std::string& topic() const { return topic_; }

private:
    std::string topic_;
    std::string sub_;
    httplib::Client cli_;
};

struct Capture : clink::DeadLetterQueue {
    void report(const clink::BadRecord& r) override {
        const std::lock_guard<std::mutex> lock(mu);
        records.push_back(r);
    }
    std::mutex mu;
    std::vector<clink::BadRecord> records;
};

struct Attached {
    explicit Attached(clink::Sink<std::string>& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{8085}, "pubsub_sink", nullptr, nullptr);
        ctx->set_dead_letter_queue(&dlq);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
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

const CheckpointBarrier kBarrier{CheckpointId{1}};

PubSubSinkOptions sink_opts(const std::string& topic, const std::string& base_url = {}) {
    PubSubSinkOptions o;
    o.http.base_url = base_url.empty() ? emulator_url() : base_url;
    o.project = kProject;
    o.topic = topic;
    o.max_retries = 2;
    return o;
}

}  // namespace

TEST(PubSubSinkContractLive, MessagesArriveExactlyAsWritten) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("exact"));
    std::vector<std::string> records = {R"({"id":1,"amount":12345678901234567890.5})",
                                        std::string{"nul\0inside", 10},
                                        "caf\xc3\xa9 \xf0\x9f\x98\x80",
                                        std::string(100U << 10, 'x')};
    // Past one publish's 1000 messages, so the sink sends more than one.
    for (int i = 0; i < 1500; ++i) {
        records.push_back("r" + std::to_string(i));
    }
    auto sink = make_pubsub_publish_sink(sink_opts(reader.topic()));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch(records));
    sink->on_barrier(kBarrier);
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    std::sort(records.begin(), records.end());
    EXPECT_EQ(got, records);
}

// Pub/Sub refuses a message with neither data nor attributes, and refuses the
// whole request with it, so one empty record in a batch once failed every
// record beside it: under dlq=drop all of them went to the dead-letter queue.
// The sink refuses the empty record before it is sent, and only it.
TEST(PubSubSinkContractLive, AnEmptyRecordIsDeadLetteredAloneAndTheRestArrive) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("empty-drop"));
    auto o = sink_opts(reader.topic());
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_pubsub_publish_sink(std::move(o));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch({"a", "", "b"}));
    EXPECT_NO_THROW(sink->on_barrier(kBarrier));
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"a", "b"}));
    ASSERT_EQ(attached.dlq.records.size(), 1U);
    EXPECT_EQ(attached.dlq.records[0].payload, "");
    EXPECT_NE(attached.dlq.records[0].error.find("empty"), std::string::npos)
        << attached.dlq.records[0].error;
}

TEST(PubSubSinkContractLive, AnEmptyRecordFailsTheSinkWithNothingOfItSent) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("empty-fail"));
    auto sink = make_pubsub_publish_sink(sink_opts(reader.topic()));
    Attached attached(*sink);
    sink->open();
    const auto why = thrown([&] {
        sink->on_data(batch({"a", "", "b"}));
        sink->on_barrier(kBarrier);
    });
    EXPECT_NE(why.find("empty"), std::string::npos) << why;
    EXPECT_NE(thrown([&] { sink->on_barrier(kBarrier); }), "");
    sink->close();
    EXPECT_TRUE(reader.drain().empty());
}

// A missing topic is the endpoint's state, not the records': the sink retries,
// then fails, and under dlq=drop dead-letters nothing.
TEST(PubSubSinkContractLive, AMissingTopicFailsTheSinkAndDeadLettersNothing) {
    REQUIRE_EMULATOR();
    auto o = sink_opts(unique("missing"));
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_pubsub_publish_sink(std::move(o));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch({"a", "b"}));
    const auto why = thrown([&] { sink->on_barrier(kBarrier); });
    EXPECT_NE(why.find("HTTP 404"), std::string::npos) << why;
    EXPECT_TRUE(attached.dlq.records.empty());
    // The batch stays pending, so a later barrier cannot pass over it.
    EXPECT_NE(thrown([&] { sink->on_barrier(kBarrier); }), "");
    sink->close();
}

// A 2xx whose body confirms no message (a proxy's or another service's answer)
// once counted as delivered with nothing published. The sink now requires one
// message id per message, and sends again.
TEST(PubSubSinkContractLive, AReplyThatConfirmsNothingIsSentAgain) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("unconfirmed"));
    clink::itest::BulkFaultProxy proxy(emulator_url());
    proxy.refuse(1, 200, "<html>ok</html>");
    auto sink = make_pubsub_publish_sink(sink_opts(reader.topic(), proxy.url()));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch({"a", "b", "c"}));
    EXPECT_NO_THROW(sink->on_barrier(kBarrier));
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"a", "b", "c"}));
}

// A reply that never confirms fails the flush once the retries are spent,
// keeping the batch, and dead-letters nothing: the records were not refused.
TEST(PubSubSinkContractLive, RepliesThatNeverConfirmFailTheFlushAndKeepTheBatch) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("never"));
    clink::itest::BulkFaultProxy proxy(emulator_url());
    proxy.refuse(3, 200, R"({"messageIds":["only-one"]})");
    auto o = sink_opts(reader.topic(), proxy.url());
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_pubsub_publish_sink(std::move(o));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch({"a", "b"}));
    const auto why = thrown([&] { sink->on_barrier(kBarrier); });
    EXPECT_NE(why.find("confirms 1 of the 2 messages"), std::string::npos) << why;
    EXPECT_TRUE(attached.dlq.records.empty());
    // The proxy forwards again, and the kept batch goes out at the next barrier.
    EXPECT_NO_THROW(sink->on_barrier(kBarrier));
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"a", "b"}));
}

// A refusal the service means as passing (503) is retried, and the messages
// arrive once, since the refused attempt never reached the emulator.
TEST(PubSubSinkContractLive, ATransientRefusalIsRetried) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("transient"));
    clink::itest::BulkFaultProxy proxy(emulator_url());
    proxy.refuse(2, 503, R"({"error":{"code":503,"status":"UNAVAILABLE"}})");
    auto sink = make_pubsub_publish_sink(sink_opts(reader.topic(), proxy.url()));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch({"a", "b"}));
    EXPECT_NO_THROW(sink->on_barrier(kBarrier));
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"a", "b"}));
}

// A reply lost after the emulator published the request: the sink cannot tell
// it from one that published nothing, sends again, and the messages arrive
// twice. At-least-once, as the record says.
TEST(PubSubSinkContractLive, AReplyLostAfterThePublishSendsTheMessagesTwice) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("garbled"));
    clink::itest::BulkFaultProxy proxy(emulator_url());
    proxy.garble(1);
    auto sink = make_pubsub_publish_sink(sink_opts(reader.topic(), proxy.url()));
    Attached attached(*sink);
    sink->open();
    sink->on_data(batch({"a", "b"}));
    EXPECT_NO_THROW(sink->on_barrier(kBarrier));
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"a", "a", "b", "b"}));
}

// A cancelled task ends a publish in flight at once, the batch still pending.
TEST(PubSubSinkContractLive, ACancelEndsAPublishInFlight) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("cancel"));
    const auto hp = emulator_hostport();
    const auto colon = hp.rfind(':');
    clink::itest::TcpFaultProxy proxy(static_cast<std::uint16_t>(std::stoi(hp.substr(colon + 1))),
                                      hp.substr(0, colon));
    auto o = sink_opts(reader.topic(), "http://127.0.0.1:" + std::to_string(proxy.port()));
    o.http.rw_timeout_ms = 60000;
    auto sink = make_pubsub_publish_sink(std::move(o));
    Attached attached(*sink);
    sink->open();
    proxy.set_blackhole(true);
    sink->on_data(batch({"a"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        attached.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { sink->on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    canceller.join();
    EXPECT_NE(why.find("cancelled"), std::string::npos) << why;
    EXPECT_LT(took, std::chrono::seconds{5});
    sink->close();
}

TEST(PubSubSinkContractLive, TheFactoryBuildsASinkThatPublishes) {
    REQUIRE_EMULATOR();
    TopicReader reader(unique("factory"));
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "pubsub_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"project", kProject},
                  {"topic", reader.topic()},
                  {"endpoint", emulator_url()},
                  {"batch_records", "2"}};
    auto sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
    sink->open();
    sink->on_data(batch({R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
    sink->on_barrier(kBarrier);
    sink->close();
    auto got = reader.drain();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{R"({"id":1})", R"({"id":2})", R"({"id":3})"}));
}
