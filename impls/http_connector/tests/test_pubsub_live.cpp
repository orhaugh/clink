// Pub/Sub LIVE integration test. SKIPPED unless CLINK_PUBSUB_EMULATOR_HOST is set
// (e.g. "localhost:8085" from docker/integration-services.yml); the live gate,
// scripts/brokers-live.sh, sets it and fails on any skip. Runs against a
// real Pub/Sub emulator over the REST API: creates a topic + subscription, then
// proves the publish sink and the pull source round-trip every record, that the
// acknowledgement once the checkpoint completes removes the messages
// server-side (a re-pull is empty), and the source's recovery contract
// (PubSubSourceAckLive), judged by what the emulator delivers to another
// subscriber, never by the source's counters.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/pubsub.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using clink::Batch;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StreamElement;
using namespace clink::http_connector;

namespace {

bool emulator_configured() {
    return std::getenv("CLINK_PUBSUB_EMULATOR_HOST") != nullptr;
}
std::string emulator_url() {
    return std::string("http://") + std::getenv("CLINK_PUBSUB_EMULATOR_HOST");
}
const std::string kProject = "clink-test";

std::string unique_name(const char* kind) {
    static int n = 0;
    return std::string("clink-it-") + kind + "-" + std::to_string(static_cast<long>(::getpid())) +
           "-" + std::to_string(n++);
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

}  // namespace

TEST(PubSubLive, PublishPullAckRoundTrip) {
    if (!emulator_configured()) {
        GTEST_SKIP() << "set CLINK_PUBSUB_EMULATOR_HOST (docker/integration-services.yml)";
    }
    const std::string topic = unique_name("topic");
    const std::string sub = unique_name("sub");

    HttpRequest::Options ho;
    ho.base_url = emulator_url();
    HttpRequest admin{ho};
    // Create the topic and subscription (tolerate ALREADY_EXISTS in case the
    // emulator image pre-created them). The admin endpoints are a PUT on the bare
    // resource path (no :publish/:pull verb suffix).
    auto t = admin.put("/v1/projects/" + kProject + "/topics/" + topic, "{}", "application/json");
    ASSERT_TRUE((t.status >= 200 && t.status < 300) || t.status == 409)
        << "create topic: " << t.status << " " << t.body;
    const std::string sub_body = R"({"topic":"projects/)" + kProject + "/topics/" + topic + "\"}";
    auto s = admin.put(
        "/v1/projects/" + kProject + "/subscriptions/" + sub, sub_body, "application/json");
    ASSERT_TRUE((s.status >= 200 && s.status < 300) || s.status == 409)
        << "create subscription: " << s.status << " " << s.body;

    // Publish five records.
    PubSubSinkOptions so;
    so.http.base_url = emulator_url();
    so.project = kProject;
    so.topic = topic;
    so.batch_records = 100;
    auto sink = make_pubsub_publish_sink(so);
    sink->open();
    Batch<std::string> b;
    const std::vector<std::string> sent = {R"({"id":1,"v":"a"})",
                                           R"({"id":2,"v":"b"})",
                                           R"({"id":3,"v":"c"})",
                                           R"({"id":4,"v":"d"})",
                                           R"({"id":5,"v":"e"})"};
    for (const auto& r : sent) {
        b.emplace(std::string{r});
    }
    sink->on_data(b);
    sink->flush();
    sink->close();

    // Pull until all five are collected (the emulator may chunk pulls).
    PubSubSourceOptions ro;
    ro.http.base_url = emulator_url();
    ro.project = kProject;
    ro.subscription = sub;
    ro.poll_interval = std::chrono::milliseconds{0};
    PubSubPullSource src{ro};
    src.open();
    Captured cap;
    auto em = capturing(cap);
    for (int attempt = 0; attempt < 50 && cap.values.size() < sent.size(); ++attempt) {
        src.produce(em);
        if (cap.values.size() < sent.size()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
    }
    std::set<std::string> got(cap.values.begin(), cap.values.end());
    std::set<std::string> want(sent.begin(), sent.end());
    EXPECT_EQ(got, want);
    EXPECT_GE(src.unacked_count(), sent.size());

    // Acknowledge once the checkpoint completes; the messages are then gone
    // server-side.
    InMemoryStateBackend backend;
    src.snapshot_offset(backend, OperatorId{1}, CheckpointId{1});
    EXPECT_GE(src.unacked_count(), sent.size()) << "acknowledged at the barrier";
    src.notify_checkpoint_complete(CheckpointId{1});

    Captured cap2;
    auto em2 = capturing(cap2);
    for (int attempt = 0; attempt < 5; ++attempt) {
        src.produce(em2);
    }
    EXPECT_EQ(src.unacked_count(), 0u);
    EXPECT_TRUE(cap2.values.empty()) << "acked messages must not redeliver";
    src.close();
}

// --- the source's acknowledgement contract against the emulator ----------

namespace {

// A topic and a subscription with the given ack deadline, and a publisher.
struct LiveSubscription {
    explicit LiveSubscription(int ack_deadline_s) {
        HttpRequest::Options ho;
        ho.base_url = emulator_url();
        HttpRequest admin{ho};
        topic = unique_name("topic");
        sub = unique_name("sub");
        auto t =
            admin.put("/v1/projects/" + kProject + "/topics/" + topic, "{}", "application/json");
        ok = (t.status >= 200 && t.status < 300) || t.status == 409;
        const std::string body = R"({"topic":"projects/)" + kProject + "/topics/" + topic +
                                 R"(","ackDeadlineSeconds":)" + std::to_string(ack_deadline_s) +
                                 "}";
        auto s = admin.put(
            "/v1/projects/" + kProject + "/subscriptions/" + sub, body, "application/json");
        ok = ok && ((s.status >= 200 && s.status < 300) || s.status == 409);
    }
    void publish(const std::vector<std::string>& payloads) const {
        PubSubSinkOptions so;
        so.http.base_url = emulator_url();
        so.project = kProject;
        so.topic = topic;
        so.batch_records = 100;
        auto sink = make_pubsub_publish_sink(so);
        sink->open();
        Batch<std::string> b;
        for (const auto& p : payloads) {
            b.emplace(std::string{p});
        }
        sink->on_data(b);
        sink->flush();
        sink->close();
    }
    PubSubSourceOptions source(std::chrono::milliseconds ack_deadline) const {
        PubSubSourceOptions o;
        o.http.base_url = emulator_url();
        o.project = kProject;
        o.subscription = sub;
        o.poll_interval = std::chrono::milliseconds{50};
        o.ack_deadline = ack_deadline;
        return o;
    }
    std::string topic;
    std::string sub;
    bool ok{false};
};

std::vector<std::string> numbered(const std::string& prefix, int n) {
    std::vector<std::string> v;
    for (int i = 0; i < n; ++i) {
        v.push_back(prefix + std::to_string(i));
    }
    return v;
}

// Pull with `src` until `want` records have arrived or `limit` has passed.
std::set<std::string> collect(PubSubPullSource& src, std::size_t want, std::chrono::seconds limit) {
    Captured cap;
    const auto until = std::chrono::steady_clock::now() + limit;
    while (cap.values.size() < want && std::chrono::steady_clock::now() < until) {
        auto em = capturing(cap);
        src.produce(em);
    }
    return {cap.values.begin(), cap.values.end()};
}

}  // namespace

// An aborted checkpoint's messages are not acknowledged; the source hands them
// back as it closes, and another subscriber receives them at once.
TEST(PubSubSourceAckLive, AnAbortedCheckpointsMessagesComeBackWhenTheSourceCloses) {
    if (!emulator_configured()) {
        GTEST_SKIP() << "set CLINK_PUBSUB_EMULATOR_HOST";
    }
    LiveSubscription ls(60);
    ASSERT_TRUE(ls.ok);
    const auto sent = numbered("abort-", 4);
    ls.publish(sent);
    InMemoryStateBackend backend;
    {
        PubSubPullSource src{ls.source(std::chrono::seconds{60})};
        src.open();
        ASSERT_EQ(collect(src, sent.size(), std::chrono::seconds{15}).size(), sent.size());
        src.snapshot_offset(backend, OperatorId{1}, CheckpointId{1});
        src.notify_checkpoint_aborted(CheckpointId{1});
        src.close();
    }
    PubSubPullSource next{ls.source(std::chrono::seconds{60})};
    next.open();
    // Well inside the 60 s lease: only a hand-back explains a redelivery.
    EXPECT_EQ(collect(next, sent.size(), std::chrono::seconds{10}),
              std::set<std::string>(sent.begin(), sent.end()));
    next.close();
}

// The checkpoint completes and the process dies before the acknowledgement is
// posted: the lease runs out and the messages are delivered again.
TEST(PubSubSourceAckLive, ACrashAfterCompletionBeforeTheAcknowledgementDeliversAgain) {
    if (!emulator_configured()) {
        GTEST_SKIP() << "set CLINK_PUBSUB_EMULATOR_HOST";
    }
    LiveSubscription ls(10);
    ASSERT_TRUE(ls.ok);
    const auto sent = numbered("crash-", 3);
    ls.publish(sent);
    InMemoryStateBackend backend;
    {
        PubSubPullSource src{ls.source(std::chrono::seconds{10})};
        src.open();
        ASSERT_EQ(collect(src, sent.size(), std::chrono::seconds{15}).size(), sent.size());
        src.snapshot_offset(backend, OperatorId{1}, CheckpointId{1});
        src.notify_checkpoint_complete(CheckpointId{1});
        // Dies here: no turn posts the acknowledge, no close hands anything back.
    }
    PubSubPullSource restarted{ls.source(std::chrono::seconds{10})};
    restarted.open();
    EXPECT_EQ(collect(restarted, sent.size(), std::chrono::seconds{30}),
              std::set<std::string>(sent.begin(), sent.end()))
        << "a message was lost";
    restarted.close();
}

// The subscription's deadline is 10 s and the checkpoint takes 25: the source
// extends its leases, so another subscriber receives nothing meanwhile, and the
// completion acknowledges every message once.
TEST(PubSubSourceAckLive, LeasesHoldMessagesAcrossACheckpointLongerThanTheDeadline) {
    if (!emulator_configured()) {
        GTEST_SKIP() << "set CLINK_PUBSUB_EMULATOR_HOST";
    }
    LiveSubscription ls(10);
    ASSERT_TRUE(ls.ok);
    const auto sent = numbered("lease-", 3);
    ls.publish(sent);
    InMemoryStateBackend backend;
    PubSubPullSource src{ls.source(std::chrono::seconds{10})};
    src.open();
    ASSERT_EQ(collect(src, sent.size(), std::chrono::seconds{15}).size(), sent.size());
    src.snapshot_offset(backend, OperatorId{1}, CheckpointId{1});
    PubSubPullSource rival{ls.source(std::chrono::seconds{10})};
    rival.open();
    std::set<std::string> stolen;
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds{25};
    while (std::chrono::steady_clock::now() < until) {
        collect(src, 1, std::chrono::seconds{1});  // the source's turns extend its leases
        for (const auto& r : collect(rival, 1, std::chrono::seconds{1})) {
            stolen.insert(r);
        }
    }
    EXPECT_TRUE(stolen.empty()) << "a lease ran out while its checkpoint was delayed";
    src.notify_checkpoint_complete(CheckpointId{1});
    collect(src, 1, std::chrono::seconds{1});
    EXPECT_EQ(src.unacked_count(), 0u);
    src.close();
    // Acknowledged: nobody receives them again, even past the deadline.
    EXPECT_TRUE(collect(rival, 1, std::chrono::seconds{12}).empty());
    rival.close();
}
