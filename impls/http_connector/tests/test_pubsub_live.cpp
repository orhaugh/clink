// Pub/Sub LIVE integration test. SKIPPED unless CLINK_PUBSUB_EMULATOR_HOST is set
// (e.g. "localhost:8085" from docker/integration-services.yml). Runs against a
// real Pub/Sub emulator over the REST API: creates a topic + subscription, then
// proves the publish sink and the pull source round-trip every record and that
// the checkpoint ack removes the messages server-side (a re-pull is empty).

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

    // Acknowledge on a checkpoint; the messages are then gone server-side.
    InMemoryStateBackend backend;
    src.snapshot_offset(backend, OperatorId{1}, CheckpointId{1});
    EXPECT_EQ(src.unacked_count(), 0u);

    Captured cap2;
    auto em2 = capturing(cap2);
    for (int attempt = 0; attempt < 5; ++attempt) {
        src.produce(em2);
    }
    EXPECT_TRUE(cap2.values.empty()) << "acked messages must not redeliver";
    src.close();
}
