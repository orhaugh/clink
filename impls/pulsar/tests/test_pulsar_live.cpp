// Apache Pulsar LIVE integration test. SKIPPED unless CLINK_PULSAR_TEST_ENDPOINT is set (a
// Pulsar service URL, e.g. pulsar://localhost:6650). Proves against a real broker: the sink
// publishes (acks awaited) and the source consumes the same messages back. The topic is
// auto-created by the broker (allowAutoTopicCreation), so no provisioning is needed; the source
// subscribes BEFORE the sink publishes so the subscription cursor covers the messages.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/pulsar/pulsar_source.hpp"

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::pulsar::PulsarSink;
using clink::pulsar::PulsarSource;

namespace {

bool pulsar_configured() {
    return std::getenv("CLINK_PULSAR_TEST_ENDPOINT") != nullptr;
}
std::string pulsar_url() {
    return std::getenv("CLINK_PULSAR_TEST_ENDPOINT");
}

std::string unique_suffix() {
    static int n = 0;
    return std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

}  // namespace

TEST(PulsarLive, PublishThenConsumeRoundTrip) {
    if (!pulsar_configured()) {
        GTEST_SKIP() << "set CLINK_PULSAR_TEST_ENDPOINT (e.g. pulsar://localhost:6650)";
    }
    const std::string topic = "clink-it-" + unique_suffix();

    // Subscribe first so the (durable) subscription cursor is positioned before the messages the
    // sink publishes next; the broker auto-creates the topic on subscribe.
    PulsarSource::Options so;
    so.conn.service_url = pulsar_url();
    so.topic = topic;
    so.subscription = "clink-it-sub-" + unique_suffix();
    so.receive_timeout = std::chrono::milliseconds{500};
    PulsarSource src(std::move(so));
    src.open();

    PulsarSink::Options ko;
    ko.conn.service_url = pulsar_url();
    ko.topic = topic;
    PulsarSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    out.emplace(R"({"id":2,"v":"b"})");
    out.emplace(R"({"id":3,"v":"c"})");
    sink.on_data(out);
    sink.flush();  // block until the broker persists all three (throws on failure)

    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{15};
    while (got.size() < 3 && std::chrono::steady_clock::now() < deadline) {
        src.produce(em);
    }

    ASSERT_EQ(got.size(), 3u) << "did not consume all published messages";
    auto has = [&](const std::string& needle) {
        return std::any_of(got.begin(), got.end(), [&](const std::string& s) {
            return s.find(needle) != std::string::npos;
        });
    };
    EXPECT_TRUE(has("\"id\":1"));
    EXPECT_TRUE(has("\"id\":2"));
    EXPECT_TRUE(has("\"id\":3"));

    sink.close();
    src.close();
}
