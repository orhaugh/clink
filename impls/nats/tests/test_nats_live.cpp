// NATS JetStream LIVE integration test. SKIPPED unless CLINK_NATS_TEST_ENDPOINT is set (a NATS
// URL, e.g. nats://localhost:4222). Proves against a real JetStream-enabled server: the sink
// publishes (acks awaited) and the source's durable pull consumer reads the same messages back.
// The test creates the stream up front via the nats.c API (stream provisioning is normally an
// admin task, so the connectors do not do it).

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <nats/nats.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/nats/nats_sink.hpp"
#include "clink/nats/nats_source.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::nats::NatsSink;
using clink::nats::NatsSource;

namespace {

bool nats_configured() {
    return std::getenv("CLINK_NATS_TEST_ENDPOINT") != nullptr;
}
std::string nats_url() {
    return std::getenv("CLINK_NATS_TEST_ENDPOINT");
}

std::string unique_name(const char* prefix) {
    static int n = 0;
    return std::string(prefix) + "_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(n++);
}

// Create a JetStream stream over `subject`. Returns true on success.
bool create_stream(const std::string& stream, const std::string& subject) {
    natsConnection* nc = nullptr;
    if (natsConnection_ConnectTo(&nc, nats_url().c_str()) != NATS_OK) {
        return false;
    }
    jsCtx* js = nullptr;
    bool ok = (natsConnection_JetStream(&js, nc, nullptr) == NATS_OK);
    if (ok) {
        jsStreamConfig cfg;
        jsStreamConfig_Init(&cfg);
        cfg.Name = stream.c_str();
        const char* subjects[] = {subject.c_str()};
        cfg.Subjects = subjects;
        cfg.SubjectsLen = 1;
        ok = (js_AddStream(nullptr, js, &cfg, nullptr, nullptr) == NATS_OK);
    }
    jsCtx_Destroy(js);
    natsConnection_Destroy(nc);
    return ok;
}

}  // namespace

TEST(NatsLive, PublishThenConsumeRoundTrip) {
    if (!nats_configured()) {
        GTEST_SKIP() << "set CLINK_NATS_TEST_ENDPOINT (e.g. nats://localhost:4222)";
    }
    const std::string stream = unique_name("CLINK_IT");
    const std::string subject = "clink.it." + unique_name("s");
    ASSERT_TRUE(create_stream(stream, subject)) << "could not create JetStream stream";

    NatsSink::Options ko;
    ko.conn.url = nats_url();
    ko.subject = subject;
    NatsSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    out.emplace(R"({"id":2,"v":"b"})");
    out.emplace(R"({"id":3,"v":"c"})");
    sink.on_data(out);
    sink.flush();  // block until the server acks all three (throws on failure)

    NatsSource::Options so;
    so.conn.url = nats_url();
    so.subject = subject;
    so.stream = stream;
    so.durable = unique_name("d");
    so.fetch_timeout = std::chrono::milliseconds{500};
    NatsSource src(std::move(so));
    src.open();

    std::vector<std::string> got;
    Emitter<std::string> em([&](StreamElement<std::string> e) -> bool {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                got.push_back(rec.value());
            }
        }
        return true;
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
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
