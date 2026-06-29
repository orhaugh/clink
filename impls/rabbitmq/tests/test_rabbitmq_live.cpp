// RabbitMQ LIVE integration test. SKIPPED unless CLINK_RABBITMQ_TEST_ENDPOINT is set (the
// broker host, e.g. localhost). Port/user/password come from CLINK_RABBITMQ_TEST_{PORT,USER,
// PASSWORD} (defaults 5672 / guest / guest). Proves against a real broker: the sink publishes
// (with publisher confirms) and the source consumes the same messages back.

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
#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/rabbitmq/rabbitmq_source.hpp"

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::rabbitmq::RabbitMqConnParams;
using clink::rabbitmq::RabbitMqSink;
using clink::rabbitmq::RabbitMqSource;

namespace {

bool rmq_configured() {
    return std::getenv("CLINK_RABBITMQ_TEST_ENDPOINT") != nullptr;
}

std::string env_or(const char* k, const std::string& dflt) {
    const char* v = std::getenv(k);
    return v != nullptr ? std::string(v) : dflt;
}

RabbitMqConnParams conn_from_env() {
    RabbitMqConnParams c;
    c.host = std::getenv("CLINK_RABBITMQ_TEST_ENDPOINT");
    c.port = std::stoi(env_or("CLINK_RABBITMQ_TEST_PORT", "5672"));
    c.user = env_or("CLINK_RABBITMQ_TEST_USER", "guest");
    c.password = env_or("CLINK_RABBITMQ_TEST_PASSWORD", "guest");
    return c;
}

std::string unique_queue() {
    static int n = 0;
    return "clink_it_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

}  // namespace

TEST(RabbitMqLive, PublishThenConsumeRoundTrip) {
    if (!rmq_configured()) {
        GTEST_SKIP() << "set CLINK_RABBITMQ_TEST_ENDPOINT (+ _PORT/_USER/_PASSWORD)";
    }
    const std::string queue = unique_queue();

    // Open the source first: it declares the (durable) queue and registers the consumer, so the
    // messages the sink publishes next are routed to a queue that already has a consumer.
    RabbitMqSource::Options so;
    so.conn = conn_from_env();
    so.queue = queue;
    so.poll_timeout = std::chrono::milliseconds{200};
    RabbitMqSource src(std::move(so));
    src.open();

    // Default exchange ("") routes by routing_key == queue name.
    RabbitMqSink::Options ko;
    ko.conn = conn_from_env();
    ko.routing_key = queue;
    RabbitMqSink sink(std::move(ko));
    sink.open();

    Batch<std::string> out;
    out.emplace(R"({"id":1,"v":"a"})");
    out.emplace(R"({"id":2,"v":"b"})");
    out.emplace(R"({"id":3,"v":"c"})");
    sink.on_data(out);
    sink.flush();  // block until the broker confirms all three (throws on failure)

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
