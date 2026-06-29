// Offline tests for the RabbitMQ source + sink: required-param validation and clean failure
// against a dead endpoint (no broker needed). The publish->consume round-trip lives in the
// env-gated test_rabbitmq_live.cpp.

#include <stdexcept>
#include <utility>

#include <gtest/gtest.h>

#include "clink/rabbitmq/rabbitmq_sink.hpp"
#include "clink/rabbitmq/rabbitmq_source.hpp"

using clink::rabbitmq::RabbitMqSink;
using clink::rabbitmq::RabbitMqSource;

namespace {

// A loopback port with no listener: connect() fails fast with ECONNREFUSED.
RabbitMqSource::Options dead_source_opts() {
    RabbitMqSource::Options o;
    o.conn.host = "127.0.0.1";
    o.conn.port = 1;
    o.queue = "clink-offline";
    return o;
}

RabbitMqSink::Options dead_sink_opts() {
    RabbitMqSink::Options o;
    o.conn.host = "127.0.0.1";
    o.conn.port = 1;
    o.routing_key = "clink-offline";
    return o;
}

}  // namespace

TEST(RabbitMqSource, RequiresQueue) {
    RabbitMqSource::Options o;  // queue empty
    EXPECT_THROW(RabbitMqSource{std::move(o)}, std::runtime_error);
}

TEST(RabbitMqSink, RequiresRoutingKey) {
    RabbitMqSink::Options o;  // routing_key empty
    EXPECT_THROW(RabbitMqSink{std::move(o)}, std::runtime_error);
}

TEST(RabbitMqSource, OpenAgainstDeadEndpointFailsCleanly) {
    RabbitMqSource src(dead_source_opts());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());  // close after a failed open must be safe
}

TEST(RabbitMqSink, OpenAgainstDeadEndpointFailsCleanly) {
    RabbitMqSink sink(dead_sink_opts());
    EXPECT_THROW(sink.open(), std::runtime_error);
    EXPECT_NO_THROW(sink.flush());  // flush with no connection is a no-op
    EXPECT_NO_THROW(sink.close());
}
