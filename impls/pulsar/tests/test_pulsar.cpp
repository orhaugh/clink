// Offline tests for the Pulsar source + sink: required-param validation and clean failure
// against a dead endpoint (no broker needed). The publish->consume round-trip lives in the
// env-gated test_pulsar_live.cpp.

#include <stdexcept>
#include <utility>

#include <gtest/gtest.h>

#include "clink/pulsar/pulsar_sink.hpp"
#include "clink/pulsar/pulsar_source.hpp"

using clink::pulsar::PulsarSink;
using clink::pulsar::PulsarSource;

namespace {

// A loopback port with no broker; a short operation timeout bounds the connect retries.
PulsarSource::Options dead_source_opts() {
    PulsarSource::Options o;
    o.conn.service_url = "pulsar://127.0.0.1:1";
    o.conn.operation_timeout_s = 5;
    o.topic = "clink-offline";
    return o;
}

PulsarSink::Options dead_sink_opts() {
    PulsarSink::Options o;
    o.conn.service_url = "pulsar://127.0.0.1:1";
    o.conn.operation_timeout_s = 5;
    o.topic = "clink-offline";
    return o;
}

}  // namespace

TEST(PulsarSource, RequiresTopic) {
    PulsarSource::Options o;  // topic empty
    EXPECT_THROW(PulsarSource{std::move(o)}, std::runtime_error);
}

TEST(PulsarSink, RequiresTopic) {
    PulsarSink::Options o;  // topic empty
    EXPECT_THROW(PulsarSink{std::move(o)}, std::runtime_error);
}

TEST(PulsarSource, OpenAgainstDeadEndpointFailsCleanly) {
    PulsarSource src(dead_source_opts());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());  // close after a failed open must be safe
}

TEST(PulsarSink, OpenAgainstDeadEndpointFailsCleanly) {
    PulsarSink sink(dead_sink_opts());
    EXPECT_THROW(sink.open(), std::runtime_error);
    EXPECT_NO_THROW(sink.flush());  // flush with no producer is a no-op
    EXPECT_NO_THROW(sink.close());
}
