// Offline tests for the NATS source + sink: required-param validation and clean failure against
// a dead endpoint (no broker needed). The publish->consume round-trip lives in the env-gated
// test_nats_live.cpp.

#include <stdexcept>
#include <utility>

#include <gtest/gtest.h>

#include "clink/nats/nats_sink.hpp"
#include "clink/nats/nats_source.hpp"

using clink::nats::NatsSink;
using clink::nats::NatsSource;

namespace {

// A loopback port with no listener: connect() fails fast.
NatsSource::Options dead_source_opts() {
    NatsSource::Options o;
    o.conn.url = "nats://127.0.0.1:1";
    o.subject = "clink.offline";
    return o;
}

NatsSink::Options dead_sink_opts() {
    NatsSink::Options o;
    o.conn.url = "nats://127.0.0.1:1";
    o.subject = "clink.offline";
    return o;
}

}  // namespace

TEST(NatsSource, RequiresSubject) {
    NatsSource::Options o;  // subject empty
    EXPECT_THROW(NatsSource{std::move(o)}, std::runtime_error);
}

TEST(NatsSink, RequiresSubject) {
    NatsSink::Options o;  // subject empty
    EXPECT_THROW(NatsSink{std::move(o)}, std::runtime_error);
}

TEST(NatsSource, OpenAgainstDeadEndpointFailsCleanly) {
    NatsSource src(dead_source_opts());
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());  // close after a failed open must be safe
}

TEST(NatsSink, OpenAgainstDeadEndpointFailsCleanly) {
    NatsSink sink(dead_sink_opts());
    EXPECT_THROW(sink.open(), std::runtime_error);
    EXPECT_NO_THROW(sink.flush());  // flush with no JetStream context is a no-op
    EXPECT_NO_THROW(sink.close());
}
