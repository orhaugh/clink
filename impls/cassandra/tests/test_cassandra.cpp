// Offline tests for the Cassandra sink: required-param validation and clean failure against a
// dead endpoint (no cluster needed). The insert + read-back lives in the env-gated
// test_cassandra_live.cpp.

#include <chrono>
#include <stdexcept>
#include <utility>

#include <gtest/gtest.h>

#include "clink/cassandra/cassandra_sink.hpp"

using clink::cassandra::CassandraSink;

namespace {

CassandraSink::Options dead_sink_opts() {
    CassandraSink::Options o;
    o.conn.contact_points = "127.0.0.1";
    o.conn.port = 1;
    o.conn.connect_timeout_ms = 2000;  // bound the connect attempt
    o.keyspace = "clink_offline";
    o.table = "t";
    return o;
}

}  // namespace

TEST(CassandraSink, RequiresKeyspaceAndTable) {
    CassandraSink::Options o;  // keyspace + table empty
    EXPECT_THROW(CassandraSink{std::move(o)}, std::runtime_error);
}

TEST(CassandraSink, OpenAgainstDeadEndpointFailsCleanly) {
    CassandraSink sink(dead_sink_opts());
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_THROW(sink.open(), std::runtime_error);
    // Bounded by the connect timeout (2 s here), with room for a slow runner.
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(20));
    // A sink that never opened refuses a flush rather than reporting it written.
    EXPECT_THROW(sink.flush(), std::runtime_error);
    EXPECT_NO_THROW(sink.close());
}
