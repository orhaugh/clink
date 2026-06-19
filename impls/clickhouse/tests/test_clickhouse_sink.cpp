// Smoke tests for the real-impl ClickHouse sink. Don't stand up a
// backing server - exercise constructor / open() / close() against
// an unreachable port to drive the clickhouse-cpp lifecycle code
// through gcov.

#include <exception>

#include <gtest/gtest.h>

#include "clink/connectors/clickhouse_sink.hpp"

using clink::ClickHouseSink;

TEST(ClickHouseSinkReal, ConstructorIsClean) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP() << "Built without clickhouse-cpp; real-impl path not exercised";
    }
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.host = "127.0.0.1";
    opts.port = 1;
    ClickHouseSink sink(std::move(opts));
    SUCCEED();
}

TEST(ClickHouseSinkReal, OpenAgainstDeadEndpointFailsCleanly) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP();
    }
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.host = "127.0.0.1";
    opts.port = 1;
    ClickHouseSink sink(std::move(opts));
    EXPECT_THROW(sink.open(), std::exception);
    EXPECT_NO_THROW(sink.close());
}

TEST(ClickHouseSinkReal, FlushAndCloseBeforeOpenAreSafe) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP();
    }
    ClickHouseSink::Options opts;
    opts.table = "events";
    ClickHouseSink sink(std::move(opts));
    EXPECT_NO_THROW(sink.flush());
    EXPECT_NO_THROW(sink.close());
}
