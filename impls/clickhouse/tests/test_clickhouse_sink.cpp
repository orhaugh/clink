// Smoke tests for the real-impl ClickHouse sink. Don't stand up a
// backing server - exercise constructor / open() / close() against
// an unreachable port to drive the clickhouse-cpp lifecycle code
// through gcov.

#include <exception>
#include <string>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/clickhouse_sink.hpp"
#include "clink/core/types.hpp"

using clink::ClickHouseSink;

namespace {

// Observes the barrier hook's dispatch without a server: the sink's flush() is
// virtual, so a subclass can count the calls the barrier makes.
class FlushCountingSink : public ClickHouseSink {
public:
    using ClickHouseSink::ClickHouseSink;
    void flush() override { ++flushes; }
    int flushes{0};
};

}  // namespace

// The barrier is the moment buffered rows have to be durable: the runner
// snapshots and acks the checkpoint right after on_barrier returns, and a
// recovery resumes the source past the records already consumed. A sink that
// kept rows buffered across the barrier lost them on the next crash - the
// tutorial's Kafka -> ClickHouse pipeline would have shown missing windows
// after a Worker kill, against a connector documented as at-least-once.
TEST(ClickHouseSink, ABarrierFlushesTheBufferedRows) {
    ClickHouseSink::Options opts;
    opts.table = "events";
    FlushCountingSink sink(std::move(opts));
    clink::Batch<std::string> batch;
    batch.emplace(std::string{R"({"sensor_id":"sensor-01","readings":10})"});
    sink.on_data(batch);  // buffered; never opened, so nothing reaches a client
    EXPECT_EQ(sink.flushes, 0) << "on_data alone must not flush a one-row buffer";
    sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{7}});
    EXPECT_EQ(sink.flushes, 1) << "the barrier must flush before the checkpoint is acked";
    sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{8}, /*terminal=*/true});
    EXPECT_EQ(sink.flushes, 2) << "a terminal barrier flushes too";
}

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
