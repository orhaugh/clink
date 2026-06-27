// Offline logic tests for RedisSink: option validation and pre-open safety. No
// Redis server required (no open()/flush-with-data, so hiredis is never called).

#include <stdexcept>

#include <gtest/gtest.h>

#include "clink/redis/redis_sink.hpp"

namespace {

using clink::redis::RedisSink;
using clink::redis::RedisSinkOptions;

RedisSinkOptions valid_opts() {
    RedisSinkOptions o;
    o.stream = "s";
    return o;
}

TEST(RedisSinkLogic, EmptyStreamThrows) {
    RedisSinkOptions o;  // stream empty
    EXPECT_THROW(RedisSink{std::move(o)}, std::runtime_error);
}

TEST(RedisSinkLogic, EmptyFieldThrows) {
    RedisSinkOptions o = valid_opts();
    o.field = "";
    EXPECT_THROW(RedisSink{std::move(o)}, std::runtime_error);
}

TEST(RedisSinkLogic, ValidConstructsAndNames) {
    RedisSink sink{valid_opts()};
    EXPECT_EQ(sink.name(), "redis_sink");
}

TEST(RedisSinkLogic, FlushAndCloseBeforeOpenAreSafe) {
    // With nothing buffered, flush()/close() must not touch the (absent)
    // connection.
    RedisSink sink{valid_opts()};
    EXPECT_NO_THROW(sink.flush());
    EXPECT_NO_THROW(sink.close());
}

}  // namespace
