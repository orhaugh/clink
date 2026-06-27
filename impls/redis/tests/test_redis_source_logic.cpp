// Offline logic tests for RedisSource: option validation and the unbounded
// contract. No Redis server required (no open(), so hiredis is never called).

#include <stdexcept>

#include <gtest/gtest.h>

#include "clink/redis/redis_source.hpp"

namespace {

using clink::redis::RedisSource;
using clink::redis::RedisSourceOptions;

RedisSourceOptions valid_opts() {
    RedisSourceOptions o;
    o.stream = "s";
    o.group = "g";
    return o;
}

TEST(RedisSourceLogic, EmptyStreamThrows) {
    RedisSourceOptions o;
    o.group = "g";  // stream empty
    EXPECT_THROW(RedisSource{std::move(o)}, std::runtime_error);
}

TEST(RedisSourceLogic, EmptyGroupThrows) {
    RedisSourceOptions o;
    o.stream = "s";  // group empty
    EXPECT_THROW(RedisSource{std::move(o)}, std::runtime_error);
}

TEST(RedisSourceLogic, ValidConstructsUnboundedAndNames) {
    RedisSource src{valid_opts()};
    EXPECT_FALSE(src.is_bounded());
    EXPECT_EQ(src.name(), "redis_source");
    EXPECT_TRUE(src.last_delivered_id().empty());
}

}  // namespace
