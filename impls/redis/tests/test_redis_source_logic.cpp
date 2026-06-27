// Offline logic tests for RedisSource: option validation and the unbounded
// contract. No Redis server required (no open(), so hiredis is never called).

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

namespace {

using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StateBackend;
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

// restore_offset scans operator state for the "__redis_last_id__:<stream>" key.
// Offline (no server): a persisted cursor for our stream is found; an unrelated
// key is filtered out.
TEST(RedisSourceLogic, RestoreOffsetFindsPersistedCursorForStream) {
    const OperatorId op{1};
    const std::string id = "5-0";
    InMemoryStateBackend has_it;
    has_it.put_operator_state(op,
                              "__redis_last_id__:s",  // valid_opts() stream is "s"
                              StateBackend::ValueView{id.data(), id.size()});
    RedisSource src{valid_opts()};
    EXPECT_TRUE(src.restore_offset(has_it, op));

    InMemoryStateBackend unrelated;
    const std::string junk = "x";
    unrelated.put_operator_state(
        op, "some_other_key", StateBackend::ValueView{junk.data(), junk.size()});
    RedisSource src2{valid_opts()};
    EXPECT_FALSE(src2.restore_offset(unrelated, op));
}

}  // namespace
