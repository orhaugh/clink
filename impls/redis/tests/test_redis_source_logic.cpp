// Offline logic tests for RedisSource: option validation, the unbounded
// contract, which group consumers count as stranded by a lower parallelism, and
// that nothing is acknowledged before a checkpoint completes. No Redis server
// required (no open(), so hiredis is never called).

#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/redis/redis_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

namespace {

using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StateBackend;
using clink::redis::GroupConsumer;
using clink::redis::RedisSource;
using clink::redis::RedisSourceOptions;
using clink::redis::stranded_consumers;

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

// A consumer counts as stranded when it is named for a subtask this layout does
// not run and still holds pending entries; anything else is left alone.
TEST(RedisSourceLogic, StrandedConsumersAreThoseNamedBeyondTheParallelismWithPending) {
    const std::vector<GroupConsumer> consumers = {
        {"clink-0", 5},     // this layout's
        {"clink-1", 2},     // this layout's
        {"clink-2", 3},     // beyond parallelism 2, pending: stranded
        {"clink-3", 0},     // beyond, but holds nothing
        {"clink-10", 1},    // beyond: stranded
        {"other-5", 4},     // another prefix: not this job's naming
        {"clink-x", 4},     // not a subtask index
        {"clink-", 4},      // no index at all
        {"clink-01x", 4},   // not all digits
        {"clinkish-7", 4},  // a longer prefix that only starts like ours
    };
    const auto stranded = stranded_consumers("clink", 2, consumers);
    ASSERT_EQ(stranded.size(), 2u);
    EXPECT_EQ(stranded[0].name, "clink-2");
    EXPECT_EQ(stranded[1].name, "clink-10");
    EXPECT_TRUE(stranded_consumers("clink", 11, consumers).empty());
    EXPECT_EQ(stranded_consumers("clink", 1, consumers).size(), 3u) << "clink-1 too";
}

TEST(RedisSourceLogic, NothingIsAcknowledgedBeforeACheckpointCompletes) {
    // No connection: a completion only moves entries in the ledger, and the
    // XACK waits for the source thread.
    RedisSource src{valid_opts()};
    InMemoryStateBackend backend;
    src.snapshot_offset(backend, OperatorId{1}, clink::CheckpointId{1});
    src.notify_checkpoint_complete(clink::CheckpointId{1});
    src.notify_checkpoint_aborted(clink::CheckpointId{2});
    EXPECT_EQ(src.unacknowledged(), 0u);
    EXPECT_EQ(src.acknowledged_total(), 0u);
}

TEST(RedisSourceLogic, AMaxPendingOfZeroIsRaisedToOne) {
    auto o = valid_opts();
    o.max_pending = 0;
    EXPECT_NO_THROW(RedisSource{std::move(o)});
}

}  // namespace
