// Kinesis source: the pure shard-to-subtask assignment (each subtask owns a
// modulo-slice of the shards) and construction validation. The full read +
// checkpoint round-trip is exercised by the endpoint-gated LocalStack test.

#include <string>

#include <gtest/gtest.h>

#include "clink/aws/kinesis_source.hpp"

using clink::aws::kinesis_shard_assigned;
using clink::aws::KinesisSource;
using clink::aws::KinesisSourceOptions;

TEST(KinesisShardAssignment, ModuloSlicePartitionsShardsAcrossSubtasks) {
    // parallelism 3: shard i goes to subtask i % 3, exactly once, no gaps.
    for (std::size_t shard = 0; shard < 9; ++shard) {
        int owners = 0;
        for (std::uint32_t sub = 0; sub < 3; ++sub) {
            if (kinesis_shard_assigned(shard, sub, 3)) {
                ++owners;
                EXPECT_EQ(shard % 3, sub);
            }
        }
        EXPECT_EQ(owners, 1) << "shard " << shard << " must have exactly one owner";
    }
}

TEST(KinesisShardAssignment, SingleSubtaskOwnsEverything) {
    for (std::size_t shard = 0; shard < 5; ++shard) {
        EXPECT_TRUE(kinesis_shard_assigned(shard, 0, 1));
    }
}

TEST(KinesisSourceCtor, RequiresStream) {
    KinesisSourceOptions o;  // no stream
    EXPECT_THROW(KinesisSource{std::move(o)}, std::runtime_error);

    KinesisSourceOptions o2;
    o2.stream = "my-stream";
    EXPECT_NO_THROW(KinesisSource{std::move(o2)});  // ctor does not touch AWS
}

TEST(KinesisSourceCtor, IsUnbounded) {
    KinesisSourceOptions o;
    o.stream = "s";
    KinesisSource src{std::move(o)};
    EXPECT_FALSE(src.is_bounded());
}

TEST(KinesisSourceCtor, RejectsInvalidInitialPosition) {
    KinesisSourceOptions o;
    o.stream = "s";
    o.initial_position = "lastest";  // typo: must not silently coerce to TRIM_HORIZON
    EXPECT_THROW(KinesisSource{std::move(o)}, std::runtime_error);

    for (const char* ok : {"trim_horizon", "latest"}) {
        KinesisSourceOptions g;
        g.stream = "s";
        g.initial_position = ok;
        EXPECT_NO_THROW(KinesisSource{std::move(g)});
    }
}
