// Smoke tests for the real-impl S3 source/sink. Don't stand up a
// backing server - exercise constructor / open() / SDK-init / close() /
// destructor against an unreachable endpoint to drive the SDK lifecycle
// code through gcov.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <gtest/gtest.h>

#include "clink/connectors/s3_sink.hpp"
#include "clink/connectors/s3_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::S3Sink;
using clink::S3Source;
using clink::StateBackend;

// ----- S3Sink -----

TEST(S3SinkReal, ConstructorAndDestructorAreClean) {
    if (!S3Sink::is_real_implementation()) {
        GTEST_SKIP() << "Built without AWS SDK; real-impl path not exercised";
    }
    S3Sink::Options opts;
    opts.bucket = "test-bucket";
    opts.key_prefix = "prefix";
    opts.endpoint_override = "http://127.0.0.1:1";  // unreachable
    S3Sink sink(std::move(opts));
    SUCCEED();
}

TEST(S3SinkReal, OpenInitializesSdkEvenWithoutBroker) {
    if (!S3Sink::is_real_implementation()) {
        GTEST_SKIP();
    }
    S3Sink::Options opts;
    opts.bucket = "x";
    opts.key_prefix = "p";
    opts.region = "us-east-1";
    opts.endpoint_override = "http://127.0.0.1:1";
    S3Sink sink(std::move(opts));
    EXPECT_NO_THROW(sink.open());
    EXPECT_NO_THROW(sink.close());
}

TEST(S3SinkReal, FlushOnEmptyBufferIsNoOp) {
    if (!S3Sink::is_real_implementation()) {
        GTEST_SKIP();
    }
    S3Sink::Options opts;
    opts.bucket = "x";
    opts.key_prefix = "p";
    opts.endpoint_override = "http://127.0.0.1:1";
    S3Sink sink(std::move(opts));
    sink.open();
    EXPECT_NO_THROW(sink.flush());
    sink.close();
}

// ----- S3Source -----

TEST(S3SourceReal, ConstructorIsClean) {
    if (!S3Source::is_real_implementation()) {
        GTEST_SKIP();
    }
    S3Source::Options opts;
    opts.bucket = "x";
    opts.key_prefix = "p";
    opts.region = std::nullopt;
    opts.endpoint_override = "http://127.0.0.1:1";
    S3Source src(std::move(opts));
    SUCCEED();
}

TEST(S3SourceReal, OpenAgainstDeadEndpointFailsCleanly) {
    if (!S3Source::is_real_implementation()) {
        GTEST_SKIP();
    }
    S3Source::Options opts;
    opts.bucket = "x";
    opts.key_prefix = "p";
    opts.endpoint_override = "http://127.0.0.1:1";
    S3Source src(std::move(opts));
    EXPECT_THROW(src.open(), std::runtime_error);
    EXPECT_NO_THROW(src.close());
}

// #60: the object-index replay cursor round-trips through restore_offset ->
// snapshot_offset without an S3 endpoint. Proves snapshot and restore agree on
// the state key and 8-byte LE encoding.
TEST(S3SourceReal, ReplayOffsetRoundTrips) {
    if (!S3Source::is_real_implementation()) {
        GTEST_SKIP();
    }
    const OperatorId op{13};
    constexpr const char* kKey = "__s3_source_object__";
    const StateBackend::KeyView key{kKey, std::strlen(kKey)};

    InMemoryStateBackend seeded;
    std::array<std::byte, 8> bytes{};
    const std::uint64_t value = 5;
    for (int i = 0; i < 8; ++i) {
        bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>((value >> (i * 8)) & 0xFF);
    }
    seeded.put_operator_state(
        op,
        key,
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});

    S3Source::Options opts;
    opts.bucket = "x";
    opts.key_prefix = "p";
    opts.endpoint_override = "http://127.0.0.1:1";
    S3Source src(std::move(opts));
    ASSERT_TRUE(src.restore_offset(seeded, op));

    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    EXPECT_EQ(out.get_operator_state(op, key), seeded.get_operator_state(op, key));

    InMemoryStateBackend empty;
    S3Source::Options o2;
    o2.bucket = "x";
    o2.key_prefix = "p";
    o2.endpoint_override = "http://127.0.0.1:1";
    S3Source src2(std::move(o2));
    EXPECT_FALSE(src2.restore_offset(empty, op));
}
