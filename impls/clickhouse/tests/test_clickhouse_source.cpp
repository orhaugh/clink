// Smoke tests for ClickHouseSource. Mirrors the sink suite: we don't
// stand up a real ClickHouse server - we exercise the constructor,
// the missing-query guard, and the unreachable-host error path so the
// lifecycle code lights up under gcov.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "clink/connectors/clickhouse_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using clink::CheckpointId;
using clink::ClickHouseSource;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StateBackend;

namespace {
// 8-byte little-endian encoding of a u64, matching the source's offset codec.
std::array<std::byte, 8> le8(std::uint64_t v) {
    std::array<std::byte, 8> b{};
    for (int i = 0; i < 8; ++i) {
        b[static_cast<std::size_t>(i)] = static_cast<std::byte>((v >> (i * 8)) & 0xFF);
    }
    return b;
}
}  // namespace

TEST(ClickHouseSourceReal, ConstructorIsClean) {
    if (!ClickHouseSource::is_real_implementation()) {
        GTEST_SKIP() << "Built without clickhouse-cpp; real-impl path not exercised";
    }
    ClickHouseSource::Options opts;
    opts.host = "127.0.0.1";
    opts.port = 1;
    opts.query = "SELECT 1";
    ClickHouseSource src(std::move(opts));
    SUCCEED();
}

TEST(ClickHouseSourceReal, MissingQueryThrowsAtOpen) {
    if (!ClickHouseSource::is_real_implementation()) {
        GTEST_SKIP();
    }
    ClickHouseSource::Options opts;
    opts.host = "127.0.0.1";
    opts.port = 1;
    // opts.query intentionally empty.
    ClickHouseSource src(std::move(opts));
    EXPECT_THROW(src.open(), std::exception);
}

TEST(ClickHouseSourceReal, OpenAgainstDeadEndpointFailsCleanly) {
    if (!ClickHouseSource::is_real_implementation()) {
        GTEST_SKIP();
    }
    ClickHouseSource::Options opts;
    opts.host = "127.0.0.1";
    opts.port = 1;  // Nothing listens here; clickhouse-cpp's connect() throws.
    opts.query = "SELECT 1";
    ClickHouseSource src(std::move(opts));
    EXPECT_THROW(src.open(), std::exception);
    EXPECT_NO_THROW(src.close());
}

// #60: the replay cursor round-trips through restore_offset -> snapshot_offset
// without a live server. Seed a backend with a row offset, restore it into the
// source's cursor, snapshot to a second backend, and verify the bytes match -
// proving snapshot and restore agree on the state key and encoding.
TEST(ClickHouseSourceReal, ReplayOffsetRoundTrips) {
    if (!ClickHouseSource::is_real_implementation()) {
        GTEST_SKIP();
    }
    const OperatorId op{7};
    constexpr const char* kKey = "__clickhouse_source_row__";
    const StateBackend::KeyView key{kKey, std::strlen(kKey)};

    InMemoryStateBackend seeded;
    const auto bytes = le8(42);
    seeded.put_operator_state(
        op,
        key,
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});

    ClickHouseSource::Options opts;
    opts.host = "127.0.0.1";
    opts.port = 1;
    opts.query = "SELECT 1";
    ClickHouseSource src(std::move(opts));
    ASSERT_TRUE(src.restore_offset(seeded, op));

    // An earlier release's 8-byte position restores and is written again in
    // the versioned form: a version byte, the index and a hash.
    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    const auto written = out.get_operator_state(op, key);
    ASSERT_TRUE(written.has_value());
    ASSERT_EQ(written->size(), 17U);
    EXPECT_EQ(std::to_integer<int>((*written)[0]), 2);
    EXPECT_EQ(std::to_integer<int>((*written)[1]), 42);

    // The versioned form round-trips exactly.
    ClickHouseSource::Options o3;
    o3.host = "127.0.0.1";
    o3.port = 1;
    o3.query = "SELECT 1";
    ClickHouseSource src3(std::move(o3));
    ASSERT_TRUE(src3.restore_offset(out, op));
    InMemoryStateBackend again;
    src3.snapshot_offset(again, op, CheckpointId{2});
    EXPECT_EQ(again.get_operator_state(op, key), written);

    // A form neither release wrote is refused.
    InMemoryStateBackend odd;
    const std::string five = "abcde";
    odd.put_operator_state(op, key, StateBackend::ValueView{five.data(), five.size()});
    ClickHouseSource::Options o4;
    o4.host = "127.0.0.1";
    o4.port = 1;
    o4.query = "SELECT 1";
    ClickHouseSource src4(std::move(o4));
    EXPECT_THROW((void)src4.restore_offset(odd, op), std::runtime_error);

    // A backend with no persisted offset -> restore is a no-op returning false.
    InMemoryStateBackend empty;
    ClickHouseSource::Options o2;
    o2.host = "127.0.0.1";
    o2.port = 1;
    o2.query = "SELECT 1";
    ClickHouseSource src2(std::move(o2));
    EXPECT_FALSE(src2.restore_offset(empty, op));
}
