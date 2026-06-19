// Smoke tests for ClickHouseSource. Mirrors the sink suite: we don't
// stand up a real ClickHouse server - we exercise the constructor,
// the missing-query guard, and the unreachable-host error path so the
// lifecycle code lights up under gcov.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>

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

    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    EXPECT_EQ(out.get_operator_state(op, key), seeded.get_operator_state(op, key));

    // A backend with no persisted offset -> restore is a no-op returning false.
    InMemoryStateBackend empty;
    ClickHouseSource::Options o2;
    o2.host = "127.0.0.1";
    o2.port = 1;
    o2.query = "SELECT 1";
    ClickHouseSource src2(std::move(o2));
    EXPECT_FALSE(src2.restore_offset(empty, op));
}
