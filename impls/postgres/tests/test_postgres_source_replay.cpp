// #60: server-free replay-cursor round-trips for the two Postgres sources.
// snapshot_offset / restore_offset only touch the source's cursor and the state
// backend - no connection - so we exercise them without a live Postgres. Seed a
// backend, restore the cursor, snapshot to a second backend, and verify the
// bytes match: this proves snapshot and restore agree on the state key + the
// 8-byte LE encoding (the bug class the #57 delegation fix was about).

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/connectors/postgres_source.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend.hpp"

using clink::CheckpointId;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::PostgresCdcSource;
using clink::PostgresSource;
using clink::StateBackend;

namespace {
std::array<std::byte, 8> le8(std::uint64_t v) {
    std::array<std::byte, 8> b{};
    for (int i = 0; i < 8; ++i) {
        b[static_cast<std::size_t>(i)] = static_cast<std::byte>((v >> (i * 8)) & 0xFF);
    }
    return b;
}

void seed(InMemoryStateBackend& backend,
          OperatorId op,
          const StateBackend::KeyView& key,
          std::uint64_t value) {
    const auto bytes = le8(value);
    backend.put_operator_state(
        op,
        key,
        StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}
}  // namespace

TEST(PostgresSourceReplay, JdbcRowOffsetRoundTrips) {
    if (!PostgresSource::is_real_implementation()) {
        GTEST_SKIP() << "Built without libpq";
    }
    const OperatorId op{11};
    constexpr const char* kKey = "__postgres_source_row__";
    const StateBackend::KeyView key{kKey, std::strlen(kKey)};

    InMemoryStateBackend seeded;
    seed(seeded, op, key, 99);

    PostgresSource::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1";
    opts.query = "SELECT 1";
    PostgresSource src(std::move(opts));
    ASSERT_TRUE(src.restore_offset(seeded, op));

    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    EXPECT_EQ(out.get_operator_state(op, key), seeded.get_operator_state(op, key));

    InMemoryStateBackend empty;
    PostgresSource::Options o2;
    o2.conninfo = "host=127.0.0.1 port=1";
    o2.query = "SELECT 1";
    PostgresSource src2(std::move(o2));
    EXPECT_FALSE(src2.restore_offset(empty, op));
}

// A checkpoint from a release that recorded the received LSN alone restores
// as a resume position with no transaction in flight, and the next checkpoint
// records it in the versioned encoding.
TEST(PostgresCdcSourceReplay, AnEarlierReleasesLsnRestoresAndIsRecordedVersioned) {
    if (!PostgresCdcSource::is_real_implementation()) {
        GTEST_SKIP() << "Built without libpq";
    }
    const OperatorId op{12};
    constexpr const char* kLegacy = "__postgres_cdc_lsn__";
    constexpr const char* kCursor = "__postgres_cdc_cursor__";

    InMemoryStateBackend seeded;
    seed(seeded, op, StateBackend::KeyView{kLegacy, std::strlen(kLegacy)}, 0x16E2A38ULL);

    PostgresCdcSource::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1";
    opts.slot_name = "test_slot";
    PostgresCdcSource src(std::move(opts));
    ASSERT_TRUE(src.restore_offset(seeded, op));

    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    const auto written =
        out.get_operator_state(op, StateBackend::KeyView{kCursor, std::strlen(kCursor)});
    ASSERT_TRUE(written.has_value());
    ASSERT_EQ(written->size(), 27U);
    EXPECT_EQ(static_cast<int>((*written)[0]), 2) << "encoding version";
    EXPECT_EQ(static_cast<int>((*written)[1]), 2) << "stream phase";
    EXPECT_EQ(static_cast<int>((*written)[2]), 0) << "no transaction in flight";
    std::uint64_t lsn = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        lsn |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*written)[3 + i])) << (i * 8);
    }
    EXPECT_EQ(lsn, 0x16E2A38ULL);

    InMemoryStateBackend empty;
    PostgresCdcSource::Options o2;
    o2.conninfo = "host=127.0.0.1 port=1";
    o2.slot_name = "test_slot";
    PostgresCdcSource src2(std::move(o2));
    EXPECT_FALSE(src2.restore_offset(empty, op));
}

// The versioned position, a transaction in flight included, survives a
// restore and the next checkpoint byte for byte.
TEST(PostgresCdcSourceReplay, TheVersionedPositionRoundTrips) {
    if (!PostgresCdcSource::is_real_implementation()) {
        GTEST_SKIP() << "Built without libpq";
    }
    const OperatorId op{13};
    constexpr const char* kCursor = "__postgres_cdc_cursor__";
    const StateBackend::KeyView key{kCursor, std::strlen(kCursor)};
    std::array<char, 27> bytes{};
    bytes[0] = 2;  // version
    bytes[1] = 1;  // snapshot phase
    bytes[2] = 1;  // in flight
    for (std::size_t i = 0; i < 8; ++i) {
        bytes[3 + i] = static_cast<char>((0x5000ULL >> (i * 8)) & 0xFF);
        bytes[11 + i] = static_cast<char>((0x6000ULL >> (i * 8)) & 0xFF);
        bytes[19 + i] = static_cast<char>((3ULL >> (i * 8)) & 0xFF);
    }
    InMemoryStateBackend seeded;
    seeded.put_operator_state(op, key, StateBackend::ValueView{bytes.data(), bytes.size()});

    PostgresCdcSource::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1";
    opts.slot_name = "test_slot";
    PostgresCdcSource src(std::move(opts));
    ASSERT_TRUE(src.restore_offset(seeded, op));
    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    EXPECT_EQ(out.get_operator_state(op, key), seeded.get_operator_state(op, key));
}

// A position written by a later release is refused, never misread.
TEST(PostgresCdcSourceReplay, AnUnknownEncodingVersionIsRefused) {
    if (!PostgresCdcSource::is_real_implementation()) {
        GTEST_SKIP() << "Built without libpq";
    }
    const OperatorId op{14};
    constexpr const char* kCursor = "__postgres_cdc_cursor__";
    std::array<char, 27> bytes{};
    bytes[0] = 3;
    bytes[1] = 2;
    InMemoryStateBackend seeded;
    seeded.put_operator_state(op,
                              StateBackend::KeyView{kCursor, std::strlen(kCursor)},
                              StateBackend::ValueView{bytes.data(), bytes.size()});
    PostgresCdcSource::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1";
    opts.slot_name = "test_slot";
    PostgresCdcSource src(std::move(opts));
    EXPECT_THROW(src.restore_offset(seeded, op), std::runtime_error);
}
