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

TEST(PostgresCdcSourceReplay, LsnRoundTrips) {
    if (!PostgresCdcSource::is_real_implementation()) {
        GTEST_SKIP() << "Built without libpq";
    }
    const OperatorId op{12};
    constexpr const char* kKey = "__postgres_cdc_lsn__";
    const StateBackend::KeyView key{kKey, std::strlen(kKey)};

    InMemoryStateBackend seeded;
    seed(seeded, op, key, 0x16E2A38ULL);

    PostgresCdcSource::Options opts;
    opts.conninfo = "host=127.0.0.1 port=1";
    opts.slot_name = "test_slot";
    PostgresCdcSource src(std::move(opts));
    ASSERT_TRUE(src.restore_offset(seeded, op));

    InMemoryStateBackend out;
    src.snapshot_offset(out, op, CheckpointId{1});
    EXPECT_EQ(out.get_operator_state(op, key), seeded.get_operator_state(op, key));

    InMemoryStateBackend empty;
    PostgresCdcSource::Options o2;
    o2.conninfo = "host=127.0.0.1 port=1";
    o2.slot_name = "test_slot";
    PostgresCdcSource src2(std::move(o2));
    EXPECT_FALSE(src2.restore_offset(empty, op));
}
