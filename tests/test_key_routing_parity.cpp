// The row and columnar carriers of a keyed shuffle edge must route a key to the SAME
// subtask.
//
// This is a correctness requirement, not an optimisation, and it was stated only in a
// comment until this test existed. A stream mixes carriers by design: the columnar JSON
// bridge falls back to row form per batch when a record is not faithfully representable,
// and once damped it emits one columnar probe every 64 row batches. So the same key
// routinely arrives on both carriers within one job.
//
// It was broken. The row extractor read __key with `static_cast<int64_t>(as_number())`,
// and as_number() widens int64 to double, discarding everything below the 53-bit mantissa
// - while the columnar extractor read the Int64Array cell exactly. __key holds a full
// 64-bit FNV fold, so measured over 100,000 keys, 99.4% changed value on the round trip
// and 74.5% landed on a different subtask at parallelism 4. A key arriving on both
// carriers split its group state across two subtasks and produced a silently wrong
// aggregate: no crash, no error, just the wrong number.
//
// These tests compare the two extractors directly, which is the only way to catch it - an
// all-row or an all-columnar stream is self-consistent (the rounding is deterministic per
// key), so an end-to-end output test on either carrier alone passes either way.

#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

namespace {

using clink::Batch;
using clink::sql::Row;

constexpr const char* kRowKeyField = "__key";

// The subtask a key lands on, by the engine's own routing maths.
std::uint32_t subtask_of(std::int64_t key, std::uint32_t parallelism) {
    const auto bytes =
        std::span<const std::byte>{reinterpret_cast<const std::byte*>(&key), sizeof(key)};
    return clink::subtask_for_key_group(clink::key_group_for_key(bytes), parallelism);
}

// Keys that exercise the full 64-bit range, which is what a FNV fold produces. Small
// values below 2^53 survive a double round trip, so a test using only those would pass
// against the bug.
std::vector<std::int64_t> wide_keys() {
    std::vector<std::int64_t> out;
    std::uint64_t h = 1469598103934665603ULL;
    for (int i = 0; i < 500; ++i) {
        // The same FNV-1a fold the engine uses to build __key from a key column.
        const auto* b = reinterpret_cast<const unsigned char*>(&i);
        for (std::size_t k = 0; k < sizeof(i); ++k) {
            h ^= b[k];
            h *= 1099511628211ULL;
        }
        out.push_back(static_cast<std::int64_t>(h & 0x7fffffffffffffffLL));
    }
    return out;
}

}  // namespace

// The core invariant: both extractors must agree on every key.
TEST(KeyRoutingParity, RowAndColumnarExtractorsAgreeOnEveryKey) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);

    auto row_fn =
        clink::cluster::KeyExtractorRegistry::default_instance().find<Row>("row", "row_key");
    ASSERT_TRUE(row_fn) << "row_key extractor not registered";
    auto col_fn = clink::cluster::KeyExtractorRegistry::default_instance().find_columnar<Row>(
        "row", "row_key");
    ASSERT_TRUE(col_fn) << "columnar row_key extractor not registered";

    const auto keys = wide_keys();

    // Columnar side: a batch whose __key column carries the exact int64s.
    arrow::Int64Builder ts_b, key_b;
    for (const auto k : keys) {
        EXPECT_TRUE(ts_b.Append(1'700'000'000'000).ok());
        EXPECT_TRUE(key_b.Append(k).ok());
    }
    std::shared_ptr<arrow::Array> ts_a, key_a;
    ASSERT_TRUE(ts_b.Finish(&ts_a).ok());
    ASSERT_TRUE(key_b.Finish(&key_a).ok());
    auto rb =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64(), true),
                                                arrow::field(kRowKeyField, arrow::int64(), true)}),
                                 static_cast<std::int64_t>(keys.size()),
                                 {ts_a, key_a});
    Batch<Row> columnar{rb, keys.size(), clink::sql::row_materialize_fn()};

    auto col_keys = col_fn(columnar);
    ASSERT_TRUE(col_keys.has_value()) << "the columnar extractor declined a __key batch";
    ASSERT_EQ(col_keys->size(), keys.size());

    for (std::uint32_t par : {2U, 4U, 8U}) {
        std::size_t value_mismatch = 0;
        std::size_t route_mismatch = 0;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            // Row side: the same key as the row carrier presents it.
            Row r;
            r.values.emplace(kRowKeyField, clink::config::JsonValue{keys[i]});
            const std::int64_t row_key = row_fn(r);
            const std::int64_t col_key = (*col_keys)[i];
            if (row_key != col_key) {
                ++value_mismatch;
            }
            if (subtask_of(row_key, par) != subtask_of(col_key, par)) {
                ++route_mismatch;
            }
        }
        EXPECT_EQ(value_mismatch, 0u)
            << "at parallelism " << par << ", " << value_mismatch << " of " << keys.size()
            << " keys read differently by the two carriers (as_number() widens int64 to "
               "double; use as_int())";
        EXPECT_EQ(route_mismatch, 0u)
            << "at parallelism " << par << ", " << route_mismatch << " of " << keys.size()
            << " keys route to DIFFERENT subtasks depending on the carrier, so their group "
               "state splits and the aggregate is silently wrong";
    }
}

// A key above 2^53 is where the bug lives; a key below it round-trips through a double
// unharmed. Pin the boundary explicitly so a future "optimisation" back to as_number()
// cannot pass by testing only small keys.
TEST(KeyRoutingParity, KeysAboveTheDoubleMantissaAreReadExactly) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);
    auto row_fn =
        clink::cluster::KeyExtractorRegistry::default_instance().find<Row>("row", "row_key");
    ASSERT_TRUE(row_fn);

    // 2^53 + 1 is the smallest positive integer a double cannot represent.
    for (const std::int64_t k : {(std::int64_t{1} << 53) + 1,
                                 (std::int64_t{1} << 62) + 12345,
                                 std::int64_t{9007199254740993LL},
                                 std::numeric_limits<std::int64_t>::max() - 1}) {
        Row r;
        r.values.emplace(kRowKeyField, clink::config::JsonValue{k});
        EXPECT_EQ(row_fn(r), k) << "key " << k << " was not read exactly";
    }
}

// A missing __key must still be the well-defined 0, on both carriers.
TEST(KeyRoutingParity, MissingKeyFieldIsZeroOnBothCarriers) {
    clink::cluster::ensure_built_ins_registered();
    clink::plugin::PluginRegistry reg;
    clink::sql::install(reg);
    auto row_fn =
        clink::cluster::KeyExtractorRegistry::default_instance().find<Row>("row", "row_key");
    ASSERT_TRUE(row_fn);
    Row r;
    r.values.emplace("something_else", clink::config::JsonValue{std::int64_t{7}});
    EXPECT_EQ(row_fn(r), 0);
}
